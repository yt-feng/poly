"""Run v2 capture with in-process, read-only BTC microstructure enrichment.

Historical v2 snapshots remain schema-compatible. Extra decision features live
under microstructure (schema 3); delayed labels are archived in separate files.
"""
from __future__ import annotations
import argparse
import asyncio
import aiohttp
from contextvars import ContextVar
import hashlib
import json
from pathlib import Path
import time
import uuid
from urllib.parse import urlparse
from archive_v2 import Archive
from capture_v2 import Collector, ASSETS, CLOB
from capture_runtime_v2 import run_capture
from microstructure_v3 import Microstructure
from microstructure_math_v3 import server_time_text
from official_reference_v3 import OFFICIAL_PRICE_URL
from market_ws_guard import MarketWSGuard, market_socket, current_tokens
from measurement_v3 import event_times, side_states

OFFICIAL_PAGE_HEADER_LIMIT = 32*1024
BOOK_RESPONSE_LIMIT = 4*1024*1024


class FeatureArchive(Archive):
    def __init__(self, root, feature_engine, collector):
        super().__init__(root)
        self.feature_engine = feature_engine
        self.collector = collector

    def write(self, kind, row):
        if kind == 'snapshots':
            row = self.feature_engine.enrich(row)
            guard = self.collector.ws_guard.summary(current_tokens(self.collector), time.monotonic())
            row['poly_ws_health'] = guard
            row['poly_ws_data_valid'] = bool(self.collector.connected.get('polymarket')) and guard['all_current_tokens_fresh']
        super().write(kind,row)


class CollectorV3(Collector):
    def __init__(self, assets, root, release=None):
        super().__init__(assets,root,release)
        self.ws_guard = MarketWSGuard()
        self.micro = Microstructure(self)
        # The parent archive is still empty at this point; no existing file is replaced.
        self.archive = FeatureArchive(root,self.micro,self)
        self.capture_id = uuid.uuid4().hex
        self._attempt_sequence = 0
        self._measurement_count = 0
        self._book_attempt = ContextVar('book_attempt', default=None)
        self._measurement_markets = {}

    def new_book_attempt(self, token):
        self._attempt_sequence += 1
        return dict(attempt_id=f'{self.capture_id}:{self._attempt_sequence}',
                    requested_token_id=str(token), market=self._measurement_markets.get(str(token)),
                    parsed=False, response_seen=False, http={})

    async def poll_book(self, token):
        attempt = self.new_book_attempt(token)
        context = self._book_attempt.set(attempt)
        try:
            await super().poll_book(token)
        except asyncio.CancelledError:
            attempt['error_type'] = 'CancelledError'
            raise
        finally:
            h = attempt['http']
            row = dict(schema='book-attempt/v1', attempt_id=attempt['attempt_id'],
                requested_token_id=attempt['requested_token_id'], market=attempt['market'],
                finished_at_ns=time.time_ns(), finished_monotonic_ns=time.monotonic_ns(),
                request_started_at_ns=h.get('request_started_at_ns'),
                request_started_monotonic_ns=h.get('request_started_monotonic_ns'),
                response_received_at_ns=h.get('response_received_at_ns'),
                response_received_monotonic_ns=h.get('response_received_monotonic_ns'),
                http_status=h.get('status'), error_type=attempt.get('error_type') or h.get('error_type'),
                not_sent_reason=h.get('not_sent_reason'), parsed=attempt['parsed'],
                response_seen=attempt['response_seen'], returned_token_id=attempt.get('returned_token_id'),
                source_event_ms=attempt.get('source_event_ms'), side_states=attempt.get('side_states'),
                raw_ref=dict(attempt_id=attempt['attempt_id'],source='polymarket_rest_book_response',
                             body_sha256=h.get('captured_body_sha256')))
            try:
                self.archive.write('measurement',row)
                self._measurement_count += 1
            finally:
                self._book_attempt.reset(context)

    def poly_message(self, payload, connection_id):
        times = event_times(payload)
        event = times[0] if times and all(t is not None and t==times[0] for t in times) else None
        self.raw('polymarket_ws',payload,connection_id=connection_id,event_ms=event)

    def raw(self,source,payload,*,connection_id=None,event_ms=None):
        self.counts[source] += 1
        ns,mono = time.time_ns(),time.monotonic_ns()
        attempt = self._book_attempt.get()
        extra = {}
        if source == 'polymarket_ws':
            # Retain per-item times for batches; no single invented batch time.
            extra['source_event_times_ms'] = event_times(payload)
        if source in ('polymarket_metadata','micro_market_rules') and isinstance(payload,dict):
            market=payload.get('market')
            if isinstance(market,dict):
                try:
                    ids=market.get('clobTokenIds',[]);outcomes=market.get('outcomes',[])
                    ids=json.loads(ids) if isinstance(ids,str) else ids
                    outcomes=json.loads(outcomes) if isinstance(outcomes,str) else outcomes
                    if isinstance(ids,list) and isinstance(outcomes,list) and len(ids)==len(outcomes):
                        for token,role in zip(ids,outcomes):
                            if str(role).lower() in ('up','down'):
                                self._measurement_markets[str(token)] = dict(slug=payload.get('slug') or market.get('slug'),
                                    condition_id=market.get('conditionId'),role=str(role).lower(),observed_at_ns=ns,
                                    token_id=str(token),metadata_source=source)
                except (ValueError,TypeError):
                    pass  # Raw metadata remains archived; mapping stays unknown.
        if attempt is not None:
            extra['attempt_id']=attempt['attempt_id']
            if source=='polymarket_book_http':attempt['http']=payload
            elif source=='polymarket_rest_book_response':
                book=payload['book'];times=event_times(book)
                attempt.update(response_seen=True,side_states=side_states(book),source_event_ms=times[0] if len(times)==1 else None,
                    returned_token_id=str(book.get('asset_id',book.get('token_id',''))) if isinstance(book,dict) else None)
            elif source=='polymarket_rest_book':attempt['parsed']=True
            elif source=='polymarket_book_attempt_error':attempt['error_type']=payload.get('error_type')
        if source in {'polymarket_ws', 'polymarket_rest_book', 'binance_spot_ws', 'chainlink_rtds'}:
            self.last_data_event_ms[source] = ns//1000000
        record=dict(schema_version=3,source=source,received_at_ns=ns,
             received_monotonic_ns=mono,source_event_ms=event_ms,connection_id=connection_id,
             payload=payload,**extra)
        if source=='polymarket_rest_book' and attempt is not None:
            # Preserve the established inline payload for existing readers.
            # The reference is additive provenance, not a replacement for it.
            # Successful books are also retained in the pre-parser evidence.
            record['payload_ref']={'source':'polymarket_rest_book_response','attempt_id':attempt['attempt_id']}
            record['parse_status']='accepted'
        self.archive.write('raw',record)
        try:
            if source == 'polymarket_rest_book':
                self.ws_guard.rest_book(payload, mono/1e9)
            elif source == 'polymarket_ws':
                self.ws_guard.ws_book(payload, mono/1e9)
            self.micro.ingest(source,payload,connection_id,event_ms,ns//1000000)
        except (ValueError,TypeError,KeyError,OverflowError) as exc:
            self.micro.parse_errors += 1
            self.micro.errors['parse:'+source] = str(exc)[:300]
            # Preserve original data first; malformed optional records cannot look healthy.
            self.archive.write('source_errors',dict(schema_version=3,source=source,
                 received_ms=ns//1000000,error=str(exc)[:300]))

    async def book_http(self, url, params):
        """Retain status and bounded failed bodies without inventing an empty book."""
        context = None
        if self._book_attempt.get() is None:
            context=self._book_attempt.set(self.new_book_attempt((params or {}).get('token_id')))
        start_ns, start_mono = time.time_ns(), time.monotonic_ns()
        evidence = dict(requested_token_id=(params or {}).get('token_id'),
                        request_started_at_ns=start_ns, request_started_monotonic_ns=start_mono,
                        status=None, response_received_at_ns=None, error_type=None)
        host = urlparse(url).hostname
        try:
            if time.monotonic() < self.cooldown.get(host, 0):
                evidence['not_sent_reason'] = 'server_directed_cooldown'
                raise RuntimeError(f'{host}: server-directed cooldown')
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
                evidence['status'] = response.status
                evidence['headers'] = {k:response.headers[k] for k in ('Date','Age','Cache-Control','Content-Type') if k in response.headers}
                if response.status in (403,418,429,451):
                    retry = response.headers.get('Retry-After','300')
                    self.cooldown[host] = time.monotonic()+max(60,int(retry) if retry.isdigit() else 300)
                try:
                    body = await response.content.readexactly(BOOK_RESPONSE_LIMIT+1)
                    evidence['body_truncated'] = True
                except asyncio.IncompleteReadError as exc:
                    body = exc.partial
                    evidence['body_truncated'] = False
                evidence.update(response_received_at_ns=time.time_ns(), response_received_monotonic_ns=time.monotonic_ns(),
                    captured_body_bytes=len(body), captured_body_sha256=hashlib.sha256(body).hexdigest())
                try:
                    response.raise_for_status()
                    if evidence['body_truncated']:
                        raise ValueError('Book response exceeds capture size bound')
                    mime = response.headers.get('Content-Type','').split(';',1)[0].strip().lower()
                    if mime != 'application/json' and not (mime.startswith('application/') and mime.endswith('+json')):
                        raise aiohttp.ContentTypeError(response.request_info, response.history,
                            status=response.status, message='Unexpected book response content type', headers=response.headers)
                    payload = json.loads(body)
                except Exception:
                    # Error bodies remain local archive evidence; no stdout,
                    # request headers, credentials, or inferred venue status.
                    import base64
                    evidence['body_base64'] = base64.b64encode(body).decode('ascii')
                    raise
                return payload
        except asyncio.CancelledError:
            evidence['error_type'] = 'CancelledError'
            raise
        except Exception as exc:
            evidence['error_type'] = type(exc).__name__
            raise
        finally:
            evidence['elapsed_ms'] = (time.monotonic_ns()-start_mono)/1000000
            try:
                self.raw('http_timing', dict(host=host, path=urlparse(url).path,
                    requested_token_id=evidence['requested_token_id'], elapsed_ms=evidence['elapsed_ms'],
                    status=evidence['status'], error=evidence['error_type']))
                self.raw('polymarket_book_http', evidence)
            finally:
                if context is not None:self._book_attempt.reset(context)

    async def get(self,url,params=None):
        if url == CLOB+'/book':
            return await self.book_http(url,params)
        start = time.monotonic_ns()
        # The delegated aiohttp path does not expose a response status here.
        status,error = None,None
        try:
            if url == CLOB+'/time' or url.startswith('https://polymarket.com/event/') or url == OFFICIAL_PRICE_URL:
                host = urlparse(url).hostname
                if time.monotonic() < self.cooldown.get(host,0):
                    raise RuntimeError(f'{host}: server-directed cooldown')
                # Official HTML and the client price API both send a CSP header
                # larger than aiohttp's default. Keep the finite origin bound.
                page_limits = ({'max_line_size': OFFICIAL_PAGE_HEADER_LIMIT,
                                'max_field_size': OFFICIAL_PAGE_HEADER_LIMIT}
                               if url.startswith('https://polymarket.com/event/') or url == OFFICIAL_PRICE_URL else {})
                async with self.session.get(url,params=params,timeout=aiohttp.ClientTimeout(total=5), **page_limits) as r:
                    status = r.status
                    if status in (403,418,429,451):
                        retry = r.headers.get('Retry-After','300')
                        self.cooldown[host] = time.monotonic()+max(60,int(retry) if retry.isdigit() else 300)
                    r.raise_for_status()
                    text = await r.text()
                    if url == OFFICIAL_PRICE_URL:
                        self.raw('micro_official_price_http', {
                            'url': url, 'params': params, 'status': status,
                            'headers': {key: r.headers[key] for key in ('Date', 'Age', 'Cache-Control', 'Content-Type') if key in r.headers}})
                    if len(text) > 4*1024*1024:
                        raise ValueError('Official response exceeds capture size bound')
                    if url == OFFICIAL_PRICE_URL:
                        return json.loads(text)
                    return server_time_text(text) if url == CLOB+'/time' else text
            return await super().get(url,params)
        except Exception as exc:
            status,error = getattr(exc,'status',None),str(exc)[:200]
            raise
        finally:
            self.raw('http_timing',dict(host=urlparse(url).hostname,path=urlparse(url).path,
                     elapsed_ms=(time.monotonic_ns()-start)/1000000,status=status,error=error))

    async def socket(self, source, url, handler, subscribe=None, heartbeat=0, dynamic=False):
        if source == 'polymarket':
            return await market_socket(self, url, handler)
        return await super().socket(source, url, handler, subscribe, heartbeat, dynamic)

    async def discover(self):
        # Both sets of tasks are owned and cancelled by the same v2 runner lifecycle.
        async with asyncio.TaskGroup() as group:
            group.create_task(super().discover())
            group.create_task(self.micro.run())

    def health(self):
        h = super().health()
        h['measurement'] = {'schema':'book-attempt/v1','capture_id':self.capture_id,
                            'completed_poll_attempt_records':self._measurement_count,
                            'raw_book_storage':'legacy inline payload plus pre-parser response and additive reference'}
        h['microstructure'] = self.micro.health()
        h['poly_ws_health'] = self.ws_guard.summary(current_tokens(self), time.monotonic())
        return h


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--assets',default='btc')
    p.add_argument('--seconds',type=int,default=14400)
    p.add_argument('--output',type=Path,default=Path('capture_v2_output'))
    p.add_argument('--release')
    p.add_argument('--require-core',action='store_true')
    p.add_argument('--require-microstructure',action='store_true',help='Smoke gate: live rules, fees, matching TWAP, spot depth and Coinbase')
    a = p.parse_args()
    assets = list(dict.fromkeys(a.assets.lower().split(',')))
    if any(x not in ASSETS for x in assets) or a.seconds <= 0:
        p.error('Supported assets and positive seconds required')
    if a.output.exists() and any(a.output.iterdir()):
        p.error('A new empty output directory is required')
    c = CollectorV3(assets,a.output,a.release)
    run_capture(c, a.seconds)
    h = c.health()
    print(json.dumps(h,indent=2))
    if a.require_core and (c.valid['poly'] < 5 or c.valid['binance'] < 5):
        raise SystemExit('Core feeds failed; see health and archived errors')
    if a.require_microstructure:
        required = ['rules','fee_model','spot_depth20','coinbase','resolution_reference']
        missing = [s for s in required if c.micro.source_valid[s] < 5]
        if missing:
            raise SystemExit('Microstructure smoke failed: '+','.join(missing))


if __name__ == '__main__':
    main()
