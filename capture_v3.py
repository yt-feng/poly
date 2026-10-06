"""Run v2 capture with in-process, read-only BTC microstructure enrichment.

Historical v2 snapshots remain schema-compatible. Extra decision features live
under microstructure (schema 3); delayed labels are archived in separate files.
"""
from __future__ import annotations
import argparse
import asyncio
import aiohttp
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import urlparse
from archive_v2 import Archive
from capture_v2 import Collector, ASSETS, CLOB
from capture_runtime_v2 import run_capture
from microstructure_v3 import Microstructure
from microstructure_math_v3 import server_time_text
from official_reference_v3 import OFFICIAL_PRICE_URL
from market_ws_guard import MarketWSGuard, market_socket, current_tokens

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

    def raw(self,source,payload,*,connection_id=None,event_ms=None):
        self.counts[source] += 1
        ns,mono = time.time_ns(),time.monotonic_ns()
        if source in {'polymarket_ws', 'polymarket_rest_book', 'binance_spot_ws', 'chainlink_rtds'}:
            self.last_data_event_ms[source] = ns//1000000
        self.archive.write('raw',dict(schema_version=3,source=source,received_at_ns=ns,
             received_monotonic_ns=mono,source_event_ms=event_ms,connection_id=connection_id,payload=payload))
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
        except Exception as exc:
            evidence['error_type'] = type(exc).__name__
            raise
        finally:
            evidence['elapsed_ms'] = (time.monotonic_ns()-start_mono)/1000000
            self.raw('http_timing', dict(host=host, path=urlparse(url).path,
                requested_token_id=evidence['requested_token_id'], elapsed_ms=evidence['elapsed_ms'],
                status=evidence['status'], error=evidence['error_type']))
            self.raw('polymarket_book_http', evidence)

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
