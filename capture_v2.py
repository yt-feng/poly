"""Independent public feeds, raw events, and deadline-driven one-second snapshots.

No authenticated trading APIs, credentials for exchanges, or order execution.
The legacy collector/CSV schema is intentionally left untouched.
"""
from __future__ import annotations
import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import time
import uuid
from urllib.parse import urlparse
import aiohttp
from archive_v2 import Archive, atomic_json, publish, upload_ready
from quality_v2 import report

ASSETS = {'btc': 'BTCUSDT', 'eth': 'ETHUSDT', 'sol': 'SOLUSDT'}
BINANCE_REST = 'https://data-api.binance.vision'
BINANCE_WS = 'wss://data-stream.binance.vision/stream?streams='
POLY_WS = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
GAMMA = 'https://gamma-api.polymarket.com'
CLOB = 'https://clob.polymarket.com'
CHAINLINK_WS = 'wss://ws-live-data.polymarket.com'


def epoch_ms(value):
    if value in (None, ''):
        return None
    try:
        n = int(value)
        return n // 1000 if n > 10**14 else n * 1000 if n < 10**11 else n
    except (ValueError, TypeError):
        try:
            return int(datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp()*1000)
        except ValueError:
            return None


def next_tick(deadline: float, now: float, interval: float = 1.0) -> float:
    """Keep a monotonic phase, skipping missed slots rather than burst-catching up."""
    return deadline + max(1, math.floor((now-deadline)/interval)+1)*interval


def window_slug(asset: str, now: float) -> str:
    return f'{asset}-updown-5m-{int(now)//300*300}'


def decode_list(value):
    return json.loads(value) if isinstance(value, str) else value


def decode_frame(text: str):
    """Return a control kind or a structured message without inventing data."""
    text = text.strip().lstrip('\ufeff').strip()
    if not text:
        return 'empty', None
    if text.lower() in ('ping', 'pong'):
        return text.lower(), None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return 'non_json', text
    if not isinstance(payload, (dict, list)):
        return 'control', payload
    return 'data', payload


def book_summary(payload: dict) -> dict:
    out = {}
    for side, reverse in [('bids', True), ('asks', False)]:
        levels = []
        for item in payload.get(side, []):
            p, q = (item['price'], item['size']) if isinstance(item, dict) else item[:2]
            p, q = float(p), float(q)
            if math.isfinite(p) and math.isfinite(q) and p >= 0 and q > 0:
                levels.append((p, q))
        levels.sort(reverse=reverse)
        key = 'bid' if reverse else 'ask'
        out[key] = levels[0][0] if levels else None
        out[key+'_size'] = levels[0][1] if levels else None
        out[key+'_depth5'] = sum(q for _, q in levels[:5]) if levels else None
        out[key+'_levels'] = len(levels)
    return out


def fresh(item, now_ms: int, max_age: int, *, require_event: bool = False) -> bool:
    if not item or not 0 <= now_ms-item['received_ms'] <= max_age:
        return False
    event_ms = item.get('event_ms')
    return (not require_event) or (event_ms is not None and -2000 <= now_ms-event_ms <= max_age)


class Collector:
    def __init__(self, assets: list[str], root: Path, release: str | None = None):
        self.assets, self.root, self.release = assets, root, release
        self.archive = Archive(root)
        self.markets, self.books, self.prices, self.tickers = {}, {}, {}, {}
        self.connected, self.counts, self.valid = {}, Counter(), Counter()
        self.last_error, self.last_error_time, self.cooldown = {}, {}, {}
        self.latest_sample_ms = None
        self.last_valid_ms = {}
        self.started_ms = int(time.time()*1000)
        self.session = None
        self.pre_shutdown_live_health = None

    def raw(self, source: str, payload, *, connection_id=None, event_ms=None):
        self.counts[source] += 1
        self.archive.write('raw', dict(schema_version=2, source=source,
                           received_at_ns=time.time_ns(), source_event_ms=event_ms,
                           connection_id=connection_id, payload=payload))

    def error(self, source: str, exc):
        message = str(exc)[:400]
        self.last_error[source] = message
        if time.monotonic()-self.last_error_time.get(source, -1000) >= 60:
            self.raw('error', {'source': source, 'message': message})
            self.last_error_time[source] = time.monotonic()
            print(f'{source}: {message}', flush=True)

    async def get(self, url, params=None):
        host = urlparse(url).hostname
        if time.monotonic() < self.cooldown.get(host, 0):
            raise RuntimeError(f'{host}: server-directed cooldown')
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status in (403, 418, 429, 451):
                retry = r.headers.get('Retry-After', '300')
                self.cooldown[host] = time.monotonic()+max(60, int(retry) if retry.isdigit() else 300)
            r.raise_for_status()
            return await r.json()

    async def discover(self):
        while True:
            now = time.time()
            wanted = {window_slug(a, now+offset) for a in self.assets for offset in (-300, 0, 300)}
            for slug in sorted(wanted):
                if slug in self.markets:
                    continue
                try:
                    payload = await self.get(GAMMA+'/markets', {'slug': slug})
                    if not isinstance(payload, list) or not payload:
                        continue
                    market = payload[0]
                    ids = decode_list(market.get('clobTokenIds', []))
                    outcomes = decode_list(market.get('outcomes', []))
                    tokens = {str(o).lower(): str(t) for o, t in zip(outcomes, ids)}
                    if 'up' not in tokens or 'down' not in tokens:
                        raise ValueError(f'Unrecognized outcome mapping for {slug}')
                    self.markets[slug] = tokens
                    self.raw('polymarket_metadata', {'slug': slug, 'market': market})
                except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError) as e:
                    self.error('discovery', e)
            # Retain the previous window on the socket to observe resolution events.
            self.markets = {k: v for k, v in self.markets.items() if k in wanted}
            token_set = {t for v in self.markets.values() for t in v.values()}
            self.books = {k: v for k, v in self.books.items() if k in token_set}
            await asyncio.sleep(5)

    async def poll_book(self, token):
        try:
            payload = await self.get(CLOB+'/book', {'token_id': token})
            ms = int(time.time()*1000)
            self.books[token] = dict(received_ms=ms, event_ms=epoch_ms(payload.get('timestamp')),
                                    **book_summary(payload))
            self.raw('polymarket_rest_book', payload, event_ms=epoch_ms(payload.get('timestamp')))
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError) as e:
            self.error('polymarket_rest_book', e)

    async def poll_books(self):
        deadline = time.monotonic()
        while True:
            tokens = [t for a in self.assets
                      for t in self.markets.get(window_slug(a, time.time()), {}).values()]
            await asyncio.gather(*(self.poll_book(t) for t in tokens))
            deadline = next_tick(deadline, time.monotonic())
            await asyncio.sleep(max(0, deadline-time.monotonic()))

    async def heartbeat(self, ws, seconds, subscriptions=None, ping_text='PING'):
        known = set()
        while True:
            if subscriptions:
                wanted = {t for v in self.markets.values() for t in v.values()}
                if not known and wanted:
                    await ws.send_json({'type': 'market', 'assets_ids': sorted(wanted),
                                        'custom_feature_enabled': True})
                else:
                    for operation, ids in [('subscribe', wanted-known), ('unsubscribe', known-wanted)]:
                        if ids:
                            await ws.send_json({'operation': operation, 'assets_ids': sorted(ids)})
                known = wanted
            await ws.send_str(ping_text)
            await asyncio.sleep(seconds)

    async def socket(self, source, url, handler, subscribe=None, heartbeat=0, dynamic=False):
        attempt = 0
        while True:
            heart = None
            connection_id = uuid.uuid4().hex
            try:
                async with self.session.ws_connect(url, autoping=True, heartbeat=None,
                                                    max_msg_size=16*1024*1024) as ws:
                    self.connected[source] = True
                    self.raw('connection', {'source': source, 'state': 'connected'}, connection_id=connection_id)
                    if subscribe:
                        await ws.send_json(subscribe)
                    if heartbeat:
                        heart = asyncio.create_task(self.heartbeat(
                            ws, heartbeat, dynamic, 'ping' if source == 'chainlink' else 'PING'))
                    while True:
                        if heart and heart.done():
                            await heart
                        msg = await asyncio.wait_for(ws.receive(), timeout=45)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            kind, payload = decode_frame(msg.data)
                            if kind in ('empty', 'ping', 'pong'):
                                if kind == 'ping':
                                    await ws.send_str('pong' if source == 'chainlink' else 'PONG')
                                continue
                            if kind != 'data':
                                self.raw('protocol_control', {'source': source, 'kind': kind,
                                         'text': msg.data}, connection_id=connection_id)
                                if kind == 'non_json':
                                    self.error(source+'_protocol', repr(msg.data[:200]))
                                continue
                            handler(payload, connection_id)
                            attempt = 0
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE,
                                          aiohttp.WSMsgType.ERROR):
                            raise RuntimeError(f'{source}: socket closed')
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError) as e:
                self.error(source, e)
            finally:
                self.connected[source] = False
                self.raw('connection', {'source': source, 'state': 'disconnected'}, connection_id=connection_id)
                if heart:
                    heart.cancel()
                    await asyncio.gather(heart, return_exceptions=True)
            attempt += 1
            await asyncio.sleep(min(60, 2**min(attempt, 6)))

    def binance_message(self, asset, message, connection_id):
        payload = message.get('data', message)
        stream = message.get('stream', '')
        event_ms = epoch_ms(payload.get('E', payload.get('T')))
        self.raw('binance_spot_ws', message, connection_id=connection_id, event_ms=event_ms)
        received = int(time.time()*1000)
        if stream.endswith('@trade') or payload.get('e') == 'trade':
            self.prices[asset] = dict(price=payload['p'], received_ms=received,
                                      event_ms=epoch_ms(payload.get('T', payload.get('E'))),
                                      trade_id=payload.get('t'))
        elif stream.endswith('@bookTicker'):
            self.tickers[asset] = dict(bid=payload['b'], ask=payload['a'], bid_size=payload['B'],
                                      ask_size=payload['A'], update_id=payload['u'],
                                      received_ms=received, event_ms=event_ms)
        if payload.get('e') == 'depthUpdate':
            key = (asset, connection_id)
            last = getattr(self, '_depth_last', {})
            previous = last.get(key)
            if previous is not None and int(payload['U']) > previous+1:
                self.raw('depth_gap', {'asset': asset, 'previous_u': previous,
                                      'next_U': payload['U']}, connection_id=connection_id)
            last[key] = max(previous or 0, int(payload['u']))
            self._depth_last = {k: v for k, v in last.items() if k[0] != asset or k == key}

    def poly_message(self, payload, connection_id):
        self.raw('polymarket_ws', payload, connection_id=connection_id)

    def chainlink_message(self, message, connection_id):
        if not isinstance(message, dict):
            return
        payload = message.get('payload', {})
        if not isinstance(payload, dict):
            return
        symbol = str(payload.get('symbol', '')).split('/')[0].lower()
        if symbol not in self.assets:
            return
        self.raw('chainlink_rtds', message, connection_id=connection_id,
                 event_ms=epoch_ms(payload.get('timestamp')))
        # A subscribe-history batch may use a generic topic. Preserve it, but
        # never relabel it as a verified live Chainlink price update.
        if 'value' in payload and 'chainlink' in message.get('topic', ''):
            self.prices['chainlink_'+symbol] = dict(price=payload['value'],
                          received_ms=int(time.time()*1000), event_ms=epoch_ms(payload.get('timestamp')))

    async def depth_snapshots(self, asset):
        while True:
            try:
                payload = await self.get(BINANCE_REST+'/api/v3/depth',
                                         {'symbol': ASSETS[asset], 'limit': 5000})
                self.raw('binance_depth_snapshot', {'symbol': ASSETS[asset], **payload})
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError) as e:
                self.error('binance_depth_snapshot', e)
            await asyncio.sleep(60)

    def snapshot(self, asset, scheduled, now):
        ms = int(time.time()*1000)
        slug = window_slug(asset, ms/1000)
        tokens = self.markets.get(slug, {})
        up, down = self.books.get(tokens.get('up')), self.books.get(tokens.get('down'))
        bp, cp = self.prices.get(asset), self.prices.get('chainlink_'+asset)
        ticker = self.tickers.get(asset)
        row = dict(schema_version=2, sample_ms=ms, asset=asset, slug=slug,
                   sampler_lag_ms=round(max(0, now-scheduled)*1000, 3),
                   window_start_ms=int(slug.rsplit('-', 1)[1])*1000,
                   up_token_id=tokens.get('up'), down_token_id=tokens.get('down'),
                   poly_source='polymarket_clob_rest', up=up, down=down,
                   binance_source='binance_spot_ws', binance_symbol=ASSETS[asset],
                   binance=bp, binance_book=ticker,
                   chainlink_source='polymarket_rtds_chainlink', chainlink=cp,
                   poly_valid=fresh(up, ms, 3000) and fresh(down, ms, 3000),
                   binance_valid=bool(self.connected.get('binance_'+asset)) and fresh(bp, ms, 5000, require_event=True),
                   chainlink_valid=bool(self.connected.get('chainlink')) and fresh(cp, ms, 10000, require_event=True),
                   binance_book_valid=bool(self.connected.get('binance_'+asset)) and fresh(ticker, ms, 5000),
                   poly_ws_connected=bool(self.connected.get('polymarket')),
                   official_price_to_beat=None, official_resolution_price=None)
        for source in ('poly', 'binance', 'chainlink'):
            self.valid[source] += int(row[source+'_valid'])
            if row[source+'_valid']:
                self.last_valid_ms[source+':'+asset] = ms
        self.latest_sample_ms = ms
        self.archive.write('snapshots', row)

    async def sample(self, duration):
        start = deadline = time.monotonic()
        while time.monotonic()-start < duration:
            now = time.monotonic()
            for asset in self.assets:
                self.snapshot(asset, deadline, now)
            following = next_tick(deadline, time.monotonic())
            if following-deadline > 1.01:
                self.raw('sampler_gap', {'skipped_slots': round(following-deadline)-1})
            deadline = following
            await asyncio.sleep(max(0, deadline-time.monotonic()))

    def health(self):
        h = dict(schema_version=2, updated_ms=int(time.time()*1000), started_ms=self.started_ms,
                 latest_sample_ms=self.latest_sample_ms, assets=self.assets,
                 connected=self.connected.copy(), raw_counts=dict(self.counts),
                 valid_snapshot_counts=dict(self.valid), last_valid_ms=self.last_valid_ms.copy(),
                 last_errors=self.last_error.copy())
        if self.pre_shutdown_live_health is not None:
            h['pre_shutdown_live_health'] = self.pre_shutdown_live_health
        return h

    async def checkpoint(self):
        while True:
            atomic_json(self.root/'health.json', self.health())
            if self.release:
                try:
                    await asyncio.to_thread(upload_ready, self.root, self.release)
                    await asyncio.to_thread(publish, self.release, [self.root/'health.json'], replace=True)
                except Exception as e:
                    self.error('publication', e)
            await asyncio.sleep(60)

    async def run(self, duration):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15),
                      headers={'User-Agent': 'poly-capture-v2/2.0'}) as self.session:
            jobs = [asyncio.create_task(self.discover()), asyncio.create_task(self.poll_books()),
                    asyncio.create_task(self.socket('polymarket', POLY_WS, self.poly_message,
                                                     heartbeat=10, dynamic=True)),
                    asyncio.create_task(self.checkpoint())]
            # The filtered RTDS route returned only a subscribe-history batch in
            # live validation. Subscribe to the documented full Chainlink topic
            # and retain only explicitly configured symbols in the handler.
            subscriptions = [{'topic': 'crypto_prices_chainlink', 'type': '*'}]
            jobs.append(asyncio.create_task(self.socket('chainlink', CHAINLINK_WS,
                        self.chainlink_message, {'action': 'subscribe', 'subscriptions': subscriptions}, heartbeat=5)))
            for asset in self.assets:
                streams = '/'.join(ASSETS[asset].lower()+'@'+s for s in
                          ('trade', 'aggTrade', 'bookTicker', 'depth@100ms', 'kline_1s', 'kline_1m'))
                jobs.append(asyncio.create_task(self.socket('binance_'+asset, BINANCE_WS+streams,
                     lambda p, c, a=asset: self.binance_message(a, p, c))))
                jobs.append(asyncio.create_task(self.depth_snapshots(asset)))
            sampler = asyncio.create_task(self.sample(duration))
            try:
                done, _ = await asyncio.wait([sampler, *jobs], return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    await task  # propagate unexpected failure instead of a green empty run
                if sampler not in done:
                    raise RuntimeError('A collector task exited unexpectedly')
            finally:
                # Preserve the last live view before socket cancellation clears
                # connection/subscription state. Final health remains post-shutdown,
                # while this nested snapshot avoids mistaking normal teardown for
                # an in-run data-freshness failure.
                self.pre_shutdown_live_health = self.health()
                for task in [sampler, *jobs]:
                    task.cancel()
                await asyncio.gather(sampler, *jobs, return_exceptions=True)
                self.archive.close()
                atomic_json(self.root/'health.json', self.health())
                report(self.root, self.root/'quality.json', assets=self.assets)
                if self.release:
                    await asyncio.to_thread(upload_ready, self.root, self.release)
                    await asyncio.to_thread(publish, self.release,
                         [self.root/'health.json', self.root/'quality.json', self.root/'quality.md'], replace=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--assets', default='btc', help='Comma-separated btc,eth,sol; explicit bounded scope.')
    p.add_argument('--seconds', type=int, default=14400)
    p.add_argument('--output', type=Path, default=Path('capture_v2_output'))
    p.add_argument('--release', help='Run-specific GitHub Release tag; requires GH_TOKEN/GH_REPO and gh.')
    p.add_argument('--require-core', action='store_true', help='Fail when no valid Poly or Binance samples were obtained.')
    a = p.parse_args()
    assets = list(dict.fromkeys(a.assets.lower().split(',')))
    if not assets or any(x not in ASSETS for x in assets) or a.seconds <= 0:
        p.error('Use supported assets and a positive duration')
    if a.output.exists() and any(a.output.iterdir()):
        p.error('Output must be empty: one directory per run prevents segment overwrite')
    c = Collector(assets, a.output, a.release)
    asyncio.run(c.run(a.seconds))
    print(json.dumps(c.health(), indent=2))
    if a.require_core and (c.valid['poly'] < 5 or c.valid['binance'] < 5):
        raise SystemExit('Core feed smoke check failed; archived diagnostics show the missing feed.')


if __name__ == '__main__':
    main()
