"""Public market-feed health: heartbeats are not evidence of current book updates.

Only reconnect on sustained book silence corroborated by changing REST levels.
No account APIs, trade signals, order placement or reconstruction of missing events.
"""
from __future__ import annotations
import asyncio
from collections import deque
from decimal import Decimal, InvalidOperation
import hashlib
import json
import time
import uuid
from urllib.parse import urlparse
import aiohttp

BOOK_EVENTS = {'book', 'price_change', 'best_bid_ask'}


def book_fingerprint(payload: dict) -> str | None:
    """Ignore server hash/timestamp, ordering and numeric string formatting."""
    if not isinstance(payload, dict) or not all(s in payload for s in ('bids', 'asks')):
        return None
    levels = []
    try:
        for side in ('bids', 'asks'):
            values = []
            for item in payload[side]:
                p, q = (item['price'], item['size']) if isinstance(item, dict) else item[:2]
                p, q = Decimal(str(p)), Decimal(str(q))
                if not p.is_finite() or not q.is_finite() or p < 0 or q < 0:
                    return None
                if q:
                    values.append((p, q))
            levels.append([(str(p.normalize()), str(q.normalize())) for p, q in sorted(values)])
    except (ValueError, TypeError, KeyError, InvalidOperation):
        return None
    return hashlib.sha256(json.dumps(levels, separators=(',', ':')).encode()).hexdigest()


def book_tokens(payload) -> set[str]:
    """Recognize current-token book evidence, not PONG/trades/lifecycle chatter."""
    result = set()
    if isinstance(payload, list):
        for item in payload:
            result.update(book_tokens(item))
    elif isinstance(payload, dict):
        kind = payload.get('event_type', payload.get('type'))
        body = payload.get('payload', payload)
        if not isinstance(body, dict) or kind not in BOOK_EVENTS:
            return result
        if kind == 'price_change':
            for item in body.get('price_changes', body.get('priceChanges', [])):
                if isinstance(item, dict):
                    token = item.get('asset_id', item.get('token_id', item.get('tokenId')))
                    if token:
                        result.add(str(token))
        else:
            token = body.get('asset_id', body.get('token_id', body.get('tokenId')))
            if token:
                result.add(str(token))
    return result


class MarketWSGuard:
    def __init__(self, silence_seconds=20., min_rest_changes=3, reconnect_cooldown=60.):
        self.silence_seconds = silence_seconds
        self.min_rest_changes = min_rest_changes
        self.reconnect_cooldown = reconnect_cooldown
        self.connection_id = None
        self.subscribed = {}
        self.rest = {}
        self.last_book = {}
        self.book_counts = {}
        self.pong_at = None
        self.last_reconnect = -float('inf')
        self.reconnects = 0
        self.quarantine = deque(maxlen=100)

    def connect(self, connection_id):
        self.connection_id = connection_id
        self.subscribed.clear()
        self.last_book.clear()
        self.pong_at = None

    def subscriptions(self, tokens, now):
        wanted = set(map(str, tokens))
        self.subscribed = {t: self.subscribed.get(t, now) for t in wanted}
        self.last_book = {t: v for t, v in self.last_book.items() if t in wanted}
        # Bound state to active subscriptions plus recently polled current tokens.
        self.rest = {t: v for t, v in self.rest.items() if t in wanted or now-v['time'] < 60}
        self.book_counts = {t: self.book_counts.get(t, 0) for t in wanted}

    def rest_book(self, payload, now):
        token = str(payload.get('asset_id', '')) if isinstance(payload, dict) else ''
        fp = book_fingerprint(payload)
        if not token or fp is None:
            return
        old = self.rest.get(token)
        changes = old['changes'] if old else deque(maxlen=128)
        if old and old['fingerprint'] != fp:
            changes.append(now)
        self.rest[token] = {'fingerprint': fp, 'time': now, 'changes': changes}

    def ws_book(self, payload, now):
        for token in book_tokens(payload) & self.subscribed.keys():
            self.last_book[token] = now
            self.book_counts[token] = self.book_counts.get(token, 0) + 1

    def token_state(self, token, now):
        subscribed_at = self.subscribed.get(token)
        ws = self.last_book.get(token)
        rest = self.rest.get(token)
        baseline = max(subscribed_at or 0, ws or 0)
        changes = [t for t in rest['changes'] if t > baseline and now-t <= 60] if rest else []
        suspect = (subscribed_at is not None and now-baseline >= self.silence_seconds and
                   rest is not None and 0 <= now-rest['time'] <= 5 and
                   len(changes) >= self.min_rest_changes)
        return {'subscribed': subscribed_at is not None,
                'book_age_seconds': round(now-ws, 3) if ws is not None else None,
                'rest_age_seconds': round(now-rest['time'], 3) if rest else None,
                'rest_changes_since_last_ws_book': len(changes),
                'book_event_count': self.book_counts.get(token, 0),
                'suspect_silence': suspect,
                'fresh_book_evidence': bool(subscribed_at is not None and ws is not None and 0 <= now-ws <= 5)}

    def suspect_tokens(self, current_tokens, now):
        return [str(t) for t in current_tokens if self.token_state(str(t), now)['suspect_silence']]

    def request_reconnect(self, current_tokens, now):
        bad = self.suspect_tokens(current_tokens, now)
        if not bad or now-self.last_reconnect < self.reconnect_cooldown:
            return None
        self.last_reconnect = now
        self.reconnects += 1
        notice = {'reason': 'current_book_silent_while_rest_levels_change',
                  'connection_id': self.connection_id, 'tokens': bad,
                  'evidence': {t: self.token_state(t, now) for t in bad},
                  'missing_events_recovered': False}
        self.quarantine.append(notice)
        return notice

    def summary(self, current_tokens, now):
        states = {str(t): self.token_state(str(t), now) for t in current_tokens}
        return {'connection_id': self.connection_id, 'current_tokens': states,
                'pong_age_seconds': round(now-self.pong_at, 3) if self.pong_at is not None else None,
                'guard_reconnects': self.reconnects,
                'all_current_tokens_fresh': bool(states) and all(x['fresh_book_evidence'] for x in states.values()),
                'suspect_current_tokens': [t for t, x in states.items() if x['suspect_silence']],
                'event_completeness_certified': False}


def current_tokens(collector):
    start = int(time.time())//300*300
    return {t for a in collector.assets
            for t in collector.markets.get(f'{a}-updown-5m-{start}', {}).values()}


async def market_socket(collector, url, handler):
    """Isolated market stream; other feeds keep the existing v2 implementation."""
    from capture_v2 import decode_frame
    guard = collector.ws_guard
    failures = 0
    while True:
        connection_id = uuid.uuid4().hex
        maintenance = None
        try:
            async with collector.session.ws_connect(url, autoping=True, heartbeat=None,
                                                    max_msg_size=16*1024*1024) as ws:
                connected_at = time.monotonic()
                guard.connect(connection_id)
                collector.connected['polymarket'] = True
                collector.raw('connection', {'source': 'polymarket', 'state': 'connected'}, connection_id=connection_id)

                async def maintain():
                    initialized = False
                    last_ping = -float('inf')
                    while True:
                        now = time.monotonic()
                        wanted = {t for v in collector.markets.values() for t in v.values()}
                        known = set(guard.subscribed)
                        frames = []
                        if not initialized and wanted:
                            frames.append({'type': 'market', 'assets_ids': sorted(wanted), 'custom_feature_enabled': True})
                        elif initialized:
                            for op, ids in [('subscribe', wanted-known), ('unsubscribe', known-wanted)]:
                                if ids:
                                    frames.append({'operation': op, 'assets_ids': sorted(ids)})
                        for frame in frames:
                            await ws.send_json(frame)
                            collector.raw('polymarket_subscription', frame, connection_id=connection_id)
                        if frames:
                            initialized = True
                            guard.subscriptions(wanted, now)
                        if now-last_ping >= 10:
                            await ws.send_str('PING')
                            last_ping = now
                        if now-(guard.pong_at if guard.pong_at is not None else connected_at) > 35:
                            raise RuntimeError('market heartbeat response timeout')
                        notice = guard.request_reconnect(current_tokens(collector), now)
                        if notice:
                            collector.raw('polymarket_ws_gap', notice, connection_id=connection_id)
                            # Wake the reader now even when PONGs keep the transport alive.
                            await ws.close(code=1000, message=b'data freshness resync')
                            return
                        await asyncio.sleep(1)

                maintenance = asyncio.create_task(maintain())
                while True:
                    if maintenance.done():
                        await maintenance
                        raise RuntimeError('market subscription maintenance exited')
                    try:
                        msg = await asyncio.wait_for(ws.receive(), timeout=1)
                    except asyncio.TimeoutError:
                        continue
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        kind, payload = decode_frame(msg.data)
                        if kind == 'pong':
                            guard.pong_at = time.monotonic()
                        elif kind == 'ping':
                            await ws.send_str('PONG')
                        elif kind == 'data':
                            handler(payload, connection_id)
                            # Reset backoff only on data for a subscribed book token.
                            if book_tokens(payload) & guard.subscribed.keys():
                                failures = 0
                        elif kind != 'empty':
                            collector.raw('protocol_control', {'source': 'polymarket', 'kind': kind,
                                          'text': msg.data}, connection_id=connection_id)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                        raise RuntimeError('market stream closed')
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError, ValueError) as exc:
            collector.error('polymarket', exc)
            if getattr(exc, 'status', 0) in (403, 418, 429, 451):
                # Respect source access/rate restrictions; no alternate route.
                collector.cooldown[urlparse(url).hostname] = time.monotonic()+300
        finally:
            collector.connected['polymarket'] = False
            collector.raw('connection', {'source': 'polymarket', 'state': 'disconnected'}, connection_id=connection_id)
            if maintenance:
                maintenance.cancel()
                await asyncio.gather(maintenance, return_exceptions=True)
            guard.subscriptions(set(), time.monotonic())
        failures += 1
        pause = max(min(60, 2**min(failures, 6)),
                    collector.cooldown.get(urlparse(url).hostname, 0)-time.monotonic())
        await asyncio.sleep(pause)
