"""Read-only HTTP/WS transports. Native payloads are archived before decoding."""
from __future__ import annotations
import asyncio
import base64
from collections import Counter
import json
import os
import random
import struct
import time
import uuid
from urllib.parse import urlsplit
import aiohttp

# Yahoo wire schema reference: ranaroussi/yfinance, yfinance/pricing.proto.
# This decoder does not synthesize missing fields or infer exchange executions.
YAHOO_FIELDS = {
    1: ('id', 'str'), 2: ('price', 'float'), 3: ('time', 'zigzag'),
    4: ('currency', 'str'), 5: ('exchange', 'str'), 6: ('quote_type', 'int'),
    7: ('market_hours', 'int'), 8: ('change_percent', 'float'),
    9: ('day_volume', 'zigzag'), 10: ('day_high', 'float'), 11: ('day_low', 'float'),
    12: ('change', 'float'), 13: ('short_name', 'str'), 14: ('expire_date', 'zigzag'),
    15: ('open_price', 'float'), 16: ('previous_close', 'float'),
    17: ('strike_price', 'float'), 18: ('underlying_symbol', 'str'),
    19: ('open_interest', 'zigzag'), 20: ('options_type', 'zigzag'),
    21: ('mini_option', 'zigzag'), 22: ('last_size', 'zigzag'), 23: ('bid', 'float'),
    24: ('bid_size', 'zigzag'), 25: ('ask', 'float'), 26: ('ask_size', 'zigzag'),
    27: ('price_hint', 'zigzag'), 28: ('vol_24hr', 'zigzag'),
    29: ('vol_all_currencies', 'zigzag'), 30: ('from_currency', 'str'),
    31: ('last_market', 'str'), 32: ('circulating_supply', 'double'), 33: ('market_cap', 'double'),
}


def decode_yahoo(encoded: str) -> dict:
    data = base64.b64decode(encoded, validate=True)
    pos, out = 0, {}
    def varint():
        nonlocal pos
        result = 0
        for shift in range(0, 70, 7):
            if pos >= len(data):
                raise ValueError('truncated_varint')
            b = data[pos]
            pos += 1
            result |= (b & 127) << shift
            if not b & 128:
                return result
        raise ValueError('oversized_varint')
    while pos < len(data):
        key = varint()
        number, wire = key >> 3, key & 7
        if number == 0:
            raise ValueError('zero_field')
        if wire == 0:
            value = varint()
        elif wire in (1, 5, 2):
            length = varint() if wire == 2 else (8 if wire == 1 else 4)
            if pos + length > len(data):
                raise ValueError('truncated_field')
            value = data[pos:pos+length]
            pos += length
        else:
            raise ValueError('unsupported_wire_type')
        name, typ = YAHOO_FIELDS.get(number, (f'unknown_{number}', 'raw'))
        if typ == 'str' and wire == 2:
            value = value.decode('utf-8')
        elif typ == 'float' and wire == 5:
            value = struct.unpack('<f', value)[0]
        elif typ == 'double' and wire == 1:
            value = struct.unpack('<d', value)[0]
        elif typ == 'zigzag' and wire == 0:
            value = (value >> 1) ^ -(value & 1)
        elif isinstance(value, bytes):
            value = {'wire': wire, 'base64': base64.b64encode(value).decode()}
        out[name] = value
    return out


class PublicHTTP:
    def __init__(self, session, journal, stats):
        self.session, self.journal, self.stats = session, journal, stats
        self.gates, self.last = {}, {}

    async def get(self, source, url, params=None, *, headers=None, context=None):
        # GET only, no account API, no signed requests, no order endpoints.
        host = urlsplit(url).netloc
        gate = self.gates.setdefault(host, asyncio.Lock())
        async with gate:
            delay = self.last.get(host, 0) + (0.25 if 'yahoo' in host else 0.08) - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self.last[host] = time.monotonic()
        for attempt in range(3):
            started = time.time_ns()
            try:
                async with self.session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    chunks, size = [], 0
                    async for chunk in response.content.iter_chunked(65536):
                        chunks.append(chunk)
                        size += len(chunk)
                        if size > 32*1024*1024:
                            raise ValueError('http_entity_exceeds_32MiB')
                    body = b''.join(chunks)
                    self.journal.wire(source, body, transport='http', url=url, params=params or {},
                                      status=response.status, request_started_ns=started,
                                      response_headers={k: response.headers[k] for k in ('Date', 'Content-Type', 'ETag', 'Last-Modified', 'Retry-After') if k in response.headers},
                                      attempt=attempt+1, context=context or {})
                    self.stats[f'http_{source}_{response.status}'] += 1
                    try:
                        parsed = json.loads(body)
                    except (ValueError, UnicodeError):
                        parsed = None
                    if response.status == 429 or response.status >= 500:
                        wait = response.headers.get('Retry-After', '')
                        delay = min(120, float(wait)) if wait.isdigit() else min(30, 2**attempt * 2)
                        await asyncio.sleep(delay + random.random())
                        continue
                    return response.status, parsed
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                self.stats[f'http_{source}_transport_error'] += 1
                self.journal.emit('audit', dict(event='http_error', source=source, url=url,
                                               error_type=type(exc).__name__, attempt=attempt+1,
                                               request_started_ns=started))
                await asyncio.sleep(2**attempt + random.random())
        return 0, None


def events(raw):
    value = json.loads(raw)
    return value if isinstance(value, list) else [value]


def market_observations(journal, stats, coverage, raw):
    if raw in ('PONG', 'PING', b'PONG', b'PING'):
        return
    try:
        for e in events(raw):
            if not isinstance(e, dict):
                continue
            typ = e.get('event_type', e.get('type', 'unknown'))
            stats[f'pm_event_{typ}'] += 1
            body = e.get('payload', e)
            tokens = {str(body.get('asset_id') or body.get('tokenId') or body.get('token_id') or '')}
            for x in body.get('price_changes', body.get('priceChanges', [])):
                tokens.add(str(x.get('asset_id') or x.get('token_id') or x.get('tokenId') or ''))
            for token in tokens - {''}:
                c = coverage.setdefault(token, {'events': 0, 'first_received_ns': time.time_ns()})
                c['events'] += 1
                c['last_received_ns'] = time.time_ns()
                c['last_event_type'] = typ
                if typ == 'book':
                    c['last_book_received_ns'] = time.time_ns()
    except (ValueError, TypeError, AttributeError):
        stats['pm_decode_errors'] += 1
        journal.emit('audit', {'event': 'pm_decode_error_raw_retained'})


async def stream(session, journal, stats, source, url, wanted, *, kind, coverage, resnapshot=None, feed=None):
    """Dynamic subscriptions, application heartbeats, reconnect + gap boundaries."""
    backoff = 1
    while True:
        target = set(wanted())
        if not target:
            await asyncio.sleep(2)
            continue
        conn, count, opened_ns = uuid.uuid4().hex, 0, time.time_ns()
        try:
            async with session.ws_connect(url, heartbeat=20 if kind != 'polymarket' else None,
                                          max_msg_size=16*1024*1024, timeout=15) as ws:
                journal.emit('audit', dict(event='ws_connected', source=source, connection_id=conn,
                                           requested_symbols=sorted(target), feed=feed))
                if kind == 'alpaca':
                    await ws.send_json({'action': 'auth', 'key': os.environ['APCA_API_KEY_ID'],
                                        'secret': os.environ['APCA_API_SECRET_KEY']})
                    # Authentication frames are never archived or logged.
                    authenticated = False
                    for _ in range(5):
                        msg = await ws.receive(timeout=15)
                        journal.wire(source, msg.data if isinstance(msg.data, (str, bytes)) else str(msg.data),
                                     connection_id=conn, transport='websocket', feed=feed)
                        payloads = events(msg.data)
                        if any(x.get('T') == 'error' for x in payloads):
                            raise RuntimeError('alpaca_auth_error')
                        if any(x.get('msg') == 'authenticated' for x in payloads):
                            authenticated = True
                            break
                    if not authenticated:
                        raise RuntimeError('alpaca_auth_not_confirmed')
                subscribed, first, next_control, last_receive = set(), True, 0, time.monotonic()
                while True:
                    now = time.monotonic()
                    if now >= next_control:
                        target = set(wanted())
                        added, removed = target - subscribed, subscribed - target
                        if kind == 'polymarket':
                            if removed:
                                await ws.send_json({'assets_ids': sorted(removed), 'operation': 'unsubscribe'})
                            if added:
                                message = {'assets_ids': sorted(added), 'custom_feature_enabled': True}
                                message.update({'type': 'market'} if first else {'operation': 'subscribe'})
                                await ws.send_json(message)
                                if resnapshot:
                                    resnapshot.set()
                            await ws.send_str('PING')
                        elif kind == 'yahoo':
                            if removed:
                                await ws.send_json({'unsubscribe': sorted(removed)})
                            if target:
                                await ws.send_json({'subscribe': sorted(target)})
                        else:
                            channels = ['trades', 'quotes', 'bars', 'updatedBars', 'dailyBars']
                            if feed in ('sip', 'delayed_sip'):
                                channels += ['statuses', 'lulds']
                            for action, symbols in [('unsubscribe', removed), ('subscribe', added)]:
                                if symbols:
                                    await ws.send_json({'action': action, **{c: sorted(symbols) for c in channels}})
                        if added or removed:
                            journal.emit('audit', dict(event='subscription_sent_not_acknowledgement', source=source,
                                                       connection_id=conn, added=sorted(added), removed=sorted(removed)))
                        subscribed, first = target, False
                        next_control = now + (10 if kind == 'polymarket' else 15)
                        if kind == 'polymarket' and now-last_receive > 45:
                            raise RuntimeError('polymarket_heartbeat_silence')
                    try:
                        msg = await ws.receive(timeout=1)
                    except asyncio.TimeoutError:
                        continue
                    if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        raise RuntimeError('websocket_closed')
                    if msg.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                        continue
                    last_receive = time.monotonic()
                    count += 1
                    journal.wire(source, msg.data, transport='websocket', connection_id=conn,
                                 connection_sequence=count, feed=feed)
                    stats[source + '_frames'] += 1
                    if kind == 'polymarket':
                        market_observations(journal, stats, coverage, msg.data)
                    elif kind == 'yahoo':
                        try:
                            wrapper = json.loads(msg.data)
                            decoded = decode_yahoo(wrapper['message'])
                            symbol = decoded.get('id', 'unknown')
                            coverage[symbol] = {'last_received_ns': time.time_ns(), 'source_time_raw': decoded.get('time')}
                            stats['yahoo_pricing_messages'] += 1
                            journal.emit('underlying_yahoo_decoded', dict(provider='yahoo_unofficial',
                                data_granularity='provider_quote_update_not_exchange_tick',
                                delay_status='unknown_do_not_assume_realtime',
                                connection_id=conn, connection_sequence=count, data=decoded))
                        except (ValueError, KeyError, TypeError, UnicodeError, struct.error):
                            stats['yahoo_decode_errors_raw_retained'] += 1
                    else:
                        for e in events(msg.data):
                            stats['alpaca_' + str(e.get('T', 'unknown'))] += 1
                            if e.get('S'):
                                coverage[e['S']] = {'last_received_ns': time.time_ns(), 'feed': feed, 'source_time_raw': e.get('t')}
                            if e.get('T') == 'error':
                                raise RuntimeError('alpaca_subscription_or_feed_error')
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError, ValueError, KeyError) as exc:
            stats[source + '_disconnects'] += 1
            journal.emit('audit', dict(event='ws_gap', source=source, connection_id=conn,
                                       connected_since_ns=opened_ns, disconnected_at_ns=time.time_ns(),
                                       error_type=type(exc).__name__, detail=str(exc) if isinstance(exc, RuntimeError) else None,
                                       frames_observed=count, missing_messages='unknown_not_recoverable_from_bars'))
        finally:
            journal.emit('audit', dict(event='ws_connection_end', source=source, connection_id=conn,
                                       ended_at_ns=time.time_ns(), frames_observed=count))
        backoff = 1 if count > 10 else min(60, backoff * 2)
        await asyncio.sleep(backoff + random.random())
