"""Independent Polymarket equity/index/commodity daily raw-data collector."""
from __future__ import annotations
import argparse
import asyncio
from collections import Counter
import contextlib
import json
import os
from pathlib import Path
import signal
import time
from urllib.parse import quote
import uuid
import zlib
import aiohttp
from .core import Journal, atomic_json, classify, epoch
from .transport import PublicHTTP, stream
from .discovery import DiscoveryMixin

GAMMA = 'https://gamma-api.polymarket.com'
CLOB = 'https://clob.polymarket.com'
DATA = 'https://data-api.polymarket.com'
PM_WS = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
YAHOO_WS = 'wss://streamer.finance.yahoo.com/?version=2'


def page_rows(value, key):
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        data = value.get(key, value.get('data', []))
        if isinstance(data, dict):
            data = data.get(key, [])
        return data if isinstance(data, list) else []
    return []


def cursor_of(value):
    if not isinstance(value, dict):
        return None
    return value.get('next_cursor') or (value.get('pagination') or {}).get('next_cursor')


def data_api_page(value):
    """Accept only the documented v2 envelope; empty pages may continue."""
    if not isinstance(value, dict) or not isinstance(value.get('data'), list):
        raise ValueError('invalid_data_api_envelope')
    if not all(isinstance(row, dict) for row in value['data']):
        raise ValueError('invalid_data_api_rows')
    pagination = value.get('pagination')
    if not isinstance(pagination, dict) or 'next_cursor' not in pagination:
        raise ValueError('missing_data_api_cursor')
    cursor = pagination['next_cursor']
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ValueError('invalid_data_api_cursor')
    if type(pagination.get('has_more')) is not bool or pagination['has_more'] != (cursor is not None):
        raise ValueError('inconsistent_data_api_pagination')
    return value['data'], cursor


class Collector(DiscoveryMixin):
    def __init__(self, journal, args):
        self.journal, self.args = journal, args
        self.stats, self.catalog, self.versions = Counter(), {}, {}
        self.pm_coverage, self.yahoo_coverage, self.alpaca_coverage = {}, {}, {}
        self.discovery, self.symbol_status = {}, {}
        self.stop, self.resnapshot = asyncio.Event(), asyncio.Event()
        self.http = self.session = None
        self.started_ns = time.time_ns()
        self.alpaca_feed = os.environ.get('EQUITY_ALPACA_FEED', 'iex')
        if self.alpaca_feed not in ('iex', 'sip', 'delayed_sip'):
            raise ValueError('EQUITY_ALPACA_FEED must be iex, sip, or delayed_sip')

    def live_catalog(self):
        now = time.time()
        return [r for r in self.catalog.values() if not r['closed'] and (epoch(r['end_date_utc']) or 0) >= now-86400]

    def token_ids(self, shard=None):
        result = {o['token_id'] for r in self.live_catalog() for o in r['outcomes']}
        if shard is not None:
            result = {t for t in result if zlib.crc32(t.encode()) % self.args.shards == shard}
        return result

    def symbols(self):
        return {r['yahoo_symbol'] for r in self.catalog.values() if r['yahoo_symbol']}

    def alpaca_symbols(self):
        # Indices, commodity futures and unvalidated equities must not reach a US stock feed.
        choices = {s for s, info in self.symbol_status.items()
                   if info.get('provider_symbol_validated') and info.get('instrument_type') in ('EQUITY', 'ETF')
                   and info.get('exchange_timezone') == 'America/New_York'}
        explicit = os.environ.get('EQUITY_ALPACA_SYMBOLS', '')
        return choices & set(explicit.split(',')) if explicit else choices

    def health(self):
        tokens = self.token_ids()
        return dict(started_at_ns=self.started_ns, observed_at_ns=time.time_ns(),
                    market_count=len(self.catalog), live_markets=len(self.live_catalog()),
                    desired_token_count=len(tokens), tokens_with_ws_events=len(tokens & self.pm_coverage.keys()),
                    missing_ws_tokens=sorted(tokens-self.pm_coverage.keys()),
                    symbols=sorted(self.symbols()), unresolved_underlyings=[r['market_id'] for r in self.catalog.values() if not r['yahoo_symbol']],
                    stats=dict(self.stats), discovery=self.discovery, symbol_status=self.symbol_status,
                    polymarket_token_coverage=self.pm_coverage, yahoo_symbol_coverage=self.yahoo_coverage,
                    alpaca_symbol_coverage=self.alpaca_coverage,
                    alpaca=dict(configured=bool(os.environ.get('APCA_API_KEY_ID') and os.environ.get('APCA_API_SECRET_KEY')),
                                feed=self.alpaca_feed, subscribed_candidates=sorted(self.alpaca_symbols())),
                    granularities=dict(polymarket='observed_native_ws_messages', yahoo='provider_quote_updates_and_1m_bars_not_exchange_ticks',
                                       alpaca='entitled_feed_trade_quote_messages_when_configured'),
                    source_delay_must_be_checked=True, continuity_guaranteed=False,
                    live_trading_enabled=False, strategy_evaluated=False)

    def accept(self, market, event):
        r, reason = classify(market, event)
        self.stats['classify_' + reason] += 1
        if r is None:
            # Candidate diagnostics keep coverage misses visible without trading unrelated events.
            if reason in ('daily_date_unconfirmed', 'end_date_missing', 'invalid_binary_outcome_mapping'):
                self.journal.emit('candidates', dict(reason=reason, market_id=market.get('id'),
                                                   question=market.get('question'), event_title=event.get('title')))
            return
        end = epoch(r['end_date_utc'])
        if not time.time()-self.args.lookback_days*86400 <= end <= time.time()+self.args.lookahead_days*86400:
            return
        from hashlib import sha256
        version = sha256(json.dumps(r, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        key = r['market_id'] or r['condition_id']
        if not key or not r['condition_id']:
            self.stats['missing_market_identity'] += 1
            return
        self.catalog[key] = r
        if self.versions.get(key) != version:
            self.versions[key] = version
            self.journal.emit('catalog', {'catalog_version': version, 'record': r})

    async def books_loop(self):
        while True:
            ids = sorted(self.token_ids())
            semaphore = asyncio.Semaphore(6)
            async def fetch(token):
                async with semaphore:
                    status, value = await self.http.get('book_http', CLOB+'/book', {'token_id': token})
                    if status == 200 and isinstance(value, dict) and 'bids' in value:
                        self.stats['rest_books'] += 1
            await asyncio.gather(*(fetch(t) for t in ids))
            self.resnapshot.clear()
            try:
                await asyncio.wait_for(self.resnapshot.wait(), self.args.book_seconds)
                await asyncio.sleep(2)
            except asyncio.TimeoutError:
                pass

    async def yahoo_loop(self):
        initial, daily_at, retry_at = set(), {}, {}
        while True:
            semaphore = asyncio.Semaphore(4)
            async def fetch(symbol):
                if time.time() < retry_at.get(symbol, 0):
                    return
                async with semaphore:
                    url = 'https://query1.finance.yahoo.com/v8/finance/chart/' + quote(symbol, safe='')
                    params = {'interval': '1m', 'range': '5d' if symbol not in initial else '1d',
                              'includePrePost': 'true', 'events': 'div,splits,capitalGains'}
                    status, value = await self.http.get('underlying_yahoo_chart', url, params,
                        context={'symbol': symbol, 'adjustment': 'raw_OHLC_preserve_adjclose_separately',
                                 'granularity': '1m', 'delay': 'provider_dependent_unknown'})
                    chart = (value or {}).get('chart', {}) if isinstance(value, dict) else {}
                    result = chart.get('result') or []
                    if status != 200 or not result:
                        retry_at[symbol] = time.time() + (900 if status in (401, 403, 404, 429) else 120)
                        self.symbol_status[symbol] = {'state': 'unavailable', 'http_status': status,
                                                     'checked_at_ns': time.time_ns()}
                        return
                    meta = result[0].get('meta', {})
                    self.symbol_status[symbol] = dict(state='chart_received' if result[0].get('timestamp') else 'metadata_only_no_bars',
                        observed_bar_count=len(result[0].get('timestamp') or []), checked_at_ns=time.time_ns(),
                        provider_symbol=meta.get('symbol'), provider_symbol_validated=str(meta.get('symbol','')).upper()==symbol.upper(),
                        instrument_type=meta.get('instrumentType'), exchange_timezone=meta.get('exchangeTimezoneName'),
                        exchange=meta.get('exchangeName'), data_granularity=meta.get('dataGranularity'),
                        exchange_data_delayed_by=meta.get('exchangeDataDelayedBy'),
                        latest_source_timestamp=(result[0].get('timestamp') or [None])[-1])
                    self.stats['yahoo_chart_success'] += 1
                    if result[0].get('timestamp'):
                        self.stats['yahoo_nonempty_chart_success'] += 1
                    initial.add(symbol)
                    if time.time()-daily_at.get(symbol, 0) >= 3600:
                        daily_status, daily = await self.http.get('underlying_yahoo_daily', url,
                            {'interval': '1d', 'range': '3mo', 'includePrePost': 'true', 'events': 'div,splits,capitalGains'},
                            context={'symbol': symbol, 'granularity': '1d', 'purpose': 'unadjusted_reference_and_corporate_actions'})
                        if daily_status == 200:
                            daily_at[symbol] = time.time()
            await asyncio.gather(*(fetch(s) for s in sorted(self.symbols())))
            await asyncio.sleep(self.args.underlying_seconds)

    async def trades(self, r, start):
        end = int(time.time())
        # Condition feeds ignore start/end and cover three years. Filter locally;
        # cursor exhaustion alone cannot prove a window older than that retention.
        params = {'condition': r['condition_id'], 'limit': 1000}
        seen, selected, pagination_complete, reason = set(), 0, False, 'page_limit'
        for number in range(100):
            status, value = await self.http.get('trades_http', DATA+'/v2/trades', params,
                context={'market_id': r['market_id'], 'pagination_page': number,
                         'deduplication': 'raw_overlap_intentional',
                         'client_window_start': start, 'client_window_end': end})
            if status != 200:
                reason = 'http_status_' + str(status)
                break
            try:
                rows, cursor = data_api_page(value)
            except ValueError as exc:
                reason = str(exc)
                break
            window_rows = []
            for row in rows:
                timestamp = row.get('timestamp')
                # REST timestamps are epoch seconds. Never turn unknown/malformed
                # timestamps into zero or infer coverage from SDK millisecond fields.
                if type(timestamp) is not int or not 0 <= timestamp < 100_000_000_000:
                    reason = 'invalid_trade_timestamp'
                    break
                if start <= timestamp <= end:
                    window_rows.append(row)
            if reason == 'invalid_trade_timestamp':
                break
            if window_rows:
                self.journal.emit('trade_window_rows', dict(market_id=r['market_id'],
                    start=start, end=end, rows=window_rows, pagination_page=number))
                selected += len(window_rows)
            if cursor is not None:
                if cursor in seen:
                    reason = 'repeated_cursor'
                    break
                seen.add(cursor)
                params['cursor'] = cursor
            else:
                pagination_complete, reason = True, 'cursor_exhausted'
                break
        # 1095 days is a conservative subset of a calendar three-year window.
        retained_window = end-1095*86400 <= start <= end
        complete = pagination_complete and retained_window
        if pagination_complete and not retained_window:
            reason = 'outside_documented_retention'
        self.journal.emit('audit', dict(event='trade_backfill_window', market_id=r['market_id'],
            start=start, end=end, api_mode='v2', pagination_complete=pagination_complete,
            window_complete=complete, reason=reason, selected_rows=selected,
            server_window='fixed_three_years', time_filter='client_inclusive',
            all_time_complete=False, pages=number+1))
        self.stats['trade_backfill_complete_windows' if complete else 'trade_backfill_truncated'] += 1
        return complete

    async def history(self, token):
        # Windowed backfill is paginated, never confused with native WS ticks.
        end = int(time.time())
        params = {'token_id': token, 'start': end-86400, 'end': end, 'bucket_seconds': 60}
        seen, points, complete, reason = set(), 0, False, 'page_limit'
        for page in range(100):
            status, value = await self.http.get('history_http', DATA+'/v2/prices-history', params,
                context={'requested_bucket_seconds': 60, 'reconstructs_orderbook': False,
                         'purpose': 'sampled_backfill_not_native_ticks', 'page': page})
            if status != 200:
                reason = 'http_status_' + str(status)
                break
            try:
                rows, cursor = data_api_page(value)
            except ValueError as exc:
                reason = str(exc)
                break
            if any(type(row.get('timestamp')) is not int
                   or not 0 <= row['timestamp'] < 100_000_000_000
                   or type(row.get('price')) not in (int, float)
                   or not 0 <= row['price'] <= 1
                   or type(row.get('resolution_seconds')) is not int
                   or row['resolution_seconds'] < 0 for row in rows):
                reason = 'invalid_history_point'
                break
            points += len(rows)
            if cursor is not None:
                if cursor in seen:
                    reason = 'repeated_cursor'
                    break
                seen.add(cursor)
                params['cursor'] = cursor
            else:
                complete, reason = True, 'cursor_exhausted'
                break
        self.journal.emit('audit', dict(event='history_backfill_window', token_id=token,
            pagination_complete=complete, points_returned=points, pages=page+1,
            requested_window='1d', start=params['start'], end=end, reason=reason,
            requested_bucket_seconds=60, all_time_complete=False))
        self.stats['history_complete_windows' if complete else 'history_incomplete_windows'] += 1
        return complete

    async def analytics_loop(self):
        watermark, history_at, resolution_at, details_at = {}, {}, {}, {}
        while True:
            # Today's open events first; do not let newly listed future contracts
            # postpone trade/history capture for the session currently trading.
            ordered = sorted(self.catalog.values(), key=lambda r: (bool(r['closed']), abs((epoch(r['end_date_utc']) or 0)-time.time())))
            for r in ordered:
                mid, now = r['market_id'], int(time.time())
                due = 3600 if r['closed'] else 120
                if now-watermark.get(mid, 0) >= due:
                    start = max(now-self.args.lookback_days*86400, watermark.get(mid, now-86400)-120)
                    if await self.trades(r, start):
                        watermark[mid] = now
                if now-resolution_at.get(mid, 0) >= 600:
                    for endpoint in ('oi', 'resolutions'):
                        await self.http.get('analytics_http', DATA+'/v2/'+endpoint, {'condition': r['condition_id']},
                                            context={'market_id': mid, 'endpoint': endpoint})
                    resolution_at[mid] = now
                if now-details_at.get(mid, 0) >= 1800:
                    await self.http.get('clob_metadata_http', CLOB+'/markets/'+r['condition_id'],
                                        context={'market_id': mid, 'purpose': 'fees_ticks_min_sizes_neg_risk_token_winners'})
                    details_at[mid] = now
                for outcome in r['outcomes']:
                    token = outcome['token_id']
                    if now-history_at.get(token, 0) >= 1800 and await self.history(token):
                        history_at[token] = now
            await asyncio.sleep(30)

    async def alpaca_rest_loop(self):
        headers = {'APCA-API-KEY-ID': os.environ['APCA_API_KEY_ID'], 'APCA-API-SECRET-KEY': os.environ['APCA_API_SECRET_KEY']}
        while True:
            symbols = sorted(self.alpaca_symbols())
            for i in range(0, len(symbols), 100):
                await self.http.get('underlying_alpaca_snapshot', 'https://data.alpaca.markets/v2/stocks/snapshots',
                    {'symbols': ','.join(symbols[i:i+100]), 'feed': self.alpaca_feed}, headers=headers,
                    context={'feed': self.alpaca_feed, 'coverage': 'IEX_only' if self.alpaca_feed=='iex' else self.alpaca_feed})
            await asyncio.sleep(60)

    async def checkpoint_loop(self):
        await asyncio.sleep(min(45, self.args.checkpoint_seconds))
        while True:
            self.journal.check_disk()
            self.journal.checkpoint(self.health())
            if self.args.release:
                from .github_archive import publish_ready
                try:
                    await asyncio.to_thread(publish_ready, self.journal.root, self.args.release)
                    self.stats['publication_success'] += 1
                except (RuntimeError, OSError) as exc:
                    self.stats['publication_failure'] += 1
                    self.journal.emit('audit', {'event': 'publication_error', 'error_type': type(exc).__name__})
            print(json.dumps({'market_count': len(self.catalog), 'tokens': len(self.token_ids()),
                              'ws_frames': self.stats.get('polymarket_ws_frames', 0),
                              'yahoo_messages': self.stats.get('yahoo_pricing_messages', 0),
                              'yahoo_charts': self.stats.get('yahoo_chart_success', 0),
                              'publication_success': self.stats.get('publication_success', 0)}), flush=True)
            await asyncio.sleep(self.args.checkpoint_seconds)

    async def run(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.stop.set)
        connector = aiohttp.TCPConnector(limit=64, limit_per_host=24, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector,
                                        headers={'User-Agent': 'poly-equity-daily/1.0 public-research-collector'}) as session:
            self.session = session
            self.http = PublicHTTP(session, self.journal, self.stats)
            jobs = [self.discovery_loop(), self.books_loop(), self.analytics_loop(), self.checkpoint_loop()]
            for shard in range(self.args.shards):
                jobs.append(stream(session, self.journal, self.stats, 'polymarket_ws', PM_WS,
                    lambda s=shard: self.token_ids(s), kind='polymarket', coverage=self.pm_coverage, resnapshot=self.resnapshot))
            if not self.args.no_yahoo:
                jobs += [self.yahoo_loop(), stream(session, self.journal, self.stats, 'underlying_yahoo_ws',
                    YAHOO_WS, self.symbols, kind='yahoo', coverage=self.yahoo_coverage)]
            if os.environ.get('APCA_API_KEY_ID') and os.environ.get('APCA_API_SECRET_KEY'):
                # Licensed feeds cannot be publicly published without encryption or an explicit rights assertion.
                if self.args.release and not self.journal.fernet and os.environ.get('EQUITY_ALPACA_PUBLIC_REDISTRIBUTION') != 'true':
                    self.journal.emit('audit', {'event': 'alpaca_blocked_archive_encryption_or_redistribution_rights_required'})
                    self.stats['alpaca_archive_rights_blocked'] += 1
                else:
                    jobs += [self.alpaca_rest_loop(), stream(session, self.journal, self.stats, 'underlying_alpaca_ws',
                        'wss://stream.data.alpaca.markets/v2/'+self.alpaca_feed, self.alpaca_symbols,
                        kind='alpaca', coverage=self.alpaca_coverage, feed=self.alpaca_feed)]
            tasks = [asyncio.create_task(c) for c in jobs]
            stop_task = asyncio.create_task(self.stop.wait())
            fatal = None
            try:
                done, _ = await asyncio.wait(tasks+[stop_task], timeout=self.args.seconds or None,
                                             return_when=asyncio.FIRST_COMPLETED)
                for t in done:
                    if t is not stop_task:
                        fatal = t.exception() or RuntimeError('collector_background_task_exited')
                        self.journal.emit('audit', {'event': 'fatal_task_exit', 'error_type': type(fatal).__name__})
            finally:
                for t in tasks+[stop_task]:
                    t.cancel()
                await asyncio.gather(*tasks, stop_task, return_exceptions=True)
                self.journal.emit('audit', {'event': 'run_end', 'inter_run_gap_not_reconstructed': True})
                self.journal.checkpoint(self.health())
            if self.args.release:
                from .github_archive import publish_ready
                await asyncio.to_thread(publish_ready, self.journal.root, self.args.release)
            if fatal:
                raise fatal
        if not self.catalog:
            raise RuntimeError('no_matching_markets_discovered_see_health_and_discovery_raw')
        if not self.pm_coverage:
            raise RuntimeError('no_polymarket_market_messages_observed_see_health')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('equity_daily_output'))
    parser.add_argument('--seconds', type=int, default=14400, help='0 = long-running service; Actions always uses a bounded duration')
    parser.add_argument('--release', help='GitHub release tag; must begin equity-daily-v1-')
    parser.add_argument('--max-pages', type=int, default=100)
    parser.add_argument('--lookback-days', type=int, default=7)
    parser.add_argument('--lookahead-days', type=int, default=8)
    parser.add_argument('--shards', type=int, default=8)
    parser.add_argument('--discovery-seconds', type=int, default=300)
    parser.add_argument('--book-seconds', type=int, default=60)
    parser.add_argument('--underlying-seconds', type=int, default=60)
    parser.add_argument('--checkpoint-seconds', type=int, default=120)
    parser.add_argument('--no-yahoo', action='store_true')
    args = parser.parse_args()
    if args.seconds < 0 or min(args.max_pages, args.shards, args.discovery_seconds, args.book_seconds,
                               args.underlying_seconds, args.checkpoint_seconds) <= 0:
        parser.error('invalid nonpositive limits')
    if args.release and not args.release.startswith('equity-daily-v1-'):
        parser.error('release must use the isolated equity-daily-v1- namespace')
    run_id = os.environ.get('GITHUB_RUN_ID', uuid.uuid4().hex) + '-' + os.environ.get('GITHUB_RUN_ATTEMPT', '1')
    journal = Journal(args.output, run_id)
    asyncio.run(Collector(journal, args).run())


if __name__ == '__main__':
    main()
