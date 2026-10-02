"""Finance-first cursor discovery: no ticker allowlist, no popularity ranking."""
from __future__ import annotations
import asyncio
import os
import time
from urllib.parse import quote
from .core import atomic_json, epoch

GAMMA = 'https://gamma-api.polymarket.com'


def event_page(value):
    if isinstance(value, list):
        return value
    if isinstance(value, dict) and isinstance(value.get('events'), list):
        return value['events']
    raise ValueError('invalid_event_page_shape')


class DiscoveryMixin:
    async def discover(self, closed=False, *, filters=None, name='finance'):
        filters = {'tag_slug': 'finance'} if filters is None else filters
        # Gamma max=100; small pages also bound event objects with many markets.
        params = dict(limit=25, order='id', ascending='false', closed=str(closed).lower(),
            end_date_min=time.strftime('%Y-%m-%dT00:00:00Z', time.gmtime(time.time()-self.args.lookback_days*86400)),
            end_date_max=time.strftime('%Y-%m-%dT23:59:59Z', time.gmtime(time.time()+self.args.lookahead_days*86400)),
            **filters)
        mode, seen, rows_total, complete, reason = 'keyset', set(), 0, False, 'page_limit'
        label = ('closed_recent:' if closed else 'open:') + name
        number = -1
        for number in range(self.args.max_pages):
            url = GAMMA + ('/events/keyset' if mode == 'keyset' else '/events')
            status, value = await self.http.get('discovery_http', url, params,
                                               context={'scan': label, 'page': number})
            # 599 is a LOCAL response-size sentinel, never an upstream status.
            # Restart smaller: cursor shape can be tied to page size by the server.
            if status == 599 and params['limit'] > 1:
                params['limit'] = max(1, params['limit']//5)
                params.pop('after_cursor', None)
                if mode == 'offset':
                    params['offset'] = 0
                seen.clear()
                self.journal.emit('audit', {'event': 'discovery_page_size_reduced',
                                            'scan': label, 'limit': params['limit']})
                continue
            if number == 0 and status in (400, 404, 405, 422):
                mode = 'offset'
                params.pop('after_cursor', None)
                params['offset'] = 0
                status, value = await self.http.get('discovery_http', GAMMA+'/events', params,
                                                    context={'scan': label, 'fallback': True})
            if status != 200:
                reason = 'http_status_' + str(status)
                break
            try:
                rows = event_page(value)
            except ValueError:
                reason = 'invalid_response_shape'
                self.stats['discovery_invalid_response_shape'] += 1
                break
            rows_total += len(rows)
            for event in rows:
                if isinstance(event, dict):
                    for market in event.get('markets') or []:
                        if isinstance(market, dict):
                            self.accept(market, event)
            cursor = value.get('next_cursor') if isinstance(value, dict) else None
            if cursor:
                if cursor in seen:
                    reason = 'repeated_cursor'
                    self.stats['discovery_repeated_cursor'] += 1
                    break
                seen.add(cursor)
                params['after_cursor'] = cursor
            elif mode == 'offset' and len(rows) == params['limit']:
                params['offset'] += len(rows)
            elif isinstance(value, dict) and (value.get('pagination') or {}).get('has_more'):
                reason = 'missing_cursor'
                self.stats['discovery_missing_cursor'] += 1
                break
            else:
                complete, reason = True, 'exhausted'
                break
        self.discovery[label] = dict(completed_at_ns=time.time_ns(), pagination_complete=complete,
            pages=number+1, events_returned=rows_total, mode=mode, reason=reason, filters=filters,
            scope='date-bounded finance/title scan; parser coverage is not guaranteed')
        self.journal.emit('audit', {'event': 'discovery_scan', **self.discovery[label], 'scan': label})
        atomic_json(self.journal.root/'catalog.json', {'family': 'equity_daily', 'records': list(self.catalog.values())})

    async def discovery_loop(self):
        tags = [s.strip() for s in os.environ.get('EQUITY_DISCOVERY_TAGS',
                'finance,finance-updown,stocks,commodities,indices').split(',') if s.strip()]
        if not tags:
            raise ValueError('at_least_one_finance_tag_required')
        # Tag IDs are looked up, never guessed. A missing optional tag is visible.
        valid_tags, crypto_id = {}, None
        for slug in dict.fromkeys(tags+['crypto']):
            status, tag = await self.http.get('discovery_tags', GAMMA+'/tags/slug/'+quote(slug, safe=''))
            if status == 200 and isinstance(tag, dict) and str(tag.get('id', '')).isdigit() and tag.get('slug') == slug:
                if slug == 'crypto':
                    crypto_id = tag['id']
                else:
                    valid_tags[slug] = str(tag['id'])
            else:
                self.journal.emit('audit', {'event': 'discovery_tag_unavailable', 'slug': slug, 'status': status})
        turn = 0
        while True:
            # Open finance first, so historic/sports payloads never delay subscriptions.
            for slug, tag_id in valid_tags.items():
                await self.discover(False, filters={'tag_id': tag_id}, name=slug)
            # Catch new/missing tags without scanning every sports event. Crypto
            # title results are excluded by both server tag and local classifier.
            if turn % 6 == 0 or not self.catalog:
                for phrase in ('close', 'Up or Down'):
                    filters = {'title_search': phrase}
                    if crypto_id:
                        filters['exclude_tag_id'] = str(crypto_id)
                    await self.discover(False, filters=filters, name='title:'+phrase)
                for slug, tag_id in valid_tags.items():
                    await self.discover(True, filters={'tag_id': tag_id}, name=slug)
            for r in list(self.catalog.values()):
                if (epoch(r['end_date_utc']) or 0) < time.time() and not r['closed']:
                    status, m = await self.http.get('metadata_http', GAMMA+'/markets/'+quote(r['market_id'], safe=''))
                    if status == 200 and isinstance(m, dict):
                        self.accept(m, r['raw_event'])
            turn += 1
            await asyncio.sleep(self.args.discovery_seconds)
