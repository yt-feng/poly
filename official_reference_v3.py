"""Read Polymarket's published opening reference from its own event page.

Gamma can omit the live threshold until the window closes. The event page's
server-rendered price query is bound to a symbol, start/end and TWAP window.
Unknown formats fail closed; no JavaScript is executed and no price is inferred.
"""
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
import re

from microstructure_math_v3 import decimal


class EventPage(HTMLParser):
    def __init__(self):
        super().__init__()
        self.canonical = set()
        self.in_script = False
        self.chunks = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'link' and attrs.get('rel') == 'canonical':
            self.canonical.add(attrs.get('href'))
        if tag == 'script':
            self.in_script = True

    def handle_endtag(self, tag):
        if tag == 'script':
            self.in_script = False

    def handle_data(self, data):
        prefix = 'self.__next_f.push('
        if self.in_script and data.startswith(prefix) and data.endswith(')'):
            payload = json.loads(data[len(prefix):-1])
            if isinstance(payload, list) and len(payload) == 2 and payload[0] == 1 and isinstance(payload[1], str):
                self.chunks.append(payload[1])


def parse_event_page(text, market, reference, received_ms):
    slug = market.get('slug', '')
    match = re.fullmatch(r'btc-updown-5m-(\d+)', slug)
    lookback = reference.get('twap_lookback_seconds')
    if not match or not market.get('conditionId') or lookback not in (30, 60):
        return None
    start = int(match[1])
    if not start*1000 <= received_ms < (start+300)*1000:
        return None  # This importer is only for a live window, never a backfill.
    iso = lambda s: datetime.fromtimestamp(s, timezone.utc).isoformat().replace('+00:00', 'Z')
    key = ['crypto-prices', 'price', 'BTC', iso(start), 'fiveminute', iso(start+300), True, lookback]
    url = 'https://polymarket.com/event/'+slug
    page = EventPage()
    page.feed(text)
    if page.canonical != {url}:
        raise ValueError('Official event page identity mismatch')
    payload = ''.join(page.chunks)
    candidates = []
    for match in re.finditer(r'\{"dehydratedAt"', payload):
        try:
            query, _ = json.JSONDecoder().raw_decode(payload[match.start():])
        except ValueError:
            continue
        query_key = query.get('queryKey')
        if query_key != key or query_key[6] is not True:
            continue
        state = query.get('state')
        if not isinstance(state, dict) or not isinstance(state.get('data'), dict):
            continue
        updated = state.get('dataUpdatedAt')
        if (state.get('status') != 'success' or state.get('error') is not None
                or state.get('isInvalidated') or not isinstance(updated, (int, float))
                or not start*1000 <= updated <= received_ms):
            continue
        value = decimal(state['data'].get('openPrice'))
        if value is not None and value > 0:
            candidates.append((value, query))
    if not candidates:
        return None
    if len({value for value, _ in candidates}) != 1:
        raise ValueError('Conflicting official event opening references')
    value, query = candidates[0]
    return {'slug': slug, 'condition_id': market['conditionId'], 'source_url': url,
            'twap_lookback_seconds': lookback, 'published_price_to_beat': str(value),
            'received_ms': received_ms, 'query': query}


def page_reference(market, reference, detail):
    r = dict(reference)
    if (not detail or detail['received_ms'] > r['metadata_received_ms']
            or detail['slug'] != market.get('slug')
            or detail['condition_id'] != market.get('conditionId')
            or detail['twap_lookback_seconds'] != r.get('twap_lookback_seconds')
            or r.get('price_to_beat_conflict')):
        return r
    value = decimal(detail['published_price_to_beat'])
    if value is None or value <= 0:
        return r
    existing = decimal(r.get('published_price_to_beat'))
    if existing is not None and existing != value:
        r.update(published_price_to_beat=None, price_to_beat_path=None,
                 price_to_beat_received_ms=None, price_to_beat_conflict=True)
    elif existing is None:
        r.update(published_price_to_beat=str(value),
                 price_to_beat_path='polymarket.event_page.crypto-prices.openPrice',
                 price_to_beat_source_url=detail['source_url'],
                 price_to_beat_received_ms=detail['received_ms'])
    return r
