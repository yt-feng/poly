"""Read Polymarket's published opening reference from its page and client API.

Gamma and page hydration may omit the live threshold. The official client's
JSON request is bound to exact Gamma identity, start/end and TWAP settings.
Unknown formats fail closed; no JavaScript is executed and no price is inferred.
"""
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
import re
from urllib.parse import urlsplit

from microstructure_math_v3 import decimal, reference_conflict_evidence

OFFICIAL_PRICE_URL = 'https://polymarket.com/api/crypto/crypto-price'


def _matching_settings(record, lookback, start, end):
    """Reject explicit contradictory metadata without inventing missing fields."""
    for name in ('cryptoMarketConfig', 'priceMarketSettings'):
        settings = record.get(name)
        if settings is None:
            continue
        if not isinstance(settings, dict):
            raise ValueError('Official opening metadata settings shape mismatch')
        expected = {'asset': 'btc', 'duration': '5m', 'twapEnabled': True,
                    'twapLookbackSeconds': lookback}
        for key, value in expected.items():
            if key in settings and (type(settings[key]) is not type(value) or settings[key] != value):
                raise ValueError('Official opening metadata TWAP conflict')
    source = record.get('resolutionSource')
    if source is not None:
        if not isinstance(source, str):
            raise ValueError('Official opening metadata resolution source shape mismatch')
        parsed = urlsplit(source)
        stream = re.fullmatch(r'/streams/([a-z0-9]+)-usd-(twap-\d+s-streams|price)/?', parsed.path.lower())
        if (parsed.hostname == 'data.chain.link' and stream
                and (stream[1] != 'btc' or stream[2] == 'price')):
            raise ValueError('Official opening metadata resolution asset or price type conflict')
        advertised = re.findall(r'twap-(\d+)s', source.lower())
        if any(int(value) != lookback for value in advertised):
            raise ValueError('Official opening metadata resolution TWAP conflict')
    for field, expected in (('eventStartTime', start), ('startTime', start), ('endDate', end)):
        if field not in record:
            continue
        value = record[field]
        try:
            stamp = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else None
            valid = stamp is not None and stamp.tzinfo is not None and stamp.timestamp()*1000 == expected
        except (ValueError, OverflowError):
            valid = False
        if not valid:
            raise ValueError('Official opening metadata window conflict')


def opening_price_request(market, reference, requested_ms, *, slug, event_detail=None):
    """Bind the official client's request to exact live Gamma market metadata."""
    match = re.fullmatch(r'btc-updown-5m-(\d+)', slug)
    condition = market.get('conditionId')
    config = market.get('cryptoMarketConfig')
    if (not match or market.get('slug') != slug or reference.get('slug') != slug
            or not isinstance(condition, str) or not re.fullmatch(r'0x[0-9a-fA-F]{64}', condition)
            or reference.get('condition_id') != condition or not isinstance(config, dict)):
        raise ValueError('Official opening request market identity mismatch')
    start = int(match[1]) * 1000
    end = start + 300000
    lookback = config.get('twapLookbackSeconds')
    if (type(requested_ms) is not int or start % 300000 or not start <= requested_ms < end
            or config.get('asset') != 'btc' or config.get('duration') != '5m'
            or config.get('twapEnabled') is not True or type(lookback) is not int
            or lookback not in (30, 60) or reference.get('twap_lookback_seconds') != lookback
            or reference.get('reference_kind') != f'chainlink_twap_{lookback}'):
        raise ValueError('Official opening request window or TWAP mismatch')
    if any(field not in market for field in ('eventStartTime', 'endDate')):
        raise ValueError('Official opening request requires exact market dates')
    _matching_settings(market, lookback, start, end)
    if event_detail is not None:
        if (not isinstance(event_detail, dict) or type(event_detail.get('received_ms')) is not int
                or event_detail['received_ms'] > reference['metadata_received_ms']):
            raise ValueError('Official opening full event receipt mismatch')
        event = event_detail.get('payload')
        if not isinstance(event, dict) or event.get('slug') != slug or not isinstance(event.get('markets'), list):
            raise ValueError('Official opening full event identity mismatch')
        matches = [m for m in event['markets'] if isinstance(m, dict) and m.get('slug') == slug]
        if not matches or any(m.get('conditionId') != condition for m in matches):
            raise ValueError('Official opening full event condition mismatch')
        for record in [event, *matches]:
            _matching_settings(record, lookback, start, end)
    return {'slug': slug, 'condition_id': condition, 'source_url': OFFICIAL_PRICE_URL,
            'requested_ms': requested_ms, 'window_start_ms': start, 'window_end_ms': end,
            'twap_lookback_seconds': lookback,
            'identity_binding': 'gamma_market_and_exact_request_not_response_condition_echo',
            'params': {'symbol': 'BTC', 'eventStartTime': market['eventStartTime'],
                       'variant': 'fiveminute', 'endDate': market['endDate'],
                       'twapEnabled': 'true', 'twapLookbackSeconds': str(lookback)}}


def parse_opening_price_response(payload, market, reference, request, received_ms, *, event_detail=None):
    expected = opening_price_request(market, reference, request['requested_ms'],
                                     slug=request['slug'], event_detail=event_detail)
    if (request != expected or type(received_ms) is not int
            or not request['requested_ms'] <= received_ms < request['window_end_ms']):
        raise ValueError('Official opening response request identity or receipt window mismatch')
    # The real live response has completed=false, incomplete=true and a valid
    # openPrice while closePrice is null. These flags describe the whole window,
    # not whether its opening reference is known. Never substitute close/current.
    if (not isinstance(payload, dict) or type(payload.get('completed')) is not bool
            or type(payload.get('incomplete')) is not bool or type(payload.get('cached')) is not bool):
        return None
    stamp = payload.get('timestamp')
    if type(stamp) is not int or not request['window_start_ms'] <= stamp <= received_ms:
        return None
    # Normally the endpoint echoes no identity. If it ever does, a conflicting
    # identity is still a rejection, not metadata to silently ignore.
    echoed = {**request['params'], 'twapEnabled': True,
              'twapLookbackSeconds': request['twap_lookback_seconds'],
              'conditionId': request['condition_id'], 'slug': request['slug']}
    if any(key in payload and (type(payload[key]) is not type(value) or payload[key] != value)
           for key, value in echoed.items()):
        raise ValueError('Official opening response echoed identity mismatch')
    value = payload.get('openPrice')
    if type(value) not in (int, float):
        return None
    value = decimal(value)
    if value is None or value <= 0:
        return None
    return {**request, 'received_ms': received_ms, 'source_timestamp_ms': stamp,
            'published_price_to_beat': str(value),
            'response_completed': payload['completed'], 'response_incomplete': payload['incomplete'],
            'response_cached': payload['cached']}


def client_price_reference(market, reference, detail, *, event_detail=None):
    if not detail:
        return dict(reference)
    try:
        expected = opening_price_request(market, reference, detail['requested_ms'],
                                         slug=detail['slug'], event_detail=event_detail)
    except (ValueError, KeyError, TypeError):
        return dict(reference)
    if any(detail.get(key) != value for key, value in expected.items()):
        return dict(reference)
    if (type(detail.get('received_ms')) is not int or type(detail.get('source_timestamp_ms')) is not int
            or not expected['requested_ms'] <= detail['received_ms'] < expected['window_end_ms']
            or not expected['window_start_ms'] <= detail['source_timestamp_ms'] <= detail['received_ms']):
        return dict(reference)
    result = page_reference(market, reference, detail)
    if (reference.get('published_price_to_beat') is None and not result.get('price_to_beat_conflict')
            and result.get('published_price_to_beat') is not None):
        result.update(price_to_beat_path='polymarket.client_api.crypto-price.openPrice',
                      price_to_beat_request=dict(expected['params']),
                      price_to_beat_identity_binding=expected['identity_binding'],
                      price_to_beat_source_timestamp_ms=detail['source_timestamp_ms'],
                      price_to_beat_source_timestamp_kind='client_response_timestamp_not_oracle_tick')
    return result


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


def page_objects(chunks):
    """Decode framed RSC JSON, independent of keys and script chunk boundaries.

    Text/binary frames have byte lengths and may contain fake JSON or newlines;
    skip those exactly rather than searching strings for a price-shaped object.
    """
    data = ''.join(chunks).encode('utf-8')
    position = 0
    while position < len(data):
        if data[position:position+1] == b'\n':
            position += 1
            continue
        header = re.match(rb'[0-9a-f]*:', data[position:])
        if not header:
            raise ValueError('Unrecognized official page stream framing')
        position += header.end()
        sized = re.match(rb'[A-Za-z]([0-9a-f]+),', data[position:])
        if sized:
            position += sized.end()+int(sized[1], 16)
            if position > len(data):
                raise ValueError('Truncated official page stream frame')
            continue
        end = data.find(b'\n', position)
        if end < 0:
            end = len(data)
        record = data[position:end]
        position = end+1
        if record[:1] not in (b'[', b'{'):
            continue  # Module/preload/control frames are not hydration state.
        stack = [json.loads(record)]
        while stack:
            value = stack.pop()
            if isinstance(value, list):
                stack.extend(value)
            elif isinstance(value, dict):
                yield value
                stack.extend(value.values())


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
    objects = list(page_objects(page.chunks))
    conditions = {obj['conditionId'] for obj in objects
                  if obj.get('slug') == slug and isinstance(obj.get('conditionId'), str)}
    if conditions != {market['conditionId']}:
        raise ValueError('Official event page condition identity mismatch')
    candidates = []
    for query in objects:
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
    return {'slug': slug, 'condition_id': next(iter(conditions)), 'source_url': url,
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
        path = ('polymarket.client_api.crypto-price.openPrice'
                if detail.get('source_url') == OFFICIAL_PRICE_URL
                else 'polymarket.event_page.crypto-prices.openPrice')
        evidence = reference_conflict_evidence(r, value, path, detail['received_ms'])
        r.update(published_price_to_beat=None, price_to_beat_path=None,
                 price_to_beat_received_ms=None, price_to_beat_conflict=True,
                 price_to_beat_conflict_evidence=evidence)
    elif existing is None:
        r.update(published_price_to_beat=str(value),
                 price_to_beat_path='polymarket.event_page.crypto-prices.openPrice',
                 price_to_beat_source_url=detail['source_url'],
                 price_to_beat_received_ms=detail['received_ms'])
    return r
