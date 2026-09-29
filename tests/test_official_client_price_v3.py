import copy
import json
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, Mock, patch

from capture_v2 import GAMMA, CLOB
from capture_v3 import CollectorV3, OFFICIAL_PAGE_HEADER_LIMIT
from microstructure_v3 import Microstructure
from microstructure_math_v3 import market_reference
from official_reference_v3 import (OFFICIAL_PRICE_URL, opening_price_request,
                                   parse_opening_price_response, client_price_reference)

FIXTURE = json.loads((Path(__file__).parent / 'fixtures/official_opening_price_20260929.json').read_text())


class ClientPriceTests(unittest.TestCase):
    def setUp(self):
        self.market = copy.deepcopy(FIXTURE['live_market'])
        self.payload = copy.deepcopy(FIXTURE['live_response'])
        self.start = int(self.market['slug'].rsplit('-', 1)[1])*1000
        self.requested = self.payload['timestamp']-100
        self.received = self.payload['timestamp']+100
        self.reference = market_reference(self.market, self.received)
        self.request = opening_price_request(self.market, self.reference, self.requested, slug=self.market['slug'])

    def parse(self, payload=None, request=None, received=None):
        return parse_opening_price_response(self.payload if payload is None else payload,
            self.market, self.reference, request or self.request, self.received if received is None else received)

    def test_real_live_response_incomplete_window_has_valid_opening(self):
        self.assertTrue(self.payload['incomplete'])
        self.assertFalse(self.payload['completed'])
        detail = self.parse()
        result = client_price_reference(self.market, self.reference, detail)
        self.assertEqual(result['published_price_to_beat'], '84380.55985779637')
        self.assertEqual(result['price_to_beat_received_ms'], self.received)
        self.assertEqual(result['price_to_beat_path'], 'polymarket.client_api.crypto-price.openPrice')
        self.assertEqual(result['price_to_beat_request'], {'symbol': 'BTC',
            'eventStartTime': '2026-09-29T12:05:00Z', 'endDate': '2026-09-29T12:10:00Z',
            'variant': 'fiveminute', 'twapEnabled': 'true', 'twapLookbackSeconds': '60'})
        self.assertIn('not_response_condition_echo', result['price_to_beat_identity_binding'])

    def test_exact_gamma_identity_dates_and_twap_required(self):
        mutations = [lambda m: m.update(slug='btc-updown-5m-1790683200'),
                     lambda m: m.update(conditionId=''),
                     lambda m: m.update(eventStartTime='2026-09-29T12:00:00Z'),
                     lambda m: m.update(endDate='2026-09-29T12:15:00Z'),
                     lambda m: m.pop('eventStartTime'),
                     lambda m: m['cryptoMarketConfig'].update(asset='eth'),
                     lambda m: m['cryptoMarketConfig'].update(duration='15m'),
                     lambda m: m['cryptoMarketConfig'].update(twapEnabled=False),
                     lambda m: m['cryptoMarketConfig'].update(twapLookbackSeconds=True),
                     lambda m: m['cryptoMarketConfig'].update(twapLookbackSeconds=30)]
        for mutate in mutations:
            m = copy.deepcopy(self.market)
            mutate(m)
            with self.subTest(market=m), self.assertRaises(ValueError):
                opening_price_request(m, self.reference, self.requested, slug=self.market['slug'])
        for at in (self.start-1, self.start+300000):
            with self.assertRaises(ValueError):
                opening_price_request(self.market, self.reference, at, slug=self.market['slug'])

    def test_request_tampering_and_explicit_wrong_response_identity_rejected(self):
        for field in self.request['params']:
            request = copy.deepcopy(self.request)
            request['params'][field] = 'wrong'
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.parse(request=request)
        for field in ('symbol', 'conditionId', 'eventStartTime', 'endDate', 'twapLookbackSeconds'):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.parse(payload={**self.payload, field: 'wrong'})

    def test_unknown_price_or_schema_never_uses_close_or_current_price(self):
        invalid = [{}, [], {'price': 12}, {**self.payload, 'completed': None},
                   {**self.payload, 'incomplete': 'true'}, {**self.payload, 'cached': None}]
        invalid += [{**self.payload, 'openPrice': value, 'closePrice': 123, 'currentPrice': 456}
                    for value in (None, True, False, 0, -1, float('nan'), float('inf'), '84380.5')]
        for payload in invalid:
            with self.subTest(payload=payload):
                self.assertIsNone(self.parse(payload=payload))

    def test_historical_response_or_rollover_never_retroactively_fills_live_rule(self):
        with self.assertRaises(ValueError):
            self.parse(payload=FIXTURE['historical_response'], received=self.start+300000)
        with self.assertRaises(ValueError):
            self.parse(received=self.requested-1)

    def test_response_timestamp_is_typed_current_window_and_not_future(self):
        for stamp in (None, True, 0, 'yesterday', self.start-1, self.received+1):
            with self.subTest(stamp=stamp):
                self.assertIsNone(self.parse(payload={**self.payload, 'timestamp': stamp}))
        for stamp in (self.start, self.requested-1, self.received):
            detail = self.parse(payload={**self.payload, 'timestamp': stamp})
            self.assertEqual(detail['source_timestamp_ms'], stamp)
            result = client_price_reference(self.market, self.reference, detail)
            self.assertEqual(result['price_to_beat_source_timestamp_ms'], stamp)
            self.assertEqual(result['price_to_beat_source_timestamp_kind'],
                             'client_response_timestamp_not_oracle_tick')
        self.assertIsNone(self.parse(payload=FIXTURE['historical_response']))

    def test_full_gamma_and_advertised_twap_cannot_contradict_request(self):
        event = {'slug': self.market['slug'], 'markets': [copy.deepcopy(self.market)],
                 'priceMarketSettings': {'twapEnabled': True, 'twapLookbackSeconds': 60}}
        detail = {'payload': event, 'received_ms': self.requested}
        self.assertEqual(opening_price_request(self.market, self.reference, self.requested,
                         slug=self.market['slug'], event_detail=detail), self.request)
        # Missing optional full-event config is acceptable; a compact exact
        # config remains mandatory. Explicit contradictory fields never are.
        sparse = {'payload': {'slug': self.market['slug'], 'markets': [
            {'slug': self.market['slug'], 'conditionId': self.market['conditionId']}]},
            'received_ms': self.requested}
        self.assertEqual(opening_price_request(self.market, self.reference, self.requested,
                         slug=self.market['slug'], event_detail=sparse), self.request)
        mutations = [lambda e: e.update(slug='wrong'),
                     lambda e: e['markets'][0].update(conditionId='0x'+'a'*64),
                     lambda e: e['markets'][0]['cryptoMarketConfig'].update(twapLookbackSeconds=30),
                     lambda e: e['markets'][0].update(endDate='2026-09-29T12:15:00Z'),
                     lambda e: e.update(resolutionSource='https://data.chain.link/streams/btc-usd-twap-30s-streams'),
                     lambda e: e['priceMarketSettings'].update(twapLookbackSeconds=30),
                     lambda e: e['priceMarketSettings'].update(twapEnabled=False)]
        valid = self.parse()
        for mutate in mutations:
            wrong = copy.deepcopy(detail)
            mutate(wrong['payload'])
            with self.subTest(event=wrong), self.assertRaises(ValueError):
                opening_price_request(self.market, self.reference, self.requested,
                                      slug=self.market['slug'], event_detail=wrong)
            # A previously valid cache must also stop applying as soon as
            # refreshed full Gamma metadata contradicts its bound request.
            self.assertIsNone(client_price_reference(self.market, self.reference, valid,
                              event_detail=wrong)['published_price_to_beat'])
        for source in ('btc-usd-twap-30s-streams', 'eth-usd-twap-60s-streams', 'btc-usd-price'):
            wrong = {**self.market, 'resolutionSource': 'https://data.chain.link/streams/'+source}
            with self.subTest(source=source), self.assertRaises(ValueError):
                opening_price_request(wrong, self.reference, self.requested, slug=self.market['slug'])

    def test_cache_cannot_cross_condition_or_twap_and_conflicts_remain_blocked(self):
        detail = self.parse()
        for key, value in (('condition_id', 'other'), ('twap_lookback_seconds', 30),
                           ('received_ms', self.start+300000), ('received_ms', None),
                           ('source_timestamp_ms', self.start-1), ('source_timestamp_ms', True)):
            self.assertIsNone(client_price_reference(self.market, self.reference, {**detail, key: value})['published_price_to_beat'])
        conflict = client_price_reference(self.market, {**self.reference, 'published_price_to_beat': '90000'}, detail)
        self.assertTrue(conflict['price_to_beat_conflict'])
        self.assertIsNone(conflict['published_price_to_beat'])
        prior = {**self.reference, 'price_to_beat_conflict': True}
        self.assertEqual(client_price_reference(self.market, prior, detail), prior)


class FallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_ssr_and_empty_or_bad_json_retry_bounded_then_cache(self):
        for first in ({}, json.JSONDecodeError('fixture', '', 0)):
            market = copy.deepcopy(FIXTURE['live_market'])
            start = int(market['slug'].rsplit('-', 1)[1])
            collector = Mock(markets={})
            collector.markets = {}
            replies = [first, FIXTURE['live_response']]
            api_calls = []

            async def get(url, params=None):
                if url == GAMMA+'/markets':
                    return [market] if params['slug'] == market['slug'] else []
                if url == GAMMA+'/events/slug/'+market['slug']:
                    return {'slug': market['slug'], 'markets': [market], 'eventMetadata': None}
                if url.startswith('https://polymarket.com/event/'):
                    return ''  # exact production failure: no matching SSR opening
                if url == OFFICIAL_PRICE_URL:
                    api_calls.append(copy.deepcopy(params))
                    value = replies.pop(0)
                    if isinstance(value, Exception):
                        raise value
                    return value
                if url == CLOB+'/markets/'+market['conditionId']:
                    return {'tokens': []}
                raise AssertionError(url)

            collector.get = get
            micro = Microstructure(collector)
            with patch('microstructure_v3.time.time', return_value=start+50):
                await micro.refresh_rules()
            self.assertNotIn(market['slug'], micro.client_price_details)
            self.assertIsNone(micro.rules[market['slug']]['published_price_to_beat'])
            with patch('microstructure_v3.time.time', return_value=start+51):
                await micro.refresh_rules()
            self.assertEqual(len(api_calls), 1)  # no tight retry loop
            with patch('microstructure_v3.time.time', return_value=start+81):
                await micro.refresh_rules()
            self.assertEqual(micro.rules[market['slug']]['published_price_to_beat'], '84380.55985779637')
            with patch('microstructure_v3.time.time', return_value=start+112):
                await micro.refresh_rules()
            self.assertEqual(len(api_calls), 2)  # successful immutable opening reused
            self.assertTrue(any(call.args[0] == 'micro_official_price_response' for call in collector.raw.call_args_list))

    async def test_client_api_http_uses_bounded_headers_and_rejects_200_invalid_json(self):
        for body in ('', '<html>not JSON</html>', json.dumps(FIXTURE['live_response'])):
            response = Mock(status=200, headers={'Date': 'fixture', 'Cache-Control': 'no-cache'})
            response.raise_for_status = Mock()
            response.text = AsyncMock(return_value=body)
            context = AsyncMock()
            context.__aenter__.return_value = response
            collector = object.__new__(CollectorV3)
            collector.session = Mock()
            collector.session.get.return_value = context
            collector.cooldown = {}
            collector.raw = Mock()
            if body.startswith('{'):
                self.assertEqual(await collector.get(OFFICIAL_PRICE_URL, {'symbol': 'BTC'}), FIXTURE['live_response'])
            else:
                with self.assertRaises(json.JSONDecodeError):
                    await collector.get(OFFICIAL_PRICE_URL, {'symbol': 'BTC'})
            kwargs = collector.session.get.call_args.kwargs
            self.assertEqual(kwargs['max_field_size'], OFFICIAL_PAGE_HEADER_LIMIT)
            self.assertEqual(kwargs['timeout'].total, 5)
            self.assertEqual(collector.session.get.call_count, 1)


if __name__ == '__main__':
    unittest.main()
