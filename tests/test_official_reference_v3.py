import copy
import json
import unittest

from microstructure_math_v3 import market_reference
from official_reference_v3 import parse_event_page, page_reference


class OfficialReferenceTests(unittest.TestCase):
    start = 1790528700000
    market = {'slug': 'btc-updown-5m-1790528700', 'conditionId': 'test-condition',
              'resolutionSource': 'https://data.chain.link/streams/btc-usd-twap-60s-streams'}

    def setUp(self):
        self.reference = market_reference(self.market, self.start+20000)
        self.query = {'dehydratedAt': self.start+19000,
                      'queryKey': ['crypto-prices', 'price', 'BTC', '2026-09-27T17:05:00Z',
                                   'fiveminute', '2026-09-27T17:10:00Z', True, 60],
                      'state': {'status': 'success', 'error': None, 'dataUpdatedAt': self.start+10000,
                                'data': {'openPrice': 84447.94584493896, 'closePrice': None}}}

    def html(self, *queries, slug=None, condition='test-condition', reorder=False, split=False,
             prefix=''):
        # Exact server-rendered query shape, with synthetic fixture identity.
        slug = slug or self.market['slug']
        data = prefix+'1:'+json.dumps({'market': {'slug': slug, 'conditionId': condition},
                                     'state': {'queries': list(queries or [self.query])}}, sort_keys=reorder)
        chunks = [data[:len(data)//2], data[len(data)//2:]] if split else [data]
        return ('<link rel="canonical" href="https://polymarket.com/event/'+slug+'">'
                +''.join('<script>self.__next_f.push('+json.dumps([1, chunk])+')</script>' for chunk in chunks))

    def parse(self, html=None, received=None):
        return parse_event_page(html or self.html(), self.market, self.reference, received or self.start+20000)

    def test_live_official_page_preserves_identity_and_availability(self):
        detail = self.parse()
        result = page_reference(self.market, self.reference, detail)
        self.assertEqual(result['published_price_to_beat'], '84447.94584493896')
        self.assertEqual(result['price_to_beat_received_ms'], self.start+20000)
        self.assertEqual(detail['query']['queryKey'], self.query['queryKey'])

    def test_field_order_and_script_chunk_boundaries_do_not_change_value(self):
        expected = self.parse()['published_price_to_beat']
        for reorder, split in ((True, False), (False, True), (True, True)):
            self.assertEqual(self.parse(self.html(reorder=reorder, split=split))['published_price_to_beat'], expected)

    def test_page_condition_must_independently_match_gamma(self):
        for condition in (None, 'other-condition'):
            with self.assertRaisesRegex(ValueError, 'condition identity'):
                self.parse(self.html(condition=condition))

    def test_text_frames_cannot_inject_queries_and_unicode_lengths_are_bytes(self):
        fake = copy.deepcopy(self.query)
        fake['state']['data']['openPrice'] = 90000
        text = '非结构化文本\n3:'+json.dumps({'state': {'queries': [fake]}})
        frame = '0:T'+format(len(text.encode()), 'x')+','+text
        self.assertEqual(self.parse(self.html(prefix=frame, split=True))['published_price_to_beat'], '84447.94584493896')

    def test_truncated_stream_fails_closed(self):
        with self.assertRaises(ValueError):
            self.parse(self.html(prefix='0:Tffffff,incomplete'))

    def test_other_symbol_window_and_twap_never_match(self):
        for index, wrong in [(2, 'ETH'), (3, '2026-09-27T17:00:00Z'), (4, 'fifteenminute'),
                             (5, '2026-09-27T17:15:00Z'), (6, False), (7, 30)]:
            q = copy.deepcopy(self.query)
            q['queryKey'][index] = wrong
            self.assertIsNone(self.parse(self.html(q)))

    def test_wrong_page_identity_fails_closed(self):
        with self.assertRaises(ValueError):
            self.parse(self.html(slug='btc-updown-5m-1790528400'))

    def test_missing_or_invalid_official_price_stays_unknown(self):
        for value in (None, 0, -1, 'NaN', 'Infinity'):
            q = copy.deepcopy(self.query)
            q['state']['data']['openPrice'] = value
            self.assertIsNone(self.parse(self.html(q)))
        self.assertIsNone(self.parse(self.html({'dehydratedAt': 1})))

    def test_future_or_pre_window_cache_is_rejected(self):
        for updated in (self.start-1, self.start+30000):
            q = copy.deepcopy(self.query)
            q['state']['dataUpdatedAt'] = updated
            self.assertIsNone(self.parse(self.html(q)))

    def test_unrecognized_query_payload_remains_unknown(self):
        for state in (None, [], {'data': []}, {'data': 'bad'}):
            q = {**self.query, 'state': state}
            self.assertIsNone(self.parse(self.html(q)))

    def test_rollover_or_future_window_is_not_retroactively_filled(self):
        self.assertIsNone(self.parse(received=self.start-1))
        self.assertIsNone(self.parse(received=self.start+300000))

    def test_duplicate_hydration_is_idempotent_but_conflict_rejected(self):
        self.assertEqual(self.parse(self.html(self.query, self.query))['published_price_to_beat'], '84447.94584493896')
        other = copy.deepcopy(self.query)
        other['state']['data']['openPrice'] = 90000
        with self.assertRaises(ValueError):
            self.parse(self.html(self.query, other))

    def test_conflicting_gamma_reference_invalidates_price(self):
        result = page_reference(self.market, {**self.reference, 'published_price_to_beat': '90000'}, self.parse())
        self.assertTrue(result['price_to_beat_conflict'])
        self.assertIsNone(result['published_price_to_beat'])

    def test_other_condition_and_future_receipt_cannot_enter_rule(self):
        detail = self.parse()
        for field, value in [('condition_id', 'other'), ('received_ms', self.start+30000),
                             ('twap_lookback_seconds', 30)]:
            result = page_reference(self.market, self.reference, {**detail, field: value})
            self.assertIsNone(result['published_price_to_beat'])

    def test_gamma_conflict_cannot_be_healed_by_page(self):
        result = page_reference(self.market, {**self.reference, 'price_to_beat_conflict': True}, self.parse())
        self.assertIsNone(result['published_price_to_beat'])

    def test_matching_gamma_value_keeps_gamma_provenance(self):
        original = {**self.reference, 'published_price_to_beat': '84447.94584493896', 'price_to_beat_path': 'gamma'}
        self.assertEqual(page_reference(self.market, original, self.parse()), original)
