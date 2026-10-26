"""Offline REST-contract fixtures; load only the collector methods under test.

This avoids initializing transports, providers, credentials, or archive publishing.
The tested AST nodes are compiled directly from the production source.
"""
import ast
from collections import Counter
import copy
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

NOW = 1_800_000_000
SOURCE = Path(__file__).resolve().parents[1] / 'collector.py'


def load_contract():
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'), filename=str(SOURCE))
    helper = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'data_api_page')
    original = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'Collector')
    methods = [n for n in original.body if isinstance(n, ast.AsyncFunctionDef) and n.name in ('trades', 'history')]
    cls = ast.ClassDef(name='Collector', bases=[], keywords=[], body=methods, decorator_list=[])
    module = ast.fix_missing_locations(ast.Module(body=[helper, cls], type_ignores=[]))
    namespace = {'DATA': 'https://data-api.polymarket.com', 'time': SimpleNamespace(time=lambda: NOW)}
    exec(compile(module, str(SOURCE), 'exec'), namespace)
    return namespace['Collector']


def page(rows=(), cursor=None):
    return {'data': list(rows), 'pagination': {'next_cursor': cursor, 'has_more': cursor is not None}}


class FakeHTTP:
    def __init__(self, responses):
        self.responses, self.calls = responses, []

    async def get(self, source, url, params, *, context):
        self.calls.append((source, url, copy.deepcopy(params), copy.deepcopy(context)))
        return self.responses(len(self.calls)) if callable(self.responses) else self.responses.pop(0)


class FakeJournal:
    def __init__(self):
        self.events = []

    def emit(self, source, record):
        self.events.append((source, copy.deepcopy(record)))


class DataAPIV2ContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.network = patch('socket.socket.connect', side_effect=AssertionError('offline_contract_network_forbidden'))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.record = {'market_id': 'fixture-market', 'condition_id': 'fixture-condition'}

    def collector(self, responses):
        obj = load_contract()()
        obj.http, obj.journal, obj.stats = FakeHTTP(responses), FakeJournal(), Counter()
        return obj

    async def test_empty_trade_page_follows_opaque_cursor_and_preserves_filters(self):
        row = {'timestamp': NOW-30, 'price': None, 'size': None, 'transaction_hash': 'fixture-tx'}
        obj = self.collector([(200, page(cursor='opaque/+?=&')), (200, page([row]))])
        self.assertTrue(await obj.trades(self.record, NOW-60))
        second = obj.http.calls[1][2]
        self.assertEqual(second, {'condition': 'fixture-condition', 'limit': 1000, 'cursor': 'opaque/+?=&'})
        self.assertNotIn('start', obj.http.calls[0][2])
        self.assertNotIn('end', obj.http.calls[0][2])
        self.assertEqual(obj.journal.events[0][1]['rows'], [row])
        self.assertTrue(obj.journal.events[-1][1]['window_complete'])

    async def test_trade_selection_includes_bounds_preserves_distinct_same_hash_fills(self):
        rows = [{'timestamp': t, 'transaction_hash': 'same-fixture-tx', 'size': None}
                for t in (NOW-61, NOW-60, NOW-1, NOW, NOW+1)]
        obj = self.collector([(200, page(rows[:2], 'continue')), (200, page(rows[2:]))])
        self.assertTrue(await obj.trades(self.record, NOW-60))
        selected = [r for source, value in obj.journal.events if source == 'trade_window_rows' for r in value['rows']]
        self.assertEqual(selected, rows[1:4])
        self.assertEqual(obj.journal.events[-1][1]['selected_rows'], 3)

    async def test_old_row_does_not_imply_undocumented_sort_completion(self):
        obj = self.collector([(200, page([{'timestamp': NOW-100}], 'later')),
                              (200, page([{'timestamp': NOW-10}]))])
        self.assertTrue(await obj.trades(self.record, NOW-60))
        self.assertEqual(len(obj.http.calls), 2)
        self.assertEqual(obj.journal.events[-1][1]['selected_rows'], 1)

    async def test_empty_terminal_trade_page_is_complete_for_retained_window(self):
        obj = self.collector([(200, page())])
        self.assertTrue(await obj.trades(self.record, NOW-60))
        self.assertEqual(obj.journal.events[-1][1]['selected_rows'], 0)
        self.assertFalse(obj.journal.events[-1][1]['all_time_complete'])

    async def test_exhaustion_does_not_prove_window_outside_retention(self):
        obj = self.collector([(200, page())])
        self.assertFalse(await obj.trades(self.record, NOW-1095*86400-1))
        audit = obj.journal.events[-1][1]
        self.assertTrue(audit['pagination_complete'])
        self.assertFalse(audit['window_complete'])
        self.assertEqual(audit['reason'], 'outside_documented_retention')

    async def test_repeated_trade_cursor_is_incomplete(self):
        obj = self.collector([(200, page(cursor='again')), (200, page(cursor='again'))])
        self.assertFalse(await obj.trades(self.record, NOW-60))
        self.assertEqual(obj.journal.events[-1][1]['reason'], 'repeated_cursor')
        self.assertEqual(len(obj.http.calls), 2)

    async def test_trade_page_budget_is_incomplete(self):
        obj = self.collector(lambda n: (200, page(cursor='cursor-'+str(n))))
        self.assertFalse(await obj.trades(self.record, NOW-60))
        self.assertEqual(len(obj.http.calls), 100)
        self.assertEqual(obj.journal.events[-1][1]['reason'], 'page_limit')

    async def test_unknown_and_millisecond_trade_timestamps_are_incomplete(self):
        for timestamp in (None, True, '1800000000', 1.5, NOW*1000):
            with self.subTest(timestamp=timestamp):
                obj = self.collector([(200, page([{'timestamp': timestamp}]))])
                self.assertFalse(await obj.trades(self.record, NOW-60))
                self.assertEqual(obj.journal.events[-1][1]['reason'], 'invalid_trade_timestamp')

    async def test_invalid_envelopes_never_count_as_empty_success(self):
        invalid = [[], {}, {'data': []}, {'data': [], 'pagination': {}},
                   {'data': [], 'pagination': {'next_cursor': None, 'has_more': True}},
                   {'data': [], 'pagination': {'next_cursor': '', 'has_more': False}},
                   {'data': [], 'pagination': {'next_cursor': True, 'has_more': True}},
                   {'data': [], 'pagination': {'next_cursor': None, 'has_more': 0}},
                   page([None])]
        for value in invalid:
            for method in ('trades', 'history'):
                with self.subTest(value=value, method=method):
                    obj = self.collector([(200, value)])
                    args = (self.record, NOW-60) if method == 'trades' else ('fixture-token',)
                    self.assertFalse(await getattr(obj, method)(*args))
                    self.assertFalse(obj.journal.events[-1][1]['pagination_complete'])

    async def test_history_empty_continuing_page_pins_window_and_token(self):
        point = {'timestamp': NOW-10, 'price': 0.5, 'resolution_seconds': 0}
        obj = self.collector([(200, page(cursor='history/+?=&')), (200, page([point]))])
        self.assertTrue(await obj.history('fixture-token'))
        expected = {'token_id': 'fixture-token', 'start': NOW-86400, 'end': NOW,
                    'bucket_seconds': 60, 'cursor': 'history/+?=&'}
        self.assertEqual(obj.http.calls[1][2], expected)
        audit = obj.journal.events[-1][1]
        self.assertEqual(audit['points_returned'], 1)
        self.assertEqual(audit['end'], NOW)
        self.assertFalse(audit['all_time_complete'])

    async def test_history_valid_empty_terminal_resolution_miss_is_not_fallback(self):
        obj = self.collector([(200, page())])
        self.assertTrue(await obj.history('fixture-token'))
        self.assertEqual(len(obj.http.calls), 1)
        self.assertEqual(obj.journal.events[-1][1]['points_returned'], 0)

    async def test_malformed_history_numeric_fields_do_not_complete_or_count(self):
        valid = {'timestamp': NOW-10, 'price': 0.5, 'resolution_seconds': 60}
        invalid = [{}]
        for key, values in {'timestamp': (None, True, str(NOW), NOW*1000, -1),
                            'price': (None, True, '0.5', -0.01, 1.01, float('nan'), float('inf')),
                            'resolution_seconds': (None, True, '60', -1, 0.5)}.items():
            invalid.extend(dict(valid, **{key: value}) for value in values)
        for point in invalid:
            with self.subTest(point=point):
                obj = self.collector([(200, page([point]))])
                self.assertFalse(await obj.history('fixture-token'))
                audit = obj.journal.events[-1][1]
                self.assertFalse(audit['pagination_complete'])
                self.assertEqual(audit['points_returned'], 0)
                self.assertEqual(audit['reason'], 'invalid_history_point')
                self.assertEqual(obj.stats['history_complete_windows'], 0)

    async def test_history_repeated_cursor_and_budget_are_incomplete(self):
        obj = self.collector([(200, page(cursor='same')), (200, page(cursor='same'))])
        self.assertFalse(await obj.history('fixture-token'))
        self.assertEqual(obj.journal.events[-1][1]['reason'], 'repeated_cursor')
        obj = self.collector(lambda n: (200, page(cursor='cursor-'+str(n))))
        self.assertFalse(await obj.history('fixture-token'))
        self.assertEqual(len(obj.http.calls), 100)
        self.assertEqual(obj.journal.events[-1][1]['reason'], 'page_limit')

    async def test_http_errors_never_call_v1_or_clob_history(self):
        for status in (400, 403, 404, 405, 429, 503):
            for method in ('trades', 'history'):
                with self.subTest(status=status, method=method):
                    obj = self.collector([(status, {'error': 'fixture', 'code': 'FIXTURE', 'retryable': False})])
                    args = (self.record, NOW-60) if method == 'trades' else ('fixture-token',)
                    self.assertFalse(await getattr(obj, method)(*args))
                    self.assertEqual(len(obj.http.calls), 1)
                    self.assertTrue(obj.http.calls[0][1].startswith('https://data-api.polymarket.com/v2/'))
                    self.assertEqual(obj.journal.events[-1][1]['reason'], 'http_status_'+str(status))


if __name__ == '__main__':
    unittest.main()
