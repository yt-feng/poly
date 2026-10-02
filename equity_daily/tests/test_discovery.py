import json
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from equity_daily.discovery import DiscoveryMixin
from equity_daily.core import Journal
from equity_daily.github_archive import bundle_ready


class StubHTTP:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []
    async def get(self, source, url, params=None, **kwargs):
        self.requests.append((url, dict(params or {})))
        return next(self.responses)


class TestDiscovery(unittest.IsolatedAsyncioTestCase):
    async def run_scan(self, responses):
        with tempfile.TemporaryDirectory() as tmp:
            x = DiscoveryMixin()
            x.args = SimpleNamespace(lookback_days=7, lookahead_days=8, max_pages=10)
            x.journal = Journal(Path(tmp)/'equity_daily_discovery', 'test')
            x.catalog, x.discovery, x.stats = {}, {}, Counter()
            x.http = StubHTTP(responses)
            x.accept = lambda market, event: None
            await x.discover()
            x.journal.checkpoint({})
            return x

    async def test_finance_latest_and_opaque_cursor(self):
        x = await self.run_scan([(200, {'events': [], 'next_cursor': 'opaque-a'}), (200, {'events': []})])
        p = x.http.requests[0][1]
        self.assertEqual(p['tag_slug'], 'finance')
        self.assertEqual(p['order'], 'id')
        self.assertEqual(p['ascending'], 'false')
        self.assertLessEqual(p['limit'], 100)
        self.assertEqual(x.http.requests[1][1]['after_cursor'], 'opaque-a')
        self.assertTrue(x.discovery['open:finance']['pagination_complete'])

    async def test_oversize_page_restarts_smaller(self):
        x = await self.run_scan([(599, None), (200, {'events': []})])
        self.assertEqual(x.http.requests[0][1]['limit'], 25)
        self.assertEqual(x.http.requests[1][1]['limit'], 5)

    async def test_200_error_not_complete(self):
        x = await self.run_scan([(200, {'error': 'unknown filter'})])
        self.assertFalse(x.discovery['open:finance']['pagination_complete'])
        self.assertEqual(x.discovery['open:finance']['reason'], 'invalid_response_shape')

    async def test_repeated_cursor_not_complete(self):
        x = await self.run_scan([(200, {'events': [], 'next_cursor': 'a'}), (200, {'events': [], 'next_cursor': 'a'})])
        self.assertFalse(x.discovery['open:finance']['pagination_complete'])
        self.assertEqual(x.discovery['open:finance']['reason'], 'repeated_cursor')

    def test_uncommitted_segment_not_bundled(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)/'equity_daily_archive'
            j = Journal(root, 'test')
            j.checkpoint({})
            j.emit('polymarket_ws', {'new': True})
            j.close_source('polymarket_ws')
            bundles = bundle_ready(root)
            import tarfile
            with tarfile.open(root/bundles[0]['file']) as tar:
                self.assertFalse(any('polymarket_ws' in name for name in tar.getnames()))
            j.checkpoint({})
            bundles = bundle_ready(root)
            with tarfile.open(root/bundles[-1]['file']) as tar:
                self.assertTrue(any('polymarket_ws' in name for name in tar.getnames()))
