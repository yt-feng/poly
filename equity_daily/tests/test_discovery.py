import json
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from equity_daily.discovery import DiscoveryMixin
from equity_daily.core import Journal, classify
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


class TestInstrumentIdentity(unittest.TestCase):
    def make(self, question, **extra):
        return classify(dict(id='1', question=question, endDate='2026-10-02T20:00:00Z',
            clobTokenIds=['1','2'], outcomes=['Up','Down'], **extra))[0]

    def test_wti_crude_not_energy_company(self):
        self.assertEqual(self.make('WTI Crude Oil (WTI) Up or Down on October 2')['yahoo_symbol'], 'CL=F')
        self.assertEqual(self.make('W&T Offshore (WTI) Up or Down on October 2')['yahoo_symbol'], 'WTI')

    def test_spy_prefix_and_open_separate(self):
        r = self.make('SPY Opens Up or Down on October 2?')
        self.assertEqual(r['yahoo_symbol'], 'SPY')
        self.assertEqual(r['event_kind'], 'daily_open_direction')

    def test_spot_silver_proxy_explicit(self):
        r = self.make('Silver (XAGUSD) Up or Down on October 2')
        self.assertEqual(r['yahoo_symbol'], 'SI=F')
        self.assertIn('not_spot', r['underlying_relation'])
        self.assertFalse(r['underlying_is_official_settlement'])

    def test_oracle_reference_not_implied_collected(self):
        r = self.make('Google (GOOGL) Up or Down on October 2',
                      resolutionSource='https://pythdata.app/explore/Equity.US.GOOGL%2FUSD')
        self.assertEqual(r['oracle_reference_symbols'], ['Equity.US.GOOGL/USD'])
        self.assertFalse(r['oracle_prices_collected'])


    def test_indices_are_not_same_named_etfs(self):
        for sym, expected in [('DAX','^GDAXI'), ('UKX','^FTSE'), ('NIK','^N225'), ('DXY','DX-Y.NYB')]:
            r = self.make(f'Index ({sym}) Up or Down on October 2')
            self.assertEqual(r['yahoo_symbol'], expected)
            self.assertEqual(r['asset_class'], 'index')

    def test_fx_pair_mapping_separate_asset_class(self):
        for pair, expected in [('USD/JPY','JPY=X'), ('EUR/USD','EURUSD=X'), ('USD/BRL','BRL=X')]:
            r = self.make(f'Foreign exchange ({pair}) Up or Down on October 2')
            self.assertEqual(r['yahoo_symbol'], expected)
            self.assertEqual(r['asset_class'], 'foreign_exchange')


class TestHistoryPagination(unittest.IsolatedAsyncioTestCase):
    async def test_all_history_pages_and_complete_flag(self):
        from equity_daily.collector import Collector
        x = SimpleNamespace(http=StubHTTP([(200, {'data':[{'price':0.1}], 'pagination':{'next_cursor':'page2','has_more':True}}),
                                           (200, {'data':[{'price':0.2}], 'pagination':{'has_more':False}})]), stats=Counter())
        rows=[]
        x.journal=SimpleNamespace(emit=lambda source,row:rows.append(row))
        self.assertTrue(await Collector.history(x, '123'))
        self.assertEqual(x.http.requests[1][1]['cursor'], 'page2')
        self.assertEqual(rows[-1]['points_returned'], 2)
        self.assertFalse(rows[-1]['all_time_complete'])

    async def test_repeated_history_cursor_is_incomplete(self):
        from equity_daily.collector import Collector
        page=(200, {'data':[], 'pagination':{'next_cursor':'repeat','has_more':True}})
        x=SimpleNamespace(http=StubHTTP([page,page]), stats=Counter(), journal=SimpleNamespace(emit=lambda *a:None))
        self.assertFalse(await Collector.history(x,'123'))

    async def test_history_error_body_not_success(self):
        from equity_daily.collector import Collector
        x=SimpleNamespace(http=StubHTTP([(200, {'error':'invalid'})]), stats=Counter(), journal=SimpleNamespace(emit=lambda *a:None))
        self.assertFalse(await Collector.history(x,'123'))
