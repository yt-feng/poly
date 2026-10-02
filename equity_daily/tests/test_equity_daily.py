import ast
import asyncio
import base64
from collections import Counter
import gzip
import hashlib
import json
import os
from pathlib import Path
import struct
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import aiohttp
from aiohttp import web
from cryptography.fernet import Fernet
from equity_daily.core import Journal, array, classify, digest, epoch
from equity_daily.collector import page_rows, cursor_of
from equity_daily.github_archive import bundle_ready, ensure_release
from equity_daily.transport import decode_yahoo, PublicHTTP, market_observations


def fixture(question, **extra):
    return dict(id='100', question=question, conditionId='0x123',
                slug='price-on-october-2-2026', endDate='2026-10-02T20:00:00Z',
                outcomes='["Up", "Down"]', clobTokenIds='["111", "222"]',
                active=True, closed=False, **extra)


class ClassificationTests(unittest.TestCase):
    def test_all_screenshot_instruments(self):
        names = ['Amazon (AMZN)', 'Microsoft (MSFT)', 'Apple (AAPL)', 'NVIDIA (NVDA)',
                 'Meta (META)', 'Tesla (TSLA)', 'Google (GOOGL)', 'Silver (SI)',
                 'Dow Jones (DJI)', 'Gold (GC)', 'Netflix (NFLX)', 'NYA (NYA)',
                 'Russell 2000 (RUT)', 'Nasdaq 100 (NDX)', 'Palantir (PLTR)',
                 'Robinhood (HOOD)', 'Hang Seng (HSI)']
        for name in names:
            with self.subTest(name=name):
                r, why = classify(fixture(name+' Up or Down on October 2?'))
                self.assertIsNotNone(r, why)
                self.assertEqual(r['family'], 'equity_daily')
                self.assertEqual(len(r['outcomes']), 2)

    def test_new_ticker_not_static_allowlist(self):
        r, _ = classify(fixture('New Company (XYZNEW) Up or Down on October 2'))
        self.assertEqual(r['yahoo_symbol'], 'XYZNEW')

    def test_threshold_parent_date(self):
        m = fixture('Apple (AAPL) closes above $260')
        m.update(outcomes='["No","Yes"]')
        r, _ = classify(m, {'title': 'Apple closing price on October 2'})
        self.assertEqual(r['event_kind'], 'close_threshold')
        self.assertEqual(r['outcomes'][0], {'label': 'No', 'token_id': '111'})

    def test_crypto_excluded(self):
        for name in ['Bitcoin (BTC)', 'Ethereum (ETH)', 'Solana (SOL)', 'XRP', 'Crypto']:
            with self.subTest(name=name):
                self.assertIsNone(classify(fixture(name+' Up or Down on October 2'))[0])

    def test_btc_short_slug_excluded(self):
        m = fixture('Apple (AAPL) Up or Down on October 2')
        m['slug'] = 'btc-updown-5m-1790956800'
        self.assertIsNone(classify(m)[0])

    def test_no_five_minute(self):
        self.assertIsNone(classify(fixture('Apple (AAPL) 5 minute Up or Down on October 2'))[0])

    def test_no_monthly(self):
        self.assertIsNone(classify(fixture('Apple (AAPL) monthly closes above $260 on October 2'))[0])

    def test_no_daily_date_assumption_from_enddate(self):
        m = fixture('Apple (AAPL) closes above $260')
        m['slug'] = 'aapl-close-price'
        self.assertEqual(classify(m)[1], 'daily_date_unconfirmed')

    def test_token_length_mismatch(self):
        m = fixture('Apple (AAPL) Up or Down on October 2')
        m['clobTokenIds'] = '["1"]'
        self.assertIsNone(classify(m)[0])

    def test_duplicate_tokens(self):
        m = fixture('Apple (AAPL) Up or Down on October 2')
        m['clobTokenIds'] = '["1","1"]'
        self.assertIsNone(classify(m)[0])

    def test_invalid_tokens(self):
        m = fixture('Apple (AAPL) Up or Down on October 2')
        m['clobTokenIds'] = '["../btc","1"]'
        self.assertIsNone(classify(m)[0])

    def test_unexpected_labels(self):
        m = fixture('Apple (AAPL) Up or Down on October 2')
        m['outcomes'] = '["Win","Lose"]'
        self.assertIsNone(classify(m)[0])

    def test_gold_not_silently_exact_contract(self):
        r, _ = classify(fixture('Gold (GC) Up or Down on October 2'))
        self.assertEqual(r['yahoo_symbol'], 'GC=F')
        self.assertIn('proxy', r['underlying_relation'])
        self.assertFalse(r['underlying_is_official_settlement'])

    def test_index_not_etf(self):
        r, _ = classify(fixture('Nasdaq 100 (NDX) Up or Down on October 2'))
        self.assertEqual(r['yahoo_symbol'], '^NDX')
        self.assertNotEqual(r['yahoo_symbol'], 'QQQ')

    def test_rules_symbol_overrides_approximation(self):
        r, _ = classify(fixture('Gold (GC) Up or Down on October 2',
                                description='See https://finance.yahoo.com/quote/GCZ26.CMX/history/'))
        self.assertEqual(r['yahoo_symbol'], 'GCZ26.CMX')
        self.assertEqual(r['mapping_evidence'], 'market_rules_yahoo_url')

    def test_hong_kong_ticker(self):
        r, _ = classify(fixture('Tencent (0700.HK) Up or Down on October 2'))
        self.assertEqual(r['yahoo_symbol'], '0700.HK')

    def test_unknown_financial_name_preserved(self):
        r, why = classify(fixture('New Company shares Up or Down on October 2'))
        self.assertEqual(why, 'accepted_underlying_unmapped')
        self.assertIsNone(r['yahoo_symbol'])

    def test_nonfinancial_direction_not_subscribed(self):
        self.assertIsNone(classify(fixture('Approval rating Up or Down on October 2'))[0])

    def test_array_and_epoch(self):
        self.assertEqual(array('bad'), [])
        self.assertEqual(array('[1,2]'), [1, 2])
        self.assertEqual(epoch('2026-10-02T20:00:00Z'), epoch('2026-10-02T16:00:00-04:00'))
        self.assertIsNone(epoch('bad'))


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)/'equity_daily_test'
    def tearDown(self):
        self.temp.cleanup()

    def test_raw_roundtrip(self):
        journal = Journal(self.root, 'test')
        raw = '{"price":"0.7300", "size":"0003.25", "future_field":"原始"}'
        journal.wire('polymarket_ws', raw)
        manifest = journal.checkpoint({})
        file = next(x for x in manifest['files'] if x['source']=='polymarket_ws')
        row = json.loads(gzip.decompress((self.root/file['file']).read_bytes()))
        self.assertEqual(base64.b64decode(row['payload_b64']).decode(), raw)
        self.assertEqual(row['payload_sha256'], hashlib.sha256(raw.encode()).hexdigest())
        self.assertGreater(row['received_at_ns'], 0)
        self.assertFalse(manifest['raw_wire_complete'])

    def test_no_cross_family_override(self):
        j = Journal(self.root, 'x')
        j.emit('audit', {'family': 'btc_5_min', 'run_id': 'bad'})
        m = j.checkpoint({})
        rows = gzip.decompress((self.root/m['files'][0]['file']).read_bytes()).decode().splitlines()
        self.assertTrue(all(json.loads(x)['family']=='equity_daily' for x in rows))

    def test_existing_directory_never_overwritten(self):
        j = Journal(self.root, 'x')
        j.checkpoint({})
        with self.assertRaises(ValueError):
            Journal(self.root, 'x')

    def test_btc_directory_rejected(self):
        with self.assertRaises(ValueError):
            Journal(Path(self.temp.name)/'capture_v2_output', 'x')

    def test_segment_rotation_checksums(self):
        j = Journal(self.root, 'x', segment_bytes=200)
        for i in range(5):
            j.emit('polymarket_ws', {'n': i})
        m = j.checkpoint({})
        self.assertGreaterEqual(len(m['files']), 5)
        for item in m['files']:
            self.assertEqual(digest(self.root/item['file']), item['sha256'])
        self.assertFalse(list(self.root.glob('*.part')))

    def test_underlying_encryption_roundtrip(self):
        key = Fernet.generate_key()
        with patch.dict(os.environ, {'EQUITY_ARCHIVE_KEY': key.decode()}):
            j = Journal(self.root, 'x')
        j.emit('underlying_yahoo_decoded', {'price': '100.1000'})
        m = j.checkpoint({})
        f = next(x for x in m['files'] if x['source'].startswith('underlying_'))
        self.assertTrue(f['encrypted'])
        decoded = gzip.decompress(Fernet(key).decrypt((self.root/f['file']).read_bytes()))
        self.assertEqual(json.loads(decoded)['price'], '100.1000')
        self.assertNotIn(key, (self.root/'manifest-000001.json').read_bytes())

    def test_bundle_idempotent_and_no_part_files(self):
        j = Journal(self.root, 'x')
        j.wire('polymarket_ws', '{"event_type":"book"}')
        j.checkpoint({})
        (self.root/'equity_daily-incomplete.jsonl.gz.part').write_bytes(b'not closed')
        first = bundle_ready(self.root)
        second = bundle_ready(self.root)
        self.assertEqual(first, second)
        with tarfile.open(self.root/first[0]['file']) as t:
            self.assertTrue(any(n.startswith('manifest-') for n in t.getnames()))
            self.assertFalse(any(n.endswith('.part') for n in t.getnames()))
            self.assertTrue(all('/' not in n for n in t.getnames()))

    def test_release_namespace_guard(self):
        with self.assertRaises(ValueError):
            ensure_release('capture-v2-123')


class ProtocolTests(unittest.TestCase):
    def test_yahoo_proto_decode(self):
        # id=AAPL, price=100.25, time=1000, day_volume=500, unknown field=7.
        raw = b'\x0a\x04AAPL'+b'\x15'+struct.pack('<f',100.25)+b'\x18\xd0\x0f'+b'\x48\xe8\x07'+b'\xa0\x06\x07'
        out = decode_yahoo(base64.b64encode(raw).decode())
        self.assertEqual(out['id'], 'AAPL')
        self.assertEqual(out['price'], 100.25)
        self.assertEqual(out['time'], 1000)
        self.assertEqual(out['day_volume'], 500)
        self.assertEqual(out['unknown_100'], 7)

    def test_malformed_proto(self):
        for raw in (b'\x00', b'\x18\x80', b'\x0a\x10A', b'\x0b'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                decode_yahoo(base64.b64encode(raw).decode())

    def test_pm_batch_and_change_new_shape(self):
        stats, coverage = Counter(), {}
        class Sink:
            def emit(self, *args):
                pass
        market_observations(Sink(), stats, coverage, json.dumps([
            {'event_type':'book','asset_id':'111','bids':[],'asks':[]},
            {'event_type':'price_change','price_changes':[{'asset_id':'222','price':'0.30'}]}]))
        market_observations(Sink(), stats, coverage, json.dumps({'type':'last_trade_price','payload':{'tokenId':'111','price':'0.50'}}))
        self.assertEqual(set(coverage), {'111','222'})
        self.assertEqual(coverage['111']['events'], 2)

    def test_pagination_shapes(self):
        self.assertEqual(page_rows({'events':[1]}, 'events'), [1])
        self.assertEqual(page_rows({'data':[2]}, 'trades'), [2])
        self.assertEqual(cursor_of({'pagination':{'next_cursor':'abc'}}), 'abc')
        self.assertEqual(cursor_of({'next_cursor':'xyz'}), 'xyz')

    def test_no_btc_imports_or_order_submission(self):
        package = Path(__file__).resolve().parents[1]
        forbidden = {'capture_v2','capture_v3','archive_v2','capture_runtime_v2','py_clob_client'}
        for file in package.glob('*.py'):
            tree = ast.parse(file.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    self.assertFalse({n.name for n in node.names} & forbidden)
                if isinstance(node, ast.ImportFrom):
                    self.assertNotIn(node.module, forbidden)
                if isinstance(node, ast.Attribute):
                    self.assertNotIn(node.attr, {'create_order','post_order','cancel_order','redeem'})


class HTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_entity_bytes_and_no_request_secrets_logged(self):
        async def handler(request):
            return web.Response(body=b'{"price":"100.1000","v":123}', content_type='application/json')
        app = web.Application()
        app.router.add_get('/chart', handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            with tempfile.TemporaryDirectory() as tmp:
                j = Journal(Path(tmp)/'equity_daily_http', 'http')
                async with aiohttp.ClientSession() as session:
                    client = PublicHTTP(session, j, Counter())
                    status, data = await client.get('underlying_test', f'http://127.0.0.1:{port}/chart',
                                                     headers={'APCA-API-SECRET-KEY':'do-not-log-me'})
                m = j.checkpoint({})
                self.assertEqual(status, 200)
                self.assertEqual(data['price'], '100.1000')
                f = next(x for x in m['files'] if x['source']=='underlying_test')
                content = gzip.decompress((j.root/f['file']).read_bytes())
                self.assertNotIn(b'do-not-log-me', content)
                self.assertEqual(json.loads(base64.b64decode(json.loads(content)['payload_b64'])), data)
        finally:
            await runner.cleanup()


if __name__ == '__main__':
    unittest.main()
