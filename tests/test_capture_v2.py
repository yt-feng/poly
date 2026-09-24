import gzip
import hashlib
import json
from datetime import date
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock
from archive_v2 import Archive, sha256
from capture_v2 import Collector, book_summary, epoch_ms, fresh, next_tick, window_slug
from binance_backfill_v2 import archive_key, download_verified, planned
from quality_v2 import longest_gap, summarize


class ParsingTests(unittest.TestCase):
    def test_nearest_five_bids_are_sorted_descending(self):
        result = book_summary({'bids': [{'price': str(p/10), 'size': str(p)} for p in range(1, 8)]})
        self.assertEqual(result['bid'], .7)
        self.assertEqual(result['bid_depth5'], 25)

    def test_nearest_five_asks_are_sorted_ascending(self):
        result = book_summary({'asks': [[str(p/10), str(p)] for p in range(7, 0, -1)]})
        self.assertEqual(result['ask'], .1)
        self.assertEqual(result['ask_depth5'], 15)

    def test_empty_book_is_null_not_zero_price(self):
        self.assertIsNone(book_summary({})['bid'])

    def test_zero_nan_and_infinite_levels_excluded(self):
        self.assertEqual(book_summary({'bids': [['NaN', '10'], ['1', '0'], ['Infinity', '10']]})['bid_levels'], 0)

    def test_source_timestamp_units(self):
        self.assertEqual(epoch_ms(1735689600010866), 1735689600010)
        self.assertEqual(epoch_ms('1735689600010'), 1735689600010)
        self.assertEqual(epoch_ms(1735689600), 1735689600000)
        self.assertEqual(epoch_ms('2025-01-01T00:00:00+00:00'), 1735689600000)
        self.assertIsNone(epoch_ms(None))

    def test_window_boundary(self):
        self.assertEqual(window_slug('btc', 1790093999.999), 'btc-updown-5m-1790093700')
        self.assertEqual(window_slug('btc', 1790094000), 'btc-updown-5m-1790094000')

    def test_fast_processing_does_not_add_full_second(self):
        self.assertEqual(next_tick(10, 10.2), 11)

    def test_slow_processing_skips_without_catchup_burst(self):
        self.assertEqual(next_tick(10, 13.7), 14)

    def test_monotonic_phase_is_preserved(self):
        self.assertAlmostEqual(next_tick(10.1, 11.4), 12.1)

    def test_stale_event_is_not_fresh_after_recent_receipt(self):
        self.assertFalse(fresh({'received_ms': 99000, 'event_ms': 1000}, 100000, 5000, require_event=True))

    def test_missing_source_timestamp_is_not_fabricated(self):
        self.assertFalse(fresh({'received_ms': 99000, 'event_ms': None}, 100000, 5000, require_event=True))
        self.assertTrue(fresh({'received_ms': 99000, 'event_ms': None}, 100000, 5000))

    def test_future_clock_anomaly_rejected(self):
        self.assertFalse(fresh({'received_ms': 99000, 'event_ms': 200000}, 100000, 5000, require_event=True))


class ArchiveTests(unittest.TestCase):
    def test_rotated_gzip_is_lossless_and_checksummed(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            archive = Archive(root, max_bytes=1)
            rows = [{'source': 'binance', 'value': None}, {'source': 'chainlink', 'value': '1.2345'}]
            for row in rows:
                archive.write('raw', row)
            archive.close()
            paths = sorted(root.glob('raw-*.gz'))
            self.assertEqual(len(paths), 2)
            restored = []
            for path in paths:
                with gzip.open(path, 'rt') as f:
                    restored.extend(json.loads(line) for line in f)
                self.assertEqual(sha256(path), path.with_suffix('.gz.sha256').read_text().split()[0])
            self.assertEqual(restored, rows)
            self.assertEqual(len(json.loads((root/'manifest.json').read_text())['files']), 2)

    def test_no_partial_file_is_declared_ready(self):
        with tempfile.TemporaryDirectory() as d:
            archive = Archive(Path(d))
            archive.write('raw', {'x': 1})
            self.assertFalse(list(Path(d).glob('*.jsonl.gz')))
            archive.close()
            self.assertTrue(list(Path(d).glob('*.jsonl.gz')))


class QualityTests(unittest.TestCase):
    def test_empty_day_reports_full_outage(self):
        q = summarize([], '2026-09-22')['daily'][0]
        self.assertEqual(q['daily_coverage'], 0)
        self.assertEqual(q['longest_gap_seconds'], 86400)

    def test_leading_and_trailing_gaps_count(self):
        self.assertEqual(longest_gap({3, 4}, 0, 10), 5)

    def test_gap_inside_day(self):
        self.assertEqual(longest_gap({0, 1, 7, 8, 9}, 0, 10), 5)

    def test_exact_duplicate_does_not_inflate_validity(self):
        row = dict(asset='btc', sample_ms=1790094000000, poly_valid=True, binance_valid=False)
        q = summarize([row, row])
        self.assertEqual(q['duplicate_rows'], 1)
        self.assertEqual(q['daily'][0]['valid_poly'], 1)
        self.assertEqual(q['daily'][0]['observed_seconds'], 1)


class BackfillTests(unittest.TestCase):
    def test_spot_archive_paths(self):
        self.assertEqual(archive_key('BTCUSDT', date(2026,9,22), 'klines_1s'),
                         'spot/daily/klines/BTCUSDT/1s/BTCUSDT-1s-2026-09-22.zip')

    def test_futures_archive_paths(self):
        self.assertEqual(archive_key('BTCUSDT', date(2026,9,22), 'aggTrades', 'futures/um'),
                         'futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-09-22.zip')

    def test_invalid_symbol_prevents_path_injection(self):
        with self.assertRaises(ValueError):
            archive_key('../../X', date.today(), 'trades')

    def test_futures_one_second_unsupported(self):
        with self.assertRaises(ValueError):
            archive_key('BTCUSDT', date.today(), 'klines_1s', 'futures/um')

    def test_plan_inclusive_range_and_latest_first(self):
        tasks = list(planned(['BTCUSDT'], date(2026,9,20), date(2026,9,22), ['trades'], 'spot'))
        self.assertEqual(len(tasks), 3)
        self.assertEqual(tasks[0][1], date(2026,9,22))

    def fake_session(self, data, checksum=None):
        check = Mock()
        check.text = (checksum or hashlib.sha256(data).hexdigest())+'  file.zip'
        body = Mock()
        body.headers = {'Content-Length': str(len(data))}
        body.iter_content.return_value = iter([data])
        body.__enter__ = Mock(return_value=body)
        body.__exit__ = Mock(return_value=False)
        session = Mock()
        session.get.side_effect = [check, body]
        return session

    def test_verified_download(self):
        with tempfile.TemporaryDirectory() as d:
            path, digest, size = download_verified(self.fake_session(b'archive'), 'spot/file.zip', Path(d), 100)
            self.assertEqual(path.read_bytes(), b'archive')
            self.assertEqual(size, 7)
            self.assertEqual(digest, sha256(path))

    def test_checksum_failure_is_not_checkpointed(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ValueError):
                download_verified(self.fake_session(b'archive', '0'*64), 'spot/file.zip', Path(d), 100)
            self.assertFalse(list(Path(d).glob('*.zip')))
            self.assertFalse(list(Path(d).glob('*.partial')))

    def test_download_budget_is_enforced(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(OverflowError):
                download_verified(self.fake_session(b'archive'), 'spot/file.zip', Path(d), 2)


class FeedIsolationTests(unittest.TestCase):
    def test_binance_and_chainlink_do_not_overwrite_each_other(self):
        with tempfile.TemporaryDirectory() as d:
            c = Collector(['btc'], Path(d))
            c.binance_message('btc', {'stream': 'btcusdt@trade', 'data': {'p':'100', 'T':1790094000000, 't':1}}, 'c1')
            c.chainlink_message({'topic':'crypto_prices_chainlink', 'payload': {'symbol':'btc/usd', 'value':'101', 'timestamp':1790094000000}}, 'c2')
            self.assertEqual(c.prices['btc']['price'], '100')
            self.assertEqual(c.prices['chainlink_btc']['price'], '101')
            c.archive.close()

    def test_binance_book_ticker_preserves_unknown_event_time(self):
        with tempfile.TemporaryDirectory() as d:
            c = Collector(['btc'], Path(d))
            c.binance_message('btc', {'stream':'btcusdt@bookTicker', 'data': {'b':'1', 'a':'2', 'B':'3', 'A':'4', 'u':1}}, 'c')
            self.assertIsNone(c.tickers['btc']['event_ms'])
            c.archive.close()

    def test_depth_gap_is_recorded(self):
        with tempfile.TemporaryDirectory() as d:
            c = Collector(['btc'], Path(d))
            for first, last in [(1,3),(5,8)]:
                c.binance_message('btc', {'data':{'e':'depthUpdate','U':first,'u':last}}, 'c')
            self.assertEqual(c.counts['depth_gap'], 1)
            c.archive.close()

    def test_reconnect_does_not_claim_depth_continuity(self):
        with tempfile.TemporaryDirectory() as d:
            c = Collector(['btc'], Path(d))
            c.binance_message('btc', {'data':{'e':'depthUpdate','U':1,'u':3}}, 'old')
            c.binance_message('btc', {'data':{'e':'depthUpdate','U':500,'u':503}}, 'new')
            self.assertEqual(c.counts['depth_gap'], 0)  # connection log supplies the discontinuity
            c.archive.close()


class HealthLifecycleTests(unittest.TestCase):
    def test_pre_shutdown_live_health_is_retained_separately(self):
        with tempfile.TemporaryDirectory() as d:
            c = Collector(['btc'], Path(d))
            live = c.health()
            self.assertNotIn('pre_shutdown_live_health', live)
            c.pre_shutdown_live_health = live
            final = c.health()
            self.assertIn('pre_shutdown_live_health', final)
            self.assertEqual(final['pre_shutdown_live_health']['started_ms'], c.started_ms)
            self.assertIsNot(final['pre_shutdown_live_health'], final)
            c.archive.close()


if __name__ == '__main__':
    unittest.main()
