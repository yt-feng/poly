import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

from archive_v2 import Archive, gh, upload_ready
from capture_v2 import Collector
from capture_watchdog_v2 import ensure_capture
from quality_v2 import read_snapshots, summarize


class OfflineCollector(Collector):
    async def idle(self, *args, **kwargs):
        await asyncio.Future()

    discover = poll_books = socket = depth_snapshots = idle

    async def sample(self, duration):
        for i in range(3):
            self.archive.write('snapshots', {'asset': 'btc', 'sample_ms': 1790380800000+i*1000})
            await asyncio.sleep(.01)


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_runs_close_tail_and_write_quality(self):
        # Run two independent lifecycles: one successful smoke is insufficient.
        with tempfile.TemporaryDirectory() as d:
            for day in ('first', 'next'):
                root = Path(d)/day
                c = OfflineCollector(['btc'], root)
                await asyncio.wait_for(c.run(1), 2)
                self.assertEqual(len(list(read_snapshots(root))), 3)
                self.assertFalse(list(root.glob('*.part')))
                self.assertEqual(json.loads((root/'quality.json').read_text())['daily'][0]['rows'], 3)

    async def test_stalled_cancellation_is_retried_and_diagnosed(self):
        async def delayed_exit():
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                await asyncio.Future()  # A socket close / cancellation race.

        with tempfile.TemporaryDirectory() as d:
            c = Collector(['btc'], Path(d))
            c.session = Mock(close=AsyncMock())
            task = asyncio.create_task(delayed_exit(), name='stalled-feed')
            await asyncio.sleep(0)
            await asyncio.wait_for(c.stop_tasks([task], grace=.01), .5)
            self.assertTrue(task.cancelled())
            self.assertIn('stalled-feed', json.loads((Path(d)/'shutdown.json').read_text())['pending'])
            c.session.close.assert_awaited_once()

    async def test_checkpoint_thread_drains_before_final_upload(self):
        active = 0
        overlap = []
        calls = []

        def uploader(root, release, *, stop=None, **kwargs):
            nonlocal active
            active += 1
            overlap.append(active)
            calls.append(stop)
            try:
                if stop is not None:
                    self.assertTrue(stop.wait(1))
                    time.sleep(.03)  # worker finishes after its coroutine cancels
            finally:
                active -= 1

        with tempfile.TemporaryDirectory() as d, patch('capture_v2.upload_ready', side_effect=uploader), patch('capture_v2.publish'):
            c = OfflineCollector(['btc'], Path(d), 'synthetic-test-only')
            await asyncio.wait_for(c.run(1), 2)
        self.assertEqual(max(overlap), 1)
        self.assertEqual(len(calls), 2)
        self.assertIsNone(calls[-1])

    async def test_feed_failure_still_preserves_samples(self):
        class Failing(OfflineCollector):
            async def discover(self):
                await asyncio.sleep(.015)
                raise RuntimeError('synthetic feed failure')

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            with self.assertRaisesRegex(RuntimeError, 'synthetic feed failure'):
                await Failing(['btc'], root).run(1)
            self.assertTrue(list(read_snapshots(root)))
            self.assertTrue((root/'quality.json').exists())
            self.assertFalse(list(root.glob('*.part')))


class PublicationTests(unittest.TestCase):
    def test_cli_timeout_is_enforced(self):
        with patch('archive_v2.subprocess.run', side_effect=subprocess.TimeoutExpired('gh', 60)) as run:
            with self.assertRaises(subprocess.TimeoutExpired):
                gh('release', 'view', 'synthetic', timeout=60)
            self.assertEqual(run.call_args.kwargs['timeout'], 60)

    def test_partial_upload_is_not_marked_durable_and_retry_is_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            archive = Archive(root)
            archive.write('snapshots', {'sample_ms': 1})
            archive.close()
            with patch('archive_v2.publish', side_effect=TimeoutError('synthetic checksum upload timeout')):
                with self.assertRaises(TimeoutError):
                    upload_ready(root, 'synthetic')
            self.assertFalse((root/'.uploaded.json').exists())
            with patch('archive_v2.publish') as publish:
                upload_ready(root, 'synthetic')
                upload_ready(root, 'synthetic')
                segments = [x for x in publish.call_args_list if len(x.args[1]) == 2]
                self.assertEqual(len(segments), 1)

    def test_stop_or_budget_keeps_pending_files_for_recovery(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            archive = Archive(root)
            archive.write('snapshots', {'sample_ms': 1})
            archive.close()
            stopped = threading.Event()
            stopped.set()
            with patch('archive_v2.publish') as publish:
                upload_ready(root, 'synthetic', stop=stopped)
                publish.assert_not_called()
                with self.assertRaises(TimeoutError):
                    upload_ready(root, 'synthetic', budget_seconds=0)
            self.assertFalse((root/'.uploaded.json').exists())
            self.assertEqual(len(list(root.glob('*.jsonl.gz'))), 1)


class WatchdogTests(unittest.TestCase):
    now = datetime(2026, 9, 27, tzinfo=timezone.utc)

    def run_record(self, status, age=60, run_id=1):
        return {'id': run_id, 'status': status, 'created_at': (self.now-timedelta(minutes=age)).isoformat()}

    def test_all_active_states_prevent_duplicate(self):
        for status in ('queued', 'pending', 'waiting', 'requested', 'in_progress'):
            call = Mock(return_value=json.dumps({'workflow_runs': [self.run_record(status)]}))
            self.assertEqual(ensure_capture('owner/repo', now=self.now, call=call)['action'], 'already_active')
            self.assertEqual(call.call_count, 1)

    def test_completed_run_is_replaced_then_repeated_event_sees_active(self):
        call = Mock(side_effect=[json.dumps({'workflow_runs': [self.run_record('completed')]}), '',
                               json.dumps({'workflow_runs': [self.run_record('queued', 0, 2)]})])
        self.assertEqual(ensure_capture('owner/repo', now=self.now, call=call)['action'], 'dispatched')
        self.assertEqual(ensure_capture('owner/repo', now=self.now, call=call)['action'], 'already_active')
        self.assertEqual(sum(c.args[:2] == ('workflow', 'run') for c in call.call_args_list), 1)

    def test_repeated_fast_failures_do_not_loop(self):
        call = Mock(return_value=json.dumps({'workflow_runs': [self.run_record('completed', i, i) for i in range(3)]}))
        with self.assertRaisesRegex(RuntimeError, 'restart loop'):
            ensure_capture('owner/repo', now=self.now, call=call)
        self.assertEqual(call.call_count, 1)

    def test_failed_inventory_never_dispatches(self):
        for response in ('not json', '{}'):
            call = Mock(return_value=response)
            with self.assertRaises((ValueError, KeyError)):
                ensure_capture('owner/repo', now=self.now, call=call)
            self.assertEqual(call.call_count, 1)

    def test_next_day_can_recover_after_restart_loop(self):
        call = Mock(side_effect=[json.dumps({'workflow_runs': [self.run_record('completed', i, i) for i in range(3)]}), ''])
        self.assertEqual(ensure_capture('owner/repo', now=self.now+timedelta(days=1), call=call)['action'], 'dispatched')


class RepeatedDayQualityTests(unittest.TestCase):
    def test_repeated_audit_keeps_real_gaps_and_midnight_boundary(self):
        start = 1790380800
        def rows():
            # Synthetic 2-day data with the real incident's 4,919-second hole.
            for second in range(2*86400):
                if 10000 <= second < 14919:
                    continue
                yield {'asset': 'btc', 'sample_ms': (start+second)*1000,
                       'poly_valid': True, 'binance_valid': True}
        for _ in range(2):
            result = summarize(rows())['daily']
            self.assertLess(result[0]['daily_coverage'], .95)
            self.assertEqual(result[0]['longest_gap_seconds'], 4919)
            self.assertEqual(result[1]['daily_coverage'], 1)
            self.assertEqual(result[1]['observed_seconds'], 86400)
