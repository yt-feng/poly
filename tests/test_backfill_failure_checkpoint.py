"""Retain a useful checkpoint when GitHub refuses an archive/state write."""
from argparse import Namespace
from contextlib import ExitStack, redirect_stdout
from datetime import date
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from archive_v2 import sha256
from binance_backfill_v2 import STATE_TAG, archive_key, run


class BackfillFailureCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.args = Namespace(output=self.root, start='2026-04-21', end='2026-04-21',
                              symbols='BTCUSDT', datasets='trades,aggTrades', market='spot',
                              max_files=24, max_attempts=48, max_bytes=1024, publish=True)
        self.state = {'schema_version': 2, 'files': {}}
        self.session = Mock(headers={})
        self.stack.enter_context(patch('binance_backfill_v2.load_state',
                                      return_value=(self.root/'backfill-state.json', self.state)))
        self.stack.enter_context(patch('binance_backfill_v2.requests.Session', return_value=self.session))
        self.stack.enter_context(patch('binance_backfill_v2.time.sleep'))
        self.stack.enter_context(redirect_stdout(io.StringIO()))

    def summary(self):
        self.session.close.assert_called_once()
        self.assertEqual(json.loads((self.root/'backfill-state.json').read_text()), self.state)
        return json.loads((self.root/'backfill-summary.json').read_text())

    def error(self, detail='HTTP 500'):
        return subprocess.CalledProcessError(1, ['gh', 'release', 'upload', STATE_TAG], stderr=detail)

    def download(self, session, key, directory, budget):
        path = directory/key.replace('/', '__')
        path.write_bytes(b'archive bytes')
        digest = sha256(path)
        path.with_suffix('.zip.CHECKSUM').write_text(digest)
        return path, digest, path.stat().st_size

    def test_checkpoint_upload_failure_is_not_replayed_in_finally(self):
        original = self.error()
        with patch('binance_backfill_v2.download_verified', side_effect=self.download), \
                patch('binance_backfill_v2.publish', side_effect=[None, original]) as publish:
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                run(self.args)
        self.assertIs(caught.exception, original)
        self.assertEqual(publish.call_count, 2)  # Archive once, checkpoint once.
        summary = self.summary()
        self.assertEqual(summary['status'], 'failed')
        self.assertIn('HTTP 500', summary['error']['stderr'])
        self.assertEqual(summary['completed_files'], 1)
        self.assertEqual(summary['pending_files'], 1)
        self.assertFalse(summary['all_requested_files_complete'])
        self.assertEqual(len(list(self.root.glob('*.zip'))), 1)
        self.assertEqual(len(list(self.root.glob('*.CHECKSUM'))), 1)

    def test_archive_upload_failure_does_not_claim_archive_complete(self):
        original = self.error()
        with patch('binance_backfill_v2.download_verified', side_effect=self.download), \
                patch('binance_backfill_v2.publish', side_effect=original) as publish:
            with self.assertRaises(subprocess.CalledProcessError):
                run(self.args)
        publish.assert_called_once()
        self.assertEqual(self.state['files'], {})
        summary = self.summary()
        self.assertEqual(summary['completed_files'], 0)
        self.assertEqual(summary['pending_files'], 2)

    def test_download_failure_preserves_original_without_remote_cleanup(self):
        original = ValueError('Official archive checksum mismatch')
        with patch('binance_backfill_v2.download_verified', side_effect=original), \
                patch('binance_backfill_v2.publish') as publish:
            with self.assertRaises(ValueError) as caught:
                run(self.args)
        self.assertIs(caught.exception, original)
        publish.assert_not_called()
        summary = self.summary()
        self.assertEqual(summary['pending_files'], 2)
        self.assertEqual(summary['error']['type'], 'ValueError')

    def complete_existing_files(self):
        for kind in self.args.datasets.split(','):
            key = archive_key('BTCUSDT', date(2026, 4, 21), kind)
            self.state['files'][key] = {'status': 'complete', 'release': 'existing-release'}

    def test_final_state_write_failure_still_has_summary(self):
        self.complete_existing_files()
        with patch('binance_backfill_v2.publish', side_effect=self.error()) as publish:
            with self.assertRaises(subprocess.CalledProcessError):
                run(self.args)
        publish.assert_called_once()
        summary = self.summary()
        self.assertEqual(summary['status'], 'failed')
        self.assertTrue(summary['all_requested_files_complete'])
        self.assertEqual(summary['downloaded_this_run'], 0)

    def test_summary_upload_failure_is_reported_locally(self):
        self.complete_existing_files()
        with patch('binance_backfill_v2.publish', side_effect=[None, self.error()]) as publish:
            with self.assertRaises(subprocess.CalledProcessError):
                run(self.args)
        self.assertEqual(publish.call_count, 2)
        self.assertEqual(self.summary()['status'], 'failed')

    def test_success_publishes_state_and_summary_with_complete_counts(self):
        self.complete_existing_files()
        uploaded = []

        def record(tag, paths, **kwargs):
            uploaded.append((tag, paths[0].name, json.loads(paths[0].read_text())))

        with patch('binance_backfill_v2.publish', side_effect=record):
            run(self.args)
        self.assertEqual([entry[1] for entry in uploaded], ['backfill-state.json', 'backfill-summary.json'])
        self.assertEqual(uploaded[1][2], self.summary())
        self.assertEqual(self.summary()['status'], 'completed')
        self.assertEqual(self.summary()['pending_files'], 0)


if __name__ == '__main__':
    unittest.main()
