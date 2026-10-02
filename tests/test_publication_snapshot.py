"""Reproduce a live manifest replacement while gh prepares an upload body."""
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from archive_v2 import atomic_json, publish


class PublicationSnapshotTests(unittest.TestCase):
    def test_atomic_metadata_replacement_cannot_change_upload_content_length(self):
        for name in ('manifest.json', 'health.json', 'quality.json', 'quality.md'):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                original = Path(tmp) / name
                original.write_text('{"generation": 1}\n')
                expected = original.read_bytes()
                uploads = []

                def fake_gh(*args, **kwargs):
                    if args[:2] == ('release', 'view'):
                        return ''
                    self.assertEqual(args[:3], ('release', 'upload', 'synthetic'))
                    uploaded = Path(args[3])
                    content_length = uploaded.stat().st_size
                    # Exactly the production race: the collector atomically
                    # replaces the path after gh stats it but before gh opens it.
                    atomic_json(original, {'generation': 2, 'rows': list(range(100))})
                    body = uploaded.read_bytes()
                    if len(body) != content_length:
                        raise RuntimeError('http2: request body larger than specified content length')
                    self.assertEqual(body, expected)
                    self.assertEqual(uploaded.name, name)
                    self.assertEqual(args[-1], '--clobber')
                    uploads.append(uploaded)
                    return ''

                with patch('archive_v2.gh', side_effect=fake_gh):
                    publish('synthetic', [original], replace=True, timeout=60, deadline=123)
                self.assertEqual(len(uploads), 1)
                self.assertFalse(uploads[0].exists())
                self.assertEqual(json.loads(original.read_text())['generation'], 2)

    def test_snapshot_removed_after_failed_upload_without_replaying_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            original = Path(tmp) / 'manifest.json'
            original.write_text('{}')
            uploads = []

            def fake_gh(*args, **kwargs):
                if args[:2] == ('release', 'view'):
                    return ''
                uploads.append(Path(args[3]))
                self.assertTrue(uploads[-1].is_file())
                raise RuntimeError('synthetic upload failure')

            with patch('archive_v2.gh', side_effect=fake_gh):
                with self.assertRaisesRegex(RuntimeError, 'synthetic upload failure'):
                    publish('synthetic', [original], replace=True)
            self.assertEqual(len(uploads), 1)
            self.assertFalse(uploads[0].exists())
            self.assertEqual(original.read_text(), '{}')

    def test_copy_pins_one_generation_even_when_source_path_is_replaced(self):
        with tempfile.TemporaryDirectory() as tmp:
            original = Path(tmp) / 'manifest.json'
            original.write_text('{"generation": 1}')
            copy = shutil.copyfileobj
            bodies = []

            def replace_while_copying(source, destination):
                atomic_json(original, {'generation': 2, 'rows': list(range(100))})
                copy(source, destination)

            def fake_gh(*args, **kwargs):
                if args[:2] == ('release', 'upload'):
                    bodies.append(json.loads(Path(args[3]).read_text()))
                return ''

            with patch('archive_v2.shutil.copyfileobj', side_effect=replace_while_copying), \
                    patch('archive_v2.gh', side_effect=fake_gh):
                publish('synthetic', [original], replace=True)
            self.assertEqual(bodies, [{'generation': 1}])
            self.assertEqual(json.loads(original.read_text())['generation'], 2)

    def test_elapsed_snapshot_preparation_does_not_extend_upload_deadline(self):
        with tempfile.TemporaryDirectory() as tmp:
            original = Path(tmp) / 'manifest.json'
            original.write_text('{}')
            with patch('archive_v2.time.monotonic', side_effect=[90, 101]), \
                    patch('archive_v2.subprocess.run') as run:
                with self.assertRaisesRegex(TimeoutError, 'deadline exhausted'):
                    publish('synthetic', [original], replace=True, timeout=60, deadline=100)
            self.assertEqual(run.call_count, 1)  # Only release view, no upload.
            self.assertEqual(run.call_args.kwargs['timeout'], 10)

    def test_immutable_segments_do_not_need_large_duplicate_copies(self):
        with tempfile.TemporaryDirectory() as tmp:
            segment = Path(tmp) / 'snapshots.jsonl.gz'
            segment.write_bytes(b'closed immutable segment')
            with patch('archive_v2.gh', return_value='') as gh:
                publish('synthetic', [segment], replace=True, timeout=60, deadline=123)
            self.assertEqual(gh.call_args.args[3], str(segment))
            self.assertEqual(gh.call_args.kwargs, {'timeout': 60, 'deadline': 123})
            self.assertEqual(segment.read_bytes(), b'closed immutable segment')


if __name__ == '__main__':
    unittest.main()
