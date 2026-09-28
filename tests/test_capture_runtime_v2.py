import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from quality_v2 import read_snapshots


class ProcessExitTests(unittest.TestCase):
    def run_cli(self, version, mode):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)/'capture'
            result = subprocess.run(
                [sys.executable, '-u', str(Path(__file__).with_name('capture_process_fixture.py')),
                 version, mode, str(output)], capture_output=True, text=True, timeout=5)
            self.assertTrue((output/'quality.json').is_file(), result.stderr)
            self.assertTrue((output/'health.json').is_file(), result.stderr)
            self.assertEqual(len(list(read_snapshots(output))), 1, result.stderr)
            self.assertFalse(list(output.rglob('*.part')))
            diagnostics = json.loads((output/'process_cleanup.json').read_text())
            self.assertLess(diagnostics['elapsed_seconds'], 2)
            return result, diagnostics

    def test_normal_and_recovered_cancel_exit_successfully_on_both_clis(self):
        for version in ('v2', 'v3'):
            for mode in ('normal', 'recover', 'executor_healthy'):
                with self.subTest(version=version, mode=mode):
                    result, diagnostics = self.run_cli(version, mode)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertFalse(diagnostics['failed'])
                    self.assertTrue(diagnostics['executor_stopped'])

    def test_permanently_cancel_resistant_task_exits_nonzero_with_closed_tail(self):
        for version in ('v2', 'v3'):
            with self.subTest(version=version):
                result, diagnostics = self.run_cli(version, 'resist')
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('discover', diagnostics['failures']['tasks'])
                self.assertIn('Capture process cleanup failed', result.stderr)

    def test_async_generator_cleanup_is_bounded_and_fails_explicitly(self):
        result, diagnostics = self.run_cli('v3', 'asyncgen')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('async_generators', diagnostics['failures'])
        self.assertTrue(diagnostics['executor_stopped'])
        self.assertIn('Capture process cleanup failed', result.stderr)

    def test_nonreturning_executor_cannot_hang_interpreter_atexit(self):
        result, diagnostics = self.run_cli('v3', 'executor_stuck')
        self.assertEqual(result.returncode, 1)
        self.assertFalse(diagnostics['executor_stopped'])
        self.assertIn('executor', diagnostics['failures'])
        self.assertIn('Capture process cleanup failed', result.stderr)

    def test_sigint_preserves_tail_and_exits_nonzero_on_both_clis(self):
        for version in ('v2', 'v3'):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as temp:
                output = Path(temp)/'capture'
                child = subprocess.Popen(
                    [sys.executable, '-u', str(Path(__file__).with_name('capture_process_fixture.py')),
                     version, 'interrupt', str(output)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    deadline = time.monotonic()+3
                    while not (output/'fixture_ready').is_file() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertTrue((output/'fixture_ready').is_file())
                    child.send_signal(signal.SIGINT)
                    _, stderr = child.communicate(timeout=3)
                    self.assertNotEqual(child.returncode, 0, stderr)
                    self.assertTrue((output/'quality.json').is_file(), stderr)
                    self.assertEqual(len(list(read_snapshots(output))), 1)
                    self.assertFalse(list(output.rglob('*.part')))
                    self.assertFalse(json.loads((output/'process_cleanup.json').read_text())['failed'])
                finally:
                    if child.poll() is None:
                        child.kill()
                        child.communicate(timeout=3)


if __name__ == '__main__':
    unittest.main()
