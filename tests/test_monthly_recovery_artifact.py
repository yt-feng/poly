"""Replay the observed GitHub push failure without contacting a remote."""
import os
from pathlib import Path
import re
import json
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest


class MonthlyRecoveryArtifactTests(unittest.TestCase):
    def test_each_part_retains_captured_csv_after_all_pushes_fail(self):
        workflow = (Path(__file__).resolve().parents[1] / '.github/workflows/'
                    'polymarket-monthly-rolling-24h.yml').read_text()
        for part in range(1, 6):
            with self.subTest(part=part), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                shutil.copyfile(Path(__file__).resolve().parents[1] / 'monthly_git_publish.sh',
                                root / 'monthly_git_publish.sh')
                job = re.search(rf'^  capture_part{part}:\n(.*?)(?=^  \w+:|\Z)',
                                workflow, re.M | re.S).group(1)
                capture = re.search(r'- name: Capture[^\n]*\n        run: \|\n'
                                    r'((?:          [^\n]*\n|\n)+)', job).group(1)
                script = textwrap.dedent(capture).replace('${{ github.run_id }}', '1234')
                script = script.replace('${{ github.run_attempt }}', '1')
                fake_bin = root/'bin'
                fake_bin.mkdir()
                programs = {
                    'python': f'#!{sys.executable}\n' + textwrap.dedent('''\
                        import sys
                        from pathlib import Path
                        path = Path(sys.argv[sys.argv.index('--output-csv')+1])
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_text('timestamp,bid,ask\\n123,0.4,0.6\\n')
                        '''),
                    'git': textwrap.dedent('''\
                        #!/bin/sh
                        echo "$*" >> git-calls.txt
                        case "$1" in
                          status) echo ' M data/monthly_runs/captured.csv' ;;
                          merge-base) exit 1 ;;
                          push) echo 'remote: Internal Server Error' >&2; exit 1 ;;
                        esac
                        '''),
                    'sleep': '#!/bin/sh\nexit 0\n',
                }
                for name, body in programs.items():
                    path = fake_bin/name
                    path.write_text(body)
                    path.chmod(0o755)
                result = subprocess.run(['bash', '-e', '-c', script], cwd=root,
                                        env={**os.environ, 'PATH': str(fake_bin)+os.pathsep+os.defpath},
                                        text=True, capture_output=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('Publish failed after 5 attempts', result.stderr)
                self.assertEqual((root/'git-calls.txt').read_text().count('push origin HEAD:main'), 5)
                recovery = job[job.index('- name: Preserve captured'):]
                self.assertIn('if: ${{ always() }}', recovery)
                self.assertIn('uses: actions/upload-artifact@v4', recovery)
                pattern = re.search(r'^          path: (.*)$', recovery, re.M).group(1)
                pattern = pattern.replace('${{ github.run_id }}', '1234')
                pattern = pattern.replace('${{ github.run_attempt }}', '1')
                selected = list(root.glob(pattern))
                self.assertEqual(len(selected), 1)
                self.assertEqual(selected[0].name, f'part{part}_chunk1_btc-updown-5m_quotes.csv')
                self.assertEqual(selected[0].read_text(), 'timestamp,bid,ask\n123,0.4,0.6\n')


class MonthlyGitPublishTests(unittest.TestCase):
    """Exercise publication against a real local remote with injected outages."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.remote = self.root / 'remote.git'
        self.repo = self.root / 'collector'
        self.real_git = shutil.which('git')
        self.git('init', '--bare', str(self.remote), cwd=self.root)
        self.git('init', '-b', 'main', str(self.repo), cwd=self.root)
        self.git('config', 'user.name', 'Test Collector')
        self.git('config', 'user.email', 'collector@example.test')
        (self.repo / 'capture.csv').write_text('ts,price\n0,100\n')
        self.git('add', 'capture.csv')
        self.git('commit', '-m', 'Initial capture')
        self.git('remote', 'add', 'origin', str(self.remote))
        self.git('push', '-u', 'origin', 'main')
        self.initial_sha = self.git('rev-parse', 'HEAD').stdout.strip()
        (self.repo / 'capture.csv').write_text('ts,price\n0,100\n1,101\n')
        self.git('commit', '-am', 'New captured chunk')
        self.capture_sha = self.git('rev-parse', 'HEAD').stdout.strip()
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        wrapper = self.bin / 'git'
        wrapper.write_text(f'#!{sys.executable}\n' + textwrap.dedent('''\
            import json, os, subprocess, sys
            from pathlib import Path
            path = Path(os.environ['GIT_TEST_STATE'])
            state = json.loads(path.read_text())
            args = sys.argv[1:]
            state['calls'].append(args)
            op = args[0]
            fail_key = op + '_failures'
            failures = state.get(fail_key, 0)
            if failures:
                if failures > 0:
                    state[fail_key] -= 1
                path.write_text(json.dumps(state))
                print('remote: Internal Server Error', file=sys.stderr)
                sys.exit(1)
            accepted_error = op == 'push' and state.pop('accepted_error', False)
            path.write_text(json.dumps(state))
            result = subprocess.run([os.environ['REAL_GIT'], *args])
            if accepted_error and result.returncode == 0:
                print('remote: response lost after accepting commit', file=sys.stderr)
                sys.exit(1)
            sys.exit(result.returncode)
            '''))
        wrapper.chmod(0o755)
        sleeper = self.bin / 'sleep'
        sleeper.write_text(f'#!{sys.executable}\n' + textwrap.dedent('''\
            import json, os, sys
            from pathlib import Path
            path = Path(os.environ['GIT_TEST_STATE'])
            state = json.loads(path.read_text())
            state['sleeps'].append(int(sys.argv[1]))
            path.write_text(json.dumps(state))
            '''))
        sleeper.chmod(0o755)

    def git(self, *args, cwd=None):
        return subprocess.run([self.real_git, *args], cwd=cwd or self.repo,
                              text=True, capture_output=True, check=True)

    def publish(self, **outage):
        state_path = self.root / 'state.json'
        state_path.write_text(json.dumps({'calls': [], 'sleeps': [], **outage}))
        result = subprocess.run(
            ['bash', str(Path(__file__).resolve().parents[1] / 'monthly_git_publish.sh')],
            cwd=self.repo, text=True, capture_output=True, timeout=15,
            env={**os.environ, 'PATH': str(self.bin) + os.pathsep + os.defpath,
                 'REAL_GIT': self.real_git, 'GIT_TEST_STATE': str(state_path)})
        return result, json.loads(state_path.read_text())

    def remote_head(self):
        return self.git('--git-dir', str(self.remote), 'rev-parse', 'main').stdout.strip()

    def test_service_push_outage_backs_off_then_publishes(self):
        result, state = self.publish(push_failures=2)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(state['sleeps'], [15, 30])
        self.assertEqual(sum(call[0] == 'push' for call in state['calls']), 3)
        self.assertEqual(self.remote_head(), self.capture_sha)

    def test_fetch_outage_does_not_push_without_current_remote_state(self):
        result, state = self.publish(fetch_failures=2)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(state['sleeps'], [15, 30])
        self.assertEqual([call[0] for call in state['calls'][:3]], ['fetch'] * 3)
        self.assertEqual(sum(call[0] == 'push' for call in state['calls']), 1)
        self.assertEqual(self.remote_head(), self.capture_sha)

    def test_accepted_commit_is_verified_after_response_failure(self):
        result, state = self.publish(accepted_error=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(state['sleeps'], [])
        self.assertEqual(sum(call[0] == 'push' for call in state['calls']), 1)
        self.assertEqual(self.remote_head(), self.capture_sha)
        self.assertIn('Verified captured commit', result.stdout)

    def test_persistent_outage_preserves_the_captured_commit(self):
        result, state = self.publish(push_failures=-1)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(state['sleeps'], [15, 30, 60, 120])
        self.assertEqual(sum(call[0] == 'push' for call in state['calls']), 5)
        self.assertEqual(self.remote_head(), self.initial_sha)
        self.assertEqual(self.git('rev-parse', 'HEAD').stdout.strip(), self.capture_sha)
        self.assertEqual((self.repo / 'capture.csv').read_text(), 'ts,price\n0,100\n1,101\n')

    def test_rebase_conflict_stops_before_any_push_and_keeps_capture(self):
        other = self.root / 'other'
        self.git('clone', '--branch', 'main', str(self.remote), str(other), cwd=self.root)
        self.git('config', 'user.name', 'Other Collector', cwd=other)
        self.git('config', 'user.email', 'other@example.test', cwd=other)
        (other / 'capture.csv').write_text('ts,price\n0,100\n1,999\n')
        self.git('commit', '-am', 'Conflicting remote capture', cwd=other)
        self.git('push', 'origin', 'main', cwd=other)
        result, state = self.publish()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(state['sleeps'], [])
        self.assertFalse(any(call[0] == 'push' for call in state['calls']))
        self.assertEqual(self.git('rev-parse', 'HEAD').stdout.strip(), self.capture_sha)
        self.assertEqual((self.repo / 'capture.csv').read_text(), 'ts,price\n0,100\n1,101\n')


if __name__ == '__main__':
    unittest.main()
