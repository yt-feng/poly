"""Replay the observed GitHub push failure without contacting a remote."""
import os
from pathlib import Path
import re
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
                self.assertIn('Push failed after 3 attempts', result.stdout)
                self.assertEqual((root/'git-calls.txt').read_text().count('push origin HEAD:main'), 3)
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


if __name__ == '__main__':
    unittest.main()
