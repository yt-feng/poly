"""Offline regressions for release-read failures and honest quality artifacts."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from archive_v2 import gh, ensure_release, error_details, safe_diagnostic
from daily_quality_v2 import main

DAY = '2026-01-01'
ENDPOINT = 'repos/o/r/releases?per_page=100&page=1'


def command_error(stderr='', command=None, code=1):
    return subprocess.CalledProcessError(code, command or ['gh', 'api', ENDPOINT], stderr=stderr)


def ok(stdout=''):
    return subprocess.CompletedProcess([], 0, stdout=stdout, stderr='')


class GhRecoveryTests(unittest.TestCase):
    def test_exact_unknown_exit_one_keeps_reason_without_retry(self):
        with patch('archive_v2.subprocess.run', side_effect=command_error('synthetic unknown CLI failure')) as run:
            with self.assertRaises(subprocess.CalledProcessError) as ctx:
                gh('api', ENDPOINT, timeout=30)
        self.assertEqual(run.call_count, 1)
        self.assertIn('synthetic unknown CLI failure', str(ctx.exception))
        self.assertEqual(error_details(ctx.exception)['returncode'], 1)
        self.assertFalse(error_details(ctx.exception)['retryable'])

    def test_bounded_same_command_transient_retries_and_final_diagnostic(self):
        args = ('api', ENDPOINT)
        with patch('archive_v2.subprocess.run', side_effect=command_error('HTTP 503: Service unavailable')) as run, patch('archive_v2.time.sleep') as sleep:
            with self.assertRaises(subprocess.CalledProcessError) as ctx:
                gh(*args, timeout=30)
        self.assertEqual([c.args[0] for c in run.call_args_list], [['gh', *args]] * 4)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [1, 2, 4])
        self.assertEqual(error_details(ctx.exception)['attempts'], 4)
        self.assertIn('HTTP 503', str(ctx.exception))

    def test_neighbor_read_consumers_recover_but_keep_identical_arguments(self):
        reads = [('api', ENDPOINT), ('api', 'repos/o/r/releases/11/assets?per_page=100&page=2'),
                 ('api', 'repos/o/r/actions/workflows/capture-v2.yml/runs?per_page=100'),
                 ('api', ENDPOINT, '--method', 'GET'),
                 ('release', 'view', 'capture-v2-11-1'),
                 ('release', 'view', 'backfill-v2', '--json', 'assets'),
                 ('release', 'download', 'capture-v2-11-1', '--pattern', 'snapshots-a.gz', '--dir', '/synthetic', '--clobber')]
        for args in reads:
            with self.subTest(args=args), patch('archive_v2.subprocess.run', side_effect=[command_error('TLS handshake timeout'), ok('[]')]) as run, patch('archive_v2.time.sleep'):
                self.assertEqual(gh(*args), '[]')
                self.assertEqual([c.args[0] for c in run.call_args_list], [['gh', *args]] * 2)
                self.assertEqual(run.call_args.kwargs['timeout'], 30)

    def test_permanent_and_unknown_failures_do_not_retry(self):
        for stderr in ['HTTP 401: Bad credentials', 'HTTP 403: rate limit exceeded',
                       'HTTP 404: Not Found', 'HTTP 422: invalid', 'unknown failure',
                       'permission denied', 'HTTP 403: TLS handshake timeout', '']:
            with self.subTest(stderr=stderr), patch('archive_v2.subprocess.run', side_effect=command_error(stderr)) as run, patch('archive_v2.time.sleep') as sleep:
                with self.assertRaises(subprocess.CalledProcessError):
                    gh('api', ENDPOINT)
                self.assertEqual(run.call_count, 1)
                sleep.assert_not_called()

    def test_writes_and_unknown_commands_never_replay_even_on_transient_errors(self):
        commands = [('api', ENDPOINT, '--method', 'POST'),
                    ('api', ENDPOINT, '-f', 'tag_name=x'),
                    ('api', ENDPOINT, '--input', '-'),
                    ('api', ENDPOINT, '--method=POST'),
                    ('api', 'graphql'), ('workflow', 'run', 'capture-v2.yml'),
                    ('release', 'create', 'capture-v2-1'),
                    ('release', 'upload', 'capture-v2-1', 'file', '--clobber'),
                    ('release', 'download', 'capture-v2-1', '--pattern', 'file', '--dir', 'out')]
        for args in commands:
            for failure in [command_error('HTTP 503'), subprocess.TimeoutExpired('gh', 30)]:
                with self.subTest(args=args, failure=type(failure).__name__), patch('archive_v2.subprocess.run', side_effect=failure) as run, patch('archive_v2.time.sleep') as sleep:
                    with self.assertRaises((subprocess.CalledProcessError, subprocess.TimeoutExpired)):
                        gh(*args)
                    self.assertEqual(run.call_count, 1)
                    sleep.assert_not_called()

    def test_timeout_is_bounded_and_preserves_timeout_type(self):
        with patch('archive_v2.subprocess.run', side_effect=subprocess.TimeoutExpired('gh', 30, stderr=b'synthetic timeout')) as run, patch('archive_v2.time.sleep'):
            with self.assertRaises(subprocess.TimeoutExpired) as ctx:
                gh('api', ENDPOINT, timeout=30)
        self.assertEqual(run.call_count, 4)
        self.assertEqual(error_details(ctx.exception)['timeout_seconds'], 30)
        self.assertEqual(error_details(ctx.exception)['stderr'], 'synthetic timeout')

    def test_shared_deadline_prevents_backoff_and_another_request(self):
        with patch('archive_v2.time.monotonic', side_effect=[99, 99.5]), patch('archive_v2.subprocess.run', side_effect=command_error('HTTP 503')) as run, patch('archive_v2.time.sleep') as sleep:
            with self.assertRaises(subprocess.CalledProcessError) as ctx:
                gh('api', ENDPOINT, timeout=30, deadline=100)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(run.call_args.kwargs['timeout'], 1)
        self.assertEqual(error_details(ctx.exception)['attempts'], 1)
        sleep.assert_not_called()

    def test_permanent_http_diagnostic_takes_priority_over_process_timeout(self):
        for status in (401,403,404,422):
            with self.subTest(status=status), patch('archive_v2.subprocess.run',side_effect=subprocess.TimeoutExpired('gh',30,stderr=f'HTTP {status}: permanent failure')) as run, patch('archive_v2.time.sleep') as sleep:
                with self.assertRaises(subprocess.TimeoutExpired) as ctx:
                    gh('api',ENDPOINT,timeout=30)
                self.assertEqual(run.call_count,1)
                self.assertFalse(error_details(ctx.exception)['retryable'])
                sleep.assert_not_called()

    def test_permanent_http_beyond_diagnostic_display_limit_still_vetoes_retry(self):
        stderr='HTTP 503: earlier failure\n'+'x'*2100+'\nHTTP 403: Forbidden'
        for failure in (command_error(stderr),subprocess.TimeoutExpired('gh',30,stderr=stderr.encode())):
            with self.subTest(failure=type(failure).__name__), patch('archive_v2.subprocess.run',side_effect=failure) as run, patch('archive_v2.time.sleep') as sleep:
                with self.assertRaises((subprocess.CalledProcessError,subprocess.TimeoutExpired)) as ctx:
                    gh('api',ENDPOINT,timeout=30)
                self.assertEqual(run.call_count,1)
                self.assertFalse(error_details(ctx.exception)['retryable'])
                self.assertLessEqual(len(ctx.exception.stderr),2012)
                sleep.assert_not_called()

    def test_release_create_only_after_explicit_not_found_and_never_retried(self):
        with patch('archive_v2.subprocess.run', side_effect=[command_error('release not found'), command_error('HTTP 503')]) as run, patch('archive_v2.time.sleep') as sleep:
            with self.assertRaises(subprocess.CalledProcessError):
                ensure_release('synthetic', timeout=30)
        self.assertEqual([c.args[0][1:3] for c in run.call_args_list], [['release', 'view'], ['release', 'create']])
        sleep.assert_not_called()
        for stderr in ['HTTP 403', 'HTTP 404', 'HTTP 503', 'unknown']:
            with self.subTest(stderr=stderr), patch('archive_v2.subprocess.run', side_effect=command_error(stderr)) as run, patch('archive_v2.time.sleep'):
                with self.assertRaises(subprocess.CalledProcessError):
                    ensure_release('synthetic')
                self.assertTrue(all(c.args[0][1:3] == ['release', 'view'] for c in run.call_args_list))

    def test_redaction_and_bounding_apply_to_exception_and_structured_details(self):
        self.assertEqual(safe_diagnostic(0), '0')
        stderr = ('Bearer ghp_syntheticsecret\nAuthorization: Basic arbitrary-secret\n'
                  'https://user:password@example.invalid/x?access_token=querysecret&x=1\n'
                  'custom-secret\n' + 'x' * 5000)
        with patch.dict(os.environ, {'GH_TOKEN': 'custom-secret'}), patch('archive_v2.subprocess.run', side_effect=command_error(stderr)):
            with self.assertRaises(subprocess.CalledProcessError) as ctx:
                gh('api', ENDPOINT)
            data = json.dumps(error_details(ctx.exception)) + str(ctx.exception)
        for secret in ['ghp_syntheticsecret', 'arbitrary-secret', 'user:password', 'querysecret', 'custom-secret']:
            self.assertNotIn(secret, data)
        self.assertLessEqual(len(ctx.exception.stderr), 2012)
        self.assertIn('[truncated]', ctx.exception.stderr)


class RebuildRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'output'
        self.args = SimpleNamespace(date=DAY, assets='btc', output=self.root, threshold=.95,
                                    publish=False, check=False, quiet=True)
        self.name = f'snapshots-{DAY}-000001.jsonl.gz'
        row = dict(asset='btc', sample_ms=1767225600000, poly_valid=True, binance_valid=True)
        self.data = gzip.compress((json.dumps(row) + '\n').encode())
        self.digest = hashlib.sha256(self.data).hexdigest()
        self.release = dict(id=11, tag_name='capture-v2-11-1', created_at='2025-12-30T00:00:00Z')
        self.inventory = [dict(id=12, name=self.name, size=len(self.data), digest='sha256:' + self.digest),
                          dict(id=13, name=self.name + '.sha256', size=100)]
        self.env = patch.dict(os.environ, {'GH_REPO': 'o/r', 'GITHUB_RUN_ID': '36857540140',
                                          'GITHUB_RUN_ATTEMPT': '1', 'GITHUB_SHA': '35e6f14d'})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.sleep = patch('archive_v2.time.sleep')
        self.sleep.start()
        self.addCleanup(self.sleep.stop)

    def cli(self, cmd, **kwargs):
        args = cmd[1:]
        if args[0] == 'api':
            return ok(json.dumps(self.inventory if '/assets?' in args[1] else [self.release]))
        if args[:2] == ['release', 'download']:
            target = Path(args[args.index('--dir') + 1])
            name = args[args.index('--pattern') + 1]
            (target / name).write_bytes(self.data if name == self.name else (self.digest + '  ' + self.name + '\n').encode())
            return ok()
        return ok()

    def failure(self, stage, complete=False):
        data = json.loads((self.root / 'failure.json').read_text())
        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['report_kind'], 'daily_quality_failure')
        self.assertEqual(data['stage'], stage)
        self.assertEqual(data['quality_report_complete'], complete)
        self.assertEqual(data['run_id'], '36857540140')
        self.assertNotIn('daily', data)
        self.assertTrue((self.root / 'failure.md').exists())
        if not complete:
            self.assertFalse((self.root / 'quality.json').exists())
            self.assertFalse((self.root / 'quality.md').exists())
        return data

    def test_exact_first_page_exit_one_creates_failure_artifact_before_quality(self):
        with patch('archive_v2.subprocess.run', side_effect=command_error('synthetic CLI error')) as run, patch('daily_quality_v2.publish') as publish:
            with self.assertRaises(subprocess.CalledProcessError):
                main(self.args)
        error = self.failure('release_inventory')['error']
        self.assertEqual(error['returncode'], 1)
        self.assertEqual(error['attempts'], 1)
        self.assertEqual(error['stderr'], 'synthetic CLI error')
        self.assertEqual(run.call_count, 1)
        publish.assert_not_called()
        self.assertEqual(json.loads((self.root/'failure.json').read_text())['context'], dict(page='1', endpoint=ENDPOINT))

    def test_timeout_artifact_is_not_fabricated_coverage(self):
        with patch('archive_v2.subprocess.run', side_effect=subprocess.TimeoutExpired('gh', 30)):
            with self.assertRaises(subprocess.TimeoutExpired):
                main(self.args)
        self.assertEqual(self.failure('release_inventory')['error']['attempts'], 4)

    def test_bad_json_from_successful_cli_fails_without_blind_retry(self):
        with patch('archive_v2.subprocess.run', return_value=ok('[{"id":')) as run:
            with self.assertRaises(json.JSONDecodeError):
                main(self.args)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(self.failure('release_inventory')['error']['type'], 'JSONDecodeError')

    def test_release_pagination_nonadvance_and_malformed_pages_fail(self):
        for response in [{}, [dict(id=True)], [self.release] * 101, [dict(self.release, created_at='2026-01-01T00:00:00Z')]]:
            with self.subTest(response=str(response)[:100]), tempfile.TemporaryDirectory() as d:
                self.root = Path(d)/'out'; self.args.output = self.root
                with patch('archive_v2.subprocess.run', return_value=ok(json.dumps(response))):
                    with self.assertRaises((ValueError, RuntimeError)):
                        main(self.args)
                self.failure('release_inventory')

    def test_asset_second_page_failure_retains_page_and_release_context(self):
        full_page = self.inventory + [dict(id=i, name=f'unrelated-{i}') for i in range(100,198)]
        def cli(cmd, **kwargs):
            if '/assets?' in cmd[2]:
                if 'page=2' in cmd[2]: raise command_error('HTTP 403: Forbidden', cmd)
                return ok(json.dumps(full_page))
            return self.cli(cmd, **kwargs)
        with patch('archive_v2.subprocess.run', side_effect=cli):
            with self.assertRaises(subprocess.CalledProcessError): main(self.args)
        data = self.failure('asset_inventory')
        self.assertEqual(data['context']['page'], '2')
        self.assertEqual(data['context']['release'], 'capture-v2-11-1')

    def test_partial_download_failure_keeps_evidence_but_never_calls_report(self):
        def cli(cmd, **kwargs):
            if cmd[1:3] == ['release','download']:
                target = Path(cmd[cmd.index('--dir')+1])
                (target/self.name).write_bytes(b'partial')
                raise command_error('unknown download error', cmd)
            return self.cli(cmd, **kwargs)
        with patch('archive_v2.subprocess.run', side_effect=cli), patch('daily_quality_v2.report') as report:
            with self.assertRaises(subprocess.CalledProcessError): main(self.args)
        self.failure('archive_download')
        report.assert_not_called()
        self.assertEqual((self.root/'capture-v2-11-1'/self.name).read_bytes(), b'partial')

    def test_sha_size_and_digest_fail_closed_with_archive_context(self):
        for field in ['checksum', 'size', 'digest']:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as d:
                self.root=Path(d)/'out'; self.args.output=self.root
                self.inventory[0].update(size=len(self.data), digest='sha256:'+hashlib.sha256(self.data).hexdigest())
                self.digest=hashlib.sha256(self.data).hexdigest()
                if field=='checksum': self.digest='0'*64
                elif field=='size': self.inventory[0]['size']+=1
                else: self.inventory[0]['digest']='sha256:'+'0'*64
                with patch('archive_v2.subprocess.run', side_effect=self.cli), patch('daily_quality_v2.publish') as publish:
                    with self.assertRaises(ValueError): main(self.args)
                self.assertEqual(self.failure('archive_verification')['context']['archive'], self.name)
                publish.assert_not_called()

    def test_invalid_snapshot_json_fails_calculation_after_integrity_checks(self):
        self.data=gzip.compress(b'{invalid\n'); self.digest=hashlib.sha256(self.data).hexdigest()
        self.inventory[0].update(size=len(self.data), digest='sha256:'+self.digest)
        with patch('archive_v2.subprocess.run', side_effect=self.cli):
            with self.assertRaises(json.JSONDecodeError): main(self.args)
        self.failure('quality_calculation')

    def test_invalid_threshold_result_never_promoted_from_staging(self):
        self.args.threshold=float('nan')
        with patch('archive_v2.subprocess.run', side_effect=self.cli):
            with self.assertRaises(ValueError): main(self.args)
        self.failure('threshold_validation')

    def test_publish_failure_is_not_reported_as_success_or_auto_replayed(self):
        self.args.publish=True
        def cli(cmd, **kwargs):
            if cmd[1:3] == ['release','upload']: raise command_error('HTTP 503: ambiguous upload', cmd)
            return self.cli(cmd, **kwargs)
        with patch('archive_v2.subprocess.run', side_effect=cli) as run:
            with self.assertRaises(subprocess.CalledProcessError): main(self.args)
        data=self.failure('publication', complete=True)
        self.assertEqual(data['publication_status'], 'failed_or_partial')
        self.assertEqual(sum(c.args[0][1:3]==['release','upload'] for c in run.call_args_list), 1)
        self.assertEqual(json.loads((self.root/'quality.json').read_text())['daily'][0]['observed_seconds'], 1)

    def test_either_report_promotion_failure_leaves_no_partial_top_level_report(self):
        original_replace=Path.replace
        for name in ('quality.json','quality.md'):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as d:
                self.root=Path(d)/'out'; self.args.output=self.root
                def replace(path,target):
                    if Path(target)==self.root/name:
                        raise OSError('synthetic report promotion failure')
                    return original_replace(path,target)
                with patch('archive_v2.subprocess.run',side_effect=self.cli), patch.object(Path,'replace',replace), patch('daily_quality_v2.publish') as publish:
                    with self.assertRaisesRegex(OSError,'promotion failure'):main(self.args)
                self.failure('report_write')
                publish.assert_not_called()

    def test_computed_coverage_failure_remains_real_quality_and_fails_check(self):
        self.args.check=True; self.args.publish=True
        with patch('archive_v2.subprocess.run', side_effect=self.cli), patch('daily_quality_v2.publish') as publish:
            with self.assertRaisesRegex(SystemExit, 'coverage below threshold'): main(self.args)
        self.assertEqual(self.failure('coverage_check', complete=True)['publication_status'], 'complete')
        publish.assert_called_once()
        data=json.loads((self.root/'quality.json').read_text())
        self.assertTrue(data['alert']); self.assertEqual(data['threshold'], .95)
        self.assertEqual(data['daily'][0]['expected_seconds'], 86400)
        self.assertEqual(data['daily'][0]['observed_seconds'], 1)

    def test_full_day_success_still_measures_every_second_and_has_no_failure_artifact(self):
        self.args.check=True; self.args.publish=True
        self.data=gzip.compress(('\n'.join(json.dumps(dict(asset='btc', sample_ms=(1767225600+i)*1000,
                  poly_valid=True, binance_valid=True)) for i in range(86400))+'\n').encode())
        self.digest=hashlib.sha256(self.data).hexdigest()
        self.inventory[0].update(size=len(self.data), digest='sha256:'+self.digest)
        with patch('archive_v2.subprocess.run', side_effect=self.cli), patch('daily_quality_v2.publish') as publish:
            result=main(self.args)
        self.assertFalse(result['alert']); self.assertEqual(result['daily'][0]['observed_seconds'],86400)
        self.assertEqual(result['daily'][0]['valid_poly_seconds'],86400)
        self.assertFalse(result['full_history_complete'])
        self.assertFalse((self.root/'failure.json').exists())
        self.assertEqual(json.loads((self.root/'quality.json').read_text()),result)
        publish.assert_called_once()

    def test_stale_output_is_rejected_before_any_request_and_left_intact(self):
        self.root.mkdir(); (self.root/'quality.json').write_text('previous evidence')
        with patch('archive_v2.subprocess.run') as run:
            with self.assertRaisesRegex(ValueError,'empty output'): main(self.args)
        run.assert_not_called()
        self.assertEqual((self.root/'quality.json').read_text(),'previous evidence')
        self.assertFalse((self.root/'failure.json').exists())


if __name__ == '__main__':
    unittest.main()
