"""Regression coverage for capture handoffs using GitHub-shaped API fixtures."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from equity_daily import continuity


REPO = 'example/poly'
WORKFLOW_ID = 3001
NOW = datetime(2026, 10, 5, 21, 0, tzinfo=timezone.utc)
WORKFLOW_PATH = '.github/workflows/equity-daily.yml'


def stamp(value):
    return value.isoformat().replace('+00:00', 'Z')


def run_fixture(run_id, *, status='in_progress', event='workflow_dispatch',
                created_at=None, **changes):
    result = {
        'id': run_id,
        'workflow_id': WORKFLOW_ID,
        'path': WORKFLOW_PATH,
        'head_branch': 'main',
        'repository': {'full_name': REPO},
        'status': status,
        'conclusion': 'success' if status == 'completed' else None,
        'event': event,
        'created_at': stamp(created_at or NOW - timedelta(hours=4)),
        'updated_at': stamp(NOW),
        'html_url': f'https://github.com/{REPO}/actions/runs/{run_id}',
    }
    result.update(changes)
    return result


def job_fixture(name='capture', status='in_progress', conclusion=None):
    return {'name': name, 'status': status, 'conclusion': conclusion}


class FakeGitHub:
    """Small in-memory server, including pages and side effects of dispatch."""

    def __init__(self, runs=(), jobs=None):
        self.runs = {row['id']: deepcopy(row) for row in runs}
        self.jobs = deepcopy(jobs or {})
        self.workflow = {'id': WORKFLOW_ID, 'path': WORKFLOW_PATH, 'state': 'active'}
        self.calls = []
        self.posts = []
        self.overrides = {}
        self.page_size = 100
        self.next_id = max([10000, *self.runs]) + 1
        self.now = NOW
        self.dispatch_error = None
        self.dispatch_result = None

    @staticmethod
    def pages(key, rows, size):
        return [{
            'total_count': len(rows), key: rows[start:start + size],
        } for start in range(0, max(1, len(rows)), size)]

    def __call__(self, args):
        self.calls.append(list(args))
        if not args or args[0] != 'api':
            raise AssertionError(f'Unexpected command: {args!r}')
        endpoint = args[1]
        method = args[args.index('--method') + 1] if '--method' in args else 'GET'
        if endpoint in self.overrides:
            result = self.overrides[endpoint]
            if isinstance(result, Exception):
                raise result
            return result if isinstance(result, str) else json.dumps(result)
        prefix = f'repos/{REPO}/actions'
        workflow_endpoint = f'{prefix}/workflows/{continuity.WORKFLOW}'
        if endpoint == workflow_endpoint and method == 'GET':
            return json.dumps(self.workflow)
        if endpoint == f'{workflow_endpoint}/dispatches' and method == 'POST':
            self.posts.append(list(args))
            if self.dispatch_error:
                raise self.dispatch_error
            successor_id = self.next_id
            self.next_id += 1
            self.runs[successor_id] = run_fixture(
                successor_id, status='queued', created_at=self.now)
            self.jobs[successor_id] = [job_fixture('test', 'queued')]
            return json.dumps(self.dispatch_result if self.dispatch_result is not None
                              else {'workflow_run_id': successor_id})
        parsed = urlsplit(endpoint)
        query = parse_qs(parsed.query)
        if parsed.path == f'{workflow_endpoint}/runs':
            if '--paginate' not in args or '--slurp' not in args:
                raise AssertionError('Run inventory must fetch all pages')
            rows = list(self.runs.values())
            if 'status' in query:
                rows = [r for r in rows if r['status'] == query['status'][0]]
            if 'created' in query:
                cutoff = datetime.fromisoformat(query['created'][0][2:].replace('Z', '+00:00'))
                rows = [r for r in rows
                        if datetime.fromisoformat(r['created_at'].replace('Z', '+00:00')) >= cutoff]
            rows.sort(key=lambda r: (r['created_at'], r['id']), reverse=True)
            return json.dumps(self.pages('workflow_runs', rows, self.page_size))
        run_prefix = f'{prefix}/runs/'
        if parsed.path.startswith(run_prefix):
            rest = parsed.path[len(run_prefix):].split('/')
            run_id = int(rest[0])
            if len(rest) == 1:
                return json.dumps(self.runs[run_id])
            if rest[1:] == ['jobs']:
                if '--paginate' not in args or '--slurp' not in args:
                    raise AssertionError('Job inventory must fetch all pages')
                return json.dumps(self.pages('jobs', self.jobs.get(run_id, []), self.page_size))
        raise AssertionError(f'Unexpected API call: {args!r}')


class ContinuityTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict(os.environ, {'EQUITY_CAPTURE_PAUSED': 'false'})
        env.start()
        self.addCleanup(env.stop)

    def ensure(self, server, current=None):
        return continuity.ensure_capture(
            REPO, current_run_id=current, now=server.now, call=server)

    def assert_closed(self, server, current=None):
        with self.assertRaises((RuntimeError, ValueError)):
            self.ensure(server, current)
        self.assertEqual(server.posts, [])

    def test_six_successive_handoffs_and_duplicate_watchdog(self):
        current = 501
        server = FakeGitHub([run_fixture(current)], {
            current: [job_fixture(status='completed', conclusion='success')],
        })
        for handoff in range(6):
            with self.subTest(handoff=handoff):
                result = self.ensure(server, current)
                self.assertEqual(result['action'], 'dispatched')
                self.assertEqual(result['predecessor_run_id'], current)
                successor = result['successor_run_id']
                self.assertEqual(len(server.posts), handoff + 1)
                # Another watchdog observes the pending successor while its
                # predecessor workflow is still publishing diagnostics.
                duplicate = self.ensure(server)
                self.assertEqual(duplicate['action'], 'already_active')
                self.assertIn(successor, duplicate['runs'])
                self.assertEqual(len(server.posts), handoff + 1)
                server.runs[current]['status'] = 'completed'
                server.runs[current]['conclusion'] = 'success'
                server.now += timedelta(hours=4)
                current = successor
                server.runs[current]['status'] = 'in_progress'
                server.jobs[current] = [job_fixture(status='completed', conclusion='success')]

    def test_every_active_status_prevents_duplicate_dispatch(self):
        self.assertTrue({'queued', 'in_progress', 'waiting', 'pending', 'requested'}
                        .issubset(continuity.ACTIVE))
        for status in sorted(continuity.ACTIVE):
            with self.subTest(status=status):
                server = FakeGitHub([run_fixture(601, status=status)])
                self.assertEqual(self.ensure(server)['action'], 'already_active')
                self.assertEqual(server.posts, [])

    def test_test_and_smoke_preparation_count_as_active(self):
        for job in ('test', 'smoke'):
            with self.subTest(job=job):
                server = FakeGitHub([run_fixture(610)], {610: [job_fixture(job)]})
                self.assertEqual(self.ensure(server)['action'], 'already_active')
                self.assertEqual(server.posts, [])

    def test_completed_capture_does_not_hide_missing_successor(self):
        for conclusion in ('success', 'failure', 'skipped', 'cancelled', 'timed_out'):
            with self.subTest(conclusion=conclusion):
                server = FakeGitHub([run_fixture(620)], {
                    620: [job_fixture(status='completed', conclusion=conclusion)],
                })
                result = self.ensure(server)
                self.assertEqual(result['action'], 'dispatched')
                self.assertIn(620, result['draining_runs'])
                self.assertEqual(len(server.posts), 1)

    def test_cancelled_workflow_watchdog_starts_replacement(self):
        server = FakeGitHub([run_fixture(630, status='completed', conclusion='cancelled')])
        self.assertEqual(self.ensure(server)['action'], 'dispatched')
        self.assertEqual(len(server.posts), 1)

    def test_current_run_must_have_completed_capture(self):
        for jobs in ([], [job_fixture('test')], [job_fixture()],
                     [job_fixture(status='completed')]):
            with self.subTest(jobs=jobs):
                server = FakeGitHub([run_fixture(640)], {640: jobs})
                self.assert_closed(server, 640)

    def test_foreign_run_identity_fails_closed(self):
        for change in (
            {'head_branch': 'feature'},
            {'repository': {'full_name': 'someone/another-repo'}},
            {'workflow_id': WORKFLOW_ID + 1},
            {'path': '.github/workflows/btc.yml'},
            {'id': 'not-a-run-id'},
        ):
            with self.subTest(change=change):
                server = FakeGitHub([run_fixture(650)])
                server.runs[650].update(change)
                self.assert_closed(server)

    def test_current_run_lookup_id_mismatch_fails_closed(self):
        server = FakeGitHub([run_fixture(651)], {
            651: [job_fixture(status='completed', conclusion='success')],
        })
        server.overrides[f'repos/{REPO}/actions/runs/651'] = run_fixture(652)
        self.assert_closed(server, 651)

    def test_workflow_identity_mismatch_fails_closed(self):
        for change in ({'id': None}, {'path': '.github/workflows/other.yml'}):
            with self.subTest(change=change):
                server = FakeGitHub()
                server.workflow.update(change)
                self.assert_closed(server)

    def test_malformed_and_truncated_run_inventories_fail_closed(self):
        endpoint = (f'repos/{REPO}/actions/workflows/{continuity.WORKFLOW}/runs'
                    f'?branch=main&status={sorted(continuity.ACTIVE)[0]}&per_page=100')
        for response in ('not JSON', {}, [], [{'total_count': 1, 'workflow_runs': []}],
                         [{'total_count': 0}], [{'total_count': '0', 'workflow_runs': []}],
                         [{'total_count': 1, 'workflow_runs': [None]}]):
            with self.subTest(response=response):
                server = FakeGitHub()
                server.overrides[endpoint] = response
                self.assert_closed(server)

    def test_active_capture_on_later_page_prevents_dispatch(self):
        runs = [run_fixture(700 + i) for i in range(101)]
        jobs = {r['id']: [job_fixture(status='completed', conclusion='success')]
                for r in runs}
        # Highest IDs are first; the oldest run is deliberately on page two.
        jobs[700] = [job_fixture()]
        server = FakeGitHub(runs, jobs)
        result = self.ensure(server)
        self.assertEqual(result['action'], 'already_active')
        self.assertIn(700, result['runs'])
        self.assertEqual(server.posts, [])

    def test_ambiguous_dispatch_is_attempted_once(self):
        for failure in (RuntimeError('connection closed after POST'),):
            server = FakeGitHub()
            server.dispatch_error = failure
            with self.assertRaises((RuntimeError, ValueError)):
                self.ensure(server)
            self.assertEqual(len(server.posts), 1)

    def test_dispatch_without_run_receipt_is_not_retried(self):
        for receipt in ({}, {'workflow_run_id': None}, {'workflow_run_id': 'invalid'}):
            with self.subTest(receipt=receipt):
                server = FakeGitHub()
                server.dispatch_result = receipt
                with self.assertRaises((RuntimeError, ValueError)):
                    self.ensure(server)
                self.assertEqual(len(server.posts), 1)

    def test_three_starts_within_fifteen_minutes_stop_loop(self):
        server = FakeGitHub([
            run_fixture(810 + i, status='completed', conclusion='failure',
                        created_at=NOW - timedelta(minutes=i * 5))
            for i in range(3)
        ])
        self.assert_closed(server)

    def test_old_starts_do_not_permanently_block_recovery(self):
        server = FakeGitHub([
            run_fixture(820 + i, status='completed', conclusion='failure',
                        created_at=NOW - timedelta(minutes=16 + i))
            for i in range(3)
        ])
        self.assertEqual(self.ensure(server)['action'], 'dispatched')
        self.assertEqual(len(server.posts), 1)

    def test_pause_blocks_all_network_calls(self):
        for value in ('true', 'TRUE', ' true '):
            with self.subTest(value=value):
                server = FakeGitHub()
                with patch.dict(os.environ, {'EQUITY_CAPTURE_PAUSED': value}):
                    self.assertEqual(self.ensure(server), {'action': 'paused'})
                self.assertEqual(server.calls, [])

    def test_ambiguous_accepted_post_is_found_by_next_watchdog(self):
        server = FakeGitHub()

        def lost_response(args):
            result = server(args)
            if '--method' in args:
                raise RuntimeError('POST accepted but its response was lost')
            return result

        with self.assertRaises(RuntimeError):
            continuity.ensure_capture(REPO, now=NOW, call=lost_response)
        self.assertEqual(len(server.posts), 1)
        self.assertEqual(self.ensure(server)['action'], 'already_active')
        self.assertEqual(len(server.posts), 1)

    def test_malformed_jobs_fail_closed(self):
        endpoint = f'repos/{REPO}/actions/runs/830/jobs?filter=latest&per_page=100'
        for response in (
            [],
            [{'total_count': 1, 'jobs': []}],
            [{'total_count': 1, 'jobs': [job_fixture(status='unknown')]}],
            [{'total_count': 2, 'jobs': [job_fixture(), job_fixture()]}],
        ):
            with self.subTest(response=response):
                server = FakeGitHub([run_fixture(830)])
                server.overrides[endpoint] = response
                self.assert_closed(server)

    def test_capture_job_on_later_page_is_inspected(self):
        jobs = [job_fixture(f'completed prerequisite {i}', 'completed', 'success')
                for i in range(100)]
        jobs.append(job_fixture(status='completed', conclusion='success'))
        server = FakeGitHub([run_fixture(840)], {840: jobs})
        result = self.ensure(server)
        self.assertEqual(result['action'], 'dispatched')
        self.assertIn(840, result['draining_runs'])

    def test_pull_request_run_does_not_prevent_production_recovery(self):
        server = FakeGitHub([run_fixture(850, event='pull_request')])
        self.assertEqual(self.ensure(server)['action'], 'dispatched')
        self.assertEqual(len(server.posts), 1)

    def test_missing_current_run_cannot_be_blindly_excluded(self):
        self.assert_closed(FakeGitHub(), 860)

    def test_successor_identity_failure_never_repeats_dispatch(self):
        for change in (
            {'head_branch': 'feature'},
            {'repository': {'full_name': 'wrong/repository'}},
            {'workflow_id': WORKFLOW_ID + 1},
            {'path': '.github/workflows/other.yml'},
            {'id': 12345},
            {'status': 'completed'},
            {'event': 'push'},
            {'html_url': 'https://github.com/wrong/repository/actions/runs/10001'},
        ):
            with self.subTest(change=change):
                server = FakeGitHub()
                successor = run_fixture(server.next_id, status='queued', created_at=NOW)
                successor.update(change)
                server.overrides[f'repos/{REPO}/actions/runs/{server.next_id}'] = successor
                with self.assertRaises((RuntimeError, ValueError)):
                    self.ensure(server)
                self.assertEqual(len(server.posts), 1)

    def test_unknown_workflow_state_fails_closed(self):
        server = FakeGitHub()
        server.workflow['state'] = 'unknown'
        self.assert_closed(server)

    def test_disabled_workflow_is_never_reenabled_or_dispatched(self):
        for state in ('disabled_manually', 'disabled_inactivity'):
            with self.subTest(state=state):
                server = FakeGitHub()
                server.workflow['state'] = state
                result = self.ensure(server)
                self.assertEqual(result['action'], 'workflow_disabled')
                self.assertEqual(server.posts, [])
                self.assertEqual(len(server.calls), 1)


if __name__ == '__main__':
    unittest.main()
