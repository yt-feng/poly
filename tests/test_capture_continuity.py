import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest
from unittest.mock import Mock

from capture_watchdog_v2 import ensure_capture, ensure_workflow, PRODUCTION_RESTART_LIMIT
from capture_continuity_probe import continue_probe


class Actions:
    def __init__(self):
        self.now = datetime(2026, 9, 28, 12, tzinfo=timezone.utc)
        self.runs = {}
        self.workers = {}
        self.posts = 0
        self.ack_change = {}
        self.add(1, age=240)

    def add(self, run_id, *, age=0, status='in_progress', conclusion=None):
        row = {'id': run_id, 'created_at': (self.now-timedelta(minutes=age)).isoformat(),
               'status': status, 'conclusion': conclusion, 'path': '.github/workflows/capture-v2.yml',
               'head_branch': 'main', 'repository': {'full_name': 'owner/repo'},
               'event': 'workflow_dispatch', 'html_url': f'https://example.test/{run_id}'}
        self.runs[run_id] = row
        self.workers[run_id] = [{'name': 'capture', 'status': 'completed', 'conclusion': conclusion or 'success'}]
        return row

    def __call__(self, *args):
        path = args[1]
        if '--method' in args:
            assert args[args.index('--method')+1] == 'POST'
            assert 'return_run_details=true' in args
            self.posts += 1
            run_id = max(self.runs)+1
            self.add(run_id, status='queued').update(self.ack_change)
            return json.dumps({'workflow_run_id': run_id})
        if '/workflows/' in path:
            return json.dumps({'workflow_runs': list(self.runs.values())})
        suffix = path.split('/runs/')[1]
        run_id = int(suffix.split('/')[0])
        if '/jobs?' in suffix:
            return json.dumps({'jobs': self.workers[run_id]})
        return json.dumps(self.runs[run_id])

    def handoff(self, current=1):
        return ensure_capture('owner/repo', current_run_id=current, now=self.now, call=self)

    def watchdog(self):
        return ensure_capture('owner/repo', now=self.now, call=self)


class ContinuityTests(unittest.TestCase):
    def test_six_production_handoffs_and_duplicate_events(self):
        api = Actions()
        for run_id in range(1, 7):
            # While the worker runs, neither handoff caller can replace it.
            api.workers[run_id][0]['status'] = 'in_progress'
            self.assertEqual(api.watchdog()['action'], 'already_active')
            api.workers[run_id][0]['status'] = 'completed'
            result = api.handoff(run_id)
            self.assertEqual(result['successor_run_id'], run_id+1)
            self.assertEqual(api.handoff(run_id)['action'], 'already_active')
            self.assertEqual(api.watchdog()['action'], 'already_active')
            api.runs[run_id]['status'] = 'completed'
            api.runs[run_id+1]['status'] = 'in_progress'
            api.now += timedelta(hours=4)
        self.assertEqual(api.posts, 6)

    def test_success_failure_cancel_and_timeout_can_handoff_only_after_worker_ends(self):
        for conclusion in ('success', 'failure', 'cancelled', 'timed_out'):
            with self.subTest(conclusion=conclusion):
                api = Actions()
                api.workers[1][0]['conclusion'] = conclusion
                self.assertEqual(api.handoff()['action'], 'dispatched')

    def test_worker_still_active_missing_or_ambiguous_is_not_ignored(self):
        for jobs in ([], [{'name': 'capture', 'status': 'in_progress'}],
                     [{'name': 'capture', 'status': 'completed'}]*2):
            api = Actions(); api.workers[1] = jobs
            with self.assertRaisesRegex(RuntimeError, 'not terminal'):
                api.handoff()
            self.assertEqual(api.posts, 0)

    def test_cannot_ignore_another_workflow_branch_or_repository(self):
        for mutation in ({'path': '.github/workflows/other.yml'}, {'head_branch': 'other'},
                         {'repository': {'full_name': 'another/repo'}}):
            api = Actions(); api.runs[1].update(mutation)
            with self.assertRaisesRegex(RuntimeError, 'identity'):
                api.handoff()
            self.assertEqual(api.posts, 0)

    def test_current_run_absent_from_inventory_fails_closed(self):
        api = Actions()
        def call(*args):
            if '/workflows/' in args[1]: return '{"workflow_runs": []}'
            return api(*args)
        with self.assertRaisesRegex(RuntimeError, 'absent'):
            ensure_capture('owner/repo', current_run_id=1, now=api.now, call=call)
        self.assertEqual(api.posts, 0)

    def test_pending_successor_prevents_self_and_watchdog_duplicates(self):
        for status in ('requested','waiting','pending','queued','in_progress'):
            api = Actions(); api.add(2, status=status)
            api.workers[2][0]['status'] = 'in_progress'
            self.assertEqual(api.handoff()['runs'], [2])
            self.assertEqual(api.watchdog()['action'], 'already_active')
            self.assertEqual(api.posts, 0)

    def test_new_watchdog_can_replace_pending_handoff_without_losing_succession(self):
        api = Actions()
        # Workflow still in_progress, but its capture job ended; a duplicate
        # watchdog replaced the pending handoff in GitHub's single pending slot.
        result = api.watchdog()
        self.assertEqual(result['draining_runs'], [1])
        self.assertEqual(result['successor_run_id'], 2)
        self.assertEqual(api.handoff()['action'], 'already_active')
        self.assertEqual(api.watchdog()['action'], 'already_active')
        self.assertEqual(api.posts, 1)

    def test_watchdog_recovers_cancelled_run_when_self_handoff_cannot_run(self):
        api = Actions(); api.runs[1].update(status='completed', conclusion='cancelled')
        self.assertEqual(api.watchdog()['successor_run_id'], 2)
        self.assertEqual(api.watchdog()['action'], 'already_active')
        self.assertEqual(api.posts, 1)

    def test_current_run_still_counts_in_restart_limit(self):
        api = Actions(); api.runs.clear()
        for n in range(1, 4): api.add(n, age=n, status='completed' if n<3 else 'in_progress')
        self.assertEqual(PRODUCTION_RESTART_LIMIT, 3)
        with self.assertRaisesRegex(RuntimeError, 'restart loop'):
            api.handoff(3)
        self.assertEqual(api.posts, 0)

    def test_unknown_inventory_state_fails_closed(self):
        api = Actions(); api.runs[1]['status']='unexpected'
        with self.assertRaises(ValueError): api.handoff()
        self.assertEqual(api.posts, 0)

    def test_dispatched_run_identity_and_state_must_be_acknowledged(self):
        for mutation in ({'head_branch':'other'}, {'path':'.github/workflows/other.yml'},
                         {'repository':{'full_name':'other/repo'}}, {'status':'completed'}, {'event':'push'}):
            api = Actions(); api.ack_change = mutation
            with self.assertRaises(RuntimeError): api.handoff()
            self.assertEqual(api.posts, 1)

    def test_pending_successor_display_title_is_not_its_dispatch_identity(self):
        api = Actions(); api.runs[1]['display_title'] = 'fixture-session step 1'
        api.ack_change = {'display_title': 'capture-continuity-probe'}
        result = ensure_workflow('owner/repo', workflow='capture-v2.yml', branch='main',
                                 worker_job='capture', current_run_id=1, now=api.now,
                                 call=api, title_prefix='fixture-session ')
        self.assertEqual(result['successor_run_id'], 2)
        self.assertEqual(api.posts, 1)

    def test_ambiguous_dispatch_is_not_retried(self):
        api = Actions(); posts=[]
        def call(*args):
            if '--method' in args:
                posts.append(args)
                raise TimeoutError('ambiguous dispatch response')
            return api(*args)
        with self.assertRaises(TimeoutError):
            ensure_capture('owner/repo', current_run_id=1, now=api.now, call=call)
        self.assertEqual(len(posts),1)

    def test_probe_is_finite_and_cannot_use_production_branch(self):
        scheduler=Mock(return_value={'action':'dispatched','successor_run_id':2})
        for hop in range(1,7):
            result=continue_probe('owner/repo','codex/probe','100',hop,hop,scheduler=scheduler)
            self.assertEqual(result['market_requests'],0)
        self.assertEqual(scheduler.call_count,5)
        self.assertTrue(result['chain_complete'])
        for args in [('main','100',1),('codex/probe','bad',1),('codex/probe','100',0),('codex/probe','100',7)]:
            with self.assertRaises(ValueError):
                continue_probe('owner/repo',*args,1,scheduler=scheduler)
        self.assertEqual(scheduler.call_count,5)
        self.assertTrue(all(c.kwargs['restart_limit']==8 for c in scheduler.call_args_list))

    def test_shared_dispatch_lock_and_collector_lock_are_preserved(self):
        root=Path(__file__).resolve().parents[1]
        production=(root/'.github/workflows/capture-v2.yml').read_text()
        watchdog=(root/'.github/workflows/capture-v2-watchdog.yml').read_text()
        for text in (production,watchdog):
            self.assertIn('group: capture-v2-continuation',text)
            self.assertNotIn('cancel-in-progress: true',text)
        self.assertIn('group: capture-v2-production',production)
        self.assertIn('needs: capture',production)
        self.assertIn('--current-run-id "$GITHUB_RUN_ID"',production)
        self.assertIn("'capture_runtime_v2.py'",production)
        self.assertIn('actions: write',production)
        self.assertIn('workflow_run:',watchdog)
        self.assertIn("cron: '23 * * * *'",watchdog)


if __name__ == '__main__':
    unittest.main()
