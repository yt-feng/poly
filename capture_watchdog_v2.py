"""Bounded capture handoff plus an independent interruption watchdog.

Production and watchdog callers must hold the same Actions concurrency lock.
A workflow is draining only when its identified capture job has ended.
"""
from datetime import datetime, timedelta, timezone
import argparse
import json
import os
from functools import partial
from urllib.parse import quote

from archive_v2 import gh

ACTIVE = {'requested', 'waiting', 'pending', 'queued', 'in_progress'}
PRODUCTION_WORKFLOW = 'capture-v2.yml'
PRODUCTION_RESTART_LIMIT = 3


def _identity(run, repo, workflow, branch):
    if (run.get('path', '').split('@', 1)[0] != f'.github/workflows/{workflow}'
            or run.get('head_branch') != branch
            or run.get('repository', {}).get('full_name') != repo):
        raise RuntimeError('Run identity does not match the expected repository, workflow and branch')


def ensure_workflow(repo, *, workflow, branch, worker_job, current_run_id=None,
                    now=None, call=partial(gh, timeout=30), inputs=None,
                    restart_limit=PRODUCTION_RESTART_LIMIT, title_prefix=None):
    """Inspect, dispatch once, and verify the acknowledged run; never retry a POST."""
    now = now or datetime.now(timezone.utc)
    response = json.loads(call('api', f'repos/{repo}/actions/workflows/{workflow}/runs?branch={quote(branch, safe="")}&per_page=100'))
    runs = response['workflow_runs']
    if not isinstance(runs, list) or any(r.get('status') not in ACTIVE | {'completed'} for r in runs):
        raise ValueError('Invalid workflow inventory; refusing a blind dispatch')
    if title_prefix is not None:
        runs = [r for r in runs if r.get('display_title', '').startswith(title_prefix)]
    def worker_ended(run_id):
        current = json.loads(call('api', f'repos/{repo}/actions/runs/{run_id}'))
        _identity(current, repo, workflow, branch)
        if current.get('id') != run_id:
            raise RuntimeError('Current run response has a mismatched ID')
        if title_prefix is not None and not current.get('display_title', '').startswith(title_prefix):
            raise RuntimeError('Probe session does not match current run')
        jobs = json.loads(call('api', f'repos/{repo}/actions/runs/{run_id}/jobs?per_page=100'))['jobs']
        workers = [j for j in jobs if j.get('name') == worker_job]
        return len(workers) == 1 and workers[0].get('status') == 'completed'

    draining = []
    if current_run_id is not None:
        current_run_id = int(current_run_id)
        if current_run_id not in {r['id'] for r in runs}:
            raise RuntimeError('Current run is absent from inventory; refusing to ignore an unknown run')
        if not worker_ended(current_run_id):
            raise RuntimeError('Capture job is not terminal; refusing an overlapping successor')
        draining.append(current_run_id)
    active = []
    for run in runs:
        if run['status'] not in ACTIVE or run['id'] == current_run_id:
            continue
        # A shared concurrency group can replace a pending handoff with a newer
        # watchdog. That watchdog must finish the same handoff instead of treating
        # the already-ended worker as a live collector forever.
        if run['status'] == 'in_progress' and worker_ended(run['id']):
            draining.append(run['id'])
        else:
            active.append(run['id'])
    if active:
        return {'action': 'already_active', 'runs': active, 'draining_runs': draining}
    recent = [r for r in runs if datetime.fromisoformat(r['created_at'].replace('Z', '+00:00')) > now-timedelta(minutes=15)]
    if len(recent) >= restart_limit:
        raise RuntimeError(f'{restart_limit} captures started within 15 minutes; refusing a restart loop')
    args = ['api', f'repos/{repo}/actions/workflows/{workflow}/dispatches', '--method', 'POST',
            '-H', 'X-GitHub-Api-Version: 2026-03-10', '-f', f'ref={branch}', '-F', 'return_run_details=true']
    for name, value in (inputs or {}).items():
        args.extend(['-f', f'inputs[{name}]={value}'])
    result = json.loads(call(*args))
    successor_id = result['workflow_run_id']
    if type(successor_id) is not int or successor_id <= 0 or successor_id == current_run_id:
        raise RuntimeError('Dispatch did not acknowledge a distinct successor run')
    successor = json.loads(call('api', f'repos/{repo}/actions/runs/{successor_id}'))
    _identity(successor, repo, workflow, branch)
    if successor.get('id') != successor_id:
        raise RuntimeError('Successor response has a mismatched ID')
    if title_prefix is not None and not successor.get('display_title', '').startswith(title_prefix):
        raise RuntimeError('Acknowledged successor belongs to a different probe session')
    if successor.get('status') not in ACTIVE or successor.get('event') != 'workflow_dispatch':
        raise RuntimeError('Acknowledged successor is not active; inspect it before retrying')
    return {'action': 'dispatched', 'predecessor_run_id': current_run_id, 'draining_runs': draining,
            'successor_run_id': successor_id, 'successor_url': successor['html_url']}


def ensure_capture(repo, *, now=None, call=partial(gh, timeout=30), current_run_id=None):
    return ensure_workflow(repo, workflow=PRODUCTION_WORKFLOW, branch='main', worker_job='capture',
                           current_run_id=current_run_id, now=now, call=call,
                           restart_limit=PRODUCTION_RESTART_LIMIT)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--current-run-id', type=int)
    args = parser.parse_args()
    print(json.dumps(ensure_capture(os.environ['GH_REPO'], current_run_id=args.current_run_id)))
