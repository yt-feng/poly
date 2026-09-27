"""Continue capture after completion; a schedule independently repairs interruption."""
from datetime import datetime, timedelta, timezone
import json
import os
from functools import partial

from archive_v2 import gh

ACTIVE = {'requested', 'waiting', 'pending', 'queued', 'in_progress'}


def ensure_capture(repo, *, now=None, call=partial(gh, timeout=60)):
    now = now or datetime.now(timezone.utc)
    # At most one production capture runs and one waits; 100 recent runs also
    # bound restart storms. A malformed/error response must fail closed.
    response = json.loads(call('api', f'repos/{repo}/actions/workflows/capture-v2.yml/runs?branch=main&per_page=100'))
    runs = response['workflow_runs']
    active = [r['id'] for r in runs if r['status'] in ACTIVE]
    if active:
        return {'action': 'already_active', 'runs': active}
    recent = [r for r in runs if datetime.fromisoformat(r['created_at'].replace('Z', '+00:00')) > now-timedelta(minutes=15)]
    if len(recent) >= 3:
        raise RuntimeError('Three captures started within 15 minutes; refusing a restart loop')
    call('workflow', 'run', 'capture-v2.yml', '--repo', repo, '--ref', 'main')
    return {'action': 'dispatched'}


if __name__ == '__main__':
    print(json.dumps(ensure_capture(os.environ['GH_REPO'])))
