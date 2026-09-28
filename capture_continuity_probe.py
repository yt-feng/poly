"""Six real Actions handoffs with no collector, market API, archive, or release.

The fixture uses the production scheduler and identity/active-run checks. Its
isolated workflow/session allows eight starts per 15 minutes, because six tiny
fixture jobs intentionally finish much faster than four-hour collectors.
Production keeps its unchanged three-start limit.
"""
import argparse
import json
import os

from capture_watchdog_v2 import ensure_workflow


def continue_probe(repo, branch, session, hop, run_id, *, scheduler=ensure_workflow):
    if not session.isdigit() or not 1 <= hop <= 6 or not branch or branch == 'main':
        raise ValueError('Probe requires an isolated branch, numeric session and hop 1..6')
    if hop == 6:
        return {'chain_complete': True, 'session': session, 'hop': hop, 'run_id': run_id,
                'market_requests': 0, 'publication_performed': False}
    result = scheduler(repo, workflow='capture-continuity-probe.yml', branch=branch,
                       worker_job='fixture', current_run_id=run_id, restart_limit=8,
                       title_prefix=f'continuity-probe {session} ',
                       inputs={'session': session, 'hop': str(hop+1)})
    if result['action'] != 'dispatched':
        raise RuntimeError('Probe successor already exists; inspect duplicate invocation')
    return {**result, 'session': session, 'hop': hop, 'market_requests': 0,
            'publication_performed': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session', required=True)
    parser.add_argument('--hop', type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(continue_probe(os.environ['GH_REPO'], os.environ['GITHUB_REF_NAME'],
                                   args.session, args.hop, int(os.environ['GITHUB_RUN_ID']))))
