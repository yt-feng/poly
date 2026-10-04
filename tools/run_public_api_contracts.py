"""Run one allowlisted synthetic contract suite and emit a small status artifact."""
from __future__ import annotations
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
TEST_DIRECTORY = 'equity_daily/tests'


def deny_network(event, args):
    if event in ('socket.connect', 'socket.getaddrinfo', 'socket.gethostbyname'):
        raise RuntimeError('offline_contract_network_forbidden')


def run():
    sys.addaudithook(deny_network)
    sys.path.insert(0, str(ROOT))
    suite = unittest.defaultTestLoader.discover(
        str(ROOT / TEST_DIRECTORY), pattern='test_data_api_v2_contract.py')
    result = unittest.TextTestRunner(stream=io.StringIO(), verbosity=0).run(suite)
    commit = os.environ.get('GITHUB_SHA', '')
    run_id = os.environ.get('GITHUB_RUN_ID', '')
    status = {
        'schema_version': 1,
        'mode': 'offline_contract',
        'status': 'passed' if result.wasSuccessful() and result.testsRun and not result.skipped else 'failed',
        'tests_run': result.testsRun,
        'failures': len(result.failures),
        'errors': len(result.errors),
        'skipped': len(result.skipped),
        'live_api_verified': False,
        'live_trading_enabled': False,
        'network_access_enabled': False,
        'commit': commit if re.fullmatch(r'[0-9a-f]{40}', commit) else None,
        'workflow_run_id': run_id if run_id.isdigit() else None,
        'source_sha256': {
            name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
            for name in ('equity_daily/collector.py', 'equity_daily/tests/test_data_api_v2_contract.py',
                         'tools/run_public_api_contracts.py')
        },
    }
    target = ROOT / 'public-api-contract-status.json'
    temporary = target.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(status, sort_keys=True, indent=2) + '\n', encoding='utf-8')
    temporary.replace(target)
    print(json.dumps(status, sort_keys=True))
    return 0 if status['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(run())
