"""Checksum-verified, resumable Binance official daily archive ingestion.

Default scope matches the existing repo: BTCUSDT since 2026-04-21. This is
not a claim that all Binance symbols or all historical datasets are complete.
"""
from __future__ import annotations
import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import time
import requests
from archive_v2 import atomic_json, ensure_release, gh, publish, sha256

BASE = 'https://data.binance.vision/data'
KINDS = ('trades', 'aggTrades', 'klines_1s', 'klines_1m')
STATE_TAG = 'binance-v2-backfill-state'


def archive_key(symbol: str, day: date, kind: str, market: str = 'spot') -> str:
    if not re.fullmatch(r'[A-Z0-9]{2,30}', symbol):
        raise ValueError('Invalid symbol')
    if market not in ('spot', 'futures/um', 'futures/cm'):
        raise ValueError('Unsupported archive market')
    if kind not in KINDS or (market != 'spot' and kind == 'klines_1s'):
        raise ValueError('Unsupported dataset/interval for this market')
    stamp = day.isoformat()
    if kind.startswith('klines_'):
        interval = kind.split('_', 1)[1]
        return f'{market}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{stamp}.zip'
    return f'{market}/daily/{kind}/{symbol}/{symbol}-{kind}-{stamp}.zip'


def download_verified(session, key: str, directory: Path, budget_bytes: int) -> tuple[Path, str, int]:
    """Stream to disk; a failed checksum never becomes a completed checkpoint."""
    url = BASE+'/'+key
    check = session.get(url+'.CHECKSUM', timeout=30)
    check.raise_for_status()
    expected = check.text.split()[0].lower()
    if not re.fullmatch('[0-9a-f]{64}', expected):
        raise ValueError('Invalid official SHA-256 checksum')
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / key.replace('/', '__')
    temp = path.with_suffix('.partial')
    digest, count = hashlib.sha256(), 0
    try:
        with session.get(url, stream=True, timeout=(10, 60)) as r:
            r.raise_for_status()
            if int(r.headers.get('Content-Length', 0)) > budget_bytes:
                raise OverflowError('Archive exceeds remaining byte budget')
            with temp.open('wb') as f:
                for chunk in r.iter_content(1024*1024):
                    count += len(chunk)
                    if count > budget_bytes:
                        raise OverflowError('Archive exceeds remaining byte budget')
                    digest.update(chunk)
                    f.write(chunk)
        if digest.hexdigest() != expected:
            raise ValueError('Official archive checksum mismatch')
        temp.replace(path)
        path.with_suffix('.zip.CHECKSUM').write_text(f'{expected}  {path.name}\n')
        return path, expected, count
    finally:
        temp.unlink(missing_ok=True)


def planned(symbols, start: date, end: date, kinds, market):
    # Keep yesterday current even while the older backlog is still being ingested.
    day = end
    while day >= start:
        for symbol in symbols:
            for kind in kinds:
                yield archive_key(symbol, day, kind, market), day
        day -= timedelta(days=1)


def load_state(directory: Path, publishing: bool):
    path = directory/'backfill-state.json'
    if publishing:
        ensure_release(STATE_TAG)
        assets = json.loads(gh('release', 'view', STATE_TAG, '--json', 'assets'))['assets']
        if any(x['name'] == path.name for x in assets):
            gh('release', 'download', STATE_TAG, '--pattern', path.name,
               '--dir', str(directory), '--clobber')
    state = json.loads(path.read_text()) if path.exists() else {'schema_version': 2, 'files': {}}
    if state.get('schema_version') != 2 or not isinstance(state.get('files'), dict):
        raise ValueError('Unsupported or corrupt checkpoint; refusing to reset it')
    return path, state


def run(args):
    args.output.mkdir(parents=True, exist_ok=True)
    state_path, state = load_state(args.output, args.publish)
    end = date.fromisoformat(args.end) if args.end else datetime.now(timezone.utc).date()-timedelta(days=1)
    start = date.fromisoformat(args.start)
    symbols = list(dict.fromkeys(args.symbols.upper().split(',')))
    kinds = list(dict.fromkeys(args.datasets.split(',')))
    if end < start or end >= datetime.now(timezone.utc).date():
        raise ValueError('Use a completed UTC date range')
    total = (end-start).days+1
    expected = total*len(symbols)*len(kinds)
    downloaded = attempts = used_bytes = 0
    checks = dict(target_files=expected, completed_files=0, unavailable_files=0, pending_files=0)
    session = requests.Session()
    session.headers['User-Agent'] = 'poly-capture-v2/2.0'
    try:
        for key, day in planned(symbols, start, end, kinds, args.market):
            existing = state['files'].get(key, {})
            if existing.get('status') == 'complete':
                if args.publish and existing.get('release'):
                    checks['completed_files'] += 1
                    continue
                local = args.output/key.replace('/', '__')
                if local.exists() and sha256(local) == existing['sha256']:
                    if args.publish:
                        tag = 'binance-v2-'+args.market.replace('/', '-')+'-'+day.strftime('%Y-%m')
                        publish(tag, [local, local.with_suffix('.zip.CHECKSUM')], replace=True)
                        existing['release'] = tag
                    checks['completed_files'] += 1
                    continue
            if existing.get('status') == 'unavailable' and time.time()-existing.get('checked_epoch', 0) < 21600:
                checks['unavailable_files'] += 1
                continue
            if downloaded >= args.max_files or attempts >= args.max_attempts or used_bytes >= args.max_bytes:
                checks['pending_files'] += 1
                continue
            attempts += 1
            try:
                path, digest, size = download_verified(session, key, args.output, args.max_bytes-used_bytes)
                used_bytes += size
                tag = None
                if args.publish:
                    tag = 'binance-v2-'+args.market.replace('/', '-')+'-'+day.strftime('%Y-%m')
                    publish(tag, [path, path.with_suffix('.zip.CHECKSUM')], replace=True)
                state['files'][key] = dict(status='complete', sha256=digest, bytes=size,
                    url=BASE+'/'+key, release=tag, checked_epoch=time.time(),
                    timestamp_unit='us' if args.market == 'spot' and day >= date(2025, 1, 1) else 'ms')
                downloaded += 1
                checks['completed_files'] += 1
                atomic_json(state_path, state)
                if args.publish:
                    publish(STATE_TAG, [state_path], replace=True)
                    path.unlink()
                    path.with_suffix('.zip.CHECKSUM').unlink()
            except requests.HTTPError as e:
                status = e.response.status_code
                if status == 404:
                    state['files'][key] = dict(status='unavailable', checked_epoch=time.time(), url=BASE+'/'+key)
                    checks['unavailable_files'] += 1
                else:
                    # 403/418/429/451: stop; do not cycle endpoints to evade restrictions.
                    raise
            except OverflowError:
                checks['pending_files'] += 1
                # A large trade archive should not block smaller kline archives.
                state['files'][key] = dict(status='over_budget', checked_epoch=time.time(), url=BASE+'/'+key)
            time.sleep(0.25)
    finally:
        session.close()
        atomic_json(state_path, state)
        if args.publish:
            publish(STATE_TAG, [state_path], replace=True)
    checks.update(schema_version=2, start=start.isoformat(), end=end.isoformat(), symbols=symbols,
                  datasets=kinds, market=args.market, downloaded_this_run=downloaded,
                  downloaded_bytes=used_bytes, all_requested_files_complete=checks['completed_files']==expected,
                  note='Unavailable and over-budget archives remain explicit gaps. Raw ZIP timestamps are preserved.')
    atomic_json(args.output/'backfill-summary.json', checks)
    if args.publish:
        publish(STATE_TAG, [args.output/'backfill-summary.json'], replace=True)
    print(json.dumps(checks, indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--start', default='2026-04-21')
    p.add_argument('--end')
    p.add_argument('--symbols', default='BTCUSDT')
    p.add_argument('--market', default='spot', choices=['spot', 'futures/um', 'futures/cm'])
    p.add_argument('--datasets', default=','.join(KINDS))
    p.add_argument('--max-files', type=int, default=24)
    p.add_argument('--max-attempts', type=int, default=48)
    p.add_argument('--max-bytes', type=int, default=1024*1024*1024)
    p.add_argument('--output', type=Path, default=Path('backfill_v2_output'))
    p.add_argument('--publish', action='store_true')
    args = p.parse_args()
    if min(args.max_files, args.max_attempts, args.max_bytes) <= 0:
        p.error('Budgets must be positive')
    run(args)
