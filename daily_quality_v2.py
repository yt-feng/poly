"""Rebuild a UTC-day report from durable, checksummed snapshot release assets."""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from archive_v2 import atomic_json, gh, publish, sha256
from quality_v2 import report


def main(args):
    day = args.date or (datetime.now(timezone.utc).date()-timedelta(days=1)).isoformat()
    begin = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    cutoff = begin-timedelta(days=1)
    releases, pages = [], 0
    for page in range(1, 101):
        batch = json.loads(gh('api', f"repos/{os.environ['GH_REPO']}/releases?per_page=100&page={page}"))
        pages += 1
        if not batch:
            break
        releases.extend(x for x in batch if x['tag_name'].startswith('capture-v2-'))
        oldest = min(datetime.fromisoformat(x['created_at'].replace('Z', '+00:00')) for x in batch)
        if oldest < cutoff:
            break
    else:
        raise RuntimeError('Release listing exceeded pagination bound; refusing an incomplete scan')
    assets = args.assets.split(',')
    args.output.mkdir(parents=True, exist_ok=True)
    downloaded = []
    for release in releases:
        names = {x['name'] for x in release.get('assets', [])}
        selected = sorted(n for n in names if n.startswith('snapshots-'+day+'-') and n.endswith('.jsonl.gz'))
        if not selected:
            continue
        target = args.output/release['tag_name']
        target.mkdir(exist_ok=True)
        for name in selected:
            if name+'.sha256' not in names:
                raise RuntimeError('Missing checksum for '+name)
            for filename in (name, name+'.sha256'):
                gh('release', 'download', release['tag_name'], '--pattern', filename,
                   '--dir', str(target), '--clobber')
            if sha256(target/name) != (target/(name+'.sha256')).read_text().split()[0]:
                raise ValueError('Snapshot checksum mismatch: '+name)
            downloaded.append(release['tag_name']+'/'+name)
    result = report(args.output, args.output/'quality.json', day, assets)
    result['release_assets_checked'] = downloaded
    result['release_pages_scanned'] = pages
    result['alert'] = any(x['daily_coverage'] < args.threshold or
                         x['valid_poly']/86400 < args.threshold or
                         x['valid_binance']/86400 < args.threshold for x in result['daily'])
    atomic_json(args.output/'quality.json', result)
    if args.publish:
        publish('quality-v2-'+day, [args.output/'quality.json', args.output/'quality.md'], replace=True)
    print(json.dumps(result, indent=2))
    if args.check and result['alert']:
        raise SystemExit('Daily data coverage below threshold; see the published quality report.')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--date')
    p.add_argument('--assets', default='btc')
    p.add_argument('--threshold', type=float, default=.95)
    p.add_argument('--output', type=Path, default=Path('daily_quality_output'))
    p.add_argument('--publish', action='store_true')
    p.add_argument('--check', action='store_true')
    args = p.parse_args()
    if not 0 <= args.threshold <= 1:
        p.error('Threshold must be between zero and one')
    main(args)
