"""UTC daily completeness reports; empty seconds are never forward-filled."""
from __future__ import annotations
import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
import gzip
import json
from pathlib import Path
from archive_v2 import atomic_json


def longest_gap(seconds: set[int], start: int, end: int) -> int:
    points = [start - 1, *sorted(s for s in seconds if start <= s < end), end]
    return max((b - a - 1 for a, b in zip(points, points[1:])), default=end-start)


def summarize(rows, requested_day: str | None = None, assets=('btc',)) -> dict:
    groups = defaultdict(lambda: {'seconds': set(), 'rows': 0, 'valid_poly': 0,
                                 'valid_binance': 0, 'valid_chainlink': 0, 'lag_max_ms': 0})
    seen = set()
    duplicate_rows = 0
    for row in rows:
        ms = int(row['sample_ms'])
        day = datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m-%d')
        if requested_day and day != requested_day:
            continue
        key = (row['asset'], ms)
        if key in seen:
            duplicate_rows += 1
            continue
        seen.add(key)
        g = groups[(day, row['asset'])]
        g['rows'] += 1
        g['seconds'].add(ms // 1000)
        for source in ('poly', 'binance', 'chainlink'):
            g['valid_' + source] += int(bool(row.get(source + '_valid')))
        g['lag_max_ms'] = max(g['lag_max_ms'], row.get('sampler_lag_ms', 0))
    if requested_day:
        for asset in assets:
            groups[(requested_day, asset)]  # report zero coverage even with no files
    results = []
    for (day, asset), g in sorted(groups.items()):
        start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())
        seconds = g.pop('seconds')
        results.append(dict(date_utc=day, asset=asset, expected_seconds=86400,
                            observed_seconds=len(seconds), daily_coverage=len(seconds)/86400,
                            longest_gap_seconds=longest_gap(seconds, start, start+86400),
                            first_sample_ms=min(seconds)*1000 if seconds else None,
                            last_sample_ms=max(seconds)*1000 if seconds else None, **g))
    return dict(schema_version=2, duplicate_rows=duplicate_rows, daily=results,
                note='Coverage measures sampled seconds, not proof of complete exchange events. '
                     'A partial UTC day includes unobserved leading/trailing seconds. '
                     'Freshness is recorded separately from transport activity.')


def read_snapshots(root: Path):
    for path in sorted(root.rglob('snapshots-*.jsonl.gz')):
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in f:
                yield json.loads(line)


def report(root: Path, output: Path, day=None, assets=('btc',)) -> dict:
    result = summarize(read_snapshots(root), day, assets)
    atomic_json(output, result)
    text = ['# Capture v2 completeness', '', result['note'], '',
            '| UTC day | Asset | Observed / 86400 | Coverage | Longest gap (s) | Valid Poly / Binance / Chainlink |',
            '|---|---|---:|---:|---:|---:|']
    for x in result['daily']:
        text.append(f"| {x['date_utc']} | {x['asset']} | {x['observed_seconds']} | "
                    f"{x['daily_coverage']:.2%} | {x['longest_gap_seconds']} | "
                    f"{x['valid_poly']} / {x['valid_binance']} / {x['valid_chainlink']} |")
    output.with_suffix('.md').write_text('\n'.join(text) + '\n')
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--date')
    p.add_argument('--assets', default='btc')
    a = p.parse_args()
    if a.date:
        date.fromisoformat(a.date)
    report(a.root, a.output, a.date, a.assets.split(','))
