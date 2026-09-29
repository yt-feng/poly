"""UTC completeness: distinct seconds, explicit gaps, never forward-fill.

Legacy valid_* fields remain row counts. Quality gates must use the new
valid_*_seconds fields. A second with an observed invalid flag is not a fully
valid second, even when another sample within that second was valid.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
import gzip
import json
from pathlib import Path
from archive_v2 import atomic_json

EXTRA = ('rules','fee_model','pair_quote','twap30','twap60','resolution_reference','spot_depth20','coinbase','coinbase_l2','fx','perp','perp_book','open_interest')
SOURCES = ('poly','binance','chainlink') + EXTRA


def gap_intervals(seconds: set[int], start: int, end: int) -> list[dict]:
    if type(start) is not int or type(end) is not int or end < start:
        raise ValueError('Invalid second interval')
    points = [start-1, *sorted(s for s in seconds if start <= s < end), end]
    return [dict(start_second=a+1, end_second_exclusive=b, duration_seconds=b-a-1)
            for a,b in zip(points,points[1:]) if b-a>1]


def longest_gap(seconds: set[int], start: int, end: int) -> int:
    return max((g['duration_seconds'] for g in gap_intervals(seconds,start,end)), default=0)


def _group():
    return {'seconds':set(), 'rows':0, 'valid_poly':0, 'valid_binance':0,
            'valid_chainlink':0, 'lag_max_ms':0, '_good':defaultdict(set),
            '_bad':defaultdict(set), '_refs':{}, '_archives':{}}


def summarize(rows, requested_day: str | None = None, assets=('btc',)) -> dict:
    if requested_day:
        date.fromisoformat(requested_day)
    groups = defaultdict(_group)
    seen = set()
    duplicate_rows = 0
    for row in rows:
        ms = int(row['sample_ms'])
        day = datetime.fromtimestamp(ms/1000,timezone.utc).strftime('%Y-%m-%d')
        if requested_day and day != requested_day:
            continue
        key = (row['asset'],ms)
        g = groups[(day,row['asset'])]
        second = ms//1000
        # Always observe validity, including conflicting retransmissions. An
        # invalid sample must not be hidden by first-row-wins de-duplication.
        for source in SOURCES:
            (g['_good'] if row.get(source+'_valid') is True else g['_bad'])[source].add(second)
        ref = row.get('_archive_ref')
        if isinstance(ref,str):
            g['_refs'].setdefault(second,ref)
            span = g['_archives'].setdefault(ref,dict(first_sample_ms=ms,last_sample_ms=ms))
            span['first_sample_ms']=min(span['first_sample_ms'],ms)
            span['last_sample_ms']=max(span['last_sample_ms'],ms)
        if key in seen:
            duplicate_rows += 1
            continue
        seen.add(key)
        g['rows'] += 1
        g['seconds'].add(second)
        for source in SOURCES:
            g['valid_'+source]=g.get('valid_'+source,0)+int(row.get(source+'_valid') is True)
        g['microstructure_rows']=g.get('microstructure_rows',0)+int('microstructure' in row)
        g['lag_max_ms']=max(g['lag_max_ms'],row.get('sampler_lag_ms',0))
    if requested_day:
        for asset in assets:
            groups[(requested_day,asset)]
    results=[]
    for (day,asset),g in sorted(groups.items()):
        start=int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())
        seconds=g.pop('seconds'); good=g.pop('_good'); bad=g.pop('_bad')
        refs=g.pop('_refs'); spans=g.pop('_archives')
        gaps=gap_intervals(seconds,start,start+86400)
        for gap in gaps:
            a,b=gap['start_second'],gap['end_second_exclusive']
            gap.update(start_utc=datetime.fromtimestamp(a,timezone.utc).isoformat(),
                end_utc_exclusive=datetime.fromtimestamp(b,timezone.utc).isoformat(),
                preceding_archive=refs.get(a-1), following_archive=refs.get(b),
                cause='UNDETERMINED_FROM_SNAPSHOTS')
        counts={'valid_'+s+'_seconds':len(good[s]-bad[s]) for s in SOURCES}
        results.append(dict(date_utc=day,asset=asset,expected_seconds=86400,
            observed_seconds=len(seconds),daily_coverage=len(seconds)/86400,
            longest_gap_seconds=max((x['duration_seconds'] for x in gaps),default=0),
            gap_count=len(gaps),missing_seconds=sum(x['duration_seconds'] for x in gaps),
            missing_intervals=gaps,archive_spans=spans,
            first_sample_ms=min(seconds)*1000 if seconds else None,
            last_sample_ms=max(seconds)*1000 if seconds else None,**g,**counts))
    return dict(schema_version=2,quality_contract_version=3,duplicate_rows=duplicate_rows,daily=results,
        validity_semantics='valid_* are legacy unique-timestamp row counts; valid_*_seconds count seconds with at least one true and no non-true observation.',
        note='Coverage measures sampled seconds, not complete exchange events. Missing intervals are not filled. '
             'Gap boundaries and adjacent archives are evidence, not a causal diagnosis. '
             'Leading/trailing unobserved seconds are included; freshness is separate from transport activity.')


def read_snapshots(root: Path):
    for path in sorted(root.rglob('snapshots-*.jsonl.gz')):
        with gzip.open(path,'rt',encoding='utf-8') as f:
            for line in f:
                row=json.loads(line)
                row['_archive_ref']=str(path.relative_to(root))
                yield row


def report(root: Path, output: Path, day=None, assets=('btc',)) -> dict:
    result=summarize(read_snapshots(root),day,assets)
    atomic_json(output,result)
    text=['# Capture v2 completeness','',result['note'],'',
          '| UTC day | Asset | Observed seconds | Coverage | Longest gap (s) | Fully valid seconds Poly / Binance / Chainlink |',
          '|---|---|---:|---:|---:|---:|']
    for x in result['daily']:
        text.append(f"| {x['date_utc']} | {x['asset']} | {x['observed_seconds']} | "
                    f"{x['daily_coverage']:.2%} | {x['longest_gap_seconds']} | "
                    f"{x['valid_poly_seconds']} / {x['valid_binance_seconds']} / {x['valid_chainlink_seconds']} |")
    text += ['','## Missing sampled-second intervals','',
             '| UTC day | Asset | Start UTC inclusive | End UTC exclusive | Seconds | Previous archive | Following archive |',
             '|---|---|---|---|---:|---|---|']
    for x in result['daily']:
        for gap in x['missing_intervals']:
            text.append(f"| {x['date_utc']} | {x['asset']} | {gap['start_utc']} | {gap['end_utc_exclusive']} | "
                        f"{gap['duration_seconds']} | {gap['preceding_archive'] or 'none'} | {gap['following_archive'] or 'none'} |")
    text += ['','## Microstructure sample validity','',
             '| UTC day | Asset | V3 rows | Rules / fees / resolution valid seconds | Coinbase / depth20 / futures valid seconds |',
             '|---|---|---:|---:|---:|']
    for x in result['daily']:
        text.append(f"| {x['date_utc']} | {x['asset']} | {x.get('microstructure_rows',0)} | "
                    f"{x['valid_rules_seconds']} / {x['valid_fee_model_seconds']} / {x['valid_resolution_reference_seconds']} | "
                    f"{x['valid_coinbase_seconds']} / {x['valid_spot_depth20_seconds']} / {x['valid_perp_seconds']} |")
    output.with_suffix('.md').write_text('\n'.join(text)+'\n',encoding='utf-8')
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--root',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--date')
    p.add_argument('--assets',default='btc')
    a=p.parse_args()
    report(a.root,a.output,a.date,a.assets.split(','))
