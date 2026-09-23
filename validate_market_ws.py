"""Read archived public data to verify current-token WS freshness across a rollover.
No strategy or order execution. A green test does not certify a lossless tape.
"""
import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path


def validate(root):
    root=Path(root);rows=[];counts=Counter();gaps=[];files=[]
    for p in sorted(root.glob('*.jsonl.gz')):
        digest=hashlib.sha256(p.read_bytes()).hexdigest()
        expected=p.with_name(p.name+'.sha256').read_text().split()[0]
        if digest!=expected:raise ValueError('Checksum mismatch: '+p.name)
        files.append({'file':p.name,'sha256':digest})
        with gzip.open(p,'rt') as f:
            for line in f:
                x=json.loads(line)
                if p.name.startswith('snapshots-'):rows.append(x)
                elif p.name.startswith('raw-'):
                    counts[x['source']]+=1
                    if x['source']=='polymarket_ws_gap':gaps.append(x)
    groups=defaultdict(list)
    for row in sorted(rows,key=lambda x:x['sample_ms']):groups[row['slug']].append(row)
    phases=[]
    for slug,items in groups.items():
        start=items[0]['sample_ms']
        eligible=[r for r in items if r['sample_ms']>=start+10000]
        valid=sum(bool(r.get('poly_ws_data_valid')) for r in eligible)
        phases.append({'slug':slug,'samples':len(items),'eligible_after_10s':len(eligible),
                       'fresh_current_token_samples':valid,
                       'fresh_fraction':valid/len(eligible) if eligible else None,
                       'max_guard_reconnects':max((r.get('poly_ws_health',{}).get('guard_reconnects',0) for r in items),default=0)})
    substantial=[p for p in phases if p['eligible_after_10s']>=15]
    passing=(len(substantial)>=2 and all(p['fresh_fraction']>=.80 for p in substantial)
             and counts['polymarket_ws']>=100)
    result={'data_only':True,'event_completeness_certified':False,'checksums_verified':len(files),
            'files':files,'samples':len(rows),'phases':phases,'source_counts':dict(counts),
            'guard_reconnect_events':gaps,'rollover_smoke_passed':passing,
            'note':'Two current-market phases, >=100 WS frames, >=80% recent book evidence after per-phase 10s warmup; not strategy approval or all-day availability.'}
    (root/'ws_validation.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--require',action='store_true');a=p.parse_args()
    r=validate(a.root);print(json.dumps({k:v for k,v in r.items() if k!='files'},indent=2))
    if a.require and not r['rollover_smoke_passed']:raise SystemExit('Current-token WS rollover test failed')
