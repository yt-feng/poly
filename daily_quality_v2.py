"""Rebuild a UTC-day report using explicit asset pagination and SHA checks.

Only acquisition and sampled-second quality are evaluated. No strategy or
account is accessed. Historical missing seconds remain missing after a repair.
"""
from __future__ import annotations
import argparse
from datetime import datetime,timedelta,timezone,date
import json
import math
import os
from pathlib import Path
import re
from archive_v2 import atomic_json,gh,publish,sha256
from quality_v2 import report


def list_release_assets(repo,release_id,*,call=None,per_page=100,max_pages=200):
    if (not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repo)
            or type(release_id) is not int or release_id<=0
            or type(per_page) is not int or not 1<=per_page<=100
            or type(max_pages) is not int or not 1<=max_pages<=200):
        raise ValueError('Invalid bounded asset inventory request')
    call=call or gh
    items={}; names={}; pages=0
    for page in range(1,max_pages+1):
        batch=json.loads(call('api',f'repos/{repo}/releases/{release_id}/assets?per_page={per_page}&page={page}',timeout=30))
        pages+=1
        if not isinstance(batch,list) or len(batch)>per_page:
            raise ValueError('Invalid asset page')
        added=0
        for item in batch:
            if not isinstance(item,dict) or type(item.get('id')) is not int or item['id']<=0 or not isinstance(item.get('name'),str):
                raise ValueError('Invalid asset identity')
            ident=item['id']; name=item['name']
            stable={k:item.get(k) for k in ('id','name','size','digest','updated_at')}
            if ident in items:
                if {k:items[ident].get(k) for k in stable}!=stable:
                    raise ValueError('Asset changed during pagination')
                continue
            if name in names and names[name]!=ident:
                raise ValueError('Conflicting asset name')
            items[ident]=item; names[name]=ident; added+=1
        if len(batch)<per_page:
            return list(items.values()),pages
        if not added:
            raise RuntimeError('Asset pagination did not advance')
    raise RuntimeError('Asset pagination limit reached; refusing incomplete inventory')


def apply_thresholds(result,threshold):
    if isinstance(threshold,bool) or not isinstance(threshold,(int,float)) or not math.isfinite(threshold) or not 0<=threshold<=1:
        raise ValueError('Threshold must be finite and between zero and one')
    if not isinstance(result.get('daily'),list) or not result['daily']:
        raise ValueError('Daily quality observations required')
    failed=[]
    for row in result['daily']:
        expected=row.get('expected_seconds')
        if expected!=86400:
            raise ValueError('Full UTC-day denominator required')
        for field,code in (('observed_seconds','OBSERVED_SECONDS_BELOW_THRESHOLD'),
                           ('valid_poly_seconds','POLY_VALID_SECONDS_BELOW_THRESHOLD'),
                           ('valid_binance_seconds','BINANCE_VALID_SECONDS_BELOW_THRESHOLD')):
            n=row.get(field)
            if type(n) is not int or not 0<=n<=expected:
                raise ValueError('Missing or invalid distinct-second coverage')
            if n/expected<threshold:
                failed.append(dict(date_utc=row['date_utc'],asset=row['asset'],reason=code,observed=n,expected=expected))
    result['threshold']=threshold
    result['alert_reasons']=failed
    result['alert']=bool(failed)
    return result


def main(args):
    day=args.date or (datetime.now(timezone.utc).date()-timedelta(days=1)).isoformat()
    date.fromisoformat(day)
    begin=datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    cutoff=begin-timedelta(days=1)
    repo=os.environ['GH_REPO']
    releases=[]; pages=0
    for page in range(1,101):
        batch=json.loads(gh('api',f'repos/{repo}/releases?per_page=100&page={page}',timeout=30))
        pages+=1
        if not isinstance(batch,list):raise ValueError('Invalid release page')
        if not batch:break
        releases.extend(x for x in batch if x['tag_name'].startswith('capture-v2-'))
        oldest=min(datetime.fromisoformat(x['created_at'].replace('Z','+00:00')) for x in batch)
        if oldest<cutoff:break
    else:
        raise RuntimeError('Release listing exceeded pagination bound')
    assets=args.assets.split(',')
    args.output=Path(args.output)
    if args.output.exists() and any(args.output.iterdir()):
        raise ValueError('New empty output directory required; stale files cannot enter a rebuild')
    args.output.mkdir(parents=True,exist_ok=True)
    downloaded=[]; inventory=[]
    for release in releases:
        tag=release['tag_name']
        if not re.fullmatch(r'capture-v2-[A-Za-z0-9_-]+',tag):raise ValueError('Invalid release tag')
        listing,count=list_release_assets(repo,release['id'])
        by_name={x['name']:x for x in listing}
        selected=sorted(n for n in by_name if n.startswith('snapshots-'+day+'-') and n.endswith('.jsonl.gz'))
        inventory.append(dict(release=tag,release_id=release['id'],asset_pages=count,listed_assets=len(listing),selected_archives=len(selected)))
        if not selected:continue
        target=args.output/tag; target.mkdir(exist_ok=True)
        for name in selected:
            if not re.fullmatch(r'snapshots-\d{4}-\d{2}-\d{2}-\d+\.jsonl\.gz',name):raise ValueError('Invalid archive name')
            if name+'.sha256' not in by_name:raise RuntimeError('Missing checksum for selected archive')
            for filename in (name,name+'.sha256'):
                gh('release','download',tag,'--pattern',filename,'--dir',str(target),'--clobber',timeout=90)
            check=(target/(name+'.sha256')).read_text().split()
            if not check or not re.fullmatch('[0-9a-fA-F]{64}',check[0]):raise ValueError('Invalid checksum syntax')
            if len(check)>2 or (len(check)==2 and check[1].lstrip('*')!=name):raise ValueError('Checksum filename mismatch')
            digest=sha256(target/name)
            if digest!=check[0].lower():raise ValueError('Snapshot checksum mismatch')
            api_digest=by_name[name].get('digest')
            if api_digest is not None and api_digest!='sha256:'+digest:raise ValueError('Asset digest conflict')
            size=by_name[name].get('size')
            if type(size) is not int or size!=(target/name).stat().st_size:raise ValueError('Asset byte-size mismatch')
            downloaded.append(tag+'/'+name)
    result=report(args.output,args.output/'quality.json',day,assets)
    result.update(release_assets_checked=downloaded,release_pages_scanned=pages,
                  release_asset_inventory=inventory,asset_pagination=True,
                  release_discovery_cutoff_utc=cutoff.isoformat(),
                  full_history_complete=False,
                  collection_scope='All explicitly enumerated selected-day archives in the bounded release discovery window; not exchange-event completeness.')
    apply_thresholds(result,args.threshold)
    atomic_json(args.output/'quality.json',result)
    if args.publish:
        publish('quality-v2-'+day,[args.output/'quality.json',args.output/'quality.md'],replace=True,timeout=90)
    if not getattr(args,'quiet',False):print(json.dumps(result,indent=2))
    if args.check and result['alert']:
        raise SystemExit('Daily data coverage below threshold; see the quality report.')
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--date');p.add_argument('--assets',default='btc')
    p.add_argument('--threshold',type=float,default=.95)
    p.add_argument('--output',type=Path,default=Path('daily_quality_output'))
    p.add_argument('--publish',action='store_true');p.add_argument('--check',action='store_true')
    p.add_argument('--quiet',action='store_true')
    args=p.parse_args()
    if not math.isfinite(args.threshold) or not 0<=args.threshold<=1:p.error('Invalid threshold')
    main(args)
