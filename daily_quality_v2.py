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
import sys
from archive_v2 import atomic_json,gh,publish,sha256,error_details,safe_diagnostic
from quality_v2 import report


def list_release_assets(repo,release_id,*,call=None,per_page=100,max_pages=200,on_page=None):
    if (not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repo)
            or type(release_id) is not int or release_id<=0
            or type(per_page) is not int or not 1<=per_page<=100
            or type(max_pages) is not int or not 1<=max_pages<=200):
        raise ValueError('Invalid bounded asset inventory request')
    call=call or gh
    items={}; names={}; pages=0
    for page in range(1,max_pages+1):
        endpoint=f'repos/{repo}/releases/{release_id}/assets?per_page={per_page}&page={page}'
        if on_page is not None:on_page(page,endpoint)
        batch=json.loads(call('api',endpoint,timeout=30))
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
    # Prepare diagnostics before the first external read. Never overwrite a
    # previous rebuild or let stale archives enter this attempt.
    args.output=Path(args.output)
    if args.output.exists() and any(args.output.iterdir()):
        raise ValueError('New empty output directory required; stale files cannot enter a rebuild')
    args.output.mkdir(parents=True,exist_ok=True)
    state=dict(stage='preflight',context={},quality_report_complete=False,
               publication_status='not_attempted')
    day=args.date or (datetime.now(timezone.utc).date()-timedelta(days=1)).isoformat()

    def stage(name,**context):
        state['stage']=name
        state['context']={key:safe_diagnostic(value,512) for key,value in context.items()}

    try:
        date.fromisoformat(day)
        begin=datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
        cutoff=begin-timedelta(days=1)
        repo=os.environ['GH_REPO']
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repo):
            raise ValueError('Invalid repository identity')
        releases=[]; pages=0; seen={}
        for page in range(1,101):
            endpoint=f'repos/{repo}/releases?per_page=100&page={page}'
            stage('release_inventory',page=page,endpoint=endpoint)
            batch=json.loads(gh('api',endpoint,timeout=30))
            pages+=1
            if not isinstance(batch,list) or len(batch)>100:raise ValueError('Invalid release page')
            if not batch:break
            added=0; created=[]
            for item in batch:
                if (not isinstance(item,dict) or type(item.get('id')) is not int or item['id']<=0
                        or not isinstance(item.get('tag_name'),str) or not isinstance(item.get('created_at'),str)):
                    raise ValueError('Invalid release identity')
                timestamp=datetime.fromisoformat(item['created_at'].replace('Z','+00:00'))
                if timestamp.tzinfo is None:raise ValueError('Release timestamp requires timezone')
                created.append(timestamp)
                identity=(item['tag_name'],item['created_at'])
                if item['id'] in seen:
                    if seen[item['id']]!=identity:raise ValueError('Release changed during pagination')
                    continue
                seen[item['id']]=identity; added+=1
                if item['tag_name'].startswith('capture-v2-'):releases.append(item)
            if not added:raise RuntimeError('Release pagination did not advance')
            if min(created)<cutoff:break
        else:
            raise RuntimeError('Release listing exceeded pagination bound')
        assets=args.assets.split(',')
        downloaded=[]; inventory=[]
        for release in releases:
            tag=release['tag_name']
            stage('asset_inventory',release=tag,release_id=release['id'])
            if not re.fullmatch(r'capture-v2-[A-Za-z0-9_-]+',tag):raise ValueError('Invalid release tag')
            listing,count=list_release_assets(repo,release['id'],on_page=lambda page,endpoint:
                stage('asset_inventory',release=tag,release_id=release['id'],page=page,endpoint=endpoint))
            by_name={x['name']:x for x in listing}
            selected=sorted(n for n in by_name if n.startswith('snapshots-'+day+'-') and n.endswith('.jsonl.gz'))
            inventory.append(dict(release=tag,release_id=release['id'],asset_pages=count,listed_assets=len(listing),selected_archives=len(selected)))
            if not selected:continue
            target=args.output/tag; target.mkdir(exist_ok=True)
            for name in selected:
                stage('archive_verification',release=tag,archive=name)
                if not re.fullmatch(r'snapshots-\d{4}-\d{2}-\d{2}-\d+\.jsonl\.gz',name):raise ValueError('Invalid archive name')
                if name+'.sha256' not in by_name:raise RuntimeError('Missing checksum for selected archive')
                for filename in (name,name+'.sha256'):
                    stage('archive_download',release=tag,asset=filename)
                    gh('release','download',tag,'--pattern',filename,'--dir',str(target),'--clobber',timeout=90)
                stage('archive_verification',release=tag,archive=name)
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
        stage('quality_calculation',verified_archives=len(downloaded))
        # report() writes files itself; keep those private until its result and
        # all daily thresholds have been validated. failure.json is a distinct
        # schema, never a replacement quality.json with invented zero coverage.
        staged=args.output/'.report-staging'/'quality.json'
        result=report(args.output,staged,day,assets)
        result.update(release_assets_checked=downloaded,release_pages_scanned=pages,
                      release_asset_inventory=inventory,asset_pagination=True,
                      release_discovery_cutoff_utc=cutoff.isoformat(),
                      full_history_complete=False,
                      collection_scope='All explicitly enumerated selected-day archives in the bounded release discovery window; not exchange-event completeness.')
        stage('threshold_validation',threshold=args.threshold)
        apply_thresholds(result,args.threshold)
        stage('report_write')
        atomic_json(staged,result)
        if not staged.with_suffix('.md').is_file():raise FileNotFoundError('Staged quality markdown is missing')
        try:
            staged.with_suffix('.md').replace(args.output/'quality.md')
            staged.replace(args.output/'quality.json')
        except Exception:
            # This attempt owns a previously empty output directory. Never leave
            # half of the report pair looking like a completed rebuild.
            for name in ('quality.json','quality.md'):
                (args.output/name).unlink(missing_ok=True)
            raise
        state['quality_report_complete']=True
        if args.publish:
            stage('publication',release='quality-v2-'+day)
            state['publication_status']='in_progress'
            publish('quality-v2-'+day,[args.output/'quality.json',args.output/'quality.md'],replace=True,timeout=90)
            state['publication_status']='complete'
        if not getattr(args,'quiet',False):print(json.dumps(result,indent=2))
        stage('coverage_check',threshold=args.threshold)
        if args.check and result['alert']:
            raise SystemExit('Daily data coverage below threshold; see the quality report.')
        return result
    except (Exception,SystemExit) as error:
        if state['publication_status']=='in_progress':state['publication_status']='failed_or_partial'
        failure=dict(schema_version=1,report_kind='daily_quality_failure',status='failed',
                     date_utc=safe_diagnostic(day,32),**state,error=error_details(error),
                     run_id=safe_diagnostic(os.environ.get('GITHUB_RUN_ID'),32),
                     run_attempt=safe_diagnostic(os.environ.get('GITHUB_RUN_ATTEMPT'),16),
                     source_sha=safe_diagnostic(os.environ.get('GITHUB_SHA'),64),
                     recorded_at_utc=datetime.now(timezone.utc).isoformat())
        try:
            atomic_json(args.output/'failure.json',failure)
            (args.output/'failure.md').write_text(
                '# Daily quality rebuild failed\n\n'
                'This is an execution failure report, not a coverage measurement.\n\n'
                + '```json\n'+json.dumps(failure,indent=2)+'\n```\n',encoding='utf-8')
        except Exception as reporting_error:
            print('Could not save failure report: '+safe_diagnostic(str(reporting_error)),file=sys.stderr)
        raise


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
