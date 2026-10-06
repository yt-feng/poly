"""Offline fixed-calendar measurement audit; no networking or trading.

The collector emits compact attempt facts pointing to existing raw evidence.
This is a measurement schema, not the stricter strategy/execution contract.
"""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path


def event_times(payload):
    items = payload if isinstance(payload, list) else [payload]
    result = []
    for item in items:
        value = item.get('timestamp') if isinstance(item, dict) else None
        if value is None and isinstance(item, dict) and isinstance(item.get('payload'), dict):
            value = item['payload'].get('timestamp')
        # Polymarket book timestamps are milliseconds. Do not guess units or
        # replace missing venue time with local receipt time.
        try:
            text = str(value)
            parsed = int(text) if text.isdigit() else None
            result.append(parsed if parsed is not None and 10**12 <= parsed < 10**14 else None)
        except (ValueError, TypeError, OverflowError):
            result.append(None)
    return result


def side_states(book):
    out = {}
    for side in ('bids', 'asks'):
        levels = book.get(side) if isinstance(book, dict) else None
        if not isinstance(levels, list):
            out[side] = 'missing_or_invalid_side'
            continue
        if not levels:
            out[side] = 'explicit_empty'
            continue
        positive = False
        try:
            for level in levels:
                p, q = (level['price'], level['size']) if isinstance(level, dict) else level[:2]
                p, q = float(p), float(q)
                if not math.isfinite(p) or not math.isfinite(q) or not 0 <= p <= 1 or q < 0:
                    raise ValueError('invalid_level')
                positive |= q > 0
            out[side] = 'positive_size_levels' if positive else 'nonpositive_size_only'
        except (ValueError, TypeError, KeyError, IndexError, OverflowError):
            out[side] = 'malformed_levels'
    return out


def make_plan(anchor):
    dt = datetime.fromisoformat(anchor.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise ValueError('timezone_required')
    ms = math.ceil(dt.timestamp()*1000)
    start = ((ms+600_000+299_999)//300_000)*300_000
    return {'schema':'measurement-calendar/v1', 'anchor_utc':dt.astimezone(timezone.utc).isoformat(),
            'start_ms':start, 'end_ms':start+172_800_000, 'window_ms':300_000,
            'decision_offsets_seconds':[60,120,240], 'horizon_seconds':30,
            'entry_rule':'first_attempt_finished_strictly_after_decision_before_target',
            'tolerance_ms':3000, 'future_skew_ms':2000,
            'windows':[{'slug':f'btc-updown-5m-{s//1000}', 'start_ms':s, 'end_ms':s+300_000}
                       for s in range(start,start+172_800_000,300_000)],
            'purpose':'measurement_only', 'promotion_allowed':False}


def validate_plan(plan):
    if plan != make_plan(plan['anchor_utc']):
        raise ValueError('calendar_changed')


def attempt_class(row):
    """Disjoint attempt outcome; absent source time is an additional flag."""
    if row.get('not_sent_reason'):
        return 'not_sent_cooldown'
    if row.get('error_type') == 'CancelledError':
        return 'cancelled_attempt'
    status = row.get('http_status')
    if status in (403,418,429,451):
        return 'access_or_rate_denied'
    if isinstance(status,int) and not 200 <= status < 300:
        return 'http_error'
    if row.get('error_type'):
        return 'transport_or_decode_error'
    if status is None:
        return 'http_status_unknown'
    if not row.get('response_seen') or not row.get('parsed'):
        return 'response_or_parse_unavailable'
    if row.get('returned_token_id') != row.get('requested_token_id'):
        return 'token_identity_mismatch_or_missing'
    states = row.get('side_states', {})
    if set(states) != {'bids','asks'} or any(x not in ('explicit_empty','positive_size_levels','nonpositive_size_only') for x in states.values()):
        return 'invalid_book_schema'
    if 'explicit_empty' in states.values():
        return 'successful_explicit_empty_side'
    if 'nonpositive_size_only' in states.values():
        return 'successful_no_positive_size_side'
    return 'successful_nonempty_book'


def source_flags(row, tolerance_ms=3000, future_skew_ms=2000):
    event = row.get('source_event_ms'); receive = row.get('response_received_at_ns')
    if type(event) is not int or type(receive) is not int:
        return ['source_or_receive_time_missing']
    age = receive/1_000_000-event
    return ['source_time_in_future'] if age < -future_skew_ms else ['source_time_stale'] if age > tolerance_ms else []


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def audit(plan, roots):
    validate_plan(plan)
    windows = {w['slug']:{**w,'attempts':0,'outcomes':Counter(),'source_flags':Counter(),
                         'checkpoints':{}} for w in plan['windows']}
    counters = Counter(); inputs=[]; seen={}; selected={}; ties=set()
    for root in map(Path,roots):
        manifest = root/'manifest.json'
        obj = json.loads(manifest.read_text())
        inputs.append({'root':str(root),'manifest_sha256':digest(manifest),
                       'raw_integrity':'raw files referenced by manifest, not reread by this compact audit'})
        for item in obj['files']:
            if item['kind'] != 'measurement':continue
            path=root/item['file']
            if Path(item['file']).name != item['file'] or path.is_symlink() or digest(path) != item['sha256']:
                raise ValueError('unsafe_or_changed_measurement_segment')
            with gzip.open(path,'rt') as stream:
                for line_number,line in enumerate(stream,1):
                    row=json.loads(line); aid=row.get('attempt_id')
                    if row.get('schema')!='book-attempt/v1' or not isinstance(aid,str) or not aid:
                        raise ValueError('invalid_measurement_record')
                    content=hashlib.sha256(json.dumps(row,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                    if aid in seen:
                        if seen[aid]!=content:raise ValueError('conflicting_attempt_id')
                        counters['duplicate_attempt']+=1;continue
                    seen[aid]=content
                    meta=row.get('market') or {};slug=meta.get('slug');role=meta.get('role')
                    if (role not in ('up','down') or not meta.get('condition_id') or
                            meta.get('token_id')!=row.get('requested_token_id') or
                            type(meta.get('observed_at_ns')) is not int or
                            type(row.get('request_started_at_ns')) is not int or
                            meta['observed_at_ns']>row['request_started_at_ns']):
                        counters['unmapped_or_unavailable_identity']+=1;continue
                    if slug not in windows:
                        counters['mapped_outside_plan']+=1;continue
                    w=windows[slug];w['attempts']+=1;w['outcomes'][attempt_class(row)]+=1
                    for flag in source_flags(row,plan['tolerance_ms'],plan['future_skew_ms']):w['source_flags'][flag]+=1
                    end=row.get('finished_at_ns')
                    if type(end) is not int:
                        counters['attempt_finish_time_missing']+=1;continue
                    # First/last ATTEMPT by recorded finish time, not first valid
                    # price: failures cannot be skipped to a favorable book.
                    for offset in plan['decision_offsets_seconds']:
                        for kind,point in (('decision',w['start_ms']+offset*1000),
                                           ('entry',w['start_ms']+offset*1000),
                                           ('target',w['start_ms']+(offset+plan['horizon_seconds'])*1000)):
                            k=(slug,role,offset,kind);t=end/1_000_000
                            qualifies=t<point if kind=='decision' else t>point if kind=='entry' else t>=point
                            if not qualifies:continue
                            old=selected.get(k)
                            if old is None or (end>old[0] if kind=='decision' else end<old[0]):
                                selected[k]=(end,row,{'file':str(path),'line':line_number,'sha256':item['sha256']})
                                ties.discard(k)
                            elif end==old[0]:ties.add(k)
        counters['measurement_segments']+=sum(x['kind']=='measurement' for x in obj['files'])
    for slug,w in windows.items():
        w['coverage']='attempts_observed' if w['attempts'] else 'no_attempt_evidence'
        for offset in plan['decision_offsets_seconds']:
            for kind in ('decision','entry','target'):
                stamps={}
                for role in ('up','down'):
                    key=f'{offset}/{kind}/{role}';record=selected.get((slug,role,offset,kind))
                    if record is None:
                        w['checkpoints'][key]={'status':'unknown_no_attempt'};continue
                    end,row,ref=record;point=w['start_ms']+(offset+(plan['horizon_seconds'] if kind=='target' else 0))*1000
                    gap=abs(end/1_000_000-point)
                    outside=(gap>=plan['horizon_seconds']*1000 if kind=='entry' else gap>plan['tolerance_ms'])
                    status=('unknown_ambiguous_same_finish_time' if (slug,role,offset,kind) in ties else
                            'unknown_outside_tolerance' if outside else attempt_class(row))
                    w['checkpoints'][key]={'status':status,'attempt_id':row['attempt_id'],'gap_ms':gap,
                        'source_flags':source_flags(row,plan['tolerance_ms'],plan['future_skew_ms']),
                        'raw_ref':row.get('raw_ref'),'measurement_ref':ref}
                    stamps[role]=row.get('response_received_at_ns')
                skew=abs(stamps['up']-stamps['down'])/1_000_000 if all(type(stamps.get(r)) is int for r in ('up','down')) else None
                w['checkpoints'][f'{offset}/{kind}/pair']={'receive_skew_ms':skew,'atomicity_certified':False}
    checkpoint_counts=Counter();outcomes=Counter();flags=Counter()
    for w in windows.values():
        outcomes.update(w['outcomes']);flags.update(w['source_flags'])
        for key,value in w['checkpoints'].items():
            if not key.endswith('/pair'):checkpoint_counts[key.split('/')[1]+':'+value['status']]+=1
    return {'plan':plan,'inputs':inputs,'counters':dict(counters),'windows':list(windows.values()),
        'attempt_outcomes':dict(outcomes),'source_flag_counts':dict(flags),'checkpoint_status_counts':dict(checkpoint_counts),
        'observed_windows':sum(w['attempts']>0 for w in windows.values()),'planned_windows':len(windows),
        'promotion_allowed':False,'pnl':None,'measurement_schema_only':True,
        'limitations':['Absent attempts stay unknown, not failed orders or empty books.',
            'This validates compact records, not raw venue authenticity or strategy/execution contracts.',
            'Successful empty response is observed response state, not a diagnosis of venue cause.',
            'Source age is a timestamp diagnostic, not certified one-way latency.']}


def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('plan');a.add_argument('--anchor',required=True);a.add_argument('--output',type=Path,required=True)
    a=sub.add_parser('audit');a.add_argument('--plan',type=Path,required=True);a.add_argument('--archive',type=Path,action='append',required=True);a.add_argument('--output',type=Path,required=True)
    args=p.parse_args();result=make_plan(args.anchor) if args.command=='plan' else audit(json.loads(args.plan.read_text()),args.archive)
    if args.output.exists():raise SystemExit('Refusing to overwrite an existing plan or report')
    args.output.write_text(json.dumps(result,sort_keys=True,indent=2)+'\n');args.output.chmod(0o600)
    print('Offline measurement file written; no collection or promotion performed.')


if __name__=='__main__':main()
