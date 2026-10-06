import asyncio
import copy
import gzip
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from capture_v3 import CollectorV3
from measurement_v3 import make_plan, validate_plan, audit, digest, event_times, side_states, attempt_class, source_flags
from archive_v2 import Archive
from test_book_observation_semantics import Response, Session


async def write_fixed_archive(root, *, malformed_target=False):
    """Synthetic books only; the real writer runs against an in-memory HTTP stub."""
    plan=make_plan('2030-01-01T00:00:00Z');w=plan['windows'][0]
    c=CollectorV3(['btc'],Path(root));c.capture_id='fixed-offline-fixture'
    # Force evidence/reference records into different closed gzip segments.
    c.archive.max_bytes=1
    market={'slug':w['slug'],'conditionId':'synthetic-condition',
            'clobTokenIds':['fixture-up','fixture-down'],'outcomes':['Up','Down']}
    with patch('capture_v3.time.time_ns',return_value=w['start_ms']*1_000_000):
        c.raw('polymarket_metadata',{'slug':w['slug'],'market':market})
    expected={}
    try:
        for offset in (59,62,90):
            ms=w['start_ms']+offset*1000
            for token in ('fixture-up','fixture-down'):
                body={'asset_id':token,'timestamp':str(ms),
                      'bids':[{'price':'.4','size':'2'}],'asks':[{'price':'.5','size':'3'}]}
                if malformed_target and offset==90 and token=='fixture-up':
                    body['bids'][0]['price']='invalid'
                c.session=Session(Response(json.dumps(body).encode()))
                with patch('capture_v3.time.time_ns',return_value=ms*1_000_000):
                    await c.poll_book(token)
                expected[f'{c.capture_id}:{c._attempt_sequence}']=body
    finally:
        c.archive.close()
    return plan,expected


class ArchiveCompatibilityTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def read_checked(root,kind):
        rows=[]
        for item in json.loads((root/'manifest.json').read_text())['files']:
            path=root/item['file']
            if item['kind']!=kind:continue
            if digest(path)!=item['sha256'] or digest(path)!=path.with_name(path.name+'.sha256').read_text().split()[0]:
                raise ValueError('fixture_checksum_mismatch')
            with gzip.open(path,'rt') as f:rows.extend((json.loads(line),path.name) for line in f)
        return rows

    async def test_writer_gzip_reference_inline_replay_and_calendar_end_to_end(self):
        from microstructure_v3 import Microstructure
        from validate_market_ws import validate
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);plan,expected=await write_fixed_archive(root)
            raw=self.read_checked(root,'raw');measurements=self.read_checked(root,'measurement')
            responses={(r['source'],r['attempt_id']):(r,name) for r,name in raw
                       if r['source']=='polymarket_rest_book_response'}
            inline={r['attempt_id']:(r,name) for r,name in raw if r['source']=='polymarket_rest_book'}
            self.assertEqual(len(inline),6);self.assertEqual(len(measurements),6)
            replay=Microstructure(None)
            for m,_ in measurements:
                ref=m['raw_ref'];response,response_file=responses[(ref['source'],ref['attempt_id'])]
                r,name=inline[m['attempt_id']]
                self.assertNotEqual(name,response_file)
                self.assertEqual(r['payload_ref'],{'source':ref['source'],'attempt_id':ref['attempt_id']})
                self.assertEqual(r['payload'],response['payload']['book'])
                self.assertEqual(r['payload'],expected[m['attempt_id']])
                # Exercise the existing payload consumer with rows read back
                # from disk. It needs no reference-aware migration.
                replay.ingest(r['source'],r['payload'],r['connection_id'],r['source_event_ms'],r['received_at_ns']//1_000_000)
            self.assertEqual(set(replay.poly_books),{'fixture-up','fixture-down'})
            ws=validate(root);self.assertEqual(ws['source_counts']['polymarket_rest_book'],6)
            self.assertFalse(ws['rollover_smoke_passed'])
            result=audit(plan,[root]);self.assertEqual(result['planned_windows'],576)
            self.assertEqual(result['observed_windows'],1)
            for kind in ('decision','entry','target'):
                for role in ('up','down'):
                    self.assertEqual(result['windows'][0]['checkpoints'][f'60/{kind}/{role}']['status'],'successful_nonempty_book')
            self.assertTrue(all(w['coverage']=='no_attempt_evidence' for w in result['windows'][1:]))
            self.assertIsNone(result['pnl']);self.assertFalse(result['promotion_allowed'])

    async def test_malformed_target_keeps_response_without_success_payload(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);plan,_=await write_fixed_archive(root,malformed_target=True)
            result=audit(plan,[root]);target=result['windows'][0]['checkpoints']['60/target/up']
            self.assertEqual(target['status'],'transport_or_decode_error')
            raw=self.read_checked(root,'raw');aid=target['attempt_id']
            self.assertTrue(any(r.get('attempt_id')==aid and r['source']=='polymarket_rest_book_response' for r,_ in raw))
            self.assertFalse(any(r.get('attempt_id')==aid and r['source']=='polymarket_rest_book' for r,_ in raw))

    async def test_legacy_inline_record_remains_usable_without_reference(self):
        from microstructure_v3 import Microstructure
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);a=Archive(root)
            body={'asset_id':'legacy','bids':[['.4','2']],'asks':[['.5','2']]}
            a.write('raw',{'schema_version':3,'source':'polymarket_rest_book','payload':body,
                           'received_at_ns':1790000000000000000,'source_event_ms':None,'connection_id':None});a.close()
            row,_=self.read_checked(root,'raw')[0];self.assertNotIn('payload_ref',row)
            replay=Microstructure(None);replay.ingest(row['source'],row['payload'],None,None,1790000000000)
            self.assertEqual(replay.poly_books['legacy']['payload'],body)


class AttemptTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.c=CollectorV3(['btc'],Path(self.tmp.name));self.addCleanup(self.c.archive.close)

    def records(self,kind):
        self.c.archive.close();out=[]
        for p in Path(self.tmp.name).glob(kind+'-*.gz'):
            with gzip.open(p,'rt') as f:out.extend(json.loads(line) for line in f)
        return out

    async def test_attempt_join_retains_legacy_inline_payload(self):
        now=int(time.time()*1000)
        market={'slug':'btc-updown-5m-1800000000','conditionId':'c','clobTokenIds':['t','d'],'outcomes':['Up','Down']}
        self.c.raw('polymarket_metadata',{'slug':market['slug'],'market':market})
        body={'asset_id':'t','timestamp':str(now),'bids':[{'price':'.4','size':'2'}],'asks':[{'price':'.5','size':'3'}]}
        self.c.session=Session(Response(json.dumps(body).encode()))
        await self.c.poll_book('t')
        records=self.records('raw');m=self.records('measurement')[0]
        joined=[r for r in records if r.get('attempt_id')==m['attempt_id']]
        self.assertEqual(len(joined),4)
        self.assertEqual(sum(r.get('payload',{}).get('book')==body for r in joined),1)
        parsed=next(r for r in joined if r['source']=='polymarket_rest_book')
        self.assertEqual(parsed['payload'],body);self.assertEqual(parsed['payload_ref']['attempt_id'],m['attempt_id'])
        self.assertEqual(attempt_class(m),'successful_nonempty_book')
        self.assertEqual(m['market']['role'],'up');self.assertEqual(m['source_event_ms'],now)
        self.assertIsNotNone(self.c.micro.poly_books['t'])
        self.assertEqual(self.c.health()['measurement']['completed_poll_attempt_records'],1)

    async def test_concurrent_requests_never_cross_context(self):
        async def get(url,params):
            await asyncio.sleep(0)
            return {'asset_id':params['token_id'],'bids':[],'asks':[]}
        self.c.get=get
        await asyncio.gather(self.c.poll_book('t1'),self.c.poll_book('t2'),self.c.poll_book('t1'))
        ms=self.records('measurement');self.assertEqual(len({r['attempt_id'] for r in ms}),3)
        rows=self.records('raw')
        for m in ms:
            evidence=next(r for r in rows if r.get('attempt_id')==m['attempt_id'] and r['source']=='polymarket_rest_book_response')
            self.assertEqual(evidence['payload']['requested_token_id'],m['requested_token_id'])
        self.assertIsNone(self.c._book_attempt.get())

    async def test_failed_request_retained_and_no_status_invented(self):
        self.c.get=AsyncMock(side_effect=asyncio.TimeoutError());self.c.error=lambda *_:None
        await self.c.poll_book('t');m=self.records('measurement')[0]
        self.assertFalse(m['response_seen']);self.assertIsNone(m['http_status'])
        self.assertEqual(attempt_class(m),'transport_or_decode_error')

    async def test_cancellation_records_unknown_and_resets_context(self):
        self.c.get=AsyncMock(side_effect=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):await self.c.poll_book('t')
        m=self.records('measurement')[0];self.assertEqual(attempt_class(m),'cancelled_attempt')
        self.assertIsNone(self.c._book_attempt.get())

    async def test_wrong_returned_token_never_measurement_success(self):
        self.c.session=Session(Response(b'{"asset_id":"other","bids":[],"asks":[]}'))
        await self.c.poll_book('t');m=self.records('measurement')[0]
        self.assertEqual(attempt_class(m),'token_identity_mismatch_or_missing')

    async def test_ws_mixed_batch_preserves_each_time_without_invented_batch_time(self):
        stamp=1790000000000
        self.c.poly_message([{'timestamp':str(stamp)},{'timestamp':str(stamp+1)},{'type':'other'}],'conn')
        r=self.records('raw')[0]
        self.assertEqual(r['source_event_times_ms'],[stamp,stamp+1,None]);self.assertIsNone(r['source_event_ms'])


class CalendarTests(unittest.TestCase):
    def setUp(self):self.plan=make_plan('2030-01-01T00:00:01Z')

    def row(self,offset=89,**changes):
        w=self.plan['windows'][0];ns=(w['start_ms']+offset*1000)*1_000_000
        r={'schema':'book-attempt/v1','attempt_id':'run:1','requested_token_id':'up','returned_token_id':'up',
           'market':{'slug':w['slug'],'role':'up','condition_id':'c','token_id':'up','observed_at_ns':ns-100_000_000},
           'finished_at_ns':ns,'request_started_at_ns':ns-50_000_000,'response_received_at_ns':ns-1_000_000,
           'http_status':200,'error_type':None,'parsed':True,'response_seen':True,
           'side_states':{'bids':'explicit_empty','asks':'positive_size_levels'},'source_event_ms':ns//1_000_000-5,
           'raw_ref':{'attempt_id':'run:1'}}
        r.update(changes);return r

    def run_audit(self,rows):
        with tempfile.TemporaryDirectory() as d:
            a=Archive(Path(d))
            for r in rows:a.write('measurement',r)
            a.close()
            if not rows:(Path(d)/'manifest.json').write_text('{"files":[]}')
            return audit(self.plan,[d])

    def test_calendar_has_576_contiguous_windows_and_no_automatic_collection(self):
        p=self.plan;self.assertEqual(len(p['windows']),576)
        self.assertEqual(datetime_utc(p['start_ms']),'2030-01-01T00:15:00+00:00')
        self.assertEqual(p['end_ms']-p['start_ms'],172800000)
        self.assertEqual(p['windows'][-1]['end_ms'],p['end_ms']);validate_plan(p)
        bad=copy.deepcopy(p);bad['windows'].pop()
        with self.assertRaises(ValueError):validate_plan(bad)
        with self.assertRaises(ValueError):make_plan('2030-01-01T00:00:00')

    def test_empty_archive_keeps_all_missing_windows_unknown(self):
        r=self.run_audit([]);self.assertEqual(r['planned_windows'],576);self.assertEqual(r['observed_windows'],0)
        self.assertTrue(all(w['coverage']=='no_attempt_evidence' for w in r['windows']))
        self.assertIsNone(r['pnl']);self.assertFalse(r['promotion_allowed'])

    def test_first_failed_target_not_replaced_by_later_success(self):
        first=self.row(90,parsed=False,error_type='TimeoutError',http_status=None)
        later=self.row(91,attempt_id='run:2')
        r=self.run_audit([later,first]);w=r['windows'][0]
        self.assertEqual(w['checkpoints']['60/target/up']['attempt_id'],'run:1')
        self.assertEqual(w['checkpoints']['60/target/up']['status'],'transport_or_decode_error')
        self.assertEqual(w['checkpoints']['60/target/down']['status'],'unknown_no_attempt')

    def test_equal_time_different_attempts_are_ambiguous(self):
        r=self.run_audit([self.row(90),self.row(90,attempt_id='run:2')])
        self.assertEqual(r['windows'][0]['checkpoints']['60/target/up']['status'],'unknown_ambiguous_same_finish_time')

    def test_duplicate_is_not_extra_evidence_and_conflict_rejected(self):
        r=self.run_audit([self.row(),self.row()]);self.assertEqual(r['windows'][0]['attempts'],1)
        self.assertEqual(r['counters']['duplicate_attempt'],1)
        with self.assertRaisesRegex(ValueError,'conflicting_attempt'):
            self.run_audit([self.row(),self.row(http_status=500)])

    def test_future_metadata_not_used_and_late_target_unknown(self):
        row=self.row(95);row['market']['observed_at_ns']=row['finished_at_ns']+1
        r=self.run_audit([row]);self.assertEqual(r['observed_windows'],0)
        r=self.run_audit([self.row(95)])
        self.assertEqual(r['windows'][0]['checkpoints']['60/target/up']['status'],'unknown_outside_tolerance')

    def test_entry_uses_first_strictly_later_attempt_without_moving_exit(self):
        r=self.run_audit([self.row(60),self.row(62,attempt_id='run:2'),self.row(90,attempt_id='run:3')])
        c=r['windows'][0]['checkpoints']
        self.assertEqual(c['60/entry/up']['attempt_id'],'run:2')
        self.assertEqual(c['60/target/up']['attempt_id'],'run:3')
        r=self.run_audit([self.row(90)])
        self.assertEqual(r['windows'][0]['checkpoints']['60/entry/up']['status'],'unknown_outside_tolerance')

    def test_source_time_and_side_semantics_keep_unknown_distinct(self):
        self.assertEqual(event_times({'timestamp':'1790000000'}),[None])
        self.assertEqual(event_times({'timestamp':'nan'}),[None])
        self.assertEqual(event_times({'payload':{'timestamp':'1790000000000'}}),[1790000000000])
        self.assertEqual(side_states({})['bids'],'missing_or_invalid_side')
        self.assertEqual(side_states({'bids':[]})['bids'],'explicit_empty')
        self.assertEqual(side_states({'bids':[['.4','0']]})['bids'],'nonpositive_size_only')
        self.assertEqual(side_states({'bids':[['.4','2'],['bad','2']]})['bids'],'malformed_levels')
        self.assertEqual(source_flags(self.row(source_event_ms=None)),['source_or_receive_time_missing'])
        self.assertEqual(source_flags(self.row(source_event_ms=self.row()['finished_at_ns']//1_000_000+3000)),['source_time_in_future'])

    def test_denial_and_bad_archive_integrity_fail_closed(self):
        self.assertEqual(attempt_class(self.row(http_status=403,error_type='ClientResponseError')),'access_or_rate_denied')
        with tempfile.TemporaryDirectory() as d:
            a=Archive(Path(d));a.write('measurement',self.row());a.close()
            p=next(Path(d).glob('*.gz'));p.write_bytes(p.read_bytes()+b'x')
            with self.assertRaisesRegex(ValueError,'changed_measurement'):audit(self.plan,[d])


def datetime_utc(ms):
    from datetime import datetime,timezone
    return datetime.fromtimestamp(ms/1000,timezone.utc).isoformat()


if __name__=='__main__':unittest.main()
