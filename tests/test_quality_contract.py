"""Synthetic quality and inventory failures; no private data or live API."""
import gzip,hashlib,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from datetime import datetime,timezone
from quality_v2 import summarize,gap_intervals,longest_gap,report
from daily_quality_v2 import list_release_assets,apply_thresholds,main

DAY='2026-01-01'
START=int(datetime(2026,1,1,tzinfo=timezone.utc).timestamp())

def row(offset=0,ms=0,**kw):
    return dict(asset='btc',sample_ms=(START+offset)*1000+ms,poly_valid=True,binance_valid=True,**kw)

class SecondsTests(unittest.TestCase):
    def test_empty_day_gap(self):
        d=summarize([],DAY)['daily'][0];self.assertEqual(d['missing_seconds'],86400);self.assertEqual(len(d['missing_intervals']),1)
    def test_full_interval(self):self.assertEqual(gap_intervals(set(range(8)),0,8),[])
    def test_leading_and_trailing(self):
        self.assertEqual([g['duration_seconds'] for g in gap_intervals({3,4},0,10)],[3,5])
    def test_internal(self):self.assertEqual(gap_intervals({0,1,7,8,9},0,10)[0]['start_second'],2)
    def test_exclusive_end(self):self.assertEqual(gap_intervals({0,4},0,5)[0]['end_second_exclusive'],4)
    def test_outside_clipped(self):self.assertEqual(longest_gap({-1,99},0,10),10)
    def test_zero_interval(self):self.assertEqual(longest_gap(set(),0,0),0)
    def test_invalid_interval(self):self.assertRaises(ValueError,gap_intervals,set(),2,1)
    def test_gap_conservation(self):
        q=summarize([row(0),row(2),row(86399)],DAY)['daily'][0];self.assertEqual(q['observed_seconds']+q['missing_seconds'],86400)
    def test_rows_not_seconds(self):
        q=summarize([row(ms=1),row(ms=999)],DAY)['daily'][0];self.assertEqual(q['valid_poly'],2);self.assertEqual(q['valid_poly_seconds'],1)
    def test_invalid_in_same_second(self):
        a,b=row(ms=1),row(ms=2);b['poly_valid']=False
        q=summarize([a,b],DAY)['daily'][0];self.assertEqual(q['valid_poly_seconds'],0);self.assertEqual(q['valid_binance_seconds'],1)
    def test_exact_duplicate(self):
        r=row();q=summarize([r,r],DAY);self.assertEqual(q['duplicate_rows'],1);self.assertEqual(q['daily'][0]['valid_poly_seconds'],1)
    def test_conflicting_duplicate_cannot_hide_invalid(self):
        a,b=row(),row();b['poly_valid']=False
        q=summarize([a,b],DAY)['daily'][0];self.assertEqual(q['valid_poly_seconds'],0)
    def test_false_string_not_true(self):
        a=row();a['poly_valid']='false';self.assertEqual(summarize([a],DAY)['daily'][0]['valid_poly_seconds'],0)
    def test_missing_flag_unknown_not_valid(self):
        a=row();del a['poly_valid'];self.assertEqual(summarize([a],DAY)['daily'][0]['valid_poly_seconds'],0)
    def test_day_filter(self):self.assertEqual(summarize([row(-1),row(0),row(86400)],DAY)['daily'][0]['rows'],1)
    def test_adjacent_archives(self):
        a=row(0,_archive_ref='old/snapshots-a');b=row(4,_archive_ref='new/snapshots-b')
        g=summarize([a,b],DAY)['daily'][0]['missing_intervals'][0]
        self.assertEqual(g['preceding_archive'],'old/snapshots-a');self.assertEqual(g['following_archive'],'new/snapshots-b')
        self.assertEqual(g['cause'],'UNDETERMINED_FROM_SNAPSHOTS')
    def test_no_input_mutation(self):
        a=row();saved=dict(a);summarize([a],DAY);self.assertEqual(a,saved)
    def test_report_disk_and_markdown(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            with gzip.open(root/'snapshots-x.jsonl.gz','wt') as f:f.write(json.dumps(row())+'\n')
            q=report(root,root/'q.json',DAY);self.assertIn('snapshots-x.jsonl.gz',q['daily'][0]['archive_spans'])
            self.assertIn('End UTC exclusive',(root/'q.md').read_text())

class ThresholdTests(unittest.TestCase):
    def fixture(self):return dict(daily=[dict(date_utc=DAY,asset='btc',expected_seconds=86400,observed_seconds=86400,valid_poly_seconds=86400,valid_binance_seconds=86400,valid_poly=172800)])
    def test_valid_day(self):self.assertFalse(apply_thresholds(self.fixture(),.95)['alert'])
    def test_row_inflation_cannot_pass(self):
        q=self.fixture();q['daily'][0]['valid_poly_seconds']=43200;self.assertTrue(apply_thresholds(q,.95)['alert'])
    def test_missing_seconds_field_fails(self):
        q=self.fixture();del q['daily'][0]['valid_poly_seconds'];self.assertRaises(ValueError,apply_thresholds,q,.95)
    def test_invalid_thresholds(self):
        for x in (float('nan'),float('inf'),-.1,1.1,True):self.assertRaises(ValueError,apply_thresholds,self.fixture(),x)
    def test_threshold_boundary(self):
        q=self.fixture();q['daily'][0]['valid_poly_seconds']=82080;self.assertFalse(apply_thresholds(q,.95)['alert'])
    def test_empty_report_not_green(self):self.assertRaises(ValueError,apply_thresholds,dict(daily=[]),.95)

class InventoryTests(unittest.TestCase):
    def call(self,pages):
        self.calls=[]
        def fake(*args,**kwargs):
            self.calls.append((args,kwargs));return json.dumps(pages.pop(0))
        return fake
    def item(self,n,name=None):return dict(id=n,name=name or f'file-{n}',size=2)
    def test_paginates_to_next_page(self):
        a,n=list_release_assets('o/r',1,call=self.call([[self.item(1),self.item(2)],[self.item(3)]]),per_page=2)
        self.assertEqual((len(a),n),(3,2));self.assertIn('page=2',self.calls[1][0][1])
    def test_empty_page_finishes(self):
        a,n=list_release_assets('o/r',1,call=self.call([[]]));self.assertEqual((a,n),([],1))
    def test_nonadvance_fails(self):
        self.assertRaises(RuntimeError,list_release_assets,'o/r',1,call=self.call([[self.item(1)],[self.item(1)]]),per_page=1)
    def test_cap_not_complete(self):
        self.assertRaises(RuntimeError,list_release_assets,'o/r',1,call=self.call([[self.item(1)]]),per_page=1,max_pages=1)
    def test_name_conflict(self):
        self.assertRaises(ValueError,list_release_assets,'o/r',1,call=self.call([[self.item(1,'x'),self.item(2,'x')]]))
    def test_changed_asset_id(self):
        self.assertRaises(ValueError,list_release_assets,'o/r',1,call=self.call([[self.item(1)],[dict(self.item(1),size=3)]]),per_page=1)
    def test_wrong_schema(self):self.assertRaises(ValueError,list_release_assets,'o/r',1,call=self.call([{}]))
    def test_bad_identity(self):self.assertRaises(ValueError,list_release_assets,'o/r',1,call=self.call([[dict(id=True,name='x')]]))
    def test_invalid_arguments(self):
        for r,i in (('bad',1),('o/r',True),('o/r',0)):self.assertRaises(ValueError,list_release_assets,r,i)

class RebuildTests(unittest.TestCase):
    def run_rebuild(self,bad_sha=False,embedded_only=False,stale=False):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)/'output'
            if stale:root.mkdir();(root/'old').write_text('x')
            name='snapshots-'+DAY+'-000001.jsonl.gz';data=gzip.compress((json.dumps(row())+'\n').encode());digest=hashlib.sha256(data).hexdigest()
            rel=dict(databaseId=11,tagName='capture-v2-11-1',createdAt='2025-12-31T00:00:00Z',
                     publishedAt='2025-12-31T00:01:00Z',isDraft=False)
            metadata=dict(data=dict(repository=dict(nameWithOwner='o/r',releases=dict(
                nodes=[rel],pageInfo=dict(hasNextPage=False,endCursor='last')))))
            inv=[dict(id=12,name=name,size=len(data),digest='sha256:'+digest),dict(id=13,name=name+'.sha256',size=70)]
            calls=[]
            def fake(*a,**kw):
                calls.append(a)
                if a[0]=='api':return json.dumps(inv if '/assets?' in a[1] else metadata)
                target=Path(a[a.index('--dir')+1]);fn=a[a.index('--pattern')+1]
                (target/fn).write_bytes(data if fn==name else ((('0'*64 if bad_sha else digest)+'  '+name+'\n').encode()));return ''
            with patch('daily_quality_v2.gh',side_effect=fake),patch.dict('os.environ',{'GH_REPO':'o/r'}):
                result=main(SimpleNamespace(date=DAY,assets='btc',output=root,threshold=.95,publish=False,check=False,quiet=True))
            self.assertTrue(any('/assets?' in str(c) for c in calls));return result
    def test_realistic_rebuild_not_embedded_asset_list(self):
        r=self.run_rebuild();self.assertEqual(r['daily'][0]['observed_seconds'],1);self.assertTrue(r['alert']);self.assertTrue(r['asset_pagination'])
    def test_bad_sha_never_accepted(self):self.assertRaises(ValueError,self.run_rebuild,bad_sha=True)
    def test_stale_output_rejected(self):self.assertRaises(ValueError,self.run_rebuild,stale=True)

if __name__=='__main__':unittest.main()
