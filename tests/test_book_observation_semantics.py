import asyncio
import base64
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import aiohttp
import polymarket_quotes as legacy
from capture_v2 import Collector, CLOB, book_summary
from capture_v3 import CollectorV3, BOOK_RESPONSE_LIMIT


class LegacySemanticsTests(unittest.TestCase):
    def test_blank_bid_has_multiple_input_causes(self):
        for body in ({}, {'bids':None}, {'bids':[]}, {'bids':[{'price':'bad','size':'2'}]}, {'bids':'schema changed'}):
            self.assertEqual(legacy.cents(legacy.best_bid_ask_from_book(body)[0]),'')
            self.assertEqual(legacy.level_count(body.get('bids')),'0')

    def test_zero_price_and_zero_size_do_not_mean_blank_bid(self):
        body={'bids':[{'price':'0','size':'0'}]}
        self.assertEqual(legacy.cents(legacy.best_bid_ask_from_book(body)[0]),'0.00')
        self.assertEqual(legacy.top_size(body['bids'],'bid'),'0.00')
        self.assertEqual(legacy.level_count(body['bids']),'1')

    def test_invalid_size_and_fallback_alias_are_lossy_but_keep_price(self):
        self.assertEqual(legacy.top_size([{'price':'.4','size':'bad'}],'bid'),'0.00')
        self.assertEqual(legacy.top_size([{'price':'.4','size':0,'amount':5}],'bid'),'5.00')

    def test_nonfinite_price_is_text_not_blank(self):
        self.assertEqual(legacy.cents(legacy.best_bid_ask_from_book({'bids':[['nan',2]]})[0]),'nan')

    def test_closed_metadata_does_not_blank_or_replace_actual_bid(self):
        info=legacy.MarketInfo('btc-updown-5m-1800000000','url','up','down','window','','btc',{'closed':True})
        with patch.object(legacy,'fetch_book',return_value={'bids':[['.4','2']],'asks':[['.5','3']]}), patch.object(legacy,'fetch_live_reference_price',return_value=''):
            self.assertEqual(legacy.snapshot_row(info,1)['sell_up_cents'],'40.00')

    def test_http_failure_does_not_produce_a_blank_row(self):
        info=legacy.MarketInfo('s','url','up','down','window','','btc',{})
        with patch.object(legacy,'fetch_book',side_effect=legacy.requests.HTTPError('synthetic')):
            with self.assertRaises(legacy.requests.HTTPError):legacy.snapshot_row(info,1)

    def test_header_migration_can_create_blank_fields_without_book_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'old.csv';p.write_text('ts_iso,sell_up_cents\ntime,40.00\n')
            legacy.ensure_csv(p)
            import csv
            with p.open() as stream:row=next(csv.DictReader(stream))
            self.assertEqual(row['sell_up_cents'],'40.00')
            self.assertEqual(row['sell_up_size'],'')
            self.assertEqual(row['level_count_bid_up'],'')

    def test_modern_filter_is_different_from_legacy_zero_size_policy(self):
        payload={'bids':[['.5','0'],['.4','2']]}
        self.assertEqual(legacy.best_bid_ask_from_book(payload)[0],.5)
        self.assertEqual(book_summary(payload)['bid'],.4)


class Content:
    def __init__(self,body):self.body=body
    async def readexactly(self,n):
        if len(self.body)<n:raise asyncio.IncompleteReadError(self.body,n)
        return self.body[:n]


class Response:
    def __init__(self,body,status=200):
        self.content=Content(body);self.status=status
        self.request_info=None;self.history=()
        self.headers={'Content-Type':'application/json','Set-Cookie':'must-not-be-archived'}
    async def __aenter__(self):return self
    async def __aexit__(self,*args):pass
    def raise_for_status(self):
        if self.status>=400:
            raise aiohttp.ClientResponseError(None,(),status=self.status,message='synthetic')


class Session:
    def __init__(self,response):self.response=response;self.calls=0
    def get(self,*args,**kwargs):self.calls+=1;return self.response


class BookObservationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.c=CollectorV3(['btc'],Path(self.tmp.name));self.addCleanup(self.c.archive.close)

    def records(self):
        self.c.archive.close();out=[]
        for p in Path(self.tmp.name).glob('raw-*.gz'):
            with gzip.open(p,'rt') as f:out.extend(json.loads(line) for line in f)
        return out

    async def test_raw_response_survives_invalid_price_before_parser(self):
        body={'asset_id':'t','bids':[{'price':'bad','size':1}],'asks':[]}
        self.c.get=AsyncMock(return_value=body)
        self.c.books['t']={'received_ms':123,'bid':.3}
        await self.c.poll_book('t')
        self.assertEqual(self.c.books['t']['received_ms'],123)
        self.assertNotIn('t',self.c.micro.poly_books)
        rows=self.records()
        self.assertEqual(next(x['payload']['book'] for x in rows if x['source']=='polymarket_rest_book_response'),body)
        self.assertFalse(any(x['source']=='polymarket_rest_book' for x in rows))
        error=next(x['payload'] for x in rows if x['source']=='polymarket_book_attempt_error')
        self.assertEqual(error['requested_token_id'],'t');self.assertTrue(error['prior_book_retained'])

    async def test_wrong_top_level_and_missing_size_keep_evidence(self):
        for body in (None,[],{'bids':[{'price':'.4'}]}):
            self.c.get=AsyncMock(return_value=body)
            await self.c.poll_book('t')
        rows=self.records()
        self.assertEqual(sum(x['source']=='polymarket_rest_book_response' for x in rows),3)
        self.assertEqual(sum(x['source']=='polymarket_book_attempt_error' for x in rows),3)
        self.assertNotIn('t',self.c.books)

    async def test_successful_empty_response_remains_distinct_from_failure(self):
        self.c.get=AsyncMock(return_value={'asset_id':'t','bids':[],'asks':[]})
        await self.c.poll_book('t');rows=self.records()
        self.assertIsNone(self.c.books['t']['bid'])
        self.assertTrue(any(x['source']=='polymarket_rest_book' for x in rows))
        self.assertFalse(any(x['source']=='polymarket_book_attempt_error' for x in rows))

    async def test_success_http_identity_timing_hash_and_header_allowlist(self):
        body=b'{"asset_id":"t","bids":[],"asks":[]}'
        self.c.session=Session(Response(body))
        await self.c.poll_book('t');rows=self.records()
        e=next(x['payload'] for x in rows if x['source']=='polymarket_book_http')
        self.assertEqual((e['requested_token_id'],e['status']),('t',200))
        self.assertEqual(e['captured_body_sha256'],hashlib.sha256(body).hexdigest())
        self.assertGreaterEqual(e['response_received_at_ns'],e['request_started_at_ns'])
        self.assertNotIn('Set-Cookie',e['headers']);self.assertNotIn('body_base64',e)

    async def test_non_json_body_preserved_and_never_becomes_empty_book(self):
        body=b'schema/proxy failure';self.c.session=Session(Response(body))
        with self.assertRaises(json.JSONDecodeError):await self.c.get(CLOB+'/book',{'token_id':'t'})
        e=self.records()[-1]['payload']
        self.assertEqual(base64.b64decode(e['body_base64']),body)
        self.assertEqual(e['status'],200);self.assertEqual(e['error_type'],'JSONDecodeError')

    async def test_wrong_content_type_does_not_relax_existing_json_contract(self):
        response=Response(b'{}');response.headers['Content-Type']='text/html'
        self.c.session=Session(response)
        with self.assertRaises(aiohttp.ContentTypeError):await self.c.get(CLOB+'/book',{'token_id':'t'})
        e=self.records()[-1]['payload']
        self.assertEqual(e['status'],200);self.assertEqual(base64.b64decode(e['body_base64']),b'{}')

    async def test_denial_preserves_status_body_and_no_retry_during_cooldown(self):
        self.c.session=Session(Response(b'forbidden',403))
        with self.assertRaises(aiohttp.ClientResponseError):await self.c.get(CLOB+'/book',{'token_id':'t'})
        with self.assertRaises(RuntimeError):await self.c.get(CLOB+'/book',{'token_id':'t'})
        self.assertEqual(self.c.session.calls,1)
        records=self.records();first,last=[x['payload'] for x in records if x['source']=='polymarket_book_http']
        self.assertEqual(first['status'],403);self.assertEqual(base64.b64decode(first['body_base64']),b'forbidden')
        self.assertIsNone(last['status']);self.assertEqual(last['not_sent_reason'],'server_directed_cooldown')

    async def test_timeout_has_unknown_http_status(self):
        self.c.session=Session(Response(b''))
        self.c.session.get=lambda *a,**kw:(_ for _ in ()).throw(asyncio.TimeoutError())
        with self.assertRaises(asyncio.TimeoutError):await self.c.get(CLOB+'/book',{'token_id':'t'})
        e=self.records()[-1]['payload'];self.assertIsNone(e['status']);self.assertIsNone(e['response_received_at_ns'])

    async def test_oversize_body_is_bounded_and_never_parsed(self):
        self.c.session=Session(Response(b'x'*(BOOK_RESPONSE_LIMIT+5)))
        with self.assertRaisesRegex(ValueError,'size bound'):await self.c.get(CLOB+'/book',{'token_id':'t'})
        e=self.records()[-1]['payload'];self.assertTrue(e['body_truncated'])
        self.assertEqual(len(base64.b64decode(e['body_base64'])),BOOK_RESPONSE_LIMIT+1)


if __name__=='__main__':unittest.main()
