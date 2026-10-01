"""Synthetic importer provenance tests. No network, account or trading calls."""
import copy
from datetime import datetime, timezone
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from microstructure_math_v3 import market_reference, event_reference, decimal
from official_reference_v3 import (OFFICIAL_PRICE_URL, page_reference,
    opening_price_request, parse_opening_price_response, client_price_reference)

START = 1767225600000
SLUG = 'btc-updown-5m-' + str(START//1000)
CONDITION = '0x' + '1'*64
KEY = 'price_to_beat_conflict_evidence'


def market(value='100.00'):
    iso=lambda ms:datetime.fromtimestamp(ms/1000,timezone.utc).isoformat().replace('+00:00','Z')
    return dict(slug=SLUG,conditionId=CONDITION,
        eventStartTime=iso(START),endDate=iso(START+300000),
        cryptoMarketConfig=dict(asset='btc',duration='5m',twapEnabled=True,twapLookbackSeconds=60),
        events=[dict(slug=SLUG,eventMetadata=dict(priceToBeat=value))],
        feesEnabled=False,active=True,closed=False,acceptingOrders=True,
        orderMinSize=5,orderPriceMinTickSize=.01)


def event_detail(value='101.25', received=START+1500):
    return dict(received_ms=received,payload=dict(slug=SLUG,
        markets=[dict(slug=SLUG,conditionId=CONDITION)],eventMetadata=dict(priceToBeat=value)))


def page_detail(value='101.25'):
    return dict(slug=SLUG,condition_id=CONDITION,twap_lookback_seconds=60,
        received_ms=START+1500,published_price_to_beat=value,
        source_url='https://polymarket.com/event/'+SLUG)


def client_detail(m,r,value=101.25):
    request=opening_price_request(m,r,START+1000,slug=SLUG)
    return parse_opening_price_response(dict(openPrice=value,closePrice=None,
        completed=False,incomplete=True,cached=True,timestamp=START+1000),
        m,r,request,START+1500)


def legacy_page(m,r,d):
    r=dict(r)
    if (not d or d['received_ms']>r['metadata_received_ms'] or d['slug']!=m.get('slug')
        or d['condition_id']!=m.get('conditionId')
        or d['twap_lookback_seconds']!=r.get('twap_lookback_seconds') or r.get('price_to_beat_conflict')):
        return r
    v=decimal(d['published_price_to_beat'])
    if v is None or v<=0:return r
    old=decimal(r.get('published_price_to_beat'))
    if old is not None and old!=v:
        r.update(published_price_to_beat=None,price_to_beat_path=None,price_to_beat_received_ms=None,price_to_beat_conflict=True)
    elif old is None:
        r.update(published_price_to_beat=str(v),price_to_beat_path='polymarket.event_page.crypto-prices.openPrice',
            price_to_beat_source_url=d['source_url'],price_to_beat_received_ms=d['received_ms'])
    return r


def legacy_event(m,r,d):
    r=dict(r)
    if not d or d.get('received_ms',0)>r['metadata_received_ms']:return r
    e=d.get('payload')
    if not isinstance(e,dict) or e.get('slug')!=m.get('slug'):return r
    matches=[x for x in e.get('markets',[]) if x.get('slug')==m.get('slug') and x.get('conditionId')==m.get('conditionId') and x.get('conditionId')]
    if not matches:return r
    meta=e.get('eventMetadata') or {}
    v=decimal(meta.get('priceToBeat')) if isinstance(meta,dict) else None
    if v is not None and v>0:
        if r.get('published_price_to_beat') is not None and decimal(r['published_price_to_beat'])!=v:
            r.update(published_price_to_beat=None,price_to_beat_path=None,price_to_beat_received_ms=None,price_to_beat_conflict=True)
        else:r.update(published_price_to_beat=str(v),price_to_beat_path='gamma.events/slug/{slug}.eventMetadata.priceToBeat',price_to_beat_received_ms=d['received_ms'])
    return r


class ConflictEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.m=market();self.r=market_reference(self.m,START+2000)
    def conflict(self):return page_reference(self.m,self.r,page_detail())
    def test_page_preserves_both_values_before_clear(self):
        out=self.conflict();self.assertIsNone(out['published_price_to_beat'])
        self.assertEqual([x['value'] for x in out[KEY]['observations']],['100.00','101.25'])
    def test_gamma_full_event_preserves_both_values(self):
        out=event_reference(self.m,self.r,event_detail())
        self.assertEqual([x['value'] for x in out[KEY]['observations']],['100.00','101.25'])
        self.assertTrue(out['price_to_beat_conflict'])
    def test_client_preserves_correct_source_label(self):
        out=client_price_reference(self.m,self.r,client_detail(self.m,self.r))
        self.assertEqual(out[KEY]['observations'][1]['path'],'polymarket.client_api.crypto-price.openPrice')
    def test_recorded_times_not_invented(self):
        e=self.conflict()[KEY];self.assertEqual(e['evaluated_at_ms'],START+2000)
        self.assertEqual([x['recorded_received_ms'] for x in e['observations']],[START+2000,START+1500])
    def test_identifies_market(self):
        e=self.conflict()[KEY];self.assertEqual(e['slug'],SLUG);self.assertEqual(e['condition_id'],CONDITION)
    def test_does_not_pick_a_winner(self):
        e=self.conflict()[KEY];self.assertIsNone(e['selected_source']);self.assertEqual(e['upstream_cause'],'not_established')
        self.assertFalse(e['external_truth_authenticated'])
    def test_equal_numeric_values_not_conflict(self):
        out=page_reference(self.m,self.r,page_detail('100.0'));self.assertNotIn(KEY,out);self.assertEqual(out,self.r)
    def test_decimal_precision_is_preserved(self):
        d=page_detail('100.00000000000000000001')
        self.assertEqual(page_reference(self.m,self.r,d)[KEY]['observations'][1]['value'],d['published_price_to_beat'])
    def test_missing_price_is_not_disagreement(self):
        for v in (None,'NaN','Infinity','-1','0','bad'):
            with self.subTest(v=v):self.assertEqual(page_reference(self.m,self.r,page_detail(v)),self.r)
    def test_missing_detail_unchanged(self):self.assertEqual(page_reference(self.m,self.r,None),self.r)
    def test_future_page_detail_not_recorded(self):
        d=page_detail();d['received_ms']=START+2001;self.assertEqual(page_reference(self.m,self.r,d),self.r)
    def test_future_event_detail_not_recorded(self):
        self.assertEqual(event_reference(self.m,self.r,event_detail(received=START+2001)),self.r)
    def test_wrong_condition_not_recorded(self):
        d=page_detail();d['condition_id']='0x'+'2'*64;self.assertEqual(page_reference(self.m,self.r,d),self.r)
    def test_wrong_slug_not_recorded(self):
        d=page_detail();d['slug']='btc-updown-5m-0';self.assertEqual(page_reference(self.m,self.r,d),self.r)
    def test_wrong_lookback_not_recorded(self):
        d=page_detail();d['twap_lookback_seconds']=30;self.assertEqual(page_reference(self.m,self.r,d),self.r)
    def test_future_client_detail_not_recorded(self):
        d=client_detail(self.m,self.r);d['received_ms']=START+2001
        self.assertEqual(client_price_reference(self.m,self.r,d),self.r)
    def test_invalid_client_request_not_recorded(self):
        d=client_detail(self.m,self.r);d['params']['symbol']='ETH'
        self.assertEqual(client_price_reference(self.m,self.r,d),self.r)
    def test_upstream_conflict_not_resurrected_by_page(self):
        r=event_reference(self.m,self.r,event_detail())
        self.assertEqual(page_reference(self.m,r,page_detail('100')),r)
    def test_upstream_conflict_not_resurrected_by_client(self):
        r=event_reference(self.m,self.r,event_detail());d=client_detail(self.m,self.r)
        self.assertEqual(client_price_reference(self.m,r,d),r)
    def test_does_not_mutate_inputs(self):
        d=page_detail();before=copy.deepcopy((self.m,self.r,d));page_reference(self.m,self.r,d)
        self.assertEqual((self.m,self.r,d),before)
    def test_evidence_detached_from_later_inputs(self):
        d=page_detail();out=page_reference(self.m,self.r,d);before=copy.deepcopy(out)
        d['published_price_to_beat']='999';self.r['price_to_beat_path']='changed';self.assertEqual(out,before)
    def test_bounded_json_without_raw_html(self):
        out=self.conflict();payload=json.dumps(out[KEY]);self.assertEqual(json.loads(payload),out[KEY]);self.assertLess(len(payload),1800)
        self.assertEqual(len(out[KEY]['observations']),2)
    def test_legacy_page_fields_identical(self):
        for old in (None,'100','100.0','101'):
            for new in ('100','101','NaN',None,'0'):
                with self.subTest(old=old,new=new):
                    m=market(old);r=market_reference(m,START+2000);d=page_detail(new)
                    actual=page_reference(m,r,d);actual.pop(KEY,None);self.assertEqual(actual,legacy_page(m,r,d))
    def test_legacy_event_fields_identical(self):
        for old in (None,'100','100.0','101'):
            for new in ('100','101','NaN',None,'0'):
                with self.subTest(old=old,new=new):
                    m=market(old);r=market_reference(m,START+2000);d=event_detail(new)
                    actual=event_reference(m,r,d);actual.pop(KEY,None);self.assertEqual(actual,legacy_event(m,r,d))
    def test_existing_collector_set_market_carries_event_pair(self):
        from microstructure_v3 import Microstructure
        c=SimpleNamespace(markets={});micro=Microstructure(c);micro.event_details[SLUG]=event_detail()
        with patch('microstructure_v3.time.time',return_value=(START+2000)/1000):micro.set_market(self.m,START+2000)
        self.assertIsNone(micro.rules[SLUG]['published_price_to_beat']);self.assertIn(KEY,micro.rules[SLUG])
    def test_existing_collector_set_market_carries_client_pair(self):
        from microstructure_v3 import Microstructure
        c=SimpleNamespace(markets={});micro=Microstructure(c);micro.client_price_details[SLUG]=client_detail(self.m,self.r)
        with patch('microstructure_v3.time.time',return_value=(START+2000)/1000):micro.set_market(self.m,START+2000)
        out=micro.rules[SLUG];self.assertIsNone(out['published_price_to_beat']);self.assertIn(KEY,out)
    def test_no_conflict_no_new_evidence(self):
        m=market(None);r=market_reference(m,START+2000);out=page_reference(m,r,page_detail())
        self.assertEqual(out['published_price_to_beat'],'101.25');self.assertNotIn(KEY,out)
    def test_prior_output_not_changed_by_next_refresh(self):
        from microstructure_v3 import Microstructure
        c=SimpleNamespace(markets={});micro=Microstructure(c)
        with patch('microstructure_v3.time.time',return_value=(START+2000)/1000):
            micro.set_market(self.m,START+2000);old=micro.rules[SLUG];saved=copy.deepcopy(old)
            micro.event_details[SLUG]=event_detail();micro.set_market(self.m,START+3000)
        self.assertEqual(old,saved);self.assertNotIn(KEY,old);self.assertIn(KEY,micro.rules[SLUG])

if __name__=='__main__':unittest.main()
