from __future__ import annotations
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from decimal import Decimal
from collections import Counter
from microstructure_math_v3 import *
from microstructure_v3 import Microstructure, fresh
from capture_v3 import CollectorV3
from quality_v2 import summarize

B = {'bids':[['.40','10'],['.39','20']], 'asks':[['.42','10'],['.43','20']]}
FEE = {'known':True,'rate':'0.07','exponent':1}


class MathTests(unittest.TestCase):
    def test_reject_nan(self):
        self.assertIsNone(number('nan')); self.assertIsNone(decimal('inf'))
    def test_book_sorted(self):
        b = {'bids':[['.1','9'],['.4','10']], 'asks':[['.8','2'],['.5','4']]}
        f=book_features(b,True); self.assertEqual(f['bid'],.4);self.assertEqual(f['ask'],.5)
    def test_duplicate_levels_aggregated(self):
        self.assertEqual(levels({'bids':[['.4','10'],['.4','3']]},'bids'),[(Decimal('.4'),Decimal(13))])
    def test_zero_size_ignored(self):
        self.assertEqual(levels({'asks':[['.4','0']]},'asks'),[])
    def test_binary_bounds(self):
        self.assertEqual(levels({'asks':[['1.1','3']]},'asks',True),[])
    def test_empty_book_is_not_zero_price(self):
        f=book_features({},True);self.assertIsNone(f['mid']);self.assertFalse(f['two_sided'])
    def test_crossed(self):
        f=book_features({'bids':[['.6','1']],'asks':[['.5','1']]},True)
        self.assertTrue(f['crossed']);self.assertIsNone(f['microprice'])
    def test_microprice(self):
        f=book_features({'bids':[['.4','3']],'asks':[['.5','1']]})
        self.assertAlmostEqual(f['microprice'],.475);self.assertAlmostEqual(f['imbalance1'],.5)
    def test_depth20_not_20x_l1(self):
        f=book_features(B);self.assertEqual(f['bid_depth20'],30)
    def test_unknown_fees(self):
        self.assertFalse(fee_config({'takerBaseFee':1000})['known'])
    def test_legacy_bps_not_rate(self):
        self.assertFalse(fee_config({'feesEnabled':True,'takerBaseFee':1000})['known'])
    def test_unknown_exponent(self):
        self.assertFalse(fee_config({'feesEnabled':True,'feeSchedule':{'rate':.07,'exponent':2}})['known'])
    def test_current_fees(self):
        f=fee_config({'feesEnabled':True,'feeSchedule':{'rate':.07,'exponent':1}})
        self.assertTrue(f['known'])
    def test_free_fee(self):
        self.assertEqual(fee_config({'feesEnabled':False})['rate'],'0')
    def test_fee_midpoint(self):
        b={'asks':[['.5','100']]};q=sweep(b,'asks',100,FEE)
        self.assertEqual(q['notional'],50);self.assertAlmostEqual(q['estimated_fee'],1.75)
    def test_sweep_multiple_levels(self):
        q=sweep(B,'asks',20,FEE);self.assertAlmostEqual(q['vwap'],.425)
    def test_sweep_partial(self):
        q=sweep(B,'asks',100,FEE);self.assertFalse(q['sufficient']);self.assertEqual(q['filled_shares'],30)
    def test_sweep_unknown_fee(self):
        self.assertIsNone(sweep(B,'asks',10,{'known':False})['estimated_fee'])
    def test_sweep_negative(self):
        with self.assertRaises(ValueError):sweep(B,'asks',-1,FEE)
    def test_pair_stale_null(self):
        q=pair_quotes(B,B,FEE,False,(10,))[0]
        self.assertIsNone(q['buy_complete_set_edge_before_other_costs'])
    def test_pair_after_fees(self):
        q=pair_quotes(B,B,FEE,True,(10,))[0]
        self.assertAlmostEqual(q['buy_complete_set_edge_before_other_costs'],10-8.4-2*(10*.07*.42*.58))
    def test_partial_pair_not_executable(self):
        q=pair_quotes(B,B,FEE,True,(100,))[0]
        self.assertIsNone(q['buy_complete_set_edge_before_other_costs'])
    def test_ofi_same_prices(self):
        p={'bid':.4,'ask':.5,'bid_size':10,'ask_size':20}
        n={**p,'bid_size':12,'ask_size':17}
        self.assertEqual(ofi(p,n),5)
    def test_ofi_no_previous(self):self.assertIsNone(ofi(None,{}))
    def test_twap_exact(self):
        self.assertEqual(twap_price({'full_accuracy_value':'65000500000000000000001','value':1}),'65000.500000000000000001')
    def test_no_twap_value(self):self.assertIsNone(twap_price({}))
    def test_bad_twap(self):self.assertIsNone(twap_price({'value':'NaN'}))
    def test_rule_match_slug(self):
        m={'slug':'a','events':[{'slug':'b','eventMetadata':{'priceToBeat':100}}]}
        self.assertIsNone(market_reference(m,10)['published_price_to_beat'])
    def test_rule_price_origin(self):
        m={'slug':'a','events':[{'slug':'a','eventMetadata':{'priceToBeat':100}}]}
        r=market_reference(m,10);self.assertEqual(r['published_price_to_beat'],'100');self.assertEqual(r['metadata_received_ms'],10)
    def test_rule_twap(self):
        r=market_reference({'resolutionSource':'https://data.chain.link/streams/btc-usd-twap-60s-streams'},1)
        self.assertEqual(r['reference_kind'],'chainlink_twap_60')
    def test_rule_no_assumption(self):
        self.assertEqual(market_reference({},1)['reference_kind'],'unknown')
    def test_rule_change_hash(self):
        self.assertNotEqual(market_reference({'description':'a'},1)['rules_sha256'],market_reference({'description':'b'},1)['rules_sha256'])
    def test_fresh_receive(self):self.assertFalse(fresh({'received_ms':100},10000,1000))
    def test_future_receive(self):self.assertFalse(fresh({'received_ms':200},100,1000))
    def test_fresh_missing_event(self):self.assertFalse(fresh({'received_ms':100},100,1000,True))
    def test_future_event(self):self.assertFalse(fresh({'received_ms':100,'event_ms':5000},100,1000,True))


class FlowTests(unittest.TestCase):
    def test_sign(self):
        f=TradeFlow();f.add(1000,1,100,2,1);f.add(1100,2,100,1,-1)
        self.assertEqual(f.summarize(1200,True)['1s']['signed_notional'],100)
    def test_duplicate(self):
        f=TradeFlow();f.add(1000,1,100,2,1);self.assertFalse(f.add(1001,1,100,2,1));self.assertEqual(len(f.rows),1)
    def test_gap(self):
        f=TradeFlow();f.add(1000,1,100,1,1);f.add(1100,4,100,1,1)
        self.assertTrue(f.summarize(1200,True)['1s']['recent_id_gap_or_overflow'])
    def test_no_future(self):
        f=TradeFlow();f.add(1500,1,100,1,1)
        self.assertEqual(f.summarize(1000,True)['1s']['messages'],0)
    def test_warmup(self):
        f=TradeFlow();f.add(1000,1,100,1,1);self.assertFalse(f.summarize(1100,True)['60s']['warmed'])
    def test_overflow(self):
        f=TradeFlow(1);f.add(1000,1,100,1,1);f.add(1100,2,100,1,1);self.assertEqual(f.last_gap,1100)
    def test_invalid_sign(self):self.assertFalse(TradeFlow().add(1,1,100,1,0))
    def test_disconnected_not_complete(self):
        s=TradeFlow().summarize(1,False);self.assertFalse(s['complete_trade_tape']);self.assertFalse(s['connected'])
    def test_rolling_warmup(self):
        f=RollingPrices();f.add(1000,100);self.assertIsNone(f.summarize(1000)['60s']['realized_vol_bps'])
    def test_rolling_vol(self):
        f=RollingPrices()
        for i in range(62):f.add(i*1000,100+i)
        self.assertGreater(f.summarize(61000)['60s']['realized_vol_bps'],0)
    def test_rolling_exact_intervals(self):
        f=RollingPrices()
        for i in range(62):f.add(i*1000,100+i)
        self.assertEqual(f.summarize(61000)['60s']['valid_intervals'],60)
        self.assertEqual(f.summarize(61000)['5s']['valid_intervals'],5)
    def test_rolling_missing_price_invalidates(self):
        f=RollingPrices()
        for i in range(62):f.add(i*1000,None if i==35 else 100+i)
        self.assertFalse(f.summarize(61000)['60s']['warmed'])
    def test_rolling_gaps(self):
        f=RollingPrices();f.add(0,100);f.add(60000,110)
        self.assertFalse(f.summarize(60000)['60s']['warmed'])


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.c=CollectorV3(['btc'],Path(self.tmp.name))
    def tearDown(self):
        self.c.archive.close();self.tmp.cleanup()
    def test_causal_metadata_before_known(self):
        self.c.snapshot('btc',time.monotonic(),time.monotonic())
        self.assertEqual(self.c.micro.source_valid['rules'],0)
    def test_core_sampler_compat(self):
        self.c.snapshot('btc',time.monotonic(),time.monotonic());self.assertIn('microstructure',self.c.health())
        self.assertEqual(self.c.valid['poly'],0)
    def test_raw_receipt_clock(self):
        self.c.raw('example',{'a':1});self.assertEqual(self.c.counts['example'],1)
    def test_coinbase_maker_sign(self):
        self.c.micro.coinbase_message({'type':'match','product_id':'BTC-USD','trade_id':1,'price':'100','size':'2','side':'sell'},'c')
        self.assertEqual(self.c.micro.flows['coinbase'].total_signed,200)
    def test_coinbase_size_zero_deletes(self):
        self.c.micro.coinbase_message({'type':'snapshot','product_id':'BTC-USD','bids':[['100','2']],'asks':[['101','3']]},'c')
        self.c.micro.coinbase_message({'type':'l2update','product_id':'BTC-USD','changes':[['buy','100','0']]},'c')
        self.assertEqual(self.c.micro.coinbase_book['bids'],{})
    def test_coinbase_price_key_normalization(self):
        self.c.micro.coinbase_message({'type':'snapshot','product_id':'BTC-USD','bids':[['100.00','2']],'asks':[['101','3']]},'c')
        self.c.micro.coinbase_message({'type':'l2update','product_id':'BTC-USD','changes':[['buy','100','0']]},'c')
        self.assertEqual(self.c.micro.coinbase_book['bids'],{})
    def test_array_external_message_rejected(self):
        with self.assertRaises(ValueError):self.c.micro.external_message('chainlink_twap',[],'c')
    def test_coinbase_updates_need_snapshot(self):
        self.c.micro.coinbase_message({'type':'l2update','product_id':'BTC-USD','changes':[['buy','100','1']]},'c')
        self.assertEqual(self.c.micro.coinbase_book,{})
    def test_reconnect_resets_book(self):
        self.c.micro.coinbase_book={'bids':{}}
        self.c.raw('connection',{'source':'coinbase','state':'connected'},connection_id='new')
        self.assertEqual(self.c.micro.coinbase_book,{})
    def test_stale_connection_not_valid(self):
        ms=int(time.time()*1000)
        self.c.connected['x']=True;self.c.micro.connections['x']='new'
        self.c.micro.external['z']={'received_ms':ms,'connection_id':'old'}
        self.assertFalse(self.c.micro.valid_external('z','x',ms))
    def test_twap_window_mismatch(self):
        with self.assertRaises(ValueError):
            self.c.micro.external_message('chainlink_twap',{'topic':'crypto_prices_twap_sixty','payload':{'symbol':'btc/usd','window_s':30,'value':100}},'c')
    def test_spot_not_twap(self):
        self.c.micro.external_message('chainlink_twap',{'topic':'crypto_prices_chainlink','payload':{'symbol':'btc/usd','value':100}},'c')
        self.assertNotIn('twap60',self.c.micro.external)
    def test_markout_delayed(self):
        ms=int(time.time()*1000)
        row={'sample_ms':ms,'slug':'s','poly_valid':True,'binance_valid':True,'binance':{'price':'100'}}
        self.c.micro.markout_labels(row,{'poly_up':{'mid':.5}})
        self.assertEqual(len(self.c.micro.markouts),1);self.assertNotIn('labels',self.c.archive.opened)
        row['sample_ms']+=1000
        self.c.micro.markout_labels(row,{'poly_up':{'mid':.55}})
        self.assertIn('labels',self.c.archive.opened)
    def test_quality_extra(self):
        r=summarize([{'sample_ms':1790097000000,'asset':'btc','microstructure':{},'twap60_valid':True}])
        self.assertEqual(r['daily'][0]['valid_twap60'],1);self.assertEqual(r['daily'][0]['microstructure_rows'],1)
    def test_quality_old_schema(self):
        r=summarize([{'sample_ms':1790097000000,'asset':'btc'}])
        self.assertEqual(r['daily'][0]['valid_twap60'],0)


if __name__ == '__main__':unittest.main()
