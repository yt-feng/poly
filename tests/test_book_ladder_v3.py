"""Synthetic public-book contract; no account, network or order capability."""
from decimal import Decimal
import copy,importlib.util,json,sys,unittest
from pathlib import Path
from microstructure_math_v3 import book_features,sweep

class LadderTests(unittest.TestCase):
    def book(self):
        return {'bids':[['0.42','2'],['0.40','30'],['0.41','8']],
                'asks':[['0.45','4'],['0.43','6'],['0.44','9']]}
    def test_binary_snapshot_contains_bounded_ladder(self):
        a=book_features(self.book(),True)['observed_ladder']
        self.assertEqual(a['schema_version'],1);self.assertEqual(a['max_levels_per_side'],20)
    def test_bid_descending_and_ask_ascending(self):
        a=book_features(self.book(),True)['observed_ladder']
        self.assertEqual([x[0] for x in a['bids']],['0.42','0.41','0.40'])
        self.assertEqual([x[0] for x in a['asks']],['0.43','0.44','0.45'])
    def test_sweep_can_use_more_than_first_level(self):
        b=self.book();a=book_features(b,True)['observed_ladder'];f={'known':True,'rate':'0.07'}
        self.assertEqual(sweep(b,'bids',7,f),sweep(a,'bids',7,f))
        self.assertEqual(sweep(b,'asks',8,f),sweep(a,'asks',8,f))
    def test_prices_are_not_inferred_from_aggregate_depth(self):
        b={'bids':[['.8','1'],['.6','10']], 'asks':[['.9','10']]}
        a=book_features(b,True)['observed_ladder'];self.assertEqual(Decimal(a['bids'][1][0]),Decimal('.6'))
    def test_duplicate_prices_match_existing_normalization(self):
        b=self.book();b['bids'].append(['.42','3.125'])
        a=book_features(b,True);self.assertEqual(a['observed_ladder']['bids'][0][1],'5.125')
        self.assertEqual(a['bid_size'],5.125)
    def test_decimal_quantity_precision_not_converted_to_float(self):
        b={'bids':[['.4','0.123456789012345678901']], 'asks':[['.5','1']]}
        self.assertEqual(book_features(b,True)['observed_ladder']['bids'][0][1],'0.123456789012345678901')
    def test_empty_book_is_empty_not_zero_quote(self):
        a=book_features({},True);self.assertEqual(a['observed_ladder']['bids'],[]);self.assertIsNone(a['bid'])
    def test_one_sided_book_remains_one_sided(self):
        a=book_features({'asks':[['.2','1']]},True)
        self.assertFalse(a['two_sided']);self.assertEqual(a['observed_ladder']['bids'],[])
    def test_truncation_is_explicit(self):
        b={'bids':[[str(Decimal(i)/100),'1'] for i in range(1,25)]}
        a=book_features(b,True);self.assertEqual(len(a['observed_ladder']['bids']),20)
        self.assertTrue(a['observed_ladder']['bids_truncated']);self.assertEqual(a['bid_levels'],24)
    def test_exact_cap_is_not_truncated(self):
        b={'bids':[[str(Decimal(i)/100),'1'] for i in range(1,21)]}
        self.assertFalse(book_features(b,True)['observed_ladder']['bids_truncated'])
    def test_unseen_tail_is_not_filled(self):
        b={'bids':[[str(Decimal(i)/100),'1'] for i in range(1,25)]}
        a=book_features(b,True)['observed_ladder'];q=sweep(a,'bids',21,{'known':True,'rate':'0.07'})
        self.assertFalse(q['sufficient']);self.assertEqual(q['filled_shares'],20)
    def test_nonbinary_sources_unchanged_schema(self):
        self.assertNotIn('observed_ladder',book_features(self.book(),False))
    def test_invalid_levels_are_not_promoted(self):
        b={'bids':[['NaN','2'],['Infinity','1'],['1','2'],['.4','0'],['.3','2']]}
        self.assertEqual(book_features(b,True)['observed_ladder']['bids'],[['0.3','2']])
    def test_crossed_book_not_execution_approved(self):
        a=book_features({'bids':[['.6','1']], 'asks':[['.5','1']]},True)
        self.assertTrue(a['crossed']);self.assertFalse(a['observed_ladder']['execution_certified'])
    def test_no_input_mutation(self):
        b=self.book();old=copy.deepcopy(b);book_features(b,True);self.assertEqual(b,old)
    def test_output_independent_of_later_input_mutation(self):
        b=self.book();a=book_features(b,True);old=copy.deepcopy(a);b['bids'][0][1]='999';self.assertEqual(a,old)
    def test_json_serializable(self):
        self.assertEqual(json.loads(json.dumps(book_features(self.book(),True))),book_features(self.book(),True))
    def test_existing_summary_values_not_changed(self):
        b=self.book();a=book_features(b,True);bids=a['observed_ladder']['bids']
        self.assertEqual(a['bid'],float(bids[0][0]));self.assertEqual(a['bid_depth5'],sum(float(x[1]) for x in bids))
if __name__=='__main__':unittest.main()
