"""Keep an unknown legacy evidence time unknown, without raising or inventing it."""
import unittest
from microstructure_math_v3 import event_reference

class MissingReceiptTests(unittest.TestCase):
    def test_missing_event_receipt_does_not_break_existing_conflict_rejection(self):
        market={'slug':'fixture','conditionId':'condition-fixture'}
        reference={'slug':'fixture','condition_id':'condition-fixture',
            'metadata_received_ms':1000,'published_price_to_beat':'100',
            'price_to_beat_path':'fixture.compact','price_to_beat_received_ms':900}
        detail={'payload':{'slug':'fixture','markets':[market],
            'eventMetadata':{'priceToBeat':'101'}}}
        result=event_reference(market,reference,detail)
        self.assertIsNone(result['published_price_to_beat'])
        self.assertTrue(result['price_to_beat_conflict'])
        evidence=result.pop('price_to_beat_conflict_evidence')
        self.assertIsNone(evidence['observations'][1]['recorded_received_ms'])
        expected={**reference,'published_price_to_beat':None,
            'price_to_beat_path':None,'price_to_beat_received_ms':None,'price_to_beat_conflict':True}
        self.assertEqual(result,expected)
        self.assertEqual(reference['published_price_to_beat'],'100')

if __name__=='__main__':unittest.main()
