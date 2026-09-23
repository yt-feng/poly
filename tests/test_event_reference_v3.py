import unittest
from microstructure_math_v3 import event_reference, market_reference, server_time_text


class EventTests(unittest.TestCase):
    def test_full_event_price(self):
        m={'slug':'s','conditionId':'c'}
        e={'slug':'s','eventMetadata':{'priceToBeat':'123.45'},'markets':[m]}
        r=event_reference(m,market_reference(m,30),{'payload':e,'received_ms':20})
        self.assertEqual(r['published_price_to_beat'],'123.45')
        self.assertEqual(r['price_to_beat_received_ms'],20)
    def test_full_event_wrong_condition(self):
        m={'slug':'s','conditionId':'c'}
        e={'slug':'s','eventMetadata':{'priceToBeat':100},'markets':[{'slug':'s','conditionId':'d'}]}
        self.assertIsNone(event_reference(m,market_reference(m,30),{'payload':e,'received_ms':20})['published_price_to_beat'])
    def test_full_event_future_not_visible(self):
        m={'slug':'s','conditionId':'c'}
        e={'slug':'s','eventMetadata':{'priceToBeat':100},'markets':[m]}
        self.assertIsNone(event_reference(m,market_reference(m,10),{'payload':e,'received_ms':20})['published_price_to_beat'])
    def test_full_event_conflict(self):
        m={'slug':'s','conditionId':'c','events':[{'slug':'s','eventMetadata':{'priceToBeat':110}}]}
        e={'slug':'s','eventMetadata':{'priceToBeat':100},'markets':[m]}
        r=event_reference(m,market_reference(m,30),{'payload':e,'received_ms':20})
        self.assertTrue(r['price_to_beat_conflict']);self.assertIsNone(r['published_price_to_beat'])
    def test_time_plain_numeric(self):
        self.assertEqual(server_time_text(' 1790129189\n'),1790129189)
    def test_time_reject_html(self):
        with self.assertRaises(ValueError):server_time_text('<html>1790129189</html>')
    def test_time_reject_json_object(self):
        with self.assertRaises(ValueError):server_time_text('{"serverTime":1790129189}')
