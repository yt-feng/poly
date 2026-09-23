import asyncio
import tempfile
import unittest
from pathlib import Path
from market_ws_guard import MarketWSGuard, book_fingerprint, book_tokens, market_socket
from capture_v3 import CollectorV3


def book(token='a', size='5', stamp='1'):
    return {'asset_id':token, 'hash':stamp, 'timestamp':stamp,
            'bids':[{'price':'0.49','size':size}], 'asks':[{'price':'0.51','size':'6'}]}


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.g=MarketWSGuard(); self.g.connect('c'); self.g.subscriptions({'a','b'},100)
    def changed(self, start=100, stop=125, token='a'):
        for t in range(start, stop+1):self.g.rest_book(book(token,str(t)),t)
    def test_true_silence(self):
        self.changed();self.assertEqual(self.g.suspect_tokens({'a'},125),['a'])
    def test_pong_not_book(self):
        self.changed();self.g.pong_at=125
        self.g.ws_book({'event_type':'PONG'},125)
        self.assertIsNotNone(self.g.request_reconnect({'a'},125))
    def test_quiet_market_not_reconnected(self):
        for t in range(100,130):self.g.rest_book(book(stamp=str(t)),t)
        self.assertIsNone(self.g.request_reconnect({'a'},129))
    def test_format_changes_not_evidence(self):
        a=book();b=book(size='5.00',stamp='different')
        b['bids'][0]['price']='.4900'
        self.assertEqual(book_fingerprint(a),book_fingerprint(b))
    def test_rest_order_ignored(self):
        a=book();a['bids'].append({'price':'.48','size':'2'})
        b=book();b['bids']=list(reversed(a['bids']))
        self.assertEqual(book_fingerprint(a),book_fingerprint(b))
    def test_invalid_rest_not_evidence(self):
        self.assertIsNone(book_fingerprint({'hash':'x'}))
        self.assertIsNone(book_fingerprint(book(size='NaN')))
        self.assertIsNone(book_fingerprint(book(size='-1')))
    def test_unsubscribed_token_ignored(self):
        self.changed(token='z');self.assertIsNone(self.g.request_reconnect({'z'},125))
    def test_previous_window_not_trigger(self):
        self.changed();self.assertIsNone(self.g.request_reconnect({'b'},125))
    def test_other_token_does_not_mask_silence(self):
        self.changed();self.g.ws_book({'event_type':'book','asset_id':'b'},125)
        self.assertEqual(self.g.suspect_tokens({'a','b'},125),['a'])
    def test_trade_not_book(self):
        self.changed();self.g.ws_book({'event_type':'last_trade_price','asset_id':'a'},125)
        self.assertIsNotNone(self.g.request_reconnect({'a'},125))
    def test_actual_book_resets(self):
        self.changed();self.g.ws_book({'event_type':'book','asset_id':'a'},125)
        self.assertIsNone(self.g.request_reconnect({'a'},125))
    def test_connection_boundary_resets(self):
        self.g.ws_book({'event_type':'book','asset_id':'a'},100)
        self.g.connect('d');self.g.subscriptions({'a'},120)
        self.assertFalse(self.g.summary({'a'},120)['all_current_tokens_fresh'])
    def test_new_subscription_warmup(self):
        self.changed();self.g.subscriptions({'b'},125);self.g.subscriptions({'a','b'},126)
        self.assertIsNone(self.g.request_reconnect({'a'},127))
    def test_stale_rest_not_evidence(self):
        self.changed();self.assertIsNone(self.g.request_reconnect({'a'},140))
    def test_too_few_rest_changes(self):
        self.g.rest_book(book(size='1'),100);self.g.rest_book(book(size='2'),125)
        self.assertIsNone(self.g.request_reconnect({'a'},125))
    def test_rate_limit(self):
        self.changed();self.assertIsNotNone(self.g.request_reconnect({'a'},125))
        self.changed(126,180);self.assertIsNone(self.g.request_reconnect({'a'},180))
        self.changed(181,186);self.assertIsNotNone(self.g.request_reconnect({'a'},186))
    def test_old_changes_expire(self):
        self.changed();self.g.rest_book(book(size='125'),200)
        self.assertIsNone(self.g.request_reconnect({'a'},200))
    def test_arrays_and_nested_price_changes(self):
        p=[{'event_type':'book','asset_id':'a'},{'event_type':'price_change','price_changes':[{'asset_id':'b'},{'asset_id':'c'}]}]
        self.assertEqual(book_tokens(p),{'a','b','c'})
    def test_enveloped_tokens(self):
        self.assertEqual(book_tokens({'type':'price_change','payload':{'priceChanges':[{'tokenId':'a'}]}}),{'a'})
    def test_controls_unknown_not_book(self):
        for p in ['PONG', {}, [], {'event_type':'new_market','assets_ids':['a']},42]:
            self.assertEqual(book_tokens(p),set())
    def test_no_completeness_claim(self):
        self.assertFalse(self.g.summary({'a'},125)['event_completeness_certified'])
    def test_state_bound(self):
        self.changed(token='old');self.g.subscriptions({'b'},1000)
        self.assertNotIn('old',self.g.rest)
    def test_v3_integration_hooks(self):
        with tempfile.TemporaryDirectory() as d:
            c=CollectorV3(['btc'],Path(d));c.ws_guard.connect('x');c.ws_guard.subscriptions({'a'},100)
            c.raw('polymarket_rest_book',book());c.raw('polymarket_ws',{'event_type':'book','asset_id':'a'})
            self.assertIn('a',c.ws_guard.last_book)
            self.assertIn('a',c.ws_guard.rest);c.archive.close()


class SocketCancellation(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_cleans_maintenance_and_connection(self):
        import aiohttp
        class WS:
            def __init__(self):self.frames=[];self.text=[];self.closed=False
            async def __aenter__(self):return self
            async def __aexit__(self,*a):self.closed=True
            async def send_json(self,x):self.frames.append(x)
            async def send_str(self,x):self.text.append(x)
            async def receive(self):await asyncio.sleep(60)
            async def close(self,**kwargs):self.closed=True
        class Session:
            def ws_connect(self,*args,**kwargs):return ws
        ws=WS()
        with tempfile.TemporaryDirectory() as d:
            c=CollectorV3(['btc'],Path(d));c.session=Session();c.markets={'current':{'up':'a','down':'b'}}
            task=asyncio.create_task(market_socket(c,'wss://example.test',c.poly_message))
            await asyncio.sleep(.03);task.cancel();await asyncio.gather(task,return_exceptions=True)
            self.assertFalse(c.connected['polymarket']);self.assertTrue(ws.closed)
            self.assertEqual(ws.frames,[{'type':'market','assets_ids':['a','b'],'custom_feature_enabled':True}])
            self.assertEqual(ws.text,['PING']);self.assertEqual(c.ws_guard.subscribed,{})
            c.archive.close()
