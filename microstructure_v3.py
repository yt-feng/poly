"""Bounded public-data enrichment for BTC five-minute research.

No orders, wallets, credentials, geography routing or paid data. Optional source
failures remain visible, never overwritten with another venue's observations.
"""
from __future__ import annotations
import asyncio
from collections import Counter, defaultdict, deque
import json
import math
import time
import aiohttp
from capture_v2 import CLOB, GAMMA, ASSETS, BINANCE_WS, CHAINLINK_WS, epoch_ms, window_slug
from microstructure_math_v3 import (number, decimal, book_features, fee_config, pair_quotes,
                                   market_reference, twap_price, TradeFlow, RollingPrices, ofi, event_reference)

FUTURES_REST = 'https://fapi.binance.com'
FUTURES_MARKET = 'wss://fstream.binance.com/market/stream?streams='
FUTURES_PUBLIC = 'wss://fstream.binance.com/public/stream?streams='
COINBASE_WS = 'wss://ws-feed.exchange.coinbase.com'
DATA_API = 'https://data-api.polymarket.com'
TOPICS = {'crypto_prices_twap_thirty': 30, 'crypto_prices_twap_sixty': 60}
OPTIONAL_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError, KeyError, TypeError)


def fresh(x, ms, ttl=5000, event=False):
    if not x or not 0 <= ms-x.get('received_ms', -10**15) <= ttl:
        return False
    return not event or (x.get('event_ms') is not None and -2000 <= ms-x['event_ms'] <= ttl)


def obs(payload, ms, event_ms=None, **kwargs):
    return dict(payload=payload, received_ms=ms, event_ms=event_ms, **kwargs)


class Microstructure:
    def __init__(self, collector):
        self.c = collector
        self.rules, self.poly_books, self.external, self.flows = {}, {}, {}, {}
        self.coinbase_book, self.previous_books = {}, {}
        self.price_history = defaultdict(RollingPrices)
        self.source_valid = Counter()
        self.last_valid, self.errors, self.latency = {}, {}, defaultdict(lambda: deque(maxlen=300))
        self.connections, self.market_due, self.clob_due = {}, {}, {}
        self.related = None
        self.event_details = {}
        self.labels_seen, self.markouts = {}, deque()
        self.clock, self.last_data_trade = {}, {}
        self.last_liquidation = None
        self.parse_errors = 0

    def ingest(self, source, payload, connection_id, event_ms, received_ms):
        """Existing v2 raw events are reused, not requested a second time."""
        if source == 'connection':
            s = payload.get('source')
            if payload.get('state') == 'connected':
                self.connections[s] = connection_id
                if s == 'coinbase':
                    self.coinbase_book = {}
                    self.flows['coinbase'] = TradeFlow()
                if s == 'perp_market':
                    self.flows['perp'] = TradeFlow()
                if s == 'binance_btc':
                    self.flows['spot'] = TradeFlow()
            return
        if source == 'polymarket_metadata':
            self.set_market(payload['market'], received_ms)
        elif source == 'polymarket_rest_book':
            token = str(payload.get('asset_id', payload.get('token_id', '')))
            if token:
                self.poly_books[token] = obs(payload, received_ms, event_ms)
        elif source == 'binance_spot_ws':
            p = payload.get('data', payload)
            if p.get('e') == 'trade' and p.get('s') == 'BTCUSDT':
                f = self.flows.setdefault('spot', TradeFlow())
                f.add(received_ms, p.get('t'), p.get('p'), p.get('q'), -1 if p.get('m') is True else 1)
        elif source == 'polymarket_ws':
            events = payload if isinstance(payload, list) else [payload]
            for p in events:
                typ = p.get('event_type', p.get('type'))
                data = p.get('payload', p)
                if typ in ('market_resolved', 'tick_size_change'):
                    self.c.archive.write('labels' if typ == 'market_resolved' else 'market_changes',
                          {'schema_version': 3, 'kind': typ, 'available_at_ms': received_ms,
                           'connection_id': connection_id, 'payload': data})
        elif source == 'http_timing':
            if payload.get('status') == 200 and not payload.get('error'):
                self.latency[payload['host']].append(payload['elapsed_ms'])

    def set_market(self, market, ms):
        slug = market.get('slug')
        if not slug or not slug.startswith('btc-updown-'):
            return
        self.rules[slug] = event_reference(market, market_reference(market, ms), self.event_details.get(slug))
        # Keep at most two hours of as-of rule observations in memory.
        cutoff = int(time.time())-7200
        for k in list(self.rules):
            if k.rsplit('-',1)[-1].isdigit() and int(k.rsplit('-',1)[-1]) < cutoff:
                self.rules.pop(k, None)
                self.event_details.pop(k, None)
        live_tokens = {str(t) for tokens in self.c.markets.values() for t in tokens.values()}
        self.poly_books = {k:v for k,v in self.poly_books.items() if k in live_tokens}
        self.previous_books = {k:v for k,v in self.previous_books.items() if k in live_tokens}

    async def periodic(self, fn, seconds):
        while True:
            try:
                await fn()
            except OPTIONAL_ERRORS as e:
                self.errors[fn.__name__] = str(e)[:300]
                self.c.error('micro_'+fn.__name__, e)
            await asyncio.sleep(seconds)

    def external_message(self, kind, message, cid):
        ms = int(time.time()*1000)
        if not isinstance(message, dict):
            raise ValueError('Expected structured exchange message')
        p = message.get('data', message)
        if not isinstance(p, dict):
            raise ValueError('Expected exchange data object')
        ev = epoch_ms(p.get('E', p.get('T', p.get('time'))))
        if kind == 'chainlink_twap' and isinstance(message.get('payload'), dict):
            ev = epoch_ms(message['payload'].get('timestamp'))
        self.c.raw(kind, message, connection_id=cid, event_ms=ev)
        if kind == 'binance_spot_depth20':
            self.external['spot_depth20'] = obs(p, ms, ev, connection_id=cid)
        elif kind == 'binance_perp_public':
            if p.get('e') == 'bookTicker':
                self.external['perp_book'] = obs(p, ms, ev, connection_id=cid)
        elif kind == 'binance_perp_market':
            typ = p.get('e')
            if typ == 'aggTrade':
                self.flows.setdefault('perp', TradeFlow()).add(ms, p.get('a'),p.get('p'),p.get('q'),-1 if p.get('m') is True else 1)
                self.external['perp_trade'] = obs(p,ms,ev,connection_id=cid)
            elif typ == 'markPriceUpdate':
                self.external['perp_mark'] = obs(p,ms,ev,connection_id=cid)
            elif typ == 'forceOrder':
                self.last_liquidation = obs(p,ms,ev,coverage='sampled_exchange_notifications_not_all_liquidations')
        elif kind == 'chainlink_twap':
            topic = message.get('topic')
            p = message.get('payload', {})
            if not isinstance(p, dict):
                raise ValueError('Expected TWAP payload object')
            if topic in TOPICS and p.get('symbol') == 'btc/usd':
                window = TOPICS[topic]
                if p.get('window_s', window) != window:
                    raise ValueError('TWAP topic/window mismatch')
                value = twap_price(p)
                if value:
                    self.external[f'twap{window}'] = obs(p,ms,epoch_ms(p.get('timestamp')),
                        connection_id=cid,price=value, window_seconds=window,
                        publisher_ms=epoch_ms(message.get('timestamp')), source=topic)

    def coinbase_message(self, p, cid):
        if not isinstance(p, dict):
            raise ValueError('Expected Coinbase message object')
        ms, ev = int(time.time()*1000), epoch_ms(p.get('time'))
        self.c.raw('coinbase_ws',p,connection_id=cid,event_ms=ev)
        kind, product = p.get('type'), p.get('product_id')
        if kind == 'error':
            raise ValueError(str(p))
        if kind == 'ticker' and product in ('BTC-USD','USDT-USD'):
            self.external['coinbase_'+product] = obs(p,ms,ev,connection_id=cid)
        if product != 'BTC-USD':
            return
        if kind == 'snapshot':
            self.coinbase_book = {'bids':{decimal(k): v for k,v in p.get('bids',[]) if decimal(k) is not None},
                                  'asks':{decimal(k): v for k,v in p.get('asks',[]) if decimal(k) is not None},
                                  'received_ms': ms, 'event_ms': ev, 'connection_id': cid}
        elif kind == 'l2update' and self.coinbase_book.get('connection_id') == cid:
            for side, price, size in p.get('changes', []):
                if side not in ('buy','sell') or decimal(price) is None or number(size) is None:
                    raise ValueError('Malformed Coinbase book level')
                price = decimal(price)
                book = self.coinbase_book['bids' if side == 'buy' else 'asks']
                if number(size) == 0:
                    book.pop(price,None)
                else:
                    book[price] = size
            self.coinbase_book.update(received_ms=ms,event_ms=ev)
        elif kind == 'match':
            # Coinbase side describes the MAKER, the opposite of aggressor direction.
            self.flows.setdefault('coinbase',TradeFlow()).add(ms,p.get('trade_id'),p.get('price'),p.get('size'),
                                                            1 if p.get('side') == 'sell' else -1)

    async def refresh_rules(self):
        now = time.time()
        slugs = [window_slug('btc',now+300), window_slug('btc',now)]
        slugs += [window_slug('btc',now-300*i) for i in range(1,13)]
        for slug in slugs:
            if self.market_due.get(slug,0) > now:
                continue
            try:
                data = await self.c.get(GAMMA+'/markets',{'slug':slug})
                if not data:
                    self.market_due[slug] = now+60
                    continue
                m = data[0]
                self.c.raw('micro_market_rules',{'slug':slug,'market':m})
                # /markets has a compact events projection that can omit eventMetadata.
                # Fetch the documented full event, never substitute an exchange quote.
                try:
                    event = await self.c.get(GAMMA+'/events/slug/'+slug)
                    ems = int(time.time()*1000)
                    self.c.raw('micro_event_details',{'slug':slug,'event':event})
                    self.event_details[slug] = obs(event,ems)
                except OPTIONAL_ERRORS as exc:
                    self.c.error('micro_event_details',exc)
                ms = int(time.time()*1000)
                self.set_market(m,ms)
                current = int(slug.rsplit('-',1)[1])+300 > now
                self.market_due[slug] = now+(30 if current else 120)
                cond = m.get('conditionId')
                if cond and self.clob_due.get(slug,0) <= now:
                    result = await self.c.get(CLOB+'/markets/'+cond)
                    self.c.raw('micro_clob_market',{'slug':slug,'response':result})
                    self.clob_due[slug] = now+(300 if current else 120)
                    winners = [str(t.get('outcome')) for t in result.get('tokens',[]) if t.get('winner') is True]
                    if result.get('closed') is True and len(winners) == 1:
                        label = {'schema_version':3,'kind':'official_clob_winner','slug':slug,
                                 'condition_id':cond,'winning_outcome':winners[0],
                                 'available_at_ms':int(time.time()*1000),'source':'clob/markets/condition_id',
                                 'actual_settlement_time_ms':None,'final_reference_price':None}
                        if self.labels_seen.get(slug) != winners[0]:
                            self.c.archive.write('labels',label)
                            self.labels_seen[slug] = winners[0]
                        self.market_due[slug] = now+3600
                if current:
                    ids = m.get('clobTokenIds') or []
                    ids = json.loads(ids) if isinstance(ids,str) else ids
                    for token in ids:
                        fee = await self.c.get(CLOB+'/fee-rate',{'token_id':token})
                        self.c.raw('micro_token_fee_rate',{'slug':slug,'token_id':token,'response':fee})
            except OPTIONAL_ERRORS as e:
                self.errors['rules:'+slug] = str(e)[:200]
                self.c.error('micro_rules',e)
        self.market_due = {k:v for k,v in self.market_due.items() if k in slugs}
        self.clob_due = {k:v for k,v in self.clob_due.items() if k in slugs}
        self.labels_seen = {k:v for k,v in self.labels_seen.items() if k in slugs}

    async def poll_oi(self):
        p = await self.c.get(FUTURES_REST+'/fapi/v1/openInterest',{'symbol':'BTCUSDT'})
        self.c.raw('binance_perp_open_interest',p,event_ms=epoch_ms(p.get('time')))
        self.external['open_interest'] = obs(p,int(time.time()*1000),epoch_ms(p.get('time')))

    async def poll_context(self):
        # Slow positioning context is kept distinct from tick-level signals.
        for path, params in [('/futures/data/openInterestHist',{'symbol':'BTCUSDT','period':'5m','limit':2}),
                             ('/futures/data/takerlongshortRatio',{'symbol':'BTCUSDT','period':'5m','limit':2}),
                             ('/fapi/v1/fundingRate',{'symbol':'BTCUSDT','limit':2})]:
            p = await self.c.get(FUTURES_REST+path,params)
            self.c.raw('binance_perp_context',{'path':path,'response':p})

    async def poll_clock(self):
        for source,url in [('polymarket',CLOB+'/time'),('binance','https://data-api.binance.vision/api/v3/time'),
                           ('coinbase','https://api.exchange.coinbase.com/time')]:
            try:
                t0 = time.time()*1000
                p = await self.c.get(url)
                t1 = time.time()*1000
                v = p.get('serverTime',p.get('epoch')) if isinstance(p,dict) else p
                server = epoch_ms(v)
                if source == 'coinbase' and number(v) is not None:
                    server = int(float(v)*1000)
                r = dict(source=source,server_ms=server,received_ms=int(t1),rtt_ms=t1-t0,
                         offset_estimate_ms=server-(t0+t1)/2 if server is not None else None,
                         uncertainty_at_least_ms=(t1-t0)/2, not_one_way_latency=True,
                         clock_precision_note='Server timestamp quantization, clock error and asymmetry are additional uncertainty')
                self.clock[source] = r
                self.c.raw('clock_probe',r)
            except OPTIONAL_ERRORS as e:
                self.c.error('clock_'+source,e)

    async def poll_trades(self):
        slug = window_slug('btc',time.time())
        rule = self.rules.get(slug,{})
        cond = rule.get('condition_id')
        if not cond:
            return
        # Bounded overlapping observations, not a claim of exhaustive backfill.
        p = await self.c.get(DATA_API+'/trades',{'market':cond,'limit':500,'takerOnly':'true'})
        self.c.raw('polymarket_public_trade_crosscheck',{'slug':slug,'condition_id':cond,
              'limit':500,'pagination_complete':False,'response':[{k:r.get(k) for k in ('asset','conditionId','side','size','price','timestamp','transactionHash','outcome')} for r in p] if isinstance(p,list) else p})
        self.last_data_trade = {'received_ms':int(time.time()*1000),'slug':slug,
                               'returned_rows':len(p) if isinstance(p,list) else None,
                               'coverage':'overlapping_latest_500_not_a_full_trade_tape'}

    async def poll_related(self):
        now = time.time()
        slug = f'btc-updown-15m-{int(now)//900*900}'
        p = await self.c.get(GAMMA+'/markets',{'slug':slug})
        if not p:
            return
        m = p[0]
        ids, outcomes = m.get('clobTokenIds',[]),m.get('outcomes',[])
        ids = json.loads(ids) if isinstance(ids,str) else ids
        outcomes = json.loads(outcomes) if isinstance(outcomes,str) else outcomes
        r = {'slug':slug,'metadata':market_reference(m,int(time.time()*1000)),
             'same_payoff_as_5m':False,'interpretation':'context_only_different_window_and_strike'}
        self.c.raw('related_15m_metadata',m)
        for token,outcome in zip(ids,outcomes):
            b = await self.c.get(CLOB+'/book',{'token_id':token})
            ms = int(time.time()*1000)
            self.c.raw('related_15m_book',{'slug':slug,'token_id':token,'book':b})
            r[str(outcome).lower()] = dict(**book_features(b,True),received_ms=ms)
        r['received_ms'] = int(time.time()*1000)
        self.related = r

    def tasks(self):
        if 'btc' not in self.c.assets:
            return []
        c = self.c
        # Different Binance traffic classes MUST use distinct routed endpoints.
        return [self.periodic(self.refresh_rules,5),self.periodic(self.poll_oi,30),
                self.periodic(self.poll_context,300),self.periodic(self.poll_clock,60),
                self.periodic(self.poll_trades,15),self.periodic(self.poll_related,15),
                c.socket('spot_depth20',BINANCE_WS+'btcusdt@depth20@100ms',
                         lambda p,cid:self.external_message('binance_spot_depth20',p,cid)),
                c.socket('perp_market',FUTURES_MARKET+'btcusdt@aggTrade/btcusdt@markPrice@1s/btcusdt@forceOrder',
                         lambda p,cid:self.external_message('binance_perp_market',p,cid)),
                c.socket('perp_public',FUTURES_PUBLIC+'btcusdt@bookTicker',
                         lambda p,cid:self.external_message('binance_perp_public',p,cid)),
                c.socket('coinbase',COINBASE_WS,self.coinbase_message,
                         {'type':'subscribe','product_ids':['BTC-USD'],
                          'channels':['ticker','matches','heartbeat','level2_batch']}),
                c.socket('coinbase_fx',COINBASE_WS,self.coinbase_message,
                         {'type':'subscribe','product_ids':['USDT-USD'],'channels':['ticker','heartbeat']}),
                c.socket('chainlink_twap',CHAINLINK_WS,
                         lambda p,cid:self.external_message('chainlink_twap',p,cid),
                         {'action':'subscribe','subscriptions':[{'topic':t,'type':'update','filters':'{"symbol":"btc/usd"}'} for t in TOPICS]},heartbeat=5)]

    async def run(self):
        jobs = [asyncio.create_task(x) for x in self.tasks()]
        if not jobs:
            await asyncio.Future()
        try:
            await asyncio.gather(*jobs)
        finally:
            for j in jobs:
                j.cancel()
            await asyncio.gather(*jobs,return_exceptions=True)

    def valid_external(self, key, source, ms, ttl=5000, event=False):
        x = self.external.get(key)
        return bool(self.c.connected.get(source)) and fresh(x,ms,ttl,event) and x.get('connection_id') == self.connections.get(source)

    def markout_labels(self, row, micro):
        now = row['sample_ms']
        # Pending labels are emitted after their horizon, NEVER in decision features.
        keep = deque()
        while self.markouts:
            item = self.markouts.popleft()
            remaining = []
            for horizon in item['pending']:
                due = item['prediction_ms']+horizon*1000
                if now < due:
                    remaining.append(horizon)
                    continue
                same = row['slug'] == item['slug']
                mid = (micro.get('poly_up') or {}).get('mid')
                poly_ok = same and row['poly_valid'] and mid is not None and item['up_mid'] is not None and now-due <= 1500
                bp = number((row.get('binance') or {}).get('price'))
                spot_ok = row['binance_valid'] and bp is not None and item['spot'] and now-due <= 1500
                self.c.archive.write('labels',{'schema_version':3,'kind':'quote_markout_not_fill_pnl',
                    'slug':item['slug'],'prediction_ms':item['prediction_ms'],'horizon_seconds':horizon,
                    'label_available_at_ms':now,'poly_valid':bool(poly_ok),'spot_valid':bool(spot_ok),
                    'up_mid_change':mid-item['up_mid'] if poly_ok else None,
                    'spot_return_bps':(bp/item['spot']-1)*10000 if spot_ok else None})
            if remaining:
                item['pending'] = remaining
                keep.append(item)
        self.markouts = keep
        self.markouts.append({'prediction_ms':now,'slug':row['slug'],'pending':[1,5,30],
            'up_mid':(micro.get('poly_up') or {}).get('mid') if row['poly_valid'] else None,
            'spot':number((row.get('binance') or {}).get('price')) if row['binance_valid'] else None})

    def enrich(self,row):
        if row['asset'] != 'btc':
            return row
        ms,slug = row['sample_ms'],row['slug']
        rule = self.rules.get(slug)
        rule_valid = bool(rule and 0 <= ms-rule['metadata_received_ms'] <= 90000)
        up,down = self.poly_books.get(row.get('up_token_id')),self.poly_books.get(row.get('down_token_id'))
        micro = {'schema_version':3,'asof_ms':ms,'rules':rule,'time_to_expiry_seconds':max(0,(row['window_start_ms']+300000-ms)/1000),
                 'decision_features_only':True}
        for name,book in [('up',up),('down',down)]:
            token = row.get(name+'_token_id')
            if book:
                features = book_features(book['payload'],True)
                old = self.previous_books.get(token)
                features['ofi_since_previous_rest_observation'] = ofi(old,features) if old and old['received_ms'] != book['received_ms'] else None
                features['received_ms'] = book['received_ms']
                features['event_ms'] = book['event_ms']
                features['receive_age_ms'] = ms-book['received_ms']
                features['hash'] = book['payload'].get('hash')
                features['tick_size'] = book['payload'].get('tick_size')
                features['min_order_size'] = book['payload'].get('min_order_size')
                self.previous_books[token] = features.copy()
                micro['poly_'+name] = features
        # Paper complete-set checks require young, near-synchronous observations.
        pair_valid = bool(rule_valid and rule['accepting_orders'] is True and rule['closed'] is False
                 and fresh(up,ms,1500) and fresh(down,ms,1500)
                 and abs(up['received_ms']-down['received_ms']) <= 250
                 and micro.get('poly_up',{}).get('two_sided',False)
                 and micro.get('poly_down',{}).get('two_sided',False)
                 and not micro.get('poly_up',{}).get('crossed',True)
                 and not micro.get('poly_down',{}).get('crossed',True)
                 and micro['time_to_expiry_seconds'] > 0)
        micro['pair_quote_gate'] = {'valid':pair_valid,'max_age_ms':1500,'max_skew_ms':250,
             'skew_ms':abs(up['received_ms']-down['received_ms']) if up and down else None,
             'other_costs_excluded':['execution_latency','leg_risk','gas','capital_lockup','venue_restrictions'],
             'paper_quotes_only':True}
        if up and down:
            micro['complete_set_quotes'] = pair_quotes(up['payload'],down['payload'],rule['fees'] if rule else {'known':False},pair_valid)
            for quote in micro['complete_set_quotes']:
                minimum = rule.get('min_order_size') if rule else None
                quote['minimum_order_size_known_and_met'] = minimum is not None and quote['shares_each_outcome'] >= minimum
                if not quote['minimum_order_size_known_and_met']:
                    quote['buy_complete_set_edge_before_other_costs'] = None
                    quote['sell_complete_set_edge_before_other_costs'] = None
        gates = {'rules':rule_valid,'fee_model':bool(rule_valid and rule['fees']['known']), 'pair_quote':pair_valid,
                 'twap30':self.valid_external('twap30','chainlink_twap',ms,10000,True),
                 'twap60':self.valid_external('twap60','chainlink_twap',ms,10000,True),
                 'spot_depth20':self.valid_external('spot_depth20','spot_depth20',ms,3000),
                 'perp':self.valid_external('perp_mark','perp_market',ms,5000,True),
                 'perp_book':self.valid_external('perp_book','perp_public',ms,3000,True),
                 'coinbase':self.valid_external('coinbase_BTC-USD','coinbase',ms,5000,True),
                 'fx':self.valid_external('coinbase_USDT-USD','coinbase_fx',ms,30000,True),
                 'open_interest':fresh(self.external.get('open_interest'),ms,65000,True)}
        for key in ('twap30','twap60','perp_mark','perp_book','open_interest'):
            micro[key] = self.external.get(key)
        micro['latest_liquidation_notification'] = self.last_liquidation
        micro['coinbase_ticker'] = self.external.get('coinbase_BTC-USD')
        micro['usdt_usd_ticker'] = self.external.get('coinbase_USDT-USD')
        if gates['spot_depth20']:
            micro['binance_depth20'] = book_features(self.external['spot_depth20']['payload'])
        cb = self.coinbase_book
        cb_valid = bool(self.c.connected.get('coinbase') and cb and cb.get('connection_id') == self.connections.get('coinbase') and fresh(cb,ms,5000))
        gates['coinbase_l2'] = cb_valid
        if cb_valid:
            micro['coinbase_l2'] = book_features({'bids':list(cb['bids'].items()),'asks':list(cb['asks'].items())})
            micro['coinbase_l2'].update(received_ms=cb['received_ms'],event_ms=cb['event_ms'])
        for name,source in [('spot','binance_btc'),('perp','perp_market'),('coinbase','coinbase')]:
            micro[name+'_flow'] = self.flows.get(name,TradeFlow()).summarize(ms,self.c.connected.get(source))
        bp = number((row.get('binance') or {}).get('price')) if row['binance_valid'] else None
        self.price_history['spot'].add(ms,bp)
        micro['spot_returns_and_vol'] = self.price_history['spot'].summarize(ms)
        ref = None
        if rule_valid:
            kind = rule['reference_kind']
            if kind in ('chainlink_twap_30','chainlink_twap_60'):
                key = 'twap'+str(int(rule['twap_lookback_seconds']))
                ref = self.external.get(key) if gates[key] else None
            elif kind == 'chainlink_spot' and row['chainlink_valid']:
                ref = row['chainlink']
        gates['matching_reference_feed'] = bool(ref and rule_valid)
        gates['official_threshold'] = bool(rule_valid and rule['published_price_to_beat'])
        reference_valid = gates['matching_reference_feed'] and gates['official_threshold']
        gates['resolution_reference'] = reference_valid
        micro['matching_resolution_reference'] = ref
        strike = number(rule['published_price_to_beat']) if rule_valid else None
        rp = number(ref.get('price')) if ref else None
        micro['reference_distance_usd'] = rp-strike if rp is not None and strike else None
        micro['reference_distance_bps'] = (rp/strike-1)*10000 if rp is not None and strike else None
        # USD and USDT prices must not be silently equated.
        cbp = number((micro['coinbase_ticker'] or {}).get('payload',{}).get('price')) if gates['coinbase'] else None
        fx = number((micro['usdt_usd_ticker'] or {}).get('payload',{}).get('price')) if gates['fx'] else None
        mark = number((micro['perp_mark'] or {}).get('payload',{}).get('p')) if gates['perp'] else None
        micro['perp_spot_basis_bps'] = (mark/bp-1)*10000 if mark and bp else None
        micro['coinbase_binance_usd_basis_bps'] = (cbp/(bp*fx)-1)*10000 if cbp and bp and fx else None
        micro['binance_vs_resolution_reference_bps'] = (bp*fx/rp-1)*10000 if bp and fx and rp else None
        micro['basis_note'] = 'Different receipt times; indicative cross-venue comparison, not an executable cross-venue spread.'
        micro['related_15m'] = self.related
        micro['related_15m_fresh'] = fresh(self.related,ms,35000)
        micro['clock_probes'] = self.clock.copy()
        micro['trade_crosscheck'] = self.last_data_trade.copy()
        micro['valid'] = gates
        row['microstructure'] = micro
        row['official_price_to_beat'] = rule['published_price_to_beat'] if rule_valid else None
        row['official_price_to_beat_provenance'] = {'path':rule['price_to_beat_path'],'received_ms':rule.get('price_to_beat_received_ms')} if rule_valid else None
        # Never put a delayed winner or settlement observation into prior features.
        for k,ok in gates.items():
            row[k+'_valid'] = ok
            self.source_valid[k] += int(ok)
            if ok:
                self.last_valid[k] = ms
        self.markout_labels(row,micro)
        return row

    def health(self):
        timing = {}
        for host,values in self.latency.items():
            vals = sorted(values)
            if vals:
                timing[host] = {'count':len(vals),'p50_ms':vals[len(vals)//2],
                                'p95_ms':vals[min(len(vals)-1,int(len(vals)*.95))]}
        return {'schema_version':3,'valid_snapshot_counts':dict(self.source_valid),
                'last_valid_ms':self.last_valid.copy(),'errors':dict(list(self.errors.items())[-30:]),
                'parse_errors':self.parse_errors,'http_latency':timing,
                'scope':'BTC-only extra feeds; public, read-only; no order execution',
                'late_markouts_pending':len(self.markouts)}
