"""Pure, causal microstructure calculations. All outputs are research diagnostics.

Cash estimates use Decimal; no fill, atomic execution or guaranteed profit is
asserted. Public level-size decreases are not labelled cancellations.
"""
from __future__ import annotations
from collections import deque
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import math


def number(value):
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (ValueError, TypeError, OverflowError):
        return None


def decimal(value):
    try:
        x = Decimal(str(value))
        return x if x.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def levels(payload, side, binary=False):
    merged = {}
    for x in payload.get(side, []):
        p, q = (x.get('price'), x.get('size')) if isinstance(x, dict) else x[:2]
        p, q = decimal(p), decimal(q)
        if p is None or q is None or p <= 0 or q <= 0 or (binary and p >= 1):
            continue
        merged[p] = merged.get(p, Decimal(0)) + q
    return sorted(merged.items(), reverse=side == 'bids')


def book_features(payload, binary=False):
    bids, asks = levels(payload, 'bids', binary), levels(payload, 'asks', binary)
    out = {'two_sided': bool(bids and asks), 'bid_levels': len(bids), 'ask_levels': len(asks)}
    for side, ls in [('bid', bids), ('ask', asks)]:
        out[side] = float(ls[0][0]) if ls else None
        out[side+'_size'] = float(ls[0][1]) if ls else None
        for n in (1, 5, 10, 20):
            out[f'{side}_depth{n}'] = float(sum(q for _, q in ls[:n])) if ls else None
    for n in (1, 5, 10, 20):
        b, a = out[f'bid_depth{n}'], out[f'ask_depth{n}']
        out[f'imbalance{n}'] = (b-a)/(b+a) if b is not None and a is not None and b+a else None
    out['crossed'] = bool(bids and asks and bids[0][0] > asks[0][0])
    out['mid'] = out['spread'] = out['microprice'] = None
    if bids and asks and not out['crossed']:
        bp, bq = bids[0]; ap, aq = asks[0]
        out.update(mid=float((bp+ap)/2), spread=float(ap-bp),
                   microprice=float((ap*bq+bp*aq)/(bq+aq)))
    return out


def fee_config(market):
    """Do not interpret legacy takerBaseFee / fee-rate bps as a curve rate."""
    if market.get('feesEnabled') is False:
        return {'known': True, 'rate': '0', 'exponent': 1, 'model': 'explicit_fee_free'}
    f = market.get('feeSchedule') or {}
    rate, exponent = decimal(f.get('rate')), number(f.get('exponent'))
    known = market.get('feesEnabled') is True and rate is not None and 0 <= rate <= 1 and exponent == 1
    return {'known': known, 'rate': str(rate) if rate is not None else None,
            'exponent': exponent, 'model': 'C_rate_p_one_minus_p_2026-09-23' if known else 'unknown',
            'rebate_excluded': True, 'schedule': f,
            'settlement_assumption': 'USDC-equivalent fee; verify actual net shares and currency in fills'}


def sweep(payload, side, shares, fee):
    target = decimal(shares)
    if target is None or target <= 0:
        raise ValueError('shares must be positive')
    left, cash, fees = target, Decimal(0), Decimal(0)
    prices = levels(payload, side, binary=True)
    worst = None
    for price, available in prices:
        qty = min(left, available)
        cash += qty*price
        if fee.get('known'):
            fees += qty*Decimal(fee['rate'])*price*(1-price)
        left -= qty
        worst = price
        if left <= 0:
            break
    filled = target-left
    return dict(requested_shares=float(target), filled_shares=float(filled), sufficient=left == 0,
                notional=float(cash), vwap=float(cash/filled) if filled else None,
                worst_price=float(worst) if worst is not None else None,
                estimated_fee=float(fees) if fee.get('known') else None,
                fee_rounding='unrounded_curve_sum; actual per-match rounding/settlement may differ')


def pair_quotes(up, down, fee, valid, sizes=(10, 100, 500, 1000)):
    out = []
    for q in sizes:
        buy = [sweep(b, 'asks', q, fee) for b in (up, down)]
        sell = [sweep(b, 'bids', q, fee) for b in (up, down)]
        r = {'shares_each_outcome': q, 'buy': buy, 'sell': sell,
             'buy_complete_set_edge_before_other_costs': None,
             'sell_complete_set_edge_before_other_costs': None,
             'sell_requires_inventory_or_collateral_split': True,
             'atomic_execution_guaranteed': False}
        for side, legs in [('buy', buy), ('sell', sell)]:
            if valid and fee.get('known') and all(x['sufficient'] for x in legs):
                gross, cost = sum(x['notional'] for x in legs), sum(x['estimated_fee'] for x in legs)
                r[f'{side}_complete_set_edge_before_other_costs'] = (q-gross-cost) if side == 'buy' else (gross-q-cost)
        out.append(r)
    return out


def ofi(previous, current):
    """Cont-style top-of-book OFI over two observations, not queue position."""
    if not previous or any(previous.get(k) is None or current.get(k) is None
                           for k in ('bid', 'ask', 'bid_size', 'ask_size')):
        return None
    b, a, qb, qa = (current[k] for k in ('bid','ask','bid_size','ask_size'))
    pb, pa, pqb, pqa = (previous[k] for k in ('bid','ask','bid_size','ask_size'))
    return (qb if b >= pb else 0)-(pqb if b <= pb else 0)-(qa if a <= pa else 0)+(pqa if a >= pa else 0)


def market_reference(market, received_ms):
    slug = market.get('slug', '')
    config = market.get('cryptoMarketConfig') or {}
    source = str(market.get('resolutionSource', ''))
    # Interpret only explicitly advertised windows; never infer from cadence.
    lookback = number(config.get('twapLookbackSeconds')) if config.get('twapEnabled') else None
    if lookback not in (30, 60):
        if 'twap-60s' in source:
            lookback = 60
        elif 'twap-30s' in source:
            lookback = 30
        else:
            lookback = None
    strike, path = None, None
    for e in market.get('events', []):
        if e.get('slug') != slug:
            continue
        v = decimal((e.get('eventMetadata') or {}).get('priceToBeat'))
        if v is not None and v > 0:
            strike, path = str(v), 'events[matching_slug].eventMetadata.priceToBeat'
            break
    identity = {k: market.get(k) for k in ('conditionId','description','resolutionSource','cryptoMarketConfig','feeSchedule','feesEnabled')}
    return {'slug': slug, 'condition_id': market.get('conditionId'), 'published_price_to_beat': strike,
            'price_to_beat_path': path, 'price_to_beat_received_ms': received_ms if strike else None,
            'metadata_received_ms': received_ms,
            'resolution_source': source, 'twap_lookback_seconds': lookback,
            'reference_kind': f'chainlink_twap_{int(lookback)}' if lookback else ('chainlink_spot' if 'chain.link' in source and 'twap' not in source.lower() else 'unknown'),
            'rules_sha256': hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest(),
            'fees': fee_config(market), 'accepting_orders': market.get('acceptingOrders'),
            'closed': market.get('closed'), 'active': market.get('active'),
            'min_order_size': number(market.get('orderMinSize')),
            'tick_size': number(market.get('orderPriceMinTickSize')),
            'rewards_min_size': market.get('rewardsMinSize'), 'rewards_max_spread': market.get('rewardsMaxSpread')}


def twap_price(payload):
    with localcontext() as ctx:
        ctx.prec = 60
        n = payload.get('full_accuracy_value')
        p = decimal(n) / Decimal(10**18) if decimal(n) is not None else decimal(payload.get('value'))
        return str(p) if p is not None and p > 0 else None


class TradeFlow:
    """Receive-time windows, unique exchange trade IDs; no inferred tape completeness."""
    def __init__(self, limit=200000):
        self.rows = deque(maxlen=limit)
        self.last_id = None
        self.started = None
        self.last_gap = None
        self.total_signed = 0.0
        self.last_received = None

    def add(self, ms, trade_id, price, qty, sign):
        price, qty = number(price), number(qty)
        if price is None or qty is None or price <= 0 or qty <= 0 or sign not in (-1,1):
            return False
        if trade_id is not None:
            tid = int(trade_id)
            if self.last_id is not None and tid <= self.last_id:
                return False
            if self.last_id is not None and tid > self.last_id+1:
                self.last_gap = ms
            self.last_id = tid
        self.started = ms if self.started is None else self.started
        while self.rows and self.rows[0][0] <= ms-60000:
            self.rows.popleft()
        if len(self.rows) == self.rows.maxlen:
            self.last_gap = ms
        self.rows.append((ms, price*qty, sign, qty))
        self.last_received = ms
        self.total_signed += price*qty*sign
        return True

    def summarize(self, now, connected):
        out = {'basis': 'locally_received_messages', 'complete_trade_tape': False,
               'connected': bool(connected), 'last_trade_received_ms': self.last_received,
               'signed_notional_since_connection': self.total_signed}
        for s in (1,5,15,60):
            rows = [r for r in self.rows if now-s*1000 < r[0] <= now]
            buy = sum(r[1] for r in rows if r[2] == 1)
            sell = sum(r[1] for r in rows if r[2] == -1)
            out[str(s)+'s'] = {'messages': len(rows), 'buy_notional': buy, 'sell_notional': sell,
                 'signed_notional': buy-sell, 'imbalance': (buy-sell)/(buy+sell) if buy+sell else None,
                 'warmed': self.started is not None and now-self.started >= s*1000,
                 'recent_id_gap_or_overflow': self.last_gap is not None and now-self.last_gap <= s*1000}
        return out


class RollingPrices:
    def __init__(self):
        self.rows = deque(maxlen=302)

    def add(self, ms, price):
        p = number(price)
        self.rows.append((ms, p if p is not None and p > 0 else None))

    def summarize(self, now):
        out = {}
        for s in (5,15,60):
            cutoff = now-s*1000
            anchor = next((x for x in reversed(self.rows) if x[0] <= cutoff), None)
            rows = ([anchor] if anchor and cutoff-anchor[0] <= 1500 else [])
            rows += [x for x in self.rows if cutoff < x[0] <= now]
            pairs = [(a,b) for a,b in zip(rows,rows[1:]) if a[1] and b[1] and 500 <= b[0]-a[0] <= 1500]
            rv = math.sqrt(sum(math.log(b[1]/a[1])**2 for a,b in pairs))*10000 if pairs else None
            warmed = bool(rows and rows[0][0] <= now-s*1000 and len(pairs) >= s-1 and len(pairs) == len(rows)-1 and rows[-1][1] and now-rows[-1][0] <= 1500)
            out[str(s)+'s'] = {'realized_vol_bps': rv if warmed else None,
                              'return_bps': (rows[-1][1]/rows[0][1]-1)*10000 if warmed and rows[0][1] else None,
                              'valid_intervals': len(pairs), 'warmed': warmed}
        return out


def event_reference(market, reference, detail):
    """Use only an explicitly matching full Gamma event; retain true availability."""
    r = dict(reference)
    if not detail or detail.get('received_ms', 0) > reference['metadata_received_ms']:
        return r
    event = detail.get('payload')
    if not isinstance(event, dict) or event.get('slug') != market.get('slug'):
        return r
    matches = [m for m in event.get('markets', []) if m.get('slug') == market.get('slug')
               and m.get('conditionId') == market.get('conditionId') and m.get('conditionId')]
    if not matches:
        return r
    meta = event.get('eventMetadata') or {}
    value = decimal(meta.get('priceToBeat')) if isinstance(meta,dict) else None
    if value is not None and value > 0:
        if r.get('published_price_to_beat') is not None and decimal(r['published_price_to_beat']) != value:
            r.update(published_price_to_beat=None,price_to_beat_path=None,price_to_beat_received_ms=None,
                     price_to_beat_conflict=True)
        else:
            r.update(published_price_to_beat=str(value),
                price_to_beat_path='gamma.events/slug/{slug}.eventMetadata.priceToBeat',
                price_to_beat_received_ms=detail['received_ms'])
    return r


def server_time_text(text):
    """CLOB /time is numeric text/plain. HTML, objects and nonfinite values fail."""
    text = text.strip()
    if not text.isdigit():
        raise ValueError('Expected numeric server epoch, not a JSON object or HTML')
    value = int(text)
    if not 10**9 <= value <= 10**14:
        raise ValueError('Server time outside supported epoch range')
    return value
