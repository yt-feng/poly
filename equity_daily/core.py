"""Classification and lossless, append-only storage; independent of capture_v2/v3."""
from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import time
from datetime import datetime, timezone
from . import FAMILY, SCHEMA_VERSION

CRYPTO = re.compile(r'\b(bitcoin|ethereum|btc|eth|solana|sol|dogecoin|doge|xrp|crypto)\b', re.I)
SHORT_WINDOW = re.compile(r'\b\d+\s*(?:min(?:ute)?s?|hours?|hrs?)\b|updown-(?:5m|15m|1h|4h)', re.I)
LONG_WINDOW = re.compile(r'\b(weekly|monthly|quarterly|this week|this month|end of (?:the )?(?:month|year)|by (?:the end of )?20\d\d)\b', re.I)
DIRECTION = re.compile(r'\bup\s*(?:or|/)\s*down\b', re.I)
CLOSE = re.compile(r'\bclos(?:e|es|ing)\b.*\b(above|below|between|over|under|at least|at most)\b', re.I)
DAY = re.compile(r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+\d{1,2}\b|\b20\d\d-\d\d-\d\d\b|\b(?:today|tomorrow|daily)\b', re.I)
SYMBOL = re.compile(r'\(([A-Z0-9^][A-Z0-9.^=-]{0,11})\)')
# These are identifier translations, NOT a closed universe. New equity tickers
# are discovered from titles/rules automatically; unresolved names are retained.
ALIASES = {
    'DJI': ('^DJI', 'index', 'index_reference'),
    'DJIA': ('^DJI', 'index', 'index_reference'),
    'NDX': ('^NDX', 'index', 'index_reference'),
    'IXIC': ('^IXIC', 'index', 'index_reference'),
    'NYA': ('^NYA', 'index', 'index_reference'),
    'RUT': ('^RUT', 'index', 'index_reference'),
    'SPX': ('^GSPC', 'index', 'index_reference'),
    'SP500': ('^GSPC', 'index', 'index_reference'),
    'HSI': ('^HSI', 'index', 'index_reference'),
    'VIX': ('^VIX', 'index', 'index_reference'),
    'GC': ('GC=F', 'commodity', 'continuous_futures_proxy_not_contract'),
    'SI': ('SI=F', 'commodity', 'continuous_futures_proxy_not_contract'),
    'CL': ('CL=F', 'commodity', 'continuous_futures_proxy_not_contract'),
    'BZ': ('BZ=F', 'commodity', 'continuous_futures_proxy_not_contract'),
    'HG': ('HG=F', 'commodity', 'continuous_futures_proxy_not_contract'),
    'NG': ('NG=F', 'commodity', 'continuous_futures_proxy_not_contract'),
}
NAMES = {
    'amazon': 'AMZN', 'microsoft': 'MSFT', 'apple': 'AAPL', 'nvidia': 'NVDA',
    'meta': 'META', 'tesla': 'TSLA', 'google': 'GOOGL', 'alphabet': 'GOOGL',
    'netflix': 'NFLX', 'palantir': 'PLTR', 'robinhood': 'HOOD',
    'dow jones': 'DJI', 'nasdaq 100': 'NDX', 'nasdaq composite': 'IXIC',
    'russell 2000': 'RUT', 's&p 500': 'SPX', 'hang seng': 'HSI',
    'gold': 'GC', 'silver': 'SI',
}


def array(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return []
    return value if isinstance(value, list) else []


def epoch(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return dt.replace(tzinfo=timezone.utc).timestamp() if dt.tzinfo is None else dt.timestamp()
    except (ValueError, TypeError):
        return None


def classify(market: dict, event: dict | None = None) -> tuple[dict | None, str]:
    """Pure classifier. Ambiguous symbol/date mappings are never silently accepted."""
    event = event or {}
    question = str(market.get('question') or '')
    title = str(event.get('title') or '')
    text = question + ' ' + title
    slug = str(market.get('slug', '')) + ' ' + str(event.get('slug', ''))
    if CRYPTO.search(text) or re.search(r'\b(?:btc|eth|sol|xrp)-', slug, re.I):
        return None, 'crypto_excluded'
    if SHORT_WINDOW.search(text + ' ' + slug) or LONG_WINDOW.search(text):
        return None, 'non_daily_excluded'
    kind = 'daily_direction' if DIRECTION.search(text) else ('close_threshold' if CLOSE.search(text) else None)
    if not kind:
        return None, 'not_supported_daily_price_event'
    if not DAY.search(text + ' ' + slug):
        return None, 'daily_date_unconfirmed'
    end = market.get('endDate') or event.get('endDate')
    if epoch(end) is None:
        return None, 'end_date_missing'
    labels, tokens = array(market.get('outcomes')), array(market.get('clobTokenIds'))
    if len(tokens) != 2 or len(labels) != 2 or len(set(map(str, tokens))) != 2:
        return None, 'invalid_binary_outcome_mapping'
    if not all(str(t).isdigit() for t in tokens):
        return None, 'invalid_token_id'
    if set(str(x).lower() for x in labels) not in ({'yes', 'no'}, {'up', 'down'}):
        return None, 'unexpected_outcome_labels'
    ticker_hits = SYMBOL.findall(question) or SYMBOL.findall(title)
    ticker = ticker_hits[0] if len(set(ticker_hits)) == 1 else None
    evidence = 'title_parenthesized_symbol' if ticker else None
    if not ticker:
        matches = {v for k, v in NAMES.items() if re.search(r'\b' + re.escape(k) + r'\b', text, re.I)}
        if len(matches) == 1:
            ticker = matches.pop()
            evidence = 'name_alias_requires_provider_validation'
    source_text = ' '.join(str(x or '') for x in [market.get('description'), market.get('resolutionSource'), event.get('description')])
    links = re.findall(r'finance\.yahoo\.com/(?:quote|chart)/([^/\s?"<>]+)', source_text, re.I)
    from urllib.parse import unquote
    links = {unquote(x).upper() for x in links}
    yahoo, asset_class, relation = None, 'unmapped', 'unresolved'
    if ticker:
        yahoo, asset_class, relation = ALIASES.get(ticker, (ticker, 'equity_candidate', 'symbol_requires_provider_validation'))
    if len(links) == 1:
        candidate = links.pop()
        if re.fullmatch(r'[A-Z0-9^][A-Z0-9.^=-]{0,19}', candidate):
            yahoo = candidate
            evidence = 'market_rules_yahoo_url'
            relation = 'rules_reference_symbol'
            if ticker is None:
                ticker = candidate
            if asset_class == 'unmapped':
                asset_class = 'index' if candidate.startswith('^') else ('commodity' if '=F' in candidate else 'equity_candidate')
    tags = ' '.join(str(t.get('slug', '') if isinstance(t, dict) else t) for t in event.get('tags', []))
    if not yahoo and not re.search(r'\b(stocks?|shares?|indices|index|equities|commodities|commodity|futures|finance)\b', text+' '+tags, re.I):
        return None, 'not_identifiably_financial'
    if yahoo and (CRYPTO.search(yahoo) or yahoo.endswith(('-USD', '-USDT'))):
        return None, 'crypto_symbol_excluded'
    record = dict(family=FAMILY, market_id=str(market.get('id', '')),
                  event_id=str(event.get('id', '')), condition_id=market.get('conditionId'),
                  question=question, event_title=title, market_slug=market.get('slug'),
                  event_slug=event.get('slug'), event_kind=kind, end_date_utc=end,
                  event_date_status='use_rule_text_not_utc_date_for_exchange_session',
                  ticker=ticker, yahoo_symbol=yahoo, asset_class=asset_class,
                  mapping_evidence=evidence, underlying_relation=relation,
                  underlying_is_official_settlement=False,
                  outcomes=[dict(label=str(label), token_id=str(token)) for label, token in zip(labels, tokens)],
                  active=market.get('active'), closed=market.get('closed'),
                  accepting_orders=market.get('acceptingOrders'),
                  resolution_source=market.get('resolutionSource'),
                  raw_market=market, raw_event={k: v for k, v in event.items() if k != 'markets'})
    return record, 'accepted' if yahoo else 'accepted_underlying_unmapped'


def atomic_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    tmp.write_text(json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2), encoding='utf-8')
    tmp.replace(path)


def digest(path: Path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


class Journal:
    """Single event-loop writer, immutable segments, exact WS/entity payload bytes."""
    def __init__(self, root: Path, run_id: str, segment_bytes=32*1024*1024):
        if not root.name.startswith('equity_daily'):
            raise ValueError('Output directory must start with equity_daily; BTC directories are forbidden')
        root.mkdir(parents=True, exist_ok=True)
        if any(root.iterdir()):
            raise ValueError('Use a new empty run directory; existing raw data is never overwritten')
        self.root, self.run_id, self.segment_bytes = root, run_id, segment_bytes
        self.opened, self.files, self.counts = {}, [], {}
        self.seq = self.snapshot_seq = 0
        self.start_ns = time.time_ns()
        self.fernet = None
        key = os.environ.get('EQUITY_ARCHIVE_KEY')
        if key:
            from cryptography.fernet import Fernet
            self.fernet = Fernet(key.encode())
        self.emit('audit', {'event': 'run_start', 'code_sha': os.environ.get('GITHUB_SHA'),
                           'underlying_encrypted': bool(self.fernet),
                           'live_trading_enabled': False, 'strategy_evaluated': False})

    def emit(self, source: str, payload: dict, *, received_ns=None):
        if not re.fullmatch(r'[a-z0-9_]+', source):
            raise ValueError('Invalid source name')
        recv = received_ns if received_ns is not None else time.time_ns()
        self.seq += 1
        row = dict(payload)
        # Envelope fields cannot be overridden by vendor fields.
        row.update(family=FAMILY, schema_version=SCHEMA_VERSION, run_id=self.run_id,
                   sequence=self.seq, received_at_ns=recv, monotonic_ns=time.monotonic_ns())
        raw = (json.dumps(row, separators=(',', ':'), ensure_ascii=False) + '\n').encode()
        day = datetime.fromtimestamp(recv / 1e9, timezone.utc).strftime('%Y-%m-%d')
        item = self.opened.get(source)
        if item and (item['bytes'] + len(raw) > self.segment_bytes or item['day'] != day):
            self.close_source(source)
            item = None
        if not item:
            name = f'{FAMILY}-{source}-{day}-{self.seq:012d}.jsonl.gz'
            path = self.root / name
            item = dict(path=path, handle=gzip.open(str(path)+'.part', 'wb', compresslevel=3),
                        bytes=0, rows=0, first_ns=recv, last_ns=recv, day=day)
            self.opened[source] = item
        item['handle'].write(raw)
        item['bytes'] += len(raw)
        item['rows'] += 1
        item['last_ns'] = recv
        self.counts[source] = self.counts.get(source, 0) + 1

    def wire(self, source: str, raw: str | bytes, **context):
        recv = time.time_ns()
        data = raw.encode('utf-8') if isinstance(raw, str) else raw
        self.emit(source, dict(payload_b64=base64.b64encode(data).decode(),
                               payload_sha256=hashlib.sha256(data).hexdigest(),
                               payload_encoding='utf8' if isinstance(raw, str) else 'bytes', **context), received_ns=recv)

    def close_source(self, source):
        item = self.opened.pop(source, None)
        if item is None:
            return
        item['handle'].close()
        path = item['path']
        part = Path(str(path) + '.part')
        encrypted = bool(self.fernet and source.startswith('underlying_'))
        if encrypted:
            path = Path(str(path) + '.fernet')
            encrypted_part = Path(str(path) + '.part')
            encrypted_part.write_bytes(self.fernet.encrypt(part.read_bytes()))
            encrypted_part.replace(path)
            part.unlink()
        else:
            part.replace(path)
        sha = digest(path)
        Path(str(path)+'.sha256').write_text(f'{sha}  {path.name}\n')
        self.files.append(dict(file=path.name, sha256=sha, bytes=path.stat().st_size,
                               rows=item['rows'], source=source, encrypted=encrypted,
                               first_received_ns=item['first_ns'], last_received_ns=item['last_ns']))

    def checkpoint(self, health: dict):
        for source in list(self.opened):
            self.close_source(source)
        self.snapshot_seq += 1
        manifest = dict(family=FAMILY, schema_version=SCHEMA_VERSION, run_id=self.run_id,
                        code_sha=os.environ.get('GITHUB_SHA'), started_at_ns=self.start_ns,
                        checkpoint_at_ns=time.time_ns(), files=list(self.files),
                        counts=dict(self.counts), health=health,
                        raw_wire_complete=False, completeness_note='Only observed feed messages; consult gaps/errors and per-token coverage',
                        live_trading_enabled=False, strategy_evaluated=False)
        path = self.root / f'manifest-{self.snapshot_seq:06d}.json'
        atomic_json(path, manifest)
        Path(str(path)+'.sha256').write_text(f'{digest(path)}  {path.name}\n')
        atomic_json(self.root / 'health.json', health)
        return manifest

    def check_disk(self):
        if shutil.disk_usage(self.root).free < 512*1024*1024:
            raise RuntimeError('disk_low_stop_without_silent_sample_drops')
