#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
BJ = ZoneInfo("Asia/Shanghai")
UTC = ZoneInfo("UTC")
CSV_FIELDS = [
    "ts_iso",
    "slug",
    "market_url",
    "window_text",
    "buy_up_cents",
    "buy_down_cents",
    "sell_up_cents",
    "sell_down_cents",
    "buy_up_size",
    "buy_down_size",
    "sell_up_size",
    "sell_down_size",
    "mid_up_cents",
    "mid_down_cents",
    "spread_up_cents",
    "spread_down_cents",
    "bid_depth_up_5",
    "ask_depth_up_5",
    "bid_depth_down_5",
    "ask_depth_down_5",
    "level_count_bid_up",
    "level_count_ask_up",
    "level_count_bid_down",
    "level_count_ask_down",
    "target_price",
    "final_price",
    "trade_count_1s",
    "trade_volume_1s",
]
TEXT_HINT_KEYS = {
    "question",
    "title",
    "description",
    "subtitle",
    "resolutioncriteria",
    "resolution_criteria",
    "rules",
}
ASSET_SYMBOL_MAP = {
    "btc": {
        "binance": "BTCUSDT",
        "coinbase": "BTC-USD",
        "kraken": "XXBTZUSD",
        "chainlink_url": "https://data.chain.link/streams/btc-usd-cexprice-streams",
    },
    "eth": {
        "binance": "ETHUSDT",
        "coinbase": "ETH-USD",
        "kraken": "XETHZUSD",
    },
    "sol": {
        "binance": "SOLUSDT",
        "coinbase": "SOL-USD",
        "kraken": "SOLUSD",
    },
}


class PolyError(RuntimeError):
    pass


@dataclass
class MarketInfo:
    slug: str
    market_url: str
    up_token_id: str
    down_token_id: str
    window_text: str
    target_price: str
    asset_key: str
    raw: dict[str, Any]


@dataclass
class WindowInfo:
    start_bj: datetime
    end_bj: datetime
    slug: str


@dataclass
class TradeTracker:
    last_ts: datetime | None = None
    seen_ids: dict[str, datetime] = field(default_factory=dict)


def log(*args: Any) -> None:
    print(*args, file=sys.stderr, flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Capture Polymarket 5-minute quote snapshots.")
    p.add_argument("--market-url", required=True, help="Any market url in the same 5m series, e.g. https://polymarket.com/zh/event/btc-updown-5m-1776752100")
    p.add_argument("--mode", choices=["once-current", "loop-current-window", "loop-range", "loop-next-hours"], default="loop-current-window")
    p.add_argument("--date-bj", default="today", help="Date in Beijing timezone, format YYYY-MM-DD, or 'today'.")
    p.add_argument("--range-start-hm", default="16:15", help="For loop-range, inclusive start HH:MM in Beijing time.")
    p.add_argument("--range-end-hm", default="16:30", help="For loop-range, exclusive end HH:MM in Beijing time.")
    p.add_argument("--duration-hours", type=float, default=2.0, help="For loop-next-hours, capture duration in hours.")
    p.add_argument("--sample-seconds", type=float, default=1.0, help="Sampling interval in seconds.")
    p.add_argument("--target-slug", default="", help="Optional exact market slug to snapshot once, e.g. btc-updown-5m-1776759300")
    p.add_argument("--target-window-end-hm", default="", help="Optional Beijing HH:MM for the window end to snapshot once, e.g. 16:45 means window 16:40-16:45")
    p.add_argument("--output-csv", default="", help="Optional explicit csv output path.")
    p.add_argument("--timeout", type=float, default=10.0)
    return p.parse_args()


def normalize_url(url: str) -> str:
    return url.strip().rstrip("/")


def slug_from_market_url(url: str) -> str:
    url = normalize_url(url)
    m = re.search(r"/event/([^/?#]+)", url)
    if not m:
        raise PolyError(f"Cannot parse slug from market url: {url}")
    return m.group(1)


def series_prefix_from_slug(slug: str) -> str:
    m = re.match(r"(.+)-\d{10}$", slug)
    if not m:
        raise PolyError(f"Slug does not end with epoch seconds: {slug}")
    return m.group(1)


def infer_asset_key(series_prefix: str) -> str:
    return series_prefix.split("-", 1)[0].lower()


def choose_date(date_bj: str) -> datetime:
    if date_bj == "today":
        return datetime.now(BJ)
    return datetime.strptime(date_bj, "%Y-%m-%d").replace(tzinfo=BJ)


def parse_hm_for_day(day_bj: datetime, hm: str) -> datetime:
    hh, mm = hm.split(":")
    return day_bj.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)


def floor_to_5m(dt: datetime) -> datetime:
    floored = dt.replace(second=0, microsecond=0)
    return floored - timedelta(minutes=floored.minute % 5)


def active_window(now_bj: datetime) -> tuple[datetime, datetime]:
    start_dt = floor_to_5m(now_bj)
    end_dt = start_dt + timedelta(minutes=5)
    return start_dt, end_dt


def epoch_slug_for_window_start(series_prefix: str, start_dt_bj: datetime) -> str:
    start_dt_utc = start_dt_bj.astimezone(UTC)
    return f"{series_prefix}-{int(start_dt_utc.timestamp())}"


def current_window_info(series_prefix: str, now_bj: datetime | None = None) -> WindowInfo:
    if now_bj is None:
        now_bj = datetime.now(BJ)
    start_dt, end_dt = active_window(now_bj)
    slug = epoch_slug_for_window_start(series_prefix, start_dt)
    return WindowInfo(start_bj=start_dt, end_bj=end_dt, slug=slug)


def build_market_url(template_url: str, slug: str) -> str:
    template_url = normalize_url(template_url)
    return re.sub(r"/event/[^/?#]+", f"/event/{slug}", template_url)


def try_json_loads(value: Any) -> Any:
    if isinstance(value, str):
        s = value.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                return json.loads(s)
            except json.JSONDecodeError:
                return value
    return value


def request_json(url: str, *, timeout: float) -> Any:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def request_text(url: str, *, timeout: float) -> str:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


def parse_book_levels(book_side: Any) -> list[tuple[float, float]]:
    levels: list[tuple[float, float]] = []
    for level in book_side or []:
        price = None
        size = 0.0
        if isinstance(level, dict):
            price = level.get("price")
            size = level.get("size") or level.get("amount") or level.get("quantity") or 0.0
        elif isinstance(level, (list, tuple)) and level:
            price = level[0]
            size = level[1] if len(level) > 1 else 0.0
        try:
            price_f = float(price)
        except Exception:
            continue
        try:
            size_f = float(size)
        except Exception:
            size_f = 0.0
        levels.append((price_f, size_f))
    return levels


def best_bid_ask_from_book(book: dict[str, Any]) -> tuple[float | None, float | None]:
    bid_levels = parse_book_levels(book.get("bids") or [])
    ask_levels = parse_book_levels(book.get("asks") or [])
    best_bid = max((p for p, _ in bid_levels), default=None)
    best_ask = min((p for p, _ in ask_levels), default=None)
    return best_bid, best_ask


def top_size(book_side: Any, best: str) -> str:
    levels = parse_book_levels(book_side)
    if not levels:
        return ""
    chosen = max(levels, key=lambda x: x[0]) if best == "bid" else min(levels, key=lambda x: x[0])
    return f"{chosen[1]:.2f}"


def level_count(book_side: Any) -> str:
    return str(len(parse_book_levels(book_side)))


def depth_sum(book_side: Any, top_n: int = 5) -> str:
    levels = parse_book_levels(book_side)[:top_n]
    if not levels:
        return ""
    return f"{sum(size for _, size in levels):.2f}"


def cents(v: float | None) -> str:
    if v is None:
        return ""
    return f"{v * 100:.2f}"


def mid_cents(best_bid: float | None, best_ask: float | None) -> str:
    if best_bid is None or best_ask is None:
        return ""
    return f"{((best_bid + best_ask) / 2.0) * 100:.2f}"


def spread_cents(best_bid: float | None, best_ask: float | None) -> str:
    if best_bid is None or best_ask is None:
        return ""
    return f"{(best_ask - best_bid) * 100:.2f}"


def normalize_price(value: Any) -> str:
    if value is None or isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        return f"{float(value):.2f}"
    if isinstance(value, str):
        s = value.strip().replace(",", "")
        if not s:
            return ""
        try:
            return f"{float(s):.2f}"
        except Exception:
            m = re.search(r"([0-9]+(?:\.[0-9]+)?)", s)
            if m:
                return f"{float(m.group(1)):.2f}"
    return ""


def round_half_up_to_half(value: float) -> float:
    return round(value * 2.0) / 2.0


def iter_nodes(obj: Any):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            yield from iter_nodes(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_nodes(item)


def collect_text_candidates(raw: dict[str, Any]) -> list[str]:
    texts: list[str] = []
    for key, value in iter_nodes(raw):
        key_norm = str(key).replace("-", "_").lower()
        if key_norm in TEXT_HINT_KEYS and isinstance(value, str):
            text = value.strip()
            if text and text not in texts:
                texts.append(text)
    return texts


def extract_threshold_from_text(raw: dict[str, Any]) -> str:
    patterns = [
        r"above\s+or\s+below[^0-9$]*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"above[^0-9$]*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"below[^0-9$]*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"高于或低于[^0-9$¥￥]*[$¥￥]?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"高于[^0-9$¥￥]*[$¥￥]?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"低于[^0-9$¥￥]*[$¥￥]?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
    ]
    for text in collect_text_candidates(raw):
        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                return normalize_price(m.group(1))
    return ""


def extract_chainlink_mid_price(text: str) -> str:
    patterns = [
        r'"midPrice"\s*:\s*"?([0-9][0-9,]*(?:\.[0-9]+)?)"?',
        r'"mid-price"\s*:\s*"?([0-9][0-9,]*(?:\.[0-9]+)?)"?',
        r'mid[- ]price[^0-9$]{0,80}\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)',
        r'Mid[- ]Price[^0-9$]{0,80}\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            price = normalize_price(m.group(1))
            if price:
                return price
    return ""


def fetch_chainlink_mid_price(asset_key: str, timeout: float) -> str:
    url = (ASSET_SYMBOL_MAP.get(asset_key) or {}).get("chainlink_url")
    if not url:
        return ""
    try:
        text = request_text(url, timeout=timeout)
        return extract_chainlink_mid_price(text)
    except Exception:
        return ""


def fetch_live_reference_price(asset_key: str, timeout: float) -> str:
    chainlink_price = fetch_chainlink_mid_price(asset_key, timeout)
    if chainlink_price:
        return chainlink_price
    symbols = ASSET_SYMBOL_MAP.get(asset_key, {})
    if not symbols:
        return ""
    try:
        data = request_json(f"https://api.binance.com/api/v3/ticker/price?symbol={symbols['binance']}", timeout=timeout)
        price = normalize_price(data.get("price"))
        if price:
            return price
    except Exception:
        pass
    try:
        data = request_json(f"https://api.exchange.coinbase.com/products/{symbols['coinbase']}/ticker", timeout=timeout)
        price = normalize_price(data.get("price"))
        if price:
            return price
    except Exception:
        pass
    try:
        data = request_json(f"https://api.kraken.com/0/public/Ticker?pair={symbols['kraken']}", timeout=timeout)
        result = data.get("result") or {}
        for entry in result.values():
            last_trade = entry.get("c") or []
            if last_trade:
                price = normalize_price(last_trade[0])
                if price:
                    return price
    except Exception:
        pass
    return ""


def fetch_window_start_reference_price(asset_key: str, start_dt_bj: datetime, timeout: float) -> str:
    symbols = ASSET_SYMBOL_MAP.get(asset_key, {})
    if not symbols:
        return ""
    start_ms = int(start_dt_bj.astimezone(UTC).timestamp() * 1000)
    end_ms = start_ms + 60_000
    try:
        data = request_json(
            f"https://api.binance.com/api/v3/klines?symbol={symbols['binance']}&interval=1m&startTime={start_ms}&endTime={end_ms}&limit=1",
            timeout=timeout,
        )
        if isinstance(data, list) and data:
            open_price = normalize_price(data[0][1])
            if open_price:
                return f"{round_half_up_to_half(float(open_price)):.2f}"
    except Exception:
        pass
    return ""


def is_near_window_start(now_bj: datetime, start_dt_bj: datetime, tolerance_seconds: float = 2.0) -> bool:
    delta = (now_bj - start_dt_bj).total_seconds()
    return 0.0 <= delta <= tolerance_seconds


def price_key_candidates(raw: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in iter_nodes(raw):
        key_text = str(key)
        if "price" in key_text.lower() or key_text.lower() in {"strike", "reference"}:
            if isinstance(value, (str, int, float, bool)) or value is None:
                out[key_text] = value
    return out


def write_debug_artifact(slug: str, market_url: str, raw: dict[str, Any], target_price: str, final_price: str) -> None:
    ts = datetime.now(BJ)
    out_path = Path("debug") / ts.strftime("%Y-%m-%d") / f"{slug}_price_debug.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts_iso": ts.isoformat(timespec="seconds"),
        "slug": slug,
        "market_url": market_url,
        "target_price_extracted": target_price,
        "final_price_extracted": final_price,
        "json_price_candidates": price_key_candidates(raw),
        "text_candidates": collect_text_candidates(raw),
        "raw_market": raw,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_market_info(slug: str, template_url: str, asset_key: str, timeout: float) -> MarketInfo:
    market = request_json(f"{GAMMA_BASE}/markets?slug={slug}", timeout=timeout)
    if isinstance(market, list):
        if not market:
            raise PolyError(f"No market found for slug: {slug}")
        market = market[0]
    outcomes = try_json_loads(market.get("outcomes")) or []
    token_ids = try_json_loads(market.get("clobTokenIds")) or []
    if not isinstance(outcomes, list) or not isinstance(token_ids, list) or len(outcomes) != len(token_ids):
        raise PolyError(f"Unexpected market schema for slug {slug}: outcomes={outcomes!r}, token_ids={token_ids!r}")
    mapping: dict[str, str] = {}
    for outcome, token_id in zip(outcomes, token_ids):
        if outcome is None or token_id is None:
            continue
        mapping[str(outcome).strip().lower()] = str(token_id)
    up_token = mapping.get("up") or mapping.get("yes")
    down_token = mapping.get("down") or mapping.get("no")
    if not up_token or not down_token:
        raise PolyError(f"Could not map Up/Down tokens from outcomes: {outcomes!r}")
    m = re.search(r"-(\d{10})$", slug)
    if not m:
        raise PolyError(f"Cannot parse epoch from slug: {slug}")
    start_dt_bj = datetime.fromtimestamp(int(m.group(1)), UTC).astimezone(BJ)
    end_dt_bj = start_dt_bj + timedelta(minutes=5)
    window_text = f"{start_dt_bj:%H:%M}-{end_dt_bj:%H:%M}"
    market_url = build_market_url(template_url, slug)
    now_bj = datetime.now(BJ)
    initial_final_price = fetch_live_reference_price(asset_key, timeout)
    target_price = fetch_window_start_reference_price(asset_key, start_dt_bj, timeout)
    if not target_price and initial_final_price and is_near_window_start(now_bj, start_dt_bj):
        target_price = initial_final_price
    if not target_price:
        target_price = extract_threshold_from_text(market)
    if not target_price or not initial_final_price:
        write_debug_artifact(slug, market_url, market, target_price, initial_final_price)
    return MarketInfo(
        slug=slug,
        market_url=market_url,
        up_token_id=up_token,
        down_token_id=down_token,
        window_text=window_text,
        target_price=target_price,
        asset_key=asset_key,
        raw=market,
    )


def fetch_book(token_id: str, timeout: float) -> dict[str, Any]:
    return request_json(f"{CLOB_BASE}/book?token_id={token_id}", timeout=timeout)


def parse_trade_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e15:
            ts /= 1000_000
        elif ts > 1e12:
            ts /= 1000
        return datetime.fromtimestamp(ts, UTC)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.isdigit():
            return parse_trade_timestamp(float(s))
        s = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except Exception:
            return None
    return None


def extract_trade_size(trade: dict[str, Any]) -> float:
    for key in ["size", "amount", "quantity", "shares", "tokenAmount", "filledAmount"]:
        if key in trade:
            try:
                return float(str(trade[key]).replace(",", ""))
            except Exception:
                continue
    return 0.0


def extract_trade_id(trade: dict[str, Any]) -> str:
    for key in ["id", "tradeID", "tradeId", "matchID", "matchId", "transactionHash", "txHash"]:
        if key in trade and trade[key] not in (None, ""):
            return str(trade[key])
    ts = None
    for key in ["timestamp", "createdAt", "created_at", "time", "matchedTime"]:
        if key in trade and trade[key] not in (None, ""):
            ts = str(trade[key])
            break
    size = extract_trade_size(trade)
    return f"fallback:{ts}:{size}"


def extract_trade_time(trade: dict[str, Any]) -> datetime | None:
    for key in ["timestamp", "createdAt", "created_at", "time", "matchedTime", "matched_at"]:
        if key in trade:
            dt = parse_trade_timestamp(trade[key])
            if dt is not None:
                return dt
    return None


def extract_trade_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ["trades", "history", "data", "results"]:
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
    return []


def fetch_recent_trades(slug: str, timeout: float) -> list[dict[str, Any]]:
    urls = [
        f"{CLOB_BASE}/trades?market={slug}",
        f"{CLOB_BASE}/trades?market_slug={slug}",
        f"{CLOB_BASE}/data/trades?market={slug}",
        f"{CLOB_BASE}/data/trades?market_slug={slug}",
    ]
    for url in urls:
        try:
            payload = request_json(url, timeout=timeout)
            trades = extract_trade_list(payload)
            if trades:
                return trades
        except Exception:
            continue
    return []


def summarize_new_trades(slug: str, tracker: TradeTracker, now_utc: datetime, timeout: float) -> tuple[str, str]:
    trades = fetch_recent_trades(slug, timeout)
    if tracker.last_ts is None:
        tracker.last_ts = now_utc - timedelta(seconds=1.5)
    window_start = tracker.last_ts
    count = 0
    volume = 0.0
    for trade in trades:
        trade_dt = extract_trade_time(trade)
        if trade_dt is None:
            continue
        trade_id = extract_trade_id(trade)
        if trade_id in tracker.seen_ids:
            continue
        if not (window_start < trade_dt <= now_utc):
            continue
        tracker.seen_ids[trade_id] = trade_dt
        count += 1
        volume += extract_trade_size(trade)
    tracker.last_ts = now_utc
    cutoff = now_utc - timedelta(minutes=10)
    tracker.seen_ids = {k: v for k, v in tracker.seen_ids.items() if v >= cutoff}
    if count == 0:
        return "", ""
    return str(count), f"{volume:.2f}"


def snapshot_row(info: MarketInfo, timeout: float, trade_tracker: TradeTracker | None = None) -> dict[str, str]:
    up_book = fetch_book(info.up_token_id, timeout=timeout)
    down_book = fetch_book(info.down_token_id, timeout=timeout)
    up_best_bid, up_best_ask = best_bid_ask_from_book(up_book)
    down_best_bid, down_best_ask = best_bid_ask_from_book(down_book)
    now = datetime.now(BJ)
    final_price = fetch_live_reference_price(info.asset_key, timeout)
    trade_count = ""
    trade_volume = ""
    if trade_tracker is not None:
        trade_count, trade_volume = summarize_new_trades(info.slug, trade_tracker, now.astimezone(UTC), timeout)
    return {
        "ts_iso": now.isoformat(timespec="seconds"),
        "slug": info.slug,
        "market_url": info.market_url,
        "window_text": info.window_text,
        "buy_up_cents": cents(up_best_ask),
        "buy_down_cents": cents(down_best_ask),
        "sell_up_cents": cents(up_best_bid),
        "sell_down_cents": cents(down_best_bid),
        "buy_up_size": top_size(up_book.get("asks"), "ask"),
        "buy_down_size": top_size(down_book.get("asks"), "ask"),
        "sell_up_size": top_size(up_book.get("bids"), "bid"),
        "sell_down_size": top_size(down_book.get("bids"), "bid"),
        "mid_up_cents": mid_cents(up_best_bid, up_best_ask),
        "mid_down_cents": mid_cents(down_best_bid, down_best_ask),
        "spread_up_cents": spread_cents(up_best_bid, up_best_ask),
        "spread_down_cents": spread_cents(down_best_bid, down_best_ask),
        "bid_depth_up_5": depth_sum(up_book.get("bids"), 5),
        "ask_depth_up_5": depth_sum(up_book.get("asks"), 5),
        "bid_depth_down_5": depth_sum(down_book.get("bids"), 5),
        "ask_depth_down_5": depth_sum(down_book.get("asks"), 5),
        "level_count_bid_up": level_count(up_book.get("bids")),
        "level_count_ask_up": level_count(up_book.get("asks")),
        "level_count_bid_down": level_count(down_book.get("bids")),
        "level_count_ask_down": level_count(down_book.get("asks")),
        "target_price": info.target_price,
        "final_price": final_price,
        "trade_count_1s": trade_count,
        "trade_volume_1s": trade_volume,
    }


def ensure_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
        return
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    current_header = rows[0] if rows else []
    if current_header == CSV_FIELDS:
        return
    existing_rows: list[dict[str, str]] = []
    if rows:
        old_header = rows[0]
        for values in rows[1:]:
            row_map = {old_header[i]: values[i] if i < len(values) else "" for i in range(len(old_header))}
            existing_rows.append(row_map)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for old_row in existing_rows:
            migrated = {field: old_row.get(field, "") for field in CSV_FIELDS}
            writer.writerow(migrated)


def append_row(path: Path, row: dict[str, str]) -> None:
    ensure_csv(path)
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})


def default_output_path(series_prefix: str, day_bj: datetime) -> Path:
    return Path("data") / day_bj.strftime("%Y-%m-%d") / f"{series_prefix}_quotes.csv"


def resolve_target_slug(args: argparse.Namespace, series_prefix: str) -> str:
    if args.target_slug:
        return args.target_slug.strip()
    if args.target_window_end_hm:
        day_bj = choose_date(args.date_bj)
        end_dt = parse_hm_for_day(day_bj, args.target_window_end_hm)
        start_dt = end_dt - timedelta(minutes=5)
        return epoch_slug_for_window_start(series_prefix, start_dt)
    return current_window_info(series_prefix).slug


def capture_once_current(args: argparse.Namespace, template_url: str, series_prefix: str, asset_key: str, out_path: Path) -> int:
    slug = resolve_target_slug(args, series_prefix)
    info = fetch_market_info(slug, template_url, asset_key, args.timeout)
    tracker = TradeTracker()
    row = snapshot_row(info, args.timeout, tracker)
    append_row(out_path, row)
    print(json.dumps(row, ensure_ascii=False))
    return 0


def capture_loop_current_window(args: argparse.Namespace, template_url: str, series_prefix: str, asset_key: str, out_path: Path) -> int:
    locked = current_window_info(series_prefix)
    info = fetch_market_info(locked.slug, template_url, asset_key, args.timeout)
    tracker = TradeTracker()
    log("locked current window:", f"{locked.start_bj.strftime('%H:%M')}-{locked.end_bj.strftime('%H:%M')}", locked.slug)
    while True:
        now_bj = datetime.now(BJ)
        if now_bj >= locked.end_bj:
            log("current window finished")
            return 0
        try:
            row = snapshot_row(info, args.timeout, tracker)
            append_row(out_path, row)
            print(json.dumps(row, ensure_ascii=False), flush=True)
        except Exception as e:
            log(f"snapshot error at {now_bj.isoformat()}: {e}")
        time.sleep(max(args.sample_seconds, 0.2))


def capture_loop_range(args: argparse.Namespace, template_url: str, series_prefix: str, asset_key: str, out_path: Path) -> int:
    day_bj = choose_date(args.date_bj)
    start_bj = parse_hm_for_day(day_bj, args.range_start_hm)
    end_bj = parse_hm_for_day(day_bj, args.range_end_hm)
    if end_bj <= start_bj:
        raise PolyError("range-end-hm must be after range-start-hm")
    log(f"capture window: {start_bj.isoformat()} -> {end_bj.isoformat()}")
    cached_slug = ""
    cached_info: MarketInfo | None = None
    tracker = TradeTracker()
    while True:
        now_bj = datetime.now(BJ)
        if now_bj >= end_bj:
            log("range finished")
            return 0
        if now_bj < start_bj:
            sleep_s = min(max((start_bj - now_bj).total_seconds(), 0.2), args.sample_seconds)
            time.sleep(sleep_s)
            continue
        locked = current_window_info(series_prefix, now_bj)
        try:
            if cached_slug != locked.slug or cached_info is None:
                cached_info = fetch_market_info(locked.slug, template_url, asset_key, args.timeout)
                cached_slug = locked.slug
                tracker = TradeTracker()
            row = snapshot_row(cached_info, args.timeout, tracker)
            append_row(out_path, row)
            print(json.dumps(row, ensure_ascii=False), flush=True)
        except Exception as e:
            log(f"snapshot error at {now_bj.isoformat()}: {e}")
        time.sleep(max(args.sample_seconds, 0.2))


def capture_loop_next_hours(args: argparse.Namespace, template_url: str, series_prefix: str, asset_key: str, out_path: Path) -> int:
    start_bj = datetime.now(BJ)
    end_bj = start_bj + timedelta(hours=args.duration_hours)
    log(f"capture next-hours: {start_bj.isoformat()} -> {end_bj.isoformat()}")
    cached_slug = ""
    cached_info: MarketInfo | None = None
    tracker = TradeTracker()
    while True:
        now_bj = datetime.now(BJ)
        if now_bj >= end_bj:
            log("next-hours finished")
            return 0
        locked = current_window_info(series_prefix, now_bj)
        try:
            if cached_slug != locked.slug or cached_info is None:
                cached_info = fetch_market_info(locked.slug, template_url, asset_key, args.timeout)
                cached_slug = locked.slug
                tracker = TradeTracker()
            row = snapshot_row(cached_info, args.timeout, tracker)
            append_row(out_path, row)
            print(json.dumps(row, ensure_ascii=False), flush=True)
        except Exception as e:
            log(f"snapshot error at {now_bj.isoformat()}: {e}")
        time.sleep(max(args.sample_seconds, 0.2))


def main() -> int:
    args = parse_args()
    template_url = normalize_url(args.market_url)
    seed_slug = slug_from_market_url(template_url)
    series_prefix = series_prefix_from_slug(seed_slug)
    asset_key = infer_asset_key(series_prefix)
    day_bj = choose_date(args.date_bj)
    out_path = Path(args.output_csv) if args.output_csv else default_output_path(series_prefix, day_bj)
    try:
        if args.mode == "once-current":
            return capture_once_current(args, template_url, series_prefix, asset_key, out_path)
        if args.mode == "loop-current-window":
            return capture_loop_current_window(args, template_url, series_prefix, asset_key, out_path)
        if args.mode == "loop-range":
            return capture_loop_range(args, template_url, series_prefix, asset_key, out_path)
        return capture_loop_next_hours(args, template_url, series_prefix, asset_key, out_path)
    except requests.HTTPError as e:
        body = ""
        try:
            body = e.response.text[:500]
        except Exception:
            pass
        log(f"HTTP error: {e}; body={body}")
        return 2
    except Exception as e:
        log(f"fatal: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
