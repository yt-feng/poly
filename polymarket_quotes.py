#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html as html_lib
import json
import re
import sys
import time
from dataclasses import dataclass
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
    "market_url",
    "window_text",
    "buy_up_cents",
    "buy_down_cents",
    "sell_up_cents",
    "sell_down_cents",
    "target_price",
    "final_price",
]

TARGET_PRICE_KEYS = {
    "targetprice",
    "target_price",
    "strike",
    "strikeprice",
    "strike_price",
    "referenceprice",
    "reference_price",
    "startprice",
    "start_price",
    "openprice",
    "open_price",
}

FINAL_PRICE_KEYS = {
    "finalprice",
    "final_price",
    "settlementprice",
    "settlement_price",
    "resolvedprice",
    "resolved_price",
    "closingprice",
    "closing_price",
    "endprice",
    "end_price",
    "currentprice",
    "current_price",
    "referencefinalprice",
    "reference_final_price",
    "markprice",
    "mark_price",
}

TEXT_HINT_KEYS = {
    "question",
    "title",
    "description",
    "subtitle",
    "resolutioncriteria",
    "resolution_criteria",
    "rules",
}

TARGET_LABEL_PATTERNS = [
    r"target\s*price",
    r"start\s*price",
    r"strike\s*price",
    r"open\s*price",
    r"目标价格",
    r"起始价格",
]

FINAL_LABEL_PATTERNS = [
    r"final\s*price",
    r"settlement\s*price",
    r"resolved\s*price",
    r"closing\s*price",
    r"current\s*price",
    r"mark\s*price",
    r"最终价格",
    r"结算价格",
    r"当前价格",
]


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
    final_price: str
    raw: dict[str, Any]


@dataclass
class WindowInfo:
    start_bj: datetime
    end_bj: datetime
    slug: str


def log(*args: Any) -> None:
    print(*args, file=sys.stderr, flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Capture Polymarket 5-minute quote snapshots.")
    p.add_argument("--market-url", required=True, help="Any market url in the same 5m series, e.g. https://polymarket.com/zh/event/btc-updown-5m-1776752100")
    p.add_argument("--mode", choices=["once-current", "loop-current-window", "loop-range"], default="loop-current-window")
    p.add_argument("--date-bj", default="today", help="Date in Beijing timezone, format YYYY-MM-DD, or 'today'.")
    p.add_argument("--range-start-hm", default="16:15", help="For loop-range, inclusive start HH:MM in Beijing time.")
    p.add_argument("--range-end-hm", default="16:30", help="For loop-range, exclusive end HH:MM in Beijing time.")
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


def best_bid_ask_from_book(book: dict[str, Any]) -> tuple[float | None, float | None]:
    bids = book.get("bids") or []
    asks = book.get("asks") or []

    def parse_price(level: Any) -> float | None:
        if isinstance(level, dict):
            p = level.get("price")
        elif isinstance(level, (list, tuple)) and level:
            p = level[0]
        else:
            p = None
        if p in (None, ""):
            return None
        try:
            return float(p)
        except Exception:
            return None

    bid_prices = [p for p in (parse_price(x) for x in bids) if p is not None]
    ask_prices = [p for p in (parse_price(x) for x in asks) if p is not None]
    best_bid = max(bid_prices) if bid_prices else None
    best_ask = min(ask_prices) if ask_prices else None
    return best_bid, best_ask


def cents(v: float | None) -> str:
    if v is None:
        return ""
    return f"{v * 100:.2f}"


def normalize_number_string(value: Any) -> str:
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


def iter_nodes(obj: Any):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            yield from iter_nodes(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_nodes(item)


def find_first_value_by_keys(raw: dict[str, Any], keys: set[str]) -> str:
    for key, value in iter_nodes(raw):
        key_norm = str(key).replace("-", "_").lower()
        key_flat = key_norm.replace("_", "")
        if key_norm in keys or key_flat in keys:
            normalized = normalize_number_string(value)
            if normalized:
                return normalized
    return ""


def collect_text_candidates(raw: dict[str, Any]) -> list[str]:
    texts: list[str] = []
    for key, value in iter_nodes(raw):
        key_norm = str(key).replace("-", "_").lower()
        if key_norm in TEXT_HINT_KEYS and isinstance(value, str):
            text = value.strip()
            if text:
                texts.append(text)
    return texts


def extract_target_price_from_json(raw: dict[str, Any]) -> str:
    direct = find_first_value_by_keys(raw, TARGET_PRICE_KEYS)
    if direct:
        return direct
    for text in collect_text_candidates(raw):
        m = re.search(
            r"(?:above|below|over|under|at|target\s*price[^0-9$]*)\$?\s*([0-9]{1,3}(?:,[0-9]{3})+(?:\.\d+)?|[0-9]{4,}(?:\.\d+)?)",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            return normalize_number_string(m.group(1))
    return ""


def extract_final_price_from_json(raw: dict[str, Any]) -> str:
    direct = find_first_value_by_keys(raw, FINAL_PRICE_KEYS)
    if direct:
        return direct
    for text in collect_text_candidates(raw):
        m = re.search(
            r"(?:final\s*price|settlement\s*price|resolved\s*price|closing\s*price|current\s*price)[^0-9$]*\$?\s*([0-9]{1,3}(?:,[0-9]{3})+(?:\.\d+)?|[0-9]{4,}(?:\.\d+)?)",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            return normalize_number_string(m.group(1))
    return ""


def extract_price_by_key_regex(text: str, keys: set[str]) -> str:
    for key in sorted(keys):
        key_json = re.escape(key)
        patterns = [
            rf'"{key_json}"\s*:\s*"?([0-9][0-9,]*(?:\.[0-9]+)?)"?',
            rf"{key_json}\s*=\s*\"?([0-9][0-9,]*(?:\.[0-9]+)?)\"?",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                return normalize_number_string(m.group(1))
    return ""


def extract_price_by_label_regex(text: str, label_patterns: list[str]) -> str:
    for label in label_patterns:
        patterns = [
            rf"{label}[^0-9$]{{0,80}}\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
            rf"([0-9][0-9,]*(?:\.[0-9]+)?)[^0-9]{{0,30}}{label}",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                return normalize_number_string(m.group(1))
    return ""


def extract_prices_from_html(html_text: str) -> tuple[str, str]:
    expanded = html_lib.unescape(html_text)
    target_price = extract_price_by_key_regex(expanded, TARGET_PRICE_KEYS)
    final_price = extract_price_by_key_regex(expanded, FINAL_PRICE_KEYS)
    if not target_price:
        target_price = extract_price_by_label_regex(expanded, TARGET_LABEL_PATTERNS)
    if not final_price:
        final_price = extract_price_by_label_regex(expanded, FINAL_LABEL_PATTERNS)
    return target_price, final_price


def price_key_candidates(raw: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in iter_nodes(raw):
        key_text = str(key)
        if "price" in key_text.lower() or key_text.lower() in {"strike", "reference"}:
            if isinstance(value, (str, int, float, bool)) or value is None:
                out[key_text] = value
    return out


def html_price_snippets(html_text: str) -> list[str]:
    expanded = html_lib.unescape(html_text)
    snippets: list[str] = []
    for keyword in ["target", "final", "current", "settlement", "price", "目标价格", "最终价格", "当前价格"]:
        for m in re.finditer(keyword, expanded, flags=re.IGNORECASE):
            start = max(0, m.start() - 120)
            end = min(len(expanded), m.end() + 180)
            snippet = re.sub(r"\s+", " ", expanded[start:end]).strip()
            if snippet and snippet not in snippets:
                snippets.append(snippet)
            if len(snippets) >= 20:
                return snippets
    return snippets


def write_debug_artifact(slug: str, market_url: str, raw: dict[str, Any], html_text: str, target_price: str, final_price: str) -> None:
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
        "html_price_snippets": html_price_snippets(html_text),
        "raw_market": raw,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_market_info(slug: str, template_url: str, timeout: float) -> MarketInfo:
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
    target_price = extract_target_price_from_json(market)
    final_price = extract_final_price_from_json(market)
    html_text = ""

    if not target_price or not final_price:
        try:
            html_text = request_text(market_url, timeout=timeout)
            html_target, html_final = extract_prices_from_html(html_text)
            if not target_price:
                target_price = html_target
            if not final_price:
                final_price = html_final
        except Exception as e:
            log(f"html fallback failed for {slug}: {e}")

    if not target_price or not final_price:
        write_debug_artifact(slug, market_url, market, html_text, target_price, final_price)

    return MarketInfo(
        slug=slug,
        market_url=market_url,
        up_token_id=up_token,
        down_token_id=down_token,
        window_text=window_text,
        target_price=target_price,
        final_price=final_price,
        raw=market,
    )


def fetch_book(token_id: str, timeout: float) -> dict[str, Any]:
    return request_json(f"{CLOB_BASE}/book?token_id={token_id}", timeout=timeout)


def snapshot_row(info: MarketInfo, timeout: float) -> dict[str, str]:
    up_book = fetch_book(info.up_token_id, timeout=timeout)
    down_book = fetch_book(info.down_token_id, timeout=timeout)
    sell_up, buy_up = best_bid_ask_from_book(up_book)
    sell_down, buy_down = best_bid_ask_from_book(down_book)
    now = datetime.now(BJ)
    return {
        "ts_iso": now.isoformat(timespec="seconds"),
        "market_url": info.market_url,
        "window_text": info.window_text,
        "buy_up_cents": cents(buy_up),
        "buy_down_cents": cents(buy_down),
        "sell_up_cents": cents(sell_up),
        "sell_down_cents": cents(sell_down),
        "target_price": info.target_price,
        "final_price": info.final_price,
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


def capture_once_current(args: argparse.Namespace, template_url: str, series_prefix: str, out_path: Path) -> int:
    slug = resolve_target_slug(args, series_prefix)
    info = fetch_market_info(slug, template_url, args.timeout)
    row = snapshot_row(info, args.timeout)
    append_row(out_path, row)
    print(json.dumps(row, ensure_ascii=False))
    return 0


def capture_loop_current_window(args: argparse.Namespace, template_url: str, series_prefix: str, out_path: Path) -> int:
    locked = current_window_info(series_prefix)
    log("locked current window:", f"{locked.start_bj.strftime('%H:%M')}-{locked.end_bj.strftime('%H:%M')}", locked.slug)
    while True:
        now_bj = datetime.now(BJ)
        if now_bj >= locked.end_bj:
            log("current window finished")
            return 0
        try:
            info = fetch_market_info(locked.slug, template_url, args.timeout)
            row = snapshot_row(info, args.timeout)
            append_row(out_path, row)
            print(json.dumps(row, ensure_ascii=False), flush=True)
        except Exception as e:
            log(f"snapshot error at {now_bj.isoformat()}: {e}")
        time.sleep(max(args.sample_seconds, 0.2))


def capture_loop_range(args: argparse.Namespace, template_url: str, series_prefix: str, out_path: Path) -> int:
    day_bj = choose_date(args.date_bj)
    start_bj = parse_hm_for_day(day_bj, args.range_start_hm)
    end_bj = parse_hm_for_day(day_bj, args.range_end_hm)
    if end_bj <= start_bj:
        raise PolyError("range-end-hm must be after range-start-hm")

    log(f"capture window: {start_bj.isoformat()} -> {end_bj.isoformat()}")
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
            info = fetch_market_info(locked.slug, template_url, args.timeout)
            row = snapshot_row(info, args.timeout)
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
    day_bj = choose_date(args.date_bj)
    out_path = Path(args.output_csv) if args.output_csv else default_output_path(series_prefix, day_bj)
    try:
        if args.mode == "once-current":
            return capture_once_current(args, template_url, series_prefix, out_path)
        if args.mode == "loop-current-window":
            return capture_loop_current_window(args, template_url, series_prefix, out_path)
        return capture_loop_range(args, template_url, series_prefix, out_path)
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
