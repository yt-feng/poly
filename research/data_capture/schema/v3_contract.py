"""Strict offline contract for reproducible v3 quote/trade observations.

The contract is intentionally stricter than the historical CSV archives. A row
is usable for a future out-of-sample replay only when event and receive times,
market/token identity, level data, execution rules, and provenance are all
present. Invalid rows are quarantined by reason; no metadata is invented.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
from statistics import median
from typing import Any, Iterable


SCHEMA_VERSION = 3
MAX_LATENCY_MS = 60_000
MAX_FUTURE_SKEW_MS = 2_000
OBSERVATION_TYPES = {"book_snapshot", "trade"}
BOOK_SIDES = {"bids", "asks"}
TOKEN_ROLES = {"up", "down"}
FORBIDDEN_KEYS = {
    "label", "labels", "outcome", "outcome_up", "winner", "winning_asset_id",
    "final_price", "target_price", "settlement", "resolved", "pnl", "payoff",
}
ALLOWED_TOP_LEVEL = {
    "schema_version", "observation_id", "source_event_time_ms", "received_time_ms", "market_id", "condition_id",
    "token_ids", "observation_type", "fees", "rules", "provenance", "books", "trade",
}


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def _positive_int(value: Any) -> bool:
    return type(value) is int and value > 0


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _iso_utc_ms(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return int(parsed.astimezone(timezone.utc).timestamp() * 1000)


def _walk_keys(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        for key, child in value.items():
            yield str(key)
            yield from _walk_keys(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_keys(child)


def _required(record: dict[str, Any], fields: Iterable[str], reasons: list[str], prefix: str = "") -> None:
    for field in fields:
        if field not in record:
            reasons.append(f"missing_{prefix}{field}")


def _validate_levels(book: Any, reasons: list[str], token_role: str) -> None:
    if not isinstance(book, dict):
        reasons.append(f"invalid_{token_role}_book")
        return
    if set(book) != BOOK_SIDES:
        reasons.append(f"invalid_{token_role}_book_keys")
        return
    for side in sorted(BOOK_SIDES):
        levels = book[side]
        if not isinstance(levels, list) or not levels:
            reasons.append(f"missing_{token_role}_{side}")
            continue
        prior = -1
        for item in levels:
            if not isinstance(item, dict) or set(item) != {"level", "price", "size"}:
                reasons.append(f"invalid_{token_role}_{side}_level")
                continue
            level = item["level"]
            if type(level) is not int or level < 0 or level <= prior:
                reasons.append(f"invalid_{token_role}_{side}_level_order")
            prior = level if type(level) is int else prior
            if not _finite(item["price"]) or not 0 < float(item["price"]) < 1:
                reasons.append(f"invalid_{token_role}_{side}_price")
            if not _finite(item["size"]) or float(item["size"]) < 0:
                reasons.append(f"invalid_{token_role}_{side}_size")


def validate_record(record: Any) -> tuple[bool, list[str]]:
    """Validate one canonical record without returning its content."""
    reasons: list[str] = []
    if not isinstance(record, dict):
        return False, ["record_not_object"]
    forbidden = sorted(set(_walk_keys(record)) & FORBIDDEN_KEYS)
    if forbidden:
        reasons.append("forbidden_future_field:" + ",".join(forbidden))
    unknown = sorted(set(record) - ALLOWED_TOP_LEVEL)
    if unknown:
        reasons.append("unknown_top_level_field:" + ",".join(unknown))
    if record.get("schema_version") != SCHEMA_VERSION:
        reasons.append("schema_version_not_3")
    _required(record, ("observation_id", "source_event_time_ms", "received_time_ms", "market_id", "condition_id",
                       "token_ids", "observation_type", "fees", "rules", "provenance"), reasons)
    for field in ("observation_id", "market_id", "condition_id"):
        if field in record and not _nonempty(record[field]):
            reasons.append(f"invalid_{field}")
    event_ms, received_ms = record.get("source_event_time_ms"), record.get("received_time_ms")
    if not _positive_int(event_ms):
        reasons.append("invalid_source_event_time_ms")
    if not _positive_int(received_ms):
        reasons.append("invalid_received_time_ms")
    if _positive_int(event_ms) and _positive_int(received_ms):
        latency = received_ms - event_ms
        if latency < -MAX_FUTURE_SKEW_MS:
            reasons.append("source_event_after_receive")
        elif latency > MAX_LATENCY_MS:
            reasons.append("latency_exceeds_60000ms")
    tokens = record.get("token_ids")
    if not isinstance(tokens, dict) or set(tokens) != TOKEN_ROLES or any(not _nonempty(tokens.get(k)) for k in TOKEN_ROLES):
        reasons.append("invalid_token_ids")
    kind = record.get("observation_type")
    if kind not in OBSERVATION_TYPES:
        reasons.append("invalid_observation_type")
    fees = record.get("fees")
    if not isinstance(fees, dict) or set(fees) != {"rate", "exponent", "asset"}:
        reasons.append("invalid_fees")
    elif (not _finite(fees["rate"]) or float(fees["rate"]) < 0 or
          not _finite(fees["exponent"]) or float(fees["exponent"]) < 0 or
          not _nonempty(fees["asset"])):
        reasons.append("invalid_fee_values")
    rules = record.get("rules")
    if not isinstance(rules, dict) or set(rules) != {"tick_size", "min_order_size"}:
        reasons.append("invalid_rules")
    elif (not _finite(rules["tick_size"]) or float(rules["tick_size"]) <= 0 or
          not _finite(rules["min_order_size"]) or float(rules["min_order_size"]) <= 0):
        reasons.append("invalid_rule_values")
    provenance = record.get("provenance")
    if not isinstance(provenance, dict) or set(provenance) != {"source", "capture_id", "source_sha256", "retrieved_at_ms"}:
        reasons.append("invalid_provenance")
    elif (not _nonempty(provenance["source"]) or not _nonempty(provenance["capture_id"]) or
          not isinstance(provenance["source_sha256"], str) or
          not re.fullmatch(r"[0-9a-f]{64}", provenance["source_sha256"]) or
          not _positive_int(provenance["retrieved_at_ms"])):
        reasons.append("invalid_provenance_values")
    if kind == "book_snapshot":
        books = record.get("books")
        if not isinstance(books, dict) or set(books) != TOKEN_ROLES:
            reasons.append("invalid_books")
        else:
            for role in sorted(TOKEN_ROLES):
                _validate_levels(books[role], reasons, role)
    elif kind == "trade":
        trade = record.get("trade")
        if not isinstance(trade, dict) or set(trade) != {"trade_id", "price", "size", "side"}:
            reasons.append("invalid_trade")
        elif (not _nonempty(trade["trade_id"]) or not _finite(trade["price"]) or not 0 < float(trade["price"]) < 1 or
              not _finite(trade["size"]) or float(trade["size"]) <= 0 or trade["side"] not in {"buy", "sell"}):
            reasons.append("invalid_trade_values")
    return not reasons, sorted(set(reasons))


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            records.append({"_line_number": line_number})
            continue
        if isinstance(value, dict):
            records.append(value)
        else:
            records.append({"_line_number": line_number, "_not_object": value})
    return records


def alignment_report(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    latencies = []
    future = 0
    missing = 0
    for record in records:
        event, received = record.get("source_event_time_ms"), record.get("received_time_ms")
        if not _positive_int(event) or not _positive_int(received):
            missing += 1
            continue
        value = received - event
        latencies.append(value)
        future += value < 0
    latencies.sort()
    percentile = lambda fraction: latencies[min(len(latencies) - 1, int((len(latencies) - 1) * fraction))] if latencies else None
    return {
        "records_with_both_timestamps": len(latencies), "missing_timestamp_records": missing,
        "future_event_records": future, "min_latency_ms": min(latencies) if latencies else None,
        "median_latency_ms": median(latencies) if latencies else None,
        "p95_latency_ms": percentile(0.95), "max_latency_ms": max(latencies) if latencies else None,
    }


def missingness_report(records: Iterable[dict[str, Any]]) -> dict[str, int]:
    records = list(records)
    paths = (
        "schema_version", "observation_id", "source_event_time_ms", "received_time_ms", "market_id",
        "condition_id", "token_ids", "observation_type", "fees", "fees.rate", "fees.exponent",
        "fees.asset", "rules", "rules.tick_size", "rules.min_order_size", "provenance",
        "provenance.source", "provenance.capture_id", "provenance.source_sha256", "provenance.retrieved_at_ms",
    )
    missing = {}
    for path in paths:
        count = 0
        for record in records:
            current: Any = record
            for part in path.split("."):
                current = current.get(part) if isinstance(current, dict) else None
            if current is None or current == "":
                count += 1
        missing[path] = count
    return missing


def validate_records(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    records = list(records)
    accepted, quarantined, reason_counts = [], [], Counter()
    seen: dict[str, str] = {}
    for index, record in enumerate(records):
        ok, reasons = validate_record(record)
        observation_id = record.get("observation_id") if isinstance(record, dict) else None
        if ok and observation_id in seen:
            digest = hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()
            if seen[observation_id] != digest:
                reasons = ["conflicting_duplicate_observation_id"]
            else:
                reasons = ["duplicate_observation_id"]
            ok = False
        if ok:
            accepted.append(record)
            seen[observation_id] = hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()
        else:
            quarantined.append({"index": index, "observation_id": observation_id if isinstance(observation_id, str) else None,
                                "reasons": reasons or ["not_qualifying"]})
            reason_counts.update(reasons or ["not_qualifying"])
    return {
        "schema_version": SCHEMA_VERSION, "input_records": len(records),
        "accepted_records": len(accepted), "quarantined_records": len(quarantined),
        "reason_counts": dict(sorted(reason_counts.items())),
        "alignment": alignment_report(records),
        "accepted_alignment": alignment_report(accepted),
        "missingness": missingness_report(records),
        "leakage_check": {"passed": not any(key in FORBIDDEN_KEYS for record in records for key in _walk_keys(record)),
                          "forbidden_keys": sorted(set(key for record in records for key in _walk_keys(record)) & FORBIDDEN_KEYS)},
        "quarantine": quarantined,
        "accepted": accepted,
    }


def validate_file(input_path: Path, output_path: Path) -> dict[str, Any]:
    report = validate_records(load_jsonl(input_path))
    report["input"] = str(input_path)
    report["output_is_diagnostic_only"] = True
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps({k: v for k, v in report.items() if k != "accepted"}, indent=2, sort_keys=True), encoding="utf-8")
    return report


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = validate_file(args.input, args.output)
    print(json.dumps({k: result[k] for k in ("input_records", "accepted_records", "quarantined_records", "reason_counts", "alignment", "leakage_check")}, sort_keys=True))
