"""Offline, read-only audit for published ``equity_daily`` release bundles.

The collector deliberately publishes immutable tar files rather than committing
raw observations to git.  This module audits assets that an operator has already
downloaded: it checks the outer and inner SHA256 sidecars, extracts only safe
members, and reports observed wire rows, source timestamps, token IDs and chart
bars.  It never downloads a release, contacts a feed, decrypts data, or treats a
manifest's cumulative inventory as proof that every referenced file is in the
selected tar.

Example::

    python -m equity_daily.release_audit \
      --input-dir /tmp/equity-release-audit \
      --metadata equity_daily/reports/equity-daily-v1-37228795593-1.metadata.json \
      --output /tmp/equity-daily-audit.json

Only the JSON report (with metadata and aggregate counts) should be committed;
raw tars and extracted payloads belong in a private temporary directory.
"""

from __future__ import annotations

import argparse
import base64
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import tarfile
import tempfile
from typing import Any, Iterable


HEX64 = re.compile(r"^[0-9a-f]{64}$")
SOURCE_FILES = {
    "polymarket_ws": "equity_daily-polymarket_ws-*.jsonl.gz",
    "underlying_yahoo_decoded": "equity_daily-underlying_yahoo_decoded-*.jsonl.gz",
    "underlying_yahoo_chart": "equity_daily-underlying_yahoo_chart-*.jsonl.gz",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sidecar_value(path: Path) -> tuple[str, str]:
    parts = path.read_text(encoding="utf-8").strip().split()
    if len(parts) != 2 or not HEX64.fullmatch(parts[0]):
        raise ValueError(f"invalid SHA256 sidecar: {path.name}")
    name = Path(parts[1].replace("\\", "/")).name
    if name != parts[1].replace("\\", "/"):
        raise ValueError(f"sidecar path is not a basename: {path.name}")
    return parts[0], name


def _iso_ms(value: int | float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _iso_ns(value: int | float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value) / 1_000_000_000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _range(values: Iterable[int | float], unit: str) -> dict[str, Any] | None:
    values = list(values)
    if not values:
        return None
    low, high = min(values), max(values)
    convert = _iso_ms if unit == "ms" else _iso_ns
    return {"min": low, "max": high, "min_utc": convert(low), "max_utc": convert(high), "unit": unit}


def _decode_payload(row: dict[str, Any]) -> Any:
    encoded = row.get("payload_b64")
    if encoded is None:
        return None
    raw = base64.b64decode(encoded, validate=True)
    expected = row.get("payload_sha256")
    if expected and hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("payload_sha256 mismatch")
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _safe_extract(bundle: Path, destination: Path) -> list[str]:
    names: list[str] = []
    with tarfile.open(bundle, "r") as archive:
        members = archive.getmembers()
        for member in members:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or not member.name:
                raise ValueError(f"unsafe tar member: {member.name!r}")
            if member.issym() or member.islnk() or not (member.isfile() or member.isdir()):
                raise ValueError(f"unsupported tar member: {member.name!r}")
        for member in members:
            archive.extract(member, destination, filter="data")
            names.append(member.name)
    return names


def _member_hashes(root: Path) -> dict[str, Any]:
    checks = []
    errors = []
    for sidecar in sorted(root.rglob("*.sha256")):
        try:
            expected, name = _sidecar_value(sidecar)
            data = sidecar.parent / name
            actual = sha256_file(data) if data.is_file() else None
            checks.append({"sidecar": sidecar.name, "file": name, "expected": expected,
                           "actual": actual, "ok": actual == expected})
            if actual != expected:
                errors.append(f"member hash mismatch: {sidecar.name}")
        except (OSError, ValueError) as exc:
            errors.append(str(exc))
    return {"checked": len(checks), "passed": sum(x["ok"] for x in checks),
            "failed": len(errors), "checks": checks, "errors": errors}


def _inspect_source(path: Path, source: str) -> dict[str, Any]:
    outer_rows = 0
    receipt_ns: list[int] = []
    source_ms: list[int] = []
    event_types: Counter[str] = Counter()
    token_ids: set[str] = set()
    symbols: Counter[str] = Counter()
    chart_bars = 0
    payload_checks = 0
    payload_errors = 0

    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            if not line.strip():
                continue
            outer_rows += 1
            row = json.loads(line)
            if isinstance(row.get("received_at_ns"), (int, float)):
                receipt_ns.append(int(row["received_at_ns"]))
            if row.get("payload_b64") is not None:
                payload_checks += 1
            try:
                payload = _decode_payload(row)
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                payload_errors += 1
                raise ValueError(f"{path.name}:{line_no}: {exc}") from exc

            if source == "polymarket_ws":
                events = payload if isinstance(payload, list) else [payload]
                for event in events:
                    if not isinstance(event, dict):
                        continue
                    event_types[str(event.get("event_type", "unknown"))] += 1
                    timestamp = event.get("timestamp")
                    if isinstance(timestamp, (int, float)) or (isinstance(timestamp, str) and timestamp.isdigit()):
                        source_ms.append(int(timestamp))
                    for key in ("asset_id", "token_id"):
                        if event.get(key) is not None:
                            token_ids.add(str(event[key]))
                    for change in event.get("price_changes", []) or []:
                        if isinstance(change, dict) and change.get("asset_id") is not None:
                            token_ids.add(str(change["asset_id"]))
            elif source == "underlying_yahoo_decoded":
                data = row.get("data") or {}
                if data.get("id") is not None:
                    symbols[str(data["id"])] += 1
                if isinstance(data.get("time"), (int, float)):
                    source_ms.append(int(data["time"]))
            elif source == "underlying_yahoo_chart":
                chart = (payload or {}).get("chart", {}) if isinstance(payload, dict) else {}
                for result in chart.get("result", []) or []:
                    if not isinstance(result, dict):
                        continue
                    meta = result.get("meta") or {}
                    if meta.get("symbol") is not None:
                        symbols[str(meta["symbol"])] += 1
                    timestamps = result.get("timestamp") or []
                    chart_bars += len(timestamps)
                    source_ms.extend(int(value) * 1000 for value in timestamps if isinstance(value, (int, float)))

    output: dict[str, Any] = {
        "file": path.name,
        "outer_rows": outer_rows,
        "received_at_ns": _range(receipt_ns, "ns"),
        "source_timestamp": _range(source_ms, "ms"),
        "payload_sha256_checks": payload_checks,
        "payload_sha256_failures": payload_errors,
    }
    if source == "polymarket_ws":
        output.update({"event_count": sum(event_types.values()), "event_type_counts": dict(sorted(event_types.items())),
                       "observed_token_id_count": len(token_ids)})
    if source in {"underlying_yahoo_decoded", "underlying_yahoo_chart"}:
        output["symbols"] = dict(sorted(symbols.items()))
    if source == "underlying_yahoo_chart":
        output["chart_bar_count"] = chart_bars
    return output


def audit_bundle(bundle: Path, outer_sidecar: Path) -> dict[str, Any]:
    expected, sidecar_name = _sidecar_value(outer_sidecar)
    if sidecar_name != bundle.name:
        raise ValueError(f"outer sidecar names {sidecar_name!r}, not {bundle.name!r}")
    actual = sha256_file(bundle)
    result: dict[str, Any] = {
        "bundle": bundle.name,
        "size_bytes": bundle.stat().st_size,
        "outer_sha256": {"expected": expected, "actual": actual, "ok": actual == expected},
    }
    if actual != expected:
        raise ValueError(f"outer hash mismatch: {bundle.name}")

    with tempfile.TemporaryDirectory(prefix="equity-daily-release-audit-") as temporary:
        root = Path(temporary)
        _safe_extract(bundle, root)
        result["member_hashes"] = _member_hashes(root)
        manifests = sorted(root.glob("manifest-*.json"))
        if len(manifests) != 1:
            raise ValueError(f"expected one manifest in {bundle.name}, found {len(manifests)}")
        manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
        declared_files = manifest.get("files", [])
        names = {p.name for p in root.iterdir() if p.is_file()}
        declared_names = {str(item.get("file")) for item in declared_files if isinstance(item, dict)}
        result["manifest"] = {
            "file": manifests[0].name,
            "run_id": manifest.get("run_id"),
            "code_sha": manifest.get("code_sha"),
            "declared_file_count": len(declared_files),
            "declared_files_present_in_tar": len(declared_names & names),
            "declared_files_missing_from_tar": len(declared_names - names),
            "counts": manifest.get("counts", {}),
        }
        health = manifest.get("health") or {}
        result["health_claim"] = {
            "desired_token_count": health.get("desired_token_count"),
            "tokens_with_ws_events": health.get("tokens_with_ws_events"),
            "missing_ws_token_count": len(health.get("missing_ws_tokens") or []),
            "polymarket_token_coverage_entries": len(health.get("polymarket_token_coverage") or {}),
            "alpaca": health.get("alpaca"),
            "futures_metadata_only_symbols": sorted(
                symbol for symbol, status in (health.get("symbol_status") or {}).items()
                if isinstance(status, dict) and status.get("state") == "metadata_only_no_bars"
            ),
        }
        source_reports: dict[str, list[dict[str, Any]]] = {}
        for source, pattern in SOURCE_FILES.items():
            files = sorted(root.glob(pattern))
            source_reports[source] = [_inspect_source(path, source) for path in files]
        result["sources"] = source_reports
        pm_tokens = [r.get("observed_token_id_count", 0) for r in source_reports["polymarket_ws"]]
        result["coverage_reconciliation"] = {
            "manifest_declared": {
                "desired": health.get("desired_token_count"),
                "with_ws_events": health.get("tokens_with_ws_events"),
                "missing": len(health.get("missing_ws_tokens") or []),
            },
            "independent_sample": {
                "observed_token_ids_by_ws_file": pm_tokens,
                "scope": "only the selected tar's WS segment; not a release-wide claim",
            },
        }
    return result


def audit_directory(input_dir: Path, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    bundles = sorted(input_dir.glob("equity-daily-bundle-*.tar"))
    if not bundles:
        raise ValueError(f"no equity-daily bundle tars found in {input_dir}")
    reports = []
    errors = []
    for bundle in bundles:
        sidecar = bundle.with_name(bundle.name + ".sha256")
        try:
            reports.append(audit_bundle(bundle, sidecar))
        except (OSError, tarfile.TarError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{bundle.name}: {exc}")
    report: dict[str, Any] = {
        "schema_version": 1,
        "audit": "offline_equity_daily_release_content_v1",
        "sample_directory": input_dir.name,
        "bundle_count": len(bundles),
        "integrity_ok": not errors and len(reports) == len(bundles),
        "errors": errors,
        "bundles": reports,
    }
    if metadata is not None:
        report["release"] = metadata
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    metadata = json.loads(args.metadata.read_text(encoding="utf-8")) if args.metadata else None
    report = audit_directory(args.input_dir, metadata)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0 if report["integrity_ok"] else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
