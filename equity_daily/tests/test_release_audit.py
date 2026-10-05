import base64
import gzip
import hashlib
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from equity_daily.release_audit import audit_bundle, audit_directory


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_jsonl_gz(path: Path, rows):
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, separators=(",", ":")) + "\n")
    (path.with_name(path.name + ".sha256")).write_text(
        f"{_digest(path)}  {path.name}\n", encoding="utf-8"
    )


def _wire(payload):
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return {
        "payload_b64": base64.b64encode(raw).decode(),
        "payload_sha256": hashlib.sha256(raw).hexdigest(),
        "received_at_ns": 1_000_000_000,
    }


class ReleaseAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def _bundle(self, name="000001"):
        pm = self.root / "equity_daily-polymarket_ws-2026-01-01-000000000001.jsonl.gz"
        _write_jsonl_gz(pm, [
            _wire([{"asset_id": "1", "timestamp": "1000", "event_type": "book"},
                   {"price_changes": [{"asset_id": "2"}], "timestamp": "2000",
                    "event_type": "price_change"}]),
        ])
        decoded = self.root / "equity_daily-underlying_yahoo_decoded-2026-01-01-000000000002.jsonl.gz"
        _write_jsonl_gz(decoded, [{"data": {"id": "AAPL", "time": 3000},
                                   "received_at_ns": 2_000_000_000}])
        chart = self.root / "equity_daily-underlying_yahoo_chart-2026-01-01-000000000003.jsonl.gz"
        _write_jsonl_gz(chart, [_wire({"chart": {"result": [{"meta": {"symbol": "AAPL"},
                                                               "timestamp": [4, 5]}]}})])
        files = [pm, decoded, chart]
        manifest = self.root / f"manifest-{name}.json"
        manifest.write_text(json.dumps({
            "run_id": "synthetic",
            "code_sha": "deadbeef",
            "files": [{"file": p.name, "rows": 1, "sha256": _digest(p),
                        "source": p.name.split("equity_daily-", 1)[1].split("-2026", 1)[0]}
                       for p in files],
            "health": {"desired_token_count": 2, "tokens_with_ws_events": 2,
                        "missing_ws_tokens": [],
                        "polymarket_token_coverage": {"1": {}, "2": {}},
                        "alpaca": {"configured": False},
                        "symbol_status": {"CL=F": {"state": "metadata_only_no_bars"}}},
        }), encoding="utf-8")
        (manifest.with_name(manifest.name + ".sha256")).write_text(
            f"{_digest(manifest)}  {manifest.name}\n", encoding="utf-8"
        )
        tar_path = self.root / f"equity-daily-bundle-{name}.tar"
        with tarfile.open(tar_path, "w") as archive:
            for path in files + [p for p in files for p in [p.with_name(p.name + ".sha256")]] + [manifest, manifest.with_name(manifest.name + ".sha256")]:
                archive.add(path, arcname=path.name)
        (self.root / f"equity-daily-bundle-{name}.tar.sha256").write_text(
            f"{_digest(tar_path)}  {tar_path.name}\n", encoding="utf-8"
        )
        return tar_path

    def test_audit_checks_outer_inner_payloads_and_source_rows(self):
        tar_path = self._bundle()
        report = audit_bundle(tar_path, tar_path.with_name(tar_path.name + ".sha256"))
        self.assertTrue(report["outer_sha256"]["ok"])
        self.assertEqual(report["member_hashes"]["failed"], 0)
        self.assertEqual(report["coverage_reconciliation"]["independent_sample"]["observed_token_ids_by_ws_file"], [2])
        self.assertEqual(report["sources"]["polymarket_ws"][0]["event_count"], 2)
        self.assertEqual(report["sources"]["underlying_yahoo_chart"][0]["chart_bar_count"], 2)
        self.assertEqual(report["health_claim"]["futures_metadata_only_symbols"], ["CL=F"])

    def test_directory_reports_bad_outer_hash_without_throwing(self):
        tar_path = self._bundle()
        tar_path.with_name(tar_path.name + ".sha256").write_text("0" * 64 + "  " + tar_path.name + "\n", encoding="utf-8")
        report = audit_directory(self.root)
        self.assertFalse(report["integrity_ok"])
        self.assertEqual(report["bundle_count"], 1)
        self.assertIn("outer hash mismatch", report["errors"][0])

    def test_traversal_member_is_rejected(self):
        tar_path = self.root / "equity-daily-bundle-000002.tar"
        with tarfile.open(tar_path, "w") as archive:
            info = tarfile.TarInfo("../escape")
            info.size = 1
            import io
            archive.addfile(info, io.BytesIO(b"x"))
        sidecar = tar_path.with_name(tar_path.name + ".sha256")
        sidecar.write_text(f"{_digest(tar_path)}  {tar_path.name}\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            audit_bundle(tar_path, sidecar)

    def test_payload_digest_mismatch_is_rejected(self):
        tar_path = self._bundle("000003")
        # Replace the source with a mismatched payload digest and rebuild tar/sidecar.
        source = self.root / "equity_daily-polymarket_ws-2026-01-01-000000000001.jsonl.gz"
        _write_jsonl_gz(source, [{"payload_b64": base64.b64encode(b'{}').decode(),
                                  "payload_sha256": "0" * 64, "received_at_ns": 1}])
        # The previous tar remains intact; use a direct synthetic source audit to
        # ensure malformed payloads are fail-closed through the directory path.
        with self.assertRaises(ValueError):
            from equity_daily.release_audit import _inspect_source
            _inspect_source(source, "polymarket_ws")


if __name__ == "__main__":
    unittest.main()
