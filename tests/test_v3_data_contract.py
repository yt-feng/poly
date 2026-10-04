import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "research" / "data_capture" / "schema"))
from v3_contract import alignment_report, load_jsonl, validate_record, validate_records


FIXTURES = Path(__file__).parent / "fixtures"


class V3DataContractTests(unittest.TestCase):
    def test_valid_snapshot_requires_and_accepts_execution_metadata(self):
        record = load_jsonl(FIXTURES / "v3_observation_valid.jsonl")[0]
        ok, reasons = validate_record(record)
        self.assertTrue(ok, reasons)

    def test_missing_rules_and_future_label_are_quarantined(self):
        report = validate_records(load_jsonl(FIXTURES / "v3_observation_invalid.jsonl"))
        self.assertEqual(report["accepted_records"], 0)
        self.assertEqual(report["quarantined_records"], 2)
        self.assertFalse(report["leakage_check"]["passed"])

    def test_alignment_reports_latency_and_rejects_future_event(self):
        record = load_jsonl(FIXTURES / "v3_observation_valid.jsonl")[0]
        report = alignment_report([record, {"source_event_time_ms": 2000, "received_time_ms": 1000}])
        self.assertEqual(report["future_event_records"], 1)
        bad = dict(record, source_event_time_ms=record["received_time_ms"] + 3000)
        ok, reasons = validate_record(bad)
        self.assertFalse(ok)
        self.assertIn("source_event_after_receive", reasons)

    def test_duplicate_and_conflicting_ids_are_quarantined(self):
        record = load_jsonl(FIXTURES / "v3_observation_valid.jsonl")[0]
        duplicate = json.loads(json.dumps(record))
        conflict = json.loads(json.dumps(record))
        conflict["received_time_ms"] += 1
        report = validate_records([record, duplicate, conflict])
        self.assertEqual(report["accepted_records"], 1)
        self.assertEqual(report["reason_counts"]["duplicate_observation_id"], 1)
        self.assertEqual(report["reason_counts"]["conflicting_duplicate_observation_id"], 1)


if __name__ == "__main__":
    unittest.main()
