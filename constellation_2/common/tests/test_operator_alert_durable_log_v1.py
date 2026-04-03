from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from constellation_2.common import operator_alert_v1 as operator_alert_module


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


class OperatorAlertDurableLogTest(unittest.TestCase):
    def _write_authoritative_state(self, truth_root: Path, *, day_utc: str, state: str, heartbeat_status: str, first_fail: str) -> None:
        _write_json(
            truth_root / "reports" / "trading_day_state_v1" / day_utc / "trading_day_state.v1.json",
            {
                "schema_id": "trading_day_state",
                "schema_version": "v1",
                "day_utc": day_utc,
                "state": state,
                "heartbeat_status": heartbeat_status,
                "first_failing_prerequisite": first_fail,
                "first_failing_path": "/tmp/first-failing-path",
                "produced_utc": f"{day_utc}T00:00:00Z",
                "producer": {"repo": "constellation_2_runtime", "module": "ops/run/c2_preopen_preflight_v1.sh"},
                "evidence_paths": ["/tmp/evidence-a", "/tmp/evidence-b"],
                "provenance": {"authoritative_write": True},
            },
        )

    def _write_authoritative_blocked_day(self, truth_root: Path, *, day_utc: str) -> None:
        _write_json(
            truth_root / "reports" / "day_start_blocked_v1" / day_utc / "day_start_blocked.v1.json",
            {
                "schema_id": "day_start_blocked",
                "schema_version": "v1",
                "day_utc": day_utc,
                "blocked_stage": "PREOPEN",
                "failing_service": "c2-preopen-preflight.service",
                "failing_script": "ops/run/c2_preopen_preflight_v1.sh",
                "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_day_start_blocked_v1.py"},
                "evidence_paths": ["/tmp/blocked-evidence"],
                "provenance": {"authoritative_write": True},
            },
        )

    def test_emitted_alert_appends_log_and_updates_pointer_and_rollup(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "constellation_2/runtime")) as td:
            truth_root = Path(td).resolve()
            receipts_root = truth_root / "local_state" / "operator_alert_receipts_v1"
            self._write_authoritative_state(truth_root, day_utc="2026-03-14", state="FAILED", heartbeat_status="FAIL", first_fail="PREOPEN_PREFLIGHT_FAILED")
            self._write_authoritative_blocked_day(truth_root, day_utc="2026-03-14")
            with patch.object(operator_alert_module, "RECEIPTS_ROOT", receipts_root), patch(
                "constellation_2.common.operator_alert_v1.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}" if name in {"notify-send", "logger"} else None,
            ), patch(
                "constellation_2.common.operator_alert_v1.subprocess.run",
                return_value=subprocess.CompletedProcess(args=["notify-send"], returncode=0),
            ) as run_mock, patch.dict(os.environ, {"INVOCATION_ID": "systemd123", "JOURNAL_STREAM": "8:8"}, clear=False):
                result = operator_alert_module.emit_operator_alert_for_day(day_utc="2026-03-14", truth_root=truth_root)
            self.assertTrue(result["emitted"])
            self.assertFalse(result["dedup_suppressed"])
            self.assertEqual(result["channel"], "notify-send")
            self.assertEqual(run_mock.call_count, 1)
            log_path = truth_root / "reports" / "operator_alert_log_v1" / "2026-03-14" / "operator_alert_log.v1.jsonl"
            pointer_path = truth_root / "reports" / "operator_alert_log_v1" / "latest_pointer.v1.json"
            rollup_path = truth_root / "reports" / "operator_alert_rollup_v1" / "2026-03-14" / "operator_alert_rollup.v1.json"
            self.assertTrue(log_path.exists())
            self.assertTrue(pointer_path.exists())
            self.assertTrue(rollup_path.exists())
            row = json.loads(log_path.read_text(encoding="utf-8").splitlines()[-1])
            self.assertEqual(row["schema_id"], "operator_alert_log")
            self.assertEqual(row["schema_version"], "v1")
            self.assertEqual(row["source_service"], "c2-preopen-preflight.service")
            self.assertEqual(row["source_script"], "ops/run/c2_preopen_preflight_v1.sh")
            self.assertIn("EMAIL_CHANNEL_NOT_CONFIGURED", row["reason"])
            self.assertIn("SMS_CHANNEL_NOT_CONFIGURED", row["reason"])
            self.assertEqual(row["channel"], "notify-send")
            self.assertTrue(row["emitted"])
            self.assertFalse(row["dedup_suppressed"])
            self.assertIn("/tmp/evidence-a", row["related_artifact_paths"])
            self.assertIn("/tmp/blocked-evidence", row["related_artifact_paths"])
            pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
            rollup = json.loads(rollup_path.read_text(encoding="utf-8"))
            self.assertEqual(pointer["log_path"], str(log_path))
            self.assertEqual(rollup["alert_count"], 1)
            self.assertEqual(rollup["emitted_count"], 1)

    def test_dedup_suppressed_attempt_still_appends_log_row(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "constellation_2/runtime")) as td:
            truth_root = Path(td).resolve()
            receipts_root = truth_root / "local_state" / "operator_alert_receipts_v1"
            self._write_authoritative_state(truth_root, day_utc="2026-03-14", state="FAILED", heartbeat_status="FAIL", first_fail="PREOPEN_PREFLIGHT_FAILED")
            self._write_authoritative_blocked_day(truth_root, day_utc="2026-03-14")
            receipts_root.mkdir(parents=True, exist_ok=True)
            (receipts_root / "2026-03-14.json").write_text(
                json.dumps({"alert_key": operator_alert_module.build_operator_alert_decision(day_utc="2026-03-14", truth_root=truth_root)["alert_key"], "channel": "notify-send"}),
                encoding="utf-8",
            )
            with patch.object(operator_alert_module, "RECEIPTS_ROOT", receipts_root), patch(
                "constellation_2.common.operator_alert_v1.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}" if name in {"notify-send", "logger"} else None,
            ), patch(
                "constellation_2.common.operator_alert_v1.subprocess.run",
                return_value=subprocess.CompletedProcess(args=["notify-send"], returncode=0),
            ) as run_mock, patch.dict(os.environ, {"INVOCATION_ID": "systemd123", "JOURNAL_STREAM": "8:8"}, clear=False):
                result = operator_alert_module.emit_operator_alert_for_day(day_utc="2026-03-14", truth_root=truth_root)
            self.assertFalse(result["emitted"])
            self.assertTrue(result["dedup_suppressed"])
            self.assertEqual(run_mock.call_count, 0)
            log_path = truth_root / "reports" / "operator_alert_log_v1" / "2026-03-14" / "operator_alert_log.v1.jsonl"
            row = json.loads(log_path.read_text(encoding="utf-8").splitlines()[-1])
            self.assertEqual(row["reason"].split(";")[0], "DEDUP_SUPPRESSED")
            self.assertTrue(row["dedup_suppressed"])
            self.assertFalse(row["emitted"])

    def test_failed_delivery_still_appends_log_row(self) -> None:
        with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "constellation_2/runtime")) as td:
            truth_root = Path(td).resolve()
            receipts_root = truth_root / "local_state" / "operator_alert_receipts_v1"
            self._write_authoritative_state(truth_root, day_utc="2026-03-14", state="FAILED", heartbeat_status="FAIL", first_fail="PREOPEN_PREFLIGHT_FAILED")
            self._write_authoritative_blocked_day(truth_root, day_utc="2026-03-14")
            with patch.object(operator_alert_module, "RECEIPTS_ROOT", receipts_root), patch(
                "constellation_2.common.operator_alert_v1.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}" if name in {"notify-send", "logger"} else None,
            ), patch(
                "constellation_2.common.operator_alert_v1.subprocess.run",
                return_value=subprocess.CompletedProcess(args=["notify-send"], returncode=1),
            ), patch.dict(os.environ, {"INVOCATION_ID": "systemd123", "JOURNAL_STREAM": "8:8"}, clear=False):
                result = operator_alert_module.emit_operator_alert_for_day(day_utc="2026-03-14", truth_root=truth_root)
            self.assertFalse(result["emitted"])
            self.assertEqual(result["reason"], "ALERT_EMISSION_FAILED")
            log_path = truth_root / "reports" / "operator_alert_log_v1" / "2026-03-14" / "operator_alert_log.v1.jsonl"
            row = json.loads(log_path.read_text(encoding="utf-8").splitlines()[-1])
            self.assertEqual(row["channel"], "none")
            self.assertFalse(row["emitted"])
            self.assertIn("ALERT_EMISSION_FAILED", row["reason"])


if __name__ == "__main__":
    unittest.main()
