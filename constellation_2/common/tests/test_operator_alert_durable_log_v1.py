from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

SOURCE_ROOT = Path("/home/node/constellation").resolve()
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common import operator_alert_v1 as operator_alert_module


REPO_ROOT = SOURCE_ROOT


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


class OperatorAlertDurableLogTest(unittest.TestCase):
    def _write_authoritative_state(
        self,
        truth_root: Path,
        *,
        day_utc: str,
        state: str,
        heartbeat_status: str,
        first_fail: str,
        orchestrator_started: bool = False,
    ) -> None:
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
                "orchestrator_started": orchestrator_started,
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

    def _write_canonical_state_machine(self, truth_root: Path, *, day_utc: str, blocker_code: str) -> Path:
        path = truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json"
        _write_json(
            path,
            {
                "schema_id": "trading_day_state_machine",
                "schema_version": "v1",
                "day_utc": day_utc,
                "evaluated_at_utc": f"{day_utc}T00:05:00Z",
                "final_start_decision": "BLOCKED_BY_DEFECT",
                "first_true_blocker": {
                    "first_true_blocker_code": blocker_code,
                    "first_true_blocker_artifact_path": "/tmp/supporting-chain.json",
                    "blocker_classification": "REGENERATION_DEFECT",
                },
                "supporting_daily_control_refs": {
                    "trading_day_execution_control_plane_path": "/tmp/trading_day_execution_control_plane.v1.json"
                },
                "state_transitions": [
                    {
                        "from_state": "INTENTS_READY",
                        "to_state": "SUPPORTING_REGEN_BLOCKED",
                        "transition_at_utc": f"{day_utc}T00:05:00Z",
                        "transition_reason_code": blocker_code,
                        "evidence_ref": "/tmp/trading_day_execution_control_plane.v1.json",
                    }
                ],
            },
        )
        return path

    def test_emitted_alert_appends_log_and_updates_pointer_and_rollup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
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
            self.assertEqual(row["source_authority"], "legacy_day_start_bridge_v1")
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

    def test_dedup_suppressed_attempt_does_not_append_duplicate_log_row(self) -> None:
        with tempfile.TemporaryDirectory() as td:
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
                first = operator_alert_module.emit_operator_alert_for_day(day_utc="2026-03-14", truth_root=truth_root)
                result = operator_alert_module.emit_operator_alert_for_day(day_utc="2026-03-14", truth_root=truth_root)
            self.assertTrue(first["emitted"])
            self.assertFalse(result["emitted"])
            self.assertTrue(result["dedup_suppressed"])
            self.assertEqual(run_mock.call_count, 1)
            log_path = truth_root / "reports" / "operator_alert_log_v1" / "2026-03-14" / "operator_alert_log.v1.jsonl"
            rows = log_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(rows), 1)

    def test_failed_delivery_still_appends_log_row(self) -> None:
        with tempfile.TemporaryDirectory() as td:
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

    def test_state_machine_fallback_is_labeled_and_does_not_degrade_to_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            truth_root = Path(td).resolve()
            self._write_canonical_state_machine(
                truth_root,
                day_utc="2026-04-13",
                blocker_code="CONSISTENCY_GATE_FAILURE",
            )
            decision = operator_alert_module.build_operator_alert_decision(day_utc="2026-04-13", truth_root=truth_root)
            self.assertEqual(decision["state"], "BLOCKED")
            self.assertTrue(decision["notify_email"])
            self.assertEqual(decision["authority_source"], "trading_day_state_machine_v1_projection")
            self.assertEqual(decision["first_failure"], "CONSISTENCY_GATE_FAILURE")

    def test_state_machine_ready_projection_stays_non_alertable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            truth_root = Path(td).resolve()
            _write_json(
                truth_root / "reports" / "trading_day_state_machine_v1" / "2026-04-14" / "trading_day_state_machine.v1.json",
                {
                    "schema_id": "trading_day_state_machine",
                    "schema_version": "v1",
                    "day_utc": "2026-04-14",
                    "evaluated_at_utc": "2026-04-14T00:05:00Z",
                    "final_start_decision": "READY_NOW",
                    "first_true_blocker": {
                        "first_true_blocker_code": "",
                        "first_true_blocker_artifact_path": "",
                        "blocker_classification": "UNKNOWN",
                    },
                    "supporting_daily_control_refs": {},
                    "state_transitions": [
                        {
                            "from_state": "SESSION_AUTHORITY_GRANTED",
                            "to_state": "DAY_OPEN_ALLOWED",
                            "transition_at_utc": "2026-04-14T00:05:00Z",
                            "transition_reason_code": "STATE_MACHINE_READY_NOW",
                            "evidence_ref": "/tmp/state-machine.json",
                        }
                    ],
                },
            )
            decision = operator_alert_module.build_operator_alert_decision(
                day_utc="2026-04-14",
                truth_root=truth_root,
                now=datetime(2026, 4, 14, 10, 0, tzinfo=ZoneInfo("America/New_York")),
            )
            self.assertEqual(decision["state"], "BOD_DUE_NOT_STARTED")
            self.assertEqual(decision["phase"], "ACTIVE_SESSION")
            self.assertTrue(decision["notify_email"])
            self.assertEqual(decision["authority_source"], "trading_day_state_machine_v1_projection")

    def test_pre_open_semantics_override_failure_like_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            truth_root = Path(td).resolve()
            self._write_authoritative_state(
                truth_root,
                day_utc="2026-04-14",
                state="PENDING",
                heartbeat_status="PENDING",
                first_fail="",
            )
            self._write_authoritative_blocked_day(truth_root, day_utc="2026-04-14")
            decision = operator_alert_module.build_operator_alert_decision(
                day_utc="2026-04-14",
                truth_root=truth_root,
                now=datetime(2026, 4, 14, 8, 55, tzinfo=ZoneInfo("America/New_York")),
            )
            self.assertEqual(decision["phase"], "PRE_OPEN")
            self.assertEqual(decision["state"], "PRE_OPEN_WAITING")
            self.assertFalse(decision["notify_email"])
            self.assertFalse(decision["notify_desktop"])
            self.assertTrue(decision["notify_log"])
            self.assertEqual(decision["orchestrator_started"], "NOT_EXPECTED")
            self.assertEqual(decision["blocked_stage"], "WAITING_FOR_BOD")
            self.assertEqual(decision["reason"], "PRE_OPEN_WAITING_FOR_BOD")
            self.assertEqual(decision["summary"], "Constellation: INFO: PRE_OPEN_WAITING (2026-04-14)")
            self.assertIn("Waiting for BOD (09:31 ET)", decision["body"])
            self.assertIn("Orchestrator: NOT_EXPECTED", decision["body"])

    def test_post_bod_missing_start_is_starting_within_grace(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            truth_root = Path(td).resolve()
            decision = operator_alert_module.build_operator_alert_decision(
                day_utc="2026-04-14",
                truth_root=truth_root,
                now=datetime(2026, 4, 14, 9, 32, tzinfo=ZoneInfo("America/New_York")),
            )
            self.assertEqual(decision["phase"], "BOD_WINDOW")
            self.assertEqual(decision["state"], "STARTING")
            self.assertFalse(decision["notify_email"])
            self.assertTrue(decision["notify_log"])
            self.assertEqual(decision["blocked_stage"], "WAITING_FOR_STARTUP_EVIDENCE")
            self.assertIn("grace window", decision["operator_message"])

    def test_post_grace_missing_start_is_bod_due_not_started(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            truth_root = Path(td).resolve()
            decision = operator_alert_module.build_operator_alert_decision(
                day_utc="2026-04-14",
                truth_root=truth_root,
                now=datetime(2026, 4, 14, 9, 37, tzinfo=ZoneInfo("America/New_York")),
            )
            self.assertEqual(decision["phase"], "ACTIVE_SESSION")
            self.assertEqual(decision["state"], "BOD_DUE_NOT_STARTED")
            self.assertTrue(decision["notify_email"])
            self.assertTrue(decision["notify_desktop"])
            self.assertEqual(decision["reason"], "ORCHESTRATOR_NOT_STARTED_AFTER_BOD_GRACE")
            self.assertIn("grace window", decision["operator_message"])

    def test_post_start_healthy_state_is_started(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            truth_root = Path(td).resolve()
            self._write_authoritative_state(
                truth_root,
                day_utc="2026-04-14",
                state="PASS",
                heartbeat_status="PASS",
                first_fail="",
                orchestrator_started=True,
            )
            decision = operator_alert_module.build_operator_alert_decision(
                day_utc="2026-04-14",
                truth_root=truth_root,
                now=datetime(2026, 4, 14, 9, 40, tzinfo=ZoneInfo("America/New_York")),
            )
            self.assertEqual(decision["state"], "STARTED_HEALTHY")
            self.assertFalse(decision["notify_email"])
            self.assertTrue(decision["notify_log"])
            self.assertEqual(decision["summary"], "Constellation: INFO: STARTED_HEALTHY (2026-04-14)")
            self.assertIn("system is healthy", decision["operator_message"])

    def test_started_degraded_notifies_email(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            truth_root = Path(td).resolve()
            self._write_authoritative_state(
                truth_root,
                day_utc="2026-04-14",
                state="PASS",
                heartbeat_status="FAIL",
                first_fail="ENGINE_HEARTBEAT_FAIL",
                orchestrator_started=True,
            )
            decision = operator_alert_module.build_operator_alert_decision(
                day_utc="2026-04-14",
                truth_root=truth_root,
                now=datetime(2026, 4, 14, 9, 40, tzinfo=ZoneInfo("America/New_York")),
            )
            self.assertEqual(decision["state"], "STARTED_DEGRADED")
            self.assertTrue(decision["notify_email"])
            self.assertEqual(decision["severity"], "ERROR")

    def test_true_blocked_condition_notifies_email(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            truth_root = Path(td).resolve()
            self._write_authoritative_state(
                truth_root,
                day_utc="2026-04-14",
                state="BLOCKED",
                heartbeat_status="PASS",
                first_fail="PREOPEN_PREFLIGHT_FAILED",
            )
            self._write_authoritative_blocked_day(truth_root, day_utc="2026-04-14")
            decision = operator_alert_module.build_operator_alert_decision(
                day_utc="2026-04-14",
                truth_root=truth_root,
                now=datetime(2026, 4, 14, 8, 55, tzinfo=ZoneInfo("America/New_York")),
            )
            self.assertEqual(decision["state"], "BLOCKED")
            self.assertTrue(decision["notify_email"])

    def test_true_failed_condition_notifies_email(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            truth_root = Path(td).resolve()
            self._write_authoritative_state(
                truth_root,
                day_utc="2026-04-14",
                state="FAILED",
                heartbeat_status="FAIL",
                first_fail="PREOPEN_PREFLIGHT_FAILED",
            )
            decision = operator_alert_module.build_operator_alert_decision(
                day_utc="2026-04-14",
                truth_root=truth_root,
                now=datetime(2026, 4, 14, 8, 55, tzinfo=ZoneInfo("America/New_York")),
            )
            self.assertEqual(decision["state"], "FAILED")
            self.assertTrue(decision["notify_email"])
            self.assertEqual(decision["severity"], "CRITICAL")


if __name__ == "__main__":
    unittest.main()
