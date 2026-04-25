from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.day_failure_causality_v1 import build_day_failure_causality_payload


DAY = "2026-04-14"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_day_failure_causality_captures_phasec_root_failure_chain(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(truth_root / "active_session_v1" / "current.json", {"active_day": DAY, "rollover_status": "ROLLOVER_WITHHELD"})
    _write_json(
        truth_root / "session_authority_status_v1" / "current.json",
        {"status_severity": "CRITICAL", "rollover_status": "ROLLOVER_WITHHELD", "first_real_blocker_code": "PARTIAL_BUILD"},
    )
    _write_json(truth_root / "reports" / "bod_execution_environment_proof_v1" / DAY / "bod_execution_environment_proof.v1.json", {"status": "PASS", "blocking_codes": []})
    _write_json(
        truth_root / "reports" / "phasec_risk_inputs_prep_v1" / DAY / "phasec_risk_inputs_prep.v1.json",
        {
            "status": "BLOCKED_BY_DEFECT",
            "blocking_codes": ["PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO"],
            "bridge_result": {"returncode": 1, "stderr": "ModuleNotFoundError: No module named 'constellation_2'"},
        },
    )
    _write_json(
        truth_root / "reports" / "startup_materialization_v1" / DAY / "startup_materialization.v1.json",
        {
            "status": "FAIL",
            "blocking_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO"],
            "phasec_materializer_result": {"returncode": -1, "stderr": "SKIPPED_PRESTART_PREP_BLOCKED"},
        },
    )
    _write_json(
        truth_root / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json",
        {"overall_status": "FAIL", "capabilities": [{"capability_id": "startup_materialization_ready", "status": "FAIL", "reason_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO"]}]},
    )
    _write_json(
        truth_root / "reports" / "paper_policy_verdict_v1" / DAY / "paper_policy_verdict.v1.json",
        {"overall_status": "FAIL", "blocking_items": [{"capability_id": "startup_materialization_ready", "reason_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO"]}]},
    )
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"boundary_status": "BLOCKED", "submission_authorized": False, "blocking_codes": ["FAIL:PAPER_POLICY_NOT_PASS"]},
    )
    _write_json(truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json", {"authority_status": "DENIED", "submit_attempted": False, "reason_codes": ["PAPER_SESSION_LEDGER_AUTHORITY_DENIED"]})
    _write_json(truth_root / "target_day_build_v1" / f"{DAY}.json", {"build_status": "BLOCKED", "closure_status": "OPEN", "blocker_chain": [{"blocker_code": "PARTIAL_BUILD"}], "hidden_dependency_check_result": {"blocking_reason_code": "PARTIAL_BUILD"}})
    _write_json(truth_root / "target_day_admission_v1" / f"{DAY}.json", {"admission_status": "BLOCKED", "blocking_reason_codes": ["PARTIAL_BUILD", "REQUIRED_GATE_FAIL"]})
    _write_json(
        truth_root / "reports" / "trading_day_state_machine_v1" / DAY / "trading_day_state_machine.v1.json",
        {"final_start_decision": "BLOCKED_VALID", "first_true_blocker": {"first_true_blocker_code": "FAIL:PAPER_POLICY_NOT_PASS", "first_true_blocker_artifact_path": str(truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json")}},
    )

    payload = build_day_failure_causality_payload(truth_root=truth_root, day_utc=DAY)

    assert payload["status"] == "FAILURE_DETECTED"
    assert payload["first_failing_artifact_id"] == "phasec_risk_inputs_prep_v1"
    assert payload["first_failing_code"] == "PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO"
    assert "ModuleNotFoundError" in payload["direct_stderr"]


def test_day_failure_causality_reports_no_failure_for_paper_open_available(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(truth_root / "active_session_v1" / "current.json", {"active_day": DAY, "rollover_status": "ACTIVE_SESSION_CONFIRMED"})
    _write_json(truth_root / "session_authority_status_v1" / "current.json", {"status_severity": "WARNING", "rollover_status": "ACTIVE_SESSION_CONFIRMED"})
    _write_json(truth_root / "target_day_admission_v1" / f"{DAY}.json", {"admission_status": "ADMIT", "blocking_reason_codes": []})
    _write_json(truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json", {"authority_status": "GRANTED", "submit_attempted": False})
    _write_json(
        truth_root / "reports" / "trading_day_state_machine_v1" / DAY / "trading_day_state_machine.v1.json",
        {
            "final_start_decision": "READY_NOW",
            "open_lifecycle_state": "PAPER_OPEN_AVAILABLE",
            "open_policy": {"policy_mode": "PAPER_READY_WHEN_GRANTED_UNBOUNDED_SAME_DAY", "environment": "PAPER", "max_successful_opens_per_day": 0, "max_late_open_attempts_per_day": 0, "same_day_open_cap_enforced": False, "time_window_enforced": False, "successful_open_already_recorded": True, "prior_success_ref": "/tmp/day_open_attempt.v1.json", "initial_open_consumed": True, "late_open_available": False, "late_open_consumed": False, "late_open_consumed_ref": "", "open_terminal": False, "terminal_reason_code": "", "policy_status": "PAPER_REPEAT_OPEN_ALLOWED_WHEN_GRANTED", "attempt_count": 1, "latest_attempt_kind": "INITIAL_BOD_TRIGGER", "latest_attempt_result": "OPEN_SUCCEEDED", "prior_trigger_kind": "INITIAL_BOD_TRIGGER", "prior_trigger_status": "EMITTED"},
            "first_true_blocker": {},
        },
    )

    payload = build_day_failure_causality_payload(truth_root=truth_root, day_utc=DAY)

    assert payload["status"] == "NO_FAILURE_DETECTED"
    assert payload["first_failing_artifact_id"] == ""


def test_day_failure_causality_live_window_expiry_still_registers_failure(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(truth_root / "active_session_v1" / "current.json", {"active_day": DAY, "rollover_status": "ACTIVE_SESSION_CONFIRMED"})
    _write_json(truth_root / "session_authority_status_v1" / "current.json", {"status_severity": "WARNING", "rollover_status": "ACTIVE_SESSION_CONFIRMED"})
    _write_json(truth_root / "target_day_admission_v1" / f"{DAY}.json", {"admission_status": "ADMIT", "blocking_reason_codes": []})
    _write_json(truth_root / "target_day_build_v1" / f"{DAY}.json", {"build_status": "COMPLETE", "closure_status": "CLOSED", "blocker_chain": []})
    _write_json(truth_root / "reports" / "bod_execution_environment_proof_v1" / DAY / "bod_execution_environment_proof.v1.json", {"status": "PASS", "blocking_codes": []})
    _write_json(truth_root / "reports" / "phasec_risk_inputs_prep_v1" / DAY / "phasec_risk_inputs_prep.v1.json", {"status": "PASS", "blocking_codes": [], "bridge_result": {"returncode": 0, "stderr": ""}})
    _write_json(truth_root / "reports" / "startup_materialization_v1" / DAY / "startup_materialization.v1.json", {"status": "SUCCESS", "blocking_codes": [], "phasec_materializer_result": {"returncode": 0, "stderr": ""}})
    _write_json(truth_root / "reports" / "capability_state_v1" / DAY / "capability_state.v1.json", {"overall_status": "PASS", "capabilities": []})
    _write_json(truth_root / "reports" / "paper_policy_verdict_v1" / DAY / "paper_policy_verdict.v1.json", {"overall_status": "PASS", "blocking_items": []})
    _write_json(truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json", {"boundary_status": "AUTHORIZED", "submission_authorized": True, "blocking_codes": []})
    _write_json(truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json", {"control_state": {"authority_status": "GRANTED"}, "submit_attempted": False})
    _write_json(truth_root / "reports" / "day_open_trigger_v1" / DAY / "day_open_trigger.v1.json", {"trigger_status": "SUPPRESSED_OUTSIDE_OPEN_WINDOW", "trigger_reason_code": "OPEN_TRIGGER_OPEN_WINDOW_EXPIRED", "consumed": False, "dedupe_key": f"{DAY}:INITIAL_BOD_TRIGGER:1"})
    _write_json(truth_root / "reports" / "day_open_attempt_v1" / DAY / "day_open_attempt.v1.json", {"final_classification": "OPEN_MISSED", "reason_codes": ["OPEN_WINDOW_EXPIRED"], "result_code": "OPEN_WINDOW_EXPIRED"})
    _write_json(
        truth_root / "reports" / "trading_day_state_machine_v1" / DAY / "trading_day_state_machine.v1.json",
        {"final_start_decision": "READY_NOW", "open_lifecycle_state": "OPEN_MISSED", "first_true_blocker": {"first_true_blocker_code": "OPEN_WINDOW_EXPIRED", "first_true_blocker_artifact_path": str(truth_root / "reports" / "day_open_attempt_v1" / DAY / "day_open_attempt.v1.json")}},
    )

    payload = build_day_failure_causality_payload(truth_root=truth_root, day_utc=DAY)

    assert payload["status"] == "FAILURE_DETECTED"
    assert payload["first_failing_artifact_id"] == "day_open_trigger_v1"
    assert payload["first_failing_code"] == "OPEN_TRIGGER_OPEN_WINDOW_EXPIRED"
