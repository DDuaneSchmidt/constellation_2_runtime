from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.operator_day_authority_summary_v1 import build_operator_day_authority_summary_payload


DAY = "2026-04-14"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _paper_policy(*, successful: bool = False) -> dict:
    return {
        "policy_mode": "PAPER_READY_WHEN_GRANTED_UNBOUNDED_SAME_DAY",
        "environment": "PAPER",
        "max_successful_opens_per_day": 0,
        "max_late_open_attempts_per_day": 0,
        "same_day_open_cap_enforced": False,
        "time_window_enforced": False,
        "successful_open_already_recorded": successful,
        "prior_success_ref": "/tmp/day_open_attempt.v1.json" if successful else "",
        "initial_open_consumed": successful,
        "late_open_available": False,
        "late_open_consumed": False,
        "late_open_consumed_ref": "",
        "open_terminal": False,
        "terminal_reason_code": "",
        "policy_status": "PAPER_REPEAT_OPEN_ALLOWED_WHEN_GRANTED" if successful else "PAPER_OPEN_AVAILABLE_WHEN_GRANTED",
        "attempt_count": 1 if successful else 0,
        "latest_attempt_kind": "INITIAL_BOD_TRIGGER" if successful else "NO_TRIGGER",
        "latest_attempt_result": "OPEN_SUCCEEDED" if successful else "",
        "prior_trigger_kind": "INITIAL_BOD_TRIGGER" if successful else "NO_TRIGGER",
        "prior_trigger_status": "EMITTED" if successful else "",
    }


def test_operator_day_authority_summary_reflects_canonical_blocked_state(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(truth_root / "active_session_v1" / "current.json", {"active_day": DAY, "rollover_status": "ROLLOVER_WITHHELD"})
    _write_json(truth_root / "session_authority_status_v1" / "current.json", {"status_severity": "CRITICAL"})
    _write_json(truth_root / "target_day_admission_v1" / f"{DAY}.json", {"admission_status": "BLOCKED"})
    _write_json(truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json", {"authority_status": "DENIED"})
    _write_json(
        truth_root / "reports" / "trading_day_state_machine_v1" / DAY / "trading_day_state_machine.v1.json",
        {
            "final_start_decision": "BLOCKED_VALID",
            "first_true_blocker": {
                "first_true_blocker_code": "FAIL:PAPER_POLICY_NOT_PASS",
                "first_true_blocker_artifact_path": "/tmp/paper_policy_verdict.v1.json",
            },
        },
    )

    payload = build_operator_day_authority_summary_payload(truth_root=truth_root, day_utc=DAY)

    assert payload["startup_open_status"] == "BLOCKED"
    assert payload["first_blocker_code"] == "FAIL:PAPER_POLICY_NOT_PASS"
    assert payload["trade_readiness_yes_no"] == "NO"
    assert payload["trade_readiness_canonical_blocker"] == "SESSION_NOT_AUTHORIZED"


def test_operator_day_authority_summary_says_paper_allowed_now_when_granted(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(truth_root / "active_session_v1" / "current.json", {"active_day": DAY, "rollover_status": "ACTIVE_SESSION_CONFIRMED"})
    _write_json(truth_root / "session_authority_status_v1" / "current.json", {"status_severity": "WARNING"})
    _write_json(truth_root / "target_day_admission_v1" / f"{DAY}.json", {"admission_status": "ADMIT"})
    _write_json(
        truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        {"control_state": {"authority_status": "GRANTED", "submission_authorized": True}, "submit_attempted": False},
    )
    _write_json(
        truth_root / "reports" / "trading_day_state_machine_v1" / DAY / "trading_day_state_machine.v1.json",
        {
            "final_start_decision": "READY_NOW",
            "open_lifecycle_state": "PAPER_OPEN_AVAILABLE",
            "open_policy": _paper_policy(),
            "first_true_blocker": {},
        },
    )

    payload = build_operator_day_authority_summary_payload(truth_root=truth_root, day_utc=DAY)

    assert payload["startup_open_status"] == "OPEN_SUBMIT_CAPABLE"
    assert "submission authority" in payload["operator_message"].lower()
    assert payload["trade_readiness_yes_no"] == "NO"


def test_operator_day_authority_summary_no_longer_forbids_repeated_paper_success(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(truth_root / "active_session_v1" / "current.json", {"active_day": DAY, "rollover_status": "ACTIVE_SESSION_CONFIRMED"})
    _write_json(truth_root / "session_authority_status_v1" / "current.json", {"status_severity": "WARNING"})
    _write_json(truth_root / "target_day_admission_v1" / f"{DAY}.json", {"admission_status": "ADMIT"})
    _write_json(
        truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        {"control_state": {"authority_status": "GRANTED"}, "submit_attempted": False},
    )
    _write_json(
        truth_root / "reports" / "trading_day_state_machine_v1" / DAY / "trading_day_state_machine.v1.json",
        {
            "final_start_decision": "READY_NOW",
            "open_lifecycle_state": "OPEN_SUCCEEDED",
            "open_policy": _paper_policy(successful=True),
            "first_true_blocker": {},
        },
    )

    payload = build_operator_day_authority_summary_payload(truth_root=truth_root, day_utc=DAY)

    assert payload["startup_open_status"] == "OPEN_NO_TRADES"
    assert "no submissions or fills" in payload["operator_message"].lower()
    assert "forbidden" not in payload["operator_message"].lower()


def test_operator_day_authority_summary_reports_open_submit_capable_for_open_success_with_submission_authorized(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(truth_root / "active_session_v1" / "current.json", {"active_day": DAY, "rollover_status": "ACTIVE_SESSION_CONFIRMED"})
    _write_json(truth_root / "session_authority_status_v1" / "current.json", {"status_severity": "WARNING"})
    _write_json(truth_root / "target_day_admission_v1" / f"{DAY}.json", {"admission_status": "ADMIT"})
    _write_json(
        truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        {"control_state": {"authority_status": "GRANTED", "submission_authorized": True}, "submit_attempted": False},
    )
    _write_json(
        truth_root / "reports" / "trading_day_state_machine_v1" / DAY / "trading_day_state_machine.v1.json",
        {
            "final_start_decision": "READY_NOW",
            "open_lifecycle_state": "OPEN_SUCCEEDED",
            "open_policy": _paper_policy(successful=True),
            "first_true_blocker": {},
        },
    )

    payload = build_operator_day_authority_summary_payload(truth_root=truth_root, day_utc=DAY)

    assert payload["startup_open_status"] == "OPEN_SUBMIT_CAPABLE"


def test_operator_day_authority_summary_projects_trade_readiness_decision(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(truth_root / "active_session_v1" / "current.json", {"active_day": DAY, "rollover_status": "ACTIVE_SESSION_CONFIRMED"})
    _write_json(truth_root / "session_authority_status_v1" / "current.json", {"status_severity": "WARNING"})
    _write_json(truth_root / "target_day_admission_v1" / f"{DAY}.json", {"admission_status": "ADMIT"})
    _write_json(
        truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        {"control_state": {"authority_status": "GRANTED", "submission_authorized": True}, "submit_attempted": False},
    )
    _write_json(
        truth_root / "reports" / "trading_day_state_machine_v1" / DAY / "trading_day_state_machine.v1.json",
        {
            "final_start_decision": "READY_NOW",
            "open_lifecycle_state": "OPEN_SUCCEEDED",
            "open_policy": _paper_policy(successful=True),
            "first_true_blocker": {},
        },
    )
    _write_json(
        truth_root / "reports" / "trade_readiness_decision_v1" / DAY / "trade_readiness_decision.v1.json",
        {
            "schema_version": "v1",
            "environment": "PAPER",
            "day_utc": DAY,
            "intent_hash": "intent-hash",
            "decision": "YES",
            "submit_allowed": True,
            "canonical_gate": "NONE",
            "canonical_blocker": None,
            "canonical_reason": "All pre-submit canonical gates pass.",
            "ordered_gate_results": [],
            "all_blockers": [],
            "evidence_artifacts": [],
            "evidence_hashes": {},
            "decision_timestamp_utc": f"{DAY}T12:00:00Z",
            "decision_writer": "test",
            "truth_root": str(truth_root),
            "policy_version": "test",
            "blocker_priority_order": [],
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
        },
    )

    payload = build_operator_day_authority_summary_payload(truth_root=truth_root, day_utc=DAY)

    assert payload["trade_readiness_yes_no"] == "YES"
    assert payload["trade_readiness_canonical_gate"] == "NONE"
    assert payload["trade_readiness_canonical_blocker"] == ""
    assert payload["input_refs"]["trade_readiness_decision_v1"].endswith("trade_readiness_decision.v1.json")
