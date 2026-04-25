from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.next_day_readiness_consistency_gate_v1 import (
    CONSISTENCY_GATE_STATUS_FAIL,
    CONSISTENCY_GATE_STATUS_PASS,
    evaluate_next_day_readiness_consistency_gate_v1,
)


DAY = "2026-04-22"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _kill_switch_payload() -> dict:
    return {
        "schema_id": "global_kill_switch_state",
        "schema_version": "v1",
        "day_utc": DAY,
        "produced_utc": f"{DAY}T00:00:00Z",
        "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
        "state": "INACTIVE",
        "allow_entries": True,
        "allow_exits": True,
        "reason_codes": [],
        "input_manifest": [],
        "state_sha256": "b" * 64,
    }


def _build_payload_blocked() -> dict:
    return {
        "build_status": "BLOCKED",
        "closure_status": "OPEN",
        "hidden_dependency_check_result": {"status": "FAIL"},
    }


def _admission_payload_blocked() -> dict:
    return {"admission_status": "BLOCKED"}


def _active_payload_blocked(*, truth_root: Path) -> dict:
    return {
        "target_day": DAY,
        "target_day_admission_status": "BLOCKED",
        "target_day_build_ref": {
            "artifact_path": str((truth_root / "target_day_build_v1" / f"{DAY}.json").resolve()),
            "artifact_sha256": "",
        },
        "target_day_admission_ref": str((truth_root / "target_day_admission_v1" / f"{DAY}.json").resolve()),
    }


def _status_payload_blocked() -> dict:
    return {"target_day": DAY, "submission_authorized": False, "generated_utc": f"{DAY}T00:00:04Z"}


def _status_payload_authorized() -> dict:
    return {"target_day": DAY, "submission_authorized": True, "generated_utc": f"{DAY}T00:00:04Z"}


def _ledger_payload_submission_blocked() -> dict:
    return {
        "authority_status": "GRANTED",
        "submission_authorized": False,
        "control_state": {
            "authority_status": "GRANTED",
            "submission_authorized": False,
        },
        "evaluated_at_utc": f"{DAY}T00:00:02Z",
    }


def _control_payload_ready_now() -> dict:
    return {
        "final_start_decision": "READY_NOW",
        "authority_result": {"ledger_authority_status": "GRANTED"},
        "evaluated_at_utc": f"{DAY}T00:00:03Z",
    }


def test_consistency_gate_allows_ready_now_when_submit_is_blocked_only_by_options_snapshot_pending(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        _kill_switch_payload(),
    )

    result = evaluate_next_day_readiness_consistency_gate_v1(
        truth_root=truth_root,
        day_utc=DAY,
        build_payload=_build_payload_blocked(),
        admission_payload=_admission_payload_blocked(),
        active_session_payload=_active_payload_blocked(truth_root=truth_root),
        session_authority_status_payload=_status_payload_blocked(),
        submit_boundary_payload={
            "boundary_status": "BLOCKED",
            "submission_authorized": False,
            "blocking_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"],
            "produced_at_utc": f"{DAY}T00:00:02Z",
        },
        paper_session_ledger_payload=_ledger_payload_submission_blocked(),
        paper_day_control_plane_payload=_control_payload_ready_now(),
    )

    assert result.status == CONSISTENCY_GATE_STATUS_PASS
    assert result.blocking_reason_codes == ()


def test_consistency_gate_still_fails_when_ready_now_submit_blocker_is_not_options_snapshot_pending(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        _kill_switch_payload(),
    )

    result = evaluate_next_day_readiness_consistency_gate_v1(
        truth_root=truth_root,
        day_utc=DAY,
        build_payload=_build_payload_blocked(),
        admission_payload=_admission_payload_blocked(),
        active_session_payload=_active_payload_blocked(truth_root=truth_root),
        session_authority_status_payload=_status_payload_blocked(),
        submit_boundary_payload={
            "boundary_status": "BLOCKED",
            "submission_authorized": False,
            "blocking_codes": ["SUBMIT_BOUNDARY_READINESS_NOT_OK"],
            "produced_at_utc": f"{DAY}T00:00:02Z",
        },
        paper_session_ledger_payload=_ledger_payload_submission_blocked(),
        paper_day_control_plane_payload=_control_payload_ready_now(),
    )

    assert result.status == CONSISTENCY_GATE_STATUS_FAIL


def test_consistency_gate_allows_ready_now_when_legacy_status_still_reports_submission_authorized(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        _kill_switch_payload(),
    )

    result = evaluate_next_day_readiness_consistency_gate_v1(
        truth_root=truth_root,
        day_utc=DAY,
        build_payload=_build_payload_blocked(),
        admission_payload=_admission_payload_blocked(),
        active_session_payload=_active_payload_blocked(truth_root=truth_root),
        session_authority_status_payload=_status_payload_authorized(),
        submit_boundary_payload={
            "boundary_status": "BLOCKED",
            "submission_authorized": False,
            "blocking_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"],
            "produced_at_utc": f"{DAY}T00:00:02Z",
        },
        paper_session_ledger_payload=_ledger_payload_submission_blocked(),
        paper_day_control_plane_payload=_control_payload_ready_now(),
    )

    assert result.status == CONSISTENCY_GATE_STATUS_PASS
    assert result.blocking_reason_codes == ()


def test_consistency_gate_still_fails_when_legacy_status_authorized_but_submit_blocker_not_phasec_veto(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        _kill_switch_payload(),
    )

    result = evaluate_next_day_readiness_consistency_gate_v1(
        truth_root=truth_root,
        day_utc=DAY,
        build_payload=_build_payload_blocked(),
        admission_payload=_admission_payload_blocked(),
        active_session_payload=_active_payload_blocked(truth_root=truth_root),
        session_authority_status_payload=_status_payload_authorized(),
        submit_boundary_payload={
            "boundary_status": "BLOCKED",
            "submission_authorized": False,
            "blocking_codes": ["SUBMIT_BOUNDARY_READINESS_NOT_OK"],
            "produced_at_utc": f"{DAY}T00:00:02Z",
        },
        paper_session_ledger_payload=_ledger_payload_submission_blocked(),
        paper_day_control_plane_payload=_control_payload_ready_now(),
    )

    assert result.status == CONSISTENCY_GATE_STATUS_FAIL
