from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.aegis_day_closure_authority_v1 import evaluate_aegis_day_closure_authority_v1


DAY = "2026-04-24"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_base(truth_root: Path, execution_root: Path) -> Path:
    _write_json(
        truth_root / "reports" / "trading_day_state_machine_v1" / DAY / "trading_day_state_machine.v1.json",
        {"final_start_decision": "READY_NOW"},
    )
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"boundary_status": "AUTHORIZED"},
    )
    _write_json(
        truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        {
            "post_submit_lifecycle": {"lineage_status": "BOUND"},
            "submit_lifecycle": {"submit_result_status": "PASS"},
        },
    )
    _write_json(
        execution_root / "execution_evidence_v1" / "latest_pointer.v1.json",
        {
            "day_utc": DAY,
            "status": "OK",
            "pointers": {
                "submissions_day_dir": str((execution_root / "execution_evidence_v1" / "submissions" / DAY).resolve())
            },
        },
    )
    _write_json(
        execution_root / "submission_index_v1" / DAY / "submission_index.v1.json",
        {"status": "PASS"},
    )
    _write_json(
        execution_root / "execution_evidence_v1" / "current_head" / DAY / "current_head.v1.json",
        {"status": "PASS"},
    )
    readiness_path = execution_root / "trade_submit_readiness_c2_v1" / "PAPER" / "DUO847203" / "status.json"
    _write_json(readiness_path, {"state": "OK", "reasons": []})
    return readiness_path


def test_authorized_and_state_machine_blocked_by_defect_fails(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "exec"
    readiness_path = _seed_base(truth_root, execution_root)
    _write_json(
        truth_root / "reports" / "trading_day_state_machine_v1" / DAY / "trading_day_state_machine.v1.json",
        {"final_start_decision": "BLOCKED_BY_DEFECT"},
    )
    payload = evaluate_aegis_day_closure_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        trade_submit_readiness_path=readiness_path,
    )
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES" for item in payload["blocking_evidence"])


def test_submit_boundary_and_state_machine_blocked_are_coherent_not_contradictory(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "exec"
    readiness_path = _seed_base(truth_root, execution_root)
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {
            "boundary_status": "BLOCKED",
            "first_blocker_code": "SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS",
            "blocking_codes": ["SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS"],
        },
    )
    _write_json(
        truth_root / "reports" / "trading_day_state_machine_v1" / DAY / "trading_day_state_machine.v1.json",
        {"final_start_decision": "BLOCKED_BY_DEFECT", "first_true_blocker": {"first_true_blocker_code": "CONSISTENCY_GATE_FAILURE"}},
    )
    payload = evaluate_aegis_day_closure_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        trade_submit_readiness_path=readiness_path,
    )
    assert payload["status"] == "FAIL"
    assert payload["canonical_blocker"] == "SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS"
    assert not any(item["code"] == "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES" for item in payload["blocking_evidence"])


def test_execution_stream_failure_present_fails(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "exec"
    readiness_path = _seed_base(truth_root, execution_root)
    _write_json(execution_root / "execution_stream_v1" / "failures" / DAY / "failure.json", {"status": "FAIL"})
    payload = evaluate_aegis_day_closure_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        trade_submit_readiness_path=readiness_path,
    )
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "EXECUTION_STREAM_FAILURE_PRESENT" for item in payload["blocking_evidence"])


def test_lineage_gap_fails(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "exec"
    readiness_path = _seed_base(truth_root, execution_root)
    _write_json(
        truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        {
            "post_submit_lifecycle": {"lineage_status": "GAP"},
            "submit_lifecycle": {"submit_result_status": "PASS"},
        },
    )
    payload = evaluate_aegis_day_closure_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        trade_submit_readiness_path=readiness_path,
    )
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "POST_SUBMIT_LINEAGE_GAP" for item in payload["blocking_evidence"])


def test_latest_pointer_old_day_fails(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "exec"
    readiness_path = _seed_base(truth_root, execution_root)
    _write_json(
        execution_root / "execution_evidence_v1" / "latest_pointer.v1.json",
        {
            "day_utc": "2026-04-23",
            "status": "OK",
            "pointers": {"submissions_day_dir": str((execution_root / "execution_evidence_v1" / "submissions" / "2026-04-23").resolve())},
        },
    )
    payload = evaluate_aegis_day_closure_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        trade_submit_readiness_path=readiness_path,
    )
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "EXECUTION_POINTER_DAY_MISMATCH" for item in payload["blocking_evidence"])


def test_trade_readiness_ok_with_not_pass_reason_fails(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "exec"
    readiness_path = _seed_base(truth_root, execution_root)
    _write_json(readiness_path, {"state": "OK", "reasons": ["INFO:PRODUCTION_POLICY_NOT_PASS"]})
    payload = evaluate_aegis_day_closure_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        trade_submit_readiness_path=readiness_path,
    )
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "SUBMIT_READINESS_POLICY_INCOHERENT" for item in payload["blocking_evidence"])


def test_all_coherent_surfaces_pass(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "exec"
    readiness_path = _seed_base(truth_root, execution_root)
    payload = evaluate_aegis_day_closure_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        trade_submit_readiness_path=readiness_path,
    )
    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] is None


def test_missing_required_surface_fails(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "exec"
    readiness_path = _seed_base(truth_root, execution_root)
    (truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json").unlink()
    payload = evaluate_aegis_day_closure_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        trade_submit_readiness_path=readiness_path,
    )
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "REQUIRED_CONTROL_SURFACE_MISSING" for item in payload["blocking_evidence"])


def test_invalid_json_surface_fails(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "exec"
    readiness_path = _seed_base(truth_root, execution_root)
    invalid_path = truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json"
    invalid_path.write_text("{invalid", encoding="utf-8")
    payload = evaluate_aegis_day_closure_authority_v1(
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        trade_submit_readiness_path=readiness_path,
    )
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "REQUIRED_CONTROL_SURFACE_INVALID" for item in payload["blocking_evidence"])
