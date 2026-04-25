from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.common.control_plane_stage_definitions_v1 import (
    CONTROL_STAGE_CONTEXT,
    CONTROL_STAGE_DAY,
    CONTROL_STAGE_SESSION,
    POLICY_ADMIT_AND_CERTIFY,
    POLICY_EVALUATE,
    POLICY_EXPLAIN_BLOCKED,
    POLICY_RECOMPUTE_FROZEN,
    POLICY_SUPERSEDE_FROM_NEW_INPUTS,
)
from constellation_2.common.control_plane_transition_engine_v1 import (
    TRANSITION_RECORD_SCHEMA,
    run_control_plane_transition_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import read_validated_surface_v1
from constellation_2.common.session_authority_v1 import write_active_session_v1
from constellation_2.common.tests.test_control_plane_bundle4_v2 import (
    ACCOUNT,
    DAY,
    ENV,
    SLEEVE,
    _seed_full_chain,
)


def _evaluation_signature(report: dict) -> dict:
    evaluation = report["evaluation"]
    return {
        "stage_id": evaluation["stage_id"],
        "authoritative_inputs_used": evaluation["authoritative_inputs_used"],
        "invariants_checked": evaluation["invariants_checked"],
        "blocked_reason_codes": evaluation["blocked_reason_codes"],
        "family_errors": evaluation["validator_results"]["family_validator_result"]["errors"],
        "boundary_errors": evaluation["validator_results"]["boundary_validator_result"]["errors"],
        "admissible_boolean": evaluation["admissible_boolean"],
        "certifiable_boolean": evaluation["certifiable_boolean"],
    }


def _read_transition_payload(path: str) -> dict:
    return read_validated_surface_v1(
        path=Path(path).resolve(),
        schema_relpath=TRANSITION_RECORD_SCHEMA,
    ).payload


def _admit_to_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, dict]:
    canonical_truth, sleeve_root, _ = _seed_full_chain(tmp_path, monkeypatch)
    day_result = run_control_plane_transition_v1(
        stage_id=CONTROL_STAGE_DAY,
        policy=POLICY_ADMIT_AND_CERTIFY,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    context_result = run_control_plane_transition_v1(
        stage_id=CONTROL_STAGE_CONTEXT,
        policy=POLICY_ADMIT_AND_CERTIFY,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        upstream_stage_path=day_result["stage_ref"]["artifact_path"],
    )
    assert day_result["ok"] is True
    assert context_result["ok"] is True
    return canonical_truth, sleeve_root, context_result


def _admit_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, dict, dict]:
    canonical_truth, sleeve_root, context_result = _admit_to_context(tmp_path, monkeypatch)
    session_result = run_control_plane_transition_v1(
        stage_id=CONTROL_STAGE_SESSION,
        policy=POLICY_ADMIT_AND_CERTIFY,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        upstream_stage_path=context_result["stage_ref"]["artifact_path"],
    )
    assert session_result["ok"] is True
    return canonical_truth, sleeve_root, context_result, session_result


def test_transition_engine_unifies_evaluate_and_mutating_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, _ = _seed_full_chain(tmp_path, monkeypatch)
    evaluate = run_control_plane_transition_v1(
        stage_id=CONTROL_STAGE_DAY,
        policy=POLICY_EVALUATE,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    admit = run_control_plane_transition_v1(
        stage_id=CONTROL_STAGE_DAY,
        policy=POLICY_ADMIT_AND_CERTIFY,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    explain = run_control_plane_transition_v1(
        stage_id=CONTROL_STAGE_DAY,
        policy=POLICY_EXPLAIN_BLOCKED,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )

    assert _evaluation_signature(evaluate) == _evaluation_signature(admit)
    assert _evaluation_signature(evaluate) == _evaluation_signature(explain)
    assert evaluate["transition_record_ref"] is None
    assert explain["transition_record_ref"] is None
    assert admit["transition_record_ref"] is not None
    assert admit["stage_ref"] is not None
    assert admit["stage_certification_ref"] is not None


def test_recompute_frozen_requires_same_authoritative_basis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, context_result, session_result = _admit_session(tmp_path, monkeypatch)

    recompute = run_control_plane_transition_v1(
        stage_id=CONTROL_STAGE_SESSION,
        policy=POLICY_RECOMPUTE_FROZEN,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        upstream_stage_path=context_result["stage_ref"]["artifact_path"],
        frozen_from_transition_record=session_result["transition_record_ref"]["artifact_path"],
    )

    assert recompute["ok"] is True
    assert recompute["stage_ref"] is None
    assert recompute["stage_certification_ref"] is None
    assert recompute["transition_record_ref"] is not None
    assert recompute["evaluation"]["input_resolution_mode"] == "FROZEN"
    assert recompute["evaluation"]["authoritative_inputs_used"] == session_result["evaluation"]["authoritative_inputs_used"]


def test_supersession_required_when_current_authoritative_input_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, context_result, session_result = _admit_session(tmp_path, monkeypatch)

    active_path = canonical_truth / "active_session_v1" / "current.json"
    active_payload = json.loads(active_path.read_text(encoding="utf-8"))
    active_payload["generated_utc"] = "2026-04-19T12:34:56Z"
    write_active_session_v1(truth_root=canonical_truth, payload=active_payload)

    frozen = run_control_plane_transition_v1(
        stage_id=CONTROL_STAGE_SESSION,
        policy=POLICY_RECOMPUTE_FROZEN,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        upstream_stage_path=context_result["stage_ref"]["artifact_path"],
        frozen_from_transition_record=session_result["transition_record_ref"]["artifact_path"],
    )
    assert frozen["ok"] is False
    assert "RECOMPUTE_FROZEN_AUTHORITATIVE_INPUTS_CHANGED" in frozen["evaluation"]["blocked_reason_codes"]

    superseded = run_control_plane_transition_v1(
        stage_id=CONTROL_STAGE_SESSION,
        policy=POLICY_SUPERSEDE_FROM_NEW_INPUTS,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        upstream_stage_path=context_result["stage_ref"]["artifact_path"],
        supersedes_transition_record=session_result["transition_record_ref"]["artifact_path"],
    )
    assert superseded["ok"] is True
    assert superseded["transition_record_ref"] is not None
    assert superseded["stage_ref"] is not None
    assert superseded["stage_certification_ref"] is not None
    payload = _read_transition_payload(superseded["transition_record_ref"]["artifact_path"])
    assert payload["supersedes_ref"]["artifact_path"] == session_result["transition_record_ref"]["artifact_path"]
    assert payload["transition_status"] == "SUPERSEDED_FROM_NEW_INPUTS"


def test_blocked_transition_persists_transition_record_with_taxonomy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, _ = _seed_full_chain(tmp_path, monkeypatch)
    blocked = run_control_plane_transition_v1(
        stage_id=CONTROL_STAGE_CONTEXT,
        policy=POLICY_ADMIT_AND_CERTIFY,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )

    assert blocked["ok"] is False
    assert blocked["stage_ref"] is None
    assert blocked["stage_certification_ref"] is None
    assert blocked["transition_record_ref"] is not None
    payload = _read_transition_payload(blocked["transition_record_ref"]["artifact_path"])
    assert payload["transition_status"] == "BLOCKED"
    assert "UPSTREAM_STAGE_ADMISSION_REQUIRED" in payload["failure_taxonomy"]
    assert payload["admission_decision"]["status"] == "BLOCKED"
    assert payload["certification_decision"]["status"] == "BLOCKED"
