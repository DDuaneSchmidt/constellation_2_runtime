from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.common.control_plane_stage_admission_v1 import certify_startup_chain_v1
from constellation_2.common.control_plane_stage_definitions_v1 import (
    CONTROL_STAGE_CONTEXT,
    CONTROL_STAGE_SESSION,
    POLICY_ADMIT_AND_CERTIFY,
    POLICY_SUPERSEDE_FROM_NEW_INPUTS,
)
from constellation_2.common.control_plane_transition_engine_v1 import (
    run_control_plane_transition_v1,
)
from constellation_2.common.control_plane_trust_projection_kernel_v1 import (
    ControlPlaneTrustProjectionError,
    materialize_advisory_truth_binding_status_v1,
    materialize_control_plane_blocked_transition_view_v1,
    materialize_control_plane_operator_status_v1,
    materialize_transition_timeline_projection_v1,
)
from constellation_2.common.session_authority_v1 import write_active_session_v1
from constellation_2.common.tests.test_control_plane_bundle4_v2 import (
    ACCOUNT,
    DAY,
    ENV,
    SLEEVE,
    _seed_full_chain,
)
from constellation_2.common.tests.test_control_plane_bundle5_v2 import _admit_session


def _without_generated(payload: dict) -> dict:
    cloned = json.loads(json.dumps(payload))
    cloned.pop("generated_at_utc", None)
    return cloned


def test_trust_plane_projections_are_deterministic_and_labeled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, candidate = _seed_full_chain(tmp_path, monkeypatch)
    certify_startup_chain_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        candidate_path=candidate,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )

    operator_a = materialize_control_plane_operator_status_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifact=True,
    )
    operator_b = materialize_control_plane_operator_status_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifact=False,
    )
    timeline_a = materialize_transition_timeline_projection_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifact=True,
    )
    timeline_b = materialize_transition_timeline_projection_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifact=False,
    )

    assert operator_a["ok"] is True
    assert operator_a["artifact_ref"] is not None
    assert operator_a["projection"]["authority_label"] == "governed_derived"
    assert operator_a["projection"]["current_operator_status"] == "CERTIFIED_READY"
    assert _without_generated(operator_a["projection"]) == _without_generated(operator_b["projection"])

    assert timeline_a["ok"] is True
    assert timeline_a["artifact_ref"] is not None
    assert timeline_a["projection"]["authority_label"] == "governed_derived"
    assert timeline_a["projection"]["timeline_rows"]
    assert _without_generated(timeline_a["projection"]) == _without_generated(timeline_b["projection"])


def test_blocked_transition_projection_preserves_taxonomy_and_evidence(
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
    report = materialize_control_plane_blocked_transition_view_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        transition_record_path=blocked["transition_record_ref"]["artifact_path"],
        emit_artifact=True,
    )

    assert blocked["ok"] is False
    assert report["ok"] is True
    assert report["artifact_ref"] is not None
    assert report["projection"]["authority_label"] == "governed_derived"
    assert "UPSTREAM_STAGE_ADMISSION_REQUIRED" in report["projection"]["blocked_reason_codes"]
    assert [row["taxonomy_code"] for row in report["projection"]["explanations"]] == ["UPSTREAM_STAGE_ADMISSION_REQUIRED"]
    assert report["projection"]["evidence_refs"] == [report["projection"]["transition_record_ref"]]


def test_advisory_truth_binding_downgrades_on_superseded_truth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, context_result, session_result = _admit_session(tmp_path, monkeypatch)
    fresh = materialize_advisory_truth_binding_status_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        stage_path=session_result["stage_ref"]["artifact_path"],
        transition_record_path=session_result["transition_record_ref"]["artifact_path"],
        certification_path=session_result["stage_certification_ref"]["artifact_path"],
        advisory_surface_label="official_recommendation_set_v1",
        advisory_authority_class="recommendation",
        emit_artifact=True,
    )
    assert fresh["ok"] is True
    assert fresh["projection"]["freshness_state"] == "fresh"
    assert fresh["projection"]["current_visibility"] == "current"

    active_path = canonical_truth / "active_session_v1" / "current.json"
    active_payload = json.loads(active_path.read_text(encoding="utf-8"))
    active_payload["generated_utc"] = "2026-04-19T12:34:56Z"
    write_active_session_v1(truth_root=canonical_truth, payload=active_payload)

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
    historical = materialize_advisory_truth_binding_status_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        stage_path=session_result["stage_ref"]["artifact_path"],
        transition_record_path=session_result["transition_record_ref"]["artifact_path"],
        certification_path=session_result["stage_certification_ref"]["artifact_path"],
        advisory_surface_label="official_recommendation_set_v1",
        advisory_authority_class="recommendation",
        emit_artifact=False,
    )

    assert superseded["ok"] is True
    assert historical["projection"]["freshness_state"] == "superseded"
    assert historical["projection"]["current_visibility"] == "historical_only"
    assert "TRUST_BINDING_SUPERSEDED" in [row["taxonomy_code"] for row in historical["projection"]["explanations"]]


def test_advisory_truth_binding_require_current_fails_closed_when_uncertified(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, _, session_result = _admit_session(tmp_path, monkeypatch)
    report = materialize_advisory_truth_binding_status_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        stage_path=session_result["stage_ref"]["artifact_path"],
        transition_record_path=session_result["transition_record_ref"]["artifact_path"],
        advisory_surface_label="official_recommendation_set_v1",
        advisory_authority_class="recommendation",
        require_current=True,
    )

    assert report["ok"] is False
    assert report["projection"]["freshness_state"] == "uncertified"
    assert report["projection"]["current_visibility"] == "historical_only"


def test_trust_plane_rejects_derived_surface_as_truth_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root, _ = _seed_full_chain(tmp_path, monkeypatch)
    derived_surface = (canonical_truth / "session_authority_status_v1" / "current.json").resolve()
    derived_surface.parent.mkdir(parents=True, exist_ok=True)
    derived_surface.write_text("{}", encoding="utf-8")

    with pytest.raises(ControlPlaneTrustProjectionError):
        materialize_advisory_truth_binding_status_v1(
            day_utc=DAY,
            sleeve_id=SLEEVE,
            environment=ENV,
            ib_account=ACCOUNT,
            canonical_truth_root=canonical_truth,
            truth_sleeves_root=sleeve_root,
            stage_path=derived_surface,
            transition_record_path=derived_surface,
            advisory_surface_label="official_recommendation_set_v1",
            advisory_authority_class="diagnostic",
        )
