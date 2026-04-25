from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from constellation_2.common.advisory_decision_state_kernel_v1 import (
    ARTIFACT_ID,
    find_latest_advisory_decision_state_v1,
    list_advisory_decision_states_v1,
    materialize_advisory_decision_state_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import _build_scope
from constellation_2.common.control_plane_stage_admission_v1 import (
    CONTROL_STAGE_SESSION_ADMITTED,
    certify_startup_chain_v1,
)
from constellation_2.common.tests.test_control_plane_bundle4_v2 import (
    ACCOUNT,
    DAY,
    ENV,
    SLEEVE,
    SOURCE_ROOT,
    _seed_full_chain,
)
from constellation_2.common.session_authority_v1 import write_active_session_v1
from constellation_2.phaseL.ui_api.advisory_read_model import build_advisory_view


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")


def _scope_id() -> str:
    return _build_scope(
        repo_root=SOURCE_ROOT,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        operation_type="fresh_paper_entry_v1",
        canonical_truth_root=SOURCE_ROOT / "__fixture_truth__",
        truth_sleeves_root=SOURCE_ROOT / "__fixture_truth_sleeves__",
    ).scope_id


def _deployment_path(truth_root: Path) -> Path:
    return (truth_root / "reports" / "deployment_state_machine_v1" / DAY / "deployment_state_machine.v1.json").resolve()


def _seed_deployment_state(
    truth_root: Path,
    *,
    decision: str = "DEPLOY_ACTIVE",
    cleanliness: str = "CLEAN",
) -> Path:
    path = _deployment_path(truth_root)
    payload = {
        "schema_id": "deployment_state_machine",
        "schema_version": "v1",
        "authority_scope": "TOP_LEVEL_DEPLOYMENT_STATE_MACHINE_OWNER",
        "day_utc": DAY,
        "deployment_attempt_id": "deploy:bundle8:test",
        "deployment_state_machine_id": "deploy_state:bundle8:test",
        "evaluated_at_utc": f"{DAY}T12:10:00Z",
        "authoritative_source": {
            "authoritative_repo_root": str(SOURCE_ROOT),
            "authoritative_git_sha": "a" * 40,
            "authoritative_branch": "bundle8-test",
            "authoritative_cleanliness_status": cleanliness,
            "dirty_entry_count": 0 if cleanliness == "CLEAN" else 1,
            "dirty_entry_sample": [] if cleanliness == "CLEAN" else ["dirty.txt"],
            "source_snapshot_id": "source_snapshot:bundle8",
        },
        "release_build": {
            "release_id": "release:bundle8",
            "release_root": "/home/node/constellation_active",
            "release_manifest_sha": "b" * 64,
            "bundled_file_hash_summary": {"file_count": 1, "aggregate_sha256": "c" * 64},
            "build_status": "READY",
            "required_startup_stack_files_present": True,
            "missing_required_startup_stack_files": [],
        },
        "active_release": {
            "active_symlink_path": "/home/node/constellation_active",
            "active_symlink_target": "/home/node/constellation_release",
            "activation_status": "ACTIVE",
            "active_runtime_contract_path": "/home/node/constellation_active/repo_role.v1.json",
            "active_runtime_contract_status": "MATCH",
        },
        "live_execution": {
            "service_unit_path": "/home/node/constellation/ops/systemd/user/c2-paper-day-orchestrator.service",
            "launcher_path": "/home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
            "resolved_execution_root": "/home/node/constellation_active",
            "active_root_match": True,
            "authoritative_service_source_path": "/home/node/constellation/ops/systemd/user/c2-paper-day-orchestrator.service",
            "authoritative_service_source_root": str(SOURCE_ROOT),
        },
        "drift_checks": {
            "authoritative_vs_release": {"status": "PASS", "details": {"fixture": True}},
            "release_vs_active": {"status": "PASS", "details": {"fixture": True}},
            "active_vs_live_execution": {"status": "PASS", "details": {"fixture": True}},
            "runtime_copy_still_executable": {"status": "PASS", "details": {"fixture": True}},
            "required_startup_stack_files_present": {"status": "PASS", "details": {"fixture": True}},
        },
        "post_activation_verification": {
            "passed": decision in {"DEPLOY_READY", "DEPLOY_ACTIVE"} and cleanliness == "CLEAN",
            "release_id": "release:bundle8",
            "release_root": "/home/node/constellation_active",
            "active_symlink_path": "/home/node/constellation_active",
            "active_symlink_target": "/home/node/constellation_release",
            "active_pointer_matches_release": True,
            "required_startup_stack_files_present": True,
            "missing_required_startup_stack_files": [],
            "service_unit_path": "/home/node/constellation/ops/systemd/user/c2-paper-day-orchestrator.service",
            "launcher_path": "/home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
            "resolved_execution_root": "/home/node/constellation_active",
            "active_root_match": True,
            "active_runtime_contract_path": "/home/node/constellation_active/repo_role.v1.json",
            "active_runtime_contract_match": True,
            "blocking_codes": [] if decision in {"DEPLOY_READY", "DEPLOY_ACTIVE"} and cleanliness == "CLEAN" else ["DEPLOYMENT_BLOCKED"],
        },
        "final_deployment_decision": decision,
        "blocking_codes": [] if decision in {"DEPLOY_READY", "DEPLOY_ACTIVE"} and cleanliness == "CLEAN" else ["DEPLOYMENT_BLOCKED"],
        "first_true_blocker_code": "" if decision in {"DEPLOY_READY", "DEPLOY_ACTIVE"} and cleanliness == "CLEAN" else "DEPLOYMENT_BLOCKED",
        "first_true_blocker_path": "" if decision in {"DEPLOY_READY", "DEPLOY_ACTIVE"} and cleanliness == "CLEAN" else str(path),
        "human_readable_summary": f"fixture {decision} {cleanliness}",
        "producer": {
            "repo": str(SOURCE_ROOT),
            "module": "test_advisory_decision_state_v1.py",
            "git_sha": "d" * 40,
        },
    }
    _write_json(path, payload)
    return path


def _bundle5_session_chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, dict, Path]:
    canonical_truth, sleeve_root, candidate = _seed_full_chain(tmp_path, monkeypatch)
    result = certify_startup_chain_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        candidate_path=candidate,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
        emit_artifacts=True,
    )
    assert result["ok"] is True
    chain_path = Path(result["startup_chain_certification_ref"]["artifact_path"]).resolve()
    assert chain_path.exists()
    session_idx = next(
        index
        for index, row in enumerate(result["stage_refs"])
        if str(row.get("artifact_id") or "").strip() == CONTROL_STAGE_SESSION_ADMITTED
    )
    session_result = {
        "stage_ref": dict(result["stage_refs"][session_idx]),
        "transition_record_ref": dict(result["stage_transition_record_refs"][session_idx]),
        "stage_certification_ref": dict(result["stage_certification_refs"][session_idx]),
    }
    return canonical_truth, sleeve_root, session_result, chain_path


def _advisory_result(
    *,
    canonical_truth: Path,
    stage_result: dict,
    chain_path: Path | None,
    deployment_path: Path | None,
    advisory_authority_class: str,
    advisory_item_id: str = "primary_advisory_surface",
    emit_artifact: bool = False,
) -> dict:
    return materialize_advisory_decision_state_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        stage_path=stage_result["stage_ref"]["artifact_path"],
        transition_record_path=stage_result["transition_record_ref"]["artifact_path"],
        advisory_item_id=advisory_item_id,
        advisory_surface_label="operator_shell_advisory",
        advisory_authority_class=advisory_authority_class,
        certification_path=stage_result["stage_certification_ref"]["artifact_path"] if stage_result.get("stage_certification_ref") else None,
        startup_chain_certification_path=str(chain_path) if chain_path is not None else None,
        deployment_state_path=str(deployment_path) if deployment_path is not None else None,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=canonical_truth.parent / "truth_sleeves",
        emit_artifact=emit_artifact,
    )


def _strip_volatile(payload: dict) -> dict:
    out = copy.deepcopy(payload)
    out.pop("generated_at_utc", None)
    return out


def test_decision_object_is_deterministic_for_same_certified_truth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, _, session_result, chain_path = _bundle5_session_chain(tmp_path, monkeypatch)
    deployment_path = _seed_deployment_state(canonical_truth)
    first = _advisory_result(
        canonical_truth=canonical_truth,
        stage_result=session_result,
        chain_path=chain_path,
        deployment_path=deployment_path,
        advisory_authority_class="recommendation",
    )
    second = _advisory_result(
        canonical_truth=canonical_truth,
        stage_result=session_result,
        chain_path=chain_path,
        deployment_path=deployment_path,
        advisory_authority_class="recommendation",
    )
    assert _strip_volatile(first["decision"]) == _strip_volatile(second["decision"])


def test_precedence_prefers_uncertified_over_other_positive_states(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, _, session_result, _ = _bundle5_session_chain(tmp_path, monkeypatch)
    deployment_path = _seed_deployment_state(canonical_truth)
    report = _advisory_result(
        canonical_truth=canonical_truth,
        stage_result=session_result,
        chain_path=None,
        deployment_path=deployment_path,
        advisory_authority_class="promotion_eligible",
    )
    decision = report["decision"]
    assert decision["decision_state"] == "uncertified"
    assert decision["actionability_state"] == "blocked"
    assert decision["promotion_eligibility_state"] == "revoked"
    assert decision["invalidation_rule_id"] == "UNCERTIFIED_BASIS"


def test_invalidation_and_downgrade_matrix_covers_missing_blocked_and_superseded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, _, session_result, chain_path = _bundle5_session_chain(tmp_path, monkeypatch)

    incomplete = _advisory_result(
        canonical_truth=canonical_truth,
        stage_result=session_result,
        chain_path=chain_path,
        deployment_path=None,
        advisory_authority_class="recommendation",
    )["decision"]
    assert incomplete["decision_state"] == "incomplete_basis"
    assert incomplete["invalidation_rule_id"] == "INCOMPLETE_RELEASE_BASIS"

    blocked = _advisory_result(
        canonical_truth=canonical_truth,
        stage_result=session_result,
        chain_path=chain_path,
        deployment_path=_seed_deployment_state(canonical_truth, decision="DEPLOY_BLOCKED_VALID"),
        advisory_authority_class="recommendation",
    )["decision"]
    assert blocked["decision_state"] == "blocked"
    assert blocked["actionability_state"] == "blocked"
    assert blocked["invalidation_rule_id"] == "RELEASE_BASELINE_BLOCKED"

    transition_path = Path(session_result["transition_record_ref"]["artifact_path"]).resolve()
    transition_payload = json.loads(transition_path.read_text(encoding="utf-8"))
    prior_ref = {
        "artifact_id": "control_stage_transition_record_v1",
        "artifact_path": session_result["transition_record_ref"]["artifact_path"],
        "artifact_sha256": session_result["transition_record_ref"]["artifact_sha256"],
    }
    transition_payload["supersedes_ref"] = prior_ref
    supersede_root = transition_path.parent.parent / "transition_bundle8_superseding"
    supersede_path = (supersede_root / "control_stage_transition_record.v1.json").resolve()
    transition_payload["transition_id"] = "transition:bundle8:superseding"
    transition_payload["generated_at_utc"] = f"{DAY}T13:30:00Z"
    _write_json(supersede_path, transition_payload)
    active_payload = json.loads((canonical_truth / "active_session_v1" / "current.json").read_text(encoding="utf-8"))
    active_payload["generated_utc"] = "2026-04-19T12:34:56Z"
    write_active_session_v1(truth_root=canonical_truth, payload=active_payload)
    superseded_report = _advisory_result(
        canonical_truth=canonical_truth,
        stage_result=session_result,
        chain_path=chain_path,
        deployment_path=_seed_deployment_state(canonical_truth),
        advisory_authority_class="recommendation",
    )["decision"]
    assert superseded_report["decision_state"] == "superseded"
    assert superseded_report["invalidation_rule_id"] == "SUPERSEDED_CERTIFIED_BASIS"


def test_stale_basis_and_promotion_eligibility_revocation_are_deterministic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, _, session_result, chain_path = _bundle5_session_chain(tmp_path, monkeypatch)
    deployment_path = _seed_deployment_state(canonical_truth)
    chain_payload = json.loads(chain_path.read_text(encoding="utf-8"))
    refs = list(chain_payload.get("stage_admission_refs") or [])
    assert refs
    session_artifact_id = str(session_result["stage_ref"]["artifact_id"] or "").strip()
    for row in refs:
        if str(row.get("artifact_id") or "").strip() == session_artifact_id:
            row["artifact_sha256"] = "f" * 64
            break
    chain_payload["stage_admission_refs"] = refs
    _write_json(chain_path, chain_payload)

    stale = _advisory_result(
        canonical_truth=canonical_truth,
        stage_result=session_result,
        chain_path=chain_path,
        deployment_path=deployment_path,
        advisory_authority_class="promotion_eligible",
    )["decision"]
    assert stale["decision_state"] == "stale"
    assert stale["promotion_eligibility_state"] == "revoked"
    assert stale["visibility_state"] == "suppressed"


def test_explanation_fidelity_preserves_refs_and_does_not_invent_confidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, _, session_result, chain_path = _bundle5_session_chain(tmp_path, monkeypatch)
    decision = _advisory_result(
        canonical_truth=canonical_truth,
        stage_result=session_result,
        chain_path=chain_path,
        deployment_path=_seed_deployment_state(canonical_truth, decision="DEPLOY_BLOCKED_VALID"),
        advisory_authority_class="recommendation",
    )["decision"]
    explanation = decision["primary_explanation"]
    assert explanation["decision_state"] == decision["decision_state"]
    assert explanation["invalidation_reason"] == decision["invalidation_rule_id"]
    assert explanation["evidence_refs"]
    assert "confidence" not in json.dumps(explanation, sort_keys=True)


def test_historical_reconstruction_and_lineage_are_preserved_in_emitted_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, _, session_result, chain_path = _bundle5_session_chain(tmp_path, monkeypatch)
    first = _advisory_result(
        canonical_truth=canonical_truth,
        stage_result=session_result,
        chain_path=chain_path,
        deployment_path=None,
        advisory_authority_class="recommendation",
        emit_artifact=True,
    )
    second = _advisory_result(
        canonical_truth=canonical_truth,
        stage_result=session_result,
        chain_path=chain_path,
        deployment_path=_seed_deployment_state(canonical_truth),
        advisory_authority_class="recommendation",
        emit_artifact=True,
    )
    refs = list_advisory_decision_states_v1(
        canonical_truth_root=canonical_truth,
        day_utc=DAY,
        scope_id=_scope_id(),
    )
    assert len(refs) >= 2
    latest = find_latest_advisory_decision_state_v1(
        canonical_truth_root=canonical_truth,
        day_utc=DAY,
        scope_id=_scope_id(),
        advisory_item_id="primary_advisory_surface",
    )
    assert latest is not None
    assert latest.payload["artifact_id"] == ARTIFACT_ID
    assert latest.payload["supersedes_ref"]["artifact_path"] == first["artifact_ref"]["artifact_path"]
    assert second["artifact_ref"]["artifact_path"] == str(latest.path)


def test_advisory_rejects_forbidden_upstream_truth_and_ui_renders_artifact_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, _, session_result, chain_path = _bundle5_session_chain(tmp_path, monkeypatch)
    with pytest.raises(Exception):
        materialize_advisory_decision_state_v1(
            day_utc=DAY,
            sleeve_id=SLEEVE,
            environment=ENV,
            ib_account=ACCOUNT,
            stage_path=SOURCE_ROOT / "constellation_2" / "runtime" / "truth" / "bad_stage.json",
            transition_record_path=session_result["transition_record_ref"]["artifact_path"],
            advisory_item_id="primary_advisory_surface",
            advisory_surface_label="operator_shell_advisory",
            advisory_authority_class="recommendation",
            certification_path=session_result["stage_certification_ref"]["artifact_path"],
            startup_chain_certification_path=str(chain_path),
            deployment_state_path=str(_seed_deployment_state(canonical_truth)),
            canonical_truth_root=canonical_truth,
            truth_sleeves_root=canonical_truth.parent / "truth_sleeves",
        )

    _advisory_result(
        canonical_truth=canonical_truth,
        stage_result=session_result,
        chain_path=chain_path,
        deployment_path=_seed_deployment_state(canonical_truth),
        advisory_authority_class="recommendation",
        emit_artifact=True,
    )
    legacy_root = (tmp_path / "legacy_advisory_runtime" / "PAPER" / "official_recommendation_set_v1" / DAY).resolve()
    _write_json(
        legacy_root / "official_recommendation_set.v1.json",
        {
            "created_at": f"{DAY}T01:00:00Z",
            "recommendations": [
                {
                    "recommendation_id": "legacy_should_not_be_used",
                    "domain_id": "legacy",
                    "action_type": "buy",
                    "priority": 1,
                    "recommendation_status": "ACTIVE",
                }
            ],
        },
    )
    import constellation_2.phaseL.ui_api.advisory_read_model as advisory_rm

    monkeypatch.setattr(advisory_rm, "GLOBAL_TRUTH_ROOT", canonical_truth)
    view = build_advisory_view(DAY)
    assert view["decisions"]
    assert "recommendations" not in view
    assert view["decisions"][0]["advisory_item_id"] == "primary_advisory_surface"
