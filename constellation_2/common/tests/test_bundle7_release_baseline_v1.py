from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.common.configuration_activation_authority_v1 import (
    run_configuration_activation_authority_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import _build_scope
from constellation_2.common.control_plane_trust_surface_validator_v1 import (
    validate_control_plane_trust_surfaces_v1,
)
from constellation_2.common.release_baseline_common_v1 import (
    ACTIVE_BASELINE_PATHS,
    forbidden_root_hits_in_active_paths_v1,
)
from constellation_2.common.release_baseline_gate_v1 import (
    build_certified_operational_readiness_v1,
    evaluate_release_baseline_gate_v1,
)
from constellation_2.common.runtime_state_readiness_validator_v1 import (
    validate_runtime_state_readiness_v1,
)
from constellation_2.common.tests.test_configuration_activation_authority_v1 import (
    _write_policy_snapshot,
)
from constellation_2.common.tests.test_control_plane_bundle4_v2 import (
    ACCOUNT,
    DAY,
    ENV,
    SLEEVE,
    SOURCE_ROOT,
)
from constellation_2.common import deployment_state_machine_v1 as deploy_common
import ops.tools.run_deployment_state_machine_v1 as deploy_runner


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _metric_view(raw_value: int | None, unit: str, display_value: str) -> dict[str, object]:
    return {
        "raw_value": raw_value,
        "unit": unit,
        "display_value": display_value,
    }


def _seed_active_path_fixture(repo_root: Path, *, forbidden_literal: str | None = None) -> None:
    for relpath in ACTIVE_BASELINE_PATHS:
        path = (repo_root / relpath).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".json":
            payload = {"status": "SAFE"}
            if forbidden_literal and relpath.endswith("run_constellation_runtime_state_snapshot_v1.py"):
                path.write_text(f'{{"unsafe":"{forbidden_literal}"}}\n', encoding="utf-8")
            else:
                path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
        elif path.suffix == ".md":
            text = "safe canonical reference\n"
            if forbidden_literal and relpath.endswith("operator_trust_panel_v1.contract.md"):
                text = f"unsafe {forbidden_literal}\n"
            path.write_text(text, encoding="utf-8")
        else:
            text = "# safe active baseline file\n"
            if forbidden_literal and relpath.endswith("run_constellation_runtime_state_snapshot_v1.py"):
                text = f'# {forbidden_literal}\n'
            path.write_text(text, encoding="utf-8")


def _seed_configuration_activation(truth_root: Path) -> None:
    policy_path = _write_policy_snapshot(
        truth_root=truth_root,
        generated_at_utc=f"{DAY}T12:00:00Z",
        effective_at_utc=f"{DAY}T12:00:00Z",
        logical_name="bundle7_config",
        document_text='{"mode":"bundle7","risk":"controlled"}\n',
    )
    result = run_configuration_activation_authority_v1(
        policy_snapshot_path=policy_path,
        truth_root=truth_root,
    )
    assert result.configuration_state_ref is not None


def _seed_runtime_state_family(truth_root: Path) -> None:
    runtime_state_path = (truth_root / "system_snapshot" / "constellation_runtime_state.v1.json").resolve()
    bug_metrics_path = (
        truth_root / "readiness_v1" / "constellation_bug_metrics_v1" / DAY / "constellation_bug_metrics.v1.json"
    ).resolve()
    platform_readiness_path = (
        truth_root / "readiness_v1" / "constellation_platform_readiness_v1" / DAY / "constellation_platform_readiness.v1.json"
    ).resolve()
    _write_json(
        runtime_state_path,
        {
            "artifact_id": "constellation_runtime_state",
            "schema_version": "1.1",
            "generated_utc": f"{DAY}T12:05:00Z",
            "system_identity": {
                "name": "Constellation 2.0",
                "repo_root": str(SOURCE_ROOT),
                "canonical_truth_root": str(truth_root.resolve()),
            },
            "system_status": {
                "mode": "PAPER",
                "overall_health": "OK",
                "overall_status": "PASS",
                "paper_trading_ready_status": "PASS",
                "operator_daily_gate_status": "PASS",
                "gate_stack_verdict_status": "PASS",
            },
            "scope_health": {
                "overall": {"status": "PASS", "reason_codes": []},
                "sleeve_execution_health": {"status": "PASS", "sleeves": []},
                "system_monitoring_health": {"status": "PASS", "freshness": {"status": "PASS", "reason_codes": []}},
            },
            "latest_operating_day": DAY,
            "latest_execution_day": DAY,
            "latest_global_activity_day": DAY,
            "lifecycle_state": {"latest_day": DAY, "stage_presence": {}, "lifecycle_monitor_status": "PASS"},
            "sleeves": [],
            "engines": [],
            "capital_state": {"capital_authority_status": "PASS", "capital_risk_envelope_status": "PASS", "latest_capital_authority_day": DAY},
            "execution_state": {"broker_connection_status": "PASS", "latest_submission_day": DAY, "latest_broker_event_day": DAY, "latest_fill_ledger_day": DAY},
            "stream_summaries": {},
            "self_diagnostics": {"status": "OK", "diagnostic_count": 0, "diagnostics": []},
            "unknowns": [],
        },
    )
    _write_json(
        bug_metrics_path,
        {
            "schema_id": "C2_BUG_METRICS_V1",
            "schema_version": 1,
            "day_utc": DAY,
            "produced_utc": f"{DAY}T12:06:00Z",
            "window_days_evaluated": {"window_7d": 7, "window_14d": 14},
            "new_bug_events_today": 0,
            "bug_velocity_7d_avg": 0,
            "bug_velocity_14d_avg": 0,
            "recurring_bug_events": [],
            "recurrence_rate": 0,
            "mttr_hours": 1,
            "median_ttr_hours": 1,
            "bug_velocity_trend": "STABLE",
            "bug_half_life_estimate_days": 1,
            "diagnostic_stability_rate": 100,
            "metric_views": {
                "new_bug_events_today": _metric_view(0, "count", "0"),
                "bug_velocity_7d_avg": _metric_view(0, "count", "0"),
                "bug_velocity_14d_avg": _metric_view(0, "count", "0"),
                "recurrence_rate": _metric_view(0, "pct", "0%"),
                "diagnostic_stability_rate": _metric_view(100, "pct", "100%"),
                "mttr_hours": _metric_view(1, "hours", "1"),
                "median_ttr_hours": _metric_view(1, "hours", "1"),
                "bug_half_life_estimate_days": _metric_view(1, "days", "1"),
            },
            "calculation_summary": {
                "new_bug_events_today_basis": "fixture",
                "bug_velocity_basis": "fixture",
                "recurrence_rate_basis": "fixture",
                "diagnostic_stability_basis": "fixture",
                "window_days_used": {
                    "velocity_7d_days": 7,
                    "velocity_14d_days": 14,
                    "recurrence_days": 14,
                },
            },
            "evidence_paths": [str(runtime_state_path)],
            "event_counts_by_day": [],
            "unknown_fields": [],
        },
    )
    _write_json(
        platform_readiness_path,
        {
            "schema_id": "C2_PLATFORM_READINESS_V1",
            "schema_version": 1,
            "day_utc": DAY,
            "produced_utc": f"{DAY}T12:07:00Z",
            "platform_readiness_state": "READY",
            "platform_readiness_score": 95,
            "platform_readiness_grade": "A",
            "score_threshold_ready": 80,
            "platform_promotion_candidate": True,
            "root_blockers": [],
            "derived_blockers": [],
            "aggregate_blocker_summary": {},
            "readiness_summary": "Ready from certified baseline.",
            "promotion_decision_basis": "fixture",
            "top_blockers_ordered": [],
            "minimum_conditions_summary": [],
            "current_vs_required": {},
            "promotion_checklist": {
                "must_be_true": [],
                "currently_false": [],
                "gating_conditions": [],
                "informational_conditions": [],
            },
            "smallest_clearance_set": [],
            "blocker_dependency_order": [],
            "score_contribution": [],
            "metric_views": {
                "platform_readiness_score": _metric_view(95, "score", "95"),
                "score_threshold_ready": _metric_view(80, "score", "80"),
                "bug_velocity_7d_avg": _metric_view(0, "count", "0"),
                "recurrence_rate": _metric_view(0, "pct", "0%"),
                "diagnostic_stability_rate": _metric_view(100, "pct", "100%"),
            },
            "policy_values": {},
            "evidence_paths": [str(runtime_state_path), str(bug_metrics_path)],
            "bug_stability_summary": "Stable",
            "calibration_support": {},
        },
    )


def _seed_deployment_active(
    truth_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_root = tmp_path / "release"
    release_root.mkdir(parents=True, exist_ok=True)
    active_pointer = tmp_path / "constellation_active"
    if active_pointer.exists() or active_pointer.is_symlink():
        active_pointer.unlink()
    active_pointer.symlink_to(release_root)
    service_path = tmp_path / "live.service"
    service_path.write_text(
        "[Service]\n"
        "ExecStart=/usr/bin/bash -lc 'exec /home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh'\n",
        encoding="utf-8",
    )
    inspection = deploy_common.ReleaseInspectionV1(
        release_id="release-001",
        release_root=release_root,
        manifest_path=release_root / "release_manifest.v1.json",
        manifest={"release_id": "release-001", "git_sha": "a" * 40},
        manifest_sha256="b" * 64,
        bundled_file_hash_summary={"file_count": 10, "aggregate_sha256": "c" * 64},
        required_startup_stack_present=True,
        missing_required_files=[],
    )

    monkeypatch.setattr(deploy_runner, "ACTIVE_POINTER", active_pointer)
    monkeypatch.setattr(deploy_runner, "git_sha_or_fail", lambda repo_root: "a" * 40)
    monkeypatch.setattr(deploy_runner, "git_branch_or_fail", lambda repo_root: "feature/test")
    monkeypatch.setattr(deploy_runner, "git_cleanliness_status", lambda repo_root: ("CLEAN", []))
    monkeypatch.setattr(deploy_runner, "inspect_release_root", lambda release_root: inspection)
    monkeypatch.setattr(
        deploy_runner,
        "evaluate_post_activation_verification",
        lambda release_root: {
            "passed": True,
            "release_id": "release-001",
            "release_root": str(release_root),
            "active_symlink_path": str(active_pointer),
            "active_symlink_target": str(release_root),
            "active_pointer_matches_release": True,
            "required_startup_stack_files_present": True,
            "missing_required_startup_stack_files": [],
            "service_unit_path": str(service_path),
            "launcher_path": "/home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
            "resolved_execution_root": "/home/node/constellation_active",
            "active_root_match": True,
            "active_runtime_contract_path": "/tmp/active_runtime_contract.v1.json",
            "active_runtime_contract_match": True,
            "blocking_codes": [],
        },
    )
    monkeypatch.setattr(
        deploy_runner,
        "evaluate_service_resolution",
        lambda service_unit_path: {
            "service_unit_path": str(service_unit_path),
            "launcher_path": "/home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
            "resolved_execution_root": "/home/node/constellation_active",
            "active_root_match": True,
        },
    )
    monkeypatch.setattr(deploy_runner, "resolve_live_service_fragment_path", lambda: service_path)
    monkeypatch.setattr(
        deploy_runner,
        "load_active_runtime_contract_if_present",
        lambda: {"release_root": str(release_root), "release_id": "release-001", "status": "ACTIVE"},
    )
    monkeypatch.setattr(
        deploy_runner,
        "read_execution_journal_identity_anchor_v1",
        lambda truth_root, day_utc: {
            "day_utc": day_utc,
            "day_attempt_id": f"trading_day_state_machine_attempt:{DAY}:test",
            "pipeline_run_id": "",
            "release_id": "",
            "git_sha": "a" * 40,
        },
    )
    monkeypatch.setattr(deploy_runner, "append_deployment_outcome_event_v1", lambda **kwargs: type("Ref", (), {"path": tmp_path / "journal.json"})())
    monkeypatch.setattr(deploy_runner, "resolve_authoritative_repo_root_v1", lambda repo_root: SOURCE_ROOT)
    monkeypatch.setattr(deploy_runner, "resolve_decision_truth_root_v1", lambda truth_root_arg, repo_root: Path(truth_root_arg).resolve())
    monkeypatch.setattr(deploy_runner, "validate_against_repo_schema_v1", deploy_runner.validate_against_repo_schema_v1)
    deploy_runner.main(["--day_utc", DAY, "--truth_root", str(truth_root)])


def _seed_trust_surface_chain(tmp_path: Path) -> tuple[Path, Path]:
    canonical_truth = (tmp_path / "truth").resolve()
    sleeve_root = (tmp_path / "truth_sleeves").resolve()
    scope = _build_scope(
        repo_root=SOURCE_ROOT,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        operation_type="fresh_paper_entry_v1",
        candidate_path=None,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    operator_path = (
        canonical_truth / "reports" / "control_plane_operator_status_v1" / DAY / scope.scope_id / "control_plane_operator_status.v1.json"
    ).resolve()
    timeline_path = (
        canonical_truth / "reports" / "transition_timeline_projection_v1" / DAY / scope.scope_id / "transition_timeline_projection.v1.json"
    ).resolve()
    transition_ref = {
        "artifact_id": "control_stage_transition_record_v1",
        "artifact_path": str((canonical_truth / "reports" / "control_stage_transition_record_v1" / DAY / "session" / scope.scope_id / "T1" / "control_stage_transition_record.v1.json").resolve()),
        "artifact_sha256": "a" * 64,
    }
    stage_ref = {
        "artifact_id": "control_stage_session_admitted_v1",
        "artifact_path": str((canonical_truth / "reports" / "control_stage_session_admitted_v1" / DAY / scope.scope_id / "control_stage_session_admitted.v1.json").resolve()),
        "artifact_sha256": "b" * 64,
    }
    cert_ref = {
        "artifact_id": "startup_chain_certification_v1",
        "artifact_path": str((canonical_truth / "reports" / "startup_chain_certification_v1" / DAY / scope.scope_id / "startup_chain_certification.v1.json").resolve()),
        "artifact_sha256": "c" * 64,
    }
    _write_json(
        operator_path,
        {
            "schema_id": "control_plane_operator_status",
            "schema_version": "v1",
            "artifact_id": "control_plane_operator_status_v1",
            "surface_kind": "projection",
            "projection_id": "op-status-1",
            "projection_version": "constellation_2.common.control_plane_trust_projection_kernel_v1",
            "generated_at_utc": f"{DAY}T12:00:00Z",
            "authority_label": "governed_derived",
            "target_day": DAY,
            "scope_id": scope.scope_id,
            "operation_type": scope.operation_type,
            "sleeve_id": scope.sleeve_id,
            "environment": scope.environment,
            "ib_account": scope.ib_account,
            "freshness_state": "fresh",
            "explanation_mapping_version": "v1",
            "chain_certification_status": "CERTIFIED",
            "current_operator_status": "CERTIFIED_READY",
            "stage_rows": [
                {
                    "stage_id": "session",
                    "stage_admitted": True,
                    "stage_certified": True,
                    "latest_transition_status": "ADMITTED_AND_CERTIFIED",
                    "latest_transition_ref": transition_ref,
                    "freshness_state": "fresh",
                }
            ],
            "latest_blocked_explanations": [],
            "recovery_supersession_lineage": [],
            "governing_stage_refs": [stage_ref],
            "governing_transition_refs": [transition_ref],
            "governing_certification_refs": [cert_ref],
            "evidence_refs": [transition_ref, stage_ref, cert_ref],
            "semantic_events": [
                {
                    "event_type": "projection_refresh",
                    "authority_label": "governed_derived",
                    "freshness_state": "fresh",
                    "projection_version": "constellation_2.common.control_plane_trust_projection_kernel_v1",
                    "governing_refs": [transition_ref, stage_ref, cert_ref],
                    "taxonomy": ["CERTIFIED"],
                }
            ],
        },
    )
    _write_json(
        timeline_path,
        {
            "schema_id": "transition_timeline_projection",
            "schema_version": "v1",
            "artifact_id": "transition_timeline_projection_v1",
            "surface_kind": "projection",
            "projection_id": "timeline-1",
            "projection_version": "constellation_2.common.control_plane_trust_projection_kernel_v1",
            "generated_at_utc": f"{DAY}T12:00:01Z",
            "authority_label": "governed_derived",
            "target_day": DAY,
            "scope_id": scope.scope_id,
            "operation_type": scope.operation_type,
            "sleeve_id": scope.sleeve_id,
            "environment": scope.environment,
            "ib_account": scope.ib_account,
            "freshness_state": "fresh",
            "timeline_rows": [
                {
                    "transition_id": "T1",
                    "stage_id": "session",
                    "policy": "admit_and_certify_v1",
                    "transition_status": "ADMITTED_AND_CERTIFIED",
                    "generated_at_utc": f"{DAY}T11:59:00Z",
                    "freshness_state": "fresh",
                    "blocked_reason_codes": [],
                    "transition_record_ref": transition_ref,
                    "stage_ref": stage_ref,
                    "stage_certification_ref": cert_ref,
                }
            ],
            "governing_stage_refs": [stage_ref],
            "governing_transition_refs": [transition_ref],
            "governing_certification_refs": [cert_ref],
            "evidence_refs": [transition_ref, stage_ref, cert_ref],
            "semantic_events": [{"event_type": "projection_refresh"}],
        },
    )
    return canonical_truth, sleeve_root


def _seed_release_gate_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, Path]:
    repo_root = (tmp_path / "baseline_repo").resolve()
    _seed_active_path_fixture(repo_root)
    canonical_truth = (tmp_path / "truth_gate").resolve()
    sleeve_root = (tmp_path / "truth_sleeves_gate").resolve()
    _seed_deployment_active(canonical_truth, tmp_path, monkeypatch)
    monkeypatch.setattr(
        "constellation_2.common.release_baseline_gate_v1.validate_configuration_activation_family_v1",
        lambda **kwargs: {"ok": True, "errors": [], "summary": {"status": "VALID"}},
    )
    monkeypatch.setattr(
        "constellation_2.common.release_baseline_gate_v1.validate_day_activation_family_v1",
        lambda **kwargs: {"ok": True, "errors": [], "summary": {"status": "PASS"}},
    )
    monkeypatch.setattr(
        "constellation_2.common.release_baseline_gate_v1.validate_global_context_family_v1",
        lambda **kwargs: {"ok": True, "errors": [], "summary": {"status": "PASS"}},
    )
    monkeypatch.setattr(
        "constellation_2.common.release_baseline_gate_v1.validate_session_authority_family_v1",
        lambda **kwargs: {"ok": True, "errors": [], "summary": {"status": "PASS"}},
    )
    monkeypatch.setattr(
        "constellation_2.common.release_baseline_gate_v1.validate_control_plane_boundary_v1",
        lambda **kwargs: {"ok": True, "errors": [], "summary": {"status": "PASS"}},
    )
    monkeypatch.setattr(
        "constellation_2.common.release_baseline_gate_v1.validate_runtime_state_readiness_v1",
        lambda **kwargs: {
            "ok": True,
            "errors": [],
            "summary": {
                "latest_operating_day": DAY,
                "overall_status": "PASS",
                "runtime_health": "OK",
                "platform_readiness_state": "READY",
                "platform_readiness_grade": "A",
            },
        },
    )
    monkeypatch.setattr(
        "constellation_2.common.release_baseline_gate_v1.validate_control_plane_trust_surfaces_v1",
        lambda **kwargs: {
            "ok": True,
            "errors": [],
            "operator_summary": {
                "current_operator_status": "CERTIFIED_READY",
                "chain_certification_status": "CERTIFIED",
                "freshness_state": "fresh",
            },
            "timeline_summary": {
                "transition_row_count": 1,
                "blocked_transition_count": 0,
                "latest_transition_id": "T1",
                "freshness_state": "fresh",
            },
        },
    )
    return repo_root, canonical_truth, sleeve_root


def test_active_path_forbidden_root_closure_holds_on_authoritative_repo() -> None:
    assert forbidden_root_hits_in_active_paths_v1() == []


def test_runtime_state_readiness_validator_passes_and_fails_deterministically(tmp_path: Path) -> None:
    truth_root = (tmp_path / "truth").resolve()
    _seed_runtime_state_family(truth_root)
    passing = validate_runtime_state_readiness_v1(
        repo_root=SOURCE_ROOT,
        canonical_truth_root=truth_root,
    )
    assert passing["ok"] is True

    bad_platform = truth_root / "readiness_v1" / "constellation_platform_readiness_v1" / DAY / "constellation_platform_readiness.v1.json"
    payload = json.loads(bad_platform.read_text(encoding="utf-8"))
    payload["evidence_paths"] = ["/tmp/wrong.json"]
    _write_json(bad_platform, payload)
    failing = validate_runtime_state_readiness_v1(
        repo_root=SOURCE_ROOT,
        canonical_truth_root=truth_root,
    )
    assert failing["ok"] is False
    assert "PLATFORM_READINESS_RUNTIME_STATE_EVIDENCE_MISSING" in failing["errors"]


def test_trust_surface_validator_passes_and_fails_deterministically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_truth, sleeve_root = _seed_trust_surface_chain(tmp_path)
    passing = validate_control_plane_trust_surfaces_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert passing["ok"] is True

    operator_path = Path(passing["validated_refs"][0]["artifact_path"])
    payload = json.loads(operator_path.read_text(encoding="utf-8"))
    payload["scope_id"] = "wrong"
    _write_json(operator_path, payload)
    failing = validate_control_plane_trust_surfaces_v1(
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert failing["ok"] is False
    assert "CONTROL_PLANE_OPERATOR_STATUS_SCOPE_MISMATCH" in failing["errors"]


def test_release_gate_passes_on_clean_certified_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root, canonical_truth, sleeve_root = _seed_release_gate_success(tmp_path, monkeypatch)
    report = evaluate_release_baseline_gate_v1(
        repo_root=repo_root,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert report["ok"] is True
    assert report["baseline_status"]["status"] == "PASS"
    assert report["canonical_root_status"]["ok"] is True
    assert all(row["ok"] for row in report["validator_statuses"].values() if row["required"])


def test_release_gate_fails_closed_on_forbidden_root_hit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, canonical_truth, sleeve_root = _seed_release_gate_success(tmp_path, monkeypatch)
    repo_root = (tmp_path / "forbidden_repo").resolve()
    _seed_active_path_fixture(repo_root, forbidden_literal="constellation_2/runtime/truth")
    report = evaluate_release_baseline_gate_v1(
        repo_root=repo_root,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert report["ok"] is False
    assert "FORBIDDEN_ACTIVE_ROOT_HITS_PRESENT" in report["blocking_errors"]
    assert report["forbidden_root_hits"]


def test_release_gate_fails_closed_on_missing_validator_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root, canonical_truth, sleeve_root = _seed_release_gate_success(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "constellation_2.common.release_baseline_gate_v1.validate_runtime_state_readiness_v1",
        lambda **kwargs: {"ok": False, "errors": ["RUNTIME_STATE_INVALID"], "summary": {}},
    )
    report = evaluate_release_baseline_gate_v1(
        repo_root=repo_root,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert report["ok"] is False
    assert "VALIDATOR_FAILED:runtime_state_readiness" in report["blocking_errors"]


def test_release_gate_fails_closed_on_unresolved_baseline_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root, canonical_truth, sleeve_root = _seed_release_gate_success(tmp_path, monkeypatch)
    deployment_path = canonical_truth / "reports" / "deployment_state_machine_v1" / DAY / "deployment_state_machine.v1.json"
    payload = json.loads(deployment_path.read_text(encoding="utf-8"))
    payload["authoritative_source"]["authoritative_cleanliness_status"] = "DIRTY"
    _write_json(deployment_path, payload)
    report = evaluate_release_baseline_gate_v1(
        repo_root=repo_root,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert report["ok"] is False
    assert "AUTHORITATIVE_BASELINE_NOT_CLEAN" in report["blocking_errors"]


def test_certified_operational_readiness_view_is_deterministic_and_authority_labeled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root, canonical_truth, sleeve_root = _seed_release_gate_success(tmp_path, monkeypatch)
    report = build_certified_operational_readiness_v1(
        repo_root=repo_root,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert report["ok"] is True
    assert report["authority_label"] == "governed_release_projection"
    assert report["blocked_state"]["blocked"] is False
    assert report["transition_timeline_summary"]["transition_row_count"] >= 1

    monkeypatch.setattr(
        "constellation_2.common.release_baseline_gate_v1.validate_runtime_state_readiness_v1",
        lambda **kwargs: {"ok": False, "errors": ["RUNTIME_STATE_CANONICAL_ROOT_MISMATCH"], "summary": {}},
    )
    blocked = build_certified_operational_readiness_v1(
        repo_root=repo_root,
        day_utc=DAY,
        sleeve_id=SLEEVE,
        environment=ENV,
        ib_account=ACCOUNT,
        canonical_truth_root=canonical_truth,
        truth_sleeves_root=sleeve_root,
    )
    assert blocked["ok"] is False
    assert blocked["blocked_state"]["blocked"] is True
