from __future__ import annotations

import re
from pathlib import Path


def resolve_repo_truth_root(repo_root: Path) -> Path:
    return (Path(repo_root).resolve() / "constellation_2" / "runtime" / "truth").resolve()


def resolve_repo_operator_input_root(repo_root: Path) -> Path:
    return (Path(repo_root).resolve() / "constellation_2").resolve()


def resolve_report_artifact_path(*, truth_root: Path, artifact_family: str, day_utc: str, filename: str) -> Path:
    return (Path(truth_root).resolve() / "reports" / artifact_family / str(day_utc).strip() / filename).resolve()


def resolve_paper_open_readiness_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="paper_open_readiness_v1",
        day_utc=day_utc,
        filename="paper_open_readiness.v1.json",
    )


def resolve_paper_session_authority_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="paper_session_authority_v1",
        day_utc=day_utc,
        filename="paper_session_authority.v1.json",
    )


def resolve_subsystem_readiness_report_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="subsystem_readiness_report_v1",
        day_utc=day_utc,
        filename="subsystem_readiness_report.v1.json",
    )


def resolve_session_readiness_refresh_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="session_readiness_refresh_v1",
        day_utc=day_utc,
        filename="session_readiness_refresh.v1.json",
    )


def resolve_startup_proof_validation_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="startup_proof_validation_v1",
        day_utc=day_utc,
        filename="startup_proof_validation.v1.json",
    )


def resolve_operator_summary_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="operator_summary_v1",
        day_utc=day_utc,
        filename="operator_summary.v1.json",
    )


def resolve_bod_execution_environment_proof_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="bod_execution_environment_proof_v1",
        day_utc=day_utc,
        filename="bod_execution_environment_proof.v1.json",
    )


def resolve_phasec_risk_inputs_prep_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="phasec_risk_inputs_prep_v1",
        day_utc=day_utc,
        filename="phasec_risk_inputs_prep.v1.json",
    )


def resolve_operator_day_authority_summary_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="operator_day_authority_summary_v1",
        day_utc=day_utc,
        filename="operator_day_authority_summary.v1.json",
    )


def resolve_day_failure_causality_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="day_failure_causality_v1",
        day_utc=day_utc,
        filename="day_failure_causality.v1.json",
    )


def resolve_day_open_trigger_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="day_open_trigger_v1",
        day_utc=day_utc,
        filename="day_open_trigger.v1.json",
    )


def resolve_day_open_attempt_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="day_open_attempt_v1",
        day_utc=day_utc,
        filename="day_open_attempt.v1.json",
    )


def resolve_intents_day_completeness_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="intents_day_completeness_v1",
        day_utc=day_utc,
        filename="intents_day_completeness.v1.json",
    )


def resolve_trading_day_intent_generation_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="trading_day_intent_generation_v1",
        day_utc=day_utc,
        filename="trading_day_intent_generation.v1.json",
    )


def resolve_startup_materialization_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="startup_materialization_v1",
        day_utc=day_utc,
        filename="startup_materialization.v1.json",
    )


def resolve_paper_trading_posture_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="paper_trading_posture_v1",
        day_utc=day_utc,
        filename="paper_trading_posture.v1.json",
    )


def resolve_submit_boundary_status_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="submit_boundary_status_v1",
        day_utc=day_utc,
        filename="submit_boundary_status.v1.json",
    )


def resolve_paper_trading_day_authority_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="paper_trading_day_authority_v1",
        day_utc=day_utc,
        filename="paper_trading_day_authority.v1.json",
    )


def resolve_trade_readiness_decision_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="trade_readiness_decision_v1",
        day_utc=day_utc,
        filename="trade_readiness_decision.v1.json",
    )


def resolve_trade_readiness_presubmit_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="trade_readiness_presubmit_v1",
        day_utc=day_utc,
        filename="trade_readiness_presubmit.v1.json",
    )


def resolve_sleeve_rollup_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="sleeve_rollup_v1",
        day_utc=day_utc,
        filename="sleeve_rollup.v1.json",
    )


def resolve_paper_session_evidence_manifest_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="paper_session_evidence_manifest_v1",
        day_utc=day_utc,
        filename="paper_session_evidence_manifest.v1.json",
    )


def resolve_paper_session_kernel_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="paper_session_kernel_v1",
        day_utc=day_utc,
        filename="paper_session_kernel.v1.json",
    )


def resolve_paper_session_ledger_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="paper_session_ledger_v1",
        day_utc=day_utc,
        filename="paper_session_ledger.v1.json",
    )


def resolve_paper_day_control_plane_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="paper_day_control_plane_v1",
        day_utc=day_utc,
        filename="paper_day_control_plane.v1.json",
    )


def resolve_paper_session_bootstrap_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="paper_session_bootstrap_v1",
        day_utc=day_utc,
        filename="paper_session_bootstrap.v1.json",
    )


def resolve_execution_reconciliation_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="execution_reconciliation_v1",
        day_utc=day_utc,
        filename="execution_reconciliation.v1.json",
    )


def resolve_execution_lifecycle_authority_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="execution_lifecycle_authority_v1",
        day_utc=day_utc,
        filename="execution_lifecycle_authority.v1.json",
    )


def resolve_paper_trading_integration_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="paper_trading_integration_v1",
        day_utc=day_utc,
        filename="paper_trading_integration.v1.json",
    )


def resolve_runtime_ledger_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "runtime_ledger_v1"
        / str(day_utc).strip()
        / "canonical_runtime_ledger.v1.jsonl"
    ).resolve()


def resolve_canonical_lifecycle_closure_root(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "canonical_lifecycle_closure_v1"
        / str(day_utc).strip()
    ).resolve()


def resolve_canonical_lifecycle_closure_path(*, truth_root: Path, day_utc: str, submission_id: str) -> Path:
    return (
        resolve_canonical_lifecycle_closure_root(
            truth_root=truth_root,
            day_utc=day_utc,
        )
        / str(submission_id).strip()
        / "canonical_lifecycle_closure.v1.json"
    ).resolve()


def resolve_fill_ledger_submission_path(*, truth_root: Path, day_utc: str, submission_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "fill_ledger_v1"
        / str(day_utc).strip()
        / f"{str(submission_id).strip()}.fill_ledger.v1.json"
    ).resolve()


def resolve_positions_effective_pointer_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "positions_v1"
        / "effective_v1"
        / "days"
        / str(day_utc).strip()
        / "positions_effective_pointer.v1.json"
    ).resolve()


def resolve_positions_snapshot_candidate_paths(*, truth_root: Path, day_utc: str) -> tuple[Path, ...]:
    root = (Path(truth_root).resolve() / "positions_v1" / "snapshots" / str(day_utc).strip()).resolve()
    return (
        (root / "positions_snapshot.v4.json").resolve(),
        (root / "positions_snapshot.v3.json").resolve(),
        (root / "positions_snapshot.v2.json").resolve(),
    )


def resolve_accounting_nav_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "accounting_v2"
        / "nav"
        / str(day_utc).strip()
        / "nav.v2.json"
    ).resolve()


def resolve_paper_day_control_plane_attempt_path(*, truth_root: Path, day_utc: str, attempt_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "paper_day_control_plane_v1"
        / str(day_utc).strip()
        / str(attempt_id).strip()
        / "paper_day_control_plane.v1.json"
    ).resolve()


def resolve_trading_day_control_plane_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="trading_day_control_plane_v1",
        day_utc=day_utc,
        filename="trading_day_control_plane.v1.json",
    )


def resolve_trading_day_execution_control_plane_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="trading_day_execution_control_plane_v1",
        day_utc=day_utc,
        filename="trading_day_execution_control_plane.v1.json",
    )


def resolve_trading_day_state_machine_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="trading_day_state_machine_v1",
        day_utc=day_utc,
        filename="trading_day_state_machine.v1.json",
    )


def resolve_truth_lifecycle_phase_result_path(*, truth_root: Path, day_utc: str, phase_id: str) -> Path:
    normalized_phase_id = re.sub(r"[^A-Z0-9_]+", "_", str(phase_id or "").strip().upper()).strip("_")
    if not normalized_phase_id:
        raise ValueError("PHASE_ID_REQUIRED")
    return (
        Path(truth_root).resolve()
        / "reports"
        / "truth_lifecycle_phase_result_v1"
        / str(day_utc).strip()
        / normalized_phase_id
        / "truth_lifecycle_phase_result.v1.json"
    ).resolve()


def resolve_truth_day_run_ledger_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="truth_day_run_ledger_v1",
        day_utc=day_utc,
        filename="truth_day_run_ledger.v1.json",
    )


def resolve_deployment_state_machine_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="deployment_state_machine_v1",
        day_utc=day_utc,
        filename="deployment_state_machine.v1.json",
    )


def resolve_execution_journal_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="execution_journal_v1",
        day_utc=day_utc,
        filename="execution_journal.v1.json",
    )


def resolve_current_system_projection_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="current_system_projection_v1",
        day_utc=day_utc,
        filename="current_system_projection.v1.json",
    )


def resolve_alerts_projection_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="alerts_projection_v1",
        day_utc=day_utc,
        filename="alerts_projection.v1.json",
    )


def resolve_performance_projection_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="performance_projection_v1",
        day_utc=day_utc,
        filename="performance_projection.v1.json",
    )


def resolve_recurrence_kill_gate_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_report_artifact_path(
        truth_root=truth_root,
        artifact_family="recurrence_kill_gate_v1",
        day_utc=day_utc,
        filename="recurrence_kill_gate.v1.json",
    )


def resolve_recurrence_registry_path(*, truth_root: Path) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "recurrence_registry_v1"
        / "recurrence_registry.v1.json"
    ).resolve()


def resolve_baseline_ready_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "readiness_v1"
        / "baseline_ready"
        / str(day_utc).strip()
        / "baseline_ready.v1.json"
    ).resolve()


def resolve_operator_statement_root(*, operator_input_root: Path) -> Path:
    return (Path(operator_input_root).resolve() / "operator_inputs" / "cash_ledger_operator_statements").resolve()


def resolve_operator_statement_path(*, operator_input_root: Path, day_utc: str) -> Path:
    return (
        resolve_operator_statement_root(operator_input_root=operator_input_root)
        / str(day_utc).strip()
        / "operator_statement.v1.json"
    ).resolve()


def resolve_paper_capital_seed_root(*, operator_input_root: Path) -> Path:
    return (Path(operator_input_root).resolve() / "operator_inputs" / "paper_capital_seed_v1").resolve()


def resolve_paper_capital_seed_path(*, operator_input_root: Path, day_utc: str) -> Path:
    return (
        resolve_paper_capital_seed_root(operator_input_root=operator_input_root)
        / str(day_utc).strip()
        / "paper_capital_seed.v1.json"
    ).resolve()


def resolve_paper_submit_smoke_test_request_root(*, operator_input_root: Path) -> Path:
    return (Path(operator_input_root).resolve() / "operator_inputs" / "paper_submit_smoke_test_v1").resolve()


def normalize_paper_submit_smoke_test_request_nonce(*, request_nonce: str | None = None) -> str:
    raw = str(request_nonce or "").strip()
    if not raw:
        return ""
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", raw):
        raise ValueError(f"invalid paper submit smoke request nonce: {raw!r}")
    return raw


def resolve_paper_submit_smoke_test_request_path(
    *,
    operator_input_root: Path,
    day_utc: str,
    request_nonce: str | None = None,
) -> Path:
    nonce = normalize_paper_submit_smoke_test_request_nonce(request_nonce=request_nonce)
    filename = "paper_submit_smoke_test_request.v1.json"
    if nonce:
        filename = f"paper_submit_smoke_test_request.{nonce}.v1.json"
    return (
        resolve_paper_submit_smoke_test_request_root(operator_input_root=operator_input_root)
        / str(day_utc).strip()
        / filename
    ).resolve()


def resolve_paper_submit_smoke_test_report_path(*, sleeve_truth_root: Path, day_utc: str) -> Path:
    return (
        Path(sleeve_truth_root).resolve()
        / "reports"
        / "paper_submit_smoke_test_v1"
        / str(day_utc).strip()
        / "paper_submit_smoke_test.v1.json"
    ).resolve()
