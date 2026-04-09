from __future__ import annotations

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
