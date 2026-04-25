from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from constellation_2.common.kill_switch_authority_v1 import (
    STATUS_PASS as KILL_SWITCH_STATUS_PASS,
    resolve_kill_switch_authority_v1,
)
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
ACTIVE_RUNTIME_CONTRACT_PATH = (
    Path("/home/node/constellation_runtime_data")
    / "runtime_contract_v1"
    / "active_runtime_contract.v1.json"
).resolve()

ACTIVE_RUNTIME_CONTRACT_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json"
)
RELEASE_MANIFEST_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json"
ACTIVE_SESSION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/active_session.v1.schema.json"
SESSION_AUTHORITY_STATUS_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/session_authority_status.v1.schema.json"
)
SESSION_AUTHORITY_ALERT_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/session_authority_alert.v1.schema.json"
)
MARKET_CALENDAR_COVERAGE_STATUS_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/market_calendar_coverage_status.v1.schema.json"
)
MARKET_CALENDAR_DAY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/MARKET_DATA/market_calendar.v1.schema.json"
TARGET_DAY_BUILD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_build.v1.schema.json"
TARGET_DAY_ADMISSION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_admission.v1.schema.json"
)
SUBMIT_BOUNDARY_STATUS_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_status.v1.schema.json"
)
PAPER_SESSION_LEDGER_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_ledger.v1.schema.json"
)
PAPER_SESSION_EVIDENCE_MANIFEST_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_evidence_manifest.v1.schema.json"
)
PAPER_SESSION_KERNEL_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_kernel.v1.schema.json"
)
PAPER_DAY_CONTROL_PLANE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_day_control_plane.v1.schema.json"
)
PAPER_TRADING_POSTURE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_trading_posture.v1.schema.json"
)
SLEEVE_ROLLUP_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_rollup.v1.schema.json"
STARTUP_MATERIALIZATION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization.v1.schema.json"
)
STARTUP_PROOF_VALIDATION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/startup_proof_validation.v1.schema.json"
)
ECONOMIC_STATE_BUILD_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/economic_state_build.v1.schema.json"
)
ECONOMIC_STATE_PACKAGE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/ECONOMIC/economic_state_package.v1.schema.json"
)
TRADING_DAY_CONTROL_PLANE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_control_plane.v1.schema.json"
)
TRADING_DAY_EXECUTION_CONTROL_PLANE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_execution_control_plane.v1.schema.json"
)
TRADING_DAY_STATE_MACHINE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_state_machine.v1.schema.json"
)
TRADING_DAY_INTENT_GENERATION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_intent_generation.v1.schema.json"
)
TRADE_SUBMIT_READINESS_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json"
)
REPLAY_CERTIFICATION_GATE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/replay_certification_gate.v1.schema.json"
)
REPLAY_CERTIFICATION_BUNDLE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/replay_certification_bundle.v1.schema.json"
)
COMPILED_ACTIVE_CONFIG_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/compiled_active_config.v1.schema.json"
)
CONFIGURATION_STATE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_state.v1.schema.json"
)
POLICY_EVOLUTION_STATE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/policy_evolution_state.v1.schema.json"
)
PAPER_POLICY_VERDICT_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_policy_verdict.v1.schema.json"
)
PRODUCTION_POLICY_VERDICT_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/production_policy_verdict.v1.schema.json"
)
CAPITAL_RISK_ENVELOPE_V2_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json"
)
RECURRENCE_KILL_GATE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/recurrence_kill_gate.v1.schema.json"
)
CONTROL_PLANE_OPERATOR_STATUS_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_plane_operator_status.v1.schema.json"
)
TRANSITION_TIMELINE_PROJECTION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/transition_timeline_projection.v1.schema.json"
)
DAY_ACTIVATION_BUILD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/day_activation_build.v1.schema.json"
DAY_ACTIVATION_PACKAGE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/CONTEXT/day_activation_package.v1.schema.json"
GLOBAL_CONTEXT_BUILD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/global_context_build.v1.schema.json"
GLOBAL_CONTEXT_PACKAGE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/CONTEXT/global_context_package.v1.schema.json"
SESSION_PROMOTION_DECISION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/session_promotion_decision.v1.schema.json"
)
EXECUTION_BUILD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_build.v1.schema.json"
EXECUTION_PACKAGE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_package.v1.schema.json"
RUNTIME_CONTROL_RECORD_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_record.v1.schema.json"
)
RUNTIME_CONTROL_DECISION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_decision.v1.schema.json"
)
RUNTIME_CONTROL_RUN_ENVELOPE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_run_envelope.v1.schema.json"
)
GATE_STACK_VERDICT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/gate_stack_verdict.v1.schema.json"
PLATFORM_READINESS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/READINESS/platform_readiness.v1.schema.json"
EXECUTION_RECONCILIATION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_reconciliation.v1.schema.json"
)
DAY_AUTHORITY_DECISION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/day_authority_decision.v1.schema.json"
)
PRE_OPEN_BUNDLE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/pre_open_bundle.v1.schema.json"
PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_startup_intent_input_convergence.v1.schema.json"
)
PAPER_STARTUP_AUTHORIZATION_CONVERGENCE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_startup_authorization_convergence.v1.schema.json"
)
STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization_input_convergence.v1.schema.json"
)
BOD_EXECUTION_ENVIRONMENT_PROOF_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/bod_execution_environment_proof.v1.schema.json"
)
PHASEC_RISK_INPUTS_PREP_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/phasec_risk_inputs_prep.v1.schema.json"
)
CAPABILITY_STATE_V1_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/capability_state.v1.schema.json"
INTENTS_DAY_COMPLETENESS_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/intents_day_completeness.v1.schema.json"
)
DEPLOYMENT_STATE_MACHINE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/deployment_state_machine.v1.schema.json"
)
GLOBAL_KILL_SWITCH_STATE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RISK/global_kill_switch_state.v1.schema.json"
)
AUTHORIZATION_GATE_VERDICT_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/authorization_gate_verdict.v1.schema.json"
)
SLEEVE_LIVE_READINESS_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/READINESS/sleeve_live_readiness.v1.schema.json"
)
BUG_METRICS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/READINESS/bug_metrics.v1.schema.json"
POLICY_SNAPSHOT_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_policy_snapshot.v1.schema.json"
)
VALIDATION_RESULT_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_validation_result.v1.schema.json"
)
COMPILE_RESULT_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_compile_result.v1.schema.json"
)
REVIEW_DIFF_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_review_diff.v1.schema.json"
)
ACTIVATION_TRANSACTION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_activation_transaction.v1.schema.json"
)
RUNTIME_TRUTH_INTEGRITY_RESULT_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/runtime_truth_integrity_result.v1.schema.json"
)
ALERTS_PROJECTION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/alerts_projection.v1.schema.json"
CURRENT_SYSTEM_PROJECTION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/current_system_projection.v1.schema.json"
)

READ_KIND_JSON = "validated_json"
READ_KIND_COLLECTION = "validated_collection"
READ_KIND_POINTER = "pointer_json"
READ_KIND_SEMANTIC = "semantic_view"


@dataclass(frozen=True)
class ControlPlaneReadRefV1:
    domain: str
    surface: str
    read_kind: str
    path: Path
    payload: Dict[str, Any]
    sha256: str
    schema_relpath: str | None
    metadata: Dict[str, Any]


@dataclass(frozen=True)
class ControlPlaneReadCollectionV1:
    domain: str
    surface: str
    read_kind: str
    refs: tuple[ControlPlaneReadRefV1, ...]
    metadata: Dict[str, Any]


@dataclass(frozen=True)
class ControlPlaneSemanticViewV1:
    domain: str
    surface: str
    read_kind: str
    refs: tuple[ControlPlaneReadRefV1, ...]
    payload: Dict[str, Any]
    metadata: Dict[str, Any]


def _read_json_object(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"CONTROL_PLANE_READ_JSON_FAILED:path={path}:err={type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"CONTROL_PLANE_READ_JSON_NOT_OBJECT:path={path}")
    return payload


def _sha256_payload(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(dict(payload)) + b"\n").hexdigest()


def _read_validated_json(
    *,
    domain: str,
    surface: str,
    path: Path,
    schema_relpath: str | None,
    metadata: Mapping[str, Any] | None = None,
) -> ControlPlaneReadRefV1:
    resolved = Path(path).resolve()
    payload = _read_json_object(resolved)
    if schema_relpath:
        validate_against_repo_schema_v1(payload, REPO_ROOT, schema_relpath)
    return ControlPlaneReadRefV1(
        domain=domain,
        surface=surface,
        read_kind=READ_KIND_JSON,
        path=resolved,
        payload=payload,
        sha256=_sha256_payload(payload),
        schema_relpath=schema_relpath,
        metadata=dict(metadata or {}),
    )


def _validated_ref_from_payload(
    *,
    domain: str,
    surface: str,
    path: Path,
    payload: Mapping[str, Any],
    schema_relpath: str | None,
    metadata: Mapping[str, Any] | None = None,
) -> ControlPlaneReadRefV1:
    if schema_relpath:
        validate_against_repo_schema_v1(dict(payload), REPO_ROOT, schema_relpath)
    return ControlPlaneReadRefV1(
        domain=domain,
        surface=surface,
        read_kind=READ_KIND_JSON,
        path=Path(path).resolve(),
        payload=dict(payload),
        sha256=_sha256_payload(payload),
        schema_relpath=schema_relpath,
        metadata=dict(metadata or {}),
    )


def _require_day(day_utc: str | None, *, surface: str) -> str:
    day = str(day_utc or "").strip()
    if not day:
        raise ValueError(f"CONTROL_PLANE_READ_DAY_REQUIRED:surface={surface}")
    return day


def _prior_day(day_utc: str) -> str:
    from datetime import date, timedelta

    return (date.fromisoformat(str(day_utc).strip()) - timedelta(days=1)).isoformat()


def _require_scope_id(scope_id: str | None, *, surface: str) -> str:
    value = str(scope_id or "").strip()
    if not value:
        raise ValueError(f"CONTROL_PLANE_READ_SCOPE_ID_REQUIRED:surface={surface}")
    return value


def _require_context_hash(context_hash: str | None, *, surface: str) -> str:
    value = str(context_hash or "").strip()
    if not value:
        raise ValueError(f"CONTROL_PLANE_READ_CONTEXT_HASH_REQUIRED:surface={surface}")
    return value


def _require_submission_id(submission_id: str | None, *, surface: str) -> str:
    value = str(submission_id or "").strip()
    if not value:
        raise ValueError(f"CONTROL_PLANE_READ_SUBMISSION_ID_REQUIRED:surface={surface}")
    return value


def _require_release_root(release_root: Path | str | None, *, surface: str) -> Path:
    if release_root is None or not str(release_root).strip():
        raise ValueError(f"CONTROL_PLANE_READ_RELEASE_ROOT_REQUIRED:surface={surface}")
    return Path(release_root).expanduser().resolve()


def _global_truth_root(truth_root: Path | str | None) -> Path:
    if truth_root is None or not str(truth_root).strip():
        raise ValueError("CONTROL_PLANE_READ_GLOBAL_TRUTH_ROOT_REQUIRED")
    return Path(truth_root).expanduser().resolve()


def _sleeve_truth_root(
    truth_sleeves_root: Path | str | None,
    *,
    sleeve_id: str = "PRIMARY",
    environment: str = "PAPER",
) -> Path:
    if truth_sleeves_root is None or not str(truth_sleeves_root).strip():
        raise ValueError("CONTROL_PLANE_READ_TRUTH_SLEEVES_ROOT_REQUIRED")
    return (
        Path(truth_sleeves_root).expanduser().resolve()
        / str(sleeve_id).strip()
        / str(environment).strip().upper()
    ).resolve()


def _published_configuration_state_path(*, truth_root: Path) -> Path:
    return (truth_root / "configuration_state_v1" / "current.json").resolve()


def _latest_compiled_active_config_path(*, truth_root: Path) -> Path:
    family_root = (truth_root / "compiled_active_config_v1").resolve()
    candidates = sorted(family_root.rglob("compiled_active_config.v1.json"))
    if not candidates:
        raise ValueError(f"CONTROL_PLANE_READ_MISSING_COMPILED_ACTIVE_CONFIG:root={family_root}")
    return candidates[-1].resolve()


def _active_session_path(*, truth_root: Path) -> Path:
    return (truth_root / "active_session_v1" / "current.json").resolve()


def _session_authority_status_path(*, truth_root: Path) -> Path:
    return (truth_root / "session_authority_status_v1" / "current.json").resolve()


def _session_authority_alert_path(*, truth_root: Path) -> Path:
    return (truth_root / "session_authority_alert_v1" / "current.json").resolve()


def _market_calendar_coverage_status_path(*, truth_root: Path) -> Path:
    return (truth_root / "market_calendar_coverage_status_v1" / "current.json").resolve()


def _market_calendar_family_root(*, truth_root: Path) -> Path:
    return (truth_root / "market_calendar_v1").resolve()


def _target_day_build_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "target_day_build_v1" / str(day_utc).strip()).with_suffix(".json").resolve()


def _target_day_admission_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "target_day_admission_v1" / str(day_utc).strip()).with_suffix(".json").resolve()


def _session_promotion_decision_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "session_promotion_decision_v1"
        / str(day_utc).strip()
        / "session_promotion_decision.v1.json"
    ).resolve()


def _day_activation_build_path(*, truth_root: Path, day_utc: str, context_hash: str) -> Path:
    return (
        truth_root
        / "reports"
        / "day_activation_build_v1"
        / str(day_utc).strip()
        / str(context_hash).strip()
        / "day_activation_build.v1.json"
    ).resolve()


def _day_activation_package_path(*, truth_root: Path, day_utc: str, context_hash: str) -> Path:
    return (
        truth_root
        / "day_activation_package_v1"
        / str(day_utc).strip()
        / str(context_hash).strip()
        / "day_activation_package.v1.json"
    ).resolve()


def _global_context_build_path(*, truth_root: Path, day_utc: str, context_hash: str) -> Path:
    return (
        truth_root
        / "reports"
        / "global_context_build_v1"
        / str(day_utc).strip()
        / str(context_hash).strip()
        / "global_context_build.v1.json"
    ).resolve()


def _global_context_package_path(*, truth_root: Path, day_utc: str, context_hash: str) -> Path:
    return (
        truth_root
        / "global_context_package_v1"
        / str(day_utc).strip()
        / str(context_hash).strip()
        / "global_context_package.v1.json"
    ).resolve()


def _execution_build_path(*, truth_root: Path, day_utc: str, submission_id: str) -> Path:
    return (
        truth_root
        / "reports"
        / "execution_build_v1"
        / str(day_utc).strip()
        / str(submission_id).strip()
        / "execution_build.v1.json"
    ).resolve()


def _execution_package_path(*, truth_root: Path, day_utc: str, submission_id: str) -> Path:
    return (
        truth_root
        / "execution_package_v1"
        / str(day_utc).strip()
        / str(submission_id).strip()
        / "execution_package.v1.json"
    ).resolve()


def _submit_boundary_status_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "submit_boundary_status_v1" / str(day_utc).strip() / "submit_boundary_status.v1.json"
    ).resolve()


def _paper_session_ledger_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "paper_session_ledger_v1" / str(day_utc).strip() / "paper_session_ledger.v1.json"
    ).resolve()


def _paper_policy_verdict_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "paper_policy_verdict_v1" / str(day_utc).strip() / "paper_policy_verdict.v1.json"
    ).resolve()


def _production_policy_verdict_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "production_policy_verdict_v1"
        / str(day_utc).strip()
        / "production_policy_verdict.v1.json"
    ).resolve()


def _capital_risk_envelope_v2_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "capital_risk_envelope_v2"
        / str(day_utc).strip()
        / "capital_risk_envelope.v2.json"
    ).resolve()


def _recurrence_kill_gate_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "recurrence_kill_gate_v1"
        / str(day_utc).strip()
        / "recurrence_kill_gate.v1.json"
    ).resolve()


def _paper_session_evidence_manifest_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "paper_session_evidence_manifest_v1"
        / str(day_utc).strip()
        / "paper_session_evidence_manifest.v1.json"
    ).resolve()


def _paper_session_kernel_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "paper_session_kernel_v1" / str(day_utc).strip() / "paper_session_kernel.v1.json"
    ).resolve()


def _paper_day_control_plane_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "paper_day_control_plane_v1" / str(day_utc).strip() / "paper_day_control_plane.v1.json"
    ).resolve()


def _paper_trading_posture_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "paper_trading_posture_v1" / str(day_utc).strip() / "paper_trading_posture.v1.json"
    ).resolve()


def _sleeve_rollup_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "sleeve_rollup_v1" / str(day_utc).strip() / "sleeve_rollup.v1.json").resolve()


def _startup_materialization_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "startup_materialization_v1" / str(day_utc).strip() / "startup_materialization.v1.json"
    ).resolve()


def _startup_proof_validation_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "startup_proof_validation_v1" / str(day_utc).strip() / "startup_proof_validation.v1.json"
    ).resolve()


def _economic_state_build_family_root(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "economic_state_build_v1" / str(day_utc).strip()).resolve()


def _economic_state_build_path(*, truth_root: Path, day_utc: str, context_hash: str) -> Path:
    return (
        _economic_state_build_family_root(truth_root=truth_root, day_utc=day_utc)
        / str(context_hash).strip()
        / "economic_state_build.v1.json"
    ).resolve()


def _economic_state_package_family_root(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "economic_state_package_v1" / str(day_utc).strip()).resolve()


def _economic_state_package_path(*, truth_root: Path, day_utc: str, context_hash: str) -> Path:
    return (
        _economic_state_package_family_root(truth_root=truth_root, day_utc=day_utc)
        / str(context_hash).strip()
        / "economic_state_package.v1.json"
    ).resolve()


def _trading_day_control_plane_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "trading_day_control_plane_v1" / str(day_utc).strip() / "trading_day_control_plane.v1.json"
    ).resolve()


def _trading_day_execution_control_plane_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "trading_day_execution_control_plane_v1"
        / str(day_utc).strip()
        / "trading_day_execution_control_plane.v1.json"
    ).resolve()


def _trading_day_state_machine_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "trading_day_state_machine_v1" / str(day_utc).strip() / "trading_day_state_machine.v1.json"
    ).resolve()


def _trading_day_intent_generation_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "trading_day_intent_generation_v1"
        / str(day_utc).strip()
        / "trading_day_intent_generation.v1.json"
    ).resolve()


def _replay_certification_gate_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "replay_certification_gate_v1" / str(day_utc).strip() / "replay_certification_gate.v1.json"
    ).resolve()


def _replay_certification_bundle_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "replay_certification_bundle_v1" / str(day_utc).strip() / "replay_certification_bundle.v1.json"
    ).resolve()


def _control_plane_operator_status_path(*, truth_root: Path, day_utc: str, scope_id: str) -> Path:
    return (
        truth_root
        / "reports"
        / "control_plane_operator_status_v1"
        / str(day_utc).strip()
        / str(scope_id).strip()
        / "control_plane_operator_status.v1.json"
    ).resolve()


def _transition_timeline_projection_path(*, truth_root: Path, day_utc: str, scope_id: str) -> Path:
    return (
        truth_root
        / "reports"
        / "transition_timeline_projection_v1"
        / str(day_utc).strip()
        / str(scope_id).strip()
        / "transition_timeline_projection.v1.json"
    ).resolve()


def _alerts_projection_primary_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "alerts_projection_v1" / str(day_utc).strip() / "alerts_projection.v1.json").resolve()


def _alerts_projection_reports_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "alerts_projection_v1" / str(day_utc).strip() / "alerts_projection.v1.json"
    ).resolve()


def _current_system_projection_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "current_system_projection_v1" / str(day_utc).strip() / "current_system_projection.v1.json"
    ).resolve()


def _runtime_truth_integrity_latest_path(*, truth_root: Path) -> Path:
    family_root = (truth_root / "reports" / "runtime_truth_integrity_result_v1").resolve()
    candidates = sorted(family_root.glob("*/runtime_truth_integrity_result.v1.json"))
    if not candidates:
        raise ValueError(f"CONTROL_PLANE_READ_MISSING_RUNTIME_TRUTH_INTEGRITY:root={family_root}")
    return candidates[-1].resolve()


def _policy_evolution_state_day_paths(*, truth_root: Path, day_utc: str) -> List[Path]:
    day_root = (truth_root / "reports" / "policy_evolution_state_v1" / str(day_utc).strip()).resolve()
    if not day_root.exists() or not day_root.is_dir():
        return []
    return sorted(day_root.glob("*/*/policy_evolution_state.v1.json"))


def _trade_submit_readiness_path(
    *,
    truth_root: Path,
    day_utc: str,
    ib_account: str,
    environment: str,
) -> Path:
    env = str(environment or "").strip().upper()
    account = str(ib_account or "").strip()
    history = (
        truth_root / "trade_submit_readiness_c2_v1" / "_history" / env / account / str(day_utc).strip() / "status.json"
    ).resolve()
    current = (truth_root / "trade_submit_readiness_c2_v1" / env / account / "status.json").resolve()
    return history if history.exists() and history.is_file() else current


def _runtime_control_kernel_root(*, truth_root: Path) -> Path:
    return (truth_root / "runtime_control_kernel_v1").resolve()


def _runtime_control_record_family_root(*, truth_root: Path) -> Path:
    return (_runtime_control_kernel_root(truth_root=truth_root) / "records").resolve()


def _runtime_control_decision_family_root(*, truth_root: Path) -> Path:
    return (_runtime_control_kernel_root(truth_root=truth_root) / "decisions").resolve()


def _runtime_control_run_envelope_family_root(*, truth_root: Path) -> Path:
    return (_runtime_control_kernel_root(truth_root=truth_root) / "run_envelopes").resolve()


def _gate_stack_verdict_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "gate_stack_verdict_v1" / str(day_utc).strip() / "gate_stack_verdict.v1.json"
    ).resolve()


def _platform_readiness_family_root(*, truth_root: Path) -> Path:
    return (truth_root / "readiness_v1" / "constellation_platform_readiness_v1").resolve()


def _platform_readiness_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        _platform_readiness_family_root(truth_root=truth_root)
        / str(day_utc).strip()
        / "constellation_platform_readiness.v1.json"
    ).resolve()


def _platform_readiness_pointer_path(*, truth_root: Path) -> Path:
    return (_platform_readiness_family_root(truth_root=truth_root) / "latest_pointer.v1.json").resolve()


def _execution_reconciliation_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "execution_reconciliation_v1"
        / str(day_utc).strip()
        / "execution_reconciliation.v1.json"
    ).resolve()


def _day_authority_decision_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "day_authority_decision_v1" / str(day_utc).strip() / "day_authority_decision.v1.json"
    ).resolve()


def _pre_open_bundle_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "pre_open_bundle_v1" / str(day_utc).strip() / "pre_open_bundle.v1.json"
    ).resolve()


def _paper_startup_intent_input_convergence_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "paper_startup_intent_input_convergence_v1"
        / str(day_utc).strip()
        / "paper_startup_intent_input_convergence.v1.json"
    ).resolve()


def _paper_startup_authorization_convergence_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "paper_startup_authorization_convergence_v1"
        / str(day_utc).strip()
        / "paper_startup_authorization_convergence.v1.json"
    ).resolve()


def _startup_materialization_input_convergence_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "startup_materialization_input_convergence_v1"
        / str(day_utc).strip()
        / "startup_materialization_input_convergence.v1.json"
    ).resolve()


def _bod_execution_environment_proof_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "bod_execution_environment_proof_v1"
        / str(day_utc).strip()
        / "bod_execution_environment_proof.v1.json"
    ).resolve()


def _phasec_risk_inputs_prep_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "phasec_risk_inputs_prep_v1"
        / str(day_utc).strip()
        / "phasec_risk_inputs_prep.v1.json"
    ).resolve()


def _capability_state_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "capability_state_v1" / str(day_utc).strip() / "capability_state.v1.json"
    ).resolve()


def _intents_day_completeness_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "intents_day_completeness_v1"
        / str(day_utc).strip()
        / "intents_day_completeness.v1.json"
    ).resolve()


def _deployment_state_machine_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "deployment_state_machine_v1"
        / str(day_utc).strip()
        / "deployment_state_machine.v1.json"
    ).resolve()


def _global_kill_switch_state_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "risk_v1" / "kill_switch_v1" / str(day_utc).strip() / "global_kill_switch_state.v1.json"
    ).resolve()


def _day_start_blocked_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "day_start_blocked_v1" / str(day_utc).strip() / "day_start_blocked.v1.json"
    ).resolve()


def _trading_day_state_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root / "reports" / "trading_day_state_v1" / str(day_utc).strip() / "trading_day_state.v1.json"
    ).resolve()


def _bug_metrics_family_root(*, truth_root: Path) -> Path:
    return (truth_root / "readiness_v1" / "constellation_bug_metrics_v1").resolve()


def _platform_bug_metrics_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        _bug_metrics_family_root(truth_root=truth_root)
        / str(day_utc).strip()
        / "constellation_bug_metrics.v1.json"
    ).resolve()


def _platform_bug_metrics_pointer_path(*, truth_root: Path) -> Path:
    return (_bug_metrics_family_root(truth_root=truth_root) / "latest_pointer.v1.json").resolve()


def _active_runtime_contract_path() -> Path:
    return ACTIVE_RUNTIME_CONTRACT_PATH


def _release_manifest_active_path() -> Path:
    contract = _read_validated_json(
        domain="release",
        surface="active_runtime_contract",
        path=_active_runtime_contract_path(),
        schema_relpath=ACTIVE_RUNTIME_CONTRACT_SCHEMA,
    )
    release_root = Path(str(contract.payload.get("release_root") or "")).expanduser().resolve()
    if not str(release_root).strip():
        raise ValueError("CONTROL_PLANE_READ_RELEASE_ROOT_MISSING")
    return (release_root / "release_manifest.v1.json").resolve()


def _release_manifest_for_release_root_path(*, release_root: Path) -> Path:
    return (Path(release_root).resolve() / "release_manifest.v1.json").resolve()


def _ib_api_handshake_pointer_path(*, truth_root: Path) -> Path:
    return (truth_root / "ib_api_handshake" / "latest_pointer.v1.json").resolve()


def _read_pointer_target_ref(*, domain: str, surface: str, truth_root: Path) -> ControlPlaneReadRefV1:
    pointer_path = _ib_api_handshake_pointer_path(truth_root=truth_root)
    pointer_ref = _read_validated_json(
        domain=domain,
        surface="ib_api_handshake_latest_pointer",
        path=pointer_path,
        schema_relpath=None,
        metadata={"truth_root": str(truth_root)},
    )
    target_path = None
    for key in ("artifact_path", "path", "target_path"):
        value = str(pointer_ref.payload.get(key) or "").strip()
        if value:
            target_path = Path(value).expanduser().resolve()
            break
    if target_path is None:
        pointers = pointer_ref.payload.get("pointers")
        if isinstance(pointers, Mapping):
            value = str(pointers.get("snapshot_path") or "").strip()
            if value:
                target_path = Path(value).expanduser().resolve()
    if target_path is None:
        raise ValueError("CONTROL_PLANE_READ_POINTER_TARGET_MISSING:surface=ib_api_handshake_latest_pointer")
    return _read_validated_json(
        domain=domain,
        surface=surface,
        path=target_path,
        schema_relpath=None,
        metadata={
            "truth_root": str(truth_root),
            "pointer_path": str(pointer_ref.path),
            "pointer_sha256": pointer_ref.sha256,
        },
    )


def _ref_row(ref: ControlPlaneReadRefV1) -> Dict[str, Any]:
    return {
        "domain": ref.domain,
        "surface": ref.surface,
        "path": str(ref.path),
        "sha256": ref.sha256,
        "schema_relpath": ref.schema_relpath,
    }


def _parse_iso_sort_value(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def _latest_candidate_key(payload: Mapping[str, Any], *, fields: Sequence[str]) -> tuple[datetime, ...]:
    return tuple(_parse_iso_sort_value(payload.get(field)) for field in fields)


def _read_latest_validated_json(
    *,
    domain: str,
    surface: str,
    root: Path,
    pattern: str,
    schema_relpath: str,
    selection_fields: Sequence[str],
    metadata: Mapping[str, Any] | None = None,
) -> ControlPlaneReadRefV1:
    resolved_root = Path(root).resolve()
    if not resolved_root.exists() or not resolved_root.is_dir():
        raise ValueError(f"CONTROL_PLANE_READ_LATEST_ROOT_MISSING:surface={surface}:root={resolved_root}")
    candidates: list[tuple[tuple[datetime, ...], Path, Dict[str, Any]]] = []
    for path in sorted(resolved_root.rglob(pattern)):
        try:
            payload = _read_json_object(path)
            validate_against_repo_schema_v1(payload, REPO_ROOT, schema_relpath)
        except Exception:
            continue
        candidates.append((_latest_candidate_key(payload, fields=selection_fields), path.resolve(), payload))
    if not candidates:
        raise ValueError(f"CONTROL_PLANE_READ_LATEST_CANDIDATE_MISSING:surface={surface}:root={resolved_root}")
    highest_key = max(item[0] for item in candidates)
    highest = [item for item in candidates if item[0] == highest_key]
    if len(highest) != 1:
        raise ValueError(
            f"CONTROL_PLANE_READ_AMBIGUOUS_LATEST:surface={surface}:count={len(highest)}:root={resolved_root}"
        )
    _, chosen_path, chosen_payload = highest[0]
    return _validated_ref_from_payload(
        domain=domain,
        surface=surface,
        path=chosen_path,
        payload=chosen_payload,
        schema_relpath=schema_relpath,
        metadata={
            **dict(metadata or {}),
            "selection_fields": list(selection_fields),
            "selection_root": str(resolved_root),
            "selection_pattern": pattern,
        },
    )


def _read_day_context_validated_json(
    *,
    domain: str,
    surface: str,
    family_root: Path,
    filename: str,
    schema_relpath: str,
    context_hash: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> ControlPlaneReadRefV1:
    resolved_root = Path(family_root).resolve()
    if context_hash is not None and str(context_hash).strip():
        requested_context = str(context_hash).strip()
        ref = _read_validated_json(
            domain=domain,
            surface=surface,
            path=(resolved_root / requested_context / filename).resolve(),
            schema_relpath=schema_relpath,
            metadata={
                **dict(metadata or {}),
                "selection_mode": "EXACT_CONTEXT_HASH",
                "context_hash": requested_context,
            },
        )
        observed_context = str(ref.payload.get("context_hash") or "").strip()
        if observed_context and observed_context != requested_context:
            raise ValueError(
                "CONTROL_PLANE_READ_CONTEXT_HASH_MISMATCH:"
                f"surface={surface}:expected={requested_context}:observed={observed_context}"
            )
        return ref
    if not resolved_root.exists() or not resolved_root.is_dir():
        raise ValueError(f"CONTROL_PLANE_READ_DAY_CONTEXT_ROOT_MISSING:surface={surface}:root={resolved_root}")
    candidates = sorted(resolved_root.glob(f"*/{filename}"))
    if not candidates:
        raise ValueError(f"CONTROL_PLANE_READ_DAY_CONTEXT_CANDIDATE_MISSING:surface={surface}:root={resolved_root}")
    if len(candidates) != 1:
        raise ValueError(
            f"CONTROL_PLANE_READ_DAY_CONTEXT_AMBIGUOUS:surface={surface}:count={len(candidates)}:root={resolved_root}"
        )
    return _read_validated_json(
        domain=domain,
        surface=surface,
        path=candidates[0].resolve(),
        schema_relpath=schema_relpath,
        metadata={
            **dict(metadata or {}),
            "selection_mode": "ONLY_CANDIDATE_IN_DAY",
            "selection_root": str(resolved_root),
        },
    )


def _read_market_calendar_day_ref(*, truth_root: Path, day_utc: str) -> ControlPlaneReadRefV1:
    family_root = _market_calendar_family_root(truth_root=truth_root)
    manifest_path = (family_root / "dataset_manifest.json").resolve()
    if not manifest_path.exists() or not manifest_path.is_file():
        raise ValueError(f"CONTROL_PLANE_READ_MARKET_CALENDAR_MANIFEST_MISSING:surface=market_calendar_day:path={manifest_path}")
    manifest = _read_json_object(manifest_path)
    files = manifest.get("files")
    if not isinstance(files, list):
        raise ValueError(
            f"CONTROL_PLANE_READ_MARKET_CALENDAR_MANIFEST_INVALID:surface=market_calendar_day:path={manifest_path}"
        )
    year_path: Path | None = None
    for entry in files:
        if not isinstance(entry, Mapping):
            continue
        try:
            if int(entry.get("year") or -1) != int(str(day_utc).strip()[0:4]):
                continue
        except Exception:
            continue
        rel = str(entry.get("file") or "").strip()
        if rel:
            year_path = (family_root / rel).resolve()
            break
    if year_path is None or not year_path.exists() or not year_path.is_file():
        raise ValueError(
            f"CONTROL_PLANE_READ_MARKET_CALENDAR_YEAR_MISSING:surface=market_calendar_day:day={day_utc}:path={year_path}"
        )
    try:
        lines = year_path.read_text(encoding="utf-8").splitlines()
    except Exception as exc:
        raise ValueError(
            f"CONTROL_PLANE_READ_MARKET_CALENDAR_YEAR_UNREADABLE:surface=market_calendar_day:path={year_path}:err={type(exc).__name__}"
        ) from exc
    for line in lines:
        if not str(line).strip():
            continue
        try:
            row = json.loads(line)
        except Exception as exc:
            raise ValueError(
                f"CONTROL_PLANE_READ_MARKET_CALENDAR_ROW_INVALID:surface=market_calendar_day:path={year_path}:err={type(exc).__name__}"
            ) from exc
        if not isinstance(row, dict):
            raise ValueError(f"CONTROL_PLANE_READ_MARKET_CALENDAR_ROW_NOT_OBJECT:surface=market_calendar_day:path={year_path}")
        if str(row.get("day_utc") or "").strip() != str(day_utc).strip():
            continue
        return _validated_ref_from_payload(
            domain="session",
            surface="market_calendar_day",
            path=year_path,
            payload=row,
            schema_relpath=MARKET_CALENDAR_DAY_SCHEMA,
            metadata={
                "truth_root": str(truth_root),
                "day_utc": str(day_utc).strip(),
                "manifest_path": str(manifest_path),
                "year_path": str(year_path),
                "selection_rule": "manifest_year_file_by_day_utc",
            },
        )
    raise ValueError(f"CONTROL_PLANE_READ_MARKET_CALENDAR_DAY_MISSING:surface=market_calendar_day:day={day_utc}")


def _read_platform_readiness_ref(*, truth_root: Path, day_utc: str) -> ControlPlaneReadRefV1:
    requested_path = _platform_readiness_path(truth_root=truth_root, day_utc=day_utc)
    pointer_path = _platform_readiness_pointer_path(truth_root=truth_root)
    if requested_path.exists() and requested_path.is_file():
        ref = _read_validated_json(
            domain="operator",
            surface="platform_readiness",
            path=requested_path,
            schema_relpath=PLATFORM_READINESS_SCHEMA,
            metadata={
                "truth_root": str(truth_root),
                "day_utc": str(day_utc).strip(),
                "requested_day_path": str(requested_path),
                "requested_day_present": True,
                "resolved_via_latest_pointer": False,
                "resolution_mode": "REQUESTED_DAY",
            },
        )
        return ControlPlaneReadRefV1(
            domain=ref.domain,
            surface=ref.surface,
            read_kind=ref.read_kind,
            path=ref.path,
            payload=ref.payload,
            sha256=ref.sha256,
            schema_relpath=ref.schema_relpath,
            metadata={**ref.metadata, "resolved_day": str(ref.payload.get("day_utc") or str(day_utc).strip())},
        )
    if not pointer_path.exists() or not pointer_path.is_file():
        raise ValueError(
            f"CONTROL_PLANE_READ_PLATFORM_READINESS_MISSING:surface=platform_readiness:requested_day={day_utc}:pointer={pointer_path}"
        )
    pointer = _read_json_object(pointer_path)
    target_text = ""
    for key in ("target_path", "artifact_path", "path"):
        value = str(pointer.get(key) or "").strip()
        if value:
            target_text = value
            break
    if not target_text:
        raise ValueError("CONTROL_PLANE_READ_PLATFORM_READINESS_POINTER_TARGET_MISSING:surface=platform_readiness")
    target_path = Path(target_text).expanduser().resolve()
    if not target_path.exists() or not target_path.is_file():
        raise ValueError(
            f"CONTROL_PLANE_READ_PLATFORM_READINESS_TARGET_MISSING:surface=platform_readiness:path={target_path}"
        )
    target_ref = _read_validated_json(
        domain="operator",
        surface="platform_readiness",
        path=target_path,
        schema_relpath=PLATFORM_READINESS_SCHEMA,
        metadata={
            "truth_root": str(truth_root),
            "day_utc": str(day_utc).strip(),
            "requested_day_path": str(requested_path),
            "requested_day_present": False,
            "resolved_via_latest_pointer": True,
            "latest_pointer_path": str(pointer_path),
            "resolution_mode": "LATEST_POINTER_FALLBACK",
        },
    )
    expected_sha = str(pointer.get("target_sha256") or "").strip().lower()
    verified = None
    if expected_sha:
        verified = hashlib.sha256(target_path.read_bytes()).hexdigest().lower() == expected_sha
        if not verified:
            raise ValueError(
                f"CONTROL_PLANE_READ_PLATFORM_READINESS_POINTER_SHA_MISMATCH:surface=platform_readiness:path={target_path}"
            )
    return ControlPlaneReadRefV1(
        domain=target_ref.domain,
        surface=target_ref.surface,
        read_kind=target_ref.read_kind,
        path=target_ref.path,
        payload=target_ref.payload,
        sha256=target_ref.sha256,
        schema_relpath=target_ref.schema_relpath,
        metadata={
            **target_ref.metadata,
            "resolved_day": str(target_ref.payload.get("day_utc") or ""),
            "latest_pointer_target_sha256": expected_sha or None,
            "latest_pointer_target_sha256_verified": verified,
        },
    )


def _resolve_surface_truth_root(
    truth_root: Path | str | None,
    truth_sleeves_root: Path | str | None,
    *,
    domain: str,
    surface: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
) -> tuple[Path, Dict[str, Any]]:
    wanted_sleeve = str(sleeve_id or "PRIMARY").strip().upper() or "PRIMARY"
    env = str(environment or "").strip().upper() or "PAPER"
    if truth_sleeves_root is not None and str(truth_sleeves_root).strip():
        root = _sleeve_truth_root(truth_sleeves_root, sleeve_id=wanted_sleeve, environment=env)
        return root, {
            "resolution_mode": "TRUTH_SLEEVES_ROOT",
            "sleeve_id": wanted_sleeve,
            "environment": env,
        }
    if str(ib_account or "").strip():
        from constellation_2.common.trade_submit_readiness_authority_v1 import (
            resolve_governed_sleeve_truth_bindings,
        )

        bindings = resolve_governed_sleeve_truth_bindings(
            repo_root=REPO_ROOT,
            environment=env,
            requested_ib_account=str(ib_account).strip(),
        )
        for binding in bindings:
            if str(binding.sleeve_id).strip().upper() == wanted_sleeve:
                return Path(binding.truth_root).resolve(), {
                    "resolution_mode": "GOVERNED_BINDING",
                    "sleeve_id": wanted_sleeve,
                    "environment": env,
                    "ib_account": str(ib_account).strip(),
                }
        raise ValueError(
            f"CONTROL_PLANE_READ_SLEEVE_BINDING_MISSING:domain={domain}:surface={surface}:sleeve_id={wanted_sleeve}"
        )
    if truth_root is not None and str(truth_root).strip():
        return _global_truth_root(truth_root), {
            "resolution_mode": "DIRECT_TRUTH_ROOT",
            "sleeve_id": wanted_sleeve,
            "environment": env,
        }
    raise ValueError(f"CONTROL_PLANE_READ_SURFACE_ROOT_REQUIRED:domain={domain}:surface={surface}")


def _read_global_kill_switch_state_ref(
    *,
    truth_root: Path,
    day_utc: str,
    truth_sleeves_root: Path | str | None = None,
) -> ControlPlaneReadRefV1:
    result = resolve_kill_switch_authority_v1(
        canonical_truth_root=truth_root,
        truth_sleeves_root=truth_sleeves_root,
        day_utc=day_utc,
    )
    if result.status != KILL_SWITCH_STATUS_PASS or not isinstance(result.payload, Mapping):
        raise ValueError(
            "CONTROL_PLANE_READ_GLOBAL_KILL_SWITCH_STATE_FAIL_CLOSED:"
            f"surface=global_kill_switch_state:reason={result.reason_code or result.status}"
        )
    payload = dict(result.payload)
    observed_day = str(payload.get("day_utc") or "").strip()
    if observed_day != str(day_utc).strip():
        raise ValueError(
            "CONTROL_PLANE_READ_GLOBAL_KILL_SWITCH_STATE_DAY_MISMATCH:"
            f"surface=global_kill_switch_state:expected={day_utc}:observed={observed_day}"
        )
    return _validated_ref_from_payload(
        domain="execution",
        surface="global_kill_switch_state",
        path=result.canonical_path,
        payload=payload,
        schema_relpath=GLOBAL_KILL_SWITCH_STATE_SCHEMA,
        metadata={
            "truth_root": str(truth_root),
            "day_utc": str(day_utc).strip(),
            "canonical_file_sha256": result.canonical_sha256,
            "sleeve_present": result.sleeve_present,
            "sleeve_paths": [str(path) for path in result.sleeve_paths],
        },
    )


def _read_authorization_gate_verdict_ref(
    *,
    truth_root: Path | str | None,
    truth_sleeves_root: Path | str | None,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
) -> ControlPlaneReadRefV1:
    scope_root, scope_meta = _resolve_surface_truth_root(
        truth_root,
        truth_sleeves_root,
        domain="execution",
        surface="authorization_gate_verdict",
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
    )
    requested_path = (
        scope_root / "reports" / "authorization_gate_verdict_v1" / str(day_utc).strip() / "authorization_gate_verdict.v1.json"
    ).resolve()
    target_path = requested_path
    head_path = (scope_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()
    resolved_via_head = False
    if head_path.exists() and head_path.is_file():
        head = _read_json_object(head_path)
        head_day = str(head.get("day_utc") or "").strip()
        if head_day == str(day_utc).strip():
            candidate_raw = str(head.get("points_to") or "").strip()
            if not candidate_raw:
                raise ValueError(
                    "CONTROL_PLANE_READ_AUTHORIZATION_GATE_HEAD_TARGET_MISSING:"
                    f"surface=authorization_gate_verdict:path={head_path}"
                )
            candidate_path = Path(candidate_raw).expanduser().resolve()
            if "authorization_gate_verdict_v1" not in str(candidate_path):
                raise ValueError(
                    "CONTROL_PLANE_READ_AUTHORIZATION_GATE_HEAD_INVALID:"
                    f"surface=authorization_gate_verdict:path={head_path}:target={candidate_path}"
                )
            if not candidate_path.exists() or not candidate_path.is_file():
                raise ValueError(
                    "CONTROL_PLANE_READ_AUTHORIZATION_GATE_HEAD_TARGET_MISSING:"
                    f"surface=authorization_gate_verdict:path={candidate_path}"
                )
            target_path = candidate_path
            resolved_via_head = True
    ref = _read_validated_json(
        domain="execution",
        surface="authorization_gate_verdict",
        path=target_path,
        schema_relpath=AUTHORIZATION_GATE_VERDICT_SCHEMA,
        metadata={
            **scope_meta,
            "requested_day_path": str(requested_path),
            "head_path": str(head_path),
            "resolved_via_head": resolved_via_head,
        },
    )
    observed_day = str(ref.payload.get("day_utc") or "").strip()
    if observed_day != str(day_utc).strip():
        raise ValueError(
            "CONTROL_PLANE_READ_AUTHORIZATION_GATE_DAY_MISMATCH:"
            f"surface=authorization_gate_verdict:expected={day_utc}:observed={observed_day}"
        )
    return ref


def _read_platform_bug_metrics_ref(*, truth_root: Path, day_utc: str) -> ControlPlaneReadRefV1:
    requested_path = _platform_bug_metrics_path(truth_root=truth_root, day_utc=day_utc)
    pointer_path = _platform_bug_metrics_pointer_path(truth_root=truth_root)
    if requested_path.exists() and requested_path.is_file():
        ref = _read_validated_json(
            domain="platform",
            surface="platform_bug_metrics",
            path=requested_path,
            schema_relpath=BUG_METRICS_SCHEMA,
            metadata={
                "truth_root": str(truth_root),
                "day_utc": str(day_utc).strip(),
                "requested_day_path": str(requested_path),
                "requested_day_present": True,
                "resolved_via_latest_pointer": False,
                "resolution_mode": "REQUESTED_DAY",
            },
        )
        return ControlPlaneReadRefV1(
            domain=ref.domain,
            surface=ref.surface,
            read_kind=ref.read_kind,
            path=ref.path,
            payload=ref.payload,
            sha256=ref.sha256,
            schema_relpath=ref.schema_relpath,
            metadata={**ref.metadata, "resolved_day": str(ref.payload.get("day_utc") or str(day_utc).strip())},
        )
    if not pointer_path.exists() or not pointer_path.is_file():
        raise ValueError(
            f"CONTROL_PLANE_READ_PLATFORM_BUG_METRICS_MISSING:surface=platform_bug_metrics:requested_day={day_utc}:pointer={pointer_path}"
        )
    pointer = _read_json_object(pointer_path)
    target_text = ""
    for key in ("target_path", "artifact_path", "path"):
        value = str(pointer.get(key) or "").strip()
        if value:
            target_text = value
            break
    if not target_text:
        raise ValueError("CONTROL_PLANE_READ_PLATFORM_BUG_METRICS_POINTER_TARGET_MISSING:surface=platform_bug_metrics")
    target_path = Path(target_text).expanduser().resolve()
    if not target_path.exists() or not target_path.is_file():
        raise ValueError(
            f"CONTROL_PLANE_READ_PLATFORM_BUG_METRICS_TARGET_MISSING:surface=platform_bug_metrics:path={target_path}"
        )
    target_ref = _read_validated_json(
        domain="platform",
        surface="platform_bug_metrics",
        path=target_path,
        schema_relpath=BUG_METRICS_SCHEMA,
        metadata={
            "truth_root": str(truth_root),
            "day_utc": str(day_utc).strip(),
            "requested_day_path": str(requested_path),
            "requested_day_present": False,
            "resolved_via_latest_pointer": True,
            "latest_pointer_path": str(pointer_path),
            "resolution_mode": "LATEST_POINTER_FALLBACK",
        },
    )
    expected_sha = str(pointer.get("target_sha256") or "").strip().lower()
    verified = None
    if expected_sha:
        verified = hashlib.sha256(target_path.read_bytes()).hexdigest().lower() == expected_sha
        if not verified:
            raise ValueError(
                f"CONTROL_PLANE_READ_PLATFORM_BUG_METRICS_POINTER_SHA_MISMATCH:surface=platform_bug_metrics:path={target_path}"
            )
    return ControlPlaneReadRefV1(
        domain=target_ref.domain,
        surface=target_ref.surface,
        read_kind=target_ref.read_kind,
        path=target_ref.path,
        payload=target_ref.payload,
        sha256=target_ref.sha256,
        schema_relpath=target_ref.schema_relpath,
        metadata={
            **target_ref.metadata,
            "resolved_day": str(target_ref.payload.get("day_utc") or ""),
            "latest_pointer_target_sha256": expected_sha or None,
            "latest_pointer_target_sha256_verified": verified,
        },
    )


def read_control_plane_surface_v1(
    *,
    domain: str,
    surface: str,
    truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    day_utc: str | None = None,
    sleeve_id: str = "PRIMARY",
    environment: str = "PAPER",
    ib_account: str = "",
    context_hash: str = "",
    submission_id: str = "",
    scope_id: str = "",
    release_root: Path | str | None = None,
) -> ControlPlaneReadRefV1:
    normalized_domain = str(domain or "").strip().lower()
    normalized_surface = str(surface or "").strip().lower()
    global_root = _global_truth_root(truth_root) if truth_root is not None and str(truth_root).strip() else None
    sleeve_root = (
        _sleeve_truth_root(truth_sleeves_root, sleeve_id=sleeve_id, environment=environment)
        if truth_sleeves_root is not None and str(truth_sleeves_root).strip()
        else None
    )

    if normalized_domain == "release" and normalized_surface == "active_runtime_contract":
        return _read_validated_json(
            domain="release",
            surface="active_runtime_contract",
            path=_active_runtime_contract_path(),
            schema_relpath=ACTIVE_RUNTIME_CONTRACT_SCHEMA,
        )
    if normalized_domain == "release" and normalized_surface == "release_manifest_active":
        return _read_validated_json(
            domain="release",
            surface="release_manifest_active",
            path=_release_manifest_active_path(),
            schema_relpath=RELEASE_MANIFEST_SCHEMA,
        )
    if normalized_domain == "release" and normalized_surface == "release_manifest_for_release_root":
        target_root = _require_release_root(release_root, surface=surface)
        return _read_validated_json(
            domain="release",
            surface="release_manifest_for_release_root",
            path=_release_manifest_for_release_root_path(release_root=target_root),
            schema_relpath=RELEASE_MANIFEST_SCHEMA,
            metadata={"release_root": str(target_root)},
        )

    if normalized_domain == "policy" and normalized_surface == "configuration_state_current":
        root = _global_truth_root(global_root)
        return _read_validated_json(
            domain="policy",
            surface="configuration_state_current",
            path=_published_configuration_state_path(truth_root=root),
            schema_relpath=CONFIGURATION_STATE_SCHEMA,
            metadata={"truth_root": str(root)},
        )
    if normalized_domain == "policy" and normalized_surface == "compiled_active_config_latest":
        root = _global_truth_root(global_root)
        return _read_validated_json(
            domain="policy",
            surface="compiled_active_config_latest",
            path=_latest_compiled_active_config_path(truth_root=root),
            schema_relpath=COMPILED_ACTIVE_CONFIG_SCHEMA,
            metadata={"truth_root": str(root)},
        )
    if normalized_domain == "policy" and normalized_surface == "paper_policy_verdict":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="policy",
            surface="paper_policy_verdict",
            path=_paper_policy_verdict_path(truth_root=root, day_utc=day),
            schema_relpath=PAPER_POLICY_VERDICT_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "policy" and normalized_surface == "production_policy_verdict":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="policy",
            surface="production_policy_verdict",
            path=_production_policy_verdict_path(truth_root=root, day_utc=day),
            schema_relpath=PRODUCTION_POLICY_VERDICT_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )

    if normalized_domain == "session" and normalized_surface == "active_session_current":
        root = _global_truth_root(global_root)
        return _read_validated_json(
            domain="session",
            surface="active_session_current",
            path=_active_session_path(truth_root=root),
            schema_relpath=ACTIVE_SESSION_SCHEMA,
            metadata={"truth_root": str(root)},
        )
    if normalized_domain == "session" and normalized_surface == "session_authority_status_current":
        root = _global_truth_root(global_root)
        return _read_validated_json(
            domain="session",
            surface="session_authority_status_current",
            path=_session_authority_status_path(truth_root=root),
            schema_relpath=SESSION_AUTHORITY_STATUS_SCHEMA,
            metadata={"truth_root": str(root)},
        )
    if normalized_domain == "session" and normalized_surface == "session_authority_alert_current":
        root = _global_truth_root(global_root)
        return _read_validated_json(
            domain="session",
            surface="session_authority_alert_current",
            path=_session_authority_alert_path(truth_root=root),
            schema_relpath=SESSION_AUTHORITY_ALERT_SCHEMA,
            metadata={"truth_root": str(root)},
        )
    if normalized_domain == "session" and normalized_surface == "market_calendar_coverage_status_current":
        root = _global_truth_root(global_root)
        return _read_validated_json(
            domain="session",
            surface="market_calendar_coverage_status_current",
            path=_market_calendar_coverage_status_path(truth_root=root),
            schema_relpath=MARKET_CALENDAR_COVERAGE_STATUS_SCHEMA,
            metadata={"truth_root": str(root)},
        )
    if normalized_domain == "session" and normalized_surface == "market_calendar_day":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_market_calendar_day_ref(truth_root=root, day_utc=day)
    if normalized_domain == "session" and normalized_surface == "target_day_build":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="session",
            surface="target_day_build",
            path=_target_day_build_path(truth_root=root, day_utc=day),
            schema_relpath=TARGET_DAY_BUILD_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "session" and normalized_surface == "target_day_admission":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="session",
            surface="target_day_admission",
            path=_target_day_admission_path(truth_root=root, day_utc=day),
            schema_relpath=TARGET_DAY_ADMISSION_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "session" and normalized_surface == "session_promotion_decision":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="session",
            surface="session_promotion_decision",
            path=_session_promotion_decision_path(truth_root=root, day_utc=day),
            schema_relpath=SESSION_PROMOTION_DECISION_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "session" and normalized_surface == "day_activation_build":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        ctx = _require_context_hash(context_hash, surface=surface)
        return _read_validated_json(
            domain="session",
            surface="day_activation_build",
            path=_day_activation_build_path(truth_root=root, day_utc=day, context_hash=ctx),
            schema_relpath=DAY_ACTIVATION_BUILD_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day, "context_hash": ctx},
        )
    if normalized_domain == "session" and normalized_surface == "day_activation_package":
        root = _global_truth_root(sleeve_root or global_root)
        day = _require_day(day_utc, surface=surface)
        ctx = _require_context_hash(context_hash, surface=surface)
        return _read_validated_json(
            domain="session",
            surface="day_activation_package",
            path=_day_activation_package_path(truth_root=root, day_utc=day, context_hash=ctx),
            schema_relpath=DAY_ACTIVATION_PACKAGE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day, "context_hash": ctx},
        )
    if normalized_domain == "session" and normalized_surface == "global_context_build":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        ctx = _require_context_hash(context_hash, surface=surface)
        return _read_validated_json(
            domain="session",
            surface="global_context_build",
            path=_global_context_build_path(truth_root=root, day_utc=day, context_hash=ctx),
            schema_relpath=GLOBAL_CONTEXT_BUILD_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day, "context_hash": ctx},
        )
    if normalized_domain == "session" and normalized_surface == "global_context_package":
        root = _global_truth_root(sleeve_root or global_root)
        day = _require_day(day_utc, surface=surface)
        ctx = _require_context_hash(context_hash, surface=surface)
        return _read_validated_json(
            domain="session",
            surface="global_context_package",
            path=_global_context_package_path(truth_root=root, day_utc=day, context_hash=ctx),
            schema_relpath=GLOBAL_CONTEXT_PACKAGE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day, "context_hash": ctx},
        )

    if normalized_domain == "lifecycle" and normalized_surface == "day_authority_decision":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="day_authority_decision",
            path=_day_authority_decision_path(truth_root=root, day_utc=day),
            schema_relpath=DAY_AUTHORITY_DECISION_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "lifecycle" and normalized_surface == "pre_open_bundle":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="pre_open_bundle",
            path=_pre_open_bundle_path(truth_root=root, day_utc=day),
            schema_relpath=PRE_OPEN_BUNDLE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "lifecycle" and normalized_surface == "paper_startup_intent_input_convergence":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="paper_startup_intent_input_convergence",
            path=_paper_startup_intent_input_convergence_path(truth_root=root, day_utc=day),
            schema_relpath=PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "lifecycle" and normalized_surface == "paper_startup_authorization_convergence":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="paper_startup_authorization_convergence",
            path=_paper_startup_authorization_convergence_path(truth_root=root, day_utc=day),
            schema_relpath=PAPER_STARTUP_AUTHORIZATION_CONVERGENCE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "lifecycle" and normalized_surface == "startup_materialization_input_convergence":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="startup_materialization_input_convergence",
            path=_startup_materialization_input_convergence_path(truth_root=root, day_utc=day),
            schema_relpath=STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "lifecycle" and normalized_surface == "bod_execution_environment_proof":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="bod_execution_environment_proof",
            path=_bod_execution_environment_proof_path(truth_root=root, day_utc=day),
            schema_relpath=BOD_EXECUTION_ENVIRONMENT_PROOF_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "lifecycle" and normalized_surface == "phasec_risk_inputs_prep":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="phasec_risk_inputs_prep",
            path=_phasec_risk_inputs_prep_path(truth_root=root, day_utc=day),
            schema_relpath=PHASEC_RISK_INPUTS_PREP_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "lifecycle" and normalized_surface == "capability_state":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="capability_state",
            path=_capability_state_path(truth_root=root, day_utc=day),
            schema_relpath=CAPABILITY_STATE_V1_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "lifecycle" and normalized_surface == "intents_day_completeness":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="intents_day_completeness",
            path=_intents_day_completeness_path(truth_root=root, day_utc=day),
            schema_relpath=INTENTS_DAY_COMPLETENESS_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "lifecycle" and normalized_surface == "day_start_blocked":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="day_start_blocked",
            path=_day_start_blocked_path(truth_root=root, day_utc=day),
            schema_relpath=None,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "lifecycle" and normalized_surface == "trading_day_state":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="trading_day_state",
            path=_trading_day_state_path(truth_root=root, day_utc=day),
            schema_relpath=None,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "lifecycle" and normalized_surface == "sleeve_live_readiness":
        root, scope_meta = _resolve_surface_truth_root(
            global_root,
            truth_sleeves_root,
            domain="lifecycle",
            surface="sleeve_live_readiness",
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
        )
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="lifecycle",
            surface="sleeve_live_readiness",
            path=(
                root / "readiness_v1" / "sleeve_live_readiness_v1" / day / "sleeve_live_readiness.v1.json"
            ).resolve(),
            schema_relpath=SLEEVE_LIVE_READINESS_SCHEMA,
            metadata={**scope_meta, "truth_root": str(root), "day_utc": day},
        )

    if normalized_domain == "execution" and normalized_surface == "submit_boundary_status":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="submit_boundary_status",
            path=_submit_boundary_status_path(truth_root=root, day_utc=day),
            schema_relpath=SUBMIT_BOUNDARY_STATUS_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "paper_session_ledger":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="paper_session_ledger",
            path=_paper_session_ledger_path(truth_root=root, day_utc=day),
            schema_relpath=PAPER_SESSION_LEDGER_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "paper_session_evidence_manifest":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="paper_session_evidence_manifest",
            path=_paper_session_evidence_manifest_path(truth_root=root, day_utc=day),
            schema_relpath=PAPER_SESSION_EVIDENCE_MANIFEST_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "paper_session_kernel":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="paper_session_kernel",
            path=_paper_session_kernel_path(truth_root=root, day_utc=day),
            schema_relpath=PAPER_SESSION_KERNEL_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "paper_day_control_plane":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="paper_day_control_plane",
            path=_paper_day_control_plane_path(truth_root=root, day_utc=day),
            schema_relpath=PAPER_DAY_CONTROL_PLANE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "paper_trading_posture":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="paper_trading_posture",
            path=_paper_trading_posture_path(truth_root=root, day_utc=day),
            schema_relpath=PAPER_TRADING_POSTURE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "sleeve_rollup":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="sleeve_rollup",
            path=_sleeve_rollup_path(truth_root=root, day_utc=day),
            schema_relpath=SLEEVE_ROLLUP_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "startup_materialization":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="startup_materialization",
            path=_startup_materialization_path(truth_root=root, day_utc=day),
            schema_relpath=STARTUP_MATERIALIZATION_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "startup_proof_validation":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="startup_proof_validation",
            path=_startup_proof_validation_path(truth_root=root, day_utc=day),
            schema_relpath=STARTUP_PROOF_VALIDATION_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "economic_state_build":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        ctx = str(context_hash or "").strip() or None
        return _read_day_context_validated_json(
            domain="execution",
            surface="economic_state_build",
            family_root=_economic_state_build_family_root(truth_root=root, day_utc=day),
            filename="economic_state_build.v1.json",
            schema_relpath=ECONOMIC_STATE_BUILD_SCHEMA,
            context_hash=ctx,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "economic_state_package":
        root, scope_meta = _resolve_surface_truth_root(
            global_root,
            truth_sleeves_root,
            domain="execution",
            surface="economic_state_package",
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
        )
        day = _require_day(day_utc, surface=surface)
        ctx = str(context_hash or "").strip() or None
        return _read_day_context_validated_json(
            domain="execution",
            surface="economic_state_package",
            family_root=_economic_state_package_family_root(truth_root=root, day_utc=day),
            filename="economic_state_package.v1.json",
            schema_relpath=ECONOMIC_STATE_PACKAGE_SCHEMA,
            context_hash=ctx,
            metadata={**scope_meta, "truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "trading_day_control_plane":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="trading_day_control_plane",
            path=_trading_day_control_plane_path(truth_root=root, day_utc=day),
            schema_relpath=TRADING_DAY_CONTROL_PLANE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "trading_day_execution_control_plane":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="trading_day_execution_control_plane",
            path=_trading_day_execution_control_plane_path(truth_root=root, day_utc=day),
            schema_relpath=TRADING_DAY_EXECUTION_CONTROL_PLANE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "trading_day_state_machine":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="trading_day_state_machine",
            path=_trading_day_state_machine_path(truth_root=root, day_utc=day),
            schema_relpath=TRADING_DAY_STATE_MACHINE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "trading_day_intent_generation":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="trading_day_intent_generation",
            path=_trading_day_intent_generation_path(truth_root=root, day_utc=day),
            schema_relpath=TRADING_DAY_INTENT_GENERATION_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "replay_certification_gate":
        root = _global_truth_root(sleeve_root or global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="replay_certification_gate",
            path=_replay_certification_gate_path(truth_root=root, day_utc=day),
            schema_relpath=REPLAY_CERTIFICATION_GATE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "replay_certification_bundle":
        root = _global_truth_root(sleeve_root or global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="replay_certification_bundle",
            path=_replay_certification_bundle_path(truth_root=root, day_utc=day),
            schema_relpath=REPLAY_CERTIFICATION_BUNDLE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "trade_submit_readiness":
        if not str(ib_account or "").strip():
            raise ValueError("CONTROL_PLANE_READ_IB_ACCOUNT_REQUIRED:surface=trade_submit_readiness")
        scope_root, scope_meta = _resolve_surface_truth_root(
            global_root,
            truth_sleeves_root,
            domain="execution",
            surface="trade_submit_readiness",
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
        )
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="trade_submit_readiness",
            path=_trade_submit_readiness_path(
                truth_root=scope_root,
                day_utc=day,
                ib_account=ib_account,
                environment=environment,
            ),
            schema_relpath=TRADE_SUBMIT_READINESS_SCHEMA,
            metadata={
                **scope_meta,
                "truth_root": str(scope_root),
                "day_utc": day,
                "environment": str(environment).strip().upper(),
                "ib_account": str(ib_account).strip(),
            },
        )
    if normalized_domain == "execution" and normalized_surface == "capital_risk_envelope":
        root = _global_truth_root(sleeve_root or global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="capital_risk_envelope",
            path=_capital_risk_envelope_v2_path(truth_root=root, day_utc=day),
            schema_relpath=CAPITAL_RISK_ENVELOPE_V2_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "recurrence_kill_gate":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="recurrence_kill_gate",
            path=_recurrence_kill_gate_path(truth_root=root, day_utc=day),
            schema_relpath=RECURRENCE_KILL_GATE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "execution_build":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        sub = _require_submission_id(submission_id, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="execution_build",
            path=_execution_build_path(truth_root=root, day_utc=day, submission_id=sub),
            schema_relpath=EXECUTION_BUILD_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day, "submission_id": sub},
        )
    if normalized_domain == "execution" and normalized_surface == "execution_package":
        root = _global_truth_root(sleeve_root or global_root)
        day = _require_day(day_utc, surface=surface)
        sub = _require_submission_id(submission_id, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="execution_package",
            path=_execution_package_path(truth_root=root, day_utc=day, submission_id=sub),
            schema_relpath=EXECUTION_PACKAGE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day, "submission_id": sub},
        )
    if normalized_domain == "execution" and normalized_surface == "runtime_control_record":
        root = _global_truth_root(global_root)
        return _read_latest_validated_json(
            domain="execution",
            surface="runtime_control_record",
            root=_runtime_control_record_family_root(truth_root=root),
            pattern="*.runtime_control_record.v1.json",
            schema_relpath=RUNTIME_CONTROL_RECORD_SCHEMA,
            selection_fields=("day_utc", "effective_at_utc", "produced_utc"),
            metadata={"truth_root": str(root)},
        )
    if normalized_domain == "execution" and normalized_surface == "runtime_control_decision":
        root = _global_truth_root(global_root)
        return _read_latest_validated_json(
            domain="execution",
            surface="runtime_control_decision",
            root=_runtime_control_decision_family_root(truth_root=root),
            pattern="*.runtime_control_decision.v1.json",
            schema_relpath=RUNTIME_CONTROL_DECISION_SCHEMA,
            selection_fields=("day_utc", "effective_at_utc", "produced_utc"),
            metadata={"truth_root": str(root)},
        )
    if normalized_domain == "execution" and normalized_surface == "runtime_control_run_envelope":
        root = _global_truth_root(global_root)
        return _read_latest_validated_json(
            domain="execution",
            surface="runtime_control_run_envelope",
            root=_runtime_control_run_envelope_family_root(truth_root=root),
            pattern="*.runtime_control_run_envelope.v1.json",
            schema_relpath=RUNTIME_CONTROL_RUN_ENVELOPE_SCHEMA,
            selection_fields=("day_utc", "produced_utc"),
            metadata={"truth_root": str(root)},
        )
    if normalized_domain == "execution" and normalized_surface == "gate_stack_verdict":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="gate_stack_verdict",
            path=_gate_stack_verdict_path(truth_root=root, day_utc=day),
            schema_relpath=GATE_STACK_VERDICT_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "execution_reconciliation":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="execution",
            surface="execution_reconciliation",
            path=_execution_reconciliation_path(truth_root=root, day_utc=day),
            schema_relpath=EXECUTION_RECONCILIATION_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "execution" and normalized_surface == "global_kill_switch_state":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_global_kill_switch_state_ref(
            truth_root=root,
            truth_sleeves_root=truth_sleeves_root,
            day_utc=day,
        )
    if normalized_domain == "execution" and normalized_surface == "authorization_gate_verdict":
        day = _require_day(day_utc, surface=surface)
        return _read_authorization_gate_verdict_ref(
            truth_root=global_root,
            truth_sleeves_root=truth_sleeves_root,
            day_utc=day,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
        )
    if normalized_domain == "execution" and normalized_surface == "ib_api_handshake_latest_pointer":
        root = _global_truth_root(global_root)
        return _read_validated_json(
            domain="execution",
            surface="ib_api_handshake_latest_pointer",
            path=_ib_api_handshake_pointer_path(truth_root=root),
            schema_relpath=None,
            metadata={"truth_root": str(root)},
        )
    if normalized_domain == "execution" and normalized_surface == "ib_api_handshake_resolved_latest":
        root = _global_truth_root(global_root)
        return _read_pointer_target_ref(domain="execution", surface="ib_api_handshake_resolved_latest", truth_root=root)

    if normalized_domain == "operator" and normalized_surface == "runtime_truth_integrity_latest":
        root = _global_truth_root(global_root)
        return _read_validated_json(
            domain="operator",
            surface="runtime_truth_integrity_latest",
            path=_runtime_truth_integrity_latest_path(truth_root=root),
            schema_relpath=RUNTIME_TRUTH_INTEGRITY_RESULT_SCHEMA,
            metadata={"truth_root": str(root)},
        )
    if normalized_domain == "operator" and normalized_surface == "alerts_projection_primary":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="operator",
            surface="alerts_projection_primary",
            path=_alerts_projection_primary_path(truth_root=root, day_utc=day),
            schema_relpath=ALERTS_PROJECTION_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "operator" and normalized_surface == "alerts_projection_reports":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="operator",
            surface="alerts_projection_reports",
            path=_alerts_projection_reports_path(truth_root=root, day_utc=day),
            schema_relpath=ALERTS_PROJECTION_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "operator" and normalized_surface == "current_system_projection":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="operator",
            surface="current_system_projection",
            path=_current_system_projection_path(truth_root=root, day_utc=day),
            schema_relpath=CURRENT_SYSTEM_PROJECTION_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "operator" and normalized_surface == "control_plane_operator_status":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        scope = _require_scope_id(scope_id, surface=surface)
        return _read_validated_json(
            domain="operator",
            surface="control_plane_operator_status",
            path=_control_plane_operator_status_path(truth_root=root, day_utc=day, scope_id=scope),
            schema_relpath=CONTROL_PLANE_OPERATOR_STATUS_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day, "scope_id": scope},
        )
    if normalized_domain == "operator" and normalized_surface == "transition_timeline_projection":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        scope = _require_scope_id(scope_id, surface=surface)
        return _read_validated_json(
            domain="operator",
            surface="transition_timeline_projection",
            path=_transition_timeline_projection_path(truth_root=root, day_utc=day, scope_id=scope),
            schema_relpath=TRANSITION_TIMELINE_PROJECTION_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day, "scope_id": scope},
        )
    if normalized_domain == "operator" and normalized_surface == "platform_readiness":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_platform_readiness_ref(truth_root=root, day_utc=day)
    if normalized_domain == "operator" and normalized_surface == "kill_switch":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        kill_switch_ref = _read_global_kill_switch_state_ref(
            truth_root=root,
            truth_sleeves_root=truth_sleeves_root,
            day_utc=day,
        )
        payload = {
            "state": str(kill_switch_ref.payload.get("state") or "UNKNOWN").strip().upper(),
            "allow_entries": kill_switch_ref.payload.get("allow_entries"),
            "allow_exits": kill_switch_ref.payload.get("allow_exits"),
            "reason_codes": list(kill_switch_ref.payload.get("reason_codes") or []),
            "path": str(kill_switch_ref.path),
        }
        return _validated_ref_from_payload(
            domain="operator",
            surface="kill_switch",
            path=kill_switch_ref.path,
            payload=payload,
            schema_relpath=None,
            metadata={
                "truth_root": str(root),
                "day_utc": day,
                "source_surface": "execution.global_kill_switch_state",
                "source_sha256": kill_switch_ref.sha256,
            },
        )

    if normalized_domain == "platform" and normalized_surface == "deployment_state_machine":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_validated_json(
            domain="platform",
            surface="deployment_state_machine",
            path=_deployment_state_machine_path(truth_root=root, day_utc=day),
            schema_relpath=DEPLOYMENT_STATE_MACHINE_SCHEMA,
            metadata={"truth_root": str(root), "day_utc": day},
        )
    if normalized_domain == "platform" and normalized_surface == "platform_bug_metrics":
        root = _global_truth_root(global_root)
        day = _require_day(day_utc, surface=surface)
        return _read_platform_bug_metrics_ref(truth_root=root, day_utc=day)

    raise ValueError(f"CONTROL_PLANE_READ_UNKNOWN_SURFACE:domain={normalized_domain}:surface={normalized_surface}")


def read_control_plane_collection_v1(
    *,
    domain: str,
    surface: str,
    truth_root: Path | str | None = None,
    day_utc: str | None = None,
) -> ControlPlaneReadCollectionV1:
    normalized_domain = str(domain or "").strip().lower()
    normalized_surface = str(surface or "").strip().lower()
    root = _global_truth_root(truth_root)
    day = _require_day(day_utc, surface=surface)

    if normalized_domain == "operator" and normalized_surface == "policy_evolution_state_day":
        refs = tuple(
            _read_validated_json(
                domain="operator",
                surface="policy_evolution_state_day",
                path=path,
                schema_relpath=POLICY_EVOLUTION_STATE_SCHEMA,
                metadata={"truth_root": str(root), "day_utc": day},
            )
            for path in _policy_evolution_state_day_paths(truth_root=root, day_utc=day)
        )
        return ControlPlaneReadCollectionV1(
            domain="operator",
            surface="policy_evolution_state_day",
            read_kind=READ_KIND_COLLECTION,
            refs=refs,
            metadata={"truth_root": str(root), "day_utc": day},
        )

    raise ValueError(f"CONTROL_PLANE_READ_UNKNOWN_COLLECTION:domain={normalized_domain}:surface={normalized_surface}")


def read_control_plane_semantic_v1(
    *,
    domain: str,
    surface: str,
    truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    day_utc: str | None = None,
    sleeve_id: str = "PRIMARY",
    environment: str = "PAPER",
    ib_account: str = "",
) -> ControlPlaneSemanticViewV1:
    normalized_domain = str(domain or "").strip().lower()
    normalized_surface = str(surface or "").strip().lower()
    root = _global_truth_root(truth_root)

    if normalized_domain == "policy" and normalized_surface == "configuration_activation_family":
        state_ref = read_control_plane_surface_v1(
            domain="policy",
            surface="configuration_state_current",
            truth_root=root,
        )
        rows = {
            "configuration_policy_snapshot_ref": POLICY_SNAPSHOT_SCHEMA,
            "configuration_validation_result_ref": VALIDATION_RESULT_SCHEMA,
            "compiled_active_config_ref": COMPILED_ACTIVE_CONFIG_SCHEMA,
            "configuration_compile_result_ref": COMPILE_RESULT_SCHEMA,
            "configuration_review_diff_ref": REVIEW_DIFF_SCHEMA,
            "configuration_activation_transaction_ref": ACTIVATION_TRANSACTION_SCHEMA,
        }
        refs: list[ControlPlaneReadRefV1] = [state_ref]
        artifact_rows: Dict[str, Any] = {"configuration_state_v1": _ref_row(state_ref)}
        for field, schema in rows.items():
            ref_row = state_ref.payload.get(field)
            if not isinstance(ref_row, Mapping):
                raise ValueError(f"CONTROL_PLANE_SEMANTIC_REF_MISSING:surface={surface}:field={field}")
            path_text = str(ref_row.get("path") or "").strip()
            if not path_text:
                raise ValueError(f"CONTROL_PLANE_SEMANTIC_REF_PATH_MISSING:surface={surface}:field={field}")
            dep_ref = _read_validated_json(
                domain="policy",
                surface=f"{normalized_surface}:{field}",
                path=Path(path_text).expanduser().resolve(),
                schema_relpath=schema,
                metadata={"truth_root": str(root), "source_surface": "configuration_state_current"},
            )
            refs.append(dep_ref)
            artifact_rows[field.replace("_ref", "")] = _ref_row(dep_ref)
        return ControlPlaneSemanticViewV1(
            domain="policy",
            surface="configuration_activation_family",
            read_kind=READ_KIND_SEMANTIC,
            refs=tuple(refs),
            payload={
                "current_path": str(state_ref.path),
                "artifacts": artifact_rows,
            },
            metadata={"truth_root": str(root)},
        )

    if normalized_domain == "session" and normalized_surface == "next_day_readiness_probe_inputs":
        day = _require_day(day_utc, surface=surface)
        if not str(ib_account or "").strip():
            raise ValueError("CONTROL_PLANE_SEMANTIC_IB_ACCOUNT_REQUIRED:surface=next_day_readiness_probe_inputs")
        current_day = _prior_day(day)
        current_paper_ref = read_control_plane_surface_v1(
            domain="policy",
            surface="paper_policy_verdict",
            truth_root=root,
            day_utc=current_day,
        )
        current_prod_ref = read_control_plane_surface_v1(
            domain="policy",
            surface="production_policy_verdict",
            truth_root=root,
            day_utc=current_day,
        )
        current_trade_ref = read_control_plane_surface_v1(
            domain="execution",
            surface="trade_submit_readiness",
            truth_root=root,
            day_utc=current_day,
            environment=environment,
            ib_account=ib_account,
        )
        refs: list[ControlPlaneReadRefV1] = [current_paper_ref, current_prod_ref, current_trade_ref]
        current_recurrence_ref: ControlPlaneReadRefV1 | None = None
        try:
            current_recurrence_ref = read_control_plane_surface_v1(
                domain="execution",
                surface="recurrence_kill_gate",
                truth_root=root,
                day_utc=current_day,
            )
            refs.append(current_recurrence_ref)
        except Exception:
            current_recurrence_ref = None
        target_paper_ref = read_control_plane_surface_v1(
            domain="policy",
            surface="paper_policy_verdict",
            truth_root=root,
            day_utc=day,
        )
        target_trade_ref = read_control_plane_surface_v1(
            domain="execution",
            surface="trade_submit_readiness",
            truth_root=root,
            day_utc=day,
            environment=environment,
            ib_account=ib_account,
        )
        refs.extend([target_paper_ref, target_trade_ref])
        current_baseline_status = (
            "PASS"
            if str(current_paper_ref.payload.get("overall_status") or "").strip().upper() == "PASS"
            and str(current_prod_ref.payload.get("overall_status") or "").strip().upper() == "PASS"
            and str(current_trade_ref.payload.get("state") or "").strip().upper() == "OK"
            and (
                current_recurrence_ref is None
                or str(current_recurrence_ref.payload.get("proof_status") or "").strip().upper() == "RECURRENCE_SAFE"
            )
            else "FAIL"
        )
        target_paper_status = str(target_paper_ref.payload.get("overall_status") or "").strip().upper() or "UNKNOWN"
        target_trade_status = (
            "PASS" if str(target_trade_ref.payload.get("state") or "").strip().upper() == "OK" else "FAIL"
        )
        predicted_blocking_items: list[Dict[str, Any]] = []
        if current_baseline_status != "PASS":
            predicted_blocking_items.append(
                {
                    "item_id": "current_day_baseline",
                    "reason": "CURRENT_DAY_BASELINE_NOT_GREEN",
                }
            )
        if target_paper_status != "PASS":
            predicted_blocking_items.append(
                {
                    "item_id": "target_day_paper_policy",
                    "reason": "TARGET_DAY_PAPER_POLICY_BLOCKED",
                }
            )
        if target_trade_status != "PASS":
            predicted_blocking_items.append(
                {
                    "item_id": "target_day_trade_submit_readiness",
                    "reason": "TARGET_DAY_TRADE_SUBMIT_NOT_OK",
                }
            )
        evidence_projection = [
            {
                "projection_id": "current_day_baseline",
                "status": current_baseline_status,
                "basis": [_ref_row(ref) for ref in refs[: 4 if current_recurrence_ref is not None else 3]],
            },
            {
                "projection_id": "target_day_paper_policy",
                "status": target_paper_status,
                "basis": [_ref_row(target_paper_ref)],
            },
            {
                "projection_id": "target_day_trade_submit_readiness",
                "status": target_trade_status,
                "basis": [_ref_row(target_trade_ref)],
            },
        ]
        if current_baseline_status == "FAIL":
            probe_status = "BLOCKED"
            confidence = "HIGH"
        elif target_paper_status == "PASS" and target_trade_status == "PASS":
            probe_status = "READY"
            confidence = "HIGH"
        else:
            probe_status = "BLOCKED"
            confidence = "HIGH"
        return ControlPlaneSemanticViewV1(
            domain="session",
            surface="next_day_readiness_probe_inputs",
            read_kind=READ_KIND_SEMANTIC,
            refs=tuple(refs),
            payload={
                "current_day_utc": current_day,
                "target_day_utc": day,
                "current_baseline_status": current_baseline_status,
                "predicted_blocking_items": predicted_blocking_items,
                "evidence_projection": evidence_projection,
                "probe_status": probe_status,
                "confidence": confidence,
            },
            metadata={
                "truth_root": str(root),
                "day_utc": day,
                "environment": str(environment).strip().upper(),
                "ib_account": str(ib_account).strip(),
            },
        )

    if normalized_domain == "session" and normalized_surface == "fresh_day_admission_inputs":
        day = _require_day(day_utc, surface=surface)
        if not str(ib_account or "").strip():
            raise ValueError("CONTROL_PLANE_SEMANTIC_IB_ACCOUNT_REQUIRED:surface=fresh_day_admission_inputs")
        probe_semantic = read_control_plane_semantic_v1(
            domain="session",
            surface="next_day_readiness_probe_inputs",
            truth_root=root,
            day_utc=day,
            environment=environment,
            ib_account=ib_account,
        )
        paper_policy_ref = read_control_plane_surface_v1(
            domain="policy",
            surface="paper_policy_verdict",
            truth_root=root,
            day_utc=day,
        )
        trade_submit_ref = read_control_plane_surface_v1(
            domain="execution",
            surface="trade_submit_readiness",
            truth_root=root,
            day_utc=day,
            environment=environment,
            ib_account=ib_account,
        )
        trading_day_ref = read_control_plane_surface_v1(
            domain="execution",
            surface="trading_day_state_machine",
            truth_root=root,
            day_utc=day,
        )
        refs = list(probe_semantic.refs) + [paper_policy_ref, trade_submit_ref, trading_day_ref]
        materialized_rows: list[Dict[str, Any]] = []
        blocking_items: list[Dict[str, Any]] = []
        required_ready = True
        paper_status = str(paper_policy_ref.payload.get("overall_status") or "").strip().upper()
        materialized_rows.append(
            {"artifact_id": "paper_policy_verdict_v1", "artifact_ref": _ref_row(paper_policy_ref), "artifact_status": paper_status or "UNKNOWN"}
        )
        if paper_status != "PASS":
            required_ready = False
            blocking_items.append({"item_id": "paper_policy_verdict_v1", "reason": "TARGET_DAY_PAPER_POLICY_NOT_PASS"})
        trade_status = (
            "PASS"
            if str(trade_submit_ref.payload.get("state") or "").strip().upper() == "OK"
            and trade_submit_ref.payload.get("ok") is True
            else "FAIL"
        )
        materialized_rows.append(
            {"artifact_id": "trade_submit_readiness_c2_v1", "artifact_ref": _ref_row(trade_submit_ref), "artifact_status": trade_status}
        )
        if trade_status != "PASS":
            required_ready = False
            blocking_items.append({"item_id": "trade_submit_readiness_c2_v1", "reason": "TARGET_DAY_TRADE_SUBMIT_NOT_OK"})
        trading_status = (
            "PASS" if str(trading_day_ref.payload.get("final_start_decision") or "").strip().upper() == "READY_NOW" else "FAIL"
        )
        materialized_rows.append(
            {"artifact_id": "trading_day_state_machine_v1", "artifact_ref": _ref_row(trading_day_ref), "artifact_status": trading_status}
        )
        if trading_status != "PASS":
            required_ready = False
            blocking_items.append({"item_id": "trading_day_state_machine_v1", "reason": "TARGET_DAY_TRADING_DAY_NOT_READY_NOW"})
        if not required_ready and str(probe_semantic.payload.get("probe_status") or "").strip().upper() != "READY":
            blocking_items.append(
                {
                    "item_id": "next_day_readiness_probe_v1",
                    "reason": f"NEXT_DAY_READINESS_PROBE_{str(probe_semantic.payload.get('probe_status') or 'INVALID').strip().upper()}",
                }
            )
        return ControlPlaneSemanticViewV1(
            domain="session",
            surface="fresh_day_admission_inputs",
            read_kind=READ_KIND_SEMANTIC,
            refs=tuple(refs),
            payload={
                "required_target_day_artifacts": [
                    {"artifact_id": "paper_policy_verdict_v1", "artifact_path": str(paper_policy_ref.path), "requirement_class": "REQUIRED_PRE_ADMISSION"},
                    {"artifact_id": "trade_submit_readiness_c2_v1", "artifact_path": str(trade_submit_ref.path), "requirement_class": "REQUIRED_PRE_ADMISSION"},
                    {"artifact_id": "trading_day_state_machine_v1", "artifact_path": str(trading_day_ref.path), "requirement_class": "REQUIRED_PRE_ADMISSION"},
                ],
                "materialized_target_day_artifacts": materialized_rows,
                "blocking_items": blocking_items,
                "missing_required_artifacts": [],
                "required_artifacts_ready": required_ready,
                "probe_status": str(probe_semantic.payload.get("probe_status") or "").strip().upper(),
            },
            metadata={
                "truth_root": str(root),
                "day_utc": day,
                "environment": str(environment).strip().upper(),
                "ib_account": str(ib_account).strip(),
            },
        )

    raise ValueError(f"CONTROL_PLANE_READ_UNKNOWN_SEMANTIC:domain={normalized_domain}:surface={normalized_surface}")


def gateway_metadata_v1(
    ref: ControlPlaneReadRefV1 | ControlPlaneReadCollectionV1 | ControlPlaneSemanticViewV1,
) -> Dict[str, Any]:
    if isinstance(ref, ControlPlaneSemanticViewV1):
        return {
            "domain": ref.domain,
            "surface": ref.surface,
            "read_kind": ref.read_kind,
            "count": len(ref.refs),
            "metadata": dict(ref.metadata),
        }
    if isinstance(ref, ControlPlaneReadCollectionV1):
        return {
            "domain": ref.domain,
            "surface": ref.surface,
            "read_kind": ref.read_kind,
            "count": len(ref.refs),
            "metadata": dict(ref.metadata),
        }
    return {
        "domain": ref.domain,
        "surface": ref.surface,
        "read_kind": ref.read_kind,
        "path": str(ref.path),
        "sha256": ref.sha256,
        "schema_relpath": ref.schema_relpath,
        "metadata": dict(ref.metadata),
    }
