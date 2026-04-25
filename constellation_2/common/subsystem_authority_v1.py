from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    parse_day_utc_v1,
    read_json_object_v1,
    sha256_file_v1,
)
from constellation_2.common.execution_identity_binding_v1 import (
    EXECUTION_IDENTITY_BINDING_OWNER,
    RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH,
    RC_EXECUTION_IDENTITY_ACCOUNT_MISSING,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING,
    RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING,
    RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION,
    RC_EXECUTION_IDENTITY_SLEEVE_MISSING,
    RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED,
    resolve_governed_execution_identity_v1,
)
from constellation_2.common.sleeve_execution_root_v1 import (
    EXECUTION_ROOT_AUTHORITY_OWNER,
    RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN,
    RC_EXECUTION_ROOT_MODE_MISSING,
    RC_EXECUTION_ROOT_PATH_MISMATCH,
    RC_EXECUTION_ROOT_PATH_UNRESOLVED,
    RC_EXECUTION_ROOT_SLEEVE_ID_MISSING,
    resolve_sleeve_execution_root_v1,
)
from constellation_2.common.session_authority_v1 import resolve_session_authority_target_day_v1
from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_sleeve_truth_bindings
from constellation_2.common.constitutional_runtime_v1 import (
    CLOSURE_STATE_BLOCKED,
    CLOSURE_STATE_COMPLETE,
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_governed_dependency_ref_v1,
    build_machine_blocker_envelope_v1,
    get_constitutional_artifact_contract_v1,
    validate_governed_artifact_payload_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[2]

SUBSYSTEM_AUTHORITY_MANIFEST_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/subsystem_authority_manifest.v1.schema.json"
)
EXECUTION_DOSSIER_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_dossier.v1.schema.json"
EXECUTION_PROFILE_DOSSIER_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_profile_dossier.v1.schema.json"
)
EXECUTION_IDENTITY_DOSSIER_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_identity_dossier.v1.schema.json"
)
ACCOUNT_TRADING_POLICY_DOSSIER_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/account_trading_policy_dossier.v1.schema.json"
)
OPERATOR_SUMMARY_DOSSIER_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_summary_dossier.v1.schema.json"
)

SUBSYSTEM_ID_SESSION_AUTHORITY = "session_authority"
SUBSYSTEM_ID_STARTUP_AUTHORITY = "startup_authority"
SUBSYSTEM_ID_DAY_AUTHORITY = "day_authority"
SUBSYSTEM_ID_MARKET_CALENDAR_COVERAGE_AUTHORITY = "market_calendar_coverage_authority"
SUBSYSTEM_ID_EXECUTION_AUTHORITY = "execution_authority"
SUBSYSTEM_ID_EXECUTION_PROFILE_AUTHORITY = "execution_profile_authority"
SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY = "execution_identity_authority"
SUBSYSTEM_ID_ACCOUNT_TRADING_POLICY_AUTHORITY = "account_trading_policy_authority"
SUBSYSTEM_ID_OPERATOR_SUMMARY_AUTHORITY = "operator_summary_authority"

ALL_SUBSYSTEM_IDS: tuple[str, ...] = (
    SUBSYSTEM_ID_SESSION_AUTHORITY,
    SUBSYSTEM_ID_STARTUP_AUTHORITY,
    SUBSYSTEM_ID_DAY_AUTHORITY,
    SUBSYSTEM_ID_MARKET_CALENDAR_COVERAGE_AUTHORITY,
    SUBSYSTEM_ID_EXECUTION_AUTHORITY,
    SUBSYSTEM_ID_EXECUTION_PROFILE_AUTHORITY,
    SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY,
    SUBSYSTEM_ID_ACCOUNT_TRADING_POLICY_AUTHORITY,
    SUBSYSTEM_ID_OPERATOR_SUMMARY_AUTHORITY,
)

MANIFEST_SUBDIR_BY_SUBSYSTEM: Dict[str, str] = {
    SUBSYSTEM_ID_SESSION_AUTHORITY: "session_authority_manifest_v1",
    SUBSYSTEM_ID_STARTUP_AUTHORITY: "startup_authority_manifest_v1",
    SUBSYSTEM_ID_DAY_AUTHORITY: "day_authority_manifest_v1",
    SUBSYSTEM_ID_MARKET_CALENDAR_COVERAGE_AUTHORITY: "market_calendar_coverage_authority_manifest_v1",
    SUBSYSTEM_ID_EXECUTION_AUTHORITY: "execution_authority_manifest_v1",
    SUBSYSTEM_ID_EXECUTION_PROFILE_AUTHORITY: "execution_profile_authority_manifest_v1",
    SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY: "execution_identity_authority_manifest_v1",
    SUBSYSTEM_ID_ACCOUNT_TRADING_POLICY_AUTHORITY: "account_trading_policy_authority_manifest_v1",
    SUBSYSTEM_ID_OPERATOR_SUMMARY_AUTHORITY: "operator_summary_authority_manifest_v1",
}

STATE_CLEAR = "CLEAR"
STATE_READY = "READY"
STATE_BLOCKED = "BLOCKED"
STATE_AMBIGUOUS_AUTHORITY = "AMBIGUOUS_AUTHORITY"
STATE_RESOLVED = "RESOLVED"
STATE_ADVISORY_ONLY = "ADVISORY_ONLY"
AUTHORITY_LEVEL_DERIVED = "derived"
OPERATOR_SUMMARY_DOSSIER_ARTIFACT_FAMILY = "operator_summary_dossier_v1"

RC_EXECUTION_ROOT_AUTHORITY_AMBIGUOUS = "EXECUTION_ROOT_AUTHORITY_AMBIGUOUS"
RC_EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS = "EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS"
RC_ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS = "ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS"


@dataclass(frozen=True)
class SubsystemArtifactBundleV1:
    manifest_refs: Dict[str, SurfaceRefV1]
    dossier_refs: Dict[str, SurfaceRefV1]
    payloads: Dict[str, Dict[str, Any]]


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _artifact_ref(path: Path | str) -> Dict[str, str]:
    p = Path(path).resolve()
    if not p.exists() or not p.is_file():
        return {"artifact_path": str(p), "artifact_sha256": ""}
    return {"artifact_path": str(p), "artifact_sha256": sha256_file_v1(p)}


def _path_ref(path: Path | str, *, ref_type: str, ref_id: str) -> Dict[str, str]:
    p = Path(path).resolve()
    return {
        "ref_id": str(ref_id),
        "ref_type": str(ref_type),
        "artifact_path": str(p),
        "artifact_sha256": sha256_file_v1(p) if p.exists() and p.is_file() else "",
    }


def _blank_first_blocker() -> Dict[str, str]:
    return {"reason_code": "", "summary": "", "source": ""}


def _first_blocker(*, reason_code: str, summary: str, source: str) -> Dict[str, str]:
    return {
        "reason_code": str(reason_code).strip(),
        "summary": str(summary).strip(),
        "source": str(source).strip(),
    }


def _ambiguity_state(
    *,
    status: str,
    summary: str,
    reason_codes: Iterable[str],
    conflicting_authorities: Iterable[str],
) -> Dict[str, Any]:
    return {
        "status": str(status).strip(),
        "summary": str(summary).strip(),
        "reason_codes": [str(code).strip() for code in reason_codes if str(code).strip()],
        "conflicting_authorities": [str(item).strip() for item in conflicting_authorities if str(item).strip()],
    }


def _dossier_payload(
    *,
    schema_id: str,
    schema_version: str,
    day_utc: str,
    subsystem_id: str,
    current_state: str,
    owner_ref: Dict[str, str],
    first_blocker: Mapping[str, Any],
    upstream_evidence_refs: Sequence[Mapping[str, Any]],
    ambiguity_state: Mapping[str, Any],
    recommended_operator_action: str,
    extra: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    payload = {
        "schema_id": schema_id,
        "schema_version": schema_version,
        "generated_utc": _utc_now(),
        "day_utc": parse_day_utc_v1(day_utc),
        "subsystem_id": str(subsystem_id).strip(),
        "current_state": str(current_state).strip(),
        "owner_ref": {
            "artifact_path": str(owner_ref.get("artifact_path") or "").strip(),
            "artifact_sha256": str(owner_ref.get("artifact_sha256") or "").strip(),
        },
        "first_blocker": {
            "reason_code": str(first_blocker.get("reason_code") or "").strip(),
            "summary": str(first_blocker.get("summary") or "").strip(),
            "source": str(first_blocker.get("source") or "").strip(),
        },
        "upstream_evidence_refs": [
            {
                "ref_id": str(row.get("ref_id") or "").strip(),
                "ref_type": str(row.get("ref_type") or "").strip(),
                "artifact_path": str(row.get("artifact_path") or "").strip(),
                "artifact_sha256": str(row.get("artifact_sha256") or "").strip(),
            }
            for row in upstream_evidence_refs
        ],
        "ambiguity_state": {
            "status": str(ambiguity_state.get("status") or "").strip(),
            "summary": str(ambiguity_state.get("summary") or "").strip(),
            "reason_codes": [
                str(code).strip()
                for code in (ambiguity_state.get("reason_codes") or [])
                if str(code).strip()
            ],
            "conflicting_authorities": [
                str(item).strip()
                for item in (ambiguity_state.get("conflicting_authorities") or [])
                if str(item).strip()
            ],
        },
        "recommended_operator_action": str(recommended_operator_action).strip(),
    }
    payload.update(dict(extra or {}))
    return payload


def _manifest_payload(
    *,
    subsystem_id: str,
    authority_owner: str,
    canonical_artifacts: Sequence[Mapping[str, Any]],
    trusted_upstream_authorities: Sequence[Mapping[str, Any]],
    precedence_rules: Sequence[Mapping[str, Any]],
    invalidation_rules: Sequence[Mapping[str, Any]],
    degraded_states: Sequence[Mapping[str, Any]],
    fail_closed_on_ambiguity: bool,
    operator_summary_ref: str = "",
) -> Dict[str, Any]:
    return {
        "schema_id": "subsystem_authority_manifest",
        "schema_version": "v1",
        "generated_utc": _utc_now(),
        "subsystem_id": str(subsystem_id).strip(),
        "authority_owner": str(authority_owner).strip(),
        "canonical_artifacts": [dict(row) for row in canonical_artifacts],
        "trusted_upstream_authorities": [dict(row) for row in trusted_upstream_authorities],
        "precedence_rules": [dict(row) for row in precedence_rules],
        "invalidation_rules": [dict(row) for row in invalidation_rules],
        "degraded_states": [dict(row) for row in degraded_states],
        "fail_closed_on_ambiguity": bool(fail_closed_on_ambiguity),
        "operator_summary_ref": str(operator_summary_ref).strip(),
    }


def resolve_subsystem_authority_manifest_path(*, truth_root: Path, subsystem_id: str) -> Path:
    manifest_dir = MANIFEST_SUBDIR_BY_SUBSYSTEM.get(str(subsystem_id).strip())
    if not manifest_dir:
        raise ValueError(f"UNKNOWN_SUBSYSTEM_ID:{subsystem_id}")
    return (Path(truth_root).resolve() / "reports" / manifest_dir / "current.json").resolve()


def resolve_execution_dossier_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "execution_dossier_v1"
        / parse_day_utc_v1(day_utc)
        / "execution_dossier.v1.json"
    ).resolve()


def resolve_execution_profile_dossier_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "execution_profile_dossier_v1"
        / parse_day_utc_v1(day_utc)
        / "execution_profile_dossier.v1.json"
    ).resolve()


def resolve_execution_identity_dossier_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "execution_identity_dossier_v1"
        / parse_day_utc_v1(day_utc)
        / "execution_identity_dossier.v1.json"
    ).resolve()


def resolve_account_trading_policy_dossier_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "account_trading_policy_dossier_v1"
        / parse_day_utc_v1(day_utc)
        / "account_trading_policy_dossier.v1.json"
    ).resolve()


def resolve_operator_summary_dossier_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "operator_summary_dossier_v1"
        / parse_day_utc_v1(day_utc)
        / "operator_summary_dossier.v1.json"
    ).resolve()


def _read_text(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8")


def _contains_all(path: Path, snippets: Iterable[str]) -> bool:
    text = _read_text(path)
    return all(str(snippet) in text for snippet in snippets)


def _read_json_optional(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        return read_json_object_v1(path)
    except Exception:
        return {}


def resolve_subsystem_target_day_v1(*, truth_root: Path, day_utc: str | None = None) -> str:
    explicit = str(day_utc or "").strip()
    if explicit:
        return parse_day_utc_v1(explicit)
    try:
        active_ref = read_control_plane_surface_v1(
            domain="session",
            surface="active_session_current",
            truth_root=truth_root,
        )
        active = dict(active_ref.payload)
    except Exception:
        active = {}
    blocked_target_day = str(active.get("blocked_target_day") or "").strip()
    rollover_status = str(active.get("rollover_status") or "").strip().upper()
    if blocked_target_day and rollover_status == "ROLLOVER_WITHHELD":
        return parse_day_utc_v1(blocked_target_day)
    for field in ("active_day", "target_day", "next_target_day", "blocked_target_day"):
        value = str(active.get(field) or "").strip()
        if value:
            return parse_day_utc_v1(value)
    return resolve_session_authority_target_day_v1()


def _active_paper_sleeve_row(repo_root: Path) -> Dict[str, Any]:
    registry_path = (Path(repo_root).resolve() / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
    registry = _read_json_optional(registry_path)
    sleeves = registry.get("sleeves")
    if not isinstance(sleeves, list):
        return {}
    for row in sleeves:
        if not isinstance(row, dict):
            continue
        if row.get("enabled") is not True:
            continue
        if str(row.get("mode") or "").strip().upper() != "PAPER":
            continue
        return row
    return {}


def _build_subsystem_authority_manifests(
    *,
    repo_root: Path,
    truth_root: Path,
) -> Dict[str, Dict[str, Any]]:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).resolve()
    status_ref = str((truth_root / "session_authority_status_v1" / "current.json").resolve())
    manifests: Dict[str, Dict[str, Any]] = {}

    manifests[SUBSYSTEM_ID_SESSION_AUTHORITY] = _manifest_payload(
        subsystem_id=SUBSYSTEM_ID_SESSION_AUTHORITY,
        authority_owner="session_authority_v1",
        canonical_artifacts=[
            {"artifact_id": "target_day_build_v1", "artifact_path": str((truth_root / "target_day_build_v1").resolve()), "authority_role": "CONTROL_PLANE"},
            {"artifact_id": "target_day_admission_v1", "artifact_path": str((truth_root / "target_day_admission_v1").resolve()), "authority_role": "CONTROL_PLANE"},
            {"artifact_id": "active_session_v1", "artifact_path": str((truth_root / "active_session_v1/current.json").resolve()), "authority_role": "CONTROL_PLANE"},
        ],
        trusted_upstream_authorities=[
            {"authority_id": "paper_startup_intent_input_convergence_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/paper_startup_intent_input_convergence_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "startup_materialization_input_convergence_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/startup_materialization_input_convergence_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "day_authority_decision_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/day_authority_decision_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
        ],
        precedence_rules=[
            {"priority": 1, "rule_id": "session_authority_single_owner", "description": "Session Authority alone owns target-day build, admission, and active-session publication.", "source_ids": ["session_authority_v1"], "outcome": "SESSION_AUTHORITY_CANONICAL"},
        ],
        invalidation_rules=[
            {"reason_code": "SESSION_AUTHORITY_ARTIFACT_MISSING", "condition": "Any target-day build, admission, or active-session artifact is missing or invalid.", "effect": "FAIL_CLOSED"},
        ],
        degraded_states=[
            {"state": STATE_BLOCKED, "reason_code": "ADMISSION_RULE_BLOCKED", "meaning": "Session Authority blocks activation until build closure is proven."},
        ],
        fail_closed_on_ambiguity=True,
        operator_summary_ref=status_ref,
    )

    manifests[SUBSYSTEM_ID_STARTUP_AUTHORITY] = _manifest_payload(
        subsystem_id=SUBSYSTEM_ID_STARTUP_AUTHORITY,
        authority_owner="session_authority_v1",
        canonical_artifacts=[
            {"artifact_id": "paper_startup_intent_input_convergence_v1", "artifact_path": str((truth_root / "reports/paper_startup_intent_input_convergence_v1").resolve()), "authority_role": "UPSTREAM_STARTUP_CONVERGENCE"},
            {"artifact_id": "startup_materialization_input_convergence_v1", "artifact_path": str((truth_root / "reports/startup_materialization_input_convergence_v1").resolve()), "authority_role": "UPSTREAM_STARTUP_CONVERGENCE"},
            {"artifact_id": "startup_materialization_v1", "artifact_path": str((truth_root / "reports/startup_materialization_v1").resolve()), "authority_role": "STARTUP_RUNTIME_FACT"},
        ],
        trusted_upstream_authorities=[
            {"authority_id": "session_authority_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/session_authority_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
        ],
        precedence_rules=[
            {"priority": 1, "rule_id": "startup_authority_consumed_through_session_authority", "description": "Startup convergence surfaces are owned upstream facts and are binding only through Session Authority target-day build and admission.", "source_ids": ["session_authority_v1", "paper_startup_intent_input_convergence_v1", "startup_materialization_input_convergence_v1"], "outcome": "SESSION_AUTHORITY_BINDS_STARTUP"},
        ],
        invalidation_rules=[
            {"reason_code": "PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_BLOCKED", "condition": "Primary startup intent-input convergence is blocked.", "effect": "FAIL_CLOSED"},
            {"reason_code": "STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_BLOCKED", "condition": "Canonical startup-materialization convergence is blocked.", "effect": "FAIL_CLOSED"},
        ],
        degraded_states=[
            {"state": STATE_BLOCKED, "reason_code": "PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_BLOCKED", "meaning": "Startup intent generation is blocked."},
            {"state": STATE_BLOCKED, "reason_code": "STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_BLOCKED", "meaning": "Startup materialization is blocked."},
        ],
        fail_closed_on_ambiguity=True,
        operator_summary_ref=status_ref,
    )

    manifests[SUBSYSTEM_ID_DAY_AUTHORITY] = _manifest_payload(
        subsystem_id=SUBSYSTEM_ID_DAY_AUTHORITY,
        authority_owner="day_authority_decision_v1",
        canonical_artifacts=[
            {"artifact_id": "day_authority_decision_v1", "artifact_path": str((truth_root / "reports/day_authority_decision_v1").resolve()), "authority_role": "VALIDATION_ARTIFACT"},
        ],
        trusted_upstream_authorities=[
            {"authority_id": "session_readiness_refresh_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/session_readiness_refresh_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
        ],
        precedence_rules=[
            {"priority": 1, "rule_id": "day_authority_is_validation_only", "description": "Day Authority validates same-day readiness but does not override Session Authority active-day publication.", "source_ids": ["day_authority_decision_v1", "session_authority_v1"], "outcome": "SESSION_AUTHORITY_REMAINS_ACTIVE_DAY_OWNER"},
        ],
        invalidation_rules=[
            {"reason_code": "DAY_AUTHORITY_DECISION_INVALID", "condition": "Same-day validation artifact is missing or invalid.", "effect": "FAIL_CLOSED"},
        ],
        degraded_states=[
            {"state": STATE_BLOCKED, "reason_code": "DAY_AUTHORITY_DECISION_INVALID", "meaning": "Day validation is unavailable or blocked."},
        ],
        fail_closed_on_ambiguity=True,
        operator_summary_ref=status_ref,
    )

    manifests[SUBSYSTEM_ID_MARKET_CALENDAR_COVERAGE_AUTHORITY] = _manifest_payload(
        subsystem_id=SUBSYSTEM_ID_MARKET_CALENDAR_COVERAGE_AUTHORITY,
        authority_owner="market_calendar_coverage_authority_v1",
        canonical_artifacts=[
            {"artifact_id": "market_calendar_coverage_status_v1", "artifact_path": str((truth_root / "market_calendar_coverage_status_v1/current.json").resolve()), "authority_role": "UPSTREAM_PREVENTION_STATUS"},
        ],
        trusted_upstream_authorities=[
            {"authority_id": "market_calendar_coverage_authority_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/market_calendar_coverage_authority_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "market_calendar_source_v1", "authority_path": str((repo_root / "constellation_2/phaseJ/source_data/market_calendar_source_v1/dataset_manifest.json").resolve()), "authority_type": "DATASET_MANIFEST"},
        ],
        precedence_rules=[
            {"priority": 1, "rule_id": "market_calendar_is_upstream_prevention_only", "description": "Market-calendar coverage authority prevents invalid future-day operation but does not replace Session Authority active-day ownership.", "source_ids": ["market_calendar_coverage_authority_v1", "session_authority_v1"], "outcome": "SESSION_AUTHORITY_REMAINS_ACTIVE_DAY_OWNER"},
        ],
        invalidation_rules=[
            {"reason_code": "SOURCE_NOT_EXTENDED", "condition": "Governed source or runtime calendar does not cover the required target day.", "effect": "FAIL_CLOSED"},
        ],
        degraded_states=[
            {"state": STATE_BLOCKED, "reason_code": "SOURCE_NOT_EXTENDED", "meaning": "Required future-day market-calendar coverage is missing."},
        ],
        fail_closed_on_ambiguity=True,
        operator_summary_ref=status_ref,
    )

    manifests[SUBSYSTEM_ID_EXECUTION_AUTHORITY] = _manifest_payload(
        subsystem_id=SUBSYSTEM_ID_EXECUTION_AUTHORITY,
        authority_owner=EXECUTION_ROOT_AUTHORITY_OWNER,
        canonical_artifacts=[
            {"artifact_id": "sleeve_execution_root_v1", "artifact_path": str((repo_root / "governance/05_CONTRACTS/C2/execution_root_authority_v1.contract.md").resolve()), "authority_role": "CANONICAL_EXECUTION_ROOT_OWNER"},
            {"artifact_id": "truth_sleeves_partition", "artifact_path": str((truth_root.parent / "truth_sleeves").resolve()), "authority_role": "CANONICAL_EXECUTION_ROOT_BASIS"},
            {"artifact_id": "authorization_gate_verdict_v1", "artifact_path": str((truth_root.parent / "truth_sleeves").resolve()), "authority_role": "SLEEVE_SCOPED_EXECUTION_GATE"},
        ],
        trusted_upstream_authorities=[
            {"authority_id": "execution_root_authority_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/execution_root_authority_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "truth_partitioning_by_sleeve_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/truth_partitioning_by_sleeve_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "multi_account_topology_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/multi_account_topology_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "C2_TRUE_EVIDENCE_SPINE_V2", "authority_path": str((repo_root / "governance/03_CONTRACTS/C2_TRUE_EVIDENCE_SPINE_V2.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "trade_submit_readiness_c2_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/trade_submit_readiness_c2_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "startup_materialization_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/startup_materialization_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
        ],
        precedence_rules=[
            {"priority": 1, "rule_id": "sleeve_execution_root_is_canonical", "description": "Execution authority is canonical only from sleeve_execution_root_v1 and truth/sleeves/<sleeve_id>/<mode>/...", "source_ids": ["execution_root_authority_v1", "truth_partitioning_by_sleeve_v1"], "outcome": "SLEEVE_EXECUTION_ROOT_CANONICAL"},
            {"priority": 2, "rule_id": "global_execution_root_references_are_legacy_only", "description": "Older global/shared execution-root references may remain for history or diagnostics but must not govern active submission or execution authority resolution.", "source_ids": ["C2_TRUE_EVIDENCE_SPINE_V2", "trade_submit_readiness_c2_v1", "startup_materialization_v1"], "outcome": "GLOBAL_EXECUTION_ROOT_NON_CANONICAL"},
        ],
        invalidation_rules=[
            {"reason_code": RC_EXECUTION_ROOT_SLEEVE_ID_MISSING, "condition": "No active sleeve_id can be proven for PAPER execution authority resolution.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_ROOT_MODE_MISSING, "condition": "Execution mode is absent while resolving the canonical sleeve execution root.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_ROOT_PATH_UNRESOLVED, "condition": "The computed sleeve execution root path cannot be resolved from governed inputs.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_ROOT_PATH_MISMATCH, "condition": "A runtime or submit path does not match the deterministic canonical sleeve execution root.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN, "condition": "A global/shared execution-root reference remains active in execution authority resolution.", "effect": "FAIL_CLOSED"},
        ],
        degraded_states=[
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_ROOT_SLEEVE_ID_MISSING, "meaning": "Execution authority is blocked because sleeve identity is missing."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_ROOT_MODE_MISSING, "meaning": "Execution authority is blocked because mode is missing."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_ROOT_PATH_UNRESOLVED, "meaning": "Execution authority is blocked because the canonical sleeve execution root could not be resolved."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_ROOT_PATH_MISMATCH, "meaning": "Execution authority is blocked because the runtime path does not match the canonical sleeve execution root."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN, "meaning": "Execution authority is blocked because a global/shared execution-root reference is still active."},
        ],
        fail_closed_on_ambiguity=True,
        operator_summary_ref=status_ref,
    )

    manifests[SUBSYSTEM_ID_EXECUTION_PROFILE_AUTHORITY] = _manifest_payload(
        subsystem_id=SUBSYSTEM_ID_EXECUTION_PROFILE_AUTHORITY,
        authority_owner="sleeve_registry_v1",
        canonical_artifacts=[
            {"artifact_id": "C2_SLEEVE_REGISTRY_V1.json", "artifact_path": str((repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()), "authority_role": "CANDIDATE_PROFILE_OWNER"},
            {"artifact_id": "run_c2_paper_day_orchestrator_v2.py", "artifact_path": str((repo_root / "ops/tools/run_c2_paper_day_orchestrator_v2.py").resolve()), "authority_role": "RUNTIME_DEFAULTS"},
            {"artifact_id": "paper_day_readiness_runbook_v1.contract.md", "artifact_path": str((repo_root / "governance/05_CONTRACTS/C2/paper_day_readiness_runbook_v1.contract.md").resolve()), "authority_role": "OPERATOR_EXECUTION_PROFILE_GUIDANCE"},
        ],
        trusted_upstream_authorities=[
            {"authority_id": "sleeve_registry_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/sleeve_registry_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "multi_account_topology_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/multi_account_topology_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
        ],
        precedence_rules=[
            {"priority": 1, "rule_id": "single_execution_profile_required", "description": "Gateway host, port, and client IDs must have one proven owner before execution may be considered safe.", "source_ids": ["C2_SLEEVE_REGISTRY_V1.json", "run_c2_paper_day_orchestrator_v2.py", "paper_day_readiness_runbook_v1.contract.md"], "outcome": "FAIL_CLOSED_AMBIGUOUS_AUTHORITY"},
        ],
        invalidation_rules=[
            {"reason_code": RC_EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS, "condition": "Sleeve registry, orchestrator defaults, and runbook examples disagree on the active IB gateway profile.", "effect": "FAIL_CLOSED"},
        ],
        degraded_states=[
            {"state": STATE_AMBIGUOUS_AUTHORITY, "reason_code": RC_EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS, "meaning": "Execution profile ownership is unresolved and must block transmit."},
        ],
        fail_closed_on_ambiguity=True,
        operator_summary_ref=status_ref,
    )

    manifests[SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY] = _manifest_payload(
        subsystem_id=SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY,
        authority_owner=EXECUTION_IDENTITY_BINDING_OWNER,
        canonical_artifacts=[
            {"artifact_id": "execution_identity_binding_v1", "artifact_path": str((repo_root / "governance/05_CONTRACTS/C2/execution_identity_binding_v1.contract.md").resolve()), "authority_role": "CANONICAL_IDENTITY_INVARIANT"},
            {"artifact_id": "C2_SLEEVE_REGISTRY_V1.json", "artifact_path": str((repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()), "authority_role": "SLEEVE_TO_ACCOUNT_AND_CLIENT_BINDING"},
            {"artifact_id": "C2_IB_ACCOUNT_REGISTRY_V1.json", "artifact_path": str((repo_root / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()), "authority_role": "ACCOUNT_ELIGIBILITY_BINDING"},
        ],
        trusted_upstream_authorities=[
            {"authority_id": "execution_identity_binding_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/execution_identity_binding_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "execution_profile_authority_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/execution_profile_authority_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "sleeve_registry_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/sleeve_registry_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "ib_account_registry_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/ib_account_registry_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
        ],
        precedence_rules=[
            {"priority": 1, "rule_id": "sleeve_registry_owns_submit_identity_binding", "description": "Sleeve registry owns the canonical sleeve/environment/account/client binding tuple for active execution identity.", "source_ids": ["C2_SLEEVE_REGISTRY_V1.json", "execution_profile_authority_v1"], "outcome": "SLEEVE_EXECUTION_IDENTITY_CANONICAL"},
            {"priority": 2, "rule_id": "ib_account_registry_owns_account_eligibility", "description": "IB account registry validates account eligibility and allowed_sleeve_ids but does not override the sleeve registry identity tuple.", "source_ids": ["C2_IB_ACCOUNT_REGISTRY_V1.json", "ib_account_registry_v1"], "outcome": "ACCOUNT_ELIGIBILITY_VALIDATES_BINDING"},
            {"priority": 3, "rule_id": "submit_boundary_must_match_governed_identity", "description": "Submit-boundary runtime account and client_id_orders must exactly match the governed execution identity before any broker-facing action.", "source_ids": ["execution_identity_binding_v1", "submit_boundary_paper_v4.py"], "outcome": "FAIL_CLOSED_ON_IDENTITY_MISMATCH"},
        ],
        invalidation_rules=[
            {"reason_code": RC_EXECUTION_IDENTITY_SLEEVE_MISSING, "condition": "No sleeve_id was provided for execution identity resolution.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING, "condition": "No environment was provided for execution identity resolution.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED, "condition": "The sleeve/environment pair is not uniquely present in the authoritative sleeve registry.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_IDENTITY_ACCOUNT_MISSING, "condition": "The canonical account_id is missing for the active sleeve/environment binding.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING, "condition": "The canonical client_id_orders is missing for the active sleeve/environment binding.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS, "condition": "Multiple candidate accounts exist for the same sleeve/environment binding.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS, "condition": "Multiple candidate orders client IDs exist for the same sleeve/environment binding.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH, "condition": "Runtime or profile account differs from the governed execution identity.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH, "condition": "Runtime or profile client_id_orders differs from the governed execution identity.", "effect": "FAIL_CLOSED"},
            {"reason_code": RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION, "condition": "The resolved sleeve/account/client tuple is not allowed by the governed registry chain.", "effect": "FAIL_CLOSED"},
        ],
        degraded_states=[
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS, "meaning": "Execution identity binding is ambiguous and must block submit."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS, "meaning": "Execution identity binding is ambiguous and must block submit."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH, "meaning": "Execution identity binding disagrees with runtime or profile account selection."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH, "meaning": "Execution identity binding disagrees with runtime or profile client_id_orders."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION, "meaning": "Execution identity binding is forbidden and must block submit."},
        ],
        fail_closed_on_ambiguity=True,
        operator_summary_ref=status_ref,
    )

    manifests[SUBSYSTEM_ID_ACCOUNT_TRADING_POLICY_AUTHORITY] = _manifest_payload(
        subsystem_id=SUBSYSTEM_ID_ACCOUNT_TRADING_POLICY_AUTHORITY,
        authority_owner="trading_symbol_policy_authority_v1",
        canonical_artifacts=[
            {"artifact_id": "ENGINE_MODEL_REGISTRY_V1.json", "artifact_path": str((repo_root / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()), "authority_role": "TRADING_SYMBOL_POLICY"},
            {"artifact_id": "C2_IB_ACCOUNT_REGISTRY_V1.json", "artifact_path": str((repo_root / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()), "authority_role": "ACCOUNT_SUBMISSION_ELIGIBILITY"},
        ],
        trusted_upstream_authorities=[
            {"authority_id": "trading_symbol_policy_authority_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/trading_symbol_policy_authority_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "ib_account_registry_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/ib_account_registry_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "sleeve_registry_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/sleeve_registry_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
        ],
        precedence_rules=[
            {"priority": 1, "rule_id": "engine_registry_owns_symbol_policy", "description": "Tradable symbol policy is authoritative from ENGINE_MODEL_REGISTRY_V1.allowed_symbols; IB account and sleeve symbol lists are non-authoritative compatibility or coverage data for symbol policy.", "source_ids": ["trading_symbol_policy_authority_v1", "ENGINE_MODEL_REGISTRY_V1.json", "C2_IB_ACCOUNT_REGISTRY_V1.json", "C2_SLEEVE_REGISTRY_V1.json"], "outcome": "ENGINE_SYMBOL_POLICY_CANONICAL"},
            {"priority": 2, "rule_id": "ib_registry_owns_account_eligibility_only", "description": "IB account registry remains authoritative for account eligibility and allowed_engine_ids only.", "source_ids": ["ib_account_registry_v1", "C2_IB_ACCOUNT_REGISTRY_V1.json"], "outcome": "ACCOUNT_ELIGIBILITY_CANONICAL"},
        ],
        invalidation_rules=[
            {"reason_code": RC_ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS, "condition": "Contracts or submit-boundary implementation disagree on whether symbol policy comes from engine, account, or sleeve registries.", "effect": "FAIL_CLOSED"},
        ],
        degraded_states=[
            {"state": STATE_AMBIGUOUS_AUTHORITY, "reason_code": RC_ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS, "meaning": "Trading-policy ownership is unresolved and must block transmit."},
        ],
        fail_closed_on_ambiguity=True,
        operator_summary_ref=status_ref,
    )

    manifests[SUBSYSTEM_ID_OPERATOR_SUMMARY_AUTHORITY] = _manifest_payload(
        subsystem_id=SUBSYSTEM_ID_OPERATOR_SUMMARY_AUTHORITY,
        authority_owner="session_authority_status_v1",
        canonical_artifacts=[
            {"artifact_id": "session_authority_status_v1", "artifact_path": str((truth_root / "session_authority_status_v1/current.json").resolve()), "authority_role": "CANONICAL_OPERATOR_SUMMARY"},
            {"artifact_id": "operator_summary_dossier_v1", "artifact_path": str((truth_root / "reports/operator_summary_dossier_v1").resolve()), "authority_role": "PER_DAY_OPERATOR_DOSSIER"},
        ],
        trusted_upstream_authorities=[
            {"authority_id": "session_authority_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/session_authority_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "submit_boundary_status_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/submit_boundary_status_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "operator_summary_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/operator_summary_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "paper_policy_verdict_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/paper_policy_verdict_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
            {"authority_id": "capability_state_v1", "authority_path": str((repo_root / "governance/05_CONTRACTS/C2/capability_state_v1.contract.md").resolve()), "authority_type": "CONTRACT"},
        ],
        precedence_rules=[
            {"priority": 1, "rule_id": "session_authority_status_is_operator_summary_owner", "description": "Session Authority Status is the operator-facing summary owner for active day and admission interpretation.", "source_ids": ["session_authority_status_v1", "session_authority_v1"], "outcome": "SESSION_AUTHORITY_STATUS_CANONICAL"},
            {"priority": 2, "rule_id": "submit_boundary_owns_submission_authorization", "description": "Submit boundary status owns submission authorization interpretation for the current day when subsystem ambiguity is clear.", "source_ids": ["submit_boundary_status_v1"], "outcome": "SUBMISSION_AUTHORIZATION_CANONICAL"},
            {"priority": 3, "rule_id": "legacy_readiness_surfaces_are_advisory", "description": "capability_state_v1, paper_policy_verdict_v1, gate_stack_verdict_v1, and legacy operator_summary_v1 remain visible for diagnostics but do not override the canonical operator summary.", "source_ids": ["capability_state_v1", "paper_policy_verdict_v1", "gate_stack_verdict_v1", "operator_summary_v1"], "outcome": "ADVISORY_ONLY"},
        ],
        invalidation_rules=[
            {"reason_code": RC_EXECUTION_ROOT_SLEEVE_ID_MISSING, "condition": "Execution authority could not prove sleeve identity.", "effect": "BLOCK_OPERATOR_SUMMARY"},
            {"reason_code": RC_EXECUTION_ROOT_MODE_MISSING, "condition": "Execution authority could not prove mode.", "effect": "BLOCK_OPERATOR_SUMMARY"},
            {"reason_code": RC_EXECUTION_ROOT_PATH_UNRESOLVED, "condition": "Execution authority could not resolve the canonical sleeve execution root path.", "effect": "BLOCK_OPERATOR_SUMMARY"},
            {"reason_code": RC_EXECUTION_ROOT_PATH_MISMATCH, "condition": "Execution authority detected a runtime path mismatch against the canonical sleeve execution root.", "effect": "BLOCK_OPERATOR_SUMMARY"},
            {"reason_code": RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN, "condition": "Execution authority detected a forbidden global/shared execution-root reference.", "effect": "BLOCK_OPERATOR_SUMMARY"},
            {"reason_code": RC_EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS, "condition": "Execution profile authority remains ambiguous.", "effect": "BLOCK_OPERATOR_SUMMARY"},
            {"reason_code": RC_ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS, "condition": "Account trading-policy authority remains ambiguous.", "effect": "BLOCK_OPERATOR_SUMMARY"},
        ],
        degraded_states=[
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_ROOT_SLEEVE_ID_MISSING, "meaning": "Operator summary must remain blocked while execution root sleeve identity is missing."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_ROOT_MODE_MISSING, "meaning": "Operator summary must remain blocked while execution root mode is missing."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_ROOT_PATH_UNRESOLVED, "meaning": "Operator summary must remain blocked while the sleeve execution root path is unresolved."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_ROOT_PATH_MISMATCH, "meaning": "Operator summary must remain blocked while runtime paths disagree with the canonical sleeve execution root."},
            {"state": STATE_BLOCKED, "reason_code": RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN, "meaning": "Operator summary must remain blocked while a forbidden global/shared execution-root reference remains active."},
            {"state": STATE_ADVISORY_ONLY, "reason_code": "", "meaning": "Legacy readiness surfaces remain visible but non-authoritative."},
        ],
        fail_closed_on_ambiguity=True,
        operator_summary_ref=status_ref,
    )
    return manifests


def build_execution_dossier_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    environment: str = "PAPER",
) -> Dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).resolve()
    day = parse_day_utc_v1(day_utc)
    owner_ref = _artifact_ref(resolve_subsystem_authority_manifest_path(truth_root=truth_root, subsystem_id=SUBSYSTEM_ID_EXECUTION_AUTHORITY))
    sleeve_row = _active_paper_sleeve_row(repo_root)
    sleeve_id = str(sleeve_row.get("sleeve_id") or "").strip().upper()
    mode = str(sleeve_row.get("mode") or str(environment or "")).strip().upper()
    runtime_contract_truth_sleeves_root = (truth_root.parent / "truth_sleeves").resolve()
    expected_execution_root = (
        (runtime_contract_truth_sleeves_root / sleeve_id / mode).resolve()
        if sleeve_id and mode
        else runtime_contract_truth_sleeves_root
    )
    evidence_refs: List[Dict[str, str]] = [
        _path_ref(repo_root / "governance/05_CONTRACTS/C2/execution_root_authority_v1.contract.md", ref_type="CONTRACT", ref_id="execution_root_authority_v1"),
        _path_ref(repo_root / "governance/05_CONTRACTS/C2/multi_account_topology_v1.contract.md", ref_type="CONTRACT", ref_id="multi_account_topology_v1"),
        _path_ref(repo_root / "governance/05_CONTRACTS/C2/truth_partitioning_by_sleeve_v1.contract.md", ref_type="CONTRACT", ref_id="truth_partitioning_by_sleeve_v1"),
        _path_ref(repo_root / "governance/03_CONTRACTS/C2_TRUE_EVIDENCE_SPINE_V2.md", ref_type="CONTRACT", ref_id="C2_TRUE_EVIDENCE_SPINE_V2"),
        _path_ref(repo_root / "governance/05_CONTRACTS/C2/trade_submit_readiness_c2_v1.contract.md", ref_type="CONTRACT", ref_id="trade_submit_readiness_c2_v1"),
        _path_ref(repo_root / "governance/05_CONTRACTS/C2/startup_materialization_v1.contract.md", ref_type="CONTRACT", ref_id="startup_materialization_v1"),
        _path_ref(repo_root / "constellation_2/common/sleeve_execution_root_v1.py", ref_type="SOURCE", ref_id="sleeve_execution_root_v1.py"),
        _path_ref(repo_root / "constellation_2/common/paper_execution_authority_v1.py", ref_type="SOURCE", ref_id="paper_execution_authority_v1.py"),
        _path_ref(repo_root / "constellation_2/phaseC/tools/run_phaseC_preflight_day_v2.py", ref_type="SOURCE", ref_id="run_phaseC_preflight_day_v2.py"),
        _path_ref(expected_execution_root, ref_type="RUNTIME_DIR", ref_id="canonical_execution_root"),
        _path_ref(expected_execution_root / "phaseC_preflight_v1" / day, ref_type="RUNTIME_DIR", ref_id="canonical_phaseC_preflight_day_dir"),
        _path_ref(expected_execution_root / "execution_evidence_v1" / "submissions" / day, ref_type="RUNTIME_DIR", ref_id="canonical_execution_evidence_day_dir"),
        _path_ref(expected_execution_root / "trade_submit_readiness_c2_v1", ref_type="RUNTIME_DIR", ref_id="canonical_trade_submit_readiness_root"),
        _path_ref(truth_root / "phaseC_preflight_v1" / day, ref_type="LEGACY_RUNTIME_DIR", ref_id="legacy_global_phaseC_preflight_day_dir"),
        _path_ref(truth_root / "execution_evidence_v1" / "submissions" / day, ref_type="LEGACY_RUNTIME_DIR", ref_id="legacy_global_execution_evidence_day_dir"),
        _path_ref(truth_root / "trade_submit_readiness_c2_v1", ref_type="LEGACY_RUNTIME_DIR", ref_id="legacy_global_trade_submit_readiness_root"),
    ]
    if not sleeve_id:
        return _dossier_payload(
            schema_id="execution_dossier",
            schema_version="v1",
            day_utc=day,
            subsystem_id=SUBSYSTEM_ID_EXECUTION_AUTHORITY,
            current_state=STATE_BLOCKED,
            owner_ref=owner_ref,
            first_blocker=_first_blocker(
                reason_code=RC_EXECUTION_ROOT_SLEEVE_ID_MISSING,
                summary="Execution authority could not prove an active PAPER sleeve_id.",
                source="execution_authority_manifest_v1",
            ),
            upstream_evidence_refs=evidence_refs,
            ambiguity_state=_ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[]),
            recommended_operator_action="Restore the governed PAPER sleeve binding so sleeve_execution_root_v1 can compute the canonical execution root.",
            extra={
                "canonical_owner_status": EXECUTION_ROOT_AUTHORITY_OWNER,
                "sleeve_id": "",
                "mode": mode,
                "truth_partition": "",
                "execution_root_path": "",
                "precedence_resolution": [
                    "Execution authority is canonical only from sleeve_execution_root_v1 and truth/sleeves/<sleeve_id>/<mode>/...",
                ],
            },
        )
    if not mode:
        return _dossier_payload(
            schema_id="execution_dossier",
            schema_version="v1",
            day_utc=day,
            subsystem_id=SUBSYSTEM_ID_EXECUTION_AUTHORITY,
            current_state=STATE_BLOCKED,
            owner_ref=owner_ref,
            first_blocker=_first_blocker(
                reason_code=RC_EXECUTION_ROOT_MODE_MISSING,
                summary=f"Execution authority could not prove mode for sleeve_id={sleeve_id}.",
                source="execution_authority_manifest_v1",
            ),
            upstream_evidence_refs=evidence_refs,
            ambiguity_state=_ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[]),
            recommended_operator_action="Restore the active PAPER sleeve mode so sleeve_execution_root_v1 can compute the canonical execution root.",
            extra={
                "canonical_owner_status": EXECUTION_ROOT_AUTHORITY_OWNER,
                "sleeve_id": sleeve_id,
                "mode": "",
                "truth_partition": "",
                "execution_root_path": "",
                "precedence_resolution": [
                    "Execution authority is canonical only from sleeve_execution_root_v1 and truth/sleeves/<sleeve_id>/<mode>/...",
                ],
            },
        )

    truth_partition = str(sleeve_row.get("truth_partition") or "").strip().replace("\\", "/").lstrip("/")
    if not truth_partition:
        return _dossier_payload(
            schema_id="execution_dossier",
            schema_version="v1",
            day_utc=day,
            subsystem_id=SUBSYSTEM_ID_EXECUTION_AUTHORITY,
            current_state=STATE_BLOCKED,
            owner_ref=owner_ref,
            first_blocker=_first_blocker(
                reason_code=RC_EXECUTION_ROOT_PATH_UNRESOLVED,
                summary=f"Missing truth_partition for sleeve_id={sleeve_id}.",
                source="sleeve_execution_root_v1",
            ),
            upstream_evidence_refs=evidence_refs,
            ambiguity_state=_ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[]),
            recommended_operator_action="Resolve the governed sleeve execution-root binding and rerun subsystem authority materialization.",
            extra={
                "canonical_owner_status": EXECUTION_ROOT_AUTHORITY_OWNER,
                "sleeve_id": sleeve_id,
                "mode": mode,
                "truth_partition": "",
                "execution_root_path": "",
                "precedence_resolution": [
                    "Execution authority is canonical only from sleeve_execution_root_v1 and truth/sleeves/<sleeve_id>/<mode>/...",
                    "Global/shared execution-root references are legacy and must not govern active execution.",
                ],
            },
        )
    normalized_partition = truth_partition
    if normalized_partition == "truth_sleeves":
        normalized_partition = ""
    elif normalized_partition.startswith("truth_sleeves/"):
        normalized_partition = normalized_partition[len("truth_sleeves/") :]
    resolved_execution_root = (runtime_contract_truth_sleeves_root / normalized_partition).resolve()
    canonical_execution_root = (runtime_contract_truth_sleeves_root / sleeve_id / mode).resolve()
    if resolved_execution_root == truth_root or str(resolved_execution_root).startswith(f"{truth_root}{Path('/')}"):
        blocker_code = RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN
        blocker_summary = f"Resolved execution root points into forbidden global truth_root: path={resolved_execution_root}"
    elif resolved_execution_root != canonical_execution_root:
        blocker_code = RC_EXECUTION_ROOT_PATH_MISMATCH
        blocker_summary = (
            f"Resolved execution root does not match canonical sleeve root: expected={canonical_execution_root}:actual={resolved_execution_root}"
        )
    elif not resolved_execution_root.exists() or not resolved_execution_root.is_dir():
        blocker_code = RC_EXECUTION_ROOT_PATH_UNRESOLVED
        blocker_summary = f"Canonical sleeve execution root is missing: path={resolved_execution_root}"
    else:
        blocker_code = ""
        blocker_summary = ""

    if blocker_code:
        return _dossier_payload(
            schema_id="execution_dossier",
            schema_version="v1",
            day_utc=day,
            subsystem_id=SUBSYSTEM_ID_EXECUTION_AUTHORITY,
            current_state=STATE_BLOCKED,
            owner_ref=owner_ref,
            first_blocker=_first_blocker(
                reason_code=blocker_code,
                summary=blocker_summary,
                source="sleeve_execution_root_v1",
            ),
            upstream_evidence_refs=evidence_refs,
            ambiguity_state=_ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[]),
            recommended_operator_action="Resolve the governed sleeve execution-root binding and rerun subsystem authority materialization.",
            extra={
                "canonical_owner_status": EXECUTION_ROOT_AUTHORITY_OWNER,
                "sleeve_id": sleeve_id,
                "mode": mode,
                "truth_partition": truth_partition,
                "execution_root_path": str(resolved_execution_root),
                "precedence_resolution": [
                    "Execution authority is canonical only from sleeve_execution_root_v1 and truth/sleeves/<sleeve_id>/<mode>/...",
                    "Global/shared execution-root references are legacy and must not govern active execution.",
                ],
            },
        )

    evidence_refs.extend(
        [
            _path_ref(
                resolved_execution_root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json",
                ref_type="RUNTIME_FILE",
                ref_id=f"sleeve_authorization_gate_verdict_v1:{sleeve_id}",
            ),
            _path_ref(
                resolved_execution_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
                ref_type="RUNTIME_FILE",
                ref_id=f"sleeve_canonical_authority_head:{sleeve_id}",
            ),
            _path_ref(
                repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json",
                ref_type="REGISTRY",
                ref_id="C2_SLEEVE_REGISTRY_V1.json",
            ),
        ]
    )
    return _dossier_payload(
        schema_id="execution_dossier",
        schema_version="v1",
        day_utc=day,
        subsystem_id=SUBSYSTEM_ID_EXECUTION_AUTHORITY,
        current_state=STATE_RESOLVED,
        owner_ref=owner_ref,
        first_blocker=_blank_first_blocker(),
        upstream_evidence_refs=evidence_refs,
        ambiguity_state=_ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[]),
        recommended_operator_action="No operator action required.",
        extra={
            "canonical_owner_status": EXECUTION_ROOT_AUTHORITY_OWNER,
            "sleeve_id": sleeve_id,
            "mode": mode,
            "truth_partition": truth_partition,
            "execution_root_path": str(resolved_execution_root),
            "precedence_resolution": [
                "Execution authority is canonical only from sleeve_execution_root_v1 and truth/sleeves/<sleeve_id>/<mode>/...",
                "Global/shared execution-root references are legacy and must not govern active execution.",
            ],
        },
    )


def build_execution_profile_dossier_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
) -> Dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).resolve()
    day = parse_day_utc_v1(day_utc)
    owner_ref = _artifact_ref(resolve_subsystem_authority_manifest_path(truth_root=truth_root, subsystem_id=SUBSYSTEM_ID_EXECUTION_PROFILE_AUTHORITY))
    sleeve_registry_path = (repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
    orchestrator_path = (repo_root / "ops/tools/run_c2_paper_day_orchestrator_v2.py").resolve()
    runbook_path = (repo_root / "governance/05_CONTRACTS/C2/paper_day_readiness_runbook_v1.contract.md").resolve()
    sleeve_row = _active_paper_sleeve_row(repo_root)
    gateway_profile = sleeve_row.get("ib_gateway_profile") if isinstance(sleeve_row, dict) else {}
    sleeve_id = str(sleeve_row.get("sleeve_id") or "").strip().upper()
    account_id = str(sleeve_row.get("ib_account") or "").strip()
    registry_profile = {
        "host": str((gateway_profile or {}).get("host") or "").strip(),
        "port": str((gateway_profile or {}).get("port") or "").strip(),
        "client_id_orders": str((gateway_profile or {}).get("client_id_orders") or "").strip(),
        "client_id_observer": str((gateway_profile or {}).get("client_id_observer") or "").strip(),
    }
    orchestrator_text = _read_text(orchestrator_path)
    runbook_text = _read_text(runbook_path)
    orchestrator_uses_4002 = '"4002"' in orchestrator_text and '"7"' in orchestrator_text
    runbook_uses_4002 = "--ib_port 4002" in runbook_text and "--ib_client_id 7" in runbook_text
    registry_claims_distinct_profile = registry_profile.get("port") and (
        registry_profile.get("port") != "4002" or registry_profile.get("client_id_orders") != "7"
    )
    ambiguous = orchestrator_uses_4002 and runbook_uses_4002 and registry_claims_distinct_profile
    evidence_refs = [
        _path_ref(repo_root / "governance/05_CONTRACTS/C2/sleeve_registry_v1.contract.md", ref_type="CONTRACT", ref_id="sleeve_registry_v1"),
        _path_ref(repo_root / "governance/05_CONTRACTS/C2/multi_account_topology_v1.contract.md", ref_type="CONTRACT", ref_id="multi_account_topology_v1"),
        _path_ref(sleeve_registry_path, ref_type="REGISTRY", ref_id="C2_SLEEVE_REGISTRY_V1.json"),
        _path_ref(orchestrator_path, ref_type="SOURCE", ref_id="run_c2_paper_day_orchestrator_v2.py"),
        _path_ref(runbook_path, ref_type="CONTRACT", ref_id="paper_day_readiness_runbook_v1"),
        _path_ref(truth_root / "ib_api_handshake" / day / "ib_api_handshake.v1.json", ref_type="RUNTIME_FILE", ref_id="ib_api_handshake_v1"),
    ]
    ambiguity_state = _ambiguity_state(
        status=STATE_AMBIGUOUS_AUTHORITY if ambiguous else STATE_CLEAR,
        summary=(
            "Sleeve registry and orchestrator/runbook defaults disagree on the active IB gateway profile."
            if ambiguous
            else "Execution profile authority is internally consistent."
        ),
        reason_codes=[RC_EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS] if ambiguous else [],
        conflicting_authorities=["C2_SLEEVE_REGISTRY_V1.json", "run_c2_paper_day_orchestrator_v2.py", "paper_day_readiness_runbook_v1.contract.md"] if ambiguous else [],
    )
    return _dossier_payload(
        schema_id="execution_profile_dossier",
        schema_version="v1",
        day_utc=day,
        subsystem_id=SUBSYSTEM_ID_EXECUTION_PROFILE_AUTHORITY,
        current_state=STATE_BLOCKED if ambiguous else STATE_RESOLVED,
        owner_ref=owner_ref,
        first_blocker=_first_blocker(
            reason_code=RC_EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS if ambiguous else "",
            summary=(
                f"Registry profile {registry_profile.get('host') or 'UNKNOWN'}:{registry_profile.get('port') or 'UNKNOWN'} orders={registry_profile.get('client_id_orders') or 'UNKNOWN'} conflicts with 127.0.0.1:4002 client_id=7 defaults."
                if ambiguous
                else ""
            ),
            source="execution_profile_authority_manifest_v1",
        ),
        upstream_evidence_refs=evidence_refs,
        ambiguity_state=ambiguity_state,
        recommended_operator_action=(
            "Resolve whether sleeve-registry gateway profile or orchestrator/runbook defaults are authoritative, then rerun subsystem authority materialization."
            if ambiguous
            else "No operator action required."
        ),
        extra={
            "canonical_owner_status": "AMBIGUOUS_UNPROVEN_CANONICAL_OWNER" if ambiguous else "sleeve_registry_v1",
            "sleeve_id": sleeve_id,
            "account_id": account_id,
            "registry_profile": registry_profile,
            "observed_runtime_profile": {
                "orchestrator_default_host": "127.0.0.1" if orchestrator_uses_4002 else "",
                "orchestrator_default_port": "4002" if orchestrator_uses_4002 else "",
                "orchestrator_default_client_id": "7" if orchestrator_uses_4002 else "",
                "runbook_host": "127.0.0.1" if runbook_uses_4002 else "",
                "runbook_port": "4002" if runbook_uses_4002 else "",
                "runbook_client_id": "7" if runbook_uses_4002 else "",
            },
        },
    )


def build_execution_identity_dossier_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    environment: str = "PAPER",
) -> Dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).resolve()
    day = parse_day_utc_v1(day_utc)
    owner_ref = _artifact_ref(
        resolve_subsystem_authority_manifest_path(
            truth_root=truth_root,
            subsystem_id=SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY,
        )
    )
    sleeve_registry_path = (repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
    account_registry_path = (repo_root / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()
    binding_contract_path = (repo_root / "governance/05_CONTRACTS/C2/execution_identity_binding_v1.contract.md").resolve()
    profile_contract_path = (repo_root / "governance/05_CONTRACTS/C2/execution_profile_authority_v1.contract.md").resolve()
    submit_boundary_path = (repo_root / "constellation_2/phaseD/lib/submit_boundary_paper_v4.py").resolve()
    sleeve_row = _active_paper_sleeve_row(repo_root)
    sleeve_id = str(sleeve_row.get("sleeve_id") or "").strip().upper()
    env = str(environment or "").strip().upper()
    evidence_refs = [
        _path_ref(binding_contract_path, ref_type="CONTRACT", ref_id="execution_identity_binding_v1"),
        _path_ref(profile_contract_path, ref_type="CONTRACT", ref_id="execution_profile_authority_v1"),
        _path_ref(sleeve_registry_path, ref_type="REGISTRY", ref_id="C2_SLEEVE_REGISTRY_V1.json"),
        _path_ref(account_registry_path, ref_type="REGISTRY", ref_id="C2_IB_ACCOUNT_REGISTRY_V1.json"),
        _path_ref(submit_boundary_path, ref_type="SOURCE", ref_id="submit_boundary_paper_v4.py"),
        _path_ref(resolve_execution_profile_dossier_path(truth_root=truth_root, day_utc=day), ref_type="RUNTIME_FILE", ref_id="execution_profile_dossier_v1"),
    ]
    if not sleeve_id:
        return _dossier_payload(
            schema_id="execution_identity_dossier",
            schema_version="v1",
            day_utc=day,
            subsystem_id=SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY,
            current_state=STATE_BLOCKED,
            owner_ref=owner_ref,
            first_blocker=_first_blocker(
                reason_code=RC_EXECUTION_IDENTITY_SLEEVE_MISSING,
                summary="Execution identity binding could not prove an active sleeve_id.",
                source="execution_identity_binding_v1",
            ),
            upstream_evidence_refs=evidence_refs,
            ambiguity_state=_ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[]),
            recommended_operator_action="Restore the active sleeve binding so execution identity can resolve deterministically.",
            extra={
                "canonical_owner_status": EXECUTION_IDENTITY_BINDING_OWNER,
                "binding_status": STATE_BLOCKED,
                "sleeve_id": "",
                "environment": env,
                "account_id": "",
                "client_id_orders": "",
                "client_id_observer": "",
                "host": "",
                "port": "",
            },
        )

    execution_profile_dossier = build_execution_profile_dossier_v1(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day,
    )
    if str(execution_profile_dossier.get("current_state") or "").strip() == STATE_BLOCKED:
        first = execution_profile_dossier.get("first_blocker") or _blank_first_blocker()
        ambiguity_state = dict(execution_profile_dossier.get("ambiguity_state") or {})
        return _dossier_payload(
            schema_id="execution_identity_dossier",
            schema_version="v1",
            day_utc=day,
            subsystem_id=SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY,
            current_state=STATE_BLOCKED,
            owner_ref=owner_ref,
            first_blocker=_first_blocker(
                reason_code=str(first.get("reason_code") or RC_EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS).strip(),
                summary=str(first.get("summary") or "Execution profile authority is unresolved.").strip(),
                source="execution_profile_dossier_v1",
            ),
            upstream_evidence_refs=evidence_refs,
            ambiguity_state=ambiguity_state,
            recommended_operator_action="Resolve execution profile authority before trusting execution identity binding.",
            extra={
                "canonical_owner_status": EXECUTION_IDENTITY_BINDING_OWNER,
                "binding_status": STATE_BLOCKED,
                "sleeve_id": sleeve_id,
                "environment": env,
                "account_id": "",
                "client_id_orders": "",
                "client_id_observer": "",
                "host": "",
                "port": "",
            },
        )

    try:
        identity = resolve_governed_execution_identity_v1(
            repo_root=repo_root,
            environment=env,
            sleeve_id=sleeve_id,
        )
    except ValueError as exc:
        error_text = str(exc)
        reason_code = error_text.split(":", 1)[0].strip() or RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION
        ambiguity_state = _ambiguity_state(
            status=STATE_AMBIGUOUS_AUTHORITY
            if reason_code in {RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS, RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS}
            else STATE_CLEAR,
            summary=error_text if reason_code in {RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS, RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS} else "",
            reason_codes=[reason_code] if reason_code in {RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS, RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS} else [],
            conflicting_authorities=["C2_SLEEVE_REGISTRY_V1.json", "C2_IB_ACCOUNT_REGISTRY_V1.json"]
            if reason_code in {RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS, RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS}
            else [],
        )
        return _dossier_payload(
            schema_id="execution_identity_dossier",
            schema_version="v1",
            day_utc=day,
            subsystem_id=SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY,
            current_state=STATE_BLOCKED,
            owner_ref=owner_ref,
            first_blocker=_first_blocker(
                reason_code=reason_code,
                summary=error_text,
                source="execution_identity_binding_v1",
            ),
            upstream_evidence_refs=evidence_refs,
            ambiguity_state=ambiguity_state,
            recommended_operator_action="Resolve the governed sleeve/account/client identity binding before any submit-capable runtime is trusted.",
            extra={
                "canonical_owner_status": EXECUTION_IDENTITY_BINDING_OWNER,
                "binding_status": STATE_BLOCKED,
                "sleeve_id": sleeve_id,
                "environment": env,
                "account_id": "",
                "client_id_orders": "",
                "client_id_observer": "",
                "host": "",
                "port": "",
            },
        )

    profile_account_id = str(execution_profile_dossier.get("account_id") or "").strip()
    profile_client_id_orders = str(((execution_profile_dossier.get("registry_profile") or {}).get("client_id_orders")) or "").strip()
    if profile_account_id and profile_account_id != identity.account_id:
        return _dossier_payload(
            schema_id="execution_identity_dossier",
            schema_version="v1",
            day_utc=day,
            subsystem_id=SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY,
            current_state=STATE_BLOCKED,
            owner_ref=owner_ref,
            first_blocker=_first_blocker(
                reason_code=RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH,
                summary=f"Execution profile dossier account_id={profile_account_id} differs from governed execution identity account_id={identity.account_id}.",
                source="execution_profile_dossier_v1",
            ),
            upstream_evidence_refs=evidence_refs,
            ambiguity_state=_ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[]),
            recommended_operator_action="Resolve execution profile versus governed identity account mismatch before trusting submit routing.",
            extra={
                "canonical_owner_status": EXECUTION_IDENTITY_BINDING_OWNER,
                "binding_status": STATE_BLOCKED,
                "sleeve_id": identity.sleeve_id,
                "environment": identity.environment,
                "account_id": identity.account_id,
                "client_id_orders": str(identity.client_id_orders),
                "client_id_observer": str(identity.client_id_observer),
                "host": identity.host,
                "port": str(identity.port),
            },
        )
    if profile_client_id_orders and profile_client_id_orders != str(identity.client_id_orders):
        return _dossier_payload(
            schema_id="execution_identity_dossier",
            schema_version="v1",
            day_utc=day,
            subsystem_id=SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY,
            current_state=STATE_BLOCKED,
            owner_ref=owner_ref,
            first_blocker=_first_blocker(
                reason_code=RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH,
                summary=f"Execution profile dossier client_id_orders={profile_client_id_orders} differs from governed execution identity client_id_orders={identity.client_id_orders}.",
                source="execution_profile_dossier_v1",
            ),
            upstream_evidence_refs=evidence_refs,
            ambiguity_state=_ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[]),
            recommended_operator_action="Resolve execution profile versus governed identity client_id_orders mismatch before trusting submit routing.",
            extra={
                "canonical_owner_status": EXECUTION_IDENTITY_BINDING_OWNER,
                "binding_status": STATE_BLOCKED,
                "sleeve_id": identity.sleeve_id,
                "environment": identity.environment,
                "account_id": identity.account_id,
                "client_id_orders": str(identity.client_id_orders),
                "client_id_observer": str(identity.client_id_observer),
                "host": identity.host,
                "port": str(identity.port),
            },
        )

    return _dossier_payload(
        schema_id="execution_identity_dossier",
        schema_version="v1",
        day_utc=day,
        subsystem_id=SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY,
        current_state=STATE_RESOLVED,
        owner_ref=owner_ref,
        first_blocker=_blank_first_blocker(),
        upstream_evidence_refs=evidence_refs,
        ambiguity_state=_ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[]),
        recommended_operator_action="No operator action required.",
        extra={
            "canonical_owner_status": EXECUTION_IDENTITY_BINDING_OWNER,
            "binding_status": STATE_RESOLVED,
            "sleeve_id": identity.sleeve_id,
            "environment": identity.environment,
            "account_id": identity.account_id,
            "client_id_orders": str(identity.client_id_orders),
            "client_id_observer": str(identity.client_id_observer),
            "host": identity.host,
            "port": str(identity.port),
        },
    )


def build_account_trading_policy_dossier_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    environment: str = "PAPER",
) -> Dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).resolve()
    day = parse_day_utc_v1(day_utc)
    owner_ref = _artifact_ref(resolve_subsystem_authority_manifest_path(truth_root=truth_root, subsystem_id=SUBSYSTEM_ID_ACCOUNT_TRADING_POLICY_AUTHORITY))
    ib_contract_path = (repo_root / "governance/05_CONTRACTS/C2/ib_account_registry_v1.contract.md").resolve()
    symbol_contract_path = (repo_root / "governance/05_CONTRACTS/C2/trading_symbol_policy_authority_v1.contract.md").resolve()
    submit_boundary_path = (repo_root / "constellation_2/phaseD/lib/submit_boundary_paper_v4.py").resolve()
    ib_registry_path = (repo_root / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()
    engine_registry_path = (repo_root / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()
    sleeve_registry_path = (repo_root / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()

    symbol_contract_ok = _contains_all(
        symbol_contract_path,
        [
            "Trading symbol policy is authoritative only from:",
            "ENGINE_MODEL_REGISTRY_V1.json",
        ],
    )
    ib_contract_ok = _contains_all(
        ib_contract_path,
        [
            "Phase D submission boundary MUST NOT use IB account registry `allowed_symbols` as the trading-symbol authority.",
        ],
    )
    code_ok = _contains_all(
        submit_boundary_path,
        [
            "_enforce_engine_symbol_policy(",
            "_read_engine_model_registry(",
        ],
    )
    ib_registry = _read_json_optional(ib_registry_path)
    active_accounts = [
        str(row.get("account_id") or "").strip()
        for row in (ib_registry.get("accounts") or [])
        if isinstance(row, dict)
        and str(row.get("environment") or "").strip().upper() == str(environment).strip().upper()
        and row.get("enabled_for_submission") is True
    ]
    ambiguity = not (symbol_contract_ok and ib_contract_ok and code_ok)
    evidence_refs = [
        _path_ref(symbol_contract_path, ref_type="CONTRACT", ref_id="trading_symbol_policy_authority_v1"),
        _path_ref(ib_contract_path, ref_type="CONTRACT", ref_id="ib_account_registry_v1"),
        _path_ref(submit_boundary_path, ref_type="SOURCE", ref_id="submit_boundary_paper_v4.py"),
        _path_ref(ib_registry_path, ref_type="REGISTRY", ref_id="C2_IB_ACCOUNT_REGISTRY_V1.json"),
        _path_ref(engine_registry_path, ref_type="REGISTRY", ref_id="ENGINE_MODEL_REGISTRY_V1.json"),
        _path_ref(sleeve_registry_path, ref_type="REGISTRY", ref_id="C2_SLEEVE_REGISTRY_V1.json"),
    ]
    ambiguity_state = _ambiguity_state(
        status=STATE_AMBIGUOUS_AUTHORITY if ambiguity else STATE_CLEAR,
        summary=(
            "Contracts or submit-boundary code do not prove that engine symbol policy outranks account or sleeve symbol lists."
            if ambiguity
            else "Engine symbol policy owns tradable symbols; IB account registry owns account eligibility only."
        ),
        reason_codes=[RC_ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS] if ambiguity else [],
        conflicting_authorities=["ENGINE_MODEL_REGISTRY_V1.json", "C2_IB_ACCOUNT_REGISTRY_V1.json", "C2_SLEEVE_REGISTRY_V1.json"] if ambiguity else [],
    )
    return _dossier_payload(
        schema_id="account_trading_policy_dossier",
        schema_version="v1",
        day_utc=day,
        subsystem_id=SUBSYSTEM_ID_ACCOUNT_TRADING_POLICY_AUTHORITY,
        current_state=STATE_BLOCKED if ambiguity else STATE_RESOLVED,
        owner_ref=owner_ref,
        first_blocker=_first_blocker(
            reason_code=RC_ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS if ambiguity else "",
            summary=(
                "Trading-policy ownership is not consistently proven between engine, account, and sleeve sources."
                if ambiguity
                else ""
            ),
            source="account_trading_policy_authority_manifest_v1",
        ),
        upstream_evidence_refs=evidence_refs,
        ambiguity_state=ambiguity_state,
        recommended_operator_action=(
            "Resolve whether tradable-symbol policy is owned by engine, account, or sleeve governance before allowing transmit."
            if ambiguity
            else "No operator action required."
        ),
        extra={
            "canonical_owner_status": "trading_symbol_policy_authority_v1" if not ambiguity else "AMBIGUOUS_UNPROVEN_CANONICAL_OWNER",
            "account_eligibility_owner": "ib_account_registry_v1",
            "submission_policy_owner": "trading_symbol_policy_authority_v1" if not ambiguity else "AMBIGUOUS_UNPROVEN_CANONICAL_OWNER",
            "active_accounts": active_accounts,
        },
    )


def _read_optional_surface_status(path: Path, *, status_field_candidates: Sequence[str], label: str) -> Dict[str, str]:
    payload = _read_json_optional(path)
    status_value = ""
    for field in status_field_candidates:
        status_value = str(payload.get(field) or "").strip()
        if status_value:
            break
    return {
        "source": label,
        "path": str(path),
        "status": status_value,
    }


def build_operator_summary_dossier_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    environment: str = "PAPER",
) -> Dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).resolve()
    day = parse_day_utc_v1(day_utc)
    owner_ref = _artifact_ref(resolve_subsystem_authority_manifest_path(truth_root=truth_root, subsystem_id=SUBSYSTEM_ID_OPERATOR_SUMMARY_AUTHORITY))
    execution_dossier = build_execution_dossier_v1(repo_root=repo_root, truth_root=truth_root, day_utc=day, environment=environment)
    execution_profile_dossier = build_execution_profile_dossier_v1(repo_root=repo_root, truth_root=truth_root, day_utc=day)
    execution_identity_dossier = build_execution_identity_dossier_v1(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day,
        environment=environment,
    )
    account_policy_dossier = build_account_trading_policy_dossier_v1(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day,
        environment=environment,
    )

    active_path = (truth_root / "active_session_v1" / "current.json").resolve()
    admission_path = (truth_root / "target_day_admission_v1" / f"{day}.json").resolve()
    build_path = (truth_root / "target_day_build_v1" / f"{day}.json").resolve()
    submit_boundary_path = (
        truth_root / "reports" / "submit_boundary_status_v1" / day / "submit_boundary_status.v1.json"
    ).resolve()
    active_payload = _read_json_optional(active_path)
    admission_payload = _read_json_optional(admission_path)
    build_payload = _read_json_optional(build_path)
    submit_boundary_payload = _read_json_optional(submit_boundary_path)

    active_day = str(active_payload.get("active_day") or "").strip()
    target_day_admission_status = str(
        admission_payload.get("admission_status")
        or active_payload.get("target_day_admission_status")
        or ""
    ).strip().upper()
    submission_authorization_status = str(submit_boundary_payload.get("boundary_status") or "UNKNOWN").strip().upper() or "UNKNOWN"
    submission_authorized = bool(submit_boundary_payload.get("submission_authorized") is True)
    build_blocker_rows = build_payload.get("blocker_chain") if isinstance(build_payload.get("blocker_chain"), list) else []
    admission_blockers = [
        str(code).strip()
        for code in (admission_payload.get("blocking_reason_codes") or [])
        if str(code).strip()
    ]
    submit_boundary_codes = [
        str(code).strip()
        for code in (submit_boundary_payload.get("blocking_codes") or [])
        if str(code).strip()
    ]

    advisory_signals = [
        {
            "source": "capability_state_v1",
            "classification": "ADVISORY_ONLY",
            "state": _read_optional_surface_status(
                truth_root / "reports" / "capability_state_v1" / day / "capability_state.v1.json",
                status_field_candidates=("overall_status",),
                label="capability_state_v1",
            )["status"]
            or "UNAVAILABLE",
            "summary": "Policy-neutral capability state remains visible but does not override Session Authority or submit-boundary ownership.",
        },
        {
            "source": "paper_policy_verdict_v1",
            "classification": "ADVISORY_ONLY",
            "state": _read_optional_surface_status(
                truth_root / "reports" / "paper_policy_verdict_v1" / day / "paper_policy_verdict.v1.json",
                status_field_candidates=("overall_status",),
                label="paper_policy_verdict_v1",
            )["status"]
            or "UNAVAILABLE",
            "summary": "PAPER policy verdict remains visible for diagnostics; admission authority is already bound through Session Authority.",
        },
        {
            "source": "gate_stack_verdict_v1",
            "classification": "ADVISORY_ONLY",
            "state": _read_optional_surface_status(
                truth_root / "reports" / "gate_stack_verdict_v1" / day / "gate_stack_verdict.v1.json",
                status_field_candidates=("readiness_status", "status", "blocking_class"),
                label="gate_stack_verdict_v1",
            )["status"]
            or "UNAVAILABLE",
            "summary": "Gate-stack verdict remains diagnostic and cannot silently override the operator summary owner.",
        },
        {
            "source": "operator_summary_v1",
            "classification": "LEGACY_DERIVED_ONLY",
            "state": _read_optional_surface_status(
                truth_root / "reports" / "operator_summary_v1" / day / "operator_summary.v1.json",
                status_field_candidates=("summary_state", "final_start_decision"),
                label="operator_summary_v1",
            )["status"]
            or "UNAVAILABLE",
            "summary": "Legacy operator summary remains visible for diagnostics but is not the canonical operator summary authority surface.",
        },
    ]

    blocked_dossiers = [
        dossier
        for dossier in (execution_dossier, execution_profile_dossier, execution_identity_dossier, account_policy_dossier)
        if str(dossier.get("current_state") or "").strip() == STATE_BLOCKED
    ]
    ambiguous_dossiers = [
        dossier
        for dossier in blocked_dossiers
        if str((dossier.get("ambiguity_state") or {}).get("status") or "").strip() == STATE_AMBIGUOUS_AUTHORITY
    ]
    if blocked_dossiers:
        first_dossier = blocked_dossiers[0]
        first = first_dossier.get("first_blocker") or _blank_first_blocker()
        ambiguity_state = _ambiguity_state(
            status=STATE_AMBIGUOUS_AUTHORITY if ambiguous_dossiers else STATE_CLEAR,
            summary=(
                "Subsystem authority ambiguity blocks operator interpretation of execution readiness."
                if ambiguous_dossiers
                else ""
            ),
            reason_codes=[
                str(code).strip()
                for dossier in ambiguous_dossiers
                for code in ((dossier.get("ambiguity_state") or {}).get("reason_codes") or [])
                if str(code).strip()
            ],
            conflicting_authorities=[
                str(item).strip()
                for dossier in ambiguous_dossiers
                for item in ((dossier.get("ambiguity_state") or {}).get("conflicting_authorities") or [])
                if str(item).strip()
            ],
        )
        current_state = STATE_BLOCKED
        first_blocker = {
            "reason_code": str(first.get("reason_code") or "").strip(),
            "summary": str(first.get("summary") or "").strip(),
            "source": str(first.get("source") or "").strip(),
        }
        recommended_action = (
            "Resolve subsystem authority ambiguity before trusting execution readiness or any future submit path."
            if ambiguous_dossiers
            else "Resolve the first blocking subsystem authority condition before trusting execution readiness or any future submit path."
        )
    elif target_day_admission_status != "ADMIT":
        blocker_code = admission_blockers[0] if admission_blockers else str((build_blocker_rows[0] or {}).get("blocker_code") or "ADMISSION_RULE_BLOCKED").strip()
        current_state = STATE_BLOCKED
        first_blocker = _first_blocker(
            reason_code=blocker_code,
            summary="Session Authority has not admitted the target day.",
            source="session_authority_v1",
        )
        ambiguity_state = _ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[])
        recommended_action = "Resolve the Session Authority blocker before trusting execution readiness."
    elif submission_authorization_status != "AUTHORIZED":
        blocker_code = submit_boundary_codes[0] if submit_boundary_codes else "SUBMISSION_AUTHORIZATION_NOT_GRANTED"
        current_state = STATE_BLOCKED
        first_blocker = _first_blocker(
            reason_code=blocker_code,
            summary="Submit boundary has not granted submission authorization for the requested day.",
            source="submit_boundary_status_v1",
        )
        ambiguity_state = _ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[])
        recommended_action = "Resolve the submit-boundary blocker before trusting execution readiness."
    else:
        current_state = STATE_READY
        first_blocker = _blank_first_blocker()
        ambiguity_state = _ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[])
        recommended_action = "No operator action required."

    evidence_refs = [
        _path_ref(active_path, ref_type="RUNTIME_FILE", ref_id="active_session_v1"),
        _path_ref(admission_path, ref_type="RUNTIME_FILE", ref_id="target_day_admission_v1"),
        _path_ref(build_path, ref_type="RUNTIME_FILE", ref_id="target_day_build_v1"),
        _path_ref(submit_boundary_path, ref_type="RUNTIME_FILE", ref_id="submit_boundary_status_v1"),
        _path_ref(resolve_subsystem_authority_manifest_path(truth_root=truth_root, subsystem_id=SUBSYSTEM_ID_EXECUTION_AUTHORITY), ref_type="RUNTIME_FILE", ref_id="execution_authority_manifest_v1"),
        _path_ref(resolve_subsystem_authority_manifest_path(truth_root=truth_root, subsystem_id=SUBSYSTEM_ID_EXECUTION_PROFILE_AUTHORITY), ref_type="RUNTIME_FILE", ref_id="execution_profile_authority_manifest_v1"),
        _path_ref(resolve_subsystem_authority_manifest_path(truth_root=truth_root, subsystem_id=SUBSYSTEM_ID_EXECUTION_IDENTITY_AUTHORITY), ref_type="RUNTIME_FILE", ref_id="execution_identity_authority_manifest_v1"),
        _path_ref(resolve_subsystem_authority_manifest_path(truth_root=truth_root, subsystem_id=SUBSYSTEM_ID_ACCOUNT_TRADING_POLICY_AUTHORITY), ref_type="RUNTIME_FILE", ref_id="account_trading_policy_authority_manifest_v1"),
    ]
    derived_from = [str(row.get("ref_id") or "").strip() for row in evidence_refs if str(row.get("ref_id") or "").strip()]
    payload = _dossier_payload(
        schema_id="operator_summary_dossier",
        schema_version="v1",
        day_utc=day,
        subsystem_id=SUBSYSTEM_ID_OPERATOR_SUMMARY_AUTHORITY,
        current_state=current_state,
        owner_ref=owner_ref,
        first_blocker=first_blocker,
        upstream_evidence_refs=evidence_refs,
        ambiguity_state=ambiguity_state,
        recommended_operator_action=recommended_action,
        extra={
            "authority_owner": "session_authority_status_v1",
            "active_day": active_day,
            "admission_status": target_day_admission_status or "BLOCKED",
            "submission_authorization_status": submission_authorization_status,
            "submission_authorized": submission_authorized,
            "advisory_only_signals": advisory_signals,
            "is_canonical": False,
            "authority_level": AUTHORITY_LEVEL_DERIVED,
            "derived_from": derived_from,
            "legacy_surface_classification": [
                "capability_state_v1=ADVISORY_ONLY",
                "paper_policy_verdict_v1=ADVISORY_ONLY",
                "gate_stack_verdict_v1=ADVISORY_ONLY",
                "operator_summary_v1=LEGACY_DERIVED_ONLY",
            ],
            "subsystem_dossier_refs": [
                _artifact_ref(resolve_execution_dossier_path(truth_root=truth_root, day_utc=day)),
                _artifact_ref(resolve_execution_profile_dossier_path(truth_root=truth_root, day_utc=day)),
                _artifact_ref(resolve_execution_identity_dossier_path(truth_root=truth_root, day_utc=day)),
                _artifact_ref(resolve_account_trading_policy_dossier_path(truth_root=truth_root, day_utc=day)),
            ],
        },
    )
    contract = get_constitutional_artifact_contract_v1(repo_root, OPERATOR_SUMMARY_DOSSIER_ARTIFACT_FAMILY)
    dependency_refs = []
    missing_dependency_artifacts: List[str] = []
    for artifact_id, path in (
        ("active_session_v1", active_path),
        ("target_day_admission_v1", admission_path),
        ("target_day_build_v1", build_path),
        ("submit_boundary_status_v1", submit_boundary_path),
    ):
        ref_dict = _artifact_ref(path)
        sha256 = str(ref_dict.get("artifact_sha256") or "").strip()
        if sha256:
            dependency_refs.append(
                build_governed_dependency_ref_v1(
                    repo_root=repo_root,
                    artifact_id=artifact_id,
                    path=path,
                    sha256=sha256,
                    finality_state=FINALITY_PROVISIONAL,
                )
            )
        else:
            missing_dependency_artifacts.append(artifact_id)
    first_blocker_code = str((payload.get("first_blocker") or {}).get("reason_code") or "").strip()
    blocker_envelope = build_machine_blocker_envelope_v1(
        closure_state=(
            CLOSURE_STATE_COMPLETE
            if payload.get("current_state") == STATE_READY and not missing_dependency_artifacts
            else CLOSURE_STATE_BLOCKED
        ),
        reason_codes=[first_blocker_code] if first_blocker_code else [],
        missing_dependency_artifacts=missing_dependency_artifacts,
    )
    payload.update(blocker_envelope)
    payload["blocking_codes"] = list(blocker_envelope["blocking_codes"])
    payload["constitutional_dependency_declaration"] = build_artifact_dependency_declaration_v1(
        artifact_type=OPERATOR_SUMMARY_DOSSIER_ARTIFACT_FAMILY,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=OPERATOR_SUMMARY_DOSSIER_ARTIFACT_FAMILY,
        declared_dependency_artifacts=[
            str(item).strip()
            for item in (contract.get("required_upstream_dependencies") or [])
            if str(item).strip()
        ],
        dependency_refs=dependency_refs,
    )
    payload["constitutional_lineage"] = build_governed_artifact_lineage_v1(
        artifact_type=OPERATOR_SUMMARY_DOSSIER_ARTIFACT_FAMILY,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=OPERATOR_SUMMARY_DOSSIER_ARTIFACT_FAMILY,
        producer_id="constellation_2.common.subsystem_authority_v1",
        generated_at_utc=str(payload.get("generated_utc") or "").strip(),
        effective_at_utc=str(payload.get("generated_utc") or "").strip(),
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[],
        code_version="",
        run_id=f"operator_summary_dossier:{day}:{environment}",
    )
    return payload


def build_subsystem_authority_payloads_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    environment: str = "PAPER",
) -> Dict[str, Dict[str, Any]]:
    manifests = _build_subsystem_authority_manifests(repo_root=repo_root, truth_root=truth_root)
    payloads: Dict[str, Dict[str, Any]] = dict(manifests)
    payloads["execution_dossier_v1"] = build_execution_dossier_v1(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        environment=environment,
    )
    payloads["execution_profile_dossier_v1"] = build_execution_profile_dossier_v1(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
    )
    payloads["execution_identity_dossier_v1"] = build_execution_identity_dossier_v1(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        environment=environment,
    )
    payloads["account_trading_policy_dossier_v1"] = build_account_trading_policy_dossier_v1(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        environment=environment,
    )
    payloads["operator_summary_dossier_v1"] = build_operator_summary_dossier_v1(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        environment=environment,
    )
    return payloads


def materialize_subsystem_authority_artifacts_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    environment: str = "PAPER",
    write: bool = True,
) -> SubsystemArtifactBundleV1:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).resolve()
    day = parse_day_utc_v1(day_utc)
    payloads = build_subsystem_authority_payloads_v1(
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day,
        environment=environment,
    )
    manifest_refs: Dict[str, SurfaceRefV1] = {}
    dossier_refs: Dict[str, SurfaceRefV1] = {}
    if write:
        assert_constitutional_writer_allowed_v1(
            repo_root,
            OPERATOR_SUMMARY_DOSSIER_ARTIFACT_FAMILY,
            "constellation_2.common.subsystem_authority_v1",
        )
        for subsystem_id in ALL_SUBSYSTEM_IDS:
            payload = payloads[subsystem_id]
            ref = atomic_write_validated_json_v1(
                path=resolve_subsystem_authority_manifest_path(truth_root=truth_root, subsystem_id=subsystem_id),
                payload=payload,
                schema_relpath=SUBSYSTEM_AUTHORITY_MANIFEST_SCHEMA_RELPATH,
            )
            manifest_refs[subsystem_id] = ref
        dossier_refs["execution_dossier_v1"] = atomic_write_validated_json_v1(
            path=resolve_execution_dossier_path(truth_root=truth_root, day_utc=day),
            payload=payloads["execution_dossier_v1"],
            schema_relpath=EXECUTION_DOSSIER_SCHEMA_RELPATH,
        )
        dossier_refs["execution_profile_dossier_v1"] = atomic_write_validated_json_v1(
            path=resolve_execution_profile_dossier_path(truth_root=truth_root, day_utc=day),
            payload=payloads["execution_profile_dossier_v1"],
            schema_relpath=EXECUTION_PROFILE_DOSSIER_SCHEMA_RELPATH,
        )
        dossier_refs["execution_identity_dossier_v1"] = atomic_write_validated_json_v1(
            path=resolve_execution_identity_dossier_path(truth_root=truth_root, day_utc=day),
            payload=payloads["execution_identity_dossier_v1"],
            schema_relpath=EXECUTION_IDENTITY_DOSSIER_SCHEMA_RELPATH,
        )
        dossier_refs["account_trading_policy_dossier_v1"] = atomic_write_validated_json_v1(
            path=resolve_account_trading_policy_dossier_path(truth_root=truth_root, day_utc=day),
            payload=payloads["account_trading_policy_dossier_v1"],
            schema_relpath=ACCOUNT_TRADING_POLICY_DOSSIER_SCHEMA_RELPATH,
        )
        dossier_refs["operator_summary_dossier_v1"] = atomic_write_validated_json_v1(
            path=resolve_operator_summary_dossier_path(truth_root=truth_root, day_utc=day),
            payload=payloads["operator_summary_dossier_v1"],
            schema_relpath=OPERATOR_SUMMARY_DOSSIER_SCHEMA_RELPATH,
        )
        validate_governed_artifact_payload_v1(
            repo_root=repo_root,
            artifact_id=OPERATOR_SUMMARY_DOSSIER_ARTIFACT_FAMILY,
            payload=payloads["operator_summary_dossier_v1"],
            required_finality_states=["provisional", "finalized", "corrected"],
        )
    return SubsystemArtifactBundleV1(
        manifest_refs=manifest_refs,
        dossier_refs=dossier_refs,
        payloads=payloads,
    )


def summarize_subsystem_authority_bundle_v1(bundle: SubsystemArtifactBundleV1, *, day_utc: str) -> Dict[str, Any]:
    operator_summary = bundle.payloads.get("operator_summary_dossier_v1") or {}
    return {
        "day_utc": parse_day_utc_v1(day_utc),
        "operator_summary_current_state": str(operator_summary.get("current_state") or "").strip(),
        "operator_summary_first_blocker": dict(operator_summary.get("first_blocker") or _blank_first_blocker()),
        "operator_summary_ambiguity_state": dict(operator_summary.get("ambiguity_state") or _ambiguity_state(status=STATE_CLEAR, summary="", reason_codes=[], conflicting_authorities=[])),
        "written_manifest_paths": {
            subsystem_id: str(ref.path)
            for subsystem_id, ref in bundle.manifest_refs.items()
        },
        "written_dossier_paths": {
            key: str(ref.path)
            for key, ref in bundle.dossier_refs.items()
        },
    }
