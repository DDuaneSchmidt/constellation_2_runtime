from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.execution_identity_binding_v1 import resolve_governed_execution_identity_v1
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import WriteResultV1, write_file_immutable_v1

REPO_ROOT = Path(__file__).resolve().parents[2]

SNAPSHOT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_snapshot_binding.v1.schema.json"
OPERATOR_HEALTH_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_trade_health.v1.schema.json"
PROVENANCE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_summary_provenance.v1.schema.json"
TIMELINE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_timeline_view.v1.schema.json"
QUEUES_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_trade_health_queues.v1.schema.json"

CORE1_HEALTH_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/broker_observation_health.v1.schema.json"
CORE1_AUDIT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/broker_fact_spine_audit.v1.schema.json"
CORE2_TRADE_IDENTITY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/trade_identity.v1.schema.json"
CORE2_STATE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/incorporated_broker_trade_state.v1.schema.json"
CORE2_DESCRIPTION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciled_trade_description.v1.schema.json"
CORE2_HEALTH_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_health.v1.schema.json"
CORE2_PROVENANCE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_provenance.v1.schema.json"
CORE3_AUTHORITY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/lifecycle_action_authority.v1.schema.json"
CORE3_PROVENANCE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/action_decision_provenance.v1.schema.json"
CORE4_BOUNDARY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_status.v1.schema.json"

CORE5_FAMILY = "operator_trade_health_v1"
CORE5_RULE_VERSION = "operator_rollup_v1"
SNAPSHOT_CONTRACT_ID = "C2_OPERATOR_SNAPSHOT_BINDING_CONTRACT_V1"
ROLLUP_CONTRACT_ID = "C2_OPERATOR_ROLLUP_CONTRACT_V1"
OPERATOR_HEALTH_CONTRACT_ID = "C2_OPERATOR_TRADE_HEALTH_CONTRACT_V1"
PROVENANCE_CONTRACT_ID = "C2_OPERATOR_SUMMARY_PROVENANCE_CONTRACT_V1"
NARRATIVE_CONTRACT_ID = "C2_OPERATOR_NARRATIVE_RENDERING_CONTRACT_V1"

MISSING_REQUIRED = "OPERATOR_SNAPSHOT_REQUIRED_LOWER_CORE_ARTIFACT_MISSING"
SNAPSHOT_INCOHERENT = "OPERATOR_SNAPSHOT_LOWER_CORE_SNAPSHOT_INCOHERENT"
SNAPSHOT_SUPERSEDED = "OPERATOR_SNAPSHOT_LOWER_CORE_SNAPSHOT_SUPERSEDED"
PROVENANCE_INCOMPLETE = "OPERATOR_SNAPSHOT_LOWER_CORE_PROVENANCE_INCOMPLETE"
RULE_VERSION_MISSING = "OPERATOR_SNAPSHOT_ROLLUP_RULE_VERSION_MISSING"
NARRATIVE_INSUFFICIENT = "OPERATOR_NARRATIVE_INSUFFICIENT_PROVENANCE"
TIMELINE_INSUFFICIENT = "OPERATOR_TIMELINE_INSUFFICIENT_ARTIFACT_DIFF"
QUEUE_DEGRADED = "OPERATOR_QUEUE_MEMBERSHIP_DEGRADED_DUE_TO_INCOMPLETE_OPERATOR_HEALTH"
QUEUE_OMITTED = "OPERATOR_QUEUE_OMITTED_DUE_TO_MISSING_CANONICAL_ROLLUP"

ARTIFACT_ROLE_FIELDS: Dict[str, List[str]] = {
    "core1_health": ["current_state", "downstream_trust_verdict", "freshness_status", "blocker_codes", "degraded_codes"],
    "core1_audit": ["current_state", "summary", "latest_observed_utc"],
    "core2_trade_identity": ["trade_identity_id", "ownership_classification", "ambiguity_state", "blocker_state", "blocker_codes"],
    "core2_description": ["lifecycle_status", "protection_status", "reconciliation_descriptive_status", "downstream_posture"],
    "core2_health": ["current_state", "freshness_status", "drift_status", "downstream_action_posture", "blocker_reasons", "degraded_reasons"],
    "core2_provenance": ["rule_versions", "upstream_core1_evidence_refs", "prior_state_change_summary"],
    "core3_authority": ["final_action_posture", "required_actions", "blocked_actions", "action_safety_posture", "first_blocker", "ambiguity_state"],
    "core3_provenance": ["core2_fields_read", "blocker_rules_fired", "conflict_rule_applied", "prior_state_comparison"],
    "core4_boundary": ["boundary_status", "submission_authorized", "blocking_codes", "failed_checks"],
}


@dataclass(frozen=True)
class LoadedArtifactV1:
    role: str
    link: Dict[str, Any]
    payload: Dict[str, Any] | None
    read_status: str
    degradation_codes: tuple[str, ...]


@dataclass(frozen=True)
class OperatorTradeHealthMaterializationV1:
    execution_root_path: Path
    materialization_set_id: str
    trade_identity_id: str
    snapshot_binding_path: Path
    operator_health_path: Path
    provenance_path: Path
    timeline_view_path: Path
    summary: Dict[str, Any]


@dataclass(frozen=True)
class OperatorTradeHealthQueueMaterializationV1:
    execution_root_path: Path
    queue_set_id: str
    report_path: Path
    summary: Dict[str, Any]


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _coerce_utc_text(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        raise ValueError("UTC_TEXT_MISSING")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _artifact_ref(path: Path) -> Dict[str, str]:
    return {"artifact_path": str(path), "artifact_sha256": _sha256_file(path)}


def _artifact_link(*, logical_name: str, source_family: str, path: Path | None, present: bool, missing_reason_code: str = "") -> Dict[str, Any]:
    resolved = Path(path).resolve() if path is not None else Path("")
    return {
        "logical_name": logical_name,
        "source_family": source_family,
        "present": bool(present),
        "artifact_path": str(resolved) if path is not None else "",
        "artifact_sha256": _sha256_file(resolved) if present and path is not None and resolved.exists() and resolved.is_file() else "",
        "missing_reason_code": str(missing_reason_code or ""),
    }


def _normalize_boundary_path(execution_root_path: Path, day_utc: str, core4_boundary_path: str | Path | None) -> Path:
    if core4_boundary_path:
        return Path(core4_boundary_path).expanduser().resolve()
    return (
        Path(execution_root_path).resolve()
        / "reports"
        / "submit_boundary_status_v1"
        / parse_day_utc_v1(day_utc)
        / "submit_boundary_status.v1.json"
    ).resolve()


def _load_artifact(*, repo_root: Path, role: str, logical_name: str, source_family: str, path: Path, schema_relpath: str, required: bool) -> LoadedArtifactV1:
    degradation_codes: List[str] = []
    if not path.exists() or not path.is_file():
        code = MISSING_REQUIRED if required else PROVENANCE_INCOMPLETE
        degradation_codes.append(code)
        return LoadedArtifactV1(
            role=role,
            link=_artifact_link(logical_name=logical_name, source_family=source_family, path=path, present=False, missing_reason_code=code),
            payload=None,
            read_status="MISSING",
            degradation_codes=tuple(degradation_codes),
        )
    try:
        payload = _read_json_obj(path)
        validate_against_repo_schema_v1(payload, repo_root, schema_relpath)
    except Exception:
        code = MISSING_REQUIRED if required else PROVENANCE_INCOMPLETE
        degradation_codes.append(code)
        return LoadedArtifactV1(
            role=role,
            link=_artifact_link(logical_name=logical_name, source_family=source_family, path=path, present=False, missing_reason_code=code),
            payload=None,
            read_status="INVALID",
            degradation_codes=tuple(degradation_codes),
        )
    return LoadedArtifactV1(
        role=role,
        link=_artifact_link(logical_name=logical_name, source_family=source_family, path=path, present=True),
        payload=payload,
        read_status="READ",
        degradation_codes=tuple(),
    )


def _write_validated_immutable_json(*, repo_root: Path, path: Path, payload: Dict[str, Any], schema_relpath: str) -> WriteResultV1:
    validate_against_repo_schema_v1(payload, repo_root, schema_relpath)
    return write_file_immutable_v1(path=path, data=canonical_json_bytes_v1(payload) + b"\n", create_dirs=True)


def _resolve_core5_root(execution_root_path: Path) -> Path:
    return (Path(execution_root_path).resolve() / CORE5_FAMILY).resolve()


def _resolve_trade_dir(*, execution_root_path: Path, day_utc: str, materialization_set_id: str, trade_identity_id: str) -> Path:
    return (_resolve_core5_root(execution_root_path) / "materializations" / parse_day_utc_v1(day_utc) / materialization_set_id / "trades" / trade_identity_id).resolve()


def _resolve_queue_report_path(*, execution_root_path: Path, day_utc: str, queue_set_id: str) -> Path:
    return (
        Path(execution_root_path).resolve()
        / "reports"
        / "operator_trade_health_queues_v1"
        / parse_day_utc_v1(day_utc)
        / queue_set_id
        / "operator_trade_health_queues.v1.json"
    ).resolve()


def _resolve_core2_paths(execution_root_path: Path, day_utc: str, core2_materialization_set_id: str, trade_identity_id: str) -> Dict[str, Path]:
    trade_dir = (
        Path(execution_root_path).resolve()
        / "reconciled_trade_state_v1"
        / "materializations"
        / parse_day_utc_v1(day_utc)
        / str(core2_materialization_set_id).strip()
        / "trades"
        / trade_identity_id
    ).resolve()
    return {
        "trade_identity": (trade_dir / "trade_identity.v1.json").resolve(),
        "state": (trade_dir / "incorporated_broker_trade_state.v1.json").resolve(),
        "description": (trade_dir / "reconciled_trade_description.v1.json").resolve(),
        "health": (trade_dir / "reconciliation_health.v1.json").resolve(),
        "provenance": (trade_dir / "reconciliation_provenance.v1.json").resolve(),
    }


def _resolve_core3_paths(execution_root_path: Path, day_utc: str, core3_materialization_set_id: str, trade_identity_id: str) -> Dict[str, Path]:
    trade_dir = (
        Path(execution_root_path).resolve()
        / "lifecycle_action_authority_v1"
        / "materializations"
        / parse_day_utc_v1(day_utc)
        / str(core3_materialization_set_id).strip()
        / "trades"
        / trade_identity_id
    ).resolve()
    return {
        "authority": (trade_dir / "lifecycle_action_authority.v1.json").resolve(),
        "provenance": (trade_dir / "action_decision_provenance.v1.json").resolve(),
    }


def _resolve_core1_paths(execution_root_path: Path, day_utc: str) -> Dict[str, Path]:
    root = Path(execution_root_path).resolve()
    day = parse_day_utc_v1(day_utc)
    return {
        "health": (root / "reports" / "broker_observation_health_v1" / day / "broker_observation_health.v1.json").resolve(),
        "audit": (root / "reports" / "broker_fact_spine_audit_v1" / day / "broker_fact_spine_audit.v1.json").resolve(),
    }


def _max_nonempty(values: Iterable[str]) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _load_lower_core_inputs(
    *,
    repo_root: Path,
    execution_root_path: Path,
    day_utc: str,
    trade_identity_id: str,
    core2_materialization_set_id: str,
    core3_materialization_set_id: str,
    core4_boundary_path: str | Path | None,
) -> Dict[str, LoadedArtifactV1]:
    core1_paths = _resolve_core1_paths(execution_root_path, day_utc)
    core2_paths = _resolve_core2_paths(execution_root_path, day_utc, core2_materialization_set_id, trade_identity_id)
    core3_paths = _resolve_core3_paths(execution_root_path, day_utc, core3_materialization_set_id, trade_identity_id)
    boundary_path = _normalize_boundary_path(execution_root_path, day_utc, core4_boundary_path)
    return {
        "core1_health": _load_artifact(repo_root=repo_root, role="core1_health", logical_name="broker_observation_health_v1", source_family="core1", path=core1_paths["health"], schema_relpath=CORE1_HEALTH_SCHEMA, required=True),
        "core1_audit": _load_artifact(repo_root=repo_root, role="core1_audit", logical_name="broker_fact_spine_audit_v1", source_family="core1", path=core1_paths["audit"], schema_relpath=CORE1_AUDIT_SCHEMA, required=False),
        "core2_trade_identity": _load_artifact(repo_root=repo_root, role="core2_trade_identity", logical_name="trade_identity_v1", source_family="core2", path=core2_paths["trade_identity"], schema_relpath=CORE2_TRADE_IDENTITY_SCHEMA, required=True),
        "core2_state": _load_artifact(repo_root=repo_root, role="core2_state", logical_name="incorporated_broker_trade_state_v1", source_family="core2", path=core2_paths["state"], schema_relpath=CORE2_STATE_SCHEMA, required=True),
        "core2_description": _load_artifact(repo_root=repo_root, role="core2_description", logical_name="reconciled_trade_description_v1", source_family="core2", path=core2_paths["description"], schema_relpath=CORE2_DESCRIPTION_SCHEMA, required=True),
        "core2_health": _load_artifact(repo_root=repo_root, role="core2_health", logical_name="reconciliation_health_v1", source_family="core2", path=core2_paths["health"], schema_relpath=CORE2_HEALTH_SCHEMA, required=True),
        "core2_provenance": _load_artifact(repo_root=repo_root, role="core2_provenance", logical_name="reconciliation_provenance_v1", source_family="core2", path=core2_paths["provenance"], schema_relpath=CORE2_PROVENANCE_SCHEMA, required=True),
        "core3_authority": _load_artifact(repo_root=repo_root, role="core3_authority", logical_name="lifecycle_action_authority_v1", source_family="core3", path=core3_paths["authority"], schema_relpath=CORE3_AUTHORITY_SCHEMA, required=True),
        "core3_provenance": _load_artifact(repo_root=repo_root, role="core3_provenance", logical_name="action_decision_provenance_v1", source_family="core3", path=core3_paths["provenance"], schema_relpath=CORE3_PROVENANCE_SCHEMA, required=True),
        "core4_boundary": _load_artifact(repo_root=repo_root, role="core4_boundary", logical_name="submit_boundary_status_v1", source_family="core4", path=boundary_path, schema_relpath=CORE4_BOUNDARY_SCHEMA, required=True),
    }
def _rule_version_set() -> Dict[str, Any]:
    return {
        "snapshot_contract_id": SNAPSHOT_CONTRACT_ID,
        "snapshot_contract_version": 1,
        "rollup_contract_id": ROLLUP_CONTRACT_ID,
        "rollup_contract_version": 1,
        "operator_health_contract_id": OPERATOR_HEALTH_CONTRACT_ID,
        "operator_health_contract_version": 1,
        "provenance_contract_id": PROVENANCE_CONTRACT_ID,
        "provenance_contract_version": 1,
        "narrative_contract_id": NARRATIVE_CONTRACT_ID,
        "narrative_contract_version": 1,
    }


def _link_matches_ref(link: Mapping[str, Any], ref: Mapping[str, Any] | None) -> bool:
    if not ref:
        return False
    return str(link.get("artifact_path") or "") == str(ref.get("artifact_path") or "") and str(link.get("artifact_sha256") or "") == str(ref.get("artifact_sha256") or "")


def _source_links_by_role(artifacts: Mapping[str, LoadedArtifactV1]) -> Dict[str, Dict[str, Any]]:
    return {role: dict(artifact.link) for role, artifact in artifacts.items()}


def _build_snapshot_binding(
    *,
    day_utc: str,
    trade_identity_id: str,
    execution_root_path: Path,
    artifacts: Mapping[str, LoadedArtifactV1],
    evaluation_utc: str,
) -> Dict[str, Any]:
    version_set = _rule_version_set()
    degradation_codes: List[str] = []
    omitted_elements: List[str] = []
    invalidation_status = "NONE"
    binding_status = "BOUND"
    coherence_status = "COHERENT"

    for role, artifact in artifacts.items():
        degradation_codes.extend(str(code) for code in artifact.degradation_codes)
        if artifact.read_status == "MISSING":
            omitted_elements.append(role)
            if invalidation_status == "NONE":
                invalidation_status = "MISSING_REQUIRED_ARTIFACT" if role != "core1_audit" else "INCOMPLETE_PROVENANCE"
        elif artifact.read_status == "INVALID":
            omitted_elements.append(f"{role}:invalid")
            if invalidation_status == "NONE":
                invalidation_status = "MISSING_REQUIRED_ARTIFACT" if role != "core1_audit" else "INCOMPLETE_PROVENANCE"

    core2_identity = artifacts["core2_trade_identity"].payload or {}
    core2_state = artifacts["core2_state"].payload or {}
    core2_health = artifacts["core2_health"].payload or {}
    core2_prov = artifacts["core2_provenance"].payload or {}
    core3_auth = artifacts["core3_authority"].payload or {}
    core4_boundary = artifacts["core4_boundary"].payload or {}
    core1_health = artifacts["core1_health"].payload or {}

    if core2_prov:
        health_ref = (((core2_prov.get("upstream_core1_evidence_refs") or {}).get("health_ref")) or {})
        if artifacts["core1_health"].payload and not _link_matches_ref(artifacts["core1_health"].link, health_ref):
            degradation_codes.append("CORE5_SNAPSHOT_CORE2_PROVENANCE_CORE1_HEALTH_REF_MISMATCH")
    if core2_state:
        health_ref = (((core2_state.get("upstream_core1_evidence_refs") or {}).get("health_ref")) or {})
        if artifacts["core1_health"].payload and not _link_matches_ref(artifacts["core1_health"].link, health_ref):
            degradation_codes.append("CORE5_SNAPSHOT_CORE2_STATE_CORE1_HEALTH_REF_MISMATCH")
    if core3_auth:
        core3_trade_id = _max_nonempty([((core3_auth.get("trade_identity_ref") or {}).get("trade_identity_id")), core3_auth.get("trade_identity_id")])
        if core3_trade_id and trade_identity_id and core3_trade_id != trade_identity_id:
            degradation_codes.append("CORE5_SNAPSHOT_CORE3_TRADE_IDENTITY_MISMATCH")
        upstream_core2 = core3_auth.get("upstream_core2_refs") or {}
        if artifacts["core2_trade_identity"].payload and not _link_matches_ref(artifacts["core2_trade_identity"].link, upstream_core2.get("trade_identity_ref") or {}):
            degradation_codes.append("CORE5_SNAPSHOT_CORE3_CORE2_TRADE_IDENTITY_REF_MISMATCH")
        if artifacts["core2_state"].payload and not _link_matches_ref(artifacts["core2_state"].link, upstream_core2.get("incorporated_state_ref") or {}):
            degradation_codes.append("CORE5_SNAPSHOT_CORE3_CORE2_STATE_REF_MISMATCH")
        if artifacts["core2_description"].payload and not _link_matches_ref(artifacts["core2_description"].link, upstream_core2.get("reconciled_description_ref") or {}):
            degradation_codes.append("CORE5_SNAPSHOT_CORE3_CORE2_DESCRIPTION_REF_MISMATCH")
        if artifacts["core2_health"].payload and not _link_matches_ref(artifacts["core2_health"].link, upstream_core2.get("reconciliation_health_ref") or {}):
            degradation_codes.append("CORE5_SNAPSHOT_CORE3_CORE2_HEALTH_REF_MISMATCH")
        if artifacts["core2_provenance"].payload and not _link_matches_ref(artifacts["core2_provenance"].link, upstream_core2.get("reconciliation_provenance_ref") or {}):
            degradation_codes.append("CORE5_SNAPSHOT_CORE3_CORE2_PROVENANCE_REF_MISMATCH")
    if core4_boundary:
        if str(core4_boundary.get("day_utc") or "") != day_utc:
            degradation_codes.append("CORE5_SNAPSHOT_CORE4_DAY_MISMATCH")
        core2_account = _max_nonempty([core2_identity.get("account_id"), core2_state.get("account_id")])
        if core2_account and str(core4_boundary.get("paper_account") or "") and str(core4_boundary.get("paper_account") or "") != core2_account:
            degradation_codes.append("CORE5_SNAPSHOT_CORE4_ACCOUNT_MISMATCH")
    if core1_health and core2_health:
        if str(core1_health.get("downstream_trust_verdict") or "") == "BLOCKED" and str(core2_health.get("current_state") or "") == "TRUSTED":
            degradation_codes.append("CORE5_SNAPSHOT_CORE1_CORE2_HEALTH_INCOHERENT")
    if invalidation_status == "MISSING_REQUIRED_ARTIFACT":
        binding_status = "BLOCKED"
        coherence_status = "DEGRADED"
    elif invalidation_status == "INCOMPLETE_PROVENANCE":
        binding_status = "DEGRADED"
        coherence_status = "DEGRADED"

    if any(code.startswith("CORE5_SNAPSHOT_") for code in degradation_codes):
        coherence_status = "INCOHERENT"
        binding_status = "BLOCKED"
        invalidation_status = "INCOHERENT_CROSS_CORE_INPUTS" if invalidation_status == "NONE" else invalidation_status
    elif artifacts["core1_audit"].read_status != "READ":
        coherence_status = "DEGRADED"
        binding_status = "DEGRADED"
        if invalidation_status == "NONE":
            invalidation_status = "INCOMPLETE_PROVENANCE"
            degradation_codes.append(PROVENANCE_INCOMPLETE)

    seed = json.dumps(
        {
            "trade_identity_id": trade_identity_id,
            "day_utc": day_utc,
            "evaluation_utc": evaluation_utc,
            "refs": {role: artifact.link for role, artifact in sorted(artifacts.items())},
            "rule_version_set": version_set,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    binding_id = _sha256_text(seed)
    return {
        "schema_id": "operator_snapshot_binding",
        "schema_version": "v1",
        "authority_owner": "operator_snapshot_binding_v1",
        "binding_id": binding_id,
        "trade_identity_id": trade_identity_id,
        "day_utc": day_utc,
        "evaluated_at_utc": evaluation_utc,
        "environment": _max_nonempty([core2_identity.get("environment"), core2_state.get("environment"), core3_auth.get("environment")]),
        "sleeve_id": _max_nonempty([core2_identity.get("sleeve_id"), core2_state.get("sleeve_id"), core3_auth.get("sleeve_id")]),
        "account_id": _max_nonempty([core2_identity.get("account_id"), core2_state.get("account_id"), core3_auth.get("account_id"), core4_boundary.get("paper_account")]),
        "core1_refs": {
            "health_ref": dict(artifacts["core1_health"].link),
            "audit_ref": dict(artifacts["core1_audit"].link),
        },
        "core2_refs": {
            "trade_identity_ref": dict(artifacts["core2_trade_identity"].link),
            "incorporated_state_ref": dict(artifacts["core2_state"].link),
            "reconciled_description_ref": dict(artifacts["core2_description"].link),
            "reconciliation_health_ref": dict(artifacts["core2_health"].link),
            "reconciliation_provenance_ref": dict(artifacts["core2_provenance"].link),
        },
        "core3_refs": {
            "lifecycle_action_authority_ref": dict(artifacts["core3_authority"].link),
            "action_decision_provenance_ref": dict(artifacts["core3_provenance"].link),
        },
        "core4_refs": {
            "submit_boundary_status_ref": dict(artifacts["core4_boundary"].link),
        },
        "rule_version_set": version_set,
        "binding_status": binding_status,
        "coherence_status": coherence_status,
        "invalidation_status": invalidation_status,
        "degradation_codes": sorted(dict.fromkeys(code for code in degradation_codes if code)),
        "omitted_elements": sorted(dict.fromkeys(text for text in omitted_elements if text)),
        "derived_only": True,
    }


def _find_prior_operator_health_path(*, execution_root_path: Path, trade_identity_id: str, current_materialization_set_id: str) -> Path | None:
    root = (_resolve_core5_root(execution_root_path) / "materializations").resolve()
    if not root.exists() or not root.is_dir():
        return None
    candidates: List[tuple[str, Path]] = []
    for path in root.glob(f"*/**/trades/{trade_identity_id}/operator_trade_health.v1.json"):
        if current_materialization_set_id in str(path):
            continue
        try:
            payload = _read_json_obj(path)
        except Exception:
            continue
        candidates.append((str(payload.get("evaluated_at_utc") or ""), path))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def _artifact_link_from_optional_path(logical_name: str, source_family: str, path: Path | None) -> Dict[str, Any]:
    if path is None:
        return _artifact_link(logical_name=logical_name, source_family=source_family, path=None, present=False, missing_reason_code="")
    return _artifact_link(logical_name=logical_name, source_family=source_family, path=path, present=path.exists() and path.is_file(), missing_reason_code="")


def _derive_prior_state_comparison(current_health_base: Mapping[str, Any], prior_path: Path | None) -> Dict[str, Any]:
    if prior_path is None:
        return {
            "comparison_status": "INITIAL_MATERIALIZATION",
            "prior_operator_health_ref": _artifact_link(logical_name="prior_operator_trade_health", source_family="core5", path=None, present=False, missing_reason_code=""),
            "changed_fields": [],
        }
    prior_payload = _read_json_obj(prior_path)
    compared_fields = [
        "overall_operator_state",
        "highest_severity",
        "required_operator_action",
        "review_reason_class",
        "blocked_reason_class",
        "stale_degraded_class",
        "rollup_reason_codes",
    ]
    changed_fields = [field for field in compared_fields if prior_payload.get(field) != current_health_base.get(field)]
    return {
        "comparison_status": "NO_MATERIAL_CHANGE" if not changed_fields else "MATERIAL_CHANGE",
        "prior_operator_health_ref": _artifact_link(logical_name="prior_operator_trade_health", source_family="core5", path=prior_path, present=True),
        "changed_fields": changed_fields,
    }


def _last_material_change_class(prior_comparison: Mapping[str, Any], current_health_base: Mapping[str, Any], prior_path: Path | None) -> str:
    status = str(prior_comparison.get("comparison_status") or "")
    if status == "INITIAL_MATERIALIZATION":
        return "INITIAL_MATERIALIZATION"
    changed_fields = [str(field) for field in (prior_comparison.get("changed_fields") or []) if str(field)]
    if not changed_fields:
        return "NO_MATERIAL_CHANGE"
    if "overall_operator_state" in changed_fields:
        return "OPERATOR_STATE_CHANGED"
    if "highest_severity" in changed_fields:
        return "SEVERITY_CHANGED"
    if "required_operator_action" in changed_fields:
        return "REQUIRED_ACTION_CHANGED"
    if "blocked_reason_class" in changed_fields and (current_health_base.get("blocked_reason_class") == "BLOCKED_BY_BOUNDARY" or (prior_path and _read_json_obj(prior_path).get("blocked_reason_class") == "BLOCKED_BY_BOUNDARY")):
        return "BOUNDARY_STATUS_CHANGED"
    if "review_reason_class" in changed_fields or "blocked_reason_class" in changed_fields:
        return "ACTION_POSTURE_CHANGED"
    if "stale_degraded_class" in changed_fields:
        return "SNAPSHOT_STATUS_CHANGED"
    return "OPERATOR_STATE_CHANGED"
def _derive_rollup(
    *,
    snapshot_binding: Mapping[str, Any],
    artifacts: Mapping[str, LoadedArtifactV1],
    prior_path: Path | None,
    trade_identity_id: str,
    day_utc: str,
    evaluation_utc: str,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    core2_identity = artifacts["core2_trade_identity"].payload or {}
    core2_health = artifacts["core2_health"].payload or {}
    core3_auth = artifacts["core3_authority"].payload or {}
    core4_boundary = artifacts["core4_boundary"].payload or {}

    overall_state = "HEALTHY"
    highest_severity = "INFO"
    required_action = "NONE"
    review_reason_class = "NONE"
    blocked_reason_class = "NONE"
    stale_degraded_class = "NONE"
    rollup_reason_codes: List[str] = []
    rollup_rules_fired: List[str] = []
    severity_rules_fired: List[str] = []
    required_action_rules_fired: List[str] = []
    source_roles_by_field: Dict[str, List[str]] = {}

    def mark(field: str, roles: Sequence[str]) -> None:
        source_roles_by_field.setdefault(field, [])
        for role in roles:
            if role not in source_roles_by_field[field]:
                source_roles_by_field[field].append(role)

    snapshot_incomplete = str(snapshot_binding.get("binding_status") or "") != "BOUND" or str(snapshot_binding.get("coherence_status") or "") != "COHERENT"
    invalidation_status = str(snapshot_binding.get("invalidation_status") or "")
    if snapshot_incomplete:
        overall_state = "DEGRADED_INCOMPLETE"
        highest_severity = "CRITICAL"
        review_reason_class = "SNAPSHOT_COHERENCE_REVIEW_REQUIRED"
        stale_degraded_class = "LOWER_CORE_PROVENANCE_INCOMPLETE" if invalidation_status == "INCOMPLETE_PROVENANCE" else "DEGRADED_SUMMARY_INCOMPLETE_LOWER_CORE_CHAIN"
        required_action = "REBUILD_CROSS_CORE_SNAPSHOT" if invalidation_status == "INCOHERENT_CROSS_CORE_INPUTS" else "INVESTIGATE_PROVENANCE_GAP"
        rollup_rules_fired.append("CORE5_RULE_SNAPSHOT_INCOMPLETE")
        severity_rules_fired.append("CORE5_SEVERITY_CRITICAL_SNAPSHOT_INCOMPLETE")
        required_action_rules_fired.append("CORE5_ACTION_INVESTIGATE_OR_REBUILD_SNAPSHOT")
        rollup_reason_codes.extend(str(code) for code in (snapshot_binding.get("degradation_codes") or []) if str(code))
        mark("overall_operator_state", ["core1_health", "core1_audit", "core2_provenance", "core3_provenance", "core4_boundary"])
        mark("review_reason_class", ["core2_provenance", "core3_provenance"])
        mark("stale_degraded_class", ["core2_provenance", "core3_provenance"])

    freshness_status = str(core2_health.get("freshness_status") or "")
    health_state = str(core2_health.get("current_state") or "")
    drift_status = str(core2_health.get("drift_status") or "")
    if freshness_status == "STALE":
        stale_degraded_class = "STALE_TRUTH_WARNING"
        if overall_state == "HEALTHY":
            overall_state = "ACTION_REQUIRED"
        if required_action == "NONE":
            required_action = "REFRESH_STALE_TRUTH"
        rollup_rules_fired.append("CORE5_RULE_CORE2_STALE_WARNING")
        required_action_rules_fired.append("CORE5_ACTION_REFRESH_STALE_TRUTH")
        mark("stale_degraded_class", ["core2_health"])
    elif freshness_status == "TOO_STALE":
        stale_degraded_class = "STALE_TRUTH_CRITICAL"
        blocked_reason_class = blocked_reason_class if blocked_reason_class != "NONE" else "BLOCKED_BY_RECONCILIATION"
        overall_state = "BLOCKED"
        required_action = "REFRESH_STALE_TRUTH"
        rollup_rules_fired.append("CORE5_RULE_CORE2_STALE_CRITICAL")
        mark("blocked_reason_class", ["core2_health"])
        mark("stale_degraded_class", ["core2_health"])

    if health_state == "BLOCKED" or str(core2_health.get("downstream_action_posture") or "") == "BLOCKED":
        blocked_reason_class = blocked_reason_class if blocked_reason_class != "NONE" else "BLOCKED_BY_RECONCILIATION"
        overall_state = "BLOCKED"
        if required_action == "NONE":
            required_action = "PERFORM_OPERATOR_REVIEW"
        rollup_rules_fired.append("CORE5_RULE_CORE2_RECON_BLOCKED")
        mark("blocked_reason_class", ["core2_health", "core2_description"])
    elif health_state == "DEGRADED" and overall_state == "HEALTHY":
        overall_state = "ACTION_REQUIRED"
        if required_action == "NONE":
            required_action = "PERFORM_OPERATOR_REVIEW"
        rollup_rules_fired.append("CORE5_RULE_CORE2_RECON_DEGRADED")
        mark("overall_operator_state", ["core2_health"])

    if drift_status == "DRIFTED" and review_reason_class == "NONE" and overall_state not in {"BLOCKED", "DEGRADED_INCOMPLETE"}:
        review_reason_class = "DRIFT_EXCEPTION_REVIEW_REQUIRED"
        overall_state = "REVIEW_REQUIRED"
        if required_action == "NONE":
            required_action = "PERFORM_OPERATOR_REVIEW"
        rollup_rules_fired.append("CORE5_RULE_CORE2_DRIFT_REVIEW")
        mark("review_reason_class", ["core2_health"])

    ownership_classification = str(core2_identity.get("ownership_classification") or "")
    if ownership_classification == "FOREIGN_MANUAL" and review_reason_class == "NONE" and overall_state not in {"BLOCKED", "DEGRADED_INCOMPLETE"}:
        review_reason_class = "FOREIGN_MANUAL_REVIEW_REQUIRED"
        overall_state = "REVIEW_REQUIRED"
        if required_action == "NONE":
            required_action = "PERFORM_OPERATOR_REVIEW"
        rollup_rules_fired.append("CORE5_RULE_FOREIGN_MANUAL_REVIEW")
        mark("review_reason_class", ["core2_trade_identity"])

    final_action_posture = str(core3_auth.get("final_action_posture") or "")
    action_safety_posture = str(core3_auth.get("action_safety_posture") or "")
    required_actions = [str(item) for item in (core3_auth.get("required_actions") or []) if str(item)]
    if final_action_posture == "BLOCKED" or action_safety_posture == "BLOCKED":
        blocked_reason_class = "BLOCKED_BY_ACTION_AUTHORITY"
        overall_state = "BLOCKED"
        required_action = "PERFORM_OPERATOR_REVIEW"
        rollup_rules_fired.append("CORE5_RULE_CORE3_BLOCKED")
        mark("blocked_reason_class", ["core3_authority"])
    elif final_action_posture == "ACTION_REQUIRED" or required_actions:
        if overall_state not in {"BLOCKED", "DEGRADED_INCOMPLETE"}:
            overall_state = "ACTION_REQUIRED"
        required_action = "FOLLOW_REQUIRED_ACTION"
        rollup_rules_fired.append("CORE5_RULE_CORE3_ACTION_REQUIRED")
        required_action_rules_fired.append("CORE5_ACTION_FOLLOW_REQUIRED_ACTION")
        mark("required_operator_action", ["core3_authority"])
    elif final_action_posture == "REVIEW_REQUIRED" or action_safety_posture == "DEGRADED_REVIEW_REQUIRED":
        if overall_state not in {"BLOCKED", "DEGRADED_INCOMPLETE"}:
            overall_state = "REVIEW_REQUIRED"
        if review_reason_class == "NONE":
            review_reason_class = "ACTION_AUTHORITY_REVIEW_REQUIRED"
        if required_action == "NONE":
            required_action = "PERFORM_OPERATOR_REVIEW"
        rollup_rules_fired.append("CORE5_RULE_CORE3_REVIEW_REQUIRED")
        mark("review_reason_class", ["core3_authority"])

    boundary_status = str(core4_boundary.get("boundary_status") or "")
    submission_authorized = bool(core4_boundary.get("submission_authorized") is True)
    if core4_boundary and (boundary_status != "AUTHORIZED" or not submission_authorized):
        blocked_reason_class = "BLOCKED_BY_BOUNDARY"
        overall_state = "BLOCKED"
        required_action = "REMEDIATE_BLOCKED_BOUNDARY"
        rollup_rules_fired.append("CORE5_RULE_BOUNDARY_BLOCKED")
        required_action_rules_fired.append("CORE5_ACTION_REMEDIATE_BLOCKED_BOUNDARY")
        rollup_reason_codes.extend(str(code) for code in (core4_boundary.get("blocking_codes") or []) if str(code))
        mark("blocked_reason_class", ["core4_boundary"])

    if overall_state in {"BLOCKED", "DEGRADED_INCOMPLETE"} or stale_degraded_class in {"STALE_TRUTH_CRITICAL", "LOWER_CORE_PROVENANCE_INCOMPLETE"}:
        highest_severity = "CRITICAL"
    elif overall_state in {"ACTION_REQUIRED", "REVIEW_REQUIRED"} or stale_degraded_class == "STALE_TRUTH_WARNING":
        highest_severity = "WARNING"
    else:
        highest_severity = "INFO"
    severity_rules_fired.append(f"CORE5_SEVERITY_{highest_severity}")

    current_base = {
        "schema_id": "operator_trade_health",
        "schema_version": "v1",
        "authority_owner": "operator_trade_health_v1",
        "canonical_owner_status": "CANONICAL_OPERATOR_HEALTH_OWNER",
        "materialization_set_id": str(snapshot_binding.get("binding_id") or ""),
        "trade_identity_id": trade_identity_id,
        "day_utc": day_utc,
        "evaluated_at_utc": evaluation_utc,
        "environment": str(snapshot_binding.get("environment") or ""),
        "sleeve_id": str(snapshot_binding.get("sleeve_id") or ""),
        "account_id": str(snapshot_binding.get("account_id") or ""),
        "overall_operator_state": overall_state,
        "highest_severity": highest_severity,
        "required_operator_action": required_action,
        "review_reason_class": review_reason_class,
        "blocked_reason_class": blocked_reason_class,
        "stale_degraded_class": stale_degraded_class,
        "rollup_reason_codes": sorted(dict.fromkeys(code for code in rollup_reason_codes if code)),
    }
    prior_comparison = _derive_prior_state_comparison(current_base, prior_path)
    current_base["last_material_change_class"] = _last_material_change_class(prior_comparison, current_base, prior_path)
    mark("last_material_change_class", ["core5_prior"])
    return current_base, {
        "rollup_rules_fired": sorted(dict.fromkeys(rollup_rules_fired)),
        "severity_rules_fired": sorted(dict.fromkeys(severity_rules_fired)),
        "required_action_rules_fired": sorted(dict.fromkeys(required_action_rules_fired)),
        "source_roles_by_field": source_roles_by_field,
        "prior_state_comparison": prior_comparison,
    }


def _claim_row(claim_id: str, conclusion_field: str, conclusion_value: str, source_roles: Sequence[str], artifacts: Mapping[str, LoadedArtifactV1], rule_ids: Sequence[str]) -> Dict[str, Any]:
    source_refs = [dict(artifacts[role].link) for role in source_roles if role in artifacts]
    fields: List[str] = []
    for role in source_roles:
        fields.extend(ARTIFACT_ROLE_FIELDS.get(role, []))
    return {
        "claim_id": claim_id,
        "conclusion_field": conclusion_field,
        "conclusion_value": str(conclusion_value),
        "source_refs": source_refs,
        "source_fields": sorted(dict.fromkeys(fields)),
        "rule_ids": sorted(dict.fromkeys(str(rule) for rule in rule_ids if str(rule))),
    }


def _build_summary_provenance(
    *,
    materialization_set_id: str,
    trade_identity_id: str,
    day_utc: str,
    evaluation_utc: str,
    snapshot_binding_ref: Mapping[str, Any],
    snapshot_binding: Mapping[str, Any],
    artifacts: Mapping[str, LoadedArtifactV1],
    current_health_base: Mapping[str, Any],
    rollup_meta: Mapping[str, Any],
) -> Dict[str, Any]:
    lower_core_artifacts_read = [
        {
            "artifact_role": role,
            "artifact_link": dict(artifact.link),
            "read_status": artifact.read_status,
        }
        for role, artifact in artifacts.items()
    ]
    lower_core_fields_consumed = [
        {"artifact_role": role, "fields": list(ARTIFACT_ROLE_FIELDS.get(role, []))}
        for role, artifact in artifacts.items()
        if artifact.payload is not None and ARTIFACT_ROLE_FIELDS.get(role)
    ]
    omitted_rows = [
        {"code": str(code), "summary": str(code)}
        for code in (snapshot_binding.get("degradation_codes") or [])
        if str(code)
    ]
    traceability_claims = [
        _claim_row("overall_operator_state", "overall_operator_state", str(current_health_base.get("overall_operator_state") or ""), rollup_meta.get("source_roles_by_field", {}).get("overall_operator_state", ["core2_health", "core3_authority", "core4_boundary"]), artifacts, rollup_meta.get("rollup_rules_fired", [])),
        _claim_row("highest_severity", "highest_severity", str(current_health_base.get("highest_severity") or ""), ["core2_health", "core3_authority", "core4_boundary"], artifacts, rollup_meta.get("severity_rules_fired", [])),
        _claim_row("required_operator_action", "required_operator_action", str(current_health_base.get("required_operator_action") or ""), rollup_meta.get("source_roles_by_field", {}).get("required_operator_action", ["core3_authority", "core4_boundary"]), artifacts, rollup_meta.get("required_action_rules_fired", [])),
        _claim_row("review_reason_class", "review_reason_class", str(current_health_base.get("review_reason_class") or ""), rollup_meta.get("source_roles_by_field", {}).get("review_reason_class", ["core2_trade_identity", "core2_health", "core3_authority"]), artifacts, rollup_meta.get("rollup_rules_fired", [])),
        _claim_row("blocked_reason_class", "blocked_reason_class", str(current_health_base.get("blocked_reason_class") or ""), rollup_meta.get("source_roles_by_field", {}).get("blocked_reason_class", ["core2_health", "core3_authority", "core4_boundary"]), artifacts, rollup_meta.get("rollup_rules_fired", [])),
        _claim_row("stale_degraded_class", "stale_degraded_class", str(current_health_base.get("stale_degraded_class") or ""), rollup_meta.get("source_roles_by_field", {}).get("stale_degraded_class", ["core2_health", "core2_provenance", "core3_provenance"]), artifacts, rollup_meta.get("rollup_rules_fired", [])),
        _claim_row("last_material_change_class", "last_material_change_class", str(current_health_base.get("last_material_change_class") or ""), ["core5_prior"], artifacts, ["CORE5_RULE_PRIOR_STATE_COMPARISON"]),
    ]
    traceability_claims[-1]["source_refs"] = [dict(rollup_meta.get("prior_state_comparison", {}).get("prior_operator_health_ref") or {})]
    traceability_claims[-1]["source_fields"] = ["overall_operator_state", "highest_severity", "required_operator_action", "review_reason_class", "blocked_reason_class", "stale_degraded_class"]
    return {
        "schema_id": "operator_summary_provenance",
        "schema_version": "v1",
        "authority_owner": "operator_summary_provenance_v1",
        "materialization_set_id": materialization_set_id,
        "trade_identity_id": trade_identity_id,
        "day_utc": day_utc,
        "evaluated_at_utc": evaluation_utc,
        "snapshot_binding_ref": dict(snapshot_binding_ref),
        "lower_core_artifacts_read": lower_core_artifacts_read,
        "lower_core_fields_consumed": lower_core_fields_consumed,
        "rollup_rules_fired": list(rollup_meta.get("rollup_rules_fired") or []),
        "severity_rules_fired": list(rollup_meta.get("severity_rules_fired") or []),
        "required_action_rules_fired": list(rollup_meta.get("required_action_rules_fired") or []),
        "omitted_or_degraded_elements": omitted_rows,
        "snapshot_coherence_result": {
            "binding_status": str(snapshot_binding.get("binding_status") or ""),
            "coherence_status": str(snapshot_binding.get("coherence_status") or ""),
            "invalidation_status": str(snapshot_binding.get("invalidation_status") or ""),
            "degradation_codes": list(snapshot_binding.get("degradation_codes") or []),
        },
        "prior_state_comparison": dict(rollup_meta.get("prior_state_comparison") or {}),
        "traceability_claims": traceability_claims,
        "derived_only": True,
    }


def _build_timeline_view(
    *,
    materialization_set_id: str,
    trade_identity_id: str,
    day_utc: str,
    evaluation_utc: str,
    operator_health_ref: Mapping[str, Any],
    summary_provenance_ref: Mapping[str, Any],
    operator_health: Mapping[str, Any],
    summary_provenance: Mapping[str, Any],
) -> Dict[str, Any]:
    prior_state = summary_provenance.get("prior_state_comparison") or {}
    degraded_reason_codes = [str(row.get("code") or "") for row in (summary_provenance.get("omitted_or_degraded_elements") or []) if str(row.get("code") or "")]
    timeline_classification = "MATERIAL_CHANGE"
    if str(prior_state.get("comparison_status") or "") == "INITIAL_MATERIALIZATION":
        timeline_classification = "INITIAL"
    elif degraded_reason_codes:
        timeline_classification = "DEGRADED"
    elif str(prior_state.get("comparison_status") or "") == "NO_MATERIAL_CHANGE":
        timeline_classification = "STABLE"

    narrative_lines = [
        f"Operator state {operator_health['overall_operator_state']}; severity {operator_health['highest_severity']}; required action {operator_health['required_operator_action']}.",
    ]
    if operator_health.get("blocked_reason_class") != "NONE":
        narrative_lines.append(f"Blocked reason class {operator_health['blocked_reason_class']} remains explicit.")
    if operator_health.get("review_reason_class") != "NONE":
        narrative_lines.append(f"Review reason class {operator_health['review_reason_class']} remains explicit.")
    if operator_health.get("stale_degraded_class") != "NONE":
        narrative_lines.append(f"Stale/degraded class {operator_health['stale_degraded_class']} remains explicit.")
    if degraded_reason_codes:
        narrative_lines.append(f"Narrative degraded due to {','.join(degraded_reason_codes)}.")

    claims = {str(row.get("claim_id") or ""): row for row in (summary_provenance.get("traceability_claims") or []) if isinstance(row, dict)}
    change_entries: List[Dict[str, Any]] = []
    changed_fields = [str(field) for field in (prior_state.get("changed_fields") or []) if str(field)]
    if not changed_fields:
        changed_fields = ["last_material_change_class"]
    for field in changed_fields:
        claim = claims.get(field) or claims.get("last_material_change_class") or {}
        statement = f"{field} changed to {claim.get('conclusion_value') or operator_health.get(field) or operator_health.get('last_material_change_class')}."
        if field == "last_material_change_class" and degraded_reason_codes:
            statement = f"Timeline degraded because {','.join(degraded_reason_codes)}."
        change_entries.append(
            {
                "classification": str(operator_health.get("last_material_change_class") or "NO_MATERIAL_CHANGE"),
                "statement": statement,
                "traceability_claim_ids": [str(claim.get("claim_id") or "last_material_change_class")],
                "lower_core_refs": list(claim.get("source_refs") or []),
            }
        )
    if not change_entries:
        change_entries.append(
            {
                "classification": "NO_MATERIAL_CHANGE",
                "statement": "No material operator-health change.",
                "traceability_claim_ids": ["last_material_change_class"],
                "lower_core_refs": [],
            }
        )
    if not degraded_reason_codes and timeline_classification == "DEGRADED":
        degraded_reason_codes = [TIMELINE_INSUFFICIENT]
    return {
        "schema_id": "operator_timeline_view",
        "schema_version": "v1",
        "authority_owner": "operator_timeline_view_v1",
        "materialization_set_id": materialization_set_id,
        "trade_identity_id": trade_identity_id,
        "day_utc": day_utc,
        "evaluated_at_utc": evaluation_utc,
        "current_operator_health_ref": dict(operator_health_ref),
        "prior_operator_health_ref": dict(prior_state.get("prior_operator_health_ref") or _artifact_link(logical_name="prior_operator_trade_health", source_family="core5", path=None, present=False, missing_reason_code="")),
        "summary_provenance_ref": dict(summary_provenance_ref),
        "timeline_classification": timeline_classification,
        "narrative_lines": narrative_lines,
        "material_change_entries": change_entries,
        "degraded_reason_codes": sorted(dict.fromkeys(code for code in degraded_reason_codes if code)),
        "derived_only": True,
    }


def materialize_operator_trade_health_v1(*, repo_root: Path, execution_root_path: Path, day_utc: str, trade_identity_id: str, core2_materialization_set_id: str, core3_materialization_set_id: str, core4_boundary_path: str | Path | None = None, evaluation_utc: str = "") -> OperatorTradeHealthMaterializationV1:
    repo_root = Path(repo_root).resolve()
    execution_root = Path(execution_root_path).resolve()
    day = parse_day_utc_v1(day_utc)
    effective_evaluation_utc = _coerce_utc_text(evaluation_utc or _now_utc())
    artifacts = _load_lower_core_inputs(
        repo_root=repo_root,
        execution_root_path=execution_root,
        day_utc=day,
        trade_identity_id=trade_identity_id,
        core2_materialization_set_id=core2_materialization_set_id,
        core3_materialization_set_id=core3_materialization_set_id,
        core4_boundary_path=core4_boundary_path,
    )
    snapshot_binding = _build_snapshot_binding(
        day_utc=day,
        trade_identity_id=trade_identity_id,
        execution_root_path=execution_root,
        artifacts=artifacts,
        evaluation_utc=effective_evaluation_utc,
    )
    materialization_set_id = str(snapshot_binding.get("binding_id") or "")
    trade_dir = _resolve_trade_dir(execution_root_path=execution_root, day_utc=day, materialization_set_id=materialization_set_id, trade_identity_id=trade_identity_id)
    snapshot_binding_path = (trade_dir / "operator_snapshot_binding.v1.json").resolve()
    snapshot_write = _write_validated_immutable_json(repo_root=repo_root, path=snapshot_binding_path, payload=snapshot_binding, schema_relpath=SNAPSHOT_SCHEMA)

    prior_path = _find_prior_operator_health_path(execution_root_path=execution_root, trade_identity_id=trade_identity_id, current_materialization_set_id=materialization_set_id)
    current_health_base, rollup_meta = _derive_rollup(snapshot_binding=snapshot_binding, artifacts=artifacts, prior_path=prior_path, trade_identity_id=trade_identity_id, day_utc=day, evaluation_utc=effective_evaluation_utc)
    snapshot_binding_ref = {"artifact_path": str(snapshot_binding_path), "artifact_sha256": snapshot_write.sha256}
    provenance_payload = _build_summary_provenance(
        materialization_set_id=materialization_set_id,
        trade_identity_id=trade_identity_id,
        day_utc=day,
        evaluation_utc=effective_evaluation_utc,
        snapshot_binding_ref=snapshot_binding_ref,
        snapshot_binding=snapshot_binding,
        artifacts=artifacts,
        current_health_base=current_health_base,
        rollup_meta=rollup_meta,
    )
    provenance_path = (trade_dir / "operator_summary_provenance.v1.json").resolve()
    provenance_write = _write_validated_immutable_json(repo_root=repo_root, path=provenance_path, payload=provenance_payload, schema_relpath=PROVENANCE_SCHEMA)

    operator_health_payload = {
        **current_health_base,
        "snapshot_binding_ref": snapshot_binding_ref,
        "summary_provenance_ref": {"artifact_path": str(provenance_path), "artifact_sha256": provenance_write.sha256},
        "lower_core_drill_down_refs": {
            "core1_health_ref": dict(artifacts["core1_health"].link),
            "core1_audit_ref": dict(artifacts["core1_audit"].link),
            "core2_trade_identity_ref": dict(artifacts["core2_trade_identity"].link),
            "core2_incorporated_state_ref": dict(artifacts["core2_state"].link),
            "core2_description_ref": dict(artifacts["core2_description"].link),
            "core2_health_ref": dict(artifacts["core2_health"].link),
            "core2_provenance_ref": dict(artifacts["core2_provenance"].link),
            "core3_authority_ref": dict(artifacts["core3_authority"].link),
            "core3_provenance_ref": dict(artifacts["core3_provenance"].link),
            "core4_boundary_ref": dict(artifacts["core4_boundary"].link),
        },
        "rollup_rule_version": CORE5_RULE_VERSION,
        "derived_only": True,
    }
    operator_health_path = (trade_dir / "operator_trade_health.v1.json").resolve()
    operator_health_write = _write_validated_immutable_json(repo_root=repo_root, path=operator_health_path, payload=operator_health_payload, schema_relpath=OPERATOR_HEALTH_SCHEMA)

    timeline_payload = _build_timeline_view(
        materialization_set_id=materialization_set_id,
        trade_identity_id=trade_identity_id,
        day_utc=day,
        evaluation_utc=effective_evaluation_utc,
        operator_health_ref={"artifact_path": str(operator_health_path), "artifact_sha256": operator_health_write.sha256},
        summary_provenance_ref={"artifact_path": str(provenance_path), "artifact_sha256": provenance_write.sha256},
        operator_health=operator_health_payload,
        summary_provenance=provenance_payload,
    )
    timeline_path = (trade_dir / "operator_timeline_view.v1.json").resolve()
    _ = _write_validated_immutable_json(repo_root=repo_root, path=timeline_path, payload=timeline_payload, schema_relpath=TIMELINE_SCHEMA)
    return OperatorTradeHealthMaterializationV1(
        execution_root_path=execution_root,
        materialization_set_id=materialization_set_id,
        trade_identity_id=trade_identity_id,
        snapshot_binding_path=snapshot_binding_path,
        operator_health_path=operator_health_path,
        provenance_path=provenance_path,
        timeline_view_path=timeline_path,
        summary=operator_health_payload,
    )


def materialize_operator_trade_health_for_scope_v1(*, repo_root: Path, environment: str, sleeve_id: str, day_utc: str, trade_identity_id: str, core2_materialization_set_id: str, core3_materialization_set_id: str, core4_boundary_path: str | Path | None = None, evaluation_utc: str = "") -> OperatorTradeHealthMaterializationV1:
    identity = resolve_governed_execution_identity_v1(repo_root=Path(repo_root).resolve(), environment=environment, sleeve_id=sleeve_id)
    execution_root = resolve_sleeve_execution_root_v1(repo_root=Path(repo_root).resolve(), environment=identity.environment, ib_account=identity.account_id, sleeve_id=identity.sleeve_id)
    return materialize_operator_trade_health_v1(
        repo_root=repo_root,
        execution_root_path=execution_root.execution_root_path,
        day_utc=day_utc,
        trade_identity_id=trade_identity_id,
        core2_materialization_set_id=core2_materialization_set_id,
        core3_materialization_set_id=core3_materialization_set_id,
        core4_boundary_path=core4_boundary_path,
        evaluation_utc=evaluation_utc,
    )


def materialize_operator_trade_health_queues_v1(*, repo_root: Path, execution_root_path: Path, day_utc: str) -> OperatorTradeHealthQueueMaterializationV1:
    repo_root = Path(repo_root).resolve()
    execution_root = Path(execution_root_path).resolve()
    day = parse_day_utc_v1(day_utc)
    health_paths = sorted((_resolve_core5_root(execution_root) / "materializations" / day).glob("*/trades/*/operator_trade_health.v1.json"))
    blocked_index: List[Dict[str, Any]] = []
    review_required_index: List[Dict[str, Any]] = []
    stale_degraded_index: List[Dict[str, Any]] = []
    drift_exception_index: List[Dict[str, Any]] = []
    unauthorized_transmit_attempt_index: List[Dict[str, Any]] = []
    recent_change_queue: List[Dict[str, Any]] = []
    queue_degradation_codes: List[str] = []
    counts = {"HEALTHY": 0, "ACTION_REQUIRED": 0, "REVIEW_REQUIRED": 0, "BLOCKED": 0, "DEGRADED_INCOMPLETE": 0}

    for path in health_paths:
        try:
            payload = _read_json_obj(path)
            validate_against_repo_schema_v1(payload, repo_root, OPERATOR_HEALTH_SCHEMA)
        except Exception:
            queue_degradation_codes.append(QUEUE_DEGRADED)
            continue
        prov_ref = payload.get("summary_provenance_ref") or {}
        prov_path = Path(str(prov_ref.get("artifact_path") or "")).resolve()
        if not prov_path.exists() or not prov_path.is_file():
            queue_degradation_codes.append(QUEUE_OMITTED)
            continue
        row = {
            "trade_identity_id": str(payload.get("trade_identity_id") or ""),
            "operator_health_ref": {"artifact_path": str(path), "artifact_sha256": _sha256_file(path)},
            "summary_provenance_ref": {"artifact_path": str(prov_path), "artifact_sha256": _sha256_file(prov_path)},
            "queue_reason_class": str(payload.get("blocked_reason_class") or payload.get("review_reason_class") or payload.get("stale_degraded_class") or "NONE"),
            "highest_severity": str(payload.get("highest_severity") or "INFO"),
            "degraded": bool(payload.get("overall_operator_state") == "DEGRADED_INCOMPLETE" or payload.get("stale_degraded_class") not in {None, "", "NONE"}),
        }
        counts[str(payload.get("overall_operator_state") or "DEGRADED_INCOMPLETE")] = counts.get(str(payload.get("overall_operator_state") or "DEGRADED_INCOMPLETE"), 0) + 1
        if payload.get("overall_operator_state") == "BLOCKED":
            blocked_index.append(dict(row))
        if payload.get("overall_operator_state") == "REVIEW_REQUIRED" or payload.get("review_reason_class") not in {None, "", "NONE"}:
            review_required_index.append(dict(row))
        if payload.get("stale_degraded_class") not in {None, "", "NONE"}:
            stale_degraded_index.append(dict(row))
        if payload.get("review_reason_class") == "DRIFT_EXCEPTION_REVIEW_REQUIRED":
            drift_exception_index.append(dict(row))
        if "UNAUTHORIZED_TRANSMIT_ATTEMPT" in set(payload.get("rollup_reason_codes") or []):
            unauthorized_transmit_attempt_index.append(dict(row))
        if payload.get("last_material_change_class") not in {None, "", "NO_MATERIAL_CHANGE"}:
            recent_change_queue.append(dict(row))

    seed = json.dumps(
        {
            "day_utc": day,
            "health_refs": [{"path": str(path), "sha256": _sha256_file(path)} for path in health_paths if path.exists() and path.is_file()],
            "rollup_rule_version": CORE5_RULE_VERSION,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    queue_set_id = _sha256_text(seed)
    report_payload = {
        "schema_id": "operator_trade_health_queues",
        "schema_version": "v1",
        "authority_owner": "operator_trade_health_queues_v1",
        "queue_set_id": queue_set_id,
        "day_utc": day,
        "generated_at_utc": _now_utc(),
        "execution_root_path": str(execution_root),
        "rollup_rule_version": CORE5_RULE_VERSION,
        "queue_membership_basis": "OPERATOR_TRADE_HEALTH_AND_PROVENANCE_ONLY",
        "blocked_index": blocked_index,
        "review_required_index": review_required_index,
        "stale_degraded_index": stale_degraded_index,
        "drift_exception_index": drift_exception_index,
        "unauthorized_transmit_attempt_index": unauthorized_transmit_attempt_index,
        "recent_change_queue": recent_change_queue,
        "queue_degradation_codes": sorted(dict.fromkeys(code for code in queue_degradation_codes if code)),
        "session_summary": {
            "total_trade_count": sum(int(value) for value in counts.values()),
            "healthy_count": int(counts.get("HEALTHY", 0)),
            "action_required_count": int(counts.get("ACTION_REQUIRED", 0)),
            "review_required_count": int(counts.get("REVIEW_REQUIRED", 0)),
            "blocked_count": int(counts.get("BLOCKED", 0)),
            "degraded_incomplete_count": int(counts.get("DEGRADED_INCOMPLETE", 0)),
        },
        "derived_only": True,
    }
    report_path = _resolve_queue_report_path(execution_root_path=execution_root, day_utc=day, queue_set_id=queue_set_id)
    _ = _write_validated_immutable_json(repo_root=repo_root, path=report_path, payload=report_payload, schema_relpath=QUEUES_SCHEMA)
    return OperatorTradeHealthQueueMaterializationV1(execution_root_path=execution_root, queue_set_id=queue_set_id, report_path=report_path, summary=report_payload)


__all__ = [
    "CORE5_FAMILY",
    "CORE5_RULE_VERSION",
    "OperatorTradeHealthMaterializationV1",
    "OperatorTradeHealthQueueMaterializationV1",
    "materialize_operator_trade_health_for_scope_v1",
    "materialize_operator_trade_health_v1",
    "materialize_operator_trade_health_queues_v1",
]
