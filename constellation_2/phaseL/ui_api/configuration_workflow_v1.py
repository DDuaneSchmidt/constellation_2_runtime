from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from constellation_2.common.configuration_activation_authority_v1 import (
    POLICY_SNAPSHOT_ARTIFACT_ID,
    POLICY_SNAPSHOT_SCHEMA,
    REQUIRED_GOVERNANCE_UTILITY_RELPATHS,
    WRITER_ID,
    run_configuration_activation_authority_v1,
)
from constellation_2.common.configuration_catalog_v1 import (
    CATALOG_PATH,
    build_active_configuration_v1,
    catalog_entries_by_key_v1,
    default_catalog_values_v1,
    load_configuration_catalog_v1,
    validate_catalog_values_v1,
    write_active_configuration_artifacts_v1,
)
from constellation_2.common.configuration_activation_family_validator_v1 import (
    ConfigurationActivationFamilyValidationError,
    load_validated_configuration_activation_family_v1,
)
from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_FINALIZED,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    resolve_constitutional_artifact_path_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_validated_json_v1,
    repo_git_sha_v1,
    sha256_file_v1,
)
from constellation_2.common.truth_root_v1 import resolve_runtime_root
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


REPO_ROOT = Path(__file__).resolve().parents[3]

DRAFT_SCHEMA_ID = "aegis.configuration_capital_cashflow_draft.v1"
SOURCE_DOCUMENT_SCHEMA_ID = "aegis.capital_cashflow_configuration_input.v1"
SOURCE_DOCUMENT_LOGICAL_NAME = "capital_cashflow_configuration_input_v1"
SOURCE_DOCUMENT_FILENAME = "capital_cashflow_configuration_input.v1.json"

LIFECYCLE_DRAFT = "DRAFT"
LIFECYCLE_VALIDATED = "VALIDATED"
LIFECYCLE_REVIEWED = "REVIEWED"
LIFECYCLE_ACTIVATED = "ACTIVATED"
LIFECYCLE_SUPERSEDED = "SUPERSEDED"
LIFECYCLE_REJECTED = "REJECTED"

VALIDATION_PASS = "PASS"
VALIDATION_FAIL = "FAIL"

REQUIRED_FIELD_NAMES = (
    "scenario",
    "include_inheritance",
    "horizon_months",
    "start_month",
)
EDITABLE_FIELD_NAMES = (
    *tuple(catalog_entries_by_key_v1().keys()),
)
ADVISORY_FIELD_NAMES = ("operator_parameters",)
LEGACY_PARAMETER_ALIASES = {
    "portfolio_headroom_multiplier_bp": "discovery_headroom_multiplier_bp",
}

LOCKED_FIELD_ROWS = (
    {
        "parameter_name": "kill_switch.state",
        "state": "locked-by-design",
        "owning_domain": "safety_control_plane",
        "reason": "Safety-critical kill switch authority remains outside UI configuration drafts.",
        "lock_class": "LOCKED_BY_DESIGN",
    },
    {
        "parameter_name": "broker.transmit_arming",
        "state": "locked-by-design",
        "owning_domain": "submit_boundary",
        "reason": "Broker transmit arming remains governed by submit-boundary authority artifacts.",
        "lock_class": "LOCKED_BY_DESIGN",
    },
    {
        "parameter_name": "submission_authorization_status",
        "state": "visible-only",
        "owning_domain": "session_authority",
        "reason": "Submission authorization is a derived authority outcome and is never UI-editable.",
        "lock_class": "DERIVED_READONLY",
    },
    {
        "parameter_name": "trade_submit_readiness.attestation_outputs",
        "state": "visible-only",
        "owning_domain": "readiness",
        "reason": "Readiness attestations are runtime-derived surfaces, not editable configuration inputs.",
        "lock_class": "DERIVED_READONLY",
    },
)


class ConfigurationWorkflowApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        reason_codes: list[str],
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = int(status_code)
        self.reason_codes = list(reason_codes)
        self.details = dict(details or {})


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _runtime_root() -> Path:
    raw = os.environ.get("C2_UI_CONFIGURATION_WORKFLOW_ROOT") or ""
    if raw.strip():
        return Path(raw).resolve()
    return resolve_runtime_root().resolve()


def _truth_root() -> Path:
    raw = os.environ.get("C2_UI_CONFIGURATION_TRUTH_ROOT") or ""
    if raw.strip():
        return Path(raw).resolve()
    return (_runtime_root() / "truth").resolve()


def _draft_root() -> Path:
    return (_runtime_root() / "ui_configuration_drafts_v1" / "capital_cashflow").resolve()


def _draft_path(draft_id: str) -> Path:
    safe_id = str(draft_id or "").strip()
    if not safe_id:
        raise ConfigurationWorkflowApiError(
            "Draft id is required.",
            status_code=400,
            reason_codes=["CONFIGURATION_DRAFT_ID_REQUIRED"],
        )
    if "/" in safe_id or "\\" in safe_id:
        raise ConfigurationWorkflowApiError(
            "Draft id is invalid.",
            status_code=400,
            reason_codes=["CONFIGURATION_DRAFT_ID_INVALID"],
        )
    return (_draft_root() / f"{safe_id}.json").resolve()


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError as exc:
        raise ConfigurationWorkflowApiError(
            "Draft not found.",
            status_code=404,
            reason_codes=["CONFIGURATION_DRAFT_NOT_FOUND"],
            details={"draft_path": str(path)},
        ) from exc
    except json.JSONDecodeError as exc:
        raise ConfigurationWorkflowApiError(
            "Draft is unreadable.",
            status_code=409,
            reason_codes=["CONFIGURATION_DRAFT_JSON_INVALID"],
            details={"draft_path": str(path)},
        ) from exc
    if not isinstance(payload, dict):
        raise ConfigurationWorkflowApiError(
            "Draft payload is invalid.",
            status_code=409,
            reason_codes=["CONFIGURATION_DRAFT_PAYLOAD_INVALID"],
            details={"draft_path": str(path)},
        )
    return payload


def _write_json_dict(path: Path, payload: Mapping[str, Any]) -> None:
    resolved = path.resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp = (resolved.parent / f".tmp.{resolved.name}.{uuid.uuid4().hex}").resolve()
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(dict(payload), handle, indent=2, sort_keys=True)
        handle.write("\n")
    tmp.replace(resolved)


def _append_lifecycle_event(draft: Dict[str, Any], *, event: str, details: Optional[Dict[str, Any]] = None) -> None:
    history = draft.get("lifecycle_history")
    if not isinstance(history, list):
        history = []
    history.append(
        {
            "event": str(event),
            "status": str(draft.get("status") or ""),
            "at_utc": _utc_now_iso(),
            "details": dict(details or {}),
        }
    )
    draft["lifecycle_history"] = history


def _ensure_draft_exists(draft_id: str) -> Dict[str, Any]:
    payload = _read_json_dict(_draft_path(draft_id))
    if str(payload.get("schema_id") or "") != DRAFT_SCHEMA_ID:
        raise ConfigurationWorkflowApiError(
            "Draft schema id is invalid.",
            status_code=409,
            reason_codes=["CONFIGURATION_DRAFT_SCHEMA_ID_INVALID"],
            details={"draft_id": draft_id},
        )
    return payload


def _ensure_payload_object(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ConfigurationWorkflowApiError(
            "Request payload must be a JSON object.",
            status_code=400,
            reason_codes=["CONFIGURATION_PAYLOAD_MUST_BE_OBJECT"],
        )
    return dict(payload)


def _extract_proposed_values(payload: Mapping[str, Any]) -> Dict[str, Any]:
    candidate = payload.get("values")
    if isinstance(candidate, dict):
        source = dict(candidate)
    else:
        source = dict(payload)
    values: Dict[str, Any] = {}
    for raw_key, value in source.items():
        key = LEGACY_PARAMETER_ALIASES.get(str(raw_key), str(raw_key))
        if key in ADVISORY_FIELD_NAMES:
            continue
        values[key] = value
    missing = [field for field in REQUIRED_FIELD_NAMES if field not in values]
    if missing:
        raise ConfigurationWorkflowApiError(
            "Draft payload is missing required capital-cashflow fields.",
            status_code=400,
            reason_codes=["CONFIGURATION_DRAFT_FIELDS_MISSING"],
            details={"missing_fields": missing},
        )
    return values


def _validate_start_month(text: str) -> bool:
    try:
        datetime.strptime(f"{text}-01", "%Y-%m-%d")
    except ValueError:
        return False
    return True


def _normalize_operator_parameters(value: Any) -> tuple[list[dict[str, str]], list[str]]:
    if value in (None, ""):
        return [], []
    if not isinstance(value, list):
        return [], ["OPERATOR_PARAMETERS_MUST_BE_ARRAY"]

    rows: list[dict[str, str]] = []
    reason_codes: list[str] = []
    seen: set[str] = set()
    if len(value) > 50:
        reason_codes.append("OPERATOR_PARAMETERS_TOO_MANY")
    for raw in value[:50]:
        if not isinstance(raw, Mapping):
            reason_codes.append("OPERATOR_PARAMETER_ROW_MUST_BE_OBJECT")
            continue
        parameter_name = str(raw.get("parameter_name") or "").strip()
        value_text = str(raw.get("value_text") or "").strip()
        value_kind = str(raw.get("value_kind") or "text").strip().lower() or "text"
        notes = str(raw.get("notes") or "").strip()
        if not parameter_name:
            reason_codes.append("OPERATOR_PARAMETER_NAME_REQUIRED")
            continue
        if len(parameter_name) > 128 or not all(ch.isalnum() or ch in "._:-/" for ch in parameter_name):
            reason_codes.append("OPERATOR_PARAMETER_NAME_INVALID")
            continue
        if parameter_name in seen:
            reason_codes.append("OPERATOR_PARAMETER_NAME_DUPLICATE")
            continue
        if not value_text:
            reason_codes.append("OPERATOR_PARAMETER_VALUE_REQUIRED")
            continue
        if len(value_text) > 512:
            reason_codes.append("OPERATOR_PARAMETER_VALUE_TOO_LONG")
            continue
        if value_kind not in {"text", "number", "money", "boolean", "percent", "json"}:
            reason_codes.append("OPERATOR_PARAMETER_VALUE_KIND_INVALID")
            continue
        if len(notes) > 512:
            reason_codes.append("OPERATOR_PARAMETER_NOTES_TOO_LONG")
            continue
        seen.add(parameter_name)
        rows.append(
            {
                "parameter_name": parameter_name,
                "value_text": value_text,
                "value_kind": value_kind,
                "notes": notes,
            }
        )
    rows.sort(key=lambda item: item["parameter_name"])
    return rows, reason_codes


def _validate_values(values: Mapping[str, Any]) -> tuple[Dict[str, Any], list[str]]:
    validation = validate_catalog_values_v1(values, environment="PAPER", approval_status="VALIDATED")
    return dict(validation.normalized_values), list(validation.reason_codes)


def _fallback_values() -> Dict[str, Any]:
    values = default_catalog_values_v1(environment="PAPER")
    values["start_month"] = datetime.now(timezone.utc).strftime("%Y-%m")
    return values


def _source_document_path(*, truth_root: Path, draft_id: str) -> Path:
    return (
        truth_root
        / "configuration_inputs_v1"
        / "capital_cashflow"
        / draft_id
        / SOURCE_DOCUMENT_FILENAME
    ).resolve()


def _source_document_refs(path: Path) -> list[dict[str, str]]:
    return [
        {
            "logical_name": SOURCE_DOCUMENT_LOGICAL_NAME,
            "path": str(path.resolve()),
            "sha256": sha256_file_v1(path),
            "document_class": "configuration_source",
        }
    ]


def _governance_utility_refs() -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for relpath in REQUIRED_GOVERNANCE_UTILITY_RELPATHS:
        path = (REPO_ROOT / relpath).resolve()
        if not path.exists() or not path.is_file():
            raise ConfigurationWorkflowApiError(
                "Required governance utility file is missing.",
                status_code=409,
                reason_codes=["CONFIGURATION_GOVERNANCE_UTILITY_MISSING"],
                details={"path": str(path), "logical_name": relpath},
            )
        refs.append(
            {
                "logical_name": relpath,
                "path": str(path),
                "sha256": sha256_file_v1(path),
            }
        )
    return refs


def _write_policy_snapshot(*, truth_root: Path, source_document_path: Path, effective_at_utc: str) -> Path:
    source_refs = _source_document_refs(source_document_path)
    utility_refs = _governance_utility_refs()
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, POLICY_SNAPSHOT_ARTIFACT_ID, WRITER_ID)
    policy_snapshot_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": POLICY_SNAPSHOT_ARTIFACT_ID,
            "generated_at_utc": effective_at_utc,
            "effective_at_utc": effective_at_utc,
            "source_document_refs": source_refs,
            "governance_utility_refs": utility_refs,
        }
    )
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type=POLICY_SNAPSHOT_ARTIFACT_ID,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=POLICY_SNAPSHOT_ARTIFACT_ID,
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=[],
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type=POLICY_SNAPSHOT_ARTIFACT_ID,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=POLICY_SNAPSHOT_ARTIFACT_ID,
        producer_id=WRITER_ID,
        generated_at_utc=effective_at_utc,
        effective_at_utc=effective_at_utc,
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=[],
        policy_snapshot_refs=[],
        code_version=repo_git_sha_v1(),
        run_id=f"policy_snapshot:{policy_snapshot_id}",
    )
    payload = {
        "schema_id": "configuration_policy_snapshot.v1",
        "schema_version": "v1",
        "policy_snapshot_id": policy_snapshot_id,
        "authority_owner": WRITER_ID,
        "generated_at_utc": effective_at_utc,
        "effective_at_utc": effective_at_utc,
        "activation_scope": "runtime",
        "source_document_refs": source_refs,
        "governance_utility_refs": utility_refs,
        "policy_digest_sha256": canonical_hash_for_c2_artifact_v1(
            {
                "source_document_refs": source_refs,
                "governance_utility_refs": utility_refs,
            }
        ),
        "closure_state": "COMPLETE",
        "blocked_reason_codes": [],
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }
    path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=POLICY_SNAPSHOT_ARTIFACT_ID,
        day_utc=effective_at_utc[:10],
        canonical_truth_root=truth_root,
        extra_variables={"policy_snapshot_id": policy_snapshot_id},
    )
    atomic_write_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=POLICY_SNAPSHOT_SCHEMA,
    )
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=POLICY_SNAPSHOT_ARTIFACT_ID,
        payload=payload,
        required_finality_states=[FINALITY_FINALIZED],
    )
    return path


def _active_document_values() -> tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    try:
        family = load_validated_configuration_activation_family_v1(truth_root=_truth_root())
    except ConfigurationActivationFamilyValidationError as exc:
        return None, {"reason_codes": ["CONFIGURATION_STATE_NOT_AVAILABLE"], "detail": str(exc)}
    except Exception as exc:
        return None, {"reason_codes": ["CONFIGURATION_STATE_READ_FAILED"], "detail": f"{type(exc).__name__}:{exc}"}

    refs = family.compiled_active_config_ref.payload.get("resolved_configuration_refs") or []
    selected: Optional[Mapping[str, Any]] = None
    for row in refs:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("logical_name") or "").strip() == SOURCE_DOCUMENT_LOGICAL_NAME:
            selected = row
            break
    if selected is None:
        return None, {"reason_codes": ["CONFIGURATION_SOURCE_DOCUMENT_NOT_FOUND"]}

    path = Path(str(selected.get("path") or "")).resolve()
    if not path.exists() or not path.is_file():
        return None, {"reason_codes": ["CONFIGURATION_SOURCE_DOCUMENT_MISSING"], "source_document_path": str(path)}
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:
        return None, {"reason_codes": ["CONFIGURATION_SOURCE_DOCUMENT_UNREADABLE"], "detail": f"{type(exc).__name__}:{exc}"}
    if not isinstance(payload, dict):
        return None, {"reason_codes": ["CONFIGURATION_SOURCE_DOCUMENT_INVALID"]}
    if str(payload.get("schema_id") or "").strip() != SOURCE_DOCUMENT_SCHEMA_ID:
        return None, {"reason_codes": ["CONFIGURATION_SOURCE_DOCUMENT_SCHEMA_ID_INVALID"]}
    values = payload.get("values")
    if not isinstance(values, Mapping):
        return None, {"reason_codes": ["CONFIGURATION_SOURCE_VALUES_MISSING"]}
    normalized, reason_codes = _validate_values(values)
    if reason_codes:
        return None, {"reason_codes": ["CONFIGURATION_SOURCE_VALUES_INVALID", *reason_codes]}
    return (
        dict(normalized),
        {
            "configuration_state_path": str(family.configuration_state_ref.path),
            "compiled_active_config_path": str(family.compiled_active_config_ref.path),
            "activation_transaction_path": str(family.activation_transaction_ref.path),
            "policy_snapshot_path": str(family.policy_snapshot_ref.path),
            "source_document_path": str(path),
        },
    )


def resolve_effective_capital_cashflow_inputs_v1() -> Dict[str, Any]:
    active_values, refs = _active_document_values()
    if active_values is not None:
        return {
            "status": "ACTIVE",
            "values": active_values,
            "reason_codes": [],
            "source_refs": refs,
        }
    return {
        "status": "NO_ACTIVE_CONFIGURATION",
        "values": _fallback_values(),
        "reason_codes": list(refs.get("reason_codes") or ["CONFIGURATION_STATE_NOT_AVAILABLE"]),
        "source_refs": refs,
    }


def build_configuration_catalog_v1() -> Dict[str, Any]:
    catalog = load_configuration_catalog_v1()
    editable_fields = [
        {
            "parameter_name": str(row["parameter_key"]),
            "parameter_key": str(row["parameter_key"]),
            "display_name": str(row["display_name"]),
            "state": "editable",
            "owning_domain": str(row["owner_domain"]),
            "owner_domain": str(row["owner_domain"]),
            "required": "required" in list(row.get("validation_rules") or []),
            "type": str(row["type"]),
            "default_value": row.get("default_value"),
            "allowed_values": list(row.get("allowed_values") or []),
            "minimum": row.get("min"),
            "maximum": row.get("max"),
            "environment_scope": list(row.get("environment_scope") or []),
            "target_policy_artifact": str(row["target_policy_artifact"]),
            "target_field_path": str(row["target_field_path"]),
            "requires_approval": bool(row["requires_approval"]),
            "effective_from": str(row["effective_from"]),
            "validation_rules": list(row.get("validation_rules") or []),
            "validation": {
                "type": str(row["type"]),
                "allowed_values": list(row.get("allowed_values") or []),
                "minimum": row.get("min"),
                "maximum": row.get("max"),
                "rules": list(row.get("validation_rules") or []),
            },
            "reason": str(row["description"]),
        }
        for row in catalog["parameters"]
    ]
    coverage_rows = [
        {
            "parameter_name": row["parameter_name"],
            "state": row["state"],
            "owning_domain": row["owning_domain"],
            "reason": row["reason"],
        }
        for row in editable_fields
    ]
    coverage_rows.extend(
        {
            "parameter_name": row["parameter_name"],
            "state": row["state"],
            "owning_domain": row["owning_domain"],
            "reason": row["reason"],
        }
        for row in LOCKED_FIELD_ROWS
    )
    return {
        "ok": True,
        "catalog_id": str(catalog.get("catalog_id") or "aegis_operator_configuration_catalog_v1"),
        "catalog_path": str(CATALOG_PATH),
        "catalog_sha256": sha256_file_v1(CATALOG_PATH),
        "generated_utc": _utc_now_iso(),
        "lifecycle": [
            LIFECYCLE_DRAFT,
            LIFECYCLE_VALIDATED,
            LIFECYCLE_REVIEWED,
            LIFECYCLE_ACTIVATED,
            LIFECYCLE_SUPERSEDED,
            LIFECYCLE_REJECTED,
        ],
        "parameters": editable_fields,
        "editable_fields": editable_fields,
        "locked_fields": [dict(row) for row in LOCKED_FIELD_ROWS],
        "coverage": coverage_rows,
    }


def build_configuration_current_v1() -> Dict[str, Any]:
    effective = resolve_effective_capital_cashflow_inputs_v1()
    return {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "domain": "capital_cashflow",
        "status": effective["status"],
        "current_values": dict(effective["values"]),
        "reason_codes": list(effective.get("reason_codes") or []),
        "source_refs": dict(effective.get("source_refs") or {}),
        "locked_fields": [dict(row) for row in LOCKED_FIELD_ROWS],
    }


def _active_configuration_current() -> dict[str, Any] | None:
    path = (_truth_root() / "active_configuration_v1" / "current.json").resolve()
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _diff_values(current_values: Mapping[str, Any], proposed_values: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field in sorted(proposed_values.keys()):
        if current_values.get(field) != proposed_values.get(field):
            rows.append(
                {
                    "field": field,
                    "current_value": current_values.get(field),
                    "proposed_value": proposed_values.get(field),
                }
            )
    return rows


def create_configuration_draft_v1(payload: Any) -> Dict[str, Any]:
    request = _ensure_payload_object(payload)
    values = _extract_proposed_values(request)
    advisory_unmapped_parameters, advisory_reason_codes = _normalize_operator_parameters(
        request.get("operator_parameters"),
    )
    if advisory_reason_codes:
        raise ConfigurationWorkflowApiError(
            "Advisory operator parameters are invalid.",
            status_code=400,
            reason_codes=advisory_reason_codes,
        )
    active_configuration = _active_configuration_current() or {}
    config_version_base = str(active_configuration.get("config_version") or "NO_ACTIVE_CONFIGURATION")
    current_values = dict(build_configuration_current_v1().get("current_values") or {})
    diff_from_active = _diff_values(current_values, values)
    now = _utc_now_iso()
    draft_id = f"cfgdraft_{uuid.uuid4().hex}"
    draft = {
        "schema_id": DRAFT_SCHEMA_ID,
        "schema_version": "v1",
        "draft_id": draft_id,
        "config_version_base": config_version_base,
        "domain": "capital_cashflow",
        "status": LIFECYCLE_DRAFT,
        "created_by": str(request.get("created_by") or "operator"),
        "created_at": now,
        "created_at_utc": now,
        "updated_at_utc": now,
        "proposed_values": dict(values),
        "advisory_unmapped_parameters": advisory_unmapped_parameters,
        "diff_from_active": diff_from_active,
        "validation_status": "NOT_VALIDATED",
        "validation": None,
        "review": None,
        "activation": None,
        "rejection": None,
        "superseded_by_draft_id": None,
        "lifecycle_history": [],
    }
    _append_lifecycle_event(draft, event="DRAFT_CREATED", details={"draft_id": draft_id})
    _write_json_dict(_draft_path(draft_id), draft)
    return {"ok": True, "draft": draft}


def get_configuration_draft_v1(draft_id: str) -> Dict[str, Any]:
    draft = _ensure_draft_exists(draft_id)
    return {"ok": True, "draft": draft}


def validate_configuration_draft_v1(draft_id: str) -> Dict[str, Any]:
    draft = _ensure_draft_exists(draft_id)
    status = str(draft.get("status") or "")
    if status in {LIFECYCLE_ACTIVATED, LIFECYCLE_SUPERSEDED, LIFECYCLE_REJECTED}:
        raise ConfigurationWorkflowApiError(
            "Draft cannot be revalidated from its current lifecycle state.",
            status_code=409,
            reason_codes=["CONFIGURATION_DRAFT_VALIDATE_STATE_INVALID"],
            details={"draft_id": draft_id, "status": status},
        )
    proposed = draft.get("proposed_values")
    if not isinstance(proposed, Mapping):
        raise ConfigurationWorkflowApiError(
            "Draft proposed values are missing.",
            status_code=409,
            reason_codes=["CONFIGURATION_DRAFT_VALUES_MISSING"],
            details={"draft_id": draft_id},
        )
    catalog_validation = validate_catalog_values_v1(proposed, environment="PAPER", approval_status="VALIDATED")
    normalized = dict(catalog_validation.normalized_values)
    reason_codes = list(catalog_validation.reason_codes)
    validation_status = VALIDATION_PASS if not reason_codes else VALIDATION_FAIL
    draft["validation"] = {
        "validated_at_utc": _utc_now_iso(),
        "status": validation_status,
        "reason_codes": list(reason_codes),
        "blocking_reason_codes": list(reason_codes),
        "normalized_values": dict(normalized) if validation_status == VALIDATION_PASS else None,
        "parameter_refs_used": list(catalog_validation.parameter_refs_used),
    }
    draft["validation_status"] = validation_status
    draft["status"] = LIFECYCLE_VALIDATED if validation_status == VALIDATION_PASS else LIFECYCLE_DRAFT
    draft["updated_at_utc"] = _utc_now_iso()
    _append_lifecycle_event(
        draft,
        event="DRAFT_VALIDATED",
        details={"validation_status": validation_status, "reason_codes": reason_codes},
    )
    _write_json_dict(_draft_path(draft_id), draft)
    return {"ok": True, "draft": draft, "validation": draft["validation"]}


def review_configuration_draft_v1(draft_id: str) -> Dict[str, Any]:
    draft = _ensure_draft_exists(draft_id)
    validation = draft.get("validation")
    if not isinstance(validation, Mapping) or str(validation.get("status") or "") != VALIDATION_PASS:
        raise ConfigurationWorkflowApiError(
            "Draft must pass validation before review.",
            status_code=409,
            reason_codes=["CONFIGURATION_REVIEW_REQUIRES_VALIDATION_PASS"],
            details={"draft_id": draft_id},
        )
    if str(draft.get("status") or "") != LIFECYCLE_VALIDATED:
        raise ConfigurationWorkflowApiError(
            "Draft lifecycle is not eligible for review.",
            status_code=409,
            reason_codes=["CONFIGURATION_REVIEW_STATE_INVALID"],
            details={"draft_id": draft_id, "status": str(draft.get("status") or "")},
        )
    proposed_values = dict(validation.get("normalized_values") or {})
    if not proposed_values:
        raise ConfigurationWorkflowApiError(
            "Validated values are unavailable.",
            status_code=409,
            reason_codes=["CONFIGURATION_VALIDATED_VALUES_MISSING"],
            details={"draft_id": draft_id},
        )
    current = build_configuration_current_v1()
    current_values = dict(current.get("current_values") or {})
    changed_fields = []
    for field in EDITABLE_FIELD_NAMES:
        if current_values.get(field) != proposed_values.get(field):
            changed_fields.append(
                {
                    "field": field,
                    "current_value": current_values.get(field),
                    "proposed_value": proposed_values.get(field),
                }
            )
    draft["review"] = {
        "reviewed_at_utc": _utc_now_iso(),
        "status": "REVIEW_READY",
        "proposed_values": proposed_values,
        "current_values": current_values,
        "changed_fields": changed_fields,
    }
    draft["status"] = LIFECYCLE_REVIEWED
    draft["updated_at_utc"] = _utc_now_iso()
    _append_lifecycle_event(
        draft,
        event="DRAFT_REVIEWED",
        details={"changed_field_count": len(changed_fields)},
    )
    _write_json_dict(_draft_path(draft_id), draft)
    return {"ok": True, "draft": draft, "review": draft["review"]}


def _mark_superseded_activated_drafts(*, activated_draft_id: str) -> list[str]:
    root = _draft_root()
    if not root.exists() or not root.is_dir():
        return []
    superseded: list[str] = []
    for path in sorted(root.glob("*.json")):
        payload = _read_json_dict(path)
        draft_id = str(payload.get("draft_id") or "").strip()
        if not draft_id or draft_id == activated_draft_id:
            continue
        if str(payload.get("status") or "") != LIFECYCLE_ACTIVATED:
            continue
        payload["status"] = LIFECYCLE_SUPERSEDED
        payload["superseded_by_draft_id"] = activated_draft_id
        payload["updated_at_utc"] = _utc_now_iso()
        _append_lifecycle_event(
            payload,
            event="DRAFT_SUPERSEDED",
            details={"superseded_by_draft_id": activated_draft_id},
        )
        _write_json_dict(path, payload)
        superseded.append(draft_id)
    return superseded


def activate_configuration_draft_v1(draft_id: str) -> Dict[str, Any]:
    draft = _ensure_draft_exists(draft_id)
    if str(draft.get("status") or "") != LIFECYCLE_REVIEWED:
        raise ConfigurationWorkflowApiError(
            "Draft must be REVIEWED before activation.",
            status_code=409,
            reason_codes=["CONFIGURATION_ACTIVATE_REQUIRES_REVIEWED_DRAFT"],
            details={"draft_id": draft_id, "status": str(draft.get("status") or "")},
        )
    validation = draft.get("validation")
    if not isinstance(validation, Mapping) or str(validation.get("status") or "") != VALIDATION_PASS:
        raise ConfigurationWorkflowApiError(
            "Activation requires a PASS validation result.",
            status_code=409,
            reason_codes=["CONFIGURATION_ACTIVATE_REQUIRES_VALIDATION_PASS"],
            details={"draft_id": draft_id},
        )
    review = draft.get("review")
    if not isinstance(review, Mapping):
        raise ConfigurationWorkflowApiError(
            "Activation requires a completed review.",
            status_code=409,
            reason_codes=["CONFIGURATION_ACTIVATE_REQUIRES_REVIEW"],
            details={"draft_id": draft_id},
        )

    normalized_values = dict(validation.get("normalized_values") or {})
    activation_validation = validate_catalog_values_v1(
        normalized_values,
        environment="PAPER",
        approval_status="ACTIVATED",
    )
    if activation_validation.reason_codes:
        raise ConfigurationWorkflowApiError(
            "Activation failed due to invalid normalized values.",
            status_code=409,
            reason_codes=["CONFIGURATION_ACTIVATE_VALUES_INVALID", *activation_validation.reason_codes],
            details={"draft_id": draft_id},
        )
    normalized_values = dict(activation_validation.normalized_values)

    effective_at_utc = _utc_now_iso()
    truth_root = _truth_root()
    source_document_path = _source_document_path(truth_root=truth_root, draft_id=draft_id)
    source_document_payload = {
        "schema_id": SOURCE_DOCUMENT_SCHEMA_ID,
        "schema_version": "v1",
        "draft_id": draft_id,
        "config_version_base": str(draft.get("config_version_base") or "NO_ACTIVE_CONFIGURATION"),
        "domain": "capital_cashflow",
        "generated_at_utc": effective_at_utc,
        "values": normalized_values,
        "advisory_unmapped_parameters": list(draft.get("advisory_unmapped_parameters") or []),
        "catalog_ref": {
            "path": str(CATALOG_PATH),
            "sha256": sha256_file_v1(CATALOG_PATH),
        },
        "parameter_refs_used": list(activation_validation.parameter_refs_used),
    }
    _write_json_dict(source_document_path, source_document_payload)
    policy_snapshot_path = _write_policy_snapshot(
        truth_root=truth_root,
        source_document_path=source_document_path,
        effective_at_utc=effective_at_utc,
    )
    activation_result = run_configuration_activation_authority_v1(
        policy_snapshot_path=policy_snapshot_path,
        truth_root=truth_root,
    )
    validation_status = str(activation_result.validation_result_ref.payload.get("validation_status") or "").strip().upper()
    if validation_status != VALIDATION_PASS:
        raise ConfigurationWorkflowApiError(
            "Activation authority failed validation.",
            status_code=409,
            reason_codes=["CONFIGURATION_ACTIVATION_AUTHORITY_VALIDATION_FAILED"],
            details={"draft_id": draft_id, "validation_status": validation_status},
        )
    if (
        activation_result.compiled_active_config_ref is None
        or activation_result.compile_result_ref is None
        or activation_result.review_diff_ref is None
        or activation_result.activation_transaction_ref is None
        or activation_result.configuration_state_ref is None
    ):
        raise ConfigurationWorkflowApiError(
            "Activation authority did not complete activation chain.",
            status_code=409,
            reason_codes=["CONFIGURATION_ACTIVATION_CHAIN_INCOMPLETE"],
            details={"draft_id": draft_id},
        )

    active_configuration = build_active_configuration_v1(
        config_version_base=str(draft.get("config_version_base") or "NO_ACTIVE_CONFIGURATION"),
        proposed_values=normalized_values,
        approved_diff=list(review.get("changed_fields") or []),
        activated_by=str(draft.get("created_by") or "operator"),
        truth_root=truth_root,
        runtime_root=_runtime_root(),
        prior_config_version=None
        if str(draft.get("config_version_base") or "") == "NO_ACTIVE_CONFIGURATION"
        else str(draft.get("config_version_base") or ""),
    )
    active_artifacts = write_active_configuration_artifacts_v1(
        active_configuration=active_configuration,
        truth_root=truth_root,
    )

    superseded_ids = _mark_superseded_activated_drafts(activated_draft_id=draft_id)
    draft["activation"] = {
        "activated_at_utc": _utc_now_iso(),
        "status": LIFECYCLE_ACTIVATED,
        "config_version": str(active_configuration["config_version"]),
        "active_configuration_path": str(active_artifacts["active_configuration_path"]),
        "active_configuration_current_path": str(active_artifacts["active_configuration_current_path"]),
        "materialized_policy_refs": list(active_artifacts["materialized_policy_refs"]),
        "policy_snapshot_path": str(activation_result.policy_snapshot_ref.path),
        "validation_result_path": str(activation_result.validation_result_ref.path),
        "compiled_active_config_path": str(activation_result.compiled_active_config_ref.path),
        "compile_result_path": str(activation_result.compile_result_ref.path),
        "review_diff_path": str(activation_result.review_diff_ref.path),
        "activation_transaction_path": str(activation_result.activation_transaction_ref.path),
        "configuration_state_path": str(activation_result.configuration_state_ref.path),
        "audit_evidence_path": str(activation_result.activation_transaction_ref.path),
        "superseded_draft_ids": superseded_ids,
    }
    draft["status"] = LIFECYCLE_ACTIVATED
    draft["superseded_by_draft_id"] = None
    draft["updated_at_utc"] = _utc_now_iso()
    _append_lifecycle_event(
        draft,
        event="DRAFT_ACTIVATED",
        details={
            "activation_transaction_path": draft["activation"]["activation_transaction_path"],
            "configuration_state_path": draft["activation"]["configuration_state_path"],
            "superseded_draft_ids": superseded_ids,
        },
    )
    _write_json_dict(_draft_path(draft_id), draft)
    return {"ok": True, "draft": draft, "activation": draft["activation"]}


def reject_configuration_draft_v1(draft_id: str, payload: Any) -> Dict[str, Any]:
    request = _ensure_payload_object(payload)
    draft = _ensure_draft_exists(draft_id)
    status = str(draft.get("status") or "")
    if status in {LIFECYCLE_ACTIVATED, LIFECYCLE_SUPERSEDED}:
        raise ConfigurationWorkflowApiError(
            "Activated or superseded drafts cannot be rejected.",
            status_code=409,
            reason_codes=["CONFIGURATION_REJECT_STATE_INVALID"],
            details={"draft_id": draft_id, "status": status},
        )
    if status == LIFECYCLE_REJECTED:
        return {"ok": True, "draft": draft}
    reason = str(request.get("reason") or "").strip()
    if not reason:
        reason = "operator_rejected"
    draft["rejection"] = {"rejected_at_utc": _utc_now_iso(), "reason": reason}
    draft["status"] = LIFECYCLE_REJECTED
    draft["updated_at_utc"] = _utc_now_iso()
    _append_lifecycle_event(draft, event="DRAFT_REJECTED", details={"reason": reason})
    _write_json_dict(_draft_path(draft_id), draft)
    return {"ok": True, "draft": draft, "rejection": draft["rejection"]}
