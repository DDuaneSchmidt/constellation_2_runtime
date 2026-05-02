from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.paper_session_fact_plane_v1 import repo_git_sha_v1, sha256_file_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
CATALOG_RELPATH = "governance/02_REGISTRIES/C2_CONFIGURATION_CATALOG_V1.json"
CATALOG_PATH = (REPO_ROOT / CATALOG_RELPATH).resolve()

POLICY_ARTIFACT_PATHS = {
    "C2_GOVERNED_EVALUATION_POLICY_V1": "governance/02_REGISTRIES/C2_GOVERNED_EVALUATION_POLICY_V1.json",
    "C2_BUNDLE_C_DRAWDOWN_POLICY_V1": "governance/02_REGISTRIES/C2_BUNDLE_C_DRAWDOWN_POLICY_V1.json",
    "C2_LIQUIDITY_SLIPPAGE_POLICY_V1": "governance/02_REGISTRIES/C2_LIQUIDITY_SLIPPAGE_POLICY_V1.json",
    "C2_RISK_POLICY_REGISTRY_V1": "governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json",
    "C2_CAPITAL_AUTHORITY_POLICY_V1": "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json",
}

SOURCE_DOCUMENT_ARTIFACTS = {"capital_cashflow_configuration_input_v1"}


class ConfigurationCatalogError(RuntimeError):
    pass


@dataclass(frozen=True)
class CatalogValidationResultV1:
    status: str
    normalized_values: dict[str, Any]
    reason_codes: list[str]
    parameter_refs_used: list[dict[str, Any]]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_configuration_catalog_v1() -> dict[str, Any]:
    try:
        payload = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigurationCatalogError(f"CONFIGURATION_CATALOG_MISSING:{CATALOG_PATH}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigurationCatalogError(f"CONFIGURATION_CATALOG_JSON_INVALID:{CATALOG_PATH}") from exc
    if not isinstance(payload, dict):
        raise ConfigurationCatalogError("CONFIGURATION_CATALOG_NOT_OBJECT")
    if str(payload.get("schema_id") or "").strip() != "C2_CONFIGURATION_CATALOG_V1":
        raise ConfigurationCatalogError("CONFIGURATION_CATALOG_SCHEMA_ID_INVALID")
    parameters = payload.get("parameters")
    if not isinstance(parameters, list) or not parameters:
        raise ConfigurationCatalogError("CONFIGURATION_CATALOG_PARAMETERS_MISSING")
    seen: set[str] = set()
    required = {
        "parameter_key",
        "display_name",
        "description",
        "type",
        "default_value",
        "environment_scope",
        "owner_domain",
        "target_policy_artifact",
        "target_field_path",
        "requires_approval",
        "effective_from",
        "validation_rules",
    }
    for row in parameters:
        if not isinstance(row, dict):
            raise ConfigurationCatalogError("CONFIGURATION_CATALOG_ROW_NOT_OBJECT")
        missing = sorted(required - set(row))
        if missing:
            raise ConfigurationCatalogError(f"CONFIGURATION_CATALOG_ROW_FIELDS_MISSING:{missing}")
        key = str(row.get("parameter_key") or "").strip()
        if not key:
            raise ConfigurationCatalogError("CONFIGURATION_CATALOG_PARAMETER_KEY_EMPTY")
        if key in seen:
            raise ConfigurationCatalogError(f"CONFIGURATION_CATALOG_PARAMETER_KEY_DUPLICATE:{key}")
        seen.add(key)
    return payload


def catalog_entries_by_key_v1() -> dict[str, dict[str, Any]]:
    catalog = load_configuration_catalog_v1()
    return {str(row["parameter_key"]): dict(row) for row in catalog["parameters"]}


def default_catalog_values_v1(*, environment: str = "PAPER") -> dict[str, Any]:
    env = str(environment or "").strip().upper() or "PAPER"
    values: dict[str, Any] = {}
    for key, row in catalog_entries_by_key_v1().items():
        scopes = {str(item).strip().upper() for item in row.get("environment_scope") or []}
        if env in scopes:
            values[key] = copy.deepcopy(row.get("default_value"))
    return values


def _policy_payload_for_catalog_row(row: Mapping[str, Any]) -> tuple[dict[str, Any] | None, Path | None, str | None]:
    artifact = str(row.get("target_policy_artifact") or "").strip()
    if artifact in SOURCE_DOCUMENT_ARTIFACTS:
        return None, None, None
    relpath = POLICY_ARTIFACT_PATHS.get(artifact)
    if not relpath:
        raise ConfigurationCatalogError(f"CONFIGURATION_CATALOG_TARGET_POLICY_UNKNOWN:{artifact}")
    path = (REPO_ROOT / relpath).resolve()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigurationCatalogError(f"CONFIGURATION_CATALOG_TARGET_POLICY_MISSING:{artifact}") from exc
    if not isinstance(payload, dict):
        raise ConfigurationCatalogError(f"CONFIGURATION_CATALOG_TARGET_POLICY_NOT_OBJECT:{artifact}")
    return payload, path, sha256_file_v1(path)


def _json_pointer_get(payload: Any, pointer: str) -> Any:
    if pointer == "":
        return payload
    current = payload
    for raw_part in str(pointer).split("/")[1:]:
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            current = current[int(part)]
        elif isinstance(current, dict):
            current = current[part]
        else:
            raise KeyError(pointer)
    return current


def _json_pointer_set(payload: Any, pointer: str, value: Any) -> None:
    parts = [part.replace("~1", "/").replace("~0", "~") for part in str(pointer).split("/")[1:]]
    current = payload
    for part in parts[:-1]:
        current = current[int(part)] if isinstance(current, list) else current[part]
    final = parts[-1]
    if isinstance(current, list):
        current[int(final)] = value
    else:
        current[final] = value


def _normalize_by_type(value: Any, row: Mapping[str, Any]) -> tuple[Any, list[str]]:
    key = str(row.get("parameter_key") or "")
    kind = str(row.get("type") or "").strip()
    reason_codes: list[str] = []
    if kind == "boolean":
        if isinstance(value, bool):
            return value, []
        return None, [f"CONFIGURATION_TYPE_INVALID:{key}"]
    if kind == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            return None, [f"CONFIGURATION_TYPE_INVALID:{key}"]
        if "min" in row and value < int(row["min"]):
            reason_codes.append(f"CONFIGURATION_RANGE_LOW:{key}")
        if "max" in row and value > int(row["max"]):
            reason_codes.append(f"CONFIGURATION_RANGE_HIGH:{key}")
        return int(value), reason_codes
    if kind == "enum":
        text = str(value or "").strip()
        allowed = [str(item) for item in row.get("allowed_values") or []]
        if text not in allowed:
            reason_codes.append(f"CONFIGURATION_ENUM_INVALID:{key}")
        return text, reason_codes
    if kind == "month":
        text = str(value or "").strip()
        try:
            datetime.strptime(f"{text}-01", "%Y-%m-%d")
        except ValueError:
            reason_codes.append(f"CONFIGURATION_MONTH_INVALID:{key}")
        return text, reason_codes
    if kind == "decimal_string":
        text = str(value or "").strip()
        try:
            numeric = Decimal(text)
        except (InvalidOperation, ValueError):
            return None, [f"CONFIGURATION_TYPE_INVALID:{key}"]
        if "min" in row and numeric < Decimal(str(row["min"])):
            reason_codes.append(f"CONFIGURATION_RANGE_LOW:{key}")
        if "max" in row and numeric > Decimal(str(row["max"])):
            reason_codes.append(f"CONFIGURATION_RANGE_HIGH:{key}")
        return text, reason_codes
    return None, [f"CONFIGURATION_TYPE_UNKNOWN:{key}"]


def validate_catalog_values_v1(
    proposed_values: Mapping[str, Any],
    *,
    environment: str = "PAPER",
    approval_status: str = "DRAFT",
) -> CatalogValidationResultV1:
    catalog = catalog_entries_by_key_v1()
    env = str(environment or "").strip().upper() or "PAPER"
    normalized: dict[str, Any] = {}
    reason_codes: list[str] = []
    parameter_refs: list[dict[str, Any]] = []

    for key in sorted(str(k) for k in proposed_values.keys()):
        if key not in catalog:
            reason_codes.append(f"CONFIGURATION_PARAMETER_NOT_CATALOGED:{key}")

    for key, row in catalog.items():
        scopes = {str(item).strip().upper() for item in row.get("environment_scope") or []}
        if env not in scopes:
            if key in proposed_values:
                reason_codes.append(f"CONFIGURATION_ENVIRONMENT_SCOPE_INVALID:{key}")
            continue
        value = proposed_values.get(key, copy.deepcopy(row.get("default_value")))
        if "required" in row.get("validation_rules", []) and value in (None, ""):
            reason_codes.append(f"CONFIGURATION_REQUIRED_MISSING:{key}")
            continue
        normalized_value, value_reasons = _normalize_by_type(value, row)
        reason_codes.extend(value_reasons)
        if not value_reasons:
            normalized[key] = normalized_value

        if bool(row.get("requires_approval")) and str(approval_status).upper() in {"ACTIVATION_REQUESTED"}:
            reason_codes.append(f"CONFIGURATION_APPROVAL_REQUIRED:{key}")

        try:
            policy_payload, policy_path, policy_sha = _policy_payload_for_catalog_row(row)
            if policy_payload is not None:
                _json_pointer_get(policy_payload, str(row["target_field_path"]))
        except Exception:
            reason_codes.append(f"CONFIGURATION_TARGET_POLICY_MAPPING_INVALID:{key}")
            policy_path = None
            policy_sha = None

        parameter_refs.append(
            {
                "parameter_key": key,
                "target_policy_artifact": str(row.get("target_policy_artifact") or ""),
                "target_field_path": str(row.get("target_field_path") or ""),
                "owner_domain": str(row.get("owner_domain") or ""),
                "policy_path": "" if policy_path is None else str(policy_path),
                "policy_sha256": "" if policy_sha is None else str(policy_sha),
            }
        )

    unique_reasons = sorted(set(reason_codes))
    return CatalogValidationResultV1(
        status="PASS" if not unique_reasons else "FAIL",
        normalized_values=normalized,
        reason_codes=unique_reasons,
        parameter_refs_used=sorted(parameter_refs, key=lambda item: item["parameter_key"]),
    )


def build_active_configuration_v1(
    *,
    config_version_base: str,
    proposed_values: Mapping[str, Any],
    approved_diff: list[dict[str, Any]],
    activated_by: str,
    truth_root: Path,
    runtime_root: Path,
    prior_config_version: str | None,
) -> dict[str, Any]:
    now = _utc_now_iso()
    normalized = dict(proposed_values)
    config_version = canonical_hash_for_c2_artifact_v1(
        {
            "schema_id": "active_configuration_v1",
            "config_version_base": str(config_version_base or ""),
            "proposed_values": normalized,
            "approved_diff": approved_diff,
            "prior_config_version": prior_config_version,
        }
    )
    return {
        "schema_id": "active_configuration_v1",
        "schema_version": "v1",
        "config_version": config_version,
        "config_version_base": str(config_version_base or ""),
        "activated_by": str(activated_by or "operator"),
        "activated_at": now,
        "git_commit": repo_git_sha_v1(),
        "truth_root": str(Path(truth_root).resolve()),
        "runtime_root": str(Path(runtime_root).resolve()),
        "approved_diff": list(approved_diff),
        "prior_config_version": prior_config_version,
        "catalog_ref": {
            "path": str(CATALOG_PATH),
            "sha256": sha256_file_v1(CATALOG_PATH),
            "schema_id": "C2_CONFIGURATION_CATALOG_V1",
        },
        "values": normalized,
    }


def materialize_policy_artifacts_v1(
    *,
    active_configuration: Mapping[str, Any],
    truth_root: Path,
) -> list[dict[str, Any]]:
    config_version = str(active_configuration.get("config_version") or "").strip()
    values = active_configuration.get("values")
    if not config_version or not isinstance(values, Mapping):
        raise ConfigurationCatalogError("ACTIVE_CONFIGURATION_INVALID")
    validation = validate_catalog_values_v1(values, environment="PAPER", approval_status="ACTIVATED")
    if validation.status != "PASS":
        raise ConfigurationCatalogError(f"ACTIVE_CONFIGURATION_VALUES_INVALID:{validation.reason_codes}")

    refs: list[dict[str, Any]] = []
    grouped: dict[str, list[dict[str, Any]]] = {}
    catalog = catalog_entries_by_key_v1()
    for key, value in validation.normalized_values.items():
        artifact = str(catalog[key]["target_policy_artifact"])
        if artifact in SOURCE_DOCUMENT_ARTIFACTS:
            continue
        grouped.setdefault(artifact, []).append(
            {
                "parameter_key": key,
                "target_field_path": str(catalog[key]["target_field_path"]),
                "value": value,
            }
        )

    root = (Path(truth_root).resolve() / "materialized_policy_artifacts_v1" / config_version).resolve()
    for artifact, parameter_rows in sorted(grouped.items()):
        source_policy, source_path, source_sha = _policy_payload_for_catalog_row({"target_policy_artifact": artifact})
        if source_policy is None or source_path is None or source_sha is None:
            continue
        effective_policy = copy.deepcopy(source_policy)
        for row in parameter_rows:
            _json_pointer_set(effective_policy, row["target_field_path"], row["value"])
        payload = {
            "schema_id": "materialized_configuration_policy_v1",
            "schema_version": "v1",
            "config_version_used": config_version,
            "materialized_at": _utc_now_iso(),
            "target_policy_artifact": artifact,
            "source_policy_ref": {"path": str(source_path), "sha256": source_sha},
            "parameter_refs_used": parameter_rows,
            "effective_policy": effective_policy,
        }
        materialized_id = canonical_hash_for_c2_artifact_v1(payload)
        path = (root / artifact / "materialized_policy.v1.json").resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        refs.append(
            {
                "target_policy_artifact": artifact,
                "path": str(path),
                "sha256": sha256_file_v1(path),
                "materialized_policy_id": materialized_id,
                "config_version_used": config_version,
                "parameter_refs_used": parameter_rows,
            }
        )
    return sorted(refs, key=lambda item: item["target_policy_artifact"])


def write_active_configuration_artifacts_v1(
    *,
    active_configuration: Mapping[str, Any],
    truth_root: Path,
) -> dict[str, Any]:
    config_version = str(active_configuration.get("config_version") or "").strip()
    if not config_version:
        raise ConfigurationCatalogError("ACTIVE_CONFIGURATION_VERSION_MISSING")
    root = Path(truth_root).resolve()
    versioned_path = (root / "active_configuration_v1" / config_version / "active_configuration.v1.json").resolve()
    current_path = (root / "active_configuration_v1" / "current.json").resolve()
    versioned_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(active_configuration)
    versioned_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    current_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    materialized_refs = materialize_policy_artifacts_v1(active_configuration=payload, truth_root=root)
    return {
        "active_configuration_path": str(versioned_path),
        "active_configuration_current_path": str(current_path),
        "active_configuration_sha256": sha256_file_v1(versioned_path),
        "materialized_policy_refs": materialized_refs,
    }


def resolve_materialized_policy_for_gate_v1(
    *,
    truth_root: Path,
    target_policy_artifact: str,
) -> dict[str, Any] | None:
    current_path = (Path(truth_root).resolve() / "active_configuration_v1" / "current.json").resolve()
    if not current_path.exists() or not current_path.is_file():
        return None
    active = json.loads(current_path.read_text(encoding="utf-8"))
    if not isinstance(active, dict):
        raise ConfigurationCatalogError("ACTIVE_CONFIGURATION_CURRENT_INVALID")
    config_version = str(active.get("config_version") or "").strip()
    if not config_version:
        raise ConfigurationCatalogError("ACTIVE_CONFIGURATION_CURRENT_VERSION_MISSING")
    artifact = str(target_policy_artifact or "").strip()
    policy_path = (
        Path(truth_root).resolve()
        / "materialized_policy_artifacts_v1"
        / config_version
        / artifact
        / "materialized_policy.v1.json"
    ).resolve()
    if not policy_path.exists() or not policy_path.is_file():
        raise ConfigurationCatalogError(f"MATERIALIZED_POLICY_MISSING:{artifact}:{policy_path}")
    payload = json.loads(policy_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ConfigurationCatalogError(f"MATERIALIZED_POLICY_INVALID:{artifact}")
    if str(payload.get("config_version_used") or "").strip() != config_version:
        raise ConfigurationCatalogError(f"MATERIALIZED_POLICY_CONFIG_VERSION_MISMATCH:{artifact}")
    return {
        "path": str(policy_path),
        "sha256": sha256_file_v1(policy_path),
        "config_version_used": config_version,
        "policy_artifact_used": artifact,
        "parameter_refs_used": list(payload.get("parameter_refs_used") or []),
        "effective_policy": dict(payload.get("effective_policy") or {}),
    }
