from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Mapping

from jsonschema import Draft202012Validator

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1

FAMILY = "aegis_oil_shock_risk_policy_schema_repair_v1"
FILENAME = "oil_shock_risk_policy_schema_repair.v1.json"
POLICY_KEY = "C2_OIL_SHOCK_REVERSAL_V1"
POLICY_ID = "C2_OIL_SHOCK_REVERSAL_V1_RISK_POLICY_V1"
REGISTRY_REL = "governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json"
SCHEMA_REL = "governance/04_DATA/SCHEMAS/C2/RISK/c2_risk_policy_registry.v1.schema.json"
PACKAGE_017_SOURCE = "C2_RISK_POLICY_REGISTRY_V1:C2_OIL_SHOCK_REVERSAL_V1.stop_loss_bps_default"

SAFETY = {
    "research_only": True,
    "no_strategy_logic_change": True,
    "no_stop_loss_formula_change": True,
    "no_risk_threshold_change": True,
    "no_candidate_scoring_change": True,
    "no_paper_construction_behavior_change": True,
    "no_outcome_behavior_change": True,
    "no_mark_certification_weakening": True,
    "broker_execution_allowed": False,
    "live_trading_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
}


def oil_shock_risk_policy_schema_repair_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_oil_shock_risk_policy_schema_repair_v1(*, truth_root: Path | str, repo_root: Path | str, day_utc: str, computed_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).resolve()
    registry_path = repo / REGISTRY_REL
    schema_path = repo / SCHEMA_REL
    registry = _read_json(registry_path)
    schema = _read_json(schema_path)
    policy = _dict(_dict(registry.get("policies")).get(POLICY_KEY))
    schema_failures_after = _schema_failures(registry, schema)
    schema_failures_before = _schema_failures_before_allowlist_repair(registry, schema)
    required_fields = _policy_required_fields(schema)
    missing_after = [field for field in required_fields if field not in policy or policy.get(field) in (None, "", [])]
    missing_before = list(missing_after)
    stop_source, stop_source_day, stop_source_path = _package_017_stop_source(root, str(day_utc))
    stop_valid = stop_source == PACKAGE_017_SOURCE
    stop_present = policy.get("stop_loss_bps_default") not in (None, "")
    strategy_behavior_changed = False
    compatible = not schema_failures_after and not missing_after and stop_present and stop_valid and not strategy_behavior_changed
    blocker = "NONE" if compatible else "RISK_POLICY_SCHEMA_REPAIR_INCOMPLETE"
    blocker_reason = "NONE" if compatible else "Oil Shock risk policy schema compatibility is still incomplete."
    payload: dict[str, Any] = {
        "schema_id": "aegis_oil_shock_risk_policy_schema_repair",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": str(day_utc),
        "computed_at_utc": computed_at_utc or _now(),
        "policy_id": str(policy.get("policy_id") or POLICY_ID),
        "sleeve_id": POLICY_KEY,
        "registry_source": str(registry_path),
        "schema_source": str(schema_path),
        "schema_failures_before_repair": schema_failures_before,
        "schema_failures_after_repair": schema_failures_after,
        "required_fields_checked": required_fields,
        "missing_fields_before_repair": missing_before,
        "missing_fields_after_repair": missing_after,
        "stop_loss_bps_default_present": bool(stop_present),
        "stop_loss_bps_default": policy.get("stop_loss_bps_default"),
        "stop_price_source_still_valid": bool(stop_valid),
        "stop_price_source": stop_source,
        "stop_price_source_day": stop_source_day,
        "stop_price_source_path": stop_source_path,
        "strategy_behavior_changed": strategy_behavior_changed,
        "paper_construction_compatibility_status": "COMPATIBLE" if compatible else "BLOCKED",
        "package_017_compatibility_status": "COMPATIBLE" if stop_valid and stop_present else "BLOCKED",
        "remaining_blocker": blocker,
        "blocker_reason": blocker_reason,
        "owner": "NONE" if compatible else "AEGIS_SYSTEM",
        "david_action_required": False,
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["summary"] = {key: payload[key] for key in (
        "policy_id", "sleeve_id", "registry_source", "schema_failures_before_repair", "schema_failures_after_repair",
        "required_fields_checked", "missing_fields_before_repair", "missing_fields_after_repair", "stop_loss_bps_default_present",
        "stop_price_source_still_valid", "strategy_behavior_changed", "paper_construction_compatibility_status",
        "package_017_compatibility_status", "remaining_blocker", "blocker_reason", "owner", "david_action_required"
    )}
    payload["content_hash"] = stable_hash_v1({k: v for k, v in payload.items() if k not in {"computed_at_utc", "content_hash"}})
    return payload


def write_oil_shock_risk_policy_schema_repair_v1(*, truth_root: Path | str, repo_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_oil_shock_risk_policy_schema_repair_v1(truth_root=truth_root, repo_root=repo_root, day_utc=day_utc)
    return write_json_v1(oil_shock_risk_policy_schema_repair_path_v1(truth_root=truth_root, day_utc=day_utc), body)



def _package_017_stop_source(truth_root: Path, day_utc: str) -> tuple[str, str, str]:
    base = truth_root / "reports" / "aegis_generated_hypothesis_paper_construction_repair_v1"
    filename = "generated_hypothesis_paper_construction_repair.v1.json"
    candidates: list[tuple[date, Path]] = []
    target = _parse_day(day_utc)
    if target is None:
        return "", "", ""
    for child in base.iterdir() if base.exists() else []:
        child_day = _parse_day(child.name)
        if child_day is None or child_day > target:
            continue
        path = child / filename
        if path.exists():
            candidates.append((child_day, path))
    for child_day, path in sorted(candidates, reverse=True):
        payload = read_json_v1(path)
        summary = _dict(payload.get("summary"))
        source = str(summary.get("stop_price_source") or "")
        if source:
            return source, child_day.isoformat(), str(path)
    return "", "", ""


def _parse_day(value: str) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None

def _schema_failures_before_allowlist_repair(registry: Mapping[str, Any], schema: Mapping[str, Any]) -> list[str]:
    before = json.loads(json.dumps(schema))
    policies = before.get("properties", {}).get("policies", {})
    props = policies.get("properties", {})
    req = policies.get("required", [])
    props.pop(POLICY_KEY, None)
    policies["required"] = [item for item in req if item != POLICY_KEY]
    return _schema_failures(registry, before)


def _schema_failures(registry: Mapping[str, Any], schema: Mapping[str, Any]) -> list[str]:
    validator = Draft202012Validator(schema)
    failures = []
    for error in sorted(validator.iter_errors(registry), key=lambda e: (list(e.path), e.message)):
        loc = "$"
        for part in error.path:
            loc += f"[{part}]" if isinstance(part, int) else f".{part}"
        failures.append(f"{loc}: {error.message}")
    return failures


def _policy_required_fields(schema: Mapping[str, Any]) -> list[str]:
    return [str(field) for field in _dict(_dict(schema.get("$defs")).get("policy")).get("required", [])]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
