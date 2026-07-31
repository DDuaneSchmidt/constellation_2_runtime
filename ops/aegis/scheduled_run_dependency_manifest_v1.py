from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.scheduled_run_registry_v1 import (
    build_scheduled_run_registry_v1,
    scheduled_run_registry_path_v1,
    write_scheduled_run_registry_v1,
)

REPORT_FAMILY = "aegis_scheduled_run_dependency_manifest_v1"
REPORT_FILENAME = "dependency_manifest.v1.json"


def scheduled_run_dependency_manifest_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def build_scheduled_run_dependency_manifest_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    registry_path = scheduled_run_registry_path_v1(truth_root=root, day_utc=day_utc)
    registry = _read_json(registry_path)
    if not registry:
        registry = build_scheduled_run_registry_v1(truth_root=root, day_utc=day_utc)
        registry_path = write_scheduled_run_registry_v1(truth_root=root, day_utc=day_utc, payload=registry)
    runs = registry.get("scheduled_runs") if isinstance(registry.get("scheduled_runs"), list) else []
    dependencies: list[dict[str, Any]] = []
    for run in runs:
        if not isinstance(run, dict):
            continue
        run_id = str(run.get("scheduled_run_id") or "")
        for dependency_id in run.get("required_dependencies") or []:
            dependencies.append(_dependency(root, day_utc, run_id, str(dependency_id)))
    dependencies.sort(key=lambda row: (row["scheduled_run_id"], row["dependency_id"]))
    source_paths = [registry_path, *[Path(row["current_artifact_path"]) for row in dependencies if row.get("current_artifact_path")]]
    existing = [path for path in source_paths if path.exists()]
    return {
        "schema_id": "aegis_scheduled_run_dependency_manifest",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": str(registry.get("generated_at") or f"{day_utc}T00:00:00Z"),
        "dependencies": dependencies,
        "dependencies_by_run_id": _group_by_run(dependencies),
        "summary": {
            "dependency_count": len(dependencies),
            "blocking_dependency_count": sum(1 for row in dependencies if row.get("is_blocking") is True),
            "ready_dependency_count": sum(1 for row in dependencies if row.get("dependency_status") == "READY"),
            "blocked_dependency_count": sum(1 for row in dependencies if row.get("dependency_status") not in {"READY", "NO_SETUP_NOT_APPLICABLE"} and row.get("is_blocking") is True),
        },
        "source_artifacts": {"scheduled_run_registry": str(registry_path), **{row["dependency_id"]: row["current_artifact_path"] for row in dependencies if row.get("artifact_exists")}},
        "source_hashes": {str(path): _sha256(path) for path in existing},
        "safety": {
            "trade_advice_allowed": False,
            "manual_trade_capture_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "live_trading_allowed": False,
            "policy_changed": False,
        },
    }


def write_scheduled_run_dependency_manifest_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    path = scheduled_run_dependency_manifest_path_v1(truth_root=truth_root, day_utc=day_utc)
    body = dict(payload or build_scheduled_run_dependency_manifest_v1(truth_root=truth_root, day_utc=day_utc))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _dependency(root: Path, day: str, run_id: str, dependency_id: str) -> dict[str, Any]:
    spec = _dependency_specs(root, day).get(dependency_id, {})
    path = Path(spec.get("path") or root / "reports" / "_missing" / day / f"{dependency_id}.json")
    payload = _read_json(path)
    exists = path.exists()
    schema_version = str(payload.get("schema_version") or payload.get("version") or "")
    required_schema = str(spec.get("required_schema_version") or "v1")
    expected_day = str(payload.get("day_utc") or payload.get("day") or day) if exists else day
    status = "READY"
    codes: list[str] = []
    if not exists:
        status = "MISSING"
        codes.append(f"{dependency_id}_MISSING")
    elif expected_day != day:
        status = "STALE"
        codes.append(f"{dependency_id}_DAY_MISMATCH")
    elif required_schema and schema_version and schema_version != required_schema:
        status = "SCHEMA_MISMATCH"
        codes.append(f"{dependency_id}_SCHEMA_MISMATCH")
    if dependency_id == "VIX_INPUT" and not exists:
        status = "NO_SETUP_NOT_APPLICABLE"
        codes = ["VIX_INPUT_NOT_REQUIRED_FOR_CURRENT_RUN"]
    return {
        "dependency_id": f"{run_id}:{dependency_id}",
        "dependency_key": dependency_id,
        "scheduled_run_id": run_id,
        "required_artifact": str(spec.get("artifact") or dependency_id),
        "producer_command": str(spec.get("producer_command") or ""),
        "freshness_window_minutes": int(spec.get("freshness_window_minutes") or 120),
        "current_artifact_path": str(path),
        "expected_day_utc": day,
        "actual_day_utc": expected_day if exists else "",
        "required_schema_version": required_schema,
        "actual_schema_version": schema_version,
        "repair_command": str(spec.get("repair_command") or ""),
        "no_repair_reason": str(spec.get("no_repair_reason") or ""),
        "is_blocking": bool(spec.get("is_blocking", True)),
        "failure_codes": sorted(set(codes)),
        "dependency_status": status,
        "artifact_exists": exists,
        "source_hash": _sha256(path) if exists else "",
    }


def _dependency_specs(root: Path, day: str) -> dict[str, dict[str, Any]]:
    reports = root / "reports"
    return {
        "MARKET_DATA": {"artifact": "market_data_intraday_operational_v1", "path": reports / "market_data_intraday_operational_v1" / day / "market_data_intraday_operational.v1.json", "producer_command": "npm run aegis:event-market-snapshot", "repair_command": "npm run aegis:event-market-snapshot", "freshness_window_minutes": 90},
        "VIX_INPUT": {"artifact": "vix_stale_operator_report_v1", "path": reports / "vix_stale_operator_report_v1" / day / "vix_stale_operator_report.v1.json", "producer_command": "npm run aegis:event-market-snapshot", "repair_command": "", "no_repair_reason": "VIX is contextual unless a sleeve explicitly requires it.", "is_blocking": False, "freshness_window_minutes": 1440},
        "SLEEVE_INPUT_CONTRACTS": {"artifact": "aegis_sleeve_input_contracts_v1", "path": reports / "aegis_sleeve_input_contracts_v1" / day / "sleeve_input_contracts.v1.json", "producer_command": "npm run aegis:sleeve-input-contracts", "repair_command": "npm run aegis:sleeve-input-contracts", "freshness_window_minutes": 1440},
        "ALLOWED_SYMBOL_UNIVERSE": {"artifact": "allowed_symbol_source_report_v1", "path": reports / "allowed_symbol_source_report_v1" / day / "allowed_symbol_source_report.v1.json", "producer_command": "npm run aegis:allowed-symbol-source-report", "repair_command": "", "no_repair_reason": "Universe expansion requires evidence and is not a safe automated repair.", "freshness_window_minutes": 1440},
        "CANDIDATE_DIAGNOSTICS_INPUTS": {"artifact": "aegis_candidate_generation_diagnostics_v1", "path": reports / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json", "producer_command": "npm run aegis:candidate-diagnostics", "repair_command": "npm run aegis:candidate-diagnostics", "freshness_window_minutes": 180},
        "CANONICAL_CANDIDATE_STATE": {"artifact": "aegis_candidate_state_v1", "path": reports / "aegis_candidate_state_v1" / day / "candidate_state.v1.json", "producer_command": "npm run aegis:roll-candidate-state", "repair_command": "npm run aegis:roll-candidate-state", "freshness_window_minutes": 180},
        "OPERATOR_ACTION_MODEL": {"artifact": "aegis_operator_action_model_v1", "path": reports / "aegis_operator_action_model_v1" / day / "operator_action_model.v1.json", "producer_command": "npm run aegis:operator-action-model", "repair_command": "npm run aegis:operator-action-model", "freshness_window_minutes": 180},
        "OUTCOME_REGISTRY": {"artifact": "aegis_outcome_registry_v1", "path": reports / "aegis_outcome_registry_v1" / day / "outcome_registry.v1.json", "producer_command": "npm run aegis:outcome-validation", "repair_command": "npm run aegis:outcome-validation", "freshness_window_minutes": 1440},
        "VALIDATION_SAMPLES": {"artifact": "aegis_validation_samples_v1", "path": reports / "aegis_validation_samples_v1" / day / "validation_samples.v1.json", "producer_command": "npm run aegis:outcome-validation", "repair_command": "npm run aegis:outcome-validation", "freshness_window_minutes": 1440},
        "STATISTICAL_SUFFICIENCY": {"artifact": "aegis_statistical_sufficiency_v1", "path": reports / "aegis_statistical_sufficiency_v1" / day / "statistical_sufficiency.v1.json", "producer_command": "npm run aegis:outcome-validation", "repair_command": "npm run aegis:outcome-validation", "freshness_window_minutes": 1440},
        "RESEARCH_ALLOCATION_ARTIFACTS": {"artifact": "aegis_research_capital_allocation_v1", "path": reports / "aegis_research_capital_allocation_v1" / day / "research_capital_allocation.v1.json", "producer_command": "npm run aegis:research-capital-allocation", "repair_command": "npm run aegis:research-capital-allocation", "freshness_window_minutes": 1440},
        "PORTAL_RUNTIME_MODEL": {"artifact": "portal_runtime_model.v1.json", "path": reports / "aegis_verified_runtime_graph_v1" / day / "portal_runtime_model.v1.json", "producer_command": "npm run aegis:verified-graph", "repair_command": "npm run aegis:verified-graph", "freshness_window_minutes": 180},
    }


def _group_by_run(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row["scheduled_run_id"], []).append(row)
    return grouped


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
