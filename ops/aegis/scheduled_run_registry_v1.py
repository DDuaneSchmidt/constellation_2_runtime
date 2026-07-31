from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

REPORT_FAMILY = "aegis_scheduled_run_registry_v1"
REPORT_FILENAME = "scheduled_run_registry.v1.json"
LOCAL_ZONE = ZoneInfo("America/New_York")

RUN_IDS = [
    "CANDIDATE_GENERATION_0950",
    "MIDDAY_MONITORING",
    "EOD_OUTCOME_UPDATE",
    "RESEARCH_ALLOCATION",
    "VALIDATION_MATURITY",
    "OPERATOR_DASHBOARD_REFRESH",
]


def scheduled_run_registry_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def build_scheduled_run_registry_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    runs = _scheduled_runs(day_utc)
    sources = _source_paths(root, day_utc)
    existing_sources = {name: str(path) for name, path in sources.items() if path.exists()}
    source_hashes = {str(path): _sha256(path) for path in sources.values() if path.exists()}
    generated_at = _derived_generated_at(sources, day_utc)
    return {
        "schema_id": "aegis_scheduled_run_registry",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": generated_at,
        "scheduled_runs": runs,
        "scheduled_runs_by_id": {row["scheduled_run_id"]: row for row in runs},
        "source_artifacts": existing_sources,
        "source_hashes": source_hashes,
        "safety": _safety_flags(),
    }


def write_scheduled_run_registry_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    path = scheduled_run_registry_path_v1(truth_root=truth_root, day_utc=day_utc)
    body = dict(payload or build_scheduled_run_registry_v1(truth_root=truth_root, day_utc=day_utc))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _scheduled_runs(day: str) -> list[dict[str, Any]]:
    rows = [
        _run("CANDIDATE_GENERATION_0950", "09:50 candidate generation", day, "09:50", "CANDIDATE_GENERATION", "npm run aegis:candidate-diagnostics", ["DATA_READY", "PAPER_CANDIDATES_READY"], ["MARKET_DATA", "VIX_INPUT", "SLEEVE_INPUT_CONTRACTS", "ALLOWED_SYMBOL_UNIVERSE", "CANDIDATE_DIAGNOSTICS_INPUTS", "CANONICAL_CANDIDATE_STATE", "OPERATOR_ACTION_MODEL"], "BLOCK_ON_MISSING_REQUIRED_INPUT", "SAFE_REPAIR_READ_ONLY_ARTIFACTS", "candidate_generation"),
        _run("MIDDAY_MONITORING", "Midday monitoring", day, "12:00", "MONITORING", "npm run aegis:operator-action-model", ["PAPER_MONITORING"], ["MARKET_DATA", "CANONICAL_CANDIDATE_STATE", "OPERATOR_ACTION_MODEL", "PORTAL_RUNTIME_MODEL"], "MONITOR_IF_DATA_AVAILABLE", "SAFE_REPAIR_READ_ONLY_ARTIFACTS", "operator_portal"),
        _run("EOD_OUTCOME_UPDATE", "End-of-day outcome update", day, "16:15", "OUTCOME_UPDATE", "npm run aegis:outcome-validation", ["OUTCOME_REALIZATION"], ["OUTCOME_REGISTRY", "CANONICAL_CANDIDATE_STATE", "MARKET_DATA"], "BLOCK_ON_MISSING_OUTCOME_INPUTS", "SAFE_REPAIR_READ_ONLY_ARTIFACTS", "outcome_validation"),
        _run("RESEARCH_ALLOCATION", "Research allocation", day, "16:30", "RESEARCH_ALLOCATION", "npm run aegis:research-capital-allocation", ["HYPOTHESIS_VALIDATION"], ["RESEARCH_ALLOCATION_ARTIFACTS", "OUTCOME_REGISTRY", "VALIDATION_SAMPLES", "STATISTICAL_SUFFICIENCY"], "BLOCK_ON_MISSING_RESEARCH_EVIDENCE", "SAFE_REPAIR_READ_ONLY_ARTIFACTS", "research_workflow"),
        _run("VALIDATION_MATURITY", "Validation maturity", day, "16:45", "VALIDATION_MATURITY", "npm run aegis:outcome-validation-self-check", ["HYPOTHESIS_VALIDATION"], ["OUTCOME_REGISTRY", "VALIDATION_SAMPLES", "STATISTICAL_SUFFICIENCY"], "BLOCK_ON_MISSING_VALIDATION_EVIDENCE", "SAFE_REPAIR_READ_ONLY_ARTIFACTS", "research_workflow"),
        _run("OPERATOR_DASHBOARD_REFRESH", "Operator dashboard refresh", day, "17:00", "OPERATOR_DASHBOARD_REFRESH", "npm run aegis:portal-smoke", ["PAPER_MONITORING"], ["PORTAL_RUNTIME_MODEL", "OPERATOR_ACTION_MODEL", "CANONICAL_CANDIDATE_STATE", "RESEARCH_ALLOCATION_ARTIFACTS"], "DEGRADE_WITH_EXPLICIT_MISSING_EVIDENCE", "SAFE_REPAIR_READ_ONLY_ARTIFACTS", "operator_portal"),
    ]
    return rows


def _run(run_id: str, name: str, day: str, local_hhmm: str, run_type: str, command: str, capabilities: list[str], dependencies: list[str], blocking_policy: str, repair_policy: str, owner: str) -> dict[str, Any]:
    local_dt = datetime.fromisoformat(f"{day}T{local_hhmm}:00").replace(tzinfo=LOCAL_ZONE)
    utc_dt = local_dt.astimezone(timezone.utc)
    return {
        "scheduled_run_id": run_id,
        "run_name": name,
        "target_time_local": local_dt.isoformat(),
        "target_time_utc": utc_dt.isoformat().replace("+00:00", "Z"),
        "run_type": run_type,
        "expected_command": command,
        "required_capabilities": capabilities,
        "required_dependencies": dependencies,
        "blocking_policy": blocking_policy,
        "repair_policy": repair_policy,
        "owner": owner,
        "enabled": True,
    }


def _source_paths(root: Path, day: str) -> dict[str, Path]:
    reports = root / "reports"
    return {
        "runtime_truth_kernel": reports / "aegis_runtime_truth_kernel_v1" / day / "runtime_truth_kernel.v1.json",
        "verified_runtime_graph": reports / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json",
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _derived_generated_at(sources: Mapping[str, Path], day: str) -> str:
    values: list[str] = []
    for path in sources.values():
        payload = _read_json(path)
        for key in ("generated_at", "generated_at_utc", "packet_generated_at_utc"):
            if payload.get(key):
                values.append(str(payload[key]))
    return sorted(values)[-1] if values else f"{day}T00:00:00Z"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _safety_flags() -> dict[str, bool]:
    return {
        "trade_advice_allowed": False,
        "manual_trade_capture_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trading_allowed": False,
        "policy_changed": False,
    }
