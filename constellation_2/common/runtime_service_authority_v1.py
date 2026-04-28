from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA_ID = "C2_RUNTIME_SERVICE_AUTHORITY_V1"
SCHEMA_VERSION = 1
STATES = {
    "NOT_REQUIRED",
    "READY",
    "DEGRADED",
    "MISSING_REQUIRED_SERVICE",
    "SERVICE_CRASHED",
    "MANUAL_MODE_READY",
    "UNKNOWN",
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _sha256_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _service_name_matches(name: str, *needles: str) -> bool:
    lower = str(name or "").strip().lower()
    return any(needle in lower for needle in needles)


def _service_running(row: dict[str, Any]) -> bool:
    state = str(row.get("state") or "").strip().upper()
    pid = row.get("pid")
    pid_running = row.get("pid_running") is True
    if isinstance(pid, int) and pid > 0 and pid_running:
        try:
            os.kill(pid, 0)
        except OSError:
            pid_running = False
    return bool(pid_running or row.get("port_open") is True or state in {"READY", "RUNNING", "ACTIVE"})


def _service_crashed(row: dict[str, Any]) -> bool:
    state = str(row.get("state") or "").strip().upper()
    health = row.get("health") if isinstance(row.get("health"), dict) else {}
    return state in {"CRASHED", "FAILED", "ERROR"} or health.get("ok") is False


def runtime_service_authority_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "runtime_service_authority_v1"
        / day_utc
        / "runtime_service_authority.v1.json"
    ).resolve()


def evaluate_runtime_service_authority_v1(
    *,
    day_utc: str,
    truth_root: Path,
    repo_root: Path,
    runtime_root: Path | None = None,
    expected_run_mode: str = "MANUAL",
    produced_utc: str | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    repo_root = Path(repo_root).resolve()
    runtime_root = (
        Path(runtime_root).resolve()
        if runtime_root is not None
        else Path("/home/node/constellation_runtime_data/runtime").resolve()
    )
    process_root = runtime_root / "process_state"
    logs_root = runtime_root / "logs"
    service_status_path = process_root / "service_status.json"
    supervisor_state_path = process_root / "supervisor_state.json"
    supervisor_log_path = logs_root / "supervisor.log"
    package_path = repo_root / "package.json"
    execution_mode_path = truth_root / "reports" / "execution_mode_authority_v1" / day_utc / "execution_mode_authority.v1.json"
    paper_authority_path = truth_root / "reports" / "paper_trading_day_authority_v1" / day_utc / "paper_trading_day_authority.v1.json"
    submit_boundary_path = truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"

    service_status = _read_json(service_status_path) or {}
    supervisor_state = _read_json(supervisor_state_path) or {}
    package_doc = _read_json(package_path) or {}
    execution_mode = _read_json(execution_mode_path) or {}
    scripts = package_doc.get("scripts") if isinstance(package_doc.get("scripts"), dict) else {}
    submit_command_exists = "aegis:paper:submit" in scripts
    mode_from_authority = str(execution_mode.get("automation_mode") or "").strip().upper()
    run_mode = mode_from_authority if mode_from_authority in {"MANUAL", "ONE_SHOT", "AUTOMATIC"} else str(expected_run_mode or "MANUAL").strip().upper()
    if run_mode not in {"MANUAL", "ONE_SHOT", "AUTOMATIC"}:
        run_mode = "MANUAL"

    service_rows = service_status.get("services") if isinstance(service_status.get("services"), list) else []
    services: list[dict[str, Any]] = []
    for item in service_rows:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        running = _service_running(item)
        crashed = _service_crashed(item)
        service_type = "generic"
        if _service_name_matches(name, "dashboard", "ops_dashboard"):
            service_type = "dashboard"
        elif _service_name_matches(name, "aegis_paper_auto_runner", "aegis:paper:auto", "paper_auto_runner"):
            service_type = "auto_runner"
        elif _service_name_matches(name, "submit", "creator"):
            service_type = "submit_creator"
        elif _service_name_matches(name, "observer", "supervisor"):
            service_type = "observer_supervisor"
        raw_required = bool(item.get("required") is True)
        required = raw_required
        requirement_basis = "service_status_required_flag" if raw_required else "service_status_optional_flag"
        if run_mode in {"MANUAL", "ONE_SHOT"} and service_type == "auto_runner":
            required = False
            requirement_basis = "not_required_for_manual_or_one_shot_paper_mode"
        if run_mode in {"MANUAL", "ONE_SHOT"} and service_type == "submit_creator" and submit_command_exists:
            required = False
            requirement_basis = "submit_command_available_for_manual_or_one_shot_paper_mode"
        services.append(
            {
                "name": name,
                "service_type": service_type,
                "required": required,
                "raw_required": raw_required,
                "requirement_basis": requirement_basis,
                "running": running,
                "absent": not running,
                "crashed": crashed,
                "absence_acceptable": not required,
                "state": str(item.get("state") or "UNKNOWN"),
                "pid": item.get("pid"),
                "port": item.get("port"),
            }
        )

    submit_services = [row for row in services if row["service_type"] == "submit_creator"]
    auto_runner_services = [row for row in services if row["service_type"] == "auto_runner"]
    dashboard_services = [row for row in services if row["service_type"] == "dashboard"]
    observer_services = [row for row in services if row["service_type"] == "observer_supervisor"]
    submit_creator_running = any(row["running"] for row in submit_services)
    auto_runner_running = any(row["running"] for row in auto_runner_services)
    dashboard_running = any(row["running"] for row in dashboard_services)
    observer_running = any(row["running"] for row in observer_services) or str(supervisor_state.get("overall_status") or "").strip().upper() == "READY"

    required_missing: list[dict[str, Any]] = []
    crashed: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    for row in services:
        if row["crashed"] and row["required"]:
            crashed.append(row)
        elif row["required"] and not row["running"]:
            required_missing.append(row)
        elif not row["running"]:
            diagnostics.append(row)

    if run_mode == "AUTOMATIC" and not auto_runner_running:
        required_missing.append(
            {
                "name": "aegis_paper_auto_runner",
                "service_type": "auto_runner",
                "required": True,
                "running": False,
                "absent": True,
                "crashed": False,
                "absence_acceptable": False,
                "state": "MISSING",
                "pid": None,
                "port": None,
            }
        )

    if run_mode == "AUTOMATIC" and not (submit_command_exists or submit_creator_running):
        required_missing.append(
            {
                "name": "submit_creator",
                "service_type": "submit_creator",
                "required": True,
                "running": False,
                "absent": True,
                "crashed": False,
                "absence_acceptable": False,
                "state": "MISSING",
                "pid": None,
                "port": None,
            }
        )

    if crashed:
        state = "SERVICE_CRASHED"
    elif required_missing:
        state = "MISSING_REQUIRED_SERVICE"
    elif run_mode in {"MANUAL", "ONE_SHOT"} and submit_command_exists:
        state = "MANUAL_MODE_READY"
    elif diagnostics:
        state = "DEGRADED"
    elif services:
        state = "READY"
    else:
        state = "UNKNOWN"

    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "produced_utc": produced_utc or _utc_now_iso(),
        "authority_scope": "RUNTIME_SERVICE_EXPECTATIONS",
        "status": "PASS" if state in {"READY", "MANUAL_MODE_READY", "NOT_REQUIRED"} else ("WARN" if state == "DEGRADED" else "FAIL"),
        "service_state": state,
        "expected_run_mode": run_mode,
        "automatic_expected": run_mode == "AUTOMATIC",
        "auto_runner_running": auto_runner_running,
        "auto_runner_required": run_mode == "AUTOMATIC",
        "submit_creator_available": submit_command_exists,
        "submit_creator_running": submit_creator_running,
        "submit_creator_required": run_mode == "AUTOMATIC",
        "observer_supervisor_running": observer_running,
        "dashboard_running": dashboard_running,
        "required_services": [row for row in services if row["required"]]
        + ([row for row in required_missing if row["name"] in {"aegis_paper_auto_runner", "submit_creator"}] if run_mode == "AUTOMATIC" else []),
        "services": services,
        "required_missing": required_missing,
        "diagnostics": diagnostics,
        "first_blocker": str((crashed or required_missing or [{}])[0].get("name") or ""),
        "input_evidence": [
            {"artifact_type": "runtime/process_state/service_status.json", "path": str(service_status_path), "exists": service_status_path.exists()},
            {"artifact_type": "runtime/process_state/supervisor_state.json", "path": str(supervisor_state_path), "exists": supervisor_state_path.exists()},
            {"artifact_type": "runtime/logs/supervisor.log", "path": str(supervisor_log_path), "exists": supervisor_log_path.exists()},
            {"artifact_type": "package.json", "path": str(package_path), "exists": package_path.exists()},
            {"artifact_type": "paper_trading_day_authority_v1", "path": str(paper_authority_path), "exists": paper_authority_path.exists()},
            {"artifact_type": "submit_boundary_status_v1", "path": str(submit_boundary_path), "exists": submit_boundary_path.exists()},
            {"artifact_type": "execution_mode_authority_v1", "path": str(execution_mode_path), "exists": execution_mode_path.exists()},
        ],
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = _sha256_payload(payload)
    return payload


def write_runtime_service_authority_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = runtime_service_authority_output_path(truth_root=truth_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(_canonical_bytes(payload) + b"\n")
    return output_path
