from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.aegis_truth.state_machines.kernel_state_types_v1 import base_state, json_files_under, read_day_events, read_json_file, write_report

SCHEMA_VERSION = "trading_readiness_state.v1"
REPORT_NAME = "trading_readiness_state_v1"
FILENAME = "trading_readiness_state.v1.json"
AUTHORITATIVE_INPUTS = {
    "day_run_ledger": "reports/truth_day_run_ledger_v1/{day}",
    "requirement_graph": "reports/aegis_requirement_graph_v1/{day}",
    "broker_supply": "reports/broker_supply_v1/{day}",
    "market_data_supply": "reports/market_data_supply_v1/{day}",
    "capital_supply": "reports/capital_supply_v1/{day}",
    "risk_budget_supply": "reports/risk_budget_supply_v1/{day}",
    "authorization_supply": "reports/authorization_supply_v1/{day}",
    "submit_boundary": "reports/submit_boundary_status_v1/{day}",
    "execution_mode_authority": "reports/execution_mode_authority_v1/{day}",
}
TRADING_OWNERS = {"trading_safety", "submit_boundary", "risk", "authorization", "broker_supply", "market_data_supply", "capital_supply", "risk_budget_supply", "execution_mode_authority"}
BLOCKING_STATUSES = {"BLOCKED", "FORBIDDEN"}


def build_trading_readiness_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN") -> dict[str, Any]:
    root = Path(truth_root)
    events = read_day_events(truth_root=root, target_day=target_day)
    trading_events = [event for event in events if event.get("owner") in TRADING_OWNERS or event.get("event_type") in {"TRADING_SAFETY_BLOCKED", "TRANSMIT_FORBIDDEN", "SUBMIT_BOUNDARY_BLOCKED"}]
    missing_inputs, input_summaries, input_paths = _input_state(root, target_day)
    report_blockers = [row for row in input_summaries if row.get("status") in BLOCKING_STATUSES]
    event_blockers = [event for event in trading_events if event.get("status") in BLOCKING_STATUSES or "FORBIDDEN" in str(event.get("event_type", ""))]
    selected: dict[str, Any] | None = None
    if any(_status_of(item) == "FORBIDDEN" for item in report_blockers) or any(event.get("status") == "FORBIDDEN" or "FORBIDDEN" in str(event.get("event_type", "")) for event in event_blockers):
        status = "FORBIDDEN"
        selected = _select_report_or_event(report_blockers, event_blockers, preferred="FORBIDDEN")
        severity = selected.get("severity", "CRITICAL") if selected else "CRITICAL"
        blocker = selected.get("blocker") if selected else "trading transmit forbidden"
        next_action = selected.get("next_action") if selected else "Do not transmit. Resolve forbidden trading condition."
    elif report_blockers or event_blockers:
        status = "BLOCKED"
        selected = _select_report_or_event(report_blockers, event_blockers, preferred="BLOCKED")
        severity = selected.get("severity", "ERROR") if selected else "ERROR"
        blocker = selected.get("blocker") if selected else "trading readiness blocked"
        next_action = selected.get("next_action") if selected else "Resolve trading safety blocker."
    elif missing_inputs:
        status = "UNKNOWN"
        blocker = "missing authoritative trading inputs: " + ", ".join(missing_inputs)
        severity = "ERROR"
        next_action = "Run missing authoritative trading producers: " + ", ".join(missing_inputs)
    else:
        status, blocker, severity = "READY", None, "INFO"
        next_action = "No operator action required."
    state_events = trading_events[-20:]
    result = base_state(schema_version=SCHEMA_VERSION, target_day=target_day, environment=environment, status=status, blocker=blocker, severity=severity, owner="trading_readiness", next_action=next_action, events=state_events, extra_paths=input_paths)
    result.update({
        "missing_inputs": missing_inputs,
        "input_summaries": input_summaries,
        "primary_trading_blocker": None if status == "READY" else {"blocker": blocker, "source": selected.get("source") if selected else "missing_inputs"},
    })
    return result


def write_trading_readiness_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN") -> Path:
    return write_report(truth_root, REPORT_NAME, target_day, FILENAME, build_trading_readiness_state(target_day=target_day, truth_root=truth_root, environment=environment))


def _input_state(root: Path, day: str) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    missing: list[str] = []
    summaries: list[dict[str, Any]] = []
    paths: list[str] = []
    for name, pattern in AUTHORITATIVE_INPUTS.items():
        path = root / pattern.format(day=day)
        files = json_files_under(path)
        if not files:
            missing.append(name)
            summaries.append({"name": name, "status": "MISSING", "path": str(path)})
            continue
        paths.extend(str(file) for file in files[:5])
        statuses = []
        blockers = []
        severities = []
        for file in files:
            payload = read_json_file(file) or {}
            statuses.append(str(payload.get("final_status") or payload.get("status") or payload.get("state") or "OK"))
            if payload.get("primary_blocker") or payload.get("blocker"):
                blockers.append(str(payload.get("primary_blocker") or payload.get("blocker")))
            if payload.get("severity"):
                severities.append(str(payload.get("severity")))
        status = "FORBIDDEN" if "FORBIDDEN" in statuses else "BLOCKED" if "BLOCKED" in statuses else "UNKNOWN" if "UNKNOWN" in statuses else "OK"
        summaries.append({"name": name, "status": status, "path": str(path), "blocker": blockers[0] if blockers else None, "severity": severities[0] if severities else None})
    return missing, summaries, paths


def _status_of(item: dict[str, Any]) -> str:
    return str(item.get("status", "UNKNOWN"))


def _select_report_or_event(report_blockers: list[dict[str, Any]], event_blockers: list[dict[str, Any]], *, preferred: str) -> dict[str, Any] | None:
    for item in report_blockers:
        if item.get("status") == preferred:
            return {"source": item.get("name"), "blocker": item.get("blocker") or f"{item.get('name')} {preferred.lower()}", "severity": item.get("severity") or ("CRITICAL" if preferred == "FORBIDDEN" else "ERROR"), "next_action": f"Repair {item.get('name')} and rerun trading readiness state."}
    for event in event_blockers:
        if event.get("status") == preferred or (preferred == "FORBIDDEN" and "FORBIDDEN" in str(event.get("event_type", ""))):
            return {"source": event.get("event_type"), "blocker": event.get("blocker") or event.get("event_type"), "severity": event.get("severity") or ("CRITICAL" if preferred == "FORBIDDEN" else "ERROR"), "next_action": event.get("next_action")}
    if report_blockers:
        item = report_blockers[0]
        return {"source": item.get("name"), "blocker": item.get("blocker") or f"{item.get('name')} blocked", "severity": item.get("severity") or "ERROR", "next_action": f"Repair {item.get('name')} and rerun trading readiness state."}
    if event_blockers:
        event = event_blockers[0]
        return {"source": event.get("event_type"), "blocker": event.get("blocker") or event.get("event_type"), "severity": event.get("severity") or "ERROR", "next_action": event.get("next_action")}
    return None
