from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from constellation_2.aegis_truth.evidence_event_v1 import build_event, utc_now_iso
from constellation_2.aegis_truth.evidence_ledger_v1 import read_events
from constellation_2.aegis_truth.projection_writer_v1 import atomic_write_json

STATUS_RANK = {"READY": 0, "DEGRADED": 1, "UNKNOWN": 2, "BLOCKED": 3, "FORBIDDEN": 4}
SEVERITY_RANK = {"INFO": 0, "WARN": 1, "ERROR": 2, "CRITICAL": 3}
TRADING_OWNERS = {"trading_safety", "submit_boundary", "risk", "authorization", "broker_supply", "market_data_supply", "capital_supply", "risk_budget_supply"}
AUTHORITATIVE_INPUTS = {
    "day_run_ledger": "reports/truth_day_run_ledger_v1/{day}",
    "requirement_graph": "reports/aegis_requirement_graph_v1/{day}",
    "broker_supply": "reports/broker_supply_v1/{day}",
    "market_data_supply": "reports/market_data_supply_v1/{day}",
    "capital_supply": "reports/capital_supply_v1/{day}",
    "risk_budget_supply": "reports/risk_budget_supply_v1/{day}",
    "authorization_supply": "reports/authorization_supply_v1/{day}",
    "submit_boundary": "reports/submit_boundary_status_v1/{day}",
}


def resolve_truth_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN") -> dict[str, Any]:
    root = Path(truth_root)
    events = read_events(truth_root=root, target_day=target_day)
    generated = utc_now_iso()
    evidence_event_ids = [event["event_id"] for event in events]
    evidence_paths = sorted({p for event in events for p in [event.get("evidence_path")] if p})
    missing_inputs = _missing_authoritative_inputs(root, target_day)
    stale_inputs = _stale_from_events(events)
    contradictions = _contradictions(events, root, target_day)
    secondary_conditions: list[dict[str, Any]] = []

    if not events:
        missing_inputs.append("evidence_ledger")
        missing_event = build_event(
            event_type="EVIDENCE_LEDGER_MISSING",
            producer="aegis.truth_resolver_v1",
            target_day=target_day,
            environment=environment,
            status="MISSING",
            blocker="aegis evidence ledger is missing",
            owner="aegis_truth",
            severity="ERROR",
            payload={"truth_root": str(root)},
            next_action="Run Aegis producers and verify ledger write permissions.",
        )
        events = [missing_event]
        evidence_event_ids = [missing_event["event_id"]]

    forbidden = _select_events(events, statuses={"FORBIDDEN"}) + [e for e in events if "FORBIDDEN" in e["event_type"] or "BREACH" in e["event_type"]]
    trading_blockers = [e for e in events if e.get("owner") in TRADING_OWNERS and e.get("status") in {"BLOCKED", "FORBIDDEN"}]
    latest_portal = _latest_portal_event(events)
    critical_portal = [latest_portal] if latest_portal and latest_portal["severity"] == "CRITICAL" and latest_portal["status"] != "OK" else []
    unknowns = [e for e in events if e.get("status") in {"UNKNOWN", "MISSING"}]

    final_status = "READY"
    selected: dict[str, Any] | None = None
    if forbidden:
        final_status, selected = "FORBIDDEN", _highest(forbidden)
    elif trading_blockers:
        final_status, selected = "BLOCKED", _highest(trading_blockers)
        secondary_conditions.extend(_conditions(critical_portal, "portal_degradation"))
    elif contradictions:
        final_status = "UNKNOWN"
        selected = _highest(events) if events else None
    elif missing_inputs or unknowns:
        final_status = "UNKNOWN"
        selected = _highest(unknowns) if unknowns else {
            "blocker": "missing authoritative evidence: " + ", ".join(sorted(set(missing_inputs))),
            "owner": "aegis_truth",
            "severity": "ERROR",
            "next_action": "Run missing authoritative producers: " + ", ".join(sorted(set(missing_inputs))),
        }
    elif critical_portal:
        final_status, selected = "DEGRADED", _highest(critical_portal)
    elif any(e.get("status") in {"WARN", "DEGRADED", "STALE"} for e in events):
        degraded = [e for e in events if e.get("status") in {"WARN", "DEGRADED", "STALE"}]
        final_status, selected = "DEGRADED", _highest(degraded)

    if final_status != "READY" and selected is None:
        selected = _highest(events)

    result = {
        "schema_version": "unified_truth_state.v1",
        "target_day": target_day,
        "environment": environment,
        "final_status": final_status,
        "primary_blocker": None if final_status == "READY" else (selected or {}).get("blocker") or (selected or {}).get("event_type") or "truth unavailable",
        "owner": None if final_status == "READY" else (selected or {}).get("owner"),
        "severity": "INFO" if final_status == "READY" else _severity_for(final_status, selected),
        "truth_confidence": _confidence(final_status, missing_inputs, contradictions),
        "evidence_event_ids": evidence_event_ids,
        "evidence_paths": evidence_paths,
        "secondary_conditions": secondary_conditions,
        "stale_inputs": stale_inputs,
        "missing_inputs": sorted(set(missing_inputs)),
        "contradictions": contradictions,
        "operator_next_action": _next_action(final_status, selected, missing_inputs, contradictions),
        "generated_at_utc": generated,
    }
    if final_status != "READY" and not result["evidence_event_ids"]:
        raise RuntimeError("non-READY truth state must cite evidence_event_ids")
    return result


def write_truth_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN") -> Path:
    state = resolve_truth_state(target_day=target_day, truth_root=truth_root, environment=environment)
    path = Path(truth_root) / "reports" / "unified_truth_state_v1" / target_day / "unified_truth_state.v1.json"
    return atomic_write_json(path, state)


def _missing_authoritative_inputs(root: Path, day: str) -> list[str]:
    missing: list[str] = []
    for name, pattern in AUTHORITATIVE_INPUTS.items():
        path = root / pattern.format(day=day)
        if not path.exists() or (path.is_dir() and not any(path.rglob("*.json"))):
            missing.append(name)
    return missing


def _select_events(events: list[dict[str, Any]], *, statuses: set[str]) -> list[dict[str, Any]]:
    return [e for e in events if e.get("status") in statuses]


def _highest(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not events:
        return None
    return sorted(events, key=lambda e: (SEVERITY_RANK.get(e.get("severity", "INFO"), 0), e.get("observed_at_utc", "")), reverse=True)[0]


def _conditions(events: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [{"kind": kind, "event_id": e["event_id"], "event_type": e["event_type"], "status": e["status"], "severity": e["severity"], "blocker": e.get("blocker")} for e in events]


def _stale_from_events(events: list[dict[str, Any]]) -> list[str]:
    stale: list[str] = []
    for index, event in enumerate(events):
        event_type = str(event.get("event_type", ""))
        if event.get("status") != "STALE" and not event_type.endswith("_STALE"):
            continue
        if _stale_event_cleared(event, events, index):
            continue
        stale.append(event_type)
    return sorted(set(stale))


def _stale_event_cleared(stale_event: dict[str, Any], events: list[dict[str, Any]], stale_index: int) -> bool:
    stale_type = str(stale_event.get("event_type", ""))
    stale_at = str(stale_event.get("observed_at_utc", ""))
    clear_types = {stale_type}
    if stale_type == "PORTAL_PROJECTION_STALE":
        clear_types.update({"PORTAL_AVAILABILITY_OBSERVED", "PROJECTION_GENERATED"})
    elif stale_type == "PROJECTION_STALE":
        clear_types.add("PROJECTION_GENERATED")
    for index, event in enumerate(events):
        observed_at = str(event.get("observed_at_utc", ""))
        if observed_at < stale_at or (observed_at == stale_at and index <= stale_index):
            continue
        if event.get("status") == "OK" and event.get("event_type") in clear_types:
            return True
    return False


def _latest_portal_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    portal_events = [e for e in events if e.get("event_type", "").startswith("PORTAL_")]
    if not portal_events:
        return None
    return max(enumerate(portal_events), key=lambda item: (item[1].get("observed_at_utc", ""), item[0]))[1]


def _contradictions(events: list[dict[str, Any]], root: Path, day: str) -> list[dict[str, Any]]:
    contradictions: list[dict[str, Any]] = []
    by_type: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        by_type.setdefault(event["event_type"], []).append(event)
    for event_type, type_events in sorted(by_type.items()):
        statuses = {event["status"] for event in type_events}
        latest = max(enumerate(type_events), key=lambda item: (item[1].get("observed_at_utc", ""), item[0]))[1]
        if "OK" in statuses and (statuses & {"BLOCKED", "FORBIDDEN", "DEGRADED", "UNKNOWN"}) and latest.get("status") != "OK":
            contradictions.append({"kind": "event_status_conflict", "event_type": event_type, "statuses": sorted(statuses), "latest_status": latest.get("status")})
    marker = root / "reports" / "aegis_test_contradiction_v1" / day / "contradiction.json"
    if marker.exists():
        contradictions.append({"kind": "test_marker", "path": str(marker)})
    return contradictions


def _severity_for(final_status: str, selected: dict[str, Any] | None) -> str:
    if selected and selected.get("severity"):
        return str(selected["severity"])
    return {"DEGRADED": "WARN", "UNKNOWN": "ERROR", "BLOCKED": "ERROR", "FORBIDDEN": "CRITICAL"}.get(final_status, "INFO")


def _confidence(final_status: str, missing_inputs: list[str], contradictions: list[dict[str, Any]]) -> str:
    if contradictions or final_status == "UNKNOWN":
        return "LOW"
    if missing_inputs:
        return "LOW"
    if final_status in {"DEGRADED", "BLOCKED", "FORBIDDEN"}:
        return "MEDIUM"
    return "HIGH"


def _next_action(final_status: str, selected: dict[str, Any] | None, missing_inputs: list[str], contradictions: list[dict[str, Any]]) -> str:
    if contradictions:
        return "Resolve contradictory authoritative surfaces before using projections."
    if missing_inputs:
        return "Run missing authoritative producers: " + ", ".join(sorted(set(missing_inputs)))
    if selected and selected.get("next_action"):
        return str(selected["next_action"])
    if final_status == "READY":
        return "No operator action required."
    return "Inspect cited evidence and repair the primary blocker."
