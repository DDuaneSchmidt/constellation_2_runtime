from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from typing import Any

from constellation_2.aegis_truth.state_machines.kernel_state_types_v1 import base_state, events_after, latest_event, read_day_events, read_json_file, write_report

SCHEMA_VERSION = "alert_lifecycle_state.v1"
REPORT_NAME = "alert_lifecycle_state_v1"
FILENAME = "alert_lifecycle_state.v1.json"


def build_alert_lifecycle_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN") -> dict[str, Any]:
    root = Path(truth_root)
    events = read_day_events(truth_root=root, target_day=target_day)
    alerts_path = root / "alerts" / "aegis_alerts_v1" / target_day / "alerts.jsonl"
    truth_state_path = root / "reports" / "unified_truth_state_v1" / target_day / "unified_truth_state.v1.json"
    truth_state = read_json_file(truth_state_path) or {}
    alerts = _read_alerts(alerts_path)
    event_by_id = {event.get("event_id"): event for event in events}
    lifecycle_alerts = []
    for alert in alerts:
        lifecycle_alerts.append(_classify_alert(alert, events, event_by_id, truth_state))
    active_alerts = [alert for alert in lifecycle_alerts if alert["lifecycle_state"] in {"OPEN", "ACK_REQUIRED"}]
    critical_active = [alert for alert in active_alerts if alert.get("severity") == "CRITICAL"]
    if critical_active:
        status, blocker, severity = "OPEN", critical_active[0].get("message") or critical_active[0].get("title"), "CRITICAL"
        next_action = "Resolve active critical alert."
    elif active_alerts:
        status, blocker, severity = "OPEN", active_alerts[0].get("message") or active_alerts[0].get("title"), active_alerts[0].get("severity", "ERROR")
        next_action = "Resolve active alert conditions."
    else:
        status, blocker, severity = "CLOSED", None, "INFO"
        next_action = "No operator action required."
    result = base_state(schema_version=SCHEMA_VERSION, target_day=target_day, environment=environment, status=status, blocker=blocker, severity=severity, owner="alert_lifecycle", next_action=next_action, events=events[-20:], extra_paths=[alerts_path if alerts_path.exists() else None, truth_state_path if truth_state_path.exists() else None])
    result.update({
        "alerts": lifecycle_alerts,
        "active_alerts": active_alerts,
        "state_counts": dict(Counter(alert["lifecycle_state"] for alert in lifecycle_alerts)),
        "alerts_path": str(alerts_path),
    })
    return result


def write_alert_lifecycle_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN") -> Path:
    return write_report(truth_root, REPORT_NAME, target_day, FILENAME, build_alert_lifecycle_state(target_day=target_day, truth_root=truth_root, environment=environment))


def _read_alerts(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    alerts: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            alerts.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return alerts


def _classify_alert(alert: dict[str, Any], events: list[dict[str, Any]], event_by_id: dict[str, dict[str, Any]], truth_state: dict[str, Any]) -> dict[str, Any]:
    title = str(alert.get("title", ""))
    source_event = event_by_id.get(alert.get("source_event_id"))
    lifecycle_state = "ACK_REQUIRED" if alert.get("ack_required") else "OPEN"
    reason = "alert remains active"
    if title == "Portal Origin Unreachable" or (source_event and str(source_event.get("event_type", "")).startswith("PORTAL_")):
        lifecycle_state, reason = _portal_alert_state(source_event, events)
    elif title == "Resolver contradiction":
        if not truth_state.get("contradictions"):
            lifecycle_state, reason = "RECOVERED", "current unified truth has no contradictions"
        else:
            lifecycle_state, reason = "OPEN", "current unified truth still has contradictions"
    elif title == "Unified truth UNKNOWN":
        if truth_state.get("final_status") == "UNKNOWN":
            lifecycle_state, reason = "OPEN", "current unified truth remains UNKNOWN"
        else:
            lifecycle_state, reason = "RECOVERED", "unified truth is no longer UNKNOWN"
    out = dict(alert)
    out.update({"lifecycle_state": lifecycle_state, "lifecycle_reason": reason, "source_event_type": source_event.get("event_type") if source_event else None})
    return out


def _portal_alert_state(source_event: dict[str, Any] | None, events: list[dict[str, Any]]) -> tuple[str, str]:
    if source_event is None:
        latest_portal = latest_event([event for event in events if str(event.get("event_type", "")).startswith("PORTAL_")])
        if latest_portal and latest_portal.get("status") == "OK":
            return "RECOVERED", "latest portal probe is OK"
        return "OPEN", "portal alert source event unavailable"
    later_ok = events_after(events, source_event, lambda event: event.get("event_type") == "PORTAL_AVAILABILITY_OBSERVED" and event.get("status") == "OK")
    if later_ok:
        payload = source_event.get("payload") or {}
        if payload.get("simulated") is True or payload.get("simulate_status") or payload.get("test_evidence") is True:
            return "HISTORICAL_TEST", "simulated portal outage preserved as historical test evidence"
        return "RECOVERED", "later portal probe is OK"
    return "OPEN", "no later successful portal probe"
