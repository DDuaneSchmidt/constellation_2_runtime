from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.aegis_truth.state_machines.kernel_state_types_v1 import age_seconds, base_state, events_after, latest_event, read_day_events, write_report

SCHEMA_VERSION = "portal_surface_state.v1"
REPORT_NAME = "portal_surface_state_v1"
FILENAME = "portal_surface_state.v1.json"
PORTAL_EVENT_TYPES = {
    "PORTAL_AVAILABILITY_OBSERVED",
    "PORTAL_ORIGIN_UNREACHABLE",
    "PORTAL_HEALTH_ENDPOINT_MISSING",
    "PORTAL_HEALTH_SCHEMA_INVALID",
    "PORTAL_PROJECTION_STALE",
}


def build_portal_surface_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN") -> dict[str, Any]:
    events = read_day_events(truth_root=truth_root, target_day=target_day)
    portal_events = [event for event in events if event.get("event_type") in PORTAL_EVENT_TYPES]
    latest = latest_event(portal_events)
    latest_ok = latest_event([event for event in portal_events if event.get("event_type") == "PORTAL_AVAILABILITY_OBSERVED" and event.get("status") == "OK"])
    recovered_from_event = None
    state_events = portal_events[-20:]
    if latest is None:
        status, blocker, severity = "UNKNOWN", "no portal availability evidence", "ERROR"
        next_action = "Run the portal availability probe."
    elif latest.get("event_type") == "PORTAL_AVAILABILITY_OBSERVED" and latest.get("status") == "OK":
        prior_down = [event for event in portal_events if event.get("event_type") == "PORTAL_ORIGIN_UNREACHABLE" or event.get("severity") == "CRITICAL"]
        recovered_from_event = prior_down[-1]["event_id"] if prior_down else None
        status, blocker, severity = "OK", None, "INFO"
        next_action = "No operator action required."
    elif latest.get("event_type") == "PORTAL_PROJECTION_STALE" or latest.get("status") == "STALE":
        status, blocker, severity = "DEGRADED", latest.get("blocker") or "portal projection stale", latest.get("severity", "WARN")
        next_action = latest.get("next_action") or "Refresh portal projection and rerun portal probe."
    elif latest.get("event_type") == "PORTAL_ORIGIN_UNREACHABLE" or latest.get("severity") == "CRITICAL":
        later_ok = events_after(portal_events, latest, lambda event: event.get("event_type") == "PORTAL_AVAILABILITY_OBSERVED" and event.get("status") == "OK")
        if later_ok:
            recovered_from_event = latest.get("event_id")
            status, blocker, severity = "RECOVERED", None, "INFO"
            next_action = "No operator action required."
            latest_ok = later_ok[-1]
        else:
            status, blocker, severity = "DOWN", latest.get("blocker") or "portal is unreachable", "CRITICAL"
            next_action = latest.get("next_action") or "Repair portal origin or tunnel, then rerun portal probe."
    else:
        status, blocker, severity = "UNKNOWN", latest.get("blocker") or "portal health schema/status unknown", latest.get("severity", "ERROR")
        next_action = latest.get("next_action") or "Inspect portal health endpoint and rerun probe."
    result = base_state(schema_version=SCHEMA_VERSION, target_day=target_day, environment=environment, status=status, blocker=blocker, severity=severity, owner="portal", next_action=next_action, events=state_events)
    result.update({
        "latest_event": latest,
        "latest_successful_probe_event_id": latest_ok.get("event_id") if latest_ok else None,
        "latest_successful_probe_age_seconds": age_seconds(latest_ok.get("observed_at_utc")) if latest_ok else None,
        "recovered_from_event_id": recovered_from_event,
        "recovery_observed": bool(recovered_from_event and status in {"OK", "RECOVERED"}),
    })
    return result


def write_portal_surface_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN") -> Path:
    return write_report(truth_root, REPORT_NAME, target_day, FILENAME, build_portal_surface_state(target_day=target_day, truth_root=truth_root, environment=environment))
