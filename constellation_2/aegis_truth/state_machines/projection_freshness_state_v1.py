from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.aegis_truth.state_machines.kernel_state_types_v1 import age_seconds, base_state, latest_event, read_day_events, write_report

SCHEMA_VERSION = "projection_freshness_state.v1"
REPORT_NAME = "projection_freshness_state_v1"
FILENAME = "projection_freshness_state.v1.json"
PROJECTION_TYPES = {"PROJECTION_GENERATED", "PROJECTION_STALE", "PROJECTION_FAILED", "PORTAL_PROJECTION_STALE", "PACKET_STALE", "PACKET_FRESHNESS_STALE"}


def build_projection_freshness_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN", max_age_seconds: int = 300) -> dict[str, Any]:
    events = read_day_events(truth_root=truth_root, target_day=target_day)
    projection_events = [event for event in events if event.get("event_type") in PROJECTION_TYPES or ("PACKET" in str(event.get("event_type", "")) and event.get("status") == "STALE")]
    latest = latest_event(projection_events)
    latest_generated = latest_event([event for event in projection_events if event.get("event_type") == "PROJECTION_GENERATED" and event.get("status") == "OK"])
    state_events = projection_events[-20:]
    if latest is None:
        status, blocker, severity = "UNKNOWN", "no projection freshness evidence", "WARN"
        next_action = "Run projection refresh."
    elif latest.get("event_type") == "PROJECTION_FAILED" or latest.get("status") == "UNKNOWN":
        status, blocker, severity = "FAILED", latest.get("blocker") or "projection generation failed", latest.get("severity", "ERROR")
        next_action = latest.get("next_action") or "Inspect projection generator."
    elif latest.get("event_type") in {"PROJECTION_STALE", "PORTAL_PROJECTION_STALE", "PACKET_STALE", "PACKET_FRESHNESS_STALE"} or latest.get("status") == "STALE":
        status, blocker, severity = "STALE", latest.get("blocker") or "projection stale", latest.get("severity", "WARN")
        next_action = latest.get("next_action") or "Refresh projections."
    elif latest_generated is not None:
        generated_age = age_seconds(latest_generated.get("observed_at_utc"))
        if generated_age is not None and generated_age > max_age_seconds:
            severity = "ERROR" if generated_age > max_age_seconds * 4 else "WARN"
            status, blocker = "STALE", f"projection age {generated_age}s exceeds {max_age_seconds}s"
            next_action = "Refresh projections."
        else:
            status, blocker, severity = "FRESH", None, "INFO"
            next_action = "No operator action required."
    else:
        status, blocker, severity = "UNKNOWN", "projection state unknown", "WARN"
        next_action = "Run projection refresh."
    result = base_state(schema_version=SCHEMA_VERSION, target_day=target_day, environment=environment, status=status, blocker=blocker, severity=severity, owner="projection", next_action=next_action, events=state_events)
    result.update({
        "latest_event": latest,
        "latest_generated_event_id": latest_generated.get("event_id") if latest_generated else None,
        "latest_generated_age_seconds": age_seconds(latest_generated.get("observed_at_utc")) if latest_generated else None,
        "max_age_seconds": max_age_seconds,
    })
    return result


def write_projection_freshness_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN", max_age_seconds: int = 300) -> Path:
    return write_report(truth_root, REPORT_NAME, target_day, FILENAME, build_projection_freshness_state(target_day=target_day, truth_root=truth_root, environment=environment, max_age_seconds=max_age_seconds))
