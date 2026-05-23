from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso

SCHEMA_VERSION = "macro_event_calendar_snapshot.v1"
ALLOWED_EVENT_TYPES = {"FOMC", "CPI", "NFP", "OPEX", "TREASURY_AUCTION", "EARNINGS_SEASON", "GEOPOLITICAL", "ENERGY_SHOCK", "RATE_SHOCK", "CREDIT_STRESS", "OTHER"}


def validate_macro_event(event: dict[str, Any]) -> None:
    if event.get("event_type") not in ALLOWED_EVENT_TYPES:
        raise ValueError(f"invalid macro event type: {event.get('event_type')}")
    validate_contract("macro_event", event)


def normalize_macro_event(row: dict[str, Any]) -> dict[str, Any]:
    event = {"event_date": str(row.get("event_date") or "")[:10], "event_type": str(row.get("event_type") or "").strip().upper(), "event_name": str(row.get("event_name") or "").strip(), "importance": str(row.get("importance") or "").strip(), "source": str(row.get("source") or "").strip(), "notes": str(row.get("notes") or "").strip(), "schema_version": "macro_event.v1"}
    validate_macro_event(event)
    return event


def recompute_macro_event_calendar_snapshot_hash(snapshot: dict[str, Any]) -> str:
    return content_hash(snapshot, exclude={"macro_event_calendar_snapshot_id", "storage_uri", "created_at", "content_hash"}, sort_lists=False)


def build_macro_event_calendar_snapshot(*, events: list[dict[str, Any]], source_path: str, created_by: str = "Aegis", created_at: str | None = None) -> dict[str, Any]:
    normalized = sorted([normalize_macro_event(event) for event in events], key=lambda row: (row["event_date"], row["event_type"], row["event_name"]))
    payload = {"macro_event_calendar_snapshot_id": "", "source_path": source_path, "event_count": len(normalized), "event_types": sorted({row["event_type"] for row in normalized}), "start_date": normalized[0]["event_date"] if normalized else "", "end_date": normalized[-1]["event_date"] if normalized else "", "storage_uri": "", "created_at": created_at or utc_now_iso(), "created_by": created_by, "schema_version": SCHEMA_VERSION, "research_label": "RESEARCH_ONLY", "content_hash": ""}
    fingerprint = recompute_macro_event_calendar_snapshot_hash(payload)
    payload["macro_event_calendar_snapshot_id"] = f"mecs_{short_hash(content_hash({'fingerprint': fingerprint, 'created_at': payload['created_at']}), 16)}"
    payload["storage_uri"] = f"research://macro_events/{payload['macro_event_calendar_snapshot_id']}"
    payload["content_hash"] = fingerprint
    validate_macro_event_calendar_snapshot(payload)
    return payload


def validate_macro_event_calendar_snapshot(snapshot: dict[str, Any]) -> None:
    validate_contract("macro_event_calendar_snapshot", snapshot)
    actual = recompute_macro_event_calendar_snapshot_hash(snapshot)
    if actual != snapshot.get("content_hash"):
        raise ValueError(f"MacroEventCalendarSnapshot content_hash mismatch: expected {snapshot.get('content_hash')}, got {actual}")
