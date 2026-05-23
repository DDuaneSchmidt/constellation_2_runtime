from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.macro_events.macro_event_calendar import validate_macro_event_calendar_snapshot
from research_lab.macro_events.macro_event_loader import import_macro_event_calendar
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.parquet_io import write_parquet_records
from research_lab.storage.paths import ensure_store_layout


def _registry_path(store: Path) -> Path:
    return store / "registries" / "macro_event_calendar_snapshots.jsonl"


def store_macro_event_calendar_snapshot(snapshot: dict[str, Any], events: list[dict[str, Any]], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_macro_event_calendar_snapshot(snapshot)
    store = ensure_store_layout(store_root)
    root = store / "macro_events" / snapshot["macro_event_calendar_snapshot_id"]
    write_json(root / "macro_event_calendar_snapshot.json", snapshot, overwrite=False)
    events_info = write_parquet_records(root / "macro_events.parquet", events, allow_json_fallback=True)
    row = {"macro_event_calendar_snapshot_id": snapshot["macro_event_calendar_snapshot_id"], "event_count": snapshot["event_count"], "event_types": snapshot["event_types"], "content_hash": snapshot["content_hash"], "created_at": snapshot["created_at"], "storage_uri": snapshot["storage_uri"], "schema_version": snapshot["schema_version"]}
    append_jsonl(_registry_path(store), row)
    audit = write_audit_event(actor=actor, entity_type="macro_event_calendar_snapshot", entity_id=snapshot["macro_event_calendar_snapshot_id"], action="macro_event_calendar_imported", previous_state_hash="", new_state_hash=snapshot["content_hash"], reason="Imported MacroEventCalendarSnapshot for research data expansion only.", metadata={"registry_row": row, "events_info": events_info}, store_root=store)
    return {"macro_event_calendar_snapshot": snapshot, "registry_row": row, "audit_event": audit, "events_info": events_info, "json_path": str(root / "macro_event_calendar_snapshot.json")}


def import_and_store_macro_event_calendar(*, input_path: Path, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    snapshot, events = import_macro_event_calendar(input_path, created_by=actor)
    return store_macro_event_calendar_snapshot(snapshot, events, store_root=store_root, actor=actor)


def latest_macro_event_calendar_snapshot(*, store_root: Path | None = None) -> dict[str, Any] | None:
    store = ensure_store_layout(store_root)
    rows = read_jsonl(_registry_path(store))
    if not rows:
        return None
    return read_json(store / "macro_events" / str(rows[-1]["macro_event_calendar_snapshot_id"]) / "macro_event_calendar_snapshot.json")
