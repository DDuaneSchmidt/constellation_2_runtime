from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash

SCHEMA_VERSION = "breadth_event.v1"


def build_breadth_event(*, breadth_snapshot_id: str, date: str, event_type: str, metrics: dict[str, Any]) -> dict[str, Any]:
    payload = {"breadth_event_id": "", "breadth_snapshot_id": breadth_snapshot_id, "date": date, "universe_snapshot_id": str(metrics.get("universe_snapshot_id") or ""), "breadth_regime": str(metrics.get("breadth_regime") or "unknown"), "event_type": event_type, "metrics": metrics, "schema_version": SCHEMA_VERSION, "content_hash": ""}
    fingerprint = content_hash(payload, exclude={"breadth_event_id", "content_hash"}, sort_lists=False)
    payload["breadth_event_id"] = f"bre_{short_hash(fingerprint, 16)}"
    payload["content_hash"] = fingerprint
    validate_contract("breadth_event", payload)
    return payload
