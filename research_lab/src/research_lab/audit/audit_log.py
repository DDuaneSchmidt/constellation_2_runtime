from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, sha256_hex, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_jsonl
from research_lab.storage.paths import ensure_store_layout


AUDIT_SCHEMA_VERSION = "audit_event.v1"


def build_audit_event(
    *,
    actor: str,
    entity_type: str,
    entity_id: str,
    action: str,
    previous_state_hash: str | None = None,
    new_state_hash: str | None = None,
    reason: str = "",
    metadata: dict[str, Any] | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    event_body = {
        "timestamp": timestamp or utc_now_iso(),
        "actor": actor,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "action": action,
        "previous_state_hash": previous_state_hash or "",
        "new_state_hash": new_state_hash or "",
        "reason": reason,
        "metadata": metadata or {},
        "schema_version": AUDIT_SCHEMA_VERSION,
    }
    event_body["event_id"] = f"audit_{short_hash(sha256_hex(event_body), 16)}"
    validate_contract("audit_event", event_body)
    return event_body


def append_audit_event(event: dict[str, Any], *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    validate_contract("audit_event", event)
    append_jsonl(store / "audit_log" / "audit_events.jsonl", event)
    return event


def write_audit_event(
    *,
    actor: str,
    entity_type: str,
    entity_id: str,
    action: str,
    previous_state_hash: str | None = None,
    new_state_hash: str | None = None,
    reason: str = "",
    metadata: dict[str, Any] | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    event = build_audit_event(
        actor=actor,
        entity_type=entity_type,
        entity_id=entity_id,
        action=action,
        previous_state_hash=previous_state_hash,
        new_state_hash=new_state_hash,
        reason=reason,
        metadata=metadata,
    )
    return append_audit_event(event, store_root=store_root)


def audit_events(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(store / "audit_log" / "audit_events.jsonl")


def state_hash(payload: dict[str, Any]) -> str:
    return content_hash(payload, sort_lists=True)
