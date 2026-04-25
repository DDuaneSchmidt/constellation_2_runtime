from __future__ import annotations

import json

from .schemas import content_hash, utc_now
from .store import ArtifactStore
from .types import AuditEvent

CANONICAL_AUDIT_STREAM = "governed_audit"


def _append_to_canonical_stream(store: ArtifactStore, payload: dict) -> None:
    path = store.stream_path(CANONICAL_AUDIT_STREAM)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def read_canonical_audit_stream(store: ArtifactStore) -> list[dict]:
    path = store.stream_path(CANONICAL_AUDIT_STREAM)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle.readlines() if line.strip()]


def reset_canonical_audit_stream(store: ArtifactStore) -> None:
    path = store.stream_path(CANONICAL_AUDIT_STREAM)
    path.write_text("", encoding="utf-8")


def emit_audit_event(
    store: ArtifactStore,
    event_type: str,
    actor: str,
    *,
    artifact_refs: tuple[str, ...] = (),
    details: dict | None = None,
) -> str:
    event_at = utc_now()
    event = AuditEvent(
        event_id=f"{event_type.lower()}-{content_hash({'event_type': event_type, 'actor': actor, 'artifact_refs': artifact_refs, 'details': details or {}, 'event_at': event_at})[:12]}",
        event_type=event_type,
        event_at=event_at,
        actor=actor,
        artifact_refs=artifact_refs,
        details=details or {},
    )
    store.write_immutable("audit_events", event.event_id, event, artifact_type="AuditEvent")
    _append_to_canonical_stream(
        store,
        {
            "stream_type": "governed_audit_event",
            "timestamp": event_at,
            "event_id": event.event_id,
            "event_type": event_type,
            "actor": actor,
            "artifact_refs": list(artifact_refs),
            "details": details or {},
        },
    )
    return event.event_id


def append_runtime_event(store: ArtifactStore, event: dict) -> str:
    event_at = utc_now()
    event_id = f"runtime-event-{content_hash({'event': event, 'event_at': event_at})[:12]}"
    _append_to_canonical_stream(
        store,
        {
            "stream_type": "runtime_event",
            "timestamp": event_at,
            "ts_epoch": None,
            "event_id": event_id,
            "event": event,
        },
    )
    return event_id
