from __future__ import annotations

from ..meta_governance.audit_log import emit_audit_event
from ..meta_governance.store import ArtifactStore


def emit_adaptation_event(
    store: ArtifactStore,
    event_type: str,
    actor: str,
    *,
    artifact_refs: tuple[str, ...] = (),
    details: dict | None = None,
) -> str:
    return emit_audit_event(store, event_type, actor, artifact_refs=artifact_refs, details=details)
