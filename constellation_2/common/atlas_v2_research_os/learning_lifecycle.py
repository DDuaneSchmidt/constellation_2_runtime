from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifact_models import LifecycleState, LifecycleTransition
from .artifact_store import ArtifactStore, ArtifactStoreError

ALLOWED_TRANSITIONS = {
    "NEW": {"UNDER_TEST", "QUARANTINED"},
    "UNDER_TEST": {"SUPPORTED", "QUARANTINED"},
    "SUPPORTED": {"STRONGLY_SUPPORTED", "WEAKENED", "STALE", "QUARANTINED"},
    "STRONGLY_SUPPORTED": {"QUARANTINED"},
    "WEAKENED": {"FALSIFIED", "QUARANTINED"},
    "FALSIFIED": {"QUARANTINED"},
    "STALE": {"RETIRED", "QUARANTINED"},
    "RETIRED": {"REOPENED", "QUARANTINED"},
    "REOPENED": {"UNDER_TEST", "QUARANTINED"},
    "QUARANTINED": set(),
}
NON_INFLUENCE_STATES = {"FALSIFIED", "RETIRED", "QUARANTINED"}


class LifecycleError(ValueError):
    pass


def transition_artifact_state(store: ArtifactStore, artifact_id: str, *, to_state: str, reason: str, created_at: str, created_by: str) -> dict[str, Any]:
    if not reason.strip():
        raise LifecycleError("lifecycle transition requires a reason")
    if to_state not in {state.value for state in LifecycleState}:
        raise LifecycleError(f"invalid lifecycle state: {to_state}")
    from_state = current_state(store, artifact_id)
    if to_state not in ALLOWED_TRANSITIONS.get(from_state, set()):
        raise LifecycleError(f"invalid lifecycle transition: {from_state} -> {to_state}")
    transition = LifecycleTransition(
        transition_id=f"lt-{artifact_id}-{len(get_lifecycle_history(store, artifact_id)) + 1:04d}",
        artifact_id=artifact_id,
        from_state=from_state,
        to_state=to_state,
        reason=reason,
        created_at=created_at,
        created_by=created_by,
    ).to_dict()
    path = transitions_path(store)
    rows = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    rows.append(transition)
    path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return transition


def get_lifecycle_history(store: ArtifactStore, artifact_id: str) -> list[dict[str, Any]]:
    path = transitions_path(store)
    if not path.exists():
        return []
    return [row for row in json.loads(path.read_text(encoding="utf-8")) if row["artifact_id"] == artifact_id]


def current_state(store: ArtifactStore, artifact_id: str) -> str:
    artifact = store.get_artifact(artifact_id)
    history = get_lifecycle_history(store, artifact_id)
    if history:
        return history[-1]["to_state"]
    return str(artifact.get("lifecycle_state", "NEW"))


def can_influence_priority(store: ArtifactStore, artifact_id: str) -> bool:
    return current_state(store, artifact_id) not in NON_INFLUENCE_STATES


def can_influence_candidate_generation(store: ArtifactStore, artifact_id: str) -> bool:
    return False


def transitions_path(store: ArtifactStore) -> Path:
    return store.root / "lifecycle_transitions.json"
