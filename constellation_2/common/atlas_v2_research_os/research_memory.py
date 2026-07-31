from __future__ import annotations

from pathlib import Path

from .artifact_store import DEFAULT_STORE_ROOT
from .memory_index import get_memory_object
from .memory_models import MemoryEvidenceMaturity, MemoryLifecycleState


def can_influence_priority(memory_id: str, root: str | Path = DEFAULT_STORE_ROOT) -> bool:
    row = get_memory_object(memory_id, root)
    if row.get("lifecycle_state") in {MemoryLifecycleState.RETIRED.value, MemoryLifecycleState.QUARANTINED.value}:
        return False
    if row.get("evidence_level") == MemoryEvidenceMaturity.GENERATED_ONLY.value:
        return "generated_only" in {str(label).lower() for label in row.get("labels", [])}
    return True


def can_influence_backlog_creation(memory_id: str, root: str | Path = DEFAULT_STORE_ROOT) -> bool:
    row = get_memory_object(memory_id, root)
    return row.get("lifecycle_state") not in {MemoryLifecycleState.QUARANTINED.value}


def can_influence_lifecycle_transition(memory_id: str, root: str | Path = DEFAULT_STORE_ROOT) -> bool:
    row = get_memory_object(memory_id, root)
    return row.get("lifecycle_state") not in {MemoryLifecycleState.QUARANTINED.value}


def can_influence_candidate_generation(memory_id: str, root: str | Path = DEFAULT_STORE_ROOT) -> bool:
    return False


def can_influence_production_interpretation(memory_id: str, root: str | Path = DEFAULT_STORE_ROOT) -> bool:
    row = get_memory_object(memory_id, root)
    return row.get("evidence_level") not in {MemoryEvidenceMaturity.GENERATED_ONLY.value, MemoryEvidenceMaturity.MOCK_ONLY.value}
