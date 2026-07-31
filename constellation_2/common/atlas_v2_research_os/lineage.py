from __future__ import annotations

from collections import deque

from .artifact_models import DERIVED_TYPES, ROOT_TYPES
from .artifact_store import ArtifactStore
from .failure_observatory import record_failure


def get_parents(store: ArtifactStore, artifact_id: str) -> list[str]:
    return list(store.get_artifact(artifact_id).get("source_artifact_ids", []))


def get_children(store: ArtifactStore, artifact_id: str) -> list[str]:
    return [item["artifact_id"] for item in store.list_artifacts() if artifact_id in item.get("source_artifact_ids", [])]


def get_ancestors(store: ArtifactStore, artifact_id: str) -> list[str]:
    seen: set[str] = set()
    queue = deque(get_parents(store, artifact_id))
    while queue:
        current = queue.popleft()
        if current in seen:
            continue
        seen.add(current)
        queue.extend(get_parents(store, current))
    return sorted(seen)


def get_descendants(store: ArtifactStore, artifact_id: str) -> list[str]:
    seen: set[str] = set()
    queue = deque(get_children(store, artifact_id))
    while queue:
        current = queue.popleft()
        if current in seen:
            continue
        seen.add(current)
        queue.extend(get_children(store, current))
    return sorted(seen)


def validate_no_orphans(store: ArtifactStore) -> tuple[bool, list[str]]:
    failures: list[str] = []
    ids = {item["artifact_id"] for item in store.list_artifacts()}
    for artifact in store.list_artifacts():
        aid = artifact["artifact_id"]
        sources = artifact.get("source_artifact_ids", [])
        if artifact["artifact_type"] in DERIVED_TYPES and not sources:
            failures.append(f"derived artifact has no source: {aid}")
        if artifact["artifact_type"] in ROOT_TYPES and not sources and not artifact.get("is_root"):
            failures.append(f"root artifact not explicitly marked: {aid}")
        for source in sources:
            if source not in ids:
                failures.append(f"artifact {aid} missing source {source}")
    if failures:
        record_failure(root=getattr(store, "root", "reports/atlas_v2_research_os"), component="LINEAGE", severity="ERROR", error_type="LINEAGE_VALIDATION_FAILED", error_message="Lineage validation failed.", recoverable=True, metadata={"failure_count": len(failures)})
    return not failures, failures


def validate_lineage_integrity(store: ArtifactStore) -> tuple[bool, list[str]]:
    failures: list[str] = []
    ok, orphan_failures = validate_no_orphans(store)
    failures.extend(orphan_failures)
    ids = {item["artifact_id"] for item in store.list_artifacts()}
    for artifact in store.list_artifacts():
        for superseded in artifact.get("supersedes_artifact_ids", []):
            if superseded not in ids:
                failures.append(f"artifact {artifact['artifact_id']} supersedes missing artifact {superseded}")
            elif superseded == artifact["artifact_id"]:
                failures.append(f"artifact supersedes itself: {artifact['artifact_id']}")
    if failures and not orphan_failures:
        record_failure(root=getattr(store, "root", "reports/atlas_v2_research_os"), component="LINEAGE", severity="ERROR", error_type="LINEAGE_VALIDATION_FAILED", error_message="Lineage validation failed.", recoverable=True, metadata={"failure_count": len(failures)})
    return not failures, failures
