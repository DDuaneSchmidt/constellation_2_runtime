from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore, ArtifactStoreError
from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.lineage import get_ancestors, get_children, get_descendants, get_parents, validate_lineage_integrity, validate_no_orphans

NOW = "2026-06-04T00:00:00Z"


def _store(tmp_path: Path) -> ArtifactStore:
    return ArtifactStore(tmp_path / "research_os")


def _root(store: ArtifactStore, artifact_id: str = "q-001") -> None:
    store.create_artifact(artifact_id=artifact_id, artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", confidence=0.4, labels=["generated"], is_root=True)


def test_creates_root_artifact(tmp_path: Path) -> None:
    store = _store(tmp_path)
    artifact = store.create_artifact(artifact_id="q-001", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", confidence=0.5, evidence_level="GENERATED_ONLY", labels=["generated"], metadata={"text": "question"}, is_root=True)
    assert artifact.artifact_id == "q-001"
    assert store.exists("q-001")
    assert (store.root / "artifact_index.json").exists()


def test_creates_derived_artifact(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _root(store)
    artifact = store.create_artifact(artifact_id="rh-001", artifact_type=ArtifactType.RESEARCH_HYPOTHESIS.value, created_at=NOW, created_by="test", source_artifact_ids=["q-001"], confidence=0.6)
    assert artifact.source_artifact_ids == ["q-001"]
    assert get_parents(store, "rh-001") == ["q-001"]


def test_rejects_orphan_derived_artifact(tmp_path: Path) -> None:
    store = _store(tmp_path)
    with pytest.raises(ArtifactStoreError, match="derived artifact requires"):
        store.create_artifact(artifact_id="rh-001", artifact_type=ArtifactType.RESEARCH_HYPOTHESIS.value, created_at=NOW, created_by="test")


def test_tracks_parent_child_lineage(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _root(store)
    store.create_artifact(artifact_id="rh-001", artifact_type=ArtifactType.RESEARCH_HYPOTHESIS.value, created_at=NOW, created_by="test", source_artifact_ids=["q-001"])
    assert get_children(store, "q-001") == ["rh-001"]


def test_tracks_ancestors_descendants(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _root(store)
    store.create_artifact(artifact_id="rh-001", artifact_type=ArtifactType.RESEARCH_HYPOTHESIS.value, created_at=NOW, created_by="test", source_artifact_ids=["q-001"])
    store.create_artifact(artifact_id="spec-001", artifact_type=ArtifactType.CHEAP_EXPERIMENT_SPEC.value, created_at=NOW, created_by="test", source_artifact_ids=["rh-001"])
    assert get_ancestors(store, "spec-001") == ["q-001", "rh-001"]
    assert get_descendants(store, "q-001") == ["rh-001", "spec-001"]


def test_validates_no_orphan_artifacts(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _root(store)
    ok, failures = validate_no_orphans(store)
    assert ok is True
    assert failures == []


def test_tracks_supersession_without_deleting_prior_artifact(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _root(store)
    replacement = store.write_updated_artifact_copy("q-001", new_artifact_id="q-002", created_at=NOW, created_by="test", metadata_updates={"revision": 2})
    assert store.exists("q-001")
    assert store.exists("q-002")
    assert replacement.supersedes_artifact_ids == ["q-001"]
    ok, failures = validate_lineage_integrity(store)
    assert ok, failures
