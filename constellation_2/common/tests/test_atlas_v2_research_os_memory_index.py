from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.memory_index import MemoryIndexError, add_memory_object, get_memory_object, link_memory_to_artifact, list_memory_objects, load_memory_index, save_memory_index, validate_memory_integrity
from constellation_2.common.atlas_v2_research_os.memory_models import MemoryType, create_memory_object

NOW = "2026-06-04T00:00:00Z"


def _store(root: Path) -> ArtifactStore:
    store = ArtifactStore(root)
    store.create_artifact(artifact_id="q-1", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)
    return store


def test_saves_and_loads_memory_index(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    index = load_memory_index(root)
    index["review_queue"].append("review-1")
    save_memory_index(index, root)
    assert load_memory_index(root)["review_queue"] == ["review-1"]


def test_add_get_list_and_link_memory_object(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    store = _store(root)
    memory = create_memory_object(memory_id="mem-1", memory_type=MemoryType.CLAIM_CLUSTER.value, created_at=NOW, source_artifact_ids=["q-1"], mechanism_tags=["OPENING_RANGE"])
    add_memory_object(memory, root, artifact_store=store)
    assert get_memory_object("mem-1", root)["memory_id"] == "mem-1"
    assert len(list_memory_objects(root, MemoryType.CLAIM_CLUSTER.value)) == 1
    linked = link_memory_to_artifact("mem-1", "q-1", root, artifact_store=store)
    assert "q-1" in linked["source_artifact_ids"]


def test_validates_memory_integrity(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    store = _store(root)
    add_memory_object(create_memory_object(memory_id="mem-2", memory_type=MemoryType.EVIDENCE_TRAIL.value, created_at=NOW, source_artifact_ids=["q-1"]), root, artifact_store=store)
    ok, failures = validate_memory_integrity(root, artifact_store=store)
    assert ok is True
    assert failures == []


def test_rejects_missing_source_artifact_unless_explicit_root(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    store = ArtifactStore(root)
    with pytest.raises(MemoryIndexError, match="no source"):
        add_memory_object(create_memory_object(memory_id="mem-3", memory_type=MemoryType.MECHANISM.value, created_at=NOW), root, artifact_store=store)
    added = add_memory_object(create_memory_object(memory_id="mem-root", memory_type=MemoryType.MECHANISM.value, created_at=NOW, is_root=True), root, artifact_store=store)
    assert added["is_root"] is True


def test_preserves_lineage(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    store = _store(root)
    row = add_memory_object(create_memory_object(memory_id="mem-4", memory_type=MemoryType.LEARNING_NODE.value, created_at=NOW, source_artifact_ids=["q-1"]), root, artifact_store=store)
    assert row["source_artifact_ids"] == ["q-1"]
    assert load_memory_index(root)["source_artifact_index"]["q-1"] == ["mem-4"]
