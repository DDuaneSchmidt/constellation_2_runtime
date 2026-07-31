from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.learning_lifecycle import LifecycleError, can_influence_priority, get_lifecycle_history, transition_artifact_state

NOW = "2026-06-04T00:00:00Z"


def _store_with_root(tmp_path: Path) -> ArtifactStore:
    store = ArtifactStore(tmp_path / "research_os")
    store.create_artifact(artifact_id="q-001", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)
    return store


def test_allows_valid_transitions(tmp_path: Path) -> None:
    store = _store_with_root(tmp_path)
    transition = transition_artifact_state(store, "q-001", to_state="UNDER_TEST", reason="begin test", created_at=NOW, created_by="test")
    assert transition["from_state"] == "NEW"
    assert transition["to_state"] == "UNDER_TEST"


def test_rejects_invalid_transitions(tmp_path: Path) -> None:
    store = _store_with_root(tmp_path)
    with pytest.raises(LifecycleError, match="invalid lifecycle transition"):
        transition_artifact_state(store, "q-001", to_state="SUPPORTED", reason="skip", created_at=NOW, created_by="test")


def test_requires_transition_reason(tmp_path: Path) -> None:
    store = _store_with_root(tmp_path)
    with pytest.raises(LifecycleError, match="requires a reason"):
        transition_artifact_state(store, "q-001", to_state="UNDER_TEST", reason="", created_at=NOW, created_by="test")


def test_preserves_transition_history(tmp_path: Path) -> None:
    store = _store_with_root(tmp_path)
    transition_artifact_state(store, "q-001", to_state="UNDER_TEST", reason="begin", created_at=NOW, created_by="test")
    transition_artifact_state(store, "q-001", to_state="SUPPORTED", reason="supported", created_at=NOW, created_by="test")
    history = get_lifecycle_history(store, "q-001")
    assert [item["to_state"] for item in history] == ["UNDER_TEST", "SUPPORTED"]


def test_prevents_retired_knowledge_from_influencing_priority(tmp_path: Path) -> None:
    store = _store_with_root(tmp_path)
    transition_artifact_state(store, "q-001", to_state="UNDER_TEST", reason="begin", created_at=NOW, created_by="test")
    transition_artifact_state(store, "q-001", to_state="SUPPORTED", reason="supported", created_at=NOW, created_by="test")
    transition_artifact_state(store, "q-001", to_state="STALE", reason="stale", created_at=NOW, created_by="test")
    transition_artifact_state(store, "q-001", to_state="RETIRED", reason="retired", created_at=NOW, created_by="test")
    assert can_influence_priority(store, "q-001") is False
