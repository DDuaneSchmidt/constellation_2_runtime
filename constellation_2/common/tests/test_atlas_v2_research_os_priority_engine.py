from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.learning_lifecycle import transition_artifact_state
from constellation_2.common.atlas_v2_research_os.priority_engine import PriorityEngine, score_item
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog

NOW = "2026-06-04T00:00:00Z"


def test_computes_expected_priority_score() -> None:
    item = {"expected_learning_value": 1, "novelty_score": 2, "candidate_impact_estimate": 3, "failure_reduction_score": 4, "evidence_gap_score": 5, "regime_gap_score": 6, "cost_estimate": 2}
    assert score_item(item) == 19


def test_selects_highest_score_deterministically(tmp_path: Path) -> None:
    backlog = ResearchBacklog(tmp_path)
    backlog.create_backlog_item(backlog_item_id="a", item_type="RESEARCH_QUESTION", title="A", description="A", created_at=NOW, created_by="test", state="READY", expected_learning_value=0.1)
    backlog.create_backlog_item(backlog_item_id="b", item_type="RESEARCH_QUESTION", title="B", description="B", created_at=NOW, created_by="test", state="READY", expected_learning_value=0.9)
    selected = PriorityEngine(backlog).select_next_items(limit=1, exploration_rate=0.0)
    assert selected == [{"backlog_item_id": "b", "priority_score": 0.9, "selection_reason": "highest deterministic priority", "selection_mode": "PRIORITY"}]


def test_supports_reproducible_exploration_with_seed(tmp_path: Path) -> None:
    backlog = ResearchBacklog(tmp_path)
    for idx in range(5):
        backlog.create_backlog_item(backlog_item_id=f"bi-{idx}", item_type="RESEARCH_QUESTION", title=str(idx), description="D", created_at=NOW, created_by="test", state="READY", expected_learning_value=idx / 10)
    first = PriorityEngine(backlog).select_next_items(limit=3, exploration_rate=1.0, seed=42)
    second = PriorityEngine(backlog).select_next_items(limit=3, exploration_rate=1.0, seed=42)
    assert first == second
    assert all(item["selection_mode"] == "EXPLORATION" for item in first)


def test_excludes_retired_falsified_quarantined_artifacts_from_influence(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    store = ArtifactStore(root)
    store.create_artifact(artifact_id="q-001", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)
    transition_artifact_state(store, "q-001", to_state="UNDER_TEST", reason="start", created_at=NOW, created_by="test")
    transition_artifact_state(store, "q-001", to_state="SUPPORTED", reason="support", created_at=NOW, created_by="test")
    transition_artifact_state(store, "q-001", to_state="STALE", reason="stale", created_at=NOW, created_by="test")
    transition_artifact_state(store, "q-001", to_state="RETIRED", reason="retire", created_at=NOW, created_by="test")
    backlog = ResearchBacklog(root)
    backlog.create_backlog_item(backlog_item_id="bi-001", item_type="RESEARCH_QUESTION", title="A", description="A", created_at=NOW, created_by="test", state="READY", source_artifact_ids=["q-001"], expected_learning_value=1)
    assert PriorityEngine(backlog, store).select_next_items(limit=1, exploration_rate=0.0) == []
