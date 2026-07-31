from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog

NOW = "2026-06-04T00:00:00Z"


def _backlog(tmp_path: Path) -> ResearchBacklog:
    return ResearchBacklog(tmp_path / "research_os")


def test_creates_backlog_item(tmp_path: Path) -> None:
    backlog = _backlog(tmp_path)
    item = backlog.create_backlog_item(backlog_item_id="bi-001", item_type="RESEARCH_QUESTION", title="Question", description="Desc", created_at=NOW, created_by="test", expected_learning_value=0.5, novelty_score=0.2, cost_estimate=0.1)
    assert item["priority_score"] == 0.6
    assert item["state"] == "NEW"


def test_updates_backlog_state(tmp_path: Path) -> None:
    backlog = _backlog(tmp_path)
    backlog.create_backlog_item(backlog_item_id="bi-001", item_type="RESEARCH_QUESTION", title="Question", description="Desc", created_at=NOW, created_by="test")
    updated = backlog.update_backlog_state("bi-001", state="READY")
    assert updated["state"] == "READY"


def test_links_item_to_artifact(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    store = ArtifactStore(root)
    store.create_artifact(artifact_id="q-001", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)
    backlog = ResearchBacklog(root)
    backlog.create_backlog_item(backlog_item_id="bi-001", item_type="RESEARCH_QUESTION", title="Question", description="Desc", created_at=NOW, created_by="test")
    linked = backlog.link_backlog_item_to_artifact("bi-001", "q-001")
    assert linked["linked_artifact_ids"] == ["q-001"]
    assert linked["source_artifact_ids"] == ["q-001"]


def test_retrieves_ready_items(tmp_path: Path) -> None:
    backlog = _backlog(tmp_path)
    backlog.create_backlog_item(backlog_item_id="bi-001", item_type="RESEARCH_QUESTION", title="One", description="Desc", created_at=NOW, created_by="test", state="READY")
    backlog.create_backlog_item(backlog_item_id="bi-002", item_type="EVIDENCE_GAP", title="Two", description="Desc", created_at=NOW, created_by="test", state="NEW")
    assert [item["backlog_item_id"] for item in backlog.get_ready_items()] == ["bi-001"]


def test_retrieves_top_priority_items(tmp_path: Path) -> None:
    backlog = _backlog(tmp_path)
    backlog.create_backlog_item(backlog_item_id="low", item_type="RESEARCH_QUESTION", title="Low", description="Desc", created_at=NOW, created_by="test", expected_learning_value=0.1)
    backlog.create_backlog_item(backlog_item_id="high", item_type="RESEARCH_QUESTION", title="High", description="Desc", created_at=NOW, created_by="test", expected_learning_value=0.9)
    assert backlog.get_top_priority_items(1)[0]["backlog_item_id"] == "high"
