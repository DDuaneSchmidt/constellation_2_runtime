from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog
from constellation_2.common.atlas_v2_research_os.stage_controller import select_backlog_items, validate_run_eligibility

NOW = "2026-06-05T00:00:00Z"


def _seed(root: Path) -> None:
    ArtifactStore(root).create_artifact(artifact_id="q-1", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)
    ResearchBacklog(root).create_backlog_item(backlog_item_id="bi-1", item_type="RESEARCH_QUESTION", title="Question", description="Desc", created_at=NOW, created_by="test", state="READY", source_artifact_ids=["q-1"], expected_learning_value=0.9)


def test_select_backlog_items_returns_priority_selection(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    assert select_backlog_items(root, limit=1)[0]["backlog_item_id"] == "bi-1"


def test_validate_run_eligibility_fails_closed_on_blocked_graph(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    _seed(root)
    selected = select_backlog_items(root, limit=1)
    result = validate_run_eligibility(root, selected_backlog_items=selected, runtime_truth={"autonomous_execution_allowed": False, "trade_advice_allowed": False}, verified_graph={"graph_status": "BLOCKED"})
    assert result["eligible"] is False
    assert result["skip_reason"] == "VERIFIED_GRAPH_BLOCKED"
    assert any(row["gate_id"] == "authority_boundary" and row["result"] == "FAIL" for row in result["safety_gate_results"])


def test_validate_run_eligibility_can_audit_without_backlog_item(tmp_path: Path) -> None:
    root = tmp_path / "research_os"
    result = validate_run_eligibility(root, selected_backlog_items=[], runtime_truth={"autonomous_execution_allowed": False, "trade_advice_allowed": False}, verified_graph={"graph_status": "READY"}, require_backlog_item=False)
    assert result["skip_reason"] is None
