from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.learning_feedback_models import LearningFeedbackSignal


def test_learning_feedback_model_requires_common_fields() -> None:
    row = LearningFeedbackSignal(
        feedback_id="lf1",
        created_at="2026-06-04T00:00:00Z",
        source_memory_ids=["mem1"],
        source_artifact_ids=["art1"],
        source_candidate_quality_evaluation_ids=["cq1"],
        affected_backlog_item_ids=["bi1"],
        affected_priority_scores={"bi1": 1.0},
        influence_type="FAILURE_ANALYSIS",
        influence_reason="test",
        evidence_level="GENERATED_ONLY",
        lifecycle_state="NEW",
        governance_status="PASS",
        lineage_status="LINKED",
        metadata={"label": "generated-only research priority"},
    ).to_dict()
    assert row["feedback_id"] == "lf1"
    assert row["source_memory_ids"] == ["mem1"]
