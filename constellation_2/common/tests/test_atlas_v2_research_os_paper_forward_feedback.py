from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.paper_forward_feedback import route_paper_forward_feedback
from constellation_2.common.atlas_v2_research_os.paper_forward_outcomes import create_paper_forward_observation_plan, record_paper_forward_observation_result

NOW = "2026-06-05T00:00:00Z"


def test_feedback_routes_to_memory_and_research_effectiveness(tmp_path) -> None:
    plan = create_paper_forward_observation_plan(
        candidate_id="candidate-feedback",
        plan_id="plan-feedback",
        observation_start="2026-06-01",
        observation_end="2026-06-05",
        mechanism_tags=["OPENING_RANGE"],
        created_at=NOW,
    )
    outcome = record_paper_forward_observation_result(plan, [{"return": value} for value in [0.01, 0.01, -0.002, 0.004, 0.003]], created_at=NOW)
    routed = route_paper_forward_feedback(outcome, root=tmp_path, created_at=NOW)
    assert routed["memory"]["evidence_level"] == "PAPER_FORWARD_OBSERVATION"
    assert routed["research_activity"]["backlog_type"] == "PAPER_FORWARD_OBSERVATION"
    assert routed["research_effectiveness_contribution"]["useful_learning_score"] >= 0
    assert routed["authority_boundary"]["trading_authorized"] is False
