from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.paper_forward_outcome_models import (
    PAPER_FORWARD_EVIDENCE_LEVEL,
    CandidateSurvivalAnalytics,
    PaperForwardObservationPlan,
    PaperForwardObservationResult,
)

NOW = "2026-06-05T00:00:00Z"


def test_creates_paper_forward_plan_result_and_analytics() -> None:
    plan = PaperForwardObservationPlan(
        plan_id="plan-1",
        candidate_id="candidate-1",
        observation_start="2026-06-01",
        observation_end="2026-06-05",
        regime_context={"label": "OPENING_SESSION"},
        mechanism_tags=["OPENING_RANGE"],
        created_at=NOW,
    ).to_dict()
    assert plan["status"] == "PENDING"
    result = PaperForwardObservationResult(
        candidate_id="candidate-1",
        plan_id="plan-1",
        observation_start="2026-06-01",
        observation_end="2026-06-05",
        sample_size=5,
        wins=4,
        losses=1,
        average_return=0.004,
        expectancy=0.004,
        max_drawdown=-0.002,
        profit_factor=8.0,
        regime_context={"label": "OPENING_SESSION"},
        hypothesis_supported=True,
        hypothesis_weakened=False,
        hypothesis_falsified=False,
        notes=["sample supports observation"],
        status="SURVIVED",
        created_at=NOW,
    ).to_dict()
    assert result["evidence_level"] == PAPER_FORWARD_EVIDENCE_LEVEL
    analytics = CandidateSurvivalAnalytics(
        candidate_id="candidate-1",
        plan_id="plan-1",
        status="SURVIVED",
        sample_size=5,
        survival_rate=0.8,
        failure_rate=0.2,
        expectancy=0.004,
        average_return=0.004,
        max_drawdown=-0.002,
        profit_factor=8.0,
        hypothesis_supported=True,
        hypothesis_weakened=False,
        hypothesis_falsified=False,
        regime_context={"label": "OPENING_SESSION"},
        notes=[],
    ).to_dict()
    assert analytics["hypothesis_supported"] is True


def test_rejects_invalid_status() -> None:
    try:
        PaperForwardObservationPlan(
            plan_id="plan-1",
            candidate_id="candidate-1",
            observation_start="2026-06-01",
            observation_end="2026-06-05",
            regime_context={},
            status="LIVE_APPROVED",
        ).to_dict()
    except ValueError as exc:
        assert "invalid paper-forward outcome status" in str(exc)
    else:
        raise AssertionError("invalid status should fail")
