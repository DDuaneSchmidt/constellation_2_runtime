from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _plan(**overrides):
    from constellation_2.common.atlas_v2_research_os.paper_trade_test_plan import create_paper_trade_test_plan

    data = dict(
        test_plan_id="plan-1",
        candidate_id="candidate-1",
        mechanism_tags=["MEAN_REVERSION"],
        regime_context="LOW_VOL",
        entry_condition_description="Observe entry when paper signal crosses threshold.",
        exit_condition_description="Observe exit when paper signal mean reverts or window expires.",
        invalidating_conditions=["Regime changes", "Data unavailable"],
        paper_observation_window={"days": 30},
        minimum_sample_size=20,
        source_artifact_ids=["artifact-1"],
        created_at="2026-06-05T00:00:00Z",
    )
    data.update(overrides)
    return create_paper_trade_test_plan(**data)

from constellation_2.common.atlas_v2_research_os.paper_trade_test_plan import validate_test_plan


def test_create_paper_trade_test_plan_contains_required_fields() -> None:
    plan = _plan()
    assert plan["test_plan_id"] == "plan-1"
    assert plan["candidate_id"] == "candidate-1"
    assert "win_rate" in plan["success_metrics"]
    assert "max_drawdown" in plan["failure_metrics"]
    assert plan["human_review_required"] is True
    assert "no broker execution" in plan["metadata"]["authority_boundary"].lower()
    assert validate_test_plan(plan) is True


def test_invalid_metric_rejected() -> None:
    try:
        _plan(success_metrics=["live_trading_approval"])
    except ValueError as exc:
        assert "invalid success metrics" in str(exc)
    else:
        raise AssertionError("invalid metric should be rejected")


def test_test_plan_requires_invalidating_conditions() -> None:
    try:
        _plan(invalidating_conditions=[])
    except ValueError as exc:
        assert "invalidating_conditions" in str(exc)
    else:
        raise AssertionError("missing invalidating conditions should fail")
