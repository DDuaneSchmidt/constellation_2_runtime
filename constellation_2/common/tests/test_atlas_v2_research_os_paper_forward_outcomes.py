from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.paper_forward_outcomes import (
    build_candidate_survival_analytics,
    calculate_paper_forward_metrics,
    create_paper_forward_observation_plan,
    record_paper_forward_observation_result,
)

NOW = "2026-06-05T00:00:00Z"


def test_calculates_metrics_and_survived_status() -> None:
    plan = create_paper_forward_observation_plan(
        candidate_id="candidate-survive",
        plan_id="plan-survive",
        observation_start="2026-06-01",
        observation_end="2026-06-05",
        regime_context={"label": "OPEN"},
        mechanism_tags=["OPENING_RANGE"],
        created_at=NOW,
    )
    result = record_paper_forward_observation_result(
        plan,
        [{"return": value} for value in [0.01, 0.008, -0.002, 0.006, 0.004]],
        created_at=NOW,
    )
    assert result["sample_size"] == 5
    assert result["wins"] == 4
    assert result["losses"] == 1
    assert result["status"] == "SURVIVED"
    assert result["hypothesis_supported"] is True
    analytics = build_candidate_survival_analytics(result)
    assert analytics["survival_rate"] == 0.8


def test_handles_empty_sample_safely() -> None:
    plan = create_paper_forward_observation_plan(
        candidate_id="candidate-empty",
        plan_id="plan-empty",
        observation_start="2026-06-01",
        observation_end="2026-06-05",
        created_at=NOW,
    )
    result = record_paper_forward_observation_result(plan, [], created_at=NOW)
    assert result["status"] == "PENDING"
    assert result["sample_size"] == 0
    assert result["profit_factor"] is None


def test_falsified_and_needs_more_data_paths() -> None:
    plan = create_paper_forward_observation_plan(
        candidate_id="candidate-fail",
        plan_id="plan-fail",
        observation_start="2026-06-01",
        observation_end="2026-06-05",
        created_at=NOW,
    )
    needs_more = record_paper_forward_observation_result(plan, [{"return": 0.01}, {"return": -0.02}], created_at=NOW)
    assert needs_more["status"] == "NEEDS_MORE_DATA"
    falsified = record_paper_forward_observation_result(
        plan,
        [{"return": value} for value in [-0.01, -0.02, -0.01, -0.004, 0.001]],
        created_at=NOW,
    )
    assert falsified["status"] == "FALSIFIED"
    assert falsified["hypothesis_falsified"] is True


def test_large_sample_metrics() -> None:
    samples = [{"return": 0.002 if index % 2 else -0.001} for index in range(200)]
    metrics = calculate_paper_forward_metrics(samples)
    assert metrics["sample_size"] == 200
    assert metrics["wins"] == 100
    assert metrics["losses"] == 100
