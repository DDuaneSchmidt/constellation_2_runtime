from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.candidate_survival_analytics import (
    candidate_failure_record,
    candidate_success_record,
    compute_candidate_survival,
    survival_counts,
)


def _outcome(**overrides: object) -> dict:
    row = {
        "outcome_id": "outcome-1",
        "candidate_id": "candidate-1",
        "test_plan_id": "plan-1",
        "created_at": "2026-06-05T00:00:00Z",
        "observation_start": "2026-06-01",
        "observation_end": "2026-06-05",
        "sample_size": 10,
        "wins": 7,
        "losses": 3,
        "average_return": 0.03,
        "expectancy": 0.02,
        "max_drawdown": -0.05,
        "profit_factor": 1.8,
        "regime_context": "HIGH_VOLATILITY",
        "hypothesis_confirmed": True,
        "hypothesis_weakened": False,
        "hypothesis_falsified": False,
        "failure_reasons": [],
        "success_reasons": ["positive expectancy"],
        "source_artifact_ids": ["artifact-1"],
        "metadata": {"mechanism": "EVENT_REACTION"},
    }
    row.update(overrides)
    return row


def test_survived_initial_test_and_success_record() -> None:
    survival = compute_candidate_survival(_outcome())
    assert survival["survival_state"] == "SURVIVED_INITIAL_TEST"
    assert survival["win_rate"] == 0.7
    assert candidate_success_record(_outcome(), survival)["success_id"].startswith("candidate-success-")


def test_falsified_and_failure_record() -> None:
    outcome = _outcome(wins=2, losses=8, expectancy=-0.03, profit_factor=0.6, hypothesis_confirmed=False, hypothesis_falsified=True, failure_reasons=["negative expectancy"])
    survival = compute_candidate_survival(outcome)
    assert survival["survival_state"] == "FALSIFIED"
    assert candidate_failure_record(outcome, survival)["failure_reasons"] == ["negative expectancy"]


def test_needs_more_data_is_not_over_interpreted() -> None:
    survival = compute_candidate_survival(_outcome(sample_size=2, wins=2, losses=0))
    assert survival["survival_state"] == "NEEDS_MORE_DATA"
    assert survival_counts([survival])["NEEDS_MORE_DATA"] == 1
