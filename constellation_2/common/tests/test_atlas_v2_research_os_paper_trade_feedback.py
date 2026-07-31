from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.candidate_survival_analytics import compute_candidate_survival
from constellation_2.common.atlas_v2_research_os.memory_index import list_memory_objects
from constellation_2.common.atlas_v2_research_os.paper_trade_feedback import (
    derive_feedback_from_outcome,
    update_learning_validation_from_paper_outcome,
    update_memory_from_paper_outcome,
    update_research_effectiveness_from_paper_outcome,
)
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog


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


def test_improving_outcome_supports_memory_and_effectiveness(tmp_path: Path) -> None:
    outcome = _outcome()
    survival = compute_candidate_survival(outcome)
    feedback = derive_feedback_from_outcome(outcome, survival)
    assert {row["feedback_type"] for row in feedback} >= {"SUPPORT_MECHANISM", "INCREASE_RESEARCH_EFFECTIVENESS"}
    memory_update = update_memory_from_paper_outcome(tmp_path, outcome, survival)
    assert memory_update["survival_state"] == "SURVIVED_INITIAL_TEST"
    assert list_memory_objects(tmp_path)[0]["lifecycle_state"] == "SUPPORTED"
    effectiveness = update_research_effectiveness_from_paper_outcome(tmp_path, outcome, survival)
    assert effectiveness["contributions"][0]["useful_learning_score"] > 0


def test_failed_outcome_records_failure_and_backlog(tmp_path: Path) -> None:
    outcome = _outcome(wins=1, losses=9, expectancy=-0.04, profit_factor=0.4, hypothesis_confirmed=False, hypothesis_falsified=True, failure_reasons=["negative expectancy"])
    survival = compute_candidate_survival(outcome)
    feedback = derive_feedback_from_outcome(outcome, survival)
    assert {row["feedback_type"] for row in feedback} >= {"RECORD_FAILURE_PATTERN", "CREATE_FAILURE_ANALYSIS_BACKLOG", "REDUCE_RESEARCH_EFFECTIVENESS"}
    memory_update = update_memory_from_paper_outcome(tmp_path, outcome, survival)
    assert memory_update["failure_pattern"]["failure_type"] == "negative expectancy"
    assert ResearchBacklog(tmp_path).list_backlog_items(item_type="FAILURE_ANALYSIS")


def test_learning_validation_feedback_marks_regression_or_improvement(tmp_path: Path) -> None:
    good = update_learning_validation_from_paper_outcome(tmp_path, _outcome(), compute_candidate_survival(_outcome()))
    bad_outcome = _outcome(wins=1, losses=9, expectancy=-0.04, profit_factor=0.4, hypothesis_confirmed=False, hypothesis_falsified=True)
    bad = update_learning_validation_from_paper_outcome(tmp_path, bad_outcome, compute_candidate_survival(bad_outcome))
    assert good["certification"]["result"] == "IMPROVING"
    assert bad["certification"]["result"] == "REGRESSING"
