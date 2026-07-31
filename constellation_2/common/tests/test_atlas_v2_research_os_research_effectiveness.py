from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.priority_engine import PriorityEngine, score_item
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog
from constellation_2.common.atlas_v2_research_os.research_effectiveness import evaluate_research_effectiveness, score_research_activity

NOW = "2026-06-04T00:00:00Z"


def _activity(activity_id: str = "ra-1", **overrides: object) -> dict:
    row = {
        "activity_id": activity_id,
        "created_at": NOW,
        "mechanism": "OPENING_RANGE",
        "worker": "atlas_v2_claim_worker",
        "backlog_type": "FAILURE_ANALYSIS",
        "experiment_type": "CHEAP_EXPERIMENT",
        "failure_category": "REGIME_MISMATCH",
        "regime": "HIGH_VOL",
        "evidence_maturity": "HISTORICAL_REPLAY",
        "cost_estimate": 2.0,
        "input_uncertainty": 0.9,
        "output_uncertainty": 0.4,
        "failures_before": 10,
        "failures_after": 6,
        "candidate_quality_before": 0.3,
        "candidate_quality_after": 0.5,
        "hypotheses_tested_before": 10,
        "hypotheses_survived_before": 3,
        "hypotheses_tested_after": 10,
        "hypotheses_survived_after": 6,
    }
    row.update(overrides)
    return row


def test_scores_useful_learning_metrics() -> None:
    contribution = score_research_activity(_activity()).to_dict()
    assert contribution["information_gain_score"] == 0.5
    assert contribution["failure_reduction_contribution"] == 0.4
    assert contribution["candidate_quality_contribution"] == 0.2
    assert contribution["hypothesis_survival_contribution"] == 0.3
    assert contribution["evidence_maturity_contribution"] == 0.4
    assert contribution["research_cost_efficiency"] == 0.9
    assert contribution["metadata"]["influence_target"] == "RESEARCH_PRIORITIZATION"


def test_rankings_identify_most_and_least_valuable_paths() -> None:
    report = evaluate_research_effectiveness([
        _activity("valuable", mechanism="OPENING_RANGE"),
        _activity("weak", mechanism="PAIR_TRADE", input_uncertainty=None, output_uncertainty=None, failures_before=None, failures_after=None, candidate_quality_before=None, candidate_quality_after=None, hypotheses_tested_before=None, hypotheses_survived_before=None, hypotheses_tested_after=None, hypotheses_survived_after=None, evidence_maturity="GENERATED_ONLY", cost_estimate=8.0),
    ])
    assert report["certification"]["result"] == "MEASURABLE"
    assert report["rankings"]["Most Valuable Mechanisms"]["entries"][0]["key"] == "OPENING_RANGE"
    assert report["rankings"]["Least Valuable Research Paths"]["entries"][0]["key"].startswith("PAIR_TRADE|")


def test_effectiveness_influences_research_priority_only(tmp_path: Path) -> None:
    report = evaluate_research_effectiveness([_activity()])
    base_item = {"expected_learning_value": 0.1, "item_type": "FAILURE_ANALYSIS", "metadata": {"mechanism_tags": ["OPENING_RANGE"]}}
    assert score_item(base_item, report) > score_item(base_item)

    backlog = ResearchBacklog(tmp_path)
    backlog.create_backlog_item(backlog_item_id="plain", item_type="RESEARCH_QUESTION", title="plain", description="plain", created_at=NOW, created_by="test", state="READY", expected_learning_value=1.0)
    backlog.create_backlog_item(backlog_item_id="effective", item_type="FAILURE_ANALYSIS", title="effective", description="effective", created_at=NOW, created_by="test", state="READY", expected_learning_value=0.1)
    rows = backlog._read()
    for row in rows:
        if row["backlog_item_id"] == "effective":
            row["metadata"] = {"mechanism_tags": ["OPENING_RANGE"]}
    backlog._write(rows)

    selected = PriorityEngine(backlog, research_effectiveness_report=report).select_next_items(limit=1, exploration_rate=0.0)
    assert selected[0]["backlog_item_id"] == "effective"
