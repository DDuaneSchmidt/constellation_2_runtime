from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.edge_qualification import compute_edge_score
from constellation_2.common.atlas_v2_research_os.edge_qualification_models import EdgeQualificationInput
from constellation_2.common.atlas_v2_research_os.historical_replay_engine import create_historical_replay_request, run_historical_replay
from constellation_2.common.atlas_v2_research_os.historical_replay_results import edge_input_with_historical_replay, route_historical_replay_backlog_items

NOW = "2026-06-05T00:00:00Z"


def base_input() -> EdgeQualificationInput:
    return EdgeQualificationInput(source_artifact_ids=["art"], source_hypothesis_ids=["hyp"], evidence_maturity=0.5, research_effectiveness=0.5, hypothesis_survival=0.5, failure_history=0.5, duplicate_risk=0.1, regime_coverage=0.5, candidate_quality_trend=0.5, learning_validation_trend=0.5, lineage_complete=True, governance_pass=True, evidence_level="HISTORICAL_REPLAY")


def replay(samples, replay_id):
    req = create_historical_replay_request(hypothesis_id="hyp", mechanism_tags=["OPENING_RANGE"], source_artifact_ids=["art"], replay_id=replay_id, created_at=NOW)
    return run_historical_replay(req, samples, created_at=NOW)


def test_positive_replay_increases_score_and_creates_review(tmp_path) -> None:
    before = compute_edge_score(base_input())["edge_score"]
    result = replay([{"return": v} for v in [0.02, 0.01, 0.03, -0.001, 0.02, 0.01, 0.015, -0.002]], "replay-positive")
    enriched = edge_input_with_historical_replay(base_input(), result)
    after = compute_edge_score(enriched)
    assert after["edge_score"] > before
    assert any("historical replay contributed" in line for line in after["explanation"])
    items = route_historical_replay_backlog_items(result, root=tmp_path, created_at=NOW)
    assert items[0]["item_type"] == "EDGE_QUALIFICATION_REVIEW"


def test_negative_replay_lowers_score_and_creates_failure_analysis(tmp_path) -> None:
    before = compute_edge_score(base_input())["edge_score"]
    result = replay([{"return": v} for v in [-0.02, -0.01, 0.001, -0.03, -0.02, -0.01]], "replay-negative")
    enriched = edge_input_with_historical_replay(base_input(), result)
    after = compute_edge_score(enriched)["edge_score"]
    assert after < before
    items = route_historical_replay_backlog_items(result, root=tmp_path, created_at=NOW)
    assert items[0]["item_type"] == "FAILURE_ANALYSIS"


def test_insufficient_sample_handled_safely(tmp_path) -> None:
    result = replay([{"return": 0.1}], "replay-small")
    enriched = edge_input_with_historical_replay(base_input(), result)
    assert enriched.historical_sample_size == 1
    assert result["certification"]["status"] == "INSUFFICIENT_SAMPLE"
    assert route_historical_replay_backlog_items(result, root=tmp_path, created_at=NOW) == []
