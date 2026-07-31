from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.outcome_performance_metrics_v1 import build_outcome_performance_metrics_v1, write_outcome_performance_metrics_v1
from ops.aegis.research_quality_control_v1 import (
    build_hypothesis_decision_policy_v1,
    build_research_allocation_recommendation_v1,
    build_research_quality_engine_v1,
    write_hypothesis_decision_policy_v1,
    write_research_quality_engine_v1,
)

DAY = "2026-06-02"


def _report(root: Path, family: str, filename: str) -> Path:
    return root / "reports" / family / DAY / filename


def _seed(root: Path) -> None:
    portfolio = [
        {"hypothesis_id": "H_BAD", "name": "Bad Early", "formal_claim": "Track bad early outcomes.", "linked_candidates": [f"bad-c{i}" for i in range(6)], "linked_paper_positions": [f"bad-p{i}" for i in range(6)], "linked_outcomes": [], "sample_count": 0},
        {"hypothesis_id": "H_POS", "name": "Positive Early", "formal_claim": "Track positive early outcomes.", "linked_candidates": [f"pos-c{i}" for i in range(6)], "linked_paper_positions": [f"pos-p{i}" for i in range(6)], "linked_outcomes": [], "sample_count": 0},
        {"hypothesis_id": "H_SUFF_BAD", "name": "Sufficient Bad", "formal_claim": "Sufficient poor outcomes should redesign.", "linked_candidates": [f"sbad-c{i}" for i in range(30)], "linked_paper_positions": [f"sbad-p{i}" for i in range(30)], "linked_outcomes": [], "sample_count": 0},
        {"hypothesis_id": "H_FLOW", "name": "Candidate Heavy", "formal_claim": "Candidates alone are not evidence.", "linked_candidates": [f"flow-c{i}" for i in range(50)], "linked_paper_positions": ["flow-p0"], "linked_outcomes": [], "sample_count": 0},
    ]
    write_json_v1(_report(root, "aegis_research_portfolio_v1", "research_portfolio.v1.json"), {"generated_at": "2026-06-02T00:00:00Z", "hypotheses": portfolio})
    write_json_v1(_report(root, "aegis_hypothesis_registry_v1", "hypothesis_registry.v1.json"), {"generated_at": "2026-06-02T00:00:00Z", "hypotheses": portfolio})
    write_json_v1(_report(root, "aegis_hypothesis_workflow_state_v1", "hypothesis_workflow_state.v1.json"), {"computed_at_utc": "2026-06-02T00:00:00Z", "hypotheses": []})
    outcomes = []
    samples = []
    for prefix, hid, returns in [
        ("bad", "H_BAD", [-0.06, -0.05, -0.04, -0.03, -0.02, 0.01]),
        ("pos", "H_POS", [0.01, 0.02, 0.03, 0.04, 0.05, 0.06]),
        ("sbad", "H_SUFF_BAD", [-0.01] * 30),
    ]:
        for i, value in enumerate(returns):
            reason = "TAKE_PROFIT_THRESHOLD_REACHED" if value > 0 else "STOP_LOSS_THRESHOLD_REACHED"
            outcomes.append({"outcome_id": f"{prefix}-o{i}", "hypothesis_id": hid, "sleeve_id": f"{prefix}-sleeve", "outcome_state": "CLOSED_WIN" if value > 0 else "CLOSED_LOSS", "realized_return": value, "holding_period_days": 2 + i % 3, "exit_trigger": reason})
            samples.append({"sample_id": f"{prefix}-sample{i}", "outcome_id": f"{prefix}-o{i}", "hypothesis_id": hid, "sleeve_id": f"{prefix}-sleeve", "inclusion_status": "INCLUDED", "sample_state": "INCLUDED", "return_value": value})
    write_json_v1(_report(root, "aegis_outcome_registry_v1", "outcome_registry.v1.json"), {"generated_at": "2026-06-02T00:00:00Z", "outcomes": outcomes})
    write_json_v1(_report(root, "aegis_validation_samples_v1", "validation_samples.v1.json"), {"generated_at": "2026-06-02T00:00:00Z", "samples": samples})
    write_json_v1(_report(root, "aegis_statistical_sufficiency_v1", "statistical_sufficiency.v1.json"), {"generated_at": "2026-06-02T00:00:00Z", "hypotheses": [
        {"hypothesis_id": "H_BAD", "sufficiency_state": "UNDERPOWERED", "usable_sample_count": 6, "state_reason_codes": ["INSUFFICIENT_CLOSED_SAMPLES"]},
        {"hypothesis_id": "H_POS", "sufficiency_state": "UNDERPOWERED", "usable_sample_count": 6, "state_reason_codes": ["INSUFFICIENT_CLOSED_SAMPLES"]},
        {"hypothesis_id": "H_SUFF_BAD", "sufficiency_state": "VALIDATED", "usable_sample_count": 30, "sample_independence_status": "PASS", "regime_coverage_status": "PASS", "state_reason_codes": ["VALIDATED_THRESHOLD_MET"]},
        {"hypothesis_id": "H_FLOW", "sufficiency_state": "UNDERPOWERED", "usable_sample_count": 0, "state_reason_codes": ["NO_INCLUDED_VALIDATION_SAMPLES"]},
    ]})
    write_json_v1(_report(root, "aegis_candidate_state_v1", "candidate_state.v1.json"), {"generated_at": "2026-06-02T00:00:00Z"})
    write_json_v1(_report(root, "aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json"), {"generated_at": "2026-06-02T00:00:00Z"})
    write_json_v1(_report(root, "aegis_research_allocation_v1", "research_allocation.v1.json"), {"generated_at": "2026-06-02T00:00:00Z", "recommendations": [{"hypothesis_id": "H_BAD", "allocation_score": 20}, {"hypothesis_id": "H_POS", "allocation_score": 20}, {"hypothesis_id": "H_SUFF_BAD", "allocation_score": 20}, {"hypothesis_id": "H_FLOW", "allocation_score": 20}]})


def test_closed_outcomes_produce_performance_metrics(tmp_path: Path) -> None:
    _seed(tmp_path)
    metrics = build_outcome_performance_metrics_v1(truth_root=tmp_path, day_utc=DAY)
    by_h = {row["hypothesis_id"]: row for row in metrics["hypotheses"]}
    bad = by_h["H_BAD"]
    assert bad["closed_outcome_count"] == 6
    assert bad["included_validation_sample_count"] == 6
    assert bad["win_count"] == 1
    assert bad["loss_count"] == 5
    assert bad["average_realized_return"] == -0.03166667
    assert bad["performance_confidence"] == "LOW"
    assert bad["underpowered"] is True
    assert bad["source_outcome_ids"]
    assert bad["source_validation_sample_ids"]


def test_quality_decision_and_allocation_consume_outcome_metrics_safely(tmp_path: Path) -> None:
    _seed(tmp_path)
    metrics = build_outcome_performance_metrics_v1(truth_root=tmp_path, day_utc=DAY)
    write_outcome_performance_metrics_v1(truth_root=tmp_path, day_utc=DAY, payload=metrics)
    allocation_before = _report(tmp_path, "aegis_research_allocation_v1", "research_allocation.v1.json").read_text(encoding="utf-8")

    quality = build_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY)
    write_research_quality_engine_v1(truth_root=tmp_path, day_utc=DAY, payload=quality)
    decisions = build_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, quality=quality)
    write_hypothesis_decision_policy_v1(truth_root=tmp_path, day_utc=DAY, payload=decisions)
    allocation = build_research_allocation_recommendation_v1(truth_root=tmp_path, day_utc=DAY, decisions=decisions)

    q_by_h = {row["hypothesis_id"]: row for row in quality["hypotheses"]}
    d_by_h = {row["hypothesis_id"]: row for row in decisions["decisions"]}
    a_by_h = {row["hypothesis_id"]: row for row in allocation["recommendations"]}

    assert q_by_h["H_BAD"]["quality_status"] == "UNDERPOWERED"
    assert q_by_h["H_BAD"]["grades"]["validation_evidence"]["confidence_level"] == "LOW"
    assert "EARLY_OUTCOME_PERFORMANCE_POOR" in q_by_h["H_BAD"]["reason_codes"]
    assert d_by_h["H_BAD"]["recommendation"] == "DECREASE_ATTENTION"
    assert d_by_h["H_BAD"]["recommendation"] != "RETIRE_RECOMMENDED"
    assert a_by_h["H_BAD"]["recommended_allocation_action"] == "DECREASE"

    assert d_by_h["H_POS"]["recommendation"] == "CONTINUE"
    assert d_by_h["H_POS"]["recommendation"] != "READY_FOR_CAPITAL_REVIEW"
    assert d_by_h["H_SUFF_BAD"]["recommendation"] == "REDESIGN"
    assert d_by_h["H_FLOW"]["recommendation"] != "INCREASE_ATTENTION"
    assert a_by_h["H_FLOW"]["recommended_allocation_action"] != "INCREASE"
    assert all(row["recommendation"] != "READY_FOR_CAPITAL_REVIEW" for row in decisions["decisions"] if row["hypothesis_id"] != "H_SUFF_BAD")
    assert _report(tmp_path, "aegis_research_allocation_v1", "research_allocation.v1.json").read_text(encoding="utf-8") == allocation_before
    assert allocation["allocation_mutation_performed"] is False
    assert allocation["broker_execution_allowed"] is False
    assert allocation["trade_advice_allowed"] is False
    assert allocation["safety"]["no_real_capital"] is True
