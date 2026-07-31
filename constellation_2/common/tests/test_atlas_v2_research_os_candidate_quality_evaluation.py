from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.candidate_quality_evaluation import compare_failure_category_distributions, compute_candidate_quality_delta, compute_failure_category_counts, compute_failure_category_distribution, evaluate_candidate_quality
from constellation_2.common.atlas_v2_research_os.candidate_quality_metrics import build_metric_set

NOW = "2026-06-04T00:00:00Z"
WINDOW = {"start": "2026-06-01", "end": "2026-06-04"}


def baseline():
    return {"baseline_id": "b1", "created_at": NOW, "created_by": "test", "source_artifact_ids": [], "raw_signals": 100, "generated_candidates": 20, "rejected_candidates": 40, "gate_suppressions": 20, "portfolio_scoring_rejections": 10, "portfolio_scoring_passes": 10, "evidence_levels": ["HISTORICAL_REPLAY", "PAPER_FORWARD_OBSERVATION"], "hypotheses_tested": 10, "hypotheses_not_falsified": 5, "repeated_failures": 10, "failure_categories": ["REGIME_MISMATCH"] * 6 + ["DUPLICATE_SIGNAL"] * 4, "measurement_window": WINDOW, "signal_universe_id": "u1", "candidate_factory_version": "cf1", "metadata": {}}


def treatment(**overrides):
    row = {"treatment_id": "t1", "created_at": NOW, "created_by": "test", "source_artifact_ids": [], "raw_signals": 100, "generated_candidates": 28, "rejected_candidates": 35, "gate_suppressions": 20, "portfolio_scoring_rejections": 8, "portfolio_scoring_passes": 14, "evidence_levels": ["HISTORICAL_REPLAY", "PAPER_FORWARD_OBSERVATION"], "hypotheses_tested": 10, "hypotheses_not_falsified": 7, "repeated_failures": 6, "failure_categories": ["REGIME_MISMATCH"] * 3 + ["DUPLICATE_SIGNAL"] * 3, "measurement_window": WINDOW, "signal_universe_id": "u1", "candidate_factory_version": "cf1", "learning_input_ids": ["learn1"], "research_os_memory_ids": ["mem1"], "metadata": {}}
    row.update(overrides)
    return row


def test_computes_metric_deltas() -> None:
    delta = compute_candidate_quality_delta(build_metric_set(baseline()), build_metric_set(treatment(), repeated_failures_before=10))
    assert delta["candidate_conversion_rate_delta"] == 0.08
    assert delta["repeated_failure_reduction_delta"] == -0.4


def test_detects_improvement() -> None:
    evaluation = evaluate_candidate_quality(baseline(), treatment())
    assert evaluation["improvement_detected"] is True
    assert evaluation["certification_result"]["status"] == "MEASUREMENT_ONLY_PASS"
    assert evaluation["certification_result"]["live_use_authorized"] is False


def test_detects_regression() -> None:
    evaluation = evaluate_candidate_quality(baseline(), treatment(generated_candidates=10, rejected_candidates=60, repeated_failures=12))
    assert evaluation["regression_detected"] is True


def test_refuses_improvement_claim_when_non_comparable() -> None:
    evaluation = evaluate_candidate_quality(baseline(), treatment(candidate_factory_version="cf2"))
    assert evaluation["comparable"] is False
    assert evaluation["improvement_detected"] is False
    assert evaluation["certification_result"]["status"] == "NON_COMPARABLE"


def test_refuses_measurement_pass_from_mock_or_generated_only() -> None:
    evaluation = evaluate_candidate_quality(baseline(), treatment(evidence_levels=["MOCK_ONLY", "GENERATED_ONLY"]))
    assert evaluation["certification_result"]["status"] == "NOT_CERTIFIED"


def test_failure_category_helpers() -> None:
    counts = compute_failure_category_counts(["REGIME_MISMATCH", "BAD_VALUE"])
    assert counts["REGIME_MISMATCH"] == 1
    assert counts["UNKNOWN"] == 1
    distribution = compute_failure_category_distribution(["REGIME_MISMATCH", "REGIME_MISMATCH"])
    assert distribution["distribution"]["REGIME_MISMATCH"] == 1.0
    comparison = compare_failure_category_distributions(["REGIME_MISMATCH", "UNKNOWN"], ["UNKNOWN", "UNKNOWN"])
    assert comparison["delta"]["UNKNOWN"] == 0.5
