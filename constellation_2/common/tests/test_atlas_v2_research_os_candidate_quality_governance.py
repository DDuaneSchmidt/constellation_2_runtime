from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.candidate_quality_evaluation import evaluate_candidate_quality
from constellation_2.common.atlas_v2_research_os.candidate_quality_governance import CandidateQualityGovernanceError, validate_candidate_quality_candidate_isolation, validate_candidate_quality_evidence_boundaries, validate_candidate_quality_measurement_allowed, validate_candidate_quality_no_authority_escalation, validate_candidate_quality_no_forbidden_recommendations

NOW = "2026-06-04T00:00:00Z"
WINDOW = {"start": "2026-06-01", "end": "2026-06-04"}


def baseline():
    return {"baseline_id": "b1", "created_at": NOW, "created_by": "test", "source_artifact_ids": [], "raw_signals": 100, "generated_candidates": 20, "rejected_candidates": 40, "gate_suppressions": 20, "portfolio_scoring_rejections": 10, "portfolio_scoring_passes": 10, "evidence_levels": ["HISTORICAL_REPLAY", "PAPER_FORWARD_OBSERVATION"], "hypotheses_tested": 10, "hypotheses_not_falsified": 5, "repeated_failures": 10, "failure_categories": ["REGIME_MISMATCH"] * 6 + ["DUPLICATE_SIGNAL"] * 4, "measurement_window": WINDOW, "signal_universe_id": "u1", "candidate_factory_version": "cf1", "metadata": {}}


def treatment(**overrides):
    row = {"treatment_id": "t1", "created_at": NOW, "created_by": "test", "source_artifact_ids": [], "raw_signals": 100, "generated_candidates": 28, "rejected_candidates": 35, "gate_suppressions": 20, "portfolio_scoring_rejections": 8, "portfolio_scoring_passes": 14, "evidence_levels": ["HISTORICAL_REPLAY", "PAPER_FORWARD_OBSERVATION"], "hypotheses_tested": 10, "hypotheses_not_falsified": 7, "repeated_failures": 6, "failure_categories": ["REGIME_MISMATCH"] * 3 + ["DUPLICATE_SIGNAL"] * 3, "measurement_window": WINDOW, "signal_universe_id": "u1", "candidate_factory_version": "cf1", "learning_input_ids": ["learn1"], "research_os_memory_ids": ["mem1"], "metadata": {}}
    row.update(overrides)
    return row


def test_rejects_forbidden_artifact_types() -> None:
    with pytest.raises(CandidateQualityGovernanceError, match="forbidden"):
        validate_candidate_quality_measurement_allowed({"artifact_type": "LiveTrade"})


def test_detects_authority_escalation() -> None:
    with pytest.raises(CandidateQualityGovernanceError, match="forbids"):
        validate_candidate_quality_no_authority_escalation({"metadata": {"capital_authorized": True}})


def test_rejects_forbidden_recommendations() -> None:
    with pytest.raises(CandidateQualityGovernanceError, match="measurement-only"):
        validate_candidate_quality_no_forbidden_recommendations({"recommendation": "Promote candidate."})


def test_rejects_operator_approved_evidence() -> None:
    with pytest.raises(CandidateQualityGovernanceError, match="OPERATOR_APPROVED"):
        validate_candidate_quality_evidence_boundaries({"evidence_levels": ["OPERATOR_APPROVED"]})


def test_candidate_isolation() -> None:
    with pytest.raises(CandidateQualityGovernanceError, match="observe but not alter"):
        validate_candidate_quality_candidate_isolation({"metadata": {"candidate_factory_modified": True}})


def test_measurement_pass_does_not_authorize_live_use() -> None:
    result = evaluate_candidate_quality(baseline(), treatment())["certification_result"]
    assert result["status"] == "MEASUREMENT_ONLY_PASS"
    assert result["live_use_authorized"] is False
    assert result["capital_authorized"] is False
    assert result["candidate_promotion_authorized"] is False
