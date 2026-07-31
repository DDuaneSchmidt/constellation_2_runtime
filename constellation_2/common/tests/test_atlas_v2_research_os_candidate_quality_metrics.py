from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.candidate_quality_metrics import candidate_conversion_rate, evidence_maturity_score, get_evidence_maturity_weight, hypothesis_survival_rate, portfolio_scoring_pass_rate, rejection_rate, repeated_failure_reduction


def test_returns_correct_evidence_weights() -> None:
    assert get_evidence_maturity_weight("GENERATED_ONLY")["weight"] == 0.10
    assert get_evidence_maturity_weight("MOCK_ONLY")["weight"] == 0.15
    assert get_evidence_maturity_weight("HISTORICAL_REPLAY")["weight"] == 0.40
    assert get_evidence_maturity_weight("PAPER_FORWARD_OBSERVATION")["weight"] == 0.70
    assert get_evidence_maturity_weight("EXTERNALLY_VALIDATED")["weight"] == 0.90


def test_excludes_operator_approved_and_unknown() -> None:
    assert get_evidence_maturity_weight("OPERATOR_APPROVED")["status"] == "EXCLUDED_OPERATOR_DISPOSITION"
    assert get_evidence_maturity_weight("BOGUS")["status"] == "UNKNOWN_EVIDENCE_LEVEL"


def test_core_metrics_compute_values() -> None:
    assert candidate_conversion_rate(20, 100)["value"] == 0.2
    assert rejection_rate(30, 100)["value"] == 0.3
    assert portfolio_scoring_pass_rate(5, 20)["value"] == 0.25
    assert evidence_maturity_score(["HISTORICAL_REPLAY", "PAPER_FORWARD_OBSERVATION"])["value"] == 0.55
    assert hypothesis_survival_rate(7, 10)["value"] == 0.7
    assert repeated_failure_reduction(10, 6)["value"] == 0.6


def test_zero_denominators_are_insufficient_data() -> None:
    assert candidate_conversion_rate(0, 0)["status"] == "INSUFFICIENT_DATA"
    assert portfolio_scoring_pass_rate(0, 0)["status"] == "INSUFFICIENT_DATA"
    assert hypothesis_survival_rate(0, 0)["status"] == "INSUFFICIENT_DATA"
    assert repeated_failure_reduction(0, 0)["status"] == "INSUFFICIENT_DATA"
