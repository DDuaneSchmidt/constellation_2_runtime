from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.candidate_quality_baseline import create_candidate_quality_baseline
from constellation_2.common.atlas_v2_research_os.candidate_quality_treatment import CandidateQualityTreatmentError, create_candidate_quality_treatment, load_candidate_quality_treatment, validate_treatment

NOW = "2026-06-04T00:00:00Z"
WINDOW = {"start": "2026-06-01", "end": "2026-06-04"}


def _baseline(root: Path):
    return create_candidate_quality_baseline(root=root, baseline_id="b1", created_at=NOW, created_by="test", raw_signals=100, generated_candidates=20, rejected_candidates=40, gate_suppressions=20, portfolio_scoring_rejections=10, portfolio_scoring_passes=10, evidence_levels=["HISTORICAL_REPLAY"], hypotheses_tested=10, hypotheses_not_falsified=5, repeated_failures=10, failure_categories=["REGIME_MISMATCH"], measurement_window=WINDOW, signal_universe_id="u1", candidate_factory_version="cf1")


def test_creates_treatment_and_records_learning_inputs(tmp_path: Path) -> None:
    root = tmp_path / "quality"
    baseline = _baseline(root)
    row = create_candidate_quality_treatment(root=root, treatment_id="t1", created_at=NOW, created_by="test", raw_signals=100, generated_candidates=25, rejected_candidates=35, gate_suppressions=20, portfolio_scoring_rejections=8, portfolio_scoring_passes=12, evidence_levels=["HISTORICAL_REPLAY"], hypotheses_tested=10, hypotheses_not_falsified=7, repeated_failures=6, failure_categories=["REGIME_MISMATCH"], measurement_window=WINDOW, signal_universe_id="u1", candidate_factory_version="cf1", learning_input_ids=["learn1"], research_os_memory_ids=["mem1"], baseline=baseline)
    assert row["learning_input_ids"] == ["learn1"]
    assert row["comparable_to_baseline"] is True
    assert validate_treatment(load_candidate_quality_treatment("t1", root)) is True


def test_marks_non_comparable_when_factory_version_differs(tmp_path: Path) -> None:
    root = tmp_path / "quality"
    baseline = _baseline(root)
    row = create_candidate_quality_treatment(root=root, treatment_id="t2", created_at=NOW, created_by="test", raw_signals=100, generated_candidates=25, rejected_candidates=35, gate_suppressions=20, portfolio_scoring_rejections=8, portfolio_scoring_passes=12, evidence_levels=["HISTORICAL_REPLAY"], hypotheses_tested=10, hypotheses_not_falsified=7, repeated_failures=6, failure_categories=["REGIME_MISMATCH"], measurement_window=WINDOW, signal_universe_id="u1", candidate_factory_version="cf2", learning_input_ids=["learn1"], research_os_memory_ids=["mem1"], baseline=baseline)
    assert row["comparable_to_baseline"] is False
    assert "candidate_factory_version differs" in row["non_comparable_reasons"]


def test_treatment_does_not_modify_candidate_factory() -> None:
    with pytest.raises(CandidateQualityTreatmentError, match="must not modify"):
        validate_treatment({"treatment_id": "t", "created_at": NOW, "created_by": "test", "learning_input_ids": [], "research_os_memory_ids": [], "raw_signals": 1, "generated_candidates": 1, "rejected_candidates": 0, "gate_suppressions": 0, "portfolio_scoring_rejections": 0, "portfolio_scoring_passes": 1, "evidence_levels": [], "hypotheses_tested": 0, "hypotheses_not_falsified": 0, "repeated_failures": 0, "failure_categories": [], "measurement_window": WINDOW, "signal_universe_id": "u", "candidate_factory_version": "cf", "metadata": {"candidate_factory_modified": True}})
