from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.candidate_quality_baseline import CandidateQualityBaselineError, create_candidate_quality_baseline, load_candidate_quality_baseline, validate_baseline

NOW = "2026-06-04T00:00:00Z"
WINDOW = {"start": "2026-06-01", "end": "2026-06-04"}


def _create(root: Path):
    return create_candidate_quality_baseline(root=root, baseline_id="b1", created_at=NOW, created_by="test", raw_signals=100, generated_candidates=20, rejected_candidates=40, gate_suppressions=20, portfolio_scoring_rejections=10, portfolio_scoring_passes=10, evidence_levels=["HISTORICAL_REPLAY"], hypotheses_tested=10, hypotheses_not_falsified=5, repeated_failures=10, failure_categories=["REGIME_MISMATCH"], measurement_window=WINDOW, signal_universe_id="u1", candidate_factory_version="cf1")


def test_creates_immutable_baseline(tmp_path: Path) -> None:
    root = tmp_path / "quality"
    _create(root)
    with pytest.raises(CandidateQualityBaselineError, match="immutable"):
        _create(root)


def test_loads_and_validates_baseline(tmp_path: Path) -> None:
    root = tmp_path / "quality"
    _create(root)
    row = load_candidate_quality_baseline("b1", root)
    assert validate_baseline(row) is True
    assert row["signal_universe_id"] == "u1"
    assert row["candidate_factory_version"] == "cf1"
    assert row["metadata"]["candidate_promotion_authorized"] is False


def test_validates_required_fields() -> None:
    with pytest.raises(CandidateQualityBaselineError, match="missing"):
        validate_baseline({"baseline_id": "b1"})
