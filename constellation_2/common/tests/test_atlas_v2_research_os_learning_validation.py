from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.learning_validation import (
    build_learning_validation_snapshot,
    compute_learning_trend_metrics,
    filter_snapshots_for_horizon,
    validate_learning_over_time,
)


def _snapshot(day: str, failures: int, duplicates: int, regimes: int, evidence: float, survival: float, quality: float) -> dict:
    return {
        "snapshot_id": f"s-{day}",
        "observed_at": f"{day}T00:00:00Z",
        "repeated_failures": failures,
        "duplicate_ideas": duplicates,
        "regime_gaps": regimes,
        "evidence_maturity_score": evidence,
        "hypothesis_survival_rate": survival,
        "candidate_quality_score": quality,
    }


def test_learning_validation_detects_improvement() -> None:
    snapshots = [
        _snapshot("2026-06-01", 10, 8, 6, 0.3, 0.4, 0.45),
        _snapshot("2026-06-05", 5, 4, 3, 0.5, 0.6, 0.7),
    ]
    report = validate_learning_over_time(snapshots, horizon="7_day", as_of="2026-06-05T00:00:00Z")
    assert report["certification"]["result"] == "IMPROVING"
    assert report["questions_answered"]["are_repeated_failures_decreasing"] == "YES"
    assert report["questions_answered"]["are_duplicate_ideas_decreasing"] == "YES"
    assert report["questions_answered"]["are_regime_gaps_decreasing"] == "YES"
    assert report["questions_answered"]["is_evidence_maturity_increasing"] == "YES"
    assert report["questions_answered"]["is_hypothesis_survival_improving"] == "YES"
    assert report["questions_answered"]["is_candidate_quality_improving"] == "YES"


def test_learning_validation_detects_regression_and_plateau() -> None:
    regression = validate_learning_over_time([
        _snapshot("2026-06-01", 4, 2, 1, 0.6, 0.7, 0.8),
        _snapshot("2026-06-05", 8, 3, 2, 0.4, 0.5, 0.6),
    ], horizon="7_day", as_of="2026-06-05T00:00:00Z")
    assert regression["certification"]["result"] == "REGRESSING"
    stable = validate_learning_over_time([
        _snapshot("2026-06-01", 4, 2, 1, 0.6, 0.7, 0.8),
        _snapshot("2026-06-05", 4, 2, 1, 0.6, 0.7, 0.8),
    ], horizon="7_day", as_of="2026-06-05T00:00:00Z")
    assert stable["certification"]["result"] == "STABLE"
    assert stable["plateaus"]


def test_learning_validation_horizons_filter() -> None:
    snapshots = [
        _snapshot("2026-01-01", 10, 10, 10, 0.1, 0.1, 0.1),
        _snapshot("2026-06-01", 5, 5, 5, 0.5, 0.5, 0.5),
        _snapshot("2026-06-05", 4, 4, 4, 0.6, 0.6, 0.6),
    ]
    assert len(filter_snapshots_for_horizon(snapshots, horizon="7_day", as_of="2026-06-05T00:00:00Z")) == 2
    assert len(filter_snapshots_for_horizon(snapshots, horizon="lifetime")) == 3


def test_learning_validation_insufficient_data() -> None:
    report = validate_learning_over_time([_snapshot("2026-06-05", 1, 1, 1, 0.1, 0.1, 0.1)], horizon="30_day")
    assert report["certification"]["result"] == "INSUFFICIENT_DATA"
    assert all(value is None for value in report["trend"]["metrics"].values())


def test_learning_validation_snapshot_from_empty_store(tmp_path: Path) -> None:
    row = build_learning_validation_snapshot(tmp_path / "store", observed_at="2026-06-05T00:00:00Z")
    assert row["repeated_failures"] == 0.0
    assert row["metadata"]["measurement_only"] is True
