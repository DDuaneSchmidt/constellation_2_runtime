from __future__ import annotations

import csv
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.family_stability_analysis import (
    build_family_stability_analysis,
    run_family_stability_analysis,
    write_family_stability_analysis,
)

NOW = "2026-06-06T00:00:00Z"


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name / "latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_inputs(root: Path) -> None:
    _write_latest(
        root,
        "candidate_family_discovery",
        {
            "report_type": "CANDIDATE_FAMILY_DISCOVERY",
            "families": [
                {
                    "family_id": "family_a",
                    "dominant_mechanism": "BREAKOUT",
                    "dominant_regime": "CHOP",
                    "dominant_timeframe": "30M",
                    "dominant_source_type": "JOURNAL_EXTRACT",
                    "candidate_count": 2,
                    "candidate_ids": ["candidate_a1", "candidate_a2"],
                    "average_expectancy": 0.01,
                    "average_profit_factor": 2.0,
                },
                {
                    "family_id": "family_b",
                    "dominant_mechanism": "REVERSAL",
                    "dominant_regime": "TRENDING",
                    "dominant_timeframe": "1H",
                    "dominant_source_type": "SCREEN_REPLAY",
                    "candidate_count": 1,
                    "candidate_ids": ["candidate_b1"],
                    "average_expectancy": -0.01,
                    "average_profit_factor": 0.8,
                },
                {
                    "family_id": "family_c",
                    "dominant_mechanism": "MEAN_REVERSION",
                    "dominant_regime": "LOW_VOLATILITY",
                    "dominant_timeframe": "5M",
                    "dominant_source_type": "MANUAL_REVIEW",
                    "candidate_count": 1,
                    "candidate_ids": ["candidate_c1"],
                },
            ],
        },
    )
    _write_latest(
        root,
        "direct_candidate_data_validation",
        {
            "report_type": "DIRECT_CANDIDATE_DATA_VALIDATION",
            "candidate_validations": [
                {"candidate_id": "candidate_a1", "classification": "CONFIRMED"},
                {"candidate_id": "candidate_a2", "classification": "CONFIRMED"},
                {"candidate_id": "candidate_b1", "classification": "BACKTEST_WEAK"},
            ],
        },
    )
    _write_latest(
        root,
        "holdout_replay_validation",
        {
            "report_type": "HOLDOUT_REPLAY_VALIDATION",
            "summary": {"families_data_blocked": ["family_c"], "families_failed": [], "families_survived_holdout": []},
        },
    )
    _write_latest(root, "backtest_aware_final_qualification", {"report_type": "BACKTEST_AWARE_FINAL_QUALIFICATION"})


def test_all_families_included_and_confidence_unchanged(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = build_family_stability_analysis(root=tmp_path, created_at=NOW)

    assert report["summary"]["families_analyzed"] == 3
    assert {row["family_id"] for row in report["family_rows"]} == {"family_a", "family_b", "family_c"}
    assert report["confidence_impact"] == "NONE"
    assert report["authority_boundary"]["confidence_change_authorized"] is False


def test_matrices_written(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_family_stability_analysis(root=tmp_path, created_at=NOW)

    paths = write_family_stability_analysis(report, root=tmp_path)

    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    assert paths["survival_matrix"].name == "family_survival_matrix.csv"
    assert paths["failure_matrix"].name == "family_failure_matrix.csv"
    assert paths["rankings"].name == "family_stability_rankings.csv"
    assert len(list(csv.DictReader(paths["rankings"].open(encoding="utf-8")))) == 3


def test_rankings_are_deterministic(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    first = build_family_stability_analysis(root=tmp_path, created_at=NOW)["family_stability_rankings"]
    second = build_family_stability_analysis(root=tmp_path, created_at=NOW)["family_stability_rankings"]

    assert first == second
    assert [row["family_id"] for row in first] == ["family_a", "family_b", "family_c"]


def test_no_promotion_authority(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = build_family_stability_analysis(root=tmp_path, created_at=NOW)
    boundary = report["authority_boundary"]

    assert boundary["hypothesis_generation_authorized"] is False
    assert boundary["candidate_creation_authorized"] is False
    assert boundary["ranking_mutation_authorized"] is False
    assert boundary["candidate_production_promotion_authorized"] is False


def test_cli_writes_family_stability_analysis(tmp_path: Path, capsys) -> None:
    _seed_inputs(tmp_path)

    result = main(["--root", str(tmp_path), "--family-stability-analysis"])

    assert result == 0
    assert (tmp_path / "family_stability_analysis" / "latest.json").exists()
    output = json.loads(capsys.readouterr().out)
    assert output["summary"]["families_analyzed"] == 3


def test_run_family_stability_analysis_returns_report(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = run_family_stability_analysis(root=tmp_path, created_at=NOW)

    assert report["report_type"] == "FAMILY_STABILITY_ANALYSIS"
    assert (tmp_path / "family_stability_analysis" / "latest_summary.md").exists()
