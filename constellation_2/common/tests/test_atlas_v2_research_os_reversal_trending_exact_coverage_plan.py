from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.reversal_trending_exact_coverage_plan import (
    COVERAGE_COLUMNS,
    FALLBACK_COLUMNS,
    REPRESENTATIVE_CANDIDATE_ID,
    SHOPPING_COLUMNS,
    build_reversal_trending_exact_coverage_plan,
    write_reversal_trending_exact_coverage_plan,
)


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2023-01-01T09:30:00,10,11,9,10.5,1000\n"
        "2023-01-02T09:30:00,10.5,11,10,10.8,1100\n",
        encoding="utf-8",
    )


def _seed(root: Path) -> None:
    _write_latest(
        root,
        "confirmed_reversal_family_expansion",
        {
            "target_family": {
                "family_id": "family_55443d63b32328bd",
                "family_name": "REVERSAL / TRENDING / 1H / SCREEN_REPLAY",
                "representative_candidates": [REPRESENTATIVE_CANDIDATE_ID],
                "mechanism": "REVERSAL",
                "regime": "TRENDING",
                "timeframe": ["1h"],
                "source_type": "SCREEN_REPLAY",
                "symbols_universe": ["BAC", "MISSING"],
                "direct_result": {
                    "classification": "BACKTEST_SUPPORTED",
                    "sample_size": 62,
                    "expectancy": 0.004254,
                    "profit_factor": 1.547891,
                    "max_drawdown": -0.149031,
                    "symbol": "BAC",
                },
            },
            "related_variants": [
                {
                    "family_id": "family_59cc928bca30cc44",
                    "family_name": "REVERSAL / TRENDING / 30M / UNKNOWN",
                    "representative_candidates": ["variant_30m"],
                    "mechanism": "REVERSAL",
                    "regime": "TRENDING",
                    "timeframe": ["30m"],
                    "source_type": "UNKNOWN",
                    "symbols_universe": ["SPY"],
                    "direct_result": {
                        "classification": "BACKTEST_SUPPORTED",
                        "sample_size": 10,
                        "expectancy": 0.001,
                        "profit_factor": 1.2,
                        "max_drawdown": -0.01,
                        "symbol": "SPY",
                    },
                }
            ],
        },
    )
    _write_latest(root, "candidate_family_discovery", {"families": []})
    _write_latest(root, "direct_candidate_data_validation", {"candidate_validations": []})
    _csv(root / "data" / "cache" / "BAC_tiingo_adjusted_daily.csv")
    _csv(root / "data" / "cache" / "SPY_30m.csv")


def test_exact_coverage_plan_flags_daily_fallback_not_exact_and_p0_missing(tmp_path: Path, monkeypatch) -> None:
    _seed(tmp_path)
    monkeypatch.chdir(tmp_path)

    report = build_reversal_trending_exact_coverage_plan(tmp_path, created_at="2026-06-06T00:00:00Z")
    rows = report["coverage_matrix"]

    assert set(report["target_family_ids"]) >= {"family_55443d63b32328bd", "family_59cc928bca30cc44"}
    assert any(row["candidate_id"] == REPRESENTATIVE_CANDIDATE_ID for row in rows)
    bac = next(row for row in rows if row["candidate_id"] == REPRESENTATIVE_CANDIDATE_ID and row["symbol"] == "BAC")
    assert bac["coverage_status"] == "FALLBACK_ONLY"
    assert bac["fallback_used"] == "true"
    assert bac["available_exact_file"] == ""
    missing = next(row for row in rows if row["symbol"] == "MISSING")
    assert missing["coverage_status"] == "MISSING_SYMBOL"
    assert any(row["priority"] == "P0" and row["candidate_id"] == REPRESENTATIVE_CANDIDATE_ID for row in report["missing_data_shopping_list"])
    assert report["confidence_impact"] == "NONE"
    assert report["authority_boundary"]["research_only"] is True
    assert not any(key.endswith("_authorized") for key in report["authority_boundary"])


def test_exact_coverage_plan_writes_required_files_and_columns(tmp_path: Path, monkeypatch) -> None:
    _seed(tmp_path)
    monkeypatch.chdir(tmp_path)
    report = build_reversal_trending_exact_coverage_plan(tmp_path, created_at="2026-06-06T00:00:00Z")
    paths = write_reversal_trending_exact_coverage_plan(report, root=tmp_path)

    for key in ["latest_json", "latest_summary", "coverage_matrix", "missing_data_shopping_list", "fallback_usage"]:
        assert paths[key].exists()

    with paths["coverage_matrix"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == COVERAGE_COLUMNS
    with paths["missing_data_shopping_list"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == SHOPPING_COLUMNS
    with paths["fallback_usage"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == FALLBACK_COLUMNS

    summary = paths["latest_summary"].read_text(encoding="utf-8")
    assert "# Build 090 — Reversal Trending Exact-Coverage Plan" in summary
    assert "## Authority Boundary" in summary
