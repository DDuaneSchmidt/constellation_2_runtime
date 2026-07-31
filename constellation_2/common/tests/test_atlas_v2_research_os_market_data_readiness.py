from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.market_data_readiness import (
    build_market_data_readiness_report,
    run_market_data_readiness,
    write_required_market_data_manifest,
)


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def test_market_data_readiness_reports_missing_candidate_csvs(tmp_path: Path) -> None:
    _write_latest(
        tmp_path,
        "candidate_symbol_attribution",
        {
            "candidate_symbol_attributions": [
                {
                    "candidate_id": "ptc_1",
                    "campaign_rank": 1,
                    "mechanism": "BREAKOUT",
                    "candidate_symbols": ["ABC", "XYZ"],
                    "candidate_timeframes": ["30m"],
                }
            ]
        },
    )
    _write_latest(tmp_path, "direct_candidate_data_validation", {"candidate_validations": [{"candidate_id": "ptc_1", "classification": "INSUFFICIENT_DATA"}]})

    report = build_market_data_readiness_report(tmp_path, created_at="2026-06-05T00:00:00Z")
    row = report["candidate_readiness"][0]

    assert report["summary"]["candidates_blocked"] == 1
    assert row["candidate_symbols"] == ["ABC", "XYZ"]
    assert row["intraday_data_required"] is True
    assert row["daily_only_validation_possible"] is False
    assert "Missing daily CSVs" in row["remaining_blocker"]
    assert report["authority_boundary"]["broker_execution_authorized"] is False


def test_market_data_readiness_detects_existing_local_daily_csv(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "cache").mkdir(parents=True)
    (tmp_path / "data" / "cache" / "ABC_tiingo_adjusted_daily.csv").write_text("date,open,high,low,close,volume\n", encoding="utf-8")
    _write_latest(
        tmp_path,
        "candidate_symbol_attribution",
        {
            "candidate_symbol_attributions": [
                {
                    "candidate_id": "ptc_1",
                    "campaign_rank": 1,
                    "mechanism": "MEAN_REVERSION",
                    "candidate_symbols": ["ABC"],
                    "candidate_timeframes": ["1d"],
                }
            ]
        },
    )

    report = build_market_data_readiness_report(tmp_path, created_at="2026-06-05T00:00:00Z")

    assert report["summary"]["existing_local_symbols"] == ["ABC"]
    assert report["summary"]["candidates_ready_for_direct_validation"] == 1
    assert report["candidate_readiness"][0]["daily_only_validation_possible"] is True


def test_market_data_readiness_writes_report_and_manifest(tmp_path: Path) -> None:
    _write_latest(
        tmp_path,
        "candidate_symbol_attribution",
        {
            "candidate_symbol_attributions": [
                {
                    "candidate_id": "ptc_1",
                    "campaign_rank": 1,
                    "mechanism": "BREAKOUT",
                    "candidate_symbols": ["ABC"],
                    "candidate_timeframes": ["5m"],
                }
            ]
        },
    )

    report = run_market_data_readiness(tmp_path, created_at="2026-06-05T00:00:00Z")
    manifest = write_required_market_data_manifest(report, tmp_path)

    assert (tmp_path / "market_data_readiness" / "latest.json").exists()
    assert (tmp_path / "market_data_readiness" / "latest_summary.md").exists()
    assert manifest.exists()
    content = manifest.read_text(encoding="utf-8")
    assert "symbol,timeframe,start_date,end_date,required_for_candidates,expected_csv_path,status,notes" in content
    assert "ABC,1d" in content
    assert "ABC,5m" in content
