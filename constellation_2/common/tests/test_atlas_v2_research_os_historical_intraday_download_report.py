from __future__ import annotations

from pathlib import Path

from constellation_2.common.atlas_v2_research_os.historical_intraday_download_report import (
    render_intraday_download_summary,
    write_intraday_download_report,
)


def test_write_intraday_download_report_writes_latest(tmp_path: Path) -> None:
    report = {
        "day": "2026-06-05",
        "mode": "DRY_RUN_ONLY",
        "dry_run": True,
        "provider": "tiingo",
        "summary": {"planned_count": 1, "downloaded_count": 0, "blocked_count": 0, "failed_count": 0},
        "download_plan": [{"symbol": "DIA", "timeframe": "30m", "target_csv_path": "data/cache/DIA_30m.csv", "status": "PLANNED_NOT_FETCHED"}],
        "download_results": [],
    }

    paths = write_intraday_download_report(report, tmp_path)

    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    assert "DIA 30m" in paths["latest_summary"].read_text(encoding="utf-8")


def test_render_intraday_download_summary_includes_authority_boundary() -> None:
    summary = render_intraday_download_summary(
        {
            "mode": "DRY_RUN_ONLY",
            "dry_run": True,
            "provider": "",
            "summary": {"planned_count": 0, "downloaded_count": 0, "blocked_count": 0, "failed_count": 0},
            "download_plan": [],
            "download_results": [],
        }
    )

    assert "no live/capital/broker/position-sizing" in summary
