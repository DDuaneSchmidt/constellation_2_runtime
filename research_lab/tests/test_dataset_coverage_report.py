from __future__ import annotations

from research_lab.datasets.coverage_report import build_coverage_report


def test_coverage_report_includes_per_symbol_first_last_dates() -> None:
    report = build_coverage_report(
        dataset_snapshot_id="ds_fixture",
        universe_snapshot_id="us_fixture",
        symbols_requested=["SPY", "QQQ"],
        symbols_loaded=["SPY"],
        symbols_missing=["QQQ"],
        canonical_rows=[
            {"symbol": "SPY", "date": "2024-01-02"},
            {"symbol": "SPY", "date": "2024-01-03"},
        ],
        start_date="2024-01-02",
        end_date="2024-01-05",
        created_at="2024-01-01T00:00:00Z",
    )

    spy = [row for row in report["coverage_by_symbol"] if row["symbol"] == "SPY"][0]
    qqq = [row for row in report["coverage_by_symbol"] if row["symbol"] == "QQQ"][0]
    assert spy["first_date"] == "2024-01-02"
    assert spy["last_date"] == "2024-01-03"
    assert qqq["quality_status"] == "missing"
    assert report["content_hash"]

