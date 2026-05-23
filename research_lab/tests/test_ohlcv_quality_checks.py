from __future__ import annotations

from research_lab.datasets.validation import validate_ohlcv_records


def _row(symbol: str = "SPY", day: str = "2026-05-15", **overrides):
    payload = {
        "date": day,
        "symbol": symbol,
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "adj_close": 100.5,
        "volume": 1000,
    }
    payload.update(overrides)
    return payload


def test_quality_report_fails_on_duplicate_symbol_date() -> None:
    report = validate_ohlcv_records([_row(), _row()], symbols_requested=["SPY"], canonical_written=True)

    assert report["quality_status"] == "fail"
    assert "duplicate_symbol_date_rows" in report["fail_reasons"]


def test_quality_report_fails_on_invalid_ohlc() -> None:
    report = validate_ohlcv_records([_row(high=98.0, low=99.0)], symbols_requested=["SPY"], canonical_written=True)

    assert report["quality_status"] == "fail"
    assert "invalid_ohlc_structure" in report["fail_reasons"]


def test_quality_report_warns_on_missing_symbol() -> None:
    report = validate_ohlcv_records([_row()], symbols_requested=["SPY", "QQQ"], canonical_written=True, minimum_rows_per_symbol=1)

    assert report["quality_status"] == "pass_with_warnings"
    assert report["symbols_missing"] == ["QQQ"]
    assert "some_symbols_missing" in report["warning_reasons"]
