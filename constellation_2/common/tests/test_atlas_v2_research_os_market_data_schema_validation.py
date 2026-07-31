from __future__ import annotations

from pathlib import Path

from constellation_2.common.atlas_v2_research_os.market_data_schema_validation import (
    infer_symbol_and_timeframe_from_filename,
    normalize_market_data_csv,
    validate_market_data_schema,
)


def test_infer_symbol_and_timeframe_from_supported_filenames() -> None:
    assert infer_symbol_and_timeframe_from_filename("data/cache/QQQ_30m.csv") == ("QQQ", "30m")
    assert infer_symbol_and_timeframe_from_filename("data/cache/AAPL_1h.csv") == ("AAPL", "1h")
    assert infer_symbol_and_timeframe_from_filename("data/cache/TLT_daily.csv") == ("TLT", "daily")
    assert infer_symbol_and_timeframe_from_filename("data/cache/SPY_tiingo_adjusted_daily.csv") == ("SPY", "daily")
    assert infer_symbol_and_timeframe_from_filename("data/cache/MSFT.csv") == ("MSFT", "daily")


def test_normalize_market_data_csv_supports_common_columns(tmp_path: Path) -> None:
    path = tmp_path / "data" / "cache" / "QQQ_30m.csv"
    path.parent.mkdir(parents=True)
    path.write_text("datetime,Open,High,Low,Close,Volume,Adj Close\n2026-01-01T09:30:00,1,2,0.5,1.5,100,1.4\n", encoding="utf-8")

    rows = normalize_market_data_csv(path)

    assert rows == [
        {
            "timestamp": "2026-01-01T09:30:00",
            "symbol": "QQQ",
            "timeframe": "30m",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100.0,
            "adjusted_close": 1.4,
            "source_file": str(path),
        }
    ]


def test_validate_market_data_schema_fails_closed_on_missing_columns(tmp_path: Path) -> None:
    path = tmp_path / "data" / "cache" / "QQQ_30m.csv"
    path.parent.mkdir(parents=True)
    path.write_text("timestamp,open,close\n2026-01-01T09:30:00,1,1.5\n", encoding="utf-8")

    result = validate_market_data_schema(path)

    assert result.status == "SCHEMA_INVALID"
    assert "Missing required columns" in result.errors[0]
