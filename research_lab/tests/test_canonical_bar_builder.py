from __future__ import annotations

from research_lab.bars.canonical_daily_bars import CANONICAL_DAILY_OHLCV_COLUMNS, canonicalize_daily_ohlcv_rows


def test_canonical_bar_builder_enforces_schema() -> None:
    rows = canonicalize_daily_ohlcv_rows(
        [
            {
                "date": "2026-05-16",
                "symbol": "spy",
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100.5",
                "adj_close": "100.4",
                "volume": "1000",
                "provider": "fixture",
                "provider_version": "v1",
            }
        ],
        bar_policy_version="bp_daily_ohlcv_v1",
        dataset_snapshot_id="ds_test",
    )

    assert list(rows[0].keys()) == CANONICAL_DAILY_OHLCV_COLUMNS
    assert rows[0]["symbol"] == "SPY"
    assert isinstance(rows[0]["open"], float)
    assert isinstance(rows[0]["volume"], int)


def test_canonical_rows_sort_by_symbol_and_date() -> None:
    rows = canonicalize_daily_ohlcv_rows(
        [
            {"date": "2026-05-17", "symbol": "QQQ", "open": 1, "high": 2, "low": 1, "close": 2, "adj_close": 2, "volume": 1, "provider": "fixture", "provider_version": "v1"},
            {"date": "2026-05-16", "symbol": "SPY", "open": 1, "high": 2, "low": 1, "close": 2, "adj_close": 2, "volume": 1, "provider": "fixture", "provider_version": "v1"},
            {"date": "2026-05-15", "symbol": "QQQ", "open": 1, "high": 2, "low": 1, "close": 2, "adj_close": 2, "volume": 1, "provider": "fixture", "provider_version": "v1"},
        ],
        bar_policy_version="bp_daily_ohlcv_v1",
    )

    assert [(row["symbol"], row["date"]) for row in rows] == [
        ("QQQ", "2026-05-15"),
        ("QQQ", "2026-05-17"),
        ("SPY", "2026-05-16"),
    ]
