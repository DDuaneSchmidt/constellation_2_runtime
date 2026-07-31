from __future__ import annotations

from datetime import UTC, datetime

from constellation_2.common.atlas_v2_research_os.timeframe_neighborhood_expansion import (
    _classify_timeframe,
    _resample_rows,
)


def test_builds_155_156_resamples_1m_rows_into_15m_ohlcv() -> None:
    rows = []
    start = datetime(2023, 1, 3, 14, 30, tzinfo=UTC)
    for minute in range(30):
        rows.append(
            {
                "timestamp": start.replace(minute=30 + minute),
                "open": 100 + minute,
                "high": 101 + minute,
                "low": 99 + minute,
                "close": 100.5 + minute,
                "volume": 10 + minute,
            }
        )

    bars = _resample_rows(rows, 15)

    assert len(bars) == 2
    assert bars[0]["timestamp"] == "2023-01-03T14:30:00Z"
    assert bars[0]["open"] == 100.0
    assert bars[0]["high"] == 115.0
    assert bars[0]["low"] == 99.0
    assert bars[0]["close"] == 114.5
    assert bars[0]["volume"] == sum(10 + minute for minute in range(15))
    assert bars[1]["timestamp"] == "2023-01-03T14:45:00Z"


def test_builds_155_156_timeframe_classification_robust_and_failed() -> None:
    exact_rows = [
        {"sample_size": 100, "classification": "EXACT_CONFIRMED_STRONG"},
        {"sample_size": 100, "classification": "EXACT_CONFIRMED_STRONG"},
        {"sample_size": 100, "classification": "EXACT_CONFIRMED_STRONG"},
    ]
    cost_rows = [
        {"classification": "NET_SURVIVES_STRONG"},
        {"classification": "NET_SURVIVES_STRONG"},
        {"classification": "NET_SURVIVES_STRONG"},
    ]
    assert _classify_timeframe(exact_rows, cost_rows, 16.0) == "TIMEFRAME_ROBUST"

    failed_exact = [
        {"sample_size": 100, "classification": "EXACT_FAILED"},
        {"sample_size": 100, "classification": "EXACT_FAILED"},
        {"sample_size": 100, "classification": "EXACT_FAILED"},
    ]
    failed_cost = [
        {"classification": "NET_FAILED"},
        {"classification": "COST_ERODED"},
        {"classification": "NET_FAILED"},
    ]
    assert _classify_timeframe(failed_exact, failed_cost, 0.0) == "TIMEFRAME_FAILED"
