from __future__ import annotations

from research_lab.providers.yfinance_daily import REQUIRED_RAW_COLUMNS, normalize_yfinance_records


def test_yfinance_adapter_normalizes_raw_provider_output() -> None:
    rows = normalize_yfinance_records(
        symbol="spy",
        rows=[
            {
                "Date": "2026-05-15",
                "Open": 100,
                "High": 105,
                "Low": 99,
                "Close": 104,
                "Adj Close": 103.5,
                "Volume": 123456,
            }
        ],
        provider_version="test-version",
        fetched_at="2026-05-18T00:00:00Z",
    )

    assert list(rows[0].keys()) == REQUIRED_RAW_COLUMNS
    assert rows[0]["symbol"] == "SPY"
    assert rows[0]["provider"] == "yfinance"
    assert rows[0]["provider_version"] == "test-version"
