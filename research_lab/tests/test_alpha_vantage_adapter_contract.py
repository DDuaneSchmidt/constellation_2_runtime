from __future__ import annotations

from datetime import date

import pytest

from research_lab.providers.alpha_vantage_daily import (
    AlphaVantageDailyProvider,
    normalize_alpha_vantage_csv,
)
from research_lab.providers.base import ProviderFetchError, ProviderUnavailableError


ALPHA_FIXTURE = (
    "timestamp,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient\n"
    "2024-01-03,100,102,99,101,100.5,1000,0.0000,1.0\n"
    "2024-01-02,99,101,98,100,99.5,900,0.0000,1.0\n"
)


def test_alpha_vantage_provider_normalizes_fixture_csv() -> None:
    rows = normalize_alpha_vantage_csv(symbol="spy", csv_text=ALPHA_FIXTURE, fetched_at="2026-05-18T00:00:00Z")

    assert [row["date"] for row in rows] == ["2024-01-02", "2024-01-03"]
    assert rows[0]["symbol"] == "SPY"
    assert rows[0]["provider"] == "alpha_vantage"
    assert rows[0]["adj_close"] == "99.5"
    assert rows[0]["dividend_amount"] == "0.0000"
    assert rows[0]["split_coefficient"] == "1.0"


def test_alpha_vantage_missing_api_key_fails_provider_init(monkeypatch) -> None:
    monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)

    with pytest.raises(ProviderUnavailableError, match="ALPHA_VANTAGE_API_KEY"):
        AlphaVantageDailyProvider()


def test_alpha_vantage_note_response_fails_symbol_explicitly() -> None:
    with pytest.raises(ProviderFetchError, match="provider failure"):
        normalize_alpha_vantage_csv(symbol="SPY", csv_text='{"Note": "API call frequency"}')


def test_alpha_vantage_error_message_response_fails_symbol_explicitly() -> None:
    with pytest.raises(ProviderFetchError, match="provider failure"):
        normalize_alpha_vantage_csv(symbol="SPY", csv_text='{"Error Message": "Invalid API call"}')


def test_alpha_vantage_unexpected_columns_fail_symbol_explicitly() -> None:
    with pytest.raises(ProviderFetchError, match="unexpected columns"):
        normalize_alpha_vantage_csv(symbol="SPY", csv_text="date,open,close\n2024-01-02,1,1\n")


def test_alpha_vantage_fetch_filters_dates_and_uses_transport(monkeypatch) -> None:
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "test-key")
    monkeypatch.setenv("ALPHA_VANTAGE_RATE_LIMIT_SLEEP_SECONDS", "0")
    provider = AlphaVantageDailyProvider(transport=lambda url: ALPHA_FIXTURE)

    rows = provider.fetch_daily_ohlcv("SPY", date(2024, 1, 3), date(2024, 1, 3))

    assert len(rows) == 1
    assert rows[0]["date"] == "2024-01-03"

