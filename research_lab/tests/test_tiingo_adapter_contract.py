from __future__ import annotations

from datetime import date
from urllib.error import HTTPError

import pytest

from research_lab.providers.base import ProviderFetchError, ProviderUnavailableError
from research_lab.providers.rate_limit import RateLimitPolicy
from research_lab.providers.tiingo_daily import TiingoDailyProvider, normalize_tiingo_json


TIINGO_FIXTURE = """[
  {
    "date": "2024-01-03T00:00:00.000Z",
    "open": 100.0,
    "high": 102.0,
    "low": 99.0,
    "close": 101.0,
    "adjOpen": 99.5,
    "adjHigh": 101.5,
    "adjLow": 98.5,
    "adjClose": 100.5,
    "volume": 1000,
    "adjVolume": 1000,
    "divCash": 0.0,
    "splitFactor": 1.0
  },
  {
    "date": "2024-01-02T00:00:00.000Z",
    "open": 99.0,
    "high": 101.0,
    "low": 98.0,
    "close": 100.0,
    "adjOpen": 98.5,
    "adjHigh": 100.5,
    "adjLow": 97.5,
    "adjClose": 99.5,
    "volume": 900,
    "adjVolume": 900,
    "divCash": 0.0,
    "splitFactor": 1.0
  }
]"""


def test_tiingo_provider_normalizes_fixture_json() -> None:
    rows = normalize_tiingo_json(symbol="spy", json_text=TIINGO_FIXTURE, fetched_at="2026-05-18T00:00:00Z")

    assert [row["date"] for row in rows] == ["2024-01-02", "2024-01-03"]
    assert rows[0]["symbol"] == "SPY"
    assert rows[0]["provider"] == "tiingo"
    assert rows[0]["adj_close"] == 99.5
    assert rows[0]["adj_open"] == 98.5
    assert rows[0]["adj_volume"] == 900
    assert rows[0]["dividend_amount"] == 0.0
    assert rows[0]["split_coefficient"] == 1.0


def test_tiingo_missing_api_key_fails_provider_init(monkeypatch) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)

    with pytest.raises(ProviderUnavailableError, match="TIINGO_API_KEY"):
        TiingoDailyProvider()


def test_tiingo_provider_error_json_fails_symbol_explicitly() -> None:
    with pytest.raises(ProviderFetchError, match="provider error"):
        normalize_tiingo_json(symbol="SPY", json_text='{"detail": "token invalid"}')


def test_tiingo_empty_json_fails_symbol_explicitly() -> None:
    with pytest.raises(ProviderFetchError, match="no daily OHLCV rows"):
        normalize_tiingo_json(symbol="SPY", json_text="[]")


def test_tiingo_unexpected_schema_fails_symbol_explicitly() -> None:
    with pytest.raises(ProviderFetchError, match="unexpected schema"):
        normalize_tiingo_json(symbol="SPY", json_text='[{"date": "2024-01-02", "open": 1}]')


def test_tiingo_http_429_fails_closed() -> None:
    def transport(url: str) -> str:
        raise HTTPError(url, 429, "Too Many Requests", None, None)

    provider = TiingoDailyProvider(
        api_key="test-key",
        transport=transport,
        rate_limit_policy=RateLimitPolicy(sleep_seconds=0, max_retries=1),
    )

    with pytest.raises(ProviderFetchError, match="HTTP 429"):
        provider.fetch_daily_ohlcv("SPY", date(2024, 1, 2), date(2024, 1, 3))


def test_tiingo_fetch_filters_dates_and_uses_transport() -> None:
    provider = TiingoDailyProvider(
        api_key="test-key",
        transport=lambda url: TIINGO_FIXTURE,
        rate_limit_policy=RateLimitPolicy(sleep_seconds=0, max_retries=1),
    )

    rows = provider.fetch_daily_ohlcv("SPY", date(2024, 1, 3), date(2024, 1, 3))

    assert len(rows) == 1
    assert rows[0]["date"] == "2024-01-03"
