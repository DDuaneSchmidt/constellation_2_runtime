from __future__ import annotations

import pytest

import research_lab.providers.factory as factory
from research_lab.providers.base import DailyOHLCVProvider
from research_lab.providers.local_csv_daily import LocalCSVDailyProvider
from research_lab.providers.stooq_daily import StooqDailyProvider


class DummyYFinanceProvider(DailyOHLCVProvider):
    provider_name = "yfinance"
    provider_version = "test"

    def fetch_daily_ohlcv(self, symbol, start, end):
        return []


def test_provider_factory_returns_yfinance_and_stooq(monkeypatch) -> None:
    monkeypatch.setattr(factory, "YFinanceDailyProvider", DummyYFinanceProvider)

    assert isinstance(factory.get_daily_ohlcv_provider("local_csv"), LocalCSVDailyProvider)
    assert isinstance(factory.get_daily_ohlcv_provider("stooq"), StooqDailyProvider)
    assert isinstance(factory.get_daily_ohlcv_provider("yfinance"), DummyYFinanceProvider)


def test_provider_factory_rejects_unknown_provider() -> None:
    with pytest.raises(ValueError):
        factory.get_daily_ohlcv_provider("not-a-provider")


def test_provider_diagnostics_supports_stooq() -> None:
    payload = factory.provider_diagnostics("stooq")

    assert payload["available"] is True
    assert payload["provider"] == "stooq"
    assert payload["adjustment_policy"] == "unadjusted_close_as_adj_close"
