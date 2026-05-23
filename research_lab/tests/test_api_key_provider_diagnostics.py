from __future__ import annotations

import research_lab.providers.factory as factory
from research_lab.providers.alpha_vantage_daily import AlphaVantageDailyProvider
from research_lab.providers.base import DailyOHLCVProvider


class DummyAlphaVantageProvider(DailyOHLCVProvider):
    provider_name = "alpha_vantage"
    provider_version = "test"

    def fetch_daily_ohlcv(self, symbol, start, end):
        return []


def test_provider_factory_returns_alpha_vantage(monkeypatch) -> None:
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "test-key")
    monkeypatch.setattr(factory, "AlphaVantageDailyProvider", DummyAlphaVantageProvider)

    assert isinstance(factory.get_daily_ohlcv_provider("alpha_vantage"), DummyAlphaVantageProvider)


def test_provider_diagnostics_reports_missing_api_key(monkeypatch) -> None:
    monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)

    payload = factory.provider_diagnostics("alpha_vantage")

    assert payload["provider"] == "alpha_vantage"
    assert payload["available"] is False
    assert payload["api_key_present"] is False
    assert payload["error"] == "missing ALPHA_VANTAGE_API_KEY"


def test_provider_diagnostics_reports_api_key_present(monkeypatch) -> None:
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "test-key")
    monkeypatch.setenv("ALPHA_VANTAGE_RATE_LIMIT_SLEEP_SECONDS", "0")

    payload = factory.provider_diagnostics("alpha_vantage")

    assert payload["available"] is True
    assert payload["api_key_present"] is True
    assert payload["rate_limit_policy"]["sleep_seconds"] == 0

