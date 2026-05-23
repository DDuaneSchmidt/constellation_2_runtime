from __future__ import annotations

from research_lab.providers.base import DailyOHLCVProvider
from research_lab.providers.alpha_vantage_daily import AlphaVantageDailyProvider, alpha_vantage_provider_diagnostics
from research_lab.providers.local_csv_daily import LocalCSVDailyProvider, local_csv_provider_diagnostics
from research_lab.providers.stooq_daily import StooqDailyProvider, stooq_provider_diagnostics
from research_lab.providers.tiingo_daily import TiingoDailyProvider, tiingo_provider_diagnostics
from research_lab.providers.yfinance_daily import YFinanceDailyProvider, yfinance_provider_diagnostics


def get_daily_ohlcv_provider(provider_name: str) -> DailyOHLCVProvider:
    name = str(provider_name or "").strip().lower()
    if name == "yfinance":
        return YFinanceDailyProvider()
    if name == "stooq":
        return StooqDailyProvider()
    if name == "local_csv":
        return LocalCSVDailyProvider()
    if name == "alpha_vantage":
        return AlphaVantageDailyProvider()
    if name == "tiingo":
        return TiingoDailyProvider()
    raise ValueError(f"Unsupported OHLCV provider: {provider_name}")


def provider_diagnostics(provider_name: str) -> dict:
    name = str(provider_name or "").strip().lower()
    if name == "yfinance":
        return yfinance_provider_diagnostics()
    if name == "stooq":
        return stooq_provider_diagnostics()
    if name == "local_csv":
        return local_csv_provider_diagnostics()
    if name == "alpha_vantage":
        return alpha_vantage_provider_diagnostics()
    if name == "tiingo":
        return tiingo_provider_diagnostics()
    return {"provider": provider_name, "available": False, "error": "unsupported_provider"}
