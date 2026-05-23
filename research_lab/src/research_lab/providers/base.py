from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any


class ProviderUnavailableError(RuntimeError):
    def __init__(self, provider_name: str, message: str, *, diagnostic: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.provider_name = provider_name
        self.diagnostic = diagnostic or {}


class ProviderFetchError(RuntimeError):
    def __init__(self, provider_name: str, symbol: str, message: str, *, diagnostic: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.provider_name = provider_name
        self.symbol = symbol
        self.diagnostic = diagnostic or {}


class DailyOHLCVProvider(ABC):
    provider_name: str
    provider_version: str
    adjustment_policy: str = "provider_adjusted_close"

    @abstractmethod
    def fetch_daily_ohlcv(self, symbol: str, start: date, end: date) -> Any:
        """Return provider-normalized daily OHLCV rows or a DataFrame-like object."""
