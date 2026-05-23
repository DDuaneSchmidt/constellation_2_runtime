from __future__ import annotations

import csv
import os
from datetime import date
from io import StringIO
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from research_lab.providers.base import DailyOHLCVProvider, ProviderFetchError, ProviderUnavailableError
from research_lab.providers.rate_limit import RateLimiter, RateLimitPolicy
from research_lab.storage.hashing import utc_now_iso


ALPHA_VANTAGE_PROVIDER_VERSION = "alpha_vantage_daily_adjusted_v1"
ALPHA_VANTAGE_ADJUSTMENT_POLICY = "provider_adjusted_close"
ALPHA_VANTAGE_REQUIRED_COLUMNS = {
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume",
}


def alpha_vantage_api_key() -> str:
    return os.environ.get("ALPHA_VANTAGE_API_KEY", "").strip()


def _default_transport(url: str) -> str:
    request = Request(url, headers={"User-Agent": "aegis-research-lab/1.0"})
    with urlopen(request, timeout=45) as response:  # pragma: no cover - network dependent
        return response.read().decode("utf-8")


def _contains_provider_failure(text: str) -> str | None:
    lowered = text.lower()
    markers = [
        "error message",
        "information",
        "note",
        "thank you for using alpha vantage",
        "premium endpoint",
        "rate limit",
        "api call frequency",
        "invalid api call",
    ]
    for marker in markers:
        if marker in lowered:
            return marker
    return None


def normalize_alpha_vantage_csv(
    *,
    symbol: str,
    csv_text: str,
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    failure_marker = _contains_provider_failure(csv_text[:1000])
    if failure_marker:
        raise ProviderFetchError(
            "alpha_vantage",
            symbol.upper(),
            f"Alpha Vantage returned provider failure text for {symbol.upper()}: {failure_marker}",
            diagnostic={"symbol": symbol.upper(), "marker": failure_marker, "response_preview": csv_text[:500]},
        )
    reader = csv.DictReader(StringIO(csv_text))
    fieldnames = {str(field or "").strip().lower() for field in (reader.fieldnames or [])}
    if not ALPHA_VANTAGE_REQUIRED_COLUMNS.issubset(fieldnames):
        raise ProviderFetchError(
            "alpha_vantage",
            symbol.upper(),
            f"Alpha Vantage returned unexpected columns for {symbol.upper()}",
            diagnostic={"symbol": symbol.upper(), "fieldnames": sorted(fieldnames), "required": sorted(ALPHA_VANTAGE_REQUIRED_COLUMNS)},
        )
    fetched = fetched_at or utc_now_iso()
    rows: list[dict[str, Any]] = []
    for row in reader:
        day = str(row.get("timestamp") or "").strip()[:10]
        if not day:
            continue
        rows.append(
            {
                "date": day,
                "symbol": symbol.upper(),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "adj_close": row.get("adjusted_close"),
                "volume": row.get("volume"),
                "provider": "alpha_vantage",
                "provider_version": ALPHA_VANTAGE_PROVIDER_VERSION,
                "fetched_at": fetched,
                "dividend_amount": row.get("dividend_amount"),
                "split_coefficient": row.get("split_coefficient"),
                "adjustment_policy": ALPHA_VANTAGE_ADJUSTMENT_POLICY,
            }
        )
    if not rows:
        raise ProviderFetchError(
            "alpha_vantage",
            symbol.upper(),
            f"Alpha Vantage returned no daily OHLCV rows for {symbol.upper()}",
            diagnostic={"symbol": symbol.upper(), "fieldnames": sorted(fieldnames)},
        )
    return sorted(rows, key=lambda item: item["date"])


class AlphaVantageDailyProvider(DailyOHLCVProvider):
    provider_name = "alpha_vantage"
    provider_version = ALPHA_VANTAGE_PROVIDER_VERSION
    adjustment_policy = ALPHA_VANTAGE_ADJUSTMENT_POLICY

    def __init__(
        self,
        *,
        api_key: str | None = None,
        transport: Callable[[str], str] | None = None,
        rate_limit_policy: RateLimitPolicy | None = None,
    ) -> None:
        self.api_key = (api_key if api_key is not None else alpha_vantage_api_key()).strip()
        if not self.api_key:
            raise ProviderUnavailableError(
                "alpha_vantage",
                "ALPHA_VANTAGE_API_KEY is required for Alpha Vantage OHLCV acquisition",
                diagnostic={"api_key_present": False},
            )
        self._transport = transport or _default_transport
        self._rate_limiter = RateLimiter(rate_limit_policy or RateLimitPolicy.from_env(prefix="ALPHA_VANTAGE"))

    def _url(self, symbol: str) -> str:
        query = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol.upper(),
            "outputsize": "full",
            "datatype": "csv",
            "apikey": self.api_key,
        }
        return "https://www.alphavantage.co/query?" + urlencode(query)

    def fetch_daily_ohlcv(self, symbol: str, start: date, end: date) -> list[dict[str, Any]]:
        clean_symbol = symbol.upper().strip()
        self._rate_limiter.wait_before_call()
        url = self._url(clean_symbol)
        safe_url = url.replace(self.api_key, "REDACTED")
        attempts = max(1, self._rate_limiter.policy.max_retries)
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                text = self._transport(url)
                rows = normalize_alpha_vantage_csv(symbol=clean_symbol, csv_text=text)
                filtered = [
                    row
                    for row in rows
                    if start.isoformat() <= str(row["date"])[:10] <= end.isoformat()
                ]
                if not filtered:
                    raise ProviderFetchError(
                        "alpha_vantage",
                        clean_symbol,
                        f"Alpha Vantage returned no rows in requested date range for {clean_symbol}",
                        diagnostic={"symbol": clean_symbol, "start": start.isoformat(), "end": end.isoformat(), "url": safe_url},
                    )
                return filtered
            except ProviderFetchError:
                raise
            except Exception as exc:  # pragma: no cover - network dependent
                last_error = exc
        raise ProviderFetchError(
            "alpha_vantage",
            clean_symbol,
            f"Alpha Vantage fetch failed for {clean_symbol}: {last_error}",
            diagnostic={"symbol": clean_symbol, "url": safe_url, "attempts": attempts, "exception_type": type(last_error).__name__ if last_error else None},
        )


def alpha_vantage_provider_diagnostics() -> dict[str, Any]:
    api_key_present = bool(alpha_vantage_api_key())
    policy = RateLimitPolicy.from_env(prefix="ALPHA_VANTAGE")
    return {
        "provider": "alpha_vantage",
        "available": api_key_present,
        "api_key_present": api_key_present,
        "provider_version": ALPHA_VANTAGE_PROVIDER_VERSION,
        "rate_limit_policy": {"sleep_seconds": policy.sleep_seconds, "max_retries": policy.max_retries},
        "adjustment_policy": ALPHA_VANTAGE_ADJUSTMENT_POLICY,
        "error": None if api_key_present else "missing ALPHA_VANTAGE_API_KEY",
    }

