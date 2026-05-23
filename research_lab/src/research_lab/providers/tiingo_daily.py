from __future__ import annotations

import json
import os
from datetime import date
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from research_lab.providers.base import DailyOHLCVProvider, ProviderFetchError, ProviderUnavailableError
from research_lab.providers.rate_limit import RateLimiter, RateLimitPolicy
from research_lab.storage.hashing import utc_now_iso


TIINGO_PROVIDER_VERSION = "tiingo_daily_prices_v1"
TIINGO_ADJUSTMENT_POLICY = "provider_adj_close"
TIINGO_REQUIRED_FIELDS = {
    "date",
    "open",
    "high",
    "low",
    "close",
    "adjClose",
    "volume",
}


def tiingo_api_key() -> str:
    return os.environ.get("TIINGO_API_KEY", "").strip()


def _default_transport(url: str) -> str:
    request = Request(url, headers={"User-Agent": "aegis-research-lab/1.0"})
    with urlopen(request, timeout=45) as response:  # pragma: no cover - network dependent
        return response.read().decode("utf-8")


def _provider_error(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ["detail", "error", "message"]:
        value = payload.get(key)
        if value:
            return str(value)
    return None


def normalize_tiingo_json(
    *,
    symbol: str,
    json_text: str,
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    clean_symbol = symbol.upper().strip()
    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise ProviderFetchError(
            "tiingo",
            clean_symbol,
            f"Tiingo returned invalid JSON for {clean_symbol}",
            diagnostic={"symbol": clean_symbol, "exception": str(exc), "response_preview": json_text[:500]},
        ) from exc

    error = _provider_error(payload)
    if error:
        raise ProviderFetchError(
            "tiingo",
            clean_symbol,
            f"Tiingo returned provider error for {clean_symbol}: {error}",
            diagnostic={"symbol": clean_symbol, "provider_error": error},
        )
    if not isinstance(payload, list) or not payload:
        raise ProviderFetchError(
            "tiingo",
            clean_symbol,
            f"Tiingo returned no daily OHLCV rows for {clean_symbol}",
            diagnostic={"symbol": clean_symbol, "response_type": type(payload).__name__},
        )

    fetched = fetched_at or utc_now_iso()
    rows: list[dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, dict):
            raise ProviderFetchError(
                "tiingo",
                clean_symbol,
                f"Tiingo returned unexpected row shape for {clean_symbol}",
                diagnostic={"symbol": clean_symbol, "row_type": type(row).__name__},
            )
        fieldnames = set(row.keys())
        if not TIINGO_REQUIRED_FIELDS.issubset(fieldnames):
            raise ProviderFetchError(
                "tiingo",
                clean_symbol,
                f"Tiingo returned unexpected schema for {clean_symbol}",
                diagnostic={"symbol": clean_symbol, "fieldnames": sorted(fieldnames), "required": sorted(TIINGO_REQUIRED_FIELDS)},
            )
        day = str(row.get("date") or "").strip()[:10]
        if not day:
            continue
        rows.append(
            {
                "date": day,
                "symbol": clean_symbol,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "adj_close": row.get("adjClose"),
                "volume": row.get("volume"),
                "provider": "tiingo",
                "provider_version": TIINGO_PROVIDER_VERSION,
                "fetched_at": fetched,
                "adj_open": row.get("adjOpen"),
                "adj_high": row.get("adjHigh"),
                "adj_low": row.get("adjLow"),
                "adj_volume": row.get("adjVolume"),
                "dividend_amount": row.get("divCash"),
                "split_coefficient": row.get("splitFactor"),
                "adjustment_policy": TIINGO_ADJUSTMENT_POLICY,
            }
        )
    if not rows:
        raise ProviderFetchError(
            "tiingo",
            clean_symbol,
            f"Tiingo returned no parseable daily OHLCV rows for {clean_symbol}",
            diagnostic={"symbol": clean_symbol},
        )
    return sorted(rows, key=lambda item: item["date"])


class TiingoDailyProvider(DailyOHLCVProvider):
    provider_name = "tiingo"
    provider_version = TIINGO_PROVIDER_VERSION
    adjustment_policy = TIINGO_ADJUSTMENT_POLICY

    def __init__(
        self,
        *,
        api_key: str | None = None,
        transport: Callable[[str], str] | None = None,
        rate_limit_policy: RateLimitPolicy | None = None,
    ) -> None:
        self.api_key = (api_key if api_key is not None else tiingo_api_key()).strip()
        if not self.api_key:
            raise ProviderUnavailableError(
                "tiingo",
                "TIINGO_API_KEY is required for Tiingo OHLCV acquisition",
                diagnostic={"api_key_present": False},
            )
        self._transport = transport or _default_transport
        self._rate_limiter = RateLimiter(
            rate_limit_policy
            or RateLimitPolicy.from_env(prefix="TIINGO", default_sleep_seconds=2.0, default_max_retries=3)
        )

    def _url(self, symbol: str, start: date, end: date) -> str:
        query = {
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "format": "json",
            "token": self.api_key,
        }
        return f"https://api.tiingo.com/tiingo/daily/{symbol.upper()}/prices?" + urlencode(query)

    def fetch_daily_ohlcv(self, symbol: str, start: date, end: date) -> list[dict[str, Any]]:
        clean_symbol = symbol.upper().strip()
        self._rate_limiter.wait_before_call()
        url = self._url(clean_symbol, start, end)
        safe_url = url.replace(self.api_key, "REDACTED")
        attempts = max(1, self._rate_limiter.policy.max_retries)
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                text = self._transport(url)
                rows = normalize_tiingo_json(symbol=clean_symbol, json_text=text)
                filtered = [
                    row
                    for row in rows
                    if start.isoformat() <= str(row["date"])[:10] <= end.isoformat()
                ]
                if not filtered:
                    raise ProviderFetchError(
                        "tiingo",
                        clean_symbol,
                        f"Tiingo returned no rows in requested date range for {clean_symbol}",
                        diagnostic={"symbol": clean_symbol, "start": start.isoformat(), "end": end.isoformat(), "url": safe_url},
                    )
                return filtered
            except ProviderFetchError:
                raise
            except HTTPError as exc:  # pragma: no cover - network dependent
                last_error = exc
                if exc.code in {401, 403, 429}:
                    raise ProviderFetchError(
                        "tiingo",
                        clean_symbol,
                        f"Tiingo HTTP {exc.code} for {clean_symbol}; provider access failed closed",
                        diagnostic={"symbol": clean_symbol, "url": safe_url, "http_status": exc.code, "attempt": attempt},
                    ) from exc
                if exc.code >= 500 and attempt < attempts:
                    continue
                raise ProviderFetchError(
                    "tiingo",
                    clean_symbol,
                    f"Tiingo HTTP {exc.code} for {clean_symbol}; provider access failed closed",
                    diagnostic={"symbol": clean_symbol, "url": safe_url, "http_status": exc.code, "attempts": attempts},
                ) from exc
            except Exception as exc:  # pragma: no cover - network dependent
                last_error = exc
        raise ProviderFetchError(
            "tiingo",
            clean_symbol,
            f"Tiingo fetch failed for {clean_symbol}: {last_error}",
            diagnostic={"symbol": clean_symbol, "url": safe_url, "attempts": attempts, "exception_type": type(last_error).__name__ if last_error else None},
        )


def tiingo_provider_diagnostics() -> dict[str, Any]:
    api_key_present = bool(tiingo_api_key())
    policy = RateLimitPolicy.from_env(prefix="TIINGO", default_sleep_seconds=2.0, default_max_retries=3)
    return {
        "provider": "tiingo",
        "available": api_key_present,
        "api_key_present": api_key_present,
        "provider_version": TIINGO_PROVIDER_VERSION,
        "rate_limit_policy": {"sleep_seconds": policy.sleep_seconds, "max_retries": policy.max_retries},
        "adjustment_policy": TIINGO_ADJUSTMENT_POLICY,
        "error": None if api_key_present else "missing TIINGO_API_KEY",
    }
