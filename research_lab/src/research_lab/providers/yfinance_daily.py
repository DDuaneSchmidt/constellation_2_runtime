from __future__ import annotations

from datetime import date
from typing import Any

from research_lab.providers.base import DailyOHLCVProvider, ProviderFetchError, ProviderUnavailableError
from research_lab.runtime.dependencies import research_dependency_health
from research_lab.storage.hashing import utc_now_iso


REQUIRED_RAW_COLUMNS = [
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "provider",
    "provider_version",
    "fetched_at",
]


def _records_from_frame(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if isinstance(frame, list):
        return [dict(row) for row in frame]
    if hasattr(frame, "reset_index") and hasattr(frame, "to_dict"):
        reset = frame.reset_index()
        return [dict(row) for row in reset.to_dict("records")]
    if hasattr(frame, "to_dict"):
        payload = frame.to_dict("records")
        return [dict(row) for row in payload]
    raise TypeError("Unsupported yfinance frame object")


def _get_value(row: dict[str, Any], *names: str) -> Any:
    lowered = {}
    for key, value in row.items():
        if isinstance(key, tuple):
            key_text = "_".join(str(part) for part in key if part not in (None, ""))
        else:
            key_text = str(key)
        lowered[key_text.lower().replace(" ", "_")] = value
    for name in names:
        key = name.lower().replace(" ", "_")
        if key in lowered:
            return lowered[key]
    return None


def normalize_yfinance_records(
    *,
    symbol: str,
    rows: Any,
    provider_version: str,
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    fetched = fetched_at or utc_now_iso()
    normalized: list[dict[str, Any]] = []
    for row in _records_from_frame(rows):
        raw_date = _get_value(row, "date", "datetime")
        if raw_date is None:
            raw_date = _get_value(row, "index")
        normalized.append(
            {
                "date": str(raw_date)[:10],
                "symbol": symbol.upper(),
                "open": _get_value(row, "open"),
                "high": _get_value(row, "high"),
                "low": _get_value(row, "low"),
                "close": _get_value(row, "close"),
                "adj_close": _get_value(row, "adj_close", "adj close"),
                "volume": _get_value(row, "volume"),
                "provider": "yfinance",
                "provider_version": provider_version,
                "fetched_at": fetched,
            }
        )
    return normalized


class YFinanceDailyProvider(DailyOHLCVProvider):
    provider_name = "yfinance"
    adjustment_policy = "provider_adjusted_close"

    def __init__(self) -> None:
        try:
            import yfinance as yf  # type: ignore
        except Exception as exc:  # pragma: no cover - depends on optional package
            raise ProviderUnavailableError(
                "yfinance",
                "yfinance provider unavailable; install yfinance to build live OHLCV snapshots",
                diagnostic=research_dependency_health(),
            ) from exc
        self._yf = yf
        self.provider_version = str(getattr(yf, "__version__", "unknown"))

    def fetch_daily_ohlcv(self, symbol: str, start: date, end: date) -> Any:
        try:
            frame = self._yf.Ticker(symbol).history(
                start=start.isoformat(),
                end=end.isoformat(),
                interval="1d",
                auto_adjust=False,
                actions=False,
            )
        except Exception as exc:  # pragma: no cover - network/provider dependent
            raise ProviderFetchError(
                "yfinance",
                symbol,
                f"yfinance fetch failed for {symbol}: {exc}",
                diagnostic={
                    "symbol": symbol,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "provider_version": self.provider_version,
                    "exception_type": type(exc).__name__,
                },
            ) from exc
        rows = normalize_yfinance_records(symbol=symbol, rows=frame, provider_version=self.provider_version)
        if not rows:
            raise ProviderFetchError(
                "yfinance",
                symbol,
                f"yfinance returned no daily OHLCV rows for {symbol}",
                diagnostic={
                    "symbol": symbol,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "provider_version": self.provider_version,
                    "empty_frame": True,
                },
            )
        return rows


def yfinance_provider_diagnostics() -> dict[str, Any]:
    health = research_dependency_health()
    try:
        provider = YFinanceDailyProvider()
        return {
            "provider": "yfinance",
            "available": True,
            "provider_version": provider.provider_version,
            "dependency_health": health,
            "error": None,
        }
    except ProviderUnavailableError as exc:
        return {
            "provider": "yfinance",
            "available": False,
            "provider_version": None,
            "dependency_health": exc.diagnostic or health,
            "error": str(exc),
        }
