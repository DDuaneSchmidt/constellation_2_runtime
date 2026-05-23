from __future__ import annotations

import csv
import os
from datetime import date
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import yaml

from research_lab.providers.base import DailyOHLCVProvider, ProviderFetchError
from research_lab.storage.hashing import utc_now_iso
from research_lab.storage.paths import research_lab_root


STOOQ_PROVIDER_VERSION = "stooq_csv_daily_v1"
STOOQ_ADJUSTMENT_POLICY = "unadjusted_close_as_adj_close"


def stooq_symbol_map_path() -> Path:
    return research_lab_root() / "providers" / "stooq_symbol_map.yaml"


def load_stooq_symbol_map(path: Path | None = None) -> dict[str, str]:
    source = path or stooq_symbol_map_path()
    payload = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
    return {str(key).upper(): str(value).lower() for key, value in dict(payload).items()}


def map_stooq_symbol(symbol: str, *, mapping: dict[str, str] | None = None) -> str:
    clean = str(symbol or "").upper().strip()
    symbol_map = mapping or load_stooq_symbol_map()
    if clean not in symbol_map:
        raise ProviderFetchError(
            "stooq",
            clean,
            f"No Stooq symbol mapping for {clean}",
            diagnostic={"symbol": clean, "known_mappings": sorted(symbol_map)},
        )
    return symbol_map[clean]


def normalize_stooq_csv(
    *,
    symbol: str,
    csv_text: str,
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    fetched = fetched_at or utc_now_iso()
    rows: list[dict[str, Any]] = []
    reader = csv.DictReader(StringIO(csv_text))
    required = {"Date", "Open", "High", "Low", "Close", "Volume"}
    if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
        return []
    for row in reader:
        if not row or str(row.get("Date") or "").lower() == "no data":
            continue
        close = row.get("Close")
        rows.append(
            {
                "date": str(row.get("Date") or "")[:10],
                "symbol": symbol.upper(),
                "open": row.get("Open"),
                "high": row.get("High"),
                "low": row.get("Low"),
                "close": close,
                "adj_close": close,
                "volume": row.get("Volume"),
                "provider": "stooq",
                "provider_version": STOOQ_PROVIDER_VERSION,
                "fetched_at": fetched,
                "adjustment_policy": STOOQ_ADJUSTMENT_POLICY,
            }
        )
    return rows


class StooqDailyProvider(DailyOHLCVProvider):
    provider_name = "stooq"
    provider_version = STOOQ_PROVIDER_VERSION
    adjustment_policy = STOOQ_ADJUSTMENT_POLICY

    def __init__(self, *, symbol_map: dict[str, str] | None = None) -> None:
        self._symbol_map = symbol_map or load_stooq_symbol_map()

    def fetch_daily_ohlcv(self, symbol: str, start: date, end: date) -> list[dict[str, Any]]:
        stooq_symbol = map_stooq_symbol(symbol, mapping=self._symbol_map)
        query = {
            "s": stooq_symbol,
            "i": "d",
            "d1": start.isoformat().replace("-", ""),
            "d2": end.isoformat().replace("-", ""),
        }
        api_key = os.environ.get("STOOQ_API_KEY", "").strip()
        if api_key:
            query["apikey"] = api_key
        params = urlencode(query)
        url = f"https://stooq.com/q/d/l/?{params}"
        safe_url = url.replace(api_key, "REDACTED") if api_key else url
        try:
            request = Request(url, headers={"User-Agent": "aegis-research-lab/1.0"})
            with urlopen(request, timeout=30) as response:
                csv_text = response.read().decode("utf-8")
        except Exception as exc:  # pragma: no cover - network dependent
            raise ProviderFetchError(
                "stooq",
                symbol,
                f"Stooq fetch failed for {symbol}: {exc}",
                diagnostic={"symbol": symbol, "stooq_symbol": stooq_symbol, "url": safe_url, "api_key_configured": bool(api_key), "exception_type": type(exc).__name__},
            ) from exc
        rows = normalize_stooq_csv(symbol=symbol, csv_text=csv_text)
        if not rows:
            raise ProviderFetchError(
                "stooq",
                symbol,
                f"Stooq returned no daily OHLCV rows for {symbol}",
                diagnostic={
                    "symbol": symbol,
                    "stooq_symbol": stooq_symbol,
                    "url": safe_url,
                    "api_key_configured": bool(api_key),
                    "empty_or_non_csv_response": csv_text[:500],
                },
            )
        return rows


def stooq_provider_diagnostics() -> dict[str, Any]:
    try:
        symbol_map = load_stooq_symbol_map()
        return {
            "provider": "stooq",
            "available": True,
            "provider_version": STOOQ_PROVIDER_VERSION,
            "mapped_symbols": sorted(symbol_map),
            "adjustment_policy": STOOQ_ADJUSTMENT_POLICY,
            "api_key_configured": bool(os.environ.get("STOOQ_API_KEY", "").strip()),
            "error": None,
        }
    except Exception as exc:
        return {
            "provider": "stooq",
            "available": False,
            "provider_version": STOOQ_PROVIDER_VERSION,
            "mapped_symbols": [],
            "adjustment_policy": STOOQ_ADJUSTMENT_POLICY,
            "api_key_configured": bool(os.environ.get("STOOQ_API_KEY", "").strip()),
            "error": str(exc),
        }
