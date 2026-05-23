from __future__ import annotations

import csv
import os
from datetime import date
from pathlib import Path
from typing import Any

from research_lab.providers.base import DailyOHLCVProvider, ProviderFetchError
from research_lab.storage.hashing import utc_now_iso
from research_lab.storage.paths import research_lab_root


LOCAL_CSV_PROVIDER_VERSION = "local_csv_daily_v1"
LOCAL_CSV_ADJUSTMENT_POLICY = "csv_adj_close_or_unadjusted_close_as_adj_close"
LOCAL_CSV_FALLBACK_ADJUSTMENT_POLICY = "unadjusted_close_as_adj_close"


def local_csv_root() -> Path:
    override = os.environ.get("LOCAL_CSV_OHLCV_ROOT", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return research_lab_root() / "local_data" / "ohlcv"


def _normalized_key_map(row: dict[str, Any]) -> dict[str, Any]:
    return {str(key).strip().lower().replace(" ", "_"): value for key, value in row.items()}


def normalize_local_csv_rows(
    *,
    symbol: str,
    csv_rows: list[dict[str, Any]],
    start: date,
    end: date,
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    fetched = fetched_at or utc_now_iso()
    normalized: list[dict[str, Any]] = []
    for raw in csv_rows:
        row = _normalized_key_map(raw)
        raw_date = str(row.get("date") or row.get("timestamp") or "").strip()[:10]
        if not raw_date:
            continue
        day = date.fromisoformat(raw_date)
        if day < start or day > end:
            continue
        close = row.get("close")
        adj_close = row.get("adj_close")
        adjustment_policy = "csv_adj_close"
        if adj_close in (None, ""):
            adj_close = close
            adjustment_policy = LOCAL_CSV_FALLBACK_ADJUSTMENT_POLICY
        normalized.append(
            {
                "date": raw_date,
                "symbol": symbol.upper(),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": close,
                "adj_close": adj_close,
                "volume": row.get("volume"),
                "provider": "local_csv",
                "provider_version": LOCAL_CSV_PROVIDER_VERSION,
                "fetched_at": fetched,
                "adjustment_policy": adjustment_policy,
            }
        )
    return sorted(normalized, key=lambda item: item["date"])


class LocalCSVDailyProvider(DailyOHLCVProvider):
    provider_name = "local_csv"
    provider_version = LOCAL_CSV_PROVIDER_VERSION
    adjustment_policy = LOCAL_CSV_ADJUSTMENT_POLICY

    def __init__(self, *, root: Path | None = None) -> None:
        self.root = (root or local_csv_root()).resolve()

    def csv_path_for_symbol(self, symbol: str) -> Path:
        return self.root / f"{symbol.upper()}.csv"

    def fetch_daily_ohlcv(self, symbol: str, start: date, end: date) -> list[dict[str, Any]]:
        path = self.csv_path_for_symbol(symbol)
        if not path.exists():
            raise ProviderFetchError(
                "local_csv",
                symbol.upper(),
                f"Local CSV file not found for {symbol.upper()}: {path}",
                diagnostic={"symbol": symbol.upper(), "path": str(path), "root": str(self.root)},
            )
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = [dict(row) for row in reader]
        normalized = normalize_local_csv_rows(symbol=symbol, csv_rows=rows, start=start, end=end)
        if not normalized:
            raise ProviderFetchError(
                "local_csv",
                symbol.upper(),
                f"Local CSV file has no rows in requested date range for {symbol.upper()}: {path}",
                diagnostic={"symbol": symbol.upper(), "path": str(path), "start": start.isoformat(), "end": end.isoformat()},
            )
        return normalized


def local_csv_provider_diagnostics() -> dict[str, Any]:
    root = local_csv_root()
    files = sorted(path.name for path in root.glob("*.csv")) if root.exists() else []
    return {
        "provider": "local_csv",
        "available": root.exists(),
        "provider_version": LOCAL_CSV_PROVIDER_VERSION,
        "root": str(root),
        "csv_files": files,
        "file_count": len(files),
        "adjustment_policy": LOCAL_CSV_ADJUSTMENT_POLICY,
        "env_override": bool(os.environ.get("LOCAL_CSV_OHLCV_ROOT", "").strip()),
        "error": None if root.exists() else "local_csv_root_not_found",
    }
