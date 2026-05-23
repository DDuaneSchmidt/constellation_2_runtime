from __future__ import annotations

from datetime import date
from typing import Any


CANONICAL_DAILY_OHLCV_COLUMNS = [
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
    "bar_policy_version",
    "dataset_snapshot_id",
]


def _records(rows: Any) -> list[dict[str, Any]]:
    if rows is None:
        return []
    if isinstance(rows, list):
        return [dict(row) for row in rows]
    if hasattr(rows, "to_dict"):
        return [dict(row) for row in rows.to_dict("records")]
    raise TypeError("Unsupported rows object")


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(float(value))


def canonicalize_daily_ohlcv_rows(
    rows: Any,
    *,
    bar_policy_version: str,
    dataset_snapshot_id: str = "",
) -> list[dict[str, Any]]:
    canonical: list[dict[str, Any]] = []
    for row in _records(rows):
        symbol = str(row.get("symbol") or "").strip().upper()
        day = str(row.get("date") or "")[:10]
        if not symbol or not day:
            continue
        canonical.append(
            {
                "date": day,
                "symbol": symbol,
                "open": _float_or_none(row.get("open")),
                "high": _float_or_none(row.get("high")),
                "low": _float_or_none(row.get("low")),
                "close": _float_or_none(row.get("close")),
                "adj_close": _float_or_none(row.get("adj_close")),
                "volume": _int_or_none(row.get("volume")),
                "provider": str(row.get("provider") or ""),
                "provider_version": str(row.get("provider_version") or ""),
                "bar_policy_version": bar_policy_version,
                "dataset_snapshot_id": dataset_snapshot_id,
            }
        )
    return sorted(canonical, key=lambda item: (item["symbol"], item["date"]))


def attach_dataset_snapshot_id(rows: list[dict[str, Any]], dataset_snapshot_id: str) -> list[dict[str, Any]]:
    return [{**row, "dataset_snapshot_id": dataset_snapshot_id} for row in rows]


def symbols_in_rows(rows: list[dict[str, Any]]) -> list[str]:
    return sorted({str(row.get("symbol") or "").upper() for row in rows if row.get("symbol")})
