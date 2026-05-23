from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.datasets.dataset_registry import load_dataset_snapshot
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.storage.paths import ensure_store_layout


def canonical_parquet_path(dataset_snapshot_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "datasets" / dataset_snapshot_id / "data" / "canonical" / "daily_ohlcv.parquet"


def load_dataset_snapshot_rows(dataset_snapshot_id: str, *, store_root: Path | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    load_dataset_snapshot(dataset_snapshot_id, store_root=store_root)
    rows = read_parquet_records(canonical_parquet_path(dataset_snapshot_id, store_root=store_root))
    return rows[:limit] if limit is not None else rows


def load_dataset_snapshot_relation(dataset_snapshot_id: str, *, store_root: Path | None = None) -> Any:
    path = canonical_parquet_path(dataset_snapshot_id, store_root=store_root)
    try:
        import duckdb  # type: ignore
    except Exception as exc:
        raise RuntimeError("duckdb unavailable; install duckdb to query canonical Parquet as a relation") from exc
    return duckdb.sql(f"SELECT * FROM read_parquet('{path}')")


def dataset_symbol_summary(dataset_snapshot_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    rows = load_dataset_snapshot_rows(dataset_snapshot_id, store_root=store_root)
    by_symbol: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "")
        if symbol not in by_symbol:
            by_symbol[symbol] = {"symbol": symbol, "row_count": 0, "first_date": None, "last_date": None}
        entry = by_symbol[symbol]
        day = row.get("date")
        entry["row_count"] += 1
        entry["first_date"] = day if entry["first_date"] is None or day < entry["first_date"] else entry["first_date"]
        entry["last_date"] = day if entry["last_date"] is None or day > entry["last_date"] else entry["last_date"]
    return [by_symbol[key] for key in sorted(by_symbol)]
