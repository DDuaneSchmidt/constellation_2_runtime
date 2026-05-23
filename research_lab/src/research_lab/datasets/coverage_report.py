from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from research_lab.storage.hashing import content_hash, utc_now_iso
from research_lab.storage.manifest_io import read_json, write_json
from research_lab.storage.paths import ensure_store_layout


def _date_text(value: Any) -> str:
    return str(value)[:10]


def _business_day_count(start: str, end: str) -> int:
    start_day = date.fromisoformat(_date_text(start))
    end_day = date.fromisoformat(_date_text(end))
    count = 0
    current = start_day
    while current <= end_day:
        if current.weekday() < 5:
            count += 1
        current = date.fromordinal(current.toordinal() + 1)
    return max(count, 1)


def build_coverage_report(
    *,
    dataset_snapshot_id: str,
    universe_snapshot_id: str,
    symbols_requested: list[str],
    symbols_loaded: list[str],
    symbols_missing: list[str],
    canonical_rows: list[dict[str, Any]],
    start_date: str,
    end_date: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    requested_days = _business_day_count(start_date, end_date)
    by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols_requested}
    for row in canonical_rows:
        symbol = str(row.get("symbol") or "").upper()
        by_symbol.setdefault(symbol, []).append(row)

    coverage_by_symbol: list[dict[str, Any]] = []
    for symbol in sorted(symbols_requested):
        rows = sorted(by_symbol.get(symbol, []), key=lambda item: _date_text(item.get("date")))
        row_count = len(rows)
        coverage_ratio = row_count / requested_days if requested_days else 0.0
        warnings: list[str] = []
        if symbol in symbols_missing:
            warnings.append("missing_symbol")
        if row_count and coverage_ratio < 0.75:
            warnings.append("low_coverage")
        coverage_by_symbol.append(
            {
                "symbol": symbol,
                "row_count": row_count,
                "first_date": _date_text(rows[0]["date"]) if rows else None,
                "last_date": _date_text(rows[-1]["date"]) if rows else None,
                "requested_start_date": start_date,
                "requested_end_date": end_date,
                "coverage_ratio": coverage_ratio,
                "missing_row_estimate": max(requested_days - row_count, 0),
                "quality_status": "missing" if symbol in symbols_missing else ("warning" if warnings else "ok"),
                "warnings": warnings,
            }
        )

    loaded_entries = [entry for entry in coverage_by_symbol if entry["row_count"] > 0]
    report = {
        "dataset_snapshot_id": dataset_snapshot_id,
        "universe_snapshot_id": universe_snapshot_id,
        "symbols_requested": sorted(symbols_requested),
        "symbols_loaded": sorted(symbols_loaded),
        "symbols_missing": sorted(symbols_missing),
        "symbol_count_requested": len(set(symbols_requested)),
        "symbol_count_loaded": len(set(symbols_loaded)),
        "row_count_total": len(canonical_rows),
        "start_date_requested": start_date,
        "end_date_requested": end_date,
        "coverage_by_symbol": coverage_by_symbol,
        "overall_coverage_ratio": (
            sum(entry["coverage_ratio"] for entry in coverage_by_symbol) / len(coverage_by_symbol)
            if coverage_by_symbol
            else 0.0
        ),
        "min_symbol_start_date": min((entry["first_date"] for entry in loaded_entries if entry["first_date"]), default=None),
        "max_symbol_end_date": max((entry["last_date"] for entry in loaded_entries if entry["last_date"]), default=None),
        "created_at": created_at or utc_now_iso(),
        "content_hash": "",
        "schema_version": "dataset_coverage_report.v1",
    }
    report["content_hash"] = content_hash(report, exclude={"created_at", "content_hash"}, sort_lists=False)
    return report


def write_coverage_report(dataset_dir: Path, report: dict[str, Any]) -> tuple[Path, str]:
    path = dataset_dir / "coverage_report.json"
    write_json(path, report, overwrite=False)
    return path, str(report["content_hash"])


def load_coverage_report(dataset_snapshot_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(ensure_store_layout(store_root) / "datasets" / dataset_snapshot_id / "coverage_report.json")
