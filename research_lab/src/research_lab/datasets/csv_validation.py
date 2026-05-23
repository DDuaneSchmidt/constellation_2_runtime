from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Any

from research_lab.acquisition.expected_files import is_template_file


def _normalized_key_map(row: dict[str, Any]) -> dict[str, Any]:
    return {str(key).strip().lower().replace(" ", "_"): value for key, value in row.items()}


def _missing(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _to_float(value: Any) -> float | None:
    if _missing(value):
        return None
    return float(value)


def _parse_rows(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")
        rows = [_normalized_key_map(dict(row)) for row in reader]
    return rows, [str(field) for field in reader.fieldnames or []]


def validate_local_csv_file(*, symbol: str, path: Path, start: str, end: str) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    if is_template_file(path):
        return {
            "symbol": symbol.upper(),
            "path": str(path),
            "exists": True,
            "readable": False,
            "template": True,
            "valid": False,
            "errors": ["template_file_ignored"],
            "warnings": [],
            "row_count": 0,
            "first_date": None,
            "last_date": None,
            "coverage_ratio": 0.0,
        }
    try:
        rows, headers = _parse_rows(path)
    except Exception as exc:
        return {
            "symbol": symbol.upper(),
            "path": str(path),
            "exists": path.exists(),
            "readable": False,
            "template": False,
            "valid": False,
            "errors": [f"parse_error: {exc}"],
            "warnings": [],
            "row_count": 0,
            "first_date": None,
            "last_date": None,
            "coverage_ratio": 0.0,
        }

    required = {"open", "high", "low", "close", "volume"}
    header_keys = {key.lower().replace(" ", "_") for key in headers}
    date_key_present = "date" in header_keys or "timestamp" in header_keys
    missing_columns = sorted(required - header_keys)
    if not date_key_present:
        errors.append("missing_date_or_timestamp_column")
    if missing_columns:
        errors.append("missing_required_columns:" + ",".join(missing_columns))
    if "adj_close" not in header_keys:
        warnings.append("adj_close_missing_will_use_close")

    start_day = date.fromisoformat(start)
    end_day = date.fromisoformat(end)
    seen_dates: set[str] = set()
    usable_dates: list[str] = []
    previous_date: str | None = None
    duplicate_dates: list[str] = []
    unsorted = False
    invalid_ohlc = False
    missing_ohlc = False
    missing_volume = False
    negative_volume = False
    nonpositive_prices = False
    date_parse_error = False

    for row in rows:
        raw_date = str(row.get("date") or row.get("timestamp") or "").strip()[:10]
        try:
            day = date.fromisoformat(raw_date)
        except Exception:
            date_parse_error = True
            continue
        if day < start_day or day > end_day:
            continue
        if previous_date is not None and raw_date < previous_date:
            unsorted = True
        previous_date = raw_date
        if raw_date in seen_dates:
            duplicate_dates.append(raw_date)
        seen_dates.add(raw_date)
        usable_dates.append(raw_date)
        try:
            open_ = _to_float(row.get("open"))
            high = _to_float(row.get("high"))
            low = _to_float(row.get("low"))
            close = _to_float(row.get("close"))
            volume = _to_float(row.get("volume"))
        except Exception:
            invalid_ohlc = True
            continue
        if any(value is None for value in [open_, high, low, close]):
            missing_ohlc = True
            continue
        if volume is None:
            missing_volume = True
        if any(float(value) <= 0 for value in [open_, high, low, close]):
            nonpositive_prices = True
        if volume is not None and volume < 0:
            negative_volume = True
        if high < low or not (low <= open_ <= high) or not (low <= close <= high):
            invalid_ohlc = True

    if date_parse_error:
        errors.append("date_column_not_parseable")
    if duplicate_dates:
        errors.append("duplicate_dates")
    if missing_ohlc:
        errors.append("missing_ohlc")
    if invalid_ohlc:
        errors.append("invalid_ohlc_structure")
    if nonpositive_prices:
        errors.append("nonpositive_prices")
    if negative_volume:
        errors.append("negative_volume")
    if missing_volume:
        warnings.append("missing_volume")
    if unsorted:
        warnings.append("dates_not_sorted_but_sortable")

    requested_days = max((end_day - start_day).days + 1, 1)
    return {
        "symbol": symbol.upper(),
        "path": str(path),
        "exists": path.exists(),
        "readable": True,
        "template": False,
        "valid": not errors,
        "errors": sorted(set(errors)),
        "warnings": sorted(set(warnings)),
        "row_count": len(usable_dates),
        "first_date": min(usable_dates) if usable_dates else None,
        "last_date": max(usable_dates) if usable_dates else None,
        "coverage_ratio": len(set(usable_dates)) / requested_days,
    }
