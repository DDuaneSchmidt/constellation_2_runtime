from __future__ import annotations

from collections import defaultdict
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.datasets.dataset_snapshot import recompute_dataset_content_hash


def validate_dataset_manifest(snapshot: dict) -> dict:
    validate_contract("dataset_snapshot", snapshot)
    expected = snapshot.get("content_hash")
    actual = recompute_dataset_content_hash(snapshot)
    return {
        "valid": expected == actual,
        "expected_content_hash": expected,
        "actual_content_hash": actual,
        "schema_valid": True,
    }


def _missing(value: Any) -> bool:
    return value is None or value == ""


def _is_nonpositive(value: Any) -> bool:
    return value is not None and value != "" and float(value) <= 0


def _is_negative(value: Any) -> bool:
    return value is not None and value != "" and float(value) < 0


def validate_ohlcv_records(
    rows: list[dict[str, Any]],
    *,
    symbols_requested: list[str],
    canonical_written: bool,
    minimum_rows_per_symbol: int = 20,
    adjustment_policy: str = "provider_adjusted_close",
) -> dict[str, Any]:
    issues: dict[str, Any] = {
        "duplicate_symbol_date_rows": [],
        "missing_ohlc_rows": [],
        "missing_adj_close_rows": [],
        "missing_volume_rows": [],
        "high_lt_low_rows": [],
        "open_outside_high_low_rows": [],
        "close_outside_high_low_rows": [],
        "nonpositive_ohlc_rows": [],
        "negative_volume_rows": [],
        "insufficient_history_symbols": [],
        "symbols_with_no_data": [],
        "extreme_one_day_adjusted_close_returns": [],
        "stale_repeated_close_sequences": [],
    }
    row_count_by_symbol: dict[str, int] = defaultdict(int)
    first_last_by_symbol: dict[str, dict[str, str | None]] = {}
    seen: set[tuple[str, str]] = set()
    loaded_symbols: set[str] = set()
    invalid_ohlc = False

    for index, row in enumerate(rows):
        symbol = str(row.get("symbol") or "").upper()
        day = str(row.get("date") or "")
        loaded_symbols.add(symbol)
        row_count_by_symbol[symbol] += 1
        first_last = first_last_by_symbol.setdefault(symbol, {"first_date": day, "last_date": day})
        first_last["first_date"] = min(str(first_last["first_date"]), day)
        first_last["last_date"] = max(str(first_last["last_date"]), day)
        key = (symbol, day)
        if key in seen:
            issues["duplicate_symbol_date_rows"].append({"symbol": symbol, "date": day})
        seen.add(key)

        ohlc = [row.get("open"), row.get("high"), row.get("low"), row.get("close")]
        if any(_missing(value) for value in ohlc):
            issues["missing_ohlc_rows"].append({"symbol": symbol, "date": day, "row_index": index})
        if _missing(row.get("adj_close")):
            issues["missing_adj_close_rows"].append({"symbol": symbol, "date": day, "row_index": index})
        if _missing(row.get("volume")):
            issues["missing_volume_rows"].append({"symbol": symbol, "date": day, "row_index": index})
        if any(_is_nonpositive(value) for value in ohlc):
            issues["nonpositive_ohlc_rows"].append({"symbol": symbol, "date": day, "row_index": index})
            invalid_ohlc = True
        if _is_negative(row.get("volume")):
            issues["negative_volume_rows"].append({"symbol": symbol, "date": day, "row_index": index})
        high = row.get("high")
        low = row.get("low")
        open_ = row.get("open")
        close = row.get("close")
        if high is not None and low is not None and float(high) < float(low):
            issues["high_lt_low_rows"].append({"symbol": symbol, "date": day, "row_index": index})
            invalid_ohlc = True
        if high is not None and low is not None and open_ is not None and not (float(low) <= float(open_) <= float(high)):
            issues["open_outside_high_low_rows"].append({"symbol": symbol, "date": day, "row_index": index})
            invalid_ohlc = True
        if high is not None and low is not None and close is not None and not (float(low) <= float(close) <= float(high)):
            issues["close_outside_high_low_rows"].append({"symbol": symbol, "date": day, "row_index": index})
            invalid_ohlc = True

    for symbol in symbols_requested:
        if symbol not in loaded_symbols:
            issues["symbols_with_no_data"].append(symbol)
    for symbol, count in sorted(row_count_by_symbol.items()):
        if count < minimum_rows_per_symbol:
            issues["insufficient_history_symbols"].append(
                {"symbol": symbol, "row_count": count, "minimum_rows": minimum_rows_per_symbol}
            )

    by_symbol_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_symbol_rows[str(row.get("symbol") or "").upper()].append(row)
    for symbol, symbol_rows in by_symbol_rows.items():
        sorted_rows = sorted(symbol_rows, key=lambda row: str(row.get("date") or ""))
        repeated_close_count = 1
        previous_close = None
        previous_adj_close = None
        for row in sorted_rows:
            close = row.get("close")
            adj_close = row.get("adj_close")
            if close == previous_close and close is not None:
                repeated_close_count += 1
            else:
                repeated_close_count = 1
            if repeated_close_count >= 5:
                issues["stale_repeated_close_sequences"].append({"symbol": symbol, "date": row.get("date"), "length": repeated_close_count})
            if previous_adj_close and adj_close:
                ret = (float(adj_close) / float(previous_adj_close)) - 1.0
                if abs(ret) >= 0.5:
                    issues["extreme_one_day_adjusted_close_returns"].append(
                        {"symbol": symbol, "date": row.get("date"), "adjusted_close_return": ret}
                    )
            previous_close = close
            previous_adj_close = adj_close

    fail_reasons: list[str] = []
    warning_reasons: list[str] = []
    if not rows:
        fail_reasons.append("no_data")
    if issues["duplicate_symbol_date_rows"]:
        fail_reasons.append("duplicate_symbol_date_rows")
    if invalid_ohlc or issues["missing_ohlc_rows"]:
        fail_reasons.append("invalid_ohlc_structure")
    if len(issues["symbols_with_no_data"]) == len(symbols_requested):
        fail_reasons.append("all_symbols_missing")
    if not canonical_written:
        fail_reasons.append("canonical_parquet_not_written")
    if issues["symbols_with_no_data"]:
        warning_reasons.append("some_symbols_missing")
    if issues["insufficient_history_symbols"]:
        warning_reasons.append("some_symbols_have_short_history")
    if issues["extreme_one_day_adjusted_close_returns"]:
        warning_reasons.append("extreme_return_flags")
    if issues["missing_adj_close_rows"] or issues["missing_volume_rows"]:
        warning_reasons.append("missing_adj_close_or_volume")
    if adjustment_policy == "unadjusted_close_as_adj_close":
        warning_reasons.append("adj_close_derived_from_close")

    if fail_reasons:
        quality_status = "fail"
    elif warning_reasons:
        quality_status = "pass_with_warnings"
    else:
        quality_status = "pass"

    return {
        "quality_status": quality_status,
        "fail_reasons": fail_reasons,
        "warning_reasons": warning_reasons,
        "checks": issues,
        "row_count": len(rows),
        "row_count_by_symbol": dict(sorted(row_count_by_symbol.items())),
        "first_last_date_by_symbol": dict(sorted(first_last_by_symbol.items())),
        "symbols_requested": symbols_requested,
        "symbols_loaded": sorted(loaded_symbols),
        "symbols_missing": issues["symbols_with_no_data"],
        "canonical_written": canonical_written,
        "adjustment_policy": adjustment_policy,
        "schema_version": "ohlcv_quality_report.v1",
    }
