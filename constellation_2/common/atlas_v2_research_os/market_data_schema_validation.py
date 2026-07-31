from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any

from .market_data_import_models import LocalMarketDataFile, MarketDataSchemaValidationResult

TIMESTAMP_COLUMNS = ("timestamp", "date", "datetime", "time", "Date", "Datetime", "Timestamp")
OPEN_COLUMNS = ("open", "Open", "adjOpen")
HIGH_COLUMNS = ("high", "High", "adjHigh")
LOW_COLUMNS = ("low", "Low", "adjLow")
CLOSE_COLUMNS = ("close", "Close", "adjClose")
VOLUME_COLUMNS = ("volume", "Volume", "adjVolume")
ADJUSTED_CLOSE_COLUMNS = ("adjusted_close", "adjustedClose", "adjClose", "Adj Close", "AdjClose")
NORMALIZED_COLUMNS = ["timestamp", "symbol", "timeframe", "open", "high", "low", "close", "volume", "adjusted_close", "source_file"]


def infer_symbol_and_timeframe_from_filename(path: str | Path) -> tuple[str, str]:
    name = Path(path).name
    stem = Path(name).stem
    upper_stem = stem.upper()
    if upper_stem.endswith("_TIINGO_ADJUSTED_DAILY"):
        return upper_stem.removesuffix("_TIINGO_ADJUSTED_DAILY"), "daily"
    if upper_stem.endswith("_DAILY"):
        return upper_stem.removesuffix("_DAILY"), "daily"
    parts = stem.split("_")
    if len(parts) >= 2 and _looks_like_timeframe(parts[-1]):
        return "_".join(parts[:-1]).upper(), parts[-1].lower()
    return stem.upper(), "daily"


def validate_market_data_schema(file: LocalMarketDataFile | str | Path) -> MarketDataSchemaValidationResult:
    local_file = _coerce_file(file)
    path = Path(local_file.path)
    errors: list[str] = []
    warnings: list[str] = []
    if not path.exists():
        return MarketDataSchemaValidationResult(path=str(path), symbol=local_file.symbol, timeframe=local_file.timeframe, status="MISSING", errors=["File does not exist."])
    try:
        rows = normalize_market_data_csv(path, symbol=local_file.symbol, timeframe=local_file.timeframe)
    except ValueError as exc:
        return MarketDataSchemaValidationResult(path=str(path), symbol=local_file.symbol, timeframe=local_file.timeframe, status="SCHEMA_INVALID", errors=[str(exc)])
    except csv.Error as exc:
        return MarketDataSchemaValidationResult(path=str(path), symbol=local_file.symbol, timeframe=local_file.timeframe, status="SCHEMA_INVALID", errors=[f"CSV parse error: {exc}"])
    if not rows:
        errors.append("No valid market data rows found.")
    timestamps = [row["timestamp"] for row in rows]
    status = "PASS" if rows and not errors else "SCHEMA_INVALID"
    if len(set(timestamps)) != len(timestamps):
        warnings.append("Duplicate timestamps detected.")
    return MarketDataSchemaValidationResult(
        path=str(path),
        symbol=local_file.symbol,
        timeframe=local_file.timeframe,
        status=status,
        row_count=len(rows),
        date_start=min(timestamps) if timestamps else "",
        date_end=max(timestamps) if timestamps else "",
        normalized_columns=list(NORMALIZED_COLUMNS),
        errors=errors,
        warnings=warnings,
    )


def normalize_market_data_csv(path: str | Path, *, symbol: str | None = None, timeframe: str | None = None) -> list[dict[str, Any]]:
    file_path = Path(path)
    inferred_symbol, inferred_timeframe = infer_symbol_and_timeframe_from_filename(file_path)
    symbol_value = (symbol or inferred_symbol).upper()
    timeframe_value = (timeframe or inferred_timeframe).lower()
    with file_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV header row is missing.")
        columns = set(reader.fieldnames)
        timestamp_col = _first_present(columns, TIMESTAMP_COLUMNS)
        open_col = _first_present(columns, OPEN_COLUMNS)
        high_col = _first_present(columns, HIGH_COLUMNS)
        low_col = _first_present(columns, LOW_COLUMNS)
        close_col = _first_present(columns, CLOSE_COLUMNS)
        volume_col = _first_present(columns, VOLUME_COLUMNS)
        adjusted_close_col = _first_present(columns, ADJUSTED_CLOSE_COLUMNS)
        missing = [
            label
            for label, col in [
                ("timestamp/date/datetime", timestamp_col),
                ("open", open_col),
                ("high", high_col),
                ("low", low_col),
                ("close", close_col),
                ("volume", volume_col),
            ]
            if not col
        ]
        if missing:
            raise ValueError("Missing required columns: " + ", ".join(missing))
        rows: list[dict[str, Any]] = []
        for raw in reader:
            timestamp = _normalize_timestamp(raw.get(timestamp_col, ""))
            if not timestamp:
                continue
            try:
                rows.append(
                    {
                        "timestamp": timestamp,
                        "symbol": symbol_value,
                        "timeframe": timeframe_value,
                        "open": float(raw.get(open_col, "")),
                        "high": float(raw.get(high_col, "")),
                        "low": float(raw.get(low_col, "")),
                        "close": float(raw.get(close_col, "")),
                        "volume": float(raw.get(volume_col, "") or 0.0),
                        "adjusted_close": float(raw.get(adjusted_close_col, "") or raw.get(close_col, "")),
                        "source_file": str(file_path),
                    }
                )
            except (TypeError, ValueError):
                continue
    rows.sort(key=lambda row: row["timestamp"])
    return rows


def _coerce_file(file: LocalMarketDataFile | str | Path) -> LocalMarketDataFile:
    if isinstance(file, LocalMarketDataFile):
        return file
    symbol, timeframe = infer_symbol_and_timeframe_from_filename(file)
    path = Path(file)
    return LocalMarketDataFile(path=str(path), symbol=symbol, timeframe=timeframe, filename=path.name, source_root=str(path.parent))


def _first_present(columns: set[str], options: tuple[str, ...]) -> str:
    for option in options:
        if option in columns:
            return option
    lowered = {column.lower(): column for column in columns}
    for option in options:
        if option.lower() in lowered:
            return lowered[option.lower()]
    return ""


def _normalize_timestamp(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).isoformat()
    except ValueError:
        return text


def _looks_like_timeframe(value: str) -> bool:
    lower = value.lower()
    return lower in {"daily", "day", "1d"} or lower.endswith(("m", "min", "h"))
