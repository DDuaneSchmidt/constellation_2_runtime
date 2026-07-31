from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .direct_candidate_data_validation import run_direct_candidate_data_validation
from .local_market_data_import import run_market_data_import_report
from .market_data_coverage_report import run_market_data_coverage_report
from .market_data_schema_validation import NORMALIZED_COLUMNS

REPORT_DIRNAME = "manual_intraday_csv_intake"
MANUAL_IMPORT_DIR = Path("data/manual_intraday_import")
SUPPORTED_SYMBOLS = {"DIA", "QQQ", "SPY"}
SUPPORTED_TIMEFRAMES = {"1m", "5m", "30m"}
PRIORITY_1_OUTPUTS = {
    ("DIA", "5m"),
    ("DIA", "30m"),
    ("QQQ", "5m"),
    ("QQQ", "30m"),
    ("SPY", "5m"),
    ("SPY", "30m"),
}
TIMESTAMP_COLUMNS = ("timestamp", "date", "datetime", "time")
OPEN_COLUMNS = ("open", "adjopen")
HIGH_COLUMNS = ("high", "adjhigh")
LOW_COLUMNS = ("low", "adjlow")
CLOSE_COLUMNS = ("close", "adjclose")
VOLUME_COLUMNS = ("volume", "adjvolume")
ADJUSTED_CLOSE_COLUMNS = ("adjusted_close", "adjustedclose", "adjclose")
AUTHORITY_BOUNDARY = {
    "manual_csv_intake_only": True,
    "historical_data_only": True,
    "external_api_calls_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "broker_endpoint_allowed": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "trade_recommendation_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}


def discover_manual_intraday_csvs(base_dir: str | Path | None = None) -> list[dict[str, Any]]:
    root = Path(base_dir or Path.cwd())
    directory = root / MANUAL_IMPORT_DIR
    directory.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.csv")):
        symbol, timeframe = _infer_manual_filename(path)
        rows.append(
            {
                "path": str(path),
                "filename": path.name,
                "symbol": symbol,
                "timeframe": timeframe,
                "supported_filename": symbol in SUPPORTED_SYMBOLS and timeframe in SUPPORTED_TIMEFRAMES,
            }
        )
    return rows


def validate_manual_intraday_schema(path: str | Path, *, symbol: str | None = None, timeframe: str | None = None) -> dict[str, Any]:
    file_path = Path(path)
    inferred_symbol, inferred_timeframe = _infer_manual_filename(file_path)
    symbol_value = (symbol or inferred_symbol).upper()
    timeframe_value = _normalize_timeframe(timeframe or inferred_timeframe)
    errors: list[str] = []
    warnings: list[str] = []
    if symbol_value not in SUPPORTED_SYMBOLS:
        errors.append(f"Unsupported or wrong symbol in filename: {symbol_value}")
    if timeframe_value not in SUPPORTED_TIMEFRAMES:
        errors.append(f"Unsupported timeframe in filename: {timeframe_value}")
    try:
        rows, row_warnings = normalize_manual_intraday_csv(file_path, symbol=symbol_value, timeframe=timeframe_value)
        warnings.extend(row_warnings)
    except ValueError as exc:
        rows = []
        errors.append(str(exc))
    if len(rows) < 30:
        errors.append(f"Fewer than 30 rows: {len(rows)}")
    return {
        "path": str(file_path),
        "filename": file_path.name,
        "symbol": symbol_value,
        "timeframe": timeframe_value,
        "status": "PASS" if not errors else "REJECTED",
        "row_count": len(rows),
        "date_start": rows[0]["timestamp"] if rows else "",
        "date_end": rows[-1]["timestamp"] if rows else "",
        "normalized_columns": list(NORMALIZED_COLUMNS),
        "errors": errors,
        "warnings": sorted(set(warnings)),
    }


def normalize_manual_intraday_csv(path: str | Path, *, symbol: str, timeframe: str) -> tuple[list[dict[str, Any]], list[str]]:
    file_path = Path(path)
    with file_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV header row is missing.")
        columns = {column.lower(): column for column in reader.fieldnames}
        timestamp_col = _first_column(columns, TIMESTAMP_COLUMNS)
        open_col = _first_column(columns, OPEN_COLUMNS)
        high_col = _first_column(columns, HIGH_COLUMNS)
        low_col = _first_column(columns, LOW_COLUMNS)
        close_col = _first_column(columns, CLOSE_COLUMNS)
        volume_col = _first_column(columns, VOLUME_COLUMNS)
        adjusted_close_col = _first_column(columns, ADJUSTED_CLOSE_COLUMNS)
        symbol_col = columns.get("symbol")
        missing = [
            label
            for label, col in [
                ("timestamp/date/datetime", timestamp_col),
                ("open", open_col),
                ("high", high_col),
                ("low", low_col),
                ("close", close_col),
            ]
            if not col
        ]
        if missing:
            raise ValueError("Missing required columns: " + ", ".join(missing))
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        parsed_datetimes: list[datetime] = []
        previous_timestamp = ""
        seen_timestamps: set[str] = set()
        for index, raw in enumerate(reader, start=2):
            if symbol_col and raw.get(symbol_col) and raw.get(symbol_col, "").strip().upper() != symbol:
                raise ValueError(f"Wrong symbol in row {index}: {raw.get(symbol_col)}")
            timestamp_text = str(raw.get(timestamp_col, "")).strip()
            parsed = _parse_timestamp(timestamp_text)
            if parsed is None:
                raise ValueError(f"Invalid timestamp in row {index}: {timestamp_text}")
            timestamp = _format_timestamp(parsed, timestamp_text)
            if timestamp in seen_timestamps:
                raise ValueError(f"Duplicate timestamp: {timestamp}")
            if previous_timestamp and timestamp < previous_timestamp:
                raise ValueError("Unsorted timestamps detected.")
            seen_timestamps.add(timestamp)
            previous_timestamp = timestamp
            parsed_datetimes.append(parsed)
            if parsed.tzinfo is None:
                warnings.append("timezone ambiguity")
            open_value = _float(raw.get(open_col), label="open", row=index)
            high_value = _float(raw.get(high_col), label="high", row=index)
            low_value = _float(raw.get(low_col), label="low", row=index)
            close_value = _float(raw.get(close_col), label="close", row=index)
            if high_value < low_value:
                raise ValueError(f"high < low in row {index}")
            if not (low_value <= open_value <= high_value):
                raise ValueError(f"open outside high-low range in row {index}")
            if not (low_value <= close_value <= high_value):
                raise ValueError(f"close outside high-low range in row {index}")
            volume_value = 0.0
            if volume_col:
                volume_value = _float(raw.get(volume_col), label="volume", row=index, allow_blank=True)
            else:
                warnings.append("missing volume")
            adjusted_close_value = close_value
            if adjusted_close_col:
                adjusted_close_value = _float(raw.get(adjusted_close_col), label="adjusted_close", row=index, allow_blank=True) or close_value
            rows.append(
                {
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "open": open_value,
                    "high": high_value,
                    "low": low_value,
                    "close": close_value,
                    "volume": volume_value,
                    "adjusted_close": adjusted_close_value,
                    "source_file": str(file_path),
                }
            )
    warnings.extend(_gap_warnings(parsed_datetimes, timeframe))
    if parsed_datetimes and (max(parsed_datetimes) - min(parsed_datetimes)).days < 30:
        warnings.append("short date range")
    warnings.append("regular-hours vs extended-hours unknown")
    return rows, warnings


def resample_1m_to_5m_and_30m(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return {"5m": _resample_rows(rows, minutes=5), "30m": _resample_rows(rows, minutes=30)}


def write_normalized_priority1_files(
    accepted_files: list[dict[str, Any]],
    *,
    base_dir: str | Path | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    root = Path(base_dir or Path.cwd())
    out_dir = root / "data" / "cache"
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[dict[str, Any]] = []
    resampled: list[dict[str, Any]] = []
    for item in accepted_files:
        symbol = item["symbol"]
        timeframe = item["timeframe"]
        rows, warnings = normalize_manual_intraday_csv(item["path"], symbol=symbol, timeframe=timeframe)
        if timeframe == "1m":
            for target_timeframe, target_rows in resample_1m_to_5m_and_30m(rows).items():
                if (symbol, target_timeframe) not in PRIORITY_1_OUTPUTS:
                    continue
                target = out_dir / f"{symbol}_{target_timeframe}.csv"
                _write_rows(target, target_rows)
                resampled.append({"source_file": item["path"], "target_csv_path": str(target), "symbol": symbol, "timeframe": target_timeframe, "rows_written": len(target_rows), "warnings": warnings})
                written.append({"target_csv_path": str(target), "symbol": symbol, "timeframe": target_timeframe, "rows_written": len(target_rows), "source_file": item["path"], "source_timeframe": "1m"})
        elif (symbol, timeframe) in PRIORITY_1_OUTPUTS:
            target = out_dir / f"{symbol}_{timeframe}.csv"
            _write_rows(target, rows)
            written.append({"target_csv_path": str(target), "symbol": symbol, "timeframe": timeframe, "rows_written": len(rows), "source_file": item["path"], "source_timeframe": timeframe})
    written.sort(key=lambda row: (row["symbol"], row["timeframe"], row["target_csv_path"]))
    resampled.sort(key=lambda row: (row["symbol"], row["timeframe"], row["target_csv_path"]))
    return written, resampled


def rerun_priority1_candidate_validation(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    return {
        "market_data_import": run_market_data_import_report(root=root, created_at=created_at).get("summary", {}),
        "market_data_coverage": run_market_data_coverage_report(root=root, created_at=created_at).get("summary", {}),
        "direct_candidate_data_validation": run_direct_candidate_data_validation(root=root, created_at=created_at).get("summary", {}),
    }


def run_manual_intraday_csv_intake(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    base_dir: str | Path | None = None,
) -> dict[str, Any]:
    created = created_at or _now()
    discovered = discover_manual_intraday_csvs(base_dir=base_dir)
    validations = [validate_manual_intraday_schema(item["path"], symbol=item["symbol"], timeframe=item["timeframe"]) for item in discovered]
    accepted = [row for row in validations if row["status"] == "PASS"]
    rejected = [row for row in validations if row["status"] != "PASS"]
    written, resampled = write_normalized_priority1_files(accepted, base_dir=base_dir)
    refresh = rerun_priority1_candidate_validation(root=root, created_at=created)
    coverage = _latest_json(Path(root) / "market_data_import" / "latest_coverage.json")
    direct = _latest_json(Path(root) / "direct_candidate_data_validation" / "latest.json")
    candidate_1 = _candidate_readiness(coverage, direct, "ptc_backtest_final_469607b8340421b7")
    candidate_2 = _candidate_readiness(coverage, direct, "ptc_backtest_final_3a4ac24107c77136")
    status_counts = Counter(row["status"] for row in validations)
    report = {
        "schema_id": "atlas_v2_research_os_manual_intraday_csv_intake",
        "schema_version": "1.0",
        "report_type": "MANUAL_INTRADAY_CSV_INTAKE",
        "created_at": created,
        "day": created[:10],
        "manual_import_dir": str(Path(base_dir or Path.cwd()) / MANUAL_IMPORT_DIR),
        "accepted_drop_filenames": [f"{symbol}_{tf}.csv" for symbol in ["DIA", "QQQ", "SPY"] for tf in ["1m", "5m", "30m"]],
        "files_discovered": discovered,
        "schema_validations": validations,
        "normalized_files_written": written,
        "resampled_files": resampled,
        "post_intake_refresh": refresh,
        "candidate_1_readiness": candidate_1,
        "candidate_2_readiness": candidate_2,
        "remaining_blockers": sorted(set((candidate_1.get("blockers") or []) + (candidate_2.get("blockers") or []))),
        "summary": {
            "files_discovered": len(discovered),
            "files_accepted": len(accepted),
            "files_rejected": len(rejected),
            "files_resampled": len(resampled),
            "normalized_files_written": len(written),
            "status_counts": dict(status_counts),
            "schema_warnings": sorted({warning for row in validations for warning in row.get("warnings", [])}),
            "candidate_1_status": candidate_1.get("coverage_status") or candidate_1.get("classification"),
            "candidate_2_status": candidate_2.get("coverage_status") or candidate_2.get("classification"),
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Manual historical CSV intake only.",
            "No external API calls.",
            "No vendor checkout or download automation.",
            "No broker access.",
            "No live trading.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper trade placement.",
            "No candidate production promotion.",
        ],
    }
    write_manual_intraday_csv_intake_report(report, root=root)
    return report


def write_manual_intraday_csv_intake_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "manual_intraday_csv_intake_report.json"
    summary_path = out_dir / "manual_intraday_csv_intake_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_manual_intraday_csv_intake_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_manual_intraday_csv_intake_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Manual Intraday CSV Intake",
        "",
        f"Manual import folder: `{report.get('manual_import_dir')}`",
        f"Files discovered: {summary.get('files_discovered')}",
        f"Files accepted: {summary.get('files_accepted')}",
        f"Files rejected: {summary.get('files_rejected')}",
        f"Files resampled: {summary.get('files_resampled')}",
        f"Normalized files written: {summary.get('normalized_files_written')}",
        f"Candidate 1 readiness: {summary.get('candidate_1_status')}",
        f"Candidate 2 readiness: {summary.get('candidate_2_status')}",
        "",
        "## Normalized Files",
    ]
    for row in report.get("normalized_files_written", []):
        lines.append(f"- `{row['target_csv_path']}` rows={row['rows_written']} source=`{row['source_file']}`")
    lines.extend(["", "## Rejections"])
    for row in report.get("schema_validations", []):
        if row.get("status") != "PASS":
            lines.append(f"- `{row['filename']}`: {'; '.join(row.get('errors') or [])}")
    lines.extend(["", "Authority: manual historical CSV intake only; no live/capital/broker/position-sizing authority.", ""])
    return "\n".join(lines)


def _candidate_readiness(coverage: dict[str, Any], direct: dict[str, Any], candidate_id: str) -> dict[str, Any]:
    coverage_row = next((row for row in coverage.get("candidate_coverage", []) if row.get("candidate_id") == candidate_id), {})
    direct_row = next((row for row in direct.get("candidate_validations", []) if row.get("candidate_id") == candidate_id), {})
    return {
        "candidate_id": candidate_id,
        "coverage_status": coverage_row.get("coverage_status", "UNKNOWN"),
        "classification": direct_row.get("classification", "UNKNOWN"),
        "required_symbols": coverage_row.get("required_symbols", []),
        "required_timeframes": coverage_row.get("required_timeframes", []),
        "available_symbols": coverage_row.get("available_symbols", []),
        "available_timeframes": coverage_row.get("available_timeframes", []),
        "missing_symbols": coverage_row.get("missing_symbols", []),
        "missing_timeframes": coverage_row.get("missing_timeframes", []),
        "blockers": coverage_row.get("blockers", []),
        "direct_result": direct_row.get("direct_result"),
    }


def _infer_manual_filename(path: str | Path) -> tuple[str, str]:
    stem = Path(path).stem
    parts = stem.split("_")
    if len(parts) != 2:
        return stem.upper(), ""
    return parts[0].upper(), _normalize_timeframe(parts[1])


def _normalize_timeframe(value: Any) -> str:
    text = str(value or "").strip().lower()
    return {"1min": "1m", "5min": "5m", "30min": "30m"}.get(text, text)


def _first_column(columns: dict[str, str], options: tuple[str, ...]) -> str:
    for option in options:
        if option.lower() in columns:
            return columns[option.lower()]
    return ""


def _parse_timestamp(value: str) -> datetime | None:
    text = value.strip().replace("Z", "+00:00")
    if not text:
        return None
    for candidate in [text, text.replace(" ", "T")]:
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            pass
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S"]:
        try:
            return datetime.strptime(value.strip(), fmt)
        except ValueError:
            pass
    return None


def _format_timestamp(parsed: datetime, original: str) -> str:
    if parsed.tzinfo is not None:
        return parsed.astimezone(UTC).replace(tzinfo=None).isoformat()
    if "T" in original:
        return parsed.isoformat()
    return parsed.isoformat()


def _float(value: Any, *, label: str, row: int, allow_blank: bool = False) -> float:
    text = str(value if value is not None else "").strip()
    if allow_blank and not text:
        return 0.0
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(f"Non-numeric {label} in row {row}") from exc


def _gap_warnings(parsed: list[datetime], timeframe: str) -> list[str]:
    if len(parsed) < 2:
        return []
    expected = {"1m": 1, "5m": 5, "30m": 30}.get(timeframe, 0)
    if not expected:
        return []
    max_gap = max((parsed[index] - parsed[index - 1] for index in range(1, len(parsed))), default=timedelta())
    if max_gap > timedelta(minutes=expected * 5):
        return ["large time gaps"]
    return []


def _resample_rows(rows: list[dict[str, Any]], *, minutes: int) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        parsed = _parse_timestamp(str(row["timestamp"]))
        if parsed is None:
            continue
        floored_minute = (parsed.minute // minutes) * minutes
        bucket_dt = parsed.replace(minute=floored_minute, second=0, microsecond=0)
        bucket = bucket_dt.isoformat()
        buckets.setdefault(bucket, []).append(row)
    output: list[dict[str, Any]] = []
    timeframe = f"{minutes}m"
    for timestamp in sorted(buckets):
        bucket_rows = buckets[timestamp]
        output.append(
            {
                "timestamp": timestamp,
                "symbol": bucket_rows[0]["symbol"],
                "timeframe": timeframe,
                "open": bucket_rows[0]["open"],
                "high": max(row["high"] for row in bucket_rows),
                "low": min(row["low"] for row in bucket_rows),
                "close": bucket_rows[-1]["close"],
                "volume": sum(row["volume"] for row in bucket_rows),
                "adjusted_close": bucket_rows[-1]["adjusted_close"],
                "source_file": bucket_rows[0]["source_file"],
            }
        )
    return output


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume", "adjusted_close", "source_file"])
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row[key] for key in ["timestamp", "open", "high", "low", "close", "volume", "adjusted_close", "source_file"]})


def _latest_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return _now()[:10]
