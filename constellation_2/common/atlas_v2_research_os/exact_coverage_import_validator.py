from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .artifact_store import DEFAULT_STORE_ROOT
from .exact_coverage_import_specification import ACCEPTED_ALIASES, CANONICAL_COLUMNS

REPORT_DIRNAME = "exact_coverage_import_validator"
SPEC_DIRNAME = "exact_coverage_import_specification"
DEFAULT_IMPORT_DIR = Path("data/manual_intraday_import")
DEFAULT_CACHE_DIR = Path("data/cache/exact_coverage_import_validator")

IMPORT_VALIDATION_MATRIX_COLUMNS = [
    "expected_filename",
    "actual_file",
    "priority",
    "family_id",
    "candidate_id",
    "symbol",
    "timeframe",
    "required_start",
    "required_end",
    "rows_found",
    "data_start",
    "data_end",
    "required_columns_present",
    "aliases_used",
    "timestamp_parseable",
    "timezone_status",
    "ohlcv_numeric",
    "ohlc_integrity_passed",
    "duplicate_timestamp_count",
    "sorted_ascending",
    "date_range_overlap",
    "timeframe_interval_match",
    "gap_count",
    "session_coverage_status",
    "validation_status",
    "failure_reasons",
]

NORMALIZED_FILE_MANIFEST_COLUMNS = [
    "symbol",
    "timeframe",
    "source_file",
    "normalized_file",
    "rows_written",
    "data_start",
    "data_end",
    "warnings",
]

GAP_REPORT_COLUMNS = ["symbol", "timeframe", "gap_start", "gap_end", "gap_minutes", "severity", "notes"]
REJECTED_FILES_COLUMNS = ["actual_file", "expected_filename", "symbol", "timeframe", "rejection_reason", "failed_rules"]
FORBIDDEN_ACTIONS = [
    "live trading",
    "broker execution",
    "capital allocation",
    "position sizing",
    "trade recommendations",
    "automatic paper placement",
    "candidate promotion",
    "production promotion",
]


def run_exact_coverage_import_validator(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    import_dir: str | Path = DEFAULT_IMPORT_DIR,
    cache_dir: str | Path = DEFAULT_CACHE_DIR,
) -> dict[str, Any]:
    report = build_exact_coverage_import_validator(
        root=root,
        created_at=created_at,
        import_dir=import_dir,
        cache_dir=cache_dir,
    )
    write_exact_coverage_import_validator(report, root=root)
    return report


def build_exact_coverage_import_validator(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    import_dir: str | Path = DEFAULT_IMPORT_DIR,
    cache_dir: str | Path = DEFAULT_CACHE_DIR,
) -> dict[str, Any]:
    root_path = Path(root)
    spec_dir = root_path / SPEC_DIRNAME
    required_rows = _required_rows(root_path, spec_dir)
    contract_rows = _read_csv(spec_dir / "import_contract.csv")
    validation_rules = _read_csv(spec_dir / "validation_rules.csv")
    contract_by_filename = {row.get("expected_filename", ""): row for row in contract_rows}
    rule_actions = {row.get("rule_id", ""): row.get("failure_action", "") for row in validation_rules}
    rule_severities = {row.get("rule_id", ""): row.get("severity", "") for row in validation_rules}

    import_path = Path(import_dir)
    cache_path = Path(cache_dir)
    actual_files = {path.name: path for path in sorted(import_path.glob("*.csv"))} if import_path.exists() else {}

    matrix_rows: list[dict[str, str]] = []
    manifest_rows: list[dict[str, str]] = []
    gap_rows: list[dict[str, str]] = []
    rejected_rows: list[dict[str, str]] = []

    for required in required_rows:
        expected = required.get("expected_filename", "")
        actual_path = actual_files.get(expected)
        if actual_path is None:
            matrix_rows.append(_missing_matrix_row(required))
            continue
        result = _validate_one_file(
            required=required,
            contract=contract_by_filename.get(expected, {}),
            actual_path=actual_path,
            cache_dir=cache_path,
            rule_actions=rule_actions,
            rule_severities=rule_severities,
        )
        matrix_rows.append(result["matrix_row"])
        gap_rows.extend(result["gap_rows"])
        if result["manifest_row"]:
            manifest_rows.append(result["manifest_row"])
        if result["rejected_row"]:
            rejected_rows.append(result["rejected_row"])

    status_counts = Counter(row["validation_status"] for row in matrix_rows)
    ready_rows = [row for row in matrix_rows if row["validation_status"] in {"VALID_READY", "VALID_WITH_WARNINGS"}]
    summary = {
        "required_files": len(required_rows),
        "files_found": sum(1 for row in matrix_rows if row["actual_file"]),
        "valid_ready": status_counts.get("VALID_READY", 0),
        "valid_with_warnings": status_counts.get("VALID_WITH_WARNINGS", 0),
        "rejected": status_counts.get("REJECTED", 0),
        "missing": status_counts.get("MISSING_FILE", 0),
        "import_ready_gap_rows": len(ready_rows),
        "confidence_impact": "NONE",
        "confidence_impact_reason": "This build validates data readiness only and does not test market evidence.",
    }
    return {
        "schema_id": "atlas_v2_research_os_exact_coverage_import_validator",
        "schema_version": "1.0",
        "report_type": "EXACT_COVERAGE_IMPORT_VALIDATOR",
        "created_at": created_at or _now(),
        "day": (created_at or _now())[:10],
        "inputs": {
            "required_exact_files": str(spec_dir / "required_exact_files.csv"),
            "import_contract": str(spec_dir / "import_contract.csv"),
            "validation_rules": str(spec_dir / "validation_rules.csv"),
            "manual_import_dir": str(import_path),
            "normalized_cache_dir": str(cache_path),
        },
        "summary": summary,
        "import_ready_gaps": [
            {
                "priority": row["priority"],
                "family_id": row["family_id"],
                "candidate_id": row["candidate_id"],
                "symbol": row["symbol"],
                "timeframe": row["timeframe"],
                "required_start": row["required_start"],
                "required_end": row["required_end"],
                "validation_status": row["validation_status"],
            }
            for row in ready_rows
        ],
        "import_validation_matrix": matrix_rows,
        "normalized_file_manifest": manifest_rows,
        "gap_report": gap_rows,
        "rejected_files": rejected_rows,
        "guardrails": [
            "Validates and normalizes manually supplied intraday CSV files only.",
            "Does not run exact replay or candidate validation.",
            "Does not promote candidates, production state, or methodology confidence.",
        ],
        "confidence_impact": "NONE",
        "confidence_impact_reason": "This build validates data readiness only and does not test market evidence.",
        "recommended_next_build": "Build 099 — Exact Replay Without Fallback",
        "authority_boundary": "Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
    }


def write_exact_coverage_import_validator(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "import_validation_matrix": out_dir / "import_validation_matrix.csv",
        "normalized_file_manifest": out_dir / "normalized_file_manifest.csv",
        "gap_report": out_dir / "gap_report.csv",
        "rejected_files": out_dir / "rejected_files.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_exact_coverage_import_validator_summary(report), encoding="utf-8")
    _write_csv(paths["import_validation_matrix"], IMPORT_VALIDATION_MATRIX_COLUMNS, report.get("import_validation_matrix") or [])
    _write_csv(paths["normalized_file_manifest"], NORMALIZED_FILE_MANIFEST_COLUMNS, report.get("normalized_file_manifest") or [])
    _write_csv(paths["gap_report"], GAP_REPORT_COLUMNS, report.get("gap_report") or [])
    _write_csv(paths["rejected_files"], REJECTED_FILES_COLUMNS, report.get("rejected_files") or [])
    return paths


def render_exact_coverage_import_validator_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    matrix = report.get("import_validation_matrix") or []
    manifest = report.get("normalized_file_manifest") or []
    rejected = report.get("rejected_files") or []
    gaps = report.get("gap_report") or []
    lines = [
        "# Build 098 — Exact Coverage Import Validator",
        "",
        "## Executive Summary",
        "",
        f"Required files: {summary.get('required_files')}",
        f"Files found: {summary.get('files_found')}",
        f"Import-ready gaps: {summary.get('import_ready_gap_rows')}",
        f"Rejected: {summary.get('rejected')}",
        f"Missing: {summary.get('missing')}",
        "",
        "## Inputs",
        "",
    ]
    for name, path in (report.get("inputs") or {}).items():
        lines.append(f"- {name}: `{path}`")
    lines.extend(["", "## Files Found", ""])
    for row in matrix:
        if row.get("actual_file"):
            lines.append(f"- `{row['actual_file']}` -> {row['validation_status']}")
    if not any(row.get("actual_file") for row in matrix):
        lines.append("- NONE")
    lines.extend(["", "## Files Missing", ""])
    missing = [row for row in matrix if row.get("validation_status") == "MISSING_FILE"]
    for row in missing[:50]:
        lines.append(f"- `{row['expected_filename']}` {row['priority']} {row['symbol']} {row['timeframe']}")
    if len(missing) > 50:
        lines.append(f"- ... {len(missing) - 50} additional missing rows omitted")
    if not missing:
        lines.append("- NONE")
    lines.extend(["", "## Files Validated", ""])
    validated = [row for row in matrix if row.get("validation_status") in {"VALID_READY", "VALID_WITH_WARNINGS"}]
    for row in validated:
        lines.append(f"- `{row['actual_file']}` {row['validation_status']} rows={row['rows_found']}")
    if not validated:
        lines.append("- NONE")
    lines.extend(["", "## Files Rejected", ""])
    for row in rejected:
        lines.append(f"- `{row['actual_file']}`: {row['rejection_reason']}")
    if not rejected:
        lines.append("- NONE")
    lines.extend(["", "## Gap and Session Warnings", ""])
    for row in gaps[:50]:
        lines.append(f"- {row['severity']} {row['symbol']} {row['timeframe']} {row['gap_start']} -> {row['gap_end']} ({row['gap_minutes']} minutes)")
    if len(gaps) > 50:
        lines.append(f"- ... {len(gaps) - 50} additional gap rows omitted")
    if not gaps:
        lines.append("- NONE")
    lines.extend(["", "## Normalized Output Manifest", ""])
    for row in manifest:
        lines.append(f"- `{row['normalized_file']}` rows={row['rows_written']} source=`{row['source_file']}`")
    if not manifest:
        lines.append("- NONE")
    lines.extend(
        [
            "",
            "## What This Build Does Not Do",
            "",
            "- It does not run candidate validation or exact replay.",
            "- It does not enable live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
            "- It does not increase methodology confidence.",
            "",
            "## Confidence Impact",
            "",
            "NONE",
            "",
            "Reason: This build validates data readiness only and does not test market evidence.",
            "",
            "## Authority Boundary",
            "",
            "Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def _validate_one_file(
    *,
    required: dict[str, str],
    contract: dict[str, str],
    actual_path: Path,
    cache_dir: Path,
    rule_actions: dict[str, str],
    rule_severities: dict[str, str],
) -> dict[str, Any]:
    rows = _read_csv(actual_path)
    required_columns = [value.strip() for value in (contract.get("required_columns") or ",".join(CANONICAL_COLUMNS)).split(",") if value.strip()]
    alias_map = _alias_map(contract)
    header = list(rows[0].keys()) if rows else []
    column_map = {name: alias_map.get(name.strip(), name.strip()) for name in header}
    aliases_used = {source: target for source, target in column_map.items() if source != target}
    present = set(column_map.values())
    missing_columns = [column for column in required_columns if column not in present]
    required_columns_present = not missing_columns

    normalized_rows: list[dict[str, str]] = []
    timestamps: list[datetime] = []
    failures: list[str] = []
    warnings: list[str] = []
    gap_rows: list[dict[str, str]] = []

    timezone_name = _declared_timezone(actual_path)
    timestamp_parseable = True
    timezone_status = "PRESENT"
    ohlcv_numeric = True
    ohlc_integrity_passed = True

    for raw in rows:
        normalized = {target: raw.get(source, "") for source, target in column_map.items() if target in CANONICAL_COLUMNS}
        if required_columns_present:
            parsed = _parse_timestamp(normalized.get("timestamp", ""), timezone_name)
            if parsed is None:
                timestamp_parseable = False
            else:
                if parsed[1] == "DECLARED":
                    timezone_status = "DECLARED"
                if parsed[1] == "MISSING":
                    timezone_status = "MISSING"
                timestamps.append(parsed[0])
                normalized["timestamp"] = parsed[0].isoformat()
            numeric_values: dict[str, float] = {}
            for column in ["open", "high", "low", "close", "volume"]:
                try:
                    numeric_values[column] = float(normalized.get(column, ""))
                except (TypeError, ValueError):
                    ohlcv_numeric = False
            if len(numeric_values) == 5:
                high = numeric_values["high"]
                low = numeric_values["low"]
                open_ = numeric_values["open"]
                close = numeric_values["close"]
                if high < low or high < open_ or high < close or low > open_ or low > close:
                    ohlc_integrity_passed = False
        normalized_rows.append({column: normalized.get(column, "") for column in CANONICAL_COLUMNS})

    if not required_columns_present:
        failures.append("REQUIRED_COLUMNS_PRESENT")
    if not timestamp_parseable:
        failures.append("TIMESTAMP_PARSEABLE")
    if timezone_status == "MISSING":
        failures.append("TIMESTAMP_TIMEZONE_PRESENT_OR_DECLARED")
    if not ohlcv_numeric:
        failures.extend(["OHLC_NUMERIC", "VOLUME_NUMERIC"])
    if not ohlc_integrity_passed:
        failures.extend(["HIGH_GTE_LOW", "HIGH_GTE_OPEN_CLOSE", "LOW_LTE_OPEN_CLOSE"])

    duplicate_count = len(timestamps) - len(set(timestamps))
    if duplicate_count:
        failures.append("NO_DUPLICATE_TIMESTAMPS")
    sorted_ascending = all(left < right for left, right in zip(timestamps, timestamps[1:])) if timestamps else False
    if timestamps and not sorted_ascending:
        failures.append("SORTED_ASCENDING")

    data_start = min(timestamps).isoformat() if timestamps else ""
    data_end = max(timestamps).isoformat() if timestamps else ""
    date_range_overlap = _date_ranges_overlap(timestamps, required.get("required_start", ""), required.get("required_end", ""))
    if timestamps and not date_range_overlap:
        failures.append("DATE_RANGE_OVERLAPS_REQUIREMENT")

    interval = _timeframe_minutes(required.get("timeframe", ""))
    timeframe_interval_match = True
    gap_count = 0
    if interval and len(timestamps) > 1:
        for left, right in zip(timestamps, timestamps[1:]):
            minutes = int((right - left).total_seconds() // 60)
            if minutes > interval:
                gap_count += 1
                gap_rows.append(
                    {
                        "symbol": required.get("symbol", ""),
                        "timeframe": required.get("timeframe", ""),
                        "gap_start": left.isoformat(),
                        "gap_end": right.isoformat(),
                        "gap_minutes": str(minutes),
                        "severity": rule_severities.get("GAPS_REPORTED", "WARNING") or "WARNING",
                        "notes": f"Observed interval exceeds expected {interval} minutes.",
                    }
                )
            if minutes <= 0 or minutes % interval != 0:
                timeframe_interval_match = False
    if not timeframe_interval_match:
        failures.append("TIMEFRAME_INTERVAL_MATCH")
    if gap_count:
        if rule_actions.get("GAPS_REPORTED") == "REJECT":
            failures.append("GAPS_REPORTED")
        else:
            warnings.append("GAPS_REPORTED")

    session_status = _session_status(timestamps)
    error_failures = sorted({failure for failure in failures if rule_actions.get(failure, "REJECT") == "REJECT"})
    validation_status = "REJECTED" if error_failures else ("VALID_WITH_WARNINGS" if warnings else "VALID_READY")
    normalized_file = ""
    manifest_row = None
    if validation_status in {"VALID_READY", "VALID_WITH_WARNINGS"}:
        cache_dir.mkdir(parents=True, exist_ok=True)
        normalized_path = cache_dir / actual_path.name
        _write_csv(normalized_path, CANONICAL_COLUMNS, normalized_rows)
        normalized_file = str(normalized_path)
        manifest_row = {
            "symbol": required.get("symbol", ""),
            "timeframe": required.get("timeframe", ""),
            "source_file": str(actual_path),
            "normalized_file": normalized_file,
            "rows_written": str(len(normalized_rows)),
            "data_start": data_start,
            "data_end": data_end,
            "warnings": ";".join(sorted(set(warnings))),
        }

    matrix_row = {
        "expected_filename": required.get("expected_filename", ""),
        "actual_file": str(actual_path),
        "priority": required.get("priority", ""),
        "family_id": required.get("family_id", ""),
        "candidate_id": required.get("candidate_id", ""),
        "symbol": required.get("symbol", ""),
        "timeframe": required.get("timeframe", ""),
        "required_start": required.get("required_start", ""),
        "required_end": required.get("required_end", ""),
        "rows_found": str(len(rows)),
        "data_start": data_start,
        "data_end": data_end,
        "required_columns_present": _bool(required_columns_present),
        "aliases_used": json.dumps(aliases_used, sort_keys=True),
        "timestamp_parseable": _bool(timestamp_parseable),
        "timezone_status": timezone_status,
        "ohlcv_numeric": _bool(ohlcv_numeric),
        "ohlc_integrity_passed": _bool(ohlc_integrity_passed),
        "duplicate_timestamp_count": str(duplicate_count),
        "sorted_ascending": _bool(sorted_ascending),
        "date_range_overlap": _bool(date_range_overlap),
        "timeframe_interval_match": _bool(timeframe_interval_match),
        "gap_count": str(gap_count),
        "session_coverage_status": session_status,
        "validation_status": validation_status,
        "failure_reasons": ";".join(error_failures),
    }
    rejected_row = None
    if validation_status == "REJECTED":
        rejected_row = {
            "actual_file": str(actual_path),
            "expected_filename": required.get("expected_filename", ""),
            "symbol": required.get("symbol", ""),
            "timeframe": required.get("timeframe", ""),
            "rejection_reason": ";".join(error_failures),
            "failed_rules": ";".join(error_failures),
        }
    return {"matrix_row": matrix_row, "manifest_row": manifest_row, "gap_rows": gap_rows, "rejected_row": rejected_row}


def _required_rows(root_path: Path, spec_dir: Path) -> list[dict[str, str]]:
    rows = _read_csv(spec_dir / "required_exact_files.csv")
    existing = {
        (
            row.get("candidate_id", ""),
            row.get("family_id", ""),
            row.get("symbol", ""),
            row.get("timeframe", ""),
            row.get("expected_filename", ""),
        )
        for row in rows
    }
    for row in _coverage_plan_required_rows(root_path) + _blocked_replay_required_rows(root_path):
        key = (
            row.get("candidate_id", ""),
            row.get("family_id", ""),
            row.get("symbol", ""),
            row.get("timeframe", ""),
            row.get("expected_filename", ""),
        )
        if key not in existing:
            rows.append(row)
            existing.add(key)
    return rows


def _coverage_plan_required_rows(root_path: Path) -> list[dict[str, str]]:
    required_rows: list[dict[str, str]] = []
    for row in _read_csv(root_path / "reversal_trending_exact_coverage_plan" / "coverage_matrix.csv"):
        symbol = row.get("symbol", "")
        timeframe = row.get("timeframe", "")
        if row.get("coverage_status") != "EXACT_COVERAGE_AVAILABLE":
            continue
        if _truthy(row.get("fallback_used")):
            continue
        if not row.get("available_exact_file"):
            continue
        expected_filename = f"{symbol}_{timeframe}.csv"
        required_rows.append(
            {
                "priority": row.get("priority", "P1") or "P1",
                "family_id": row.get("family_id", ""),
                "candidate_id": row.get("candidate_id", ""),
                "symbol": symbol,
                "timeframe": timeframe,
                "required_start": row.get("required_start", ""),
                "required_end": row.get("required_end", ""),
                "expected_filename": expected_filename,
                "blocks_exact_validation": "true",
                "fallback_used": "false",
                "fallback_file": row.get("available_fallback_file", ""),
                "reason": "exact coverage plan has exact symbol/timeframe file; validate without fallback",
            }
        )
    return required_rows


def _blocked_replay_required_rows(root_path: Path) -> list[dict[str, str]]:
    blocked_rows = _read_csv(root_path / "exact_replay_without_fallback" / "blocked_exact_replay.csv")
    if not blocked_rows:
        return []
    coverage_by_key = {
        (
            row.get("candidate_id", ""),
            row.get("family_id", ""),
            row.get("symbol", ""),
            row.get("timeframe", ""),
        ): row
        for row in _read_csv(root_path / "reversal_trending_exact_coverage_plan" / "coverage_matrix.csv")
    }
    required_rows: list[dict[str, str]] = []
    for row in blocked_rows:
        required_file = Path(row.get("required_file", "")).name
        if not required_file:
            continue
        symbol = row.get("symbol", "")
        timeframe = row.get("timeframe", "")
        if required_file != f"{symbol}_{timeframe}.csv":
            continue
        key = (row.get("candidate_id", ""), row.get("family_id", ""), symbol, timeframe)
        coverage = coverage_by_key.get(key, {})
        required_rows.append(
            {
                "priority": coverage.get("priority", "P1") or "P1",
                "family_id": row.get("family_id", ""),
                "candidate_id": row.get("candidate_id", ""),
                "symbol": symbol,
                "timeframe": timeframe,
                "required_start": coverage.get("required_start", ""),
                "required_end": coverage.get("required_end", ""),
                "expected_filename": required_file,
                "blocks_exact_validation": "true",
                "fallback_used": "false",
                "fallback_file": coverage.get("available_fallback_file", ""),
                "reason": "required by blocked exact replay row; validate exact file without fallback",
            }
        )
    return required_rows


def _missing_matrix_row(required: dict[str, str]) -> dict[str, str]:
    return {
        "expected_filename": required.get("expected_filename", ""),
        "actual_file": "",
        "priority": required.get("priority", ""),
        "family_id": required.get("family_id", ""),
        "candidate_id": required.get("candidate_id", ""),
        "symbol": required.get("symbol", ""),
        "timeframe": required.get("timeframe", ""),
        "required_start": required.get("required_start", ""),
        "required_end": required.get("required_end", ""),
        "rows_found": "0",
        "data_start": "",
        "data_end": "",
        "required_columns_present": "false",
        "aliases_used": "{}",
        "timestamp_parseable": "false",
        "timezone_status": "MISSING_FILE",
        "ohlcv_numeric": "false",
        "ohlc_integrity_passed": "false",
        "duplicate_timestamp_count": "0",
        "sorted_ascending": "false",
        "date_range_overlap": "false",
        "timeframe_interval_match": "false",
        "gap_count": "0",
        "session_coverage_status": "MISSING_FILE",
        "validation_status": "MISSING_FILE",
        "failure_reasons": "MISSING_FILE",
    }


def _alias_map(contract: dict[str, str]) -> dict[str, str]:
    aliases = dict(ACCEPTED_ALIASES)
    raw = contract.get("accepted_aliases", "")
    if raw:
        try:
            aliases.update(json.loads(raw))
        except json.JSONDecodeError:
            pass
    aliases.update({column: column for column in CANONICAL_COLUMNS})
    return aliases


def _parse_timestamp(value: str, timezone_name: str | None) -> tuple[datetime, str] | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    try:
        parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        if not timezone_name:
            return parsed, "MISSING"
        try:
            return parsed.replace(tzinfo=ZoneInfo(timezone_name)), "DECLARED"
        except ZoneInfoNotFoundError:
            return None
    return parsed.astimezone(UTC), "PRESENT"


def _declared_timezone(path: Path) -> str | None:
    for sidecar in [path.with_suffix(path.suffix + ".timezone"), path.with_suffix(path.suffix + ".tz")]:
        if sidecar.exists():
            value = sidecar.read_text(encoding="utf-8").strip()
            return value or None
    json_sidecar = path.with_suffix(path.suffix + ".json")
    if json_sidecar.exists():
        try:
            payload = json.loads(json_sidecar.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        for key in ["timezone", "declared_timezone", "timestamp_timezone"]:
            if payload.get(key):
                return str(payload[key])
    return None


def _date_ranges_overlap(timestamps: list[datetime], required_start: str, required_end: str) -> bool:
    if not timestamps:
        return False
    try:
        start = datetime.fromisoformat(required_start).date()
        end = datetime.fromisoformat(required_end).date()
    except ValueError:
        return False
    return min(ts.date() for ts in timestamps) <= end and max(ts.date() for ts in timestamps) >= start


def _timeframe_minutes(value: str) -> int | None:
    return {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60}.get(value)


def _session_status(timestamps: list[datetime]) -> str:
    if not timestamps:
        return "NO_ROWS"
    regular_start = time(9, 30)
    regular_end = time(16, 0)
    has_regular = any(regular_start <= ts.timetz().replace(tzinfo=None) <= regular_end for ts in timestamps)
    has_extended = any(ts.timetz().replace(tzinfo=None) < regular_start or ts.timetz().replace(tzinfo=None) > regular_end for ts in timestamps)
    if has_regular and has_extended:
        return "REGULAR_AND_EXTENDED_HOURS_PRESENT"
    if has_regular:
        return "REGULAR_HOURS_PRESENT"
    return "NO_REGULAR_HOURS"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _bool(value: bool) -> str:
    return "true" if value else "false"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
