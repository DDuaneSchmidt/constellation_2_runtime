from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "exact_coverage_import_specification"
SOURCE_DIRNAME = "reversal_trending_exact_coverage_plan"

REQUIRED_EXACT_FILES_COLUMNS = [
    "priority",
    "family_id",
    "candidate_id",
    "symbol",
    "timeframe",
    "required_start",
    "required_end",
    "expected_filename",
    "blocks_exact_validation",
    "fallback_used",
    "fallback_file",
    "reason",
]

IMPORT_CONTRACT_COLUMNS = [
    "expected_filename",
    "symbol",
    "timeframe",
    "required_columns",
    "accepted_aliases",
    "timestamp_timezone_required",
    "regular_hours_policy",
    "extended_hours_policy",
    "duplicate_policy",
    "gap_policy",
    "sort_required",
    "output_normalized_path",
]

VALIDATION_RULE_COLUMNS = [
    "rule_id",
    "rule_name",
    "severity",
    "description",
    "failure_action",
]

CANONICAL_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
ACCEPTED_ALIASES = {
    "datetime": "timestamp",
    "date_time": "timestamp",
    "time": "timestamp",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "v": "volume",
}
TIMEFRAME_NORMALIZATION = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "1M": "1m",
    "5M": "5m",
    "15M": "15m",
    "30M": "30m",
    "1H": "1h",
}
FORBIDDEN_AUTHORITY_TERMS = [
    "live trading",
    "broker execution",
    "capital allocation",
    "position sizing",
    "trade recommendations",
    "automatic paper placement",
    "candidate promotion",
    "production promotion",
    "methodology confidence increase",
    "paid data acquisition",
    "external API calls",
]


def run_exact_coverage_import_specification(
    root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None
) -> dict[str, Any]:
    report = build_exact_coverage_import_specification(root=root, created_at=created_at)
    write_exact_coverage_import_specification(report, root=root)
    return report


def build_exact_coverage_import_specification(
    root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None
) -> dict[str, Any]:
    root_path = Path(root)
    source_dir = root_path / SOURCE_DIRNAME
    created = created_at or _now()

    shopping_rows = _read_csv(source_dir / "missing_data_shopping_list.csv")
    coverage_rows = _read_csv(source_dir / "coverage_matrix.csv")
    fallback_rows = _read_csv(source_dir / "fallback_usage.csv")
    latest = _read_json(source_dir / "latest.json", {})

    prioritized_rows = [row for row in shopping_rows if row.get("priority") in {"P0", "P1"}]
    coverage_by_key = {
        _row_key(row): row
        for row in coverage_rows
        if row.get("candidate_id") and row.get("family_id") and row.get("symbol") and row.get("timeframe")
    }
    fallback_by_key = {
        _fallback_key(row): row
        for row in fallback_rows
        if row.get("candidate_id") and row.get("family_id") and row.get("symbol") and row.get("required_timeframe")
    }

    required_rows = [
        _required_exact_file_row(row, coverage_by_key, fallback_by_key)
        for row in prioritized_rows
    ]
    contract_rows = _import_contract_rows(required_rows)
    validation_rows = validation_rules()
    families_blocked = sorted({row["family_id"] for row in required_rows if _truthy(row["blocks_exact_validation"])})
    summary = {
        "required_files": len(required_rows),
        "unique_expected_files": len({row["expected_filename"] for row in required_rows}),
        "P0_files": sum(row["priority"] == "P0" for row in required_rows),
        "P1_files": sum(row["priority"] == "P1" for row in required_rows),
        "families_blocked": families_blocked,
        "confidence_impact": "NONE",
        "confidence_impact_reason": "This build defines data import requirements only. It does not validate new market evidence.",
    }
    return {
        "schema_id": "atlas_v2_research_os_exact_coverage_import_specification",
        "schema_version": "1.0",
        "report_type": "EXACT_COVERAGE_IMPORT_SPECIFICATION",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "missing_data_shopping_list": str(source_dir / "missing_data_shopping_list.csv"),
            "coverage_matrix": str(source_dir / "coverage_matrix.csv"),
            "fallback_usage": str(source_dir / "fallback_usage.csv"),
            "latest": str(source_dir / "latest.json"),
        },
        "source_summary": latest.get("summary", {}),
        "summary": summary,
        "required_exact_files": required_rows,
        "import_contract": contract_rows,
        "validation_rules": validation_rows,
        "vendor_format_examples": vendor_format_examples(),
        "confidence_impact": "NONE",
        "confidence_impact_reason": "This build defines data import requirements only. It does not validate new market evidence.",
        "recommended_next_build": "Build 098 — Exact Coverage Import Validator",
        "authority_boundary": {
            "research_only": True,
            "forbidden_actions": FORBIDDEN_AUTHORITY_TERMS,
        },
        "guardrails": [
            "Specification only.",
            "No paid data acquisition or external API calls.",
            "No replay, ranking, qualification, paper placement, promotion, or confidence increase.",
        ],
    }


def write_exact_coverage_import_specification(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "required_exact_files": out_dir / "required_exact_files.csv",
        "import_contract": out_dir / "import_contract.csv",
        "validation_rules": out_dir / "validation_rules.csv",
        "vendor_format_examples": out_dir / "vendor_format_examples.md",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_exact_coverage_import_specification_summary(report), encoding="utf-8")
    paths["vendor_format_examples"].write_text(vendor_format_examples(), encoding="utf-8")
    _write_csv(paths["required_exact_files"], REQUIRED_EXACT_FILES_COLUMNS, report.get("required_exact_files") or [])
    _write_csv(paths["import_contract"], IMPORT_CONTRACT_COLUMNS, report.get("import_contract") or [])
    _write_csv(paths["validation_rules"], VALIDATION_RULE_COLUMNS, report.get("validation_rules") or [])
    return paths


def normalize_timeframe_label(value: str) -> str:
    if value not in TIMEFRAME_NORMALIZATION:
        raise ValueError(f"unsupported timeframe label: {value}")
    return TIMEFRAME_NORMALIZATION[value]


def normalize_column_name(value: str) -> str:
    cleaned = value.strip()
    return ACCEPTED_ALIASES.get(cleaned, cleaned)


def normalize_required_columns(columns: list[str]) -> list[str]:
    return [normalize_column_name(column) for column in columns]


def validation_rules() -> list[dict[str, str]]:
    return [
        {
            "rule_id": "REQUIRED_COLUMNS_PRESENT",
            "rule_name": "Required columns present",
            "severity": "ERROR",
            "description": "CSV must contain timestamp, open, high, low, close, volume after accepted alias normalization.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "TIMESTAMP_PARSEABLE",
            "rule_name": "Timestamp parseable",
            "severity": "ERROR",
            "description": "Every timestamp must parse to a valid datetime.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "TIMESTAMP_TIMEZONE_PRESENT_OR_DECLARED",
            "rule_name": "Timestamp timezone present or declared",
            "severity": "ERROR",
            "description": "Timezone-aware timestamps are required; naive timestamps are accepted only with an external timezone declaration.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "OHLC_NUMERIC",
            "rule_name": "OHLC numeric",
            "severity": "ERROR",
            "description": "Open, high, low, and close values must be numeric.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "VOLUME_NUMERIC",
            "rule_name": "Volume numeric",
            "severity": "ERROR",
            "description": "Volume values must be numeric.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "HIGH_GTE_LOW",
            "rule_name": "High greater than or equal to low",
            "severity": "ERROR",
            "description": "High must be greater than or equal to low for every row.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "HIGH_GTE_OPEN_CLOSE",
            "rule_name": "High bounds open and close",
            "severity": "ERROR",
            "description": "High must be greater than or equal to open and close for every row.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "LOW_LTE_OPEN_CLOSE",
            "rule_name": "Low bounds open and close",
            "severity": "ERROR",
            "description": "Low must be less than or equal to open and close for every row.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "NO_DUPLICATE_TIMESTAMPS",
            "rule_name": "No duplicate timestamps",
            "severity": "ERROR",
            "description": "Duplicate timestamps must be rejected or deduplicated with a report before exact replay.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "SORTED_ASCENDING",
            "rule_name": "Sorted ascending",
            "severity": "ERROR",
            "description": "Rows must be sorted by timestamp ascending before exact replay.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "DATE_RANGE_OVERLAPS_REQUIREMENT",
            "rule_name": "Date range overlaps requirement",
            "severity": "ERROR",
            "description": "Imported data must overlap the required start and end window for the blocking candidate requirement.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "TIMEFRAME_INTERVAL_MATCH",
            "rule_name": "Timeframe interval match",
            "severity": "ERROR",
            "description": "Observed timestamp spacing must match the expected normalized timeframe except for reported market-session gaps.",
            "failure_action": "REJECT",
        },
        {
            "rule_id": "GAPS_REPORTED",
            "rule_name": "Gaps reported",
            "severity": "WARNING",
            "description": "Large time gaps are allowed only when preserved and reported.",
            "failure_action": "ACCEPT_WITH_WARNING",
        },
        {
            "rule_id": "SESSION_COVERAGE_REPORTED",
            "rule_name": "Session coverage reported",
            "severity": "INFO",
            "description": "Regular-hours and extended-hours coverage must be preserved and flagged when known.",
            "failure_action": "REPORT_ONLY",
        },
    ]


def vendor_format_examples() -> str:
    return """# Vendor Format Examples

## Canonical

```csv
timestamp,open,high,low,close,volume
2023-01-03T09:30:00-05:00,33.12,33.20,33.01,33.15,123456
```

## Alias Layout

```csv
datetime,o,h,l,c,v
2023-01-03T09:30:00-05:00,33.12,33.20,33.01,33.15,123456
```

## Naive Timestamp Layout Requiring Declared Timezone

```csv
timestamp,open,high,low,close,volume
2023-01-03 09:30:00,33.12,33.20,33.01,33.15,123456
```

Naive timestamps are accepted only when a timezone declaration is supplied outside the CSV or in metadata.

All accepted layouts normalize to:

```csv
timestamp,open,high,low,close,volume
```
"""


def render_exact_coverage_import_specification_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    required = report.get("required_exact_files") or []
    contract = report.get("import_contract") or []
    lines = [
        "# Build 097 — Exact Coverage Import Specification",
        "",
        "## Executive Summary",
        "",
        f"Required exact-file rows: {summary.get('required_files')}",
        f"Unique expected files: {summary.get('unique_expected_files')}",
        f"P0 files: {summary.get('P0_files')}",
        f"P1 files: {summary.get('P1_files')}",
        f"Families blocked: {', '.join(summary.get('families_blocked') or [])}",
        "",
        "## Source Inputs",
        "",
    ]
    for name, path in (report.get("source_inputs") or {}).items():
        lines.append(f"- {name}: `{path}`")
    lines.extend(["", "## Required Exact Files", ""])
    for row in required[:30]:
        lines.append(f"- {row['priority']} `{row['expected_filename']}` for {row['family_id']} / {row['candidate_id']}")
    if len(required) > 30:
        lines.append(f"- ... {len(required) - 30} additional rows omitted from summary")
    lines.extend(["", "## Import Contract", ""])
    lines.append("Expected filenames use `{SYMBOL}_{TIMEFRAME}.csv` and normalize accepted timeframe labels to `1m`, `5m`, `15m`, `30m`, or `1h`.")
    lines.append("Required columns normalize to `timestamp,open,high,low,close,volume`.")
    lines.append(f"Unique contract files: {len(contract)}")
    lines.extend(["", "## Validation Rules", ""])
    for row in report.get("validation_rules") or []:
        lines.append(f"- {row['severity']} {row['rule_id']}: {row['failure_action']}")
    lines.extend(
        [
            "",
            "## Vendor Format Examples",
            "",
            "See `vendor_format_examples.md` for canonical, alias, and naive-timestamp layouts.",
            "",
            "## What This Build Does Not Do",
            "",
            "- It does not acquire paid data or call external APIs.",
            "- It does not run exact replay or validate new market evidence.",
            "- It does not change ranking, qualification, paper placement, promotion, or confidence methodology.",
            "",
            "## Confidence Impact",
            "",
            "NONE",
            "",
            "Reason: This build defines data import requirements only. It does not validate new market evidence.",
            "",
            "## Authority Boundary",
            "",
            "Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, production promotion, methodology confidence increase, paid data acquisition, or external API calls.",
            "",
        ]
    )
    return "\n".join(lines)


def _required_exact_file_row(
    row: dict[str, str],
    coverage_by_key: dict[tuple[str, str, str, str], dict[str, str]],
    fallback_by_key: dict[tuple[str, str, str, str], dict[str, str]],
) -> dict[str, str]:
    symbol = row.get("symbol", "").strip().upper()
    timeframe = normalize_timeframe_label(row.get("timeframe", "").strip())
    base = {
        "priority": row.get("priority", ""),
        "family_id": row.get("family_id", ""),
        "candidate_id": row.get("candidate_id", ""),
        "symbol": symbol,
        "timeframe": timeframe,
        "required_start": row.get("required_start", ""),
        "required_end": row.get("required_end", ""),
        "expected_filename": f"{symbol}_{timeframe}.csv",
        "blocks_exact_validation": row.get("blocks_exact_validation", "true"),
        "reason": row.get("reason", ""),
    }
    coverage = coverage_by_key.get(_row_key(base), {})
    fallback = fallback_by_key.get(_row_key(base), {})
    base["fallback_used"] = coverage.get("fallback_used", "false")
    base["fallback_file"] = coverage.get("available_fallback_file") or fallback.get("fallback_file", "")
    return base


def _import_contract_rows(required_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in required_rows:
        filename = row["expected_filename"]
        if filename in seen:
            continue
        seen.add(filename)
        rows.append(
            {
                "expected_filename": filename,
                "symbol": row["symbol"],
                "timeframe": row["timeframe"],
                "required_columns": ",".join(CANONICAL_COLUMNS),
                "accepted_aliases": json.dumps(ACCEPTED_ALIASES, sort_keys=True, separators=(",", ":")),
                "timestamp_timezone_required": "true",
                "regular_hours_policy": "preserve_and_flag",
                "extended_hours_policy": "preserve_and_flag",
                "duplicate_policy": "reject_or_dedupe_with_report",
                "gap_policy": "allow_but_report",
                "sort_required": "true",
                "output_normalized_path": f"data/manual_intraday_import/{filename}",
            }
        )
    return rows


def _row_key(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        row.get("candidate_id", ""),
        row.get("family_id", ""),
        row.get("symbol", "").upper(),
        normalize_timeframe_label(row.get("timeframe", row.get("required_timeframe", "")).strip()),
    )


def _fallback_key(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        row.get("candidate_id", ""),
        row.get("family_id", ""),
        row.get("symbol", "").upper(),
        normalize_timeframe_label(row.get("required_timeframe", "").strip()),
    )


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
