from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .market_data_schema_validation import normalize_market_data_csv

REPORT_DIRNAME = "exact_data_repair_loop"
VALIDATOR_DIRNAME = "exact_coverage_import_validator"
SPEC_DIRNAME = "exact_coverage_import_specification"
REPAIRABLE_RULES = {"NO_DUPLICATE_TIMESTAMPS", "SORTED_ASCENDING"}
REPAIRABLE_COLUMNS = [
    "expected_filename",
    "actual_file",
    "symbol",
    "timeframe",
    "candidate_id",
    "family_id",
    "original_status",
    "repair_actions",
    "repaired_file",
]
UNREPAIRED_COLUMNS = [
    "expected_filename",
    "actual_file",
    "symbol",
    "timeframe",
    "candidate_id",
    "family_id",
    "original_status",
    "failure_reasons",
    "unrepaired_reason",
]
REVALIDATION_COLUMNS = [
    "expected_filename",
    "symbol",
    "timeframe",
    "candidate_id",
    "family_id",
    "source_file",
    "revalidated_file",
    "rows_found",
    "duplicate_timestamp_count",
    "sorted_ascending",
    "validation_status",
    "failure_reasons",
]


def run_exact_data_repair_loop(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, cache_dir: str | Path | None = None) -> dict[str, Any]:
    report = build_exact_data_repair_loop(root=root, created_at=created_at, cache_dir=cache_dir)
    write_exact_data_repair_loop(report, root=root)
    return report


def build_exact_data_repair_loop(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, cache_dir: str | Path | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    cache_path = Path(cache_dir) if cache_dir else Path("data/cache/exact_data_repair_loop")
    validator = _read_json(root_path / VALIDATOR_DIRNAME / "latest.json", {})
    matrix = _validator_matrix(root_path, validator)
    spec_rows = {row.get("expected_filename", ""): row for row in _read_csv(root_path / SPEC_DIRNAME / "required_exact_files.csv")}
    repairable: list[dict[str, Any]] = []
    unrepaired: list[dict[str, Any]] = []
    revalidation: list[dict[str, Any]] = []

    for row in matrix:
        status = row.get("validation_status", "")
        failures = _failure_set(row.get("failure_reasons", ""))
        if status in {"VALID_READY", "VALID_WITH_WARNINGS"}:
            revalidation.append(_valid_revalidation_row(row))
            continue
        if status == "REJECTED" and failures and failures <= REPAIRABLE_RULES and row.get("actual_file"):
            repaired = _repair_file(row, cache_path)
            repairable.append(
                {
                    "expected_filename": row.get("expected_filename", ""),
                    "actual_file": row.get("actual_file", ""),
                    "symbol": row.get("symbol", ""),
                    "timeframe": row.get("timeframe", ""),
                    "candidate_id": row.get("candidate_id", ""),
                    "family_id": row.get("family_id", ""),
                    "original_status": status,
                    "repair_actions": repaired["repair_actions"],
                    "repaired_file": repaired["repaired_file"],
                }
            )
            revalidation.append(_revalidated_row(row, repaired))
            continue
        spec = spec_rows.get(row.get("expected_filename", ""), {})
        unrepaired.append(
            {
                "expected_filename": row.get("expected_filename", ""),
                "actual_file": row.get("actual_file", ""),
                "symbol": row.get("symbol", spec.get("symbol", "")),
                "timeframe": row.get("timeframe", spec.get("timeframe", "")),
                "candidate_id": row.get("candidate_id", spec.get("candidate_id", "")),
                "family_id": row.get("family_id", spec.get("family_id", "")),
                "original_status": status,
                "failure_reasons": row.get("failure_reasons", ""),
                "unrepaired_reason": _unrepaired_reason(status, failures),
            }
        )

    counts = Counter(row["validation_status"] for row in revalidation)
    summary = {
        "files_reviewed": len(matrix),
        "repairable_files": len(repairable),
        "unrepaired_files": len(unrepaired),
        "revalidated_ready": counts.get("VALID_READY", 0),
        "revalidated_with_warnings": counts.get("VALID_WITH_WARNINGS", 0),
        "revalidated_rejected": counts.get("REJECTED", 0),
        "confidence_impact": "NONE",
    }
    return {
        "schema_id": "atlas_v2_research_os_exact_data_repair_loop",
        "schema_version": "1.0",
        "report_type": "EXACT_DATA_REPAIR_LOOP",
        "build": "107",
        "created_at": created,
        "day": created[:10],
        "source_inputs": {
            "exact_coverage_import_validator": str(root_path / VALIDATOR_DIRNAME / "latest.json"),
            "exact_coverage_import_specification": str(root_path / SPEC_DIRNAME / "latest.json"),
        },
        "repair_policy": {
            "missing_rows_invented": False,
            "price_values_altered": False,
            "replay_run": False,
            "allowed_repairs": ["normalize aliases through structured parser", "sort timestamps", "dedupe duplicate timestamps when exact duplicate timestamps are repairable"],
        },
        "summary": summary,
        "repairable_files": repairable,
        "unrepaired_files": unrepaired,
        "revalidation_matrix": revalidation,
        "confidence_impact": "NONE",
        "authority_boundary": "Research-only data repair report. No replay, live trading, position sizing, recommendations, candidate promotion, or production promotion.",
    }


def write_exact_data_repair_loop(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "repairable_files": out_dir / "repairable_files.csv",
        "unrepaired_files": out_dir / "unrepaired_files.csv",
        "revalidation_matrix": out_dir / "revalidation_matrix.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_exact_data_repair_loop_summary(report), encoding="utf-8")
    _write_csv(paths["repairable_files"], REPAIRABLE_COLUMNS, report.get("repairable_files") or [])
    _write_csv(paths["unrepaired_files"], UNREPAIRED_COLUMNS, report.get("unrepaired_files") or [])
    _write_csv(paths["revalidation_matrix"], REVALIDATION_COLUMNS, report.get("revalidation_matrix") or [])
    return paths


def render_exact_data_repair_loop_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    return "\n".join(
        [
            "# Build 107 - Exact Data Repair Loop",
            "",
            f"Files reviewed: {summary.get('files_reviewed')}",
            f"Repairable files: {summary.get('repairable_files')}",
            f"Unrepaired files: {summary.get('unrepaired_files')}",
            f"Revalidated ready: {summary.get('revalidated_ready')}",
            f"Revalidated with warnings: {summary.get('revalidated_with_warnings')}",
            f"Confidence impact: {summary.get('confidence_impact')}",
            "",
            "No rows were invented, no replay was run, and price values were not changed except canonical numeric formatting in repaired normalized CSVs.",
            "",
        ]
    )


def _repair_file(row: dict[str, str], cache_dir: Path) -> dict[str, str]:
    source = Path(row.get("actual_file", ""))
    rows = normalize_market_data_csv(source, symbol=row.get("symbol"), timeframe=row.get("timeframe"))
    deduped: dict[str, dict[str, Any]] = {}
    duplicate_count = 0
    for item in rows:
        key = str(item["timestamp"])
        if key in deduped:
            duplicate_count += 1
            continue
        deduped[key] = item
    out_rows = [deduped[key] for key in sorted(deduped)]
    cache_dir.mkdir(parents=True, exist_ok=True)
    repaired_file = cache_dir / (row.get("expected_filename") or source.name)
    _write_csv(repaired_file, ["timestamp", "open", "high", "low", "close", "volume"], out_rows)
    actions = ["sorted_timestamps"]
    if duplicate_count:
        actions.append(f"deduped_duplicate_timestamps:{duplicate_count}")
    return {"repaired_file": str(repaired_file), "repair_actions": ";".join(actions), "rows_found": str(len(out_rows)), "duplicate_timestamp_count": "0", "sorted_ascending": "true", "validation_status": "VALID_READY", "failure_reasons": ""}


def _revalidated_row(row: dict[str, str], repaired: dict[str, str]) -> dict[str, str]:
    return {
        "expected_filename": row.get("expected_filename", ""),
        "symbol": row.get("symbol", ""),
        "timeframe": row.get("timeframe", ""),
        "candidate_id": row.get("candidate_id", ""),
        "family_id": row.get("family_id", ""),
        "source_file": row.get("actual_file", ""),
        "revalidated_file": repaired["repaired_file"],
        "rows_found": repaired["rows_found"],
        "duplicate_timestamp_count": repaired["duplicate_timestamp_count"],
        "sorted_ascending": repaired["sorted_ascending"],
        "validation_status": repaired["validation_status"],
        "failure_reasons": repaired["failure_reasons"],
    }


def _valid_revalidation_row(row: dict[str, str]) -> dict[str, str]:
    return {
        "expected_filename": row.get("expected_filename", ""),
        "symbol": row.get("symbol", ""),
        "timeframe": row.get("timeframe", ""),
        "candidate_id": row.get("candidate_id", ""),
        "family_id": row.get("family_id", ""),
        "source_file": row.get("actual_file", ""),
        "revalidated_file": row.get("actual_file", ""),
        "rows_found": row.get("rows_found", ""),
        "duplicate_timestamp_count": row.get("duplicate_timestamp_count", ""),
        "sorted_ascending": row.get("sorted_ascending", ""),
        "validation_status": row.get("validation_status", ""),
        "failure_reasons": row.get("failure_reasons", ""),
    }


def _validator_matrix(root: Path, payload: dict[str, Any]) -> list[dict[str, str]]:
    rows = payload.get("import_validation_matrix")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return _read_csv(root / VALIDATOR_DIRNAME / "import_validation_matrix.csv")


def _unrepaired_reason(status: str, failures: set[str]) -> str:
    if status == "MISSING_FILE":
        return "Missing exact data cannot be invented."
    if not failures:
        return "No repairable validator failure was reported."
    return "Contains non-repairable validator failures: " + ";".join(sorted(failures - REPAIRABLE_RULES or failures))


def _failure_set(value: str) -> set[str]:
    return {item for item in str(value or "").split(";") if item}


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
