from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.acquisition.expected_files import expected_csv_files_report
from research_lab.datasets.csv_validation import validate_local_csv_file
from research_lab.storage.hashing import utc_now_iso
from research_lab.storage.manifest_io import read_json, write_json
from research_lab.universes.universe_registry import load_universe_snapshot


def build_csv_readiness_report(
    *,
    universe_snapshot_id: str,
    csv_root: Path,
    start: str,
    end: str,
    allow_missing_symbols: bool,
    store_root: Path | None = None,
) -> dict[str, Any]:
    universe = load_universe_snapshot(universe_snapshot_id, store_root=store_root)
    expected = expected_csv_files_report(universe_snapshot_id=universe_snapshot_id, csv_root=csv_root, store_root=store_root)
    symbols_expected = sorted(str(row["symbol"]).upper() for row in universe["symbols"])
    per_symbol: list[dict[str, Any]] = []
    blocking_errors: list[str] = []
    warnings: list[str] = []
    for symbol in symbols_expected:
        path = csv_root.resolve() / f"{symbol}.csv"
        if not path.exists():
            per_symbol.append(
                {
                    "symbol": symbol,
                    "path": str(path),
                    "exists": False,
                    "valid": False,
                    "errors": ["missing_file"],
                    "warnings": [],
                    "row_count": 0,
                    "first_date": None,
                    "last_date": None,
                    "coverage_ratio": 0.0,
                }
            )
            if not allow_missing_symbols:
                blocking_errors.append(f"{symbol}:missing_file")
            continue
        result = validate_local_csv_file(symbol=symbol, path=path, start=start, end=end)
        per_symbol.append(result)
        for error in result["errors"]:
            blocking_errors.append(f"{symbol}:{error}")
        for warning in result["warnings"]:
            warnings.append(f"{symbol}:{warning}")

    symbols_present = sorted(row["symbol"] for row in per_symbol if row.get("exists") and row.get("valid"))
    symbols_missing = sorted(row["symbol"] for row in per_symbol if not row.get("exists"))
    if not symbols_present:
        blocking_errors.append("no_symbols_present")
    if expected["extra_files"]:
        warnings.append("extra_csv_files_not_in_universe")
    if expected["template_files_ignored"]:
        warnings.append("template_files_ignored")

    ready_to_build = not blocking_errors
    overall_status = "ready" if ready_to_build and not warnings else ("ready_with_warnings" if ready_to_build else "blocked")
    return {
        "universe_snapshot_id": universe_snapshot_id,
        "csv_root": str(csv_root.resolve()),
        "start_date": start,
        "end_date": end,
        "allow_missing_symbols": allow_missing_symbols,
        "symbols_expected": symbols_expected,
        "symbols_present": symbols_present,
        "symbols_missing": symbols_missing,
        "extra_files": expected["extra_files"],
        "template_files_ignored": expected["template_files_ignored"],
        "overall_status": overall_status,
        "ready_to_build": ready_to_build,
        "blocking_errors": sorted(set(blocking_errors)),
        "warnings": sorted(set(warnings)),
        "per_symbol": per_symbol,
        "created_at": utc_now_iso(),
        "schema_version": "csv_readiness_report.v1",
    }


def write_csv_readiness_report(report: dict[str, Any], output_path: Path) -> Path:
    return write_json(output_path, report, overwrite=True)


def load_csv_readiness_report(path: Path) -> dict[str, Any]:
    return read_json(path)


def validate_readiness_report_for_build(
    *,
    report_path: Path,
    universe_snapshot_id: str,
    csv_root: Path,
) -> dict[str, Any]:
    if not report_path.exists():
        raise RuntimeError(f"Readiness report required but missing: {report_path}")
    report = load_csv_readiness_report(report_path)
    if report.get("universe_snapshot_id") != universe_snapshot_id:
        raise RuntimeError("Readiness report universe_snapshot_id mismatch")
    if str(Path(report.get("csv_root", "")).resolve()) != str(csv_root.resolve()):
        raise RuntimeError("Readiness report csv_root mismatch")
    if report.get("ready_to_build") is not True:
        raise RuntimeError("Readiness report is not ready_to_build=true")
    return report

