from __future__ import annotations

import csv
import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Iterable

from .data_source_inventory import AUTHORITY_BOUNDARY, AUTHORITY_TEXT


DEFAULT_REPORT_ROOT = Path("reports/portfolio_research_os")
DEFAULT_DATA_ROOT = Path("data/portfolio_research_os")
TODAY = date(2026, 6, 6)

IMPORT_READY = "IMPORT_READY"
PARTIALLY_READY = "PARTIALLY_READY"
DATA_REQUIRED = "DATA_REQUIRED"
REJECTED = "REJECTED"
USER_ASSUMPTION = "USER_ASSUMPTION"

REPORT_DIRS = {
    "import_folder_structure": "import_folder_structure",
    "vendor_request_templates": "vendor_request_templates",
    "data_file_naming_contract": "data_file_naming_contract",
    "raw_data_import_scanner": "raw_data_import_scanner",
    "universe_import_validator": "universe_import_validator",
    "price_import_validator": "price_import_validator",
    "fundamental_import_validator": "fundamental_import_validator",
    "benchmark_import_validator": "benchmark_import_validator",
    "sector_industry_map_validator": "sector_industry_map_validator",
    "portfolio_data_import_readiness": "portfolio_data_import_readiness",
    "normalized_data_cache": "normalized_data_cache",
}

FOLDERS = [
    "data/portfolio_research_os",
    "data/portfolio_research_os/raw",
    "data/portfolio_research_os/raw/prices",
    "data/portfolio_research_os/raw/fundamentals",
    "data/portfolio_research_os/raw/dividends_splits",
    "data/portfolio_research_os/raw/universe",
    "data/portfolio_research_os/raw/benchmarks",
    "data/portfolio_research_os/raw/risk_free",
    "data/portfolio_research_os/normalized",
    "data/portfolio_research_os/cache",
]

REQUIRED_FILES = {
    "pit_universe.csv": ("raw/universe/pit_universe.csv", "point_in_time_universe_contract"),
    "daily_prices.csv": ("raw/prices/daily_prices.csv", "historical_price_return_contract"),
    "fundamentals_pit.csv": ("raw/fundamentals/fundamentals_pit.csv", "fundamental_data_contract"),
    "dividends_splits.csv": ("raw/dividends_splits/dividends_splits.csv", "historical_price_return_contract"),
    "benchmark_returns.csv": ("raw/benchmarks/benchmark_returns.csv", "benchmark_data_contract"),
    "risk_free_rates.csv": ("raw/risk_free/risk_free_rates.csv", "benchmark_data_contract"),
    "sector_industry_map.csv": ("raw/universe/sector_industry_map.csv", "point_in_time_universe_contract"),
}

UNIVERSE_COLUMNS = [
    "as_of_date",
    "ticker",
    "security_id",
    "security_type",
    "name",
    "sector",
    "industry",
    "market_cap",
    "average_dollar_volume",
    "is_active",
    "is_delisted",
    "delisting_date",
    "eligible_for_portfolio",
    "exclusion_reason",
]
PRICE_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "adjusted_close", "volume", "dividend", "split_factor"]
FUNDAMENTAL_COLUMNS = [
    "as_of_date",
    "report_date",
    "filing_date",
    "ticker",
    "revenue",
    "eps",
    "free_cash_flow",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "debt",
    "cash",
    "shares_outstanding",
    "book_value",
    "dividend_per_share",
    "buybacks",
]
SECTOR_COLUMNS = ["ticker", "as_of_date", "sector", "industry", "source"]
BENCHMARK_IDS = ["VTI", "60_40_PROXY", "SIMPLE_FACTOR_PROXY", "OAK_HARVEST_PROXY", "CASH_OR_RISK_FREE"]


def run_import_folder_structure(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_import_folder_structure(data_root=data_root, created_at=created_at)
    write_import_folder_structure(report, report_root)
    return report


def run_vendor_request_templates(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_vendor_request_templates(created_at=created_at)
    write_vendor_request_templates(report, report_root)
    return report


def run_data_file_naming_contract(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_file_naming_contract(created_at=created_at)
    write_data_file_naming_contract(report, report_root)
    return report


def run_raw_data_import_scanner(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_raw_data_import_scanner(data_root=data_root, created_at=created_at)
    write_raw_data_import_scanner(report, report_root)
    return report


def run_universe_import_validator(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_universe_import_validator(data_root=data_root, created_at=created_at)
    write_simple_report(report, report_root, "universe_import_validator", "Portfolio Atlas P050 - Universe Import Validator")
    return report


def run_price_import_validator(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_price_import_validator(data_root=data_root, created_at=created_at)
    write_simple_report(report, report_root, "price_import_validator", "Portfolio Atlas P051 - Price Import Validator")
    return report


def run_fundamental_import_validator(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_fundamental_import_validator(data_root=data_root, created_at=created_at)
    write_simple_report(report, report_root, "fundamental_import_validator", "Portfolio Atlas P052 - Fundamental Import Validator")
    return report


def run_benchmark_import_validator(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_benchmark_import_validator(data_root=data_root, created_at=created_at)
    write_simple_report(report, report_root, "benchmark_import_validator", "Portfolio Atlas P053 - Benchmark Import Validator")
    return report


def run_sector_industry_map_validator(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_sector_industry_map_validator(data_root=data_root, created_at=created_at)
    write_simple_report(report, report_root, "sector_industry_map_validator", "Portfolio Atlas P054 - Sector/Industry Map Validator")
    return report


def run_portfolio_data_import_readiness(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_portfolio_data_import_readiness(data_root=data_root, created_at=created_at)
    write_portfolio_data_import_readiness(report, report_root)
    return report


def run_normalized_data_cache(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_normalized_data_cache(data_root=data_root, created_at=created_at)
    write_normalized_data_cache(report, report_root)
    return report


def build_import_folder_structure(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(data_root)
    created = created_at or _now()
    rows = []
    for folder in FOLDERS:
        path = Path(folder)
        if str(root) != str(DEFAULT_DATA_ROOT):
            path = root / Path(folder).relative_to(DEFAULT_DATA_ROOT)
        path.mkdir(parents=True, exist_ok=True)
        rows.append({"folder": str(path), "exists": True, "purpose": _folder_purpose(path)})
    return {
        "schema_id": "portfolio_research_os_import_folder_structure",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P046",
        "created_at": created,
        "status": IMPORT_READY,
        "folder_manifest": rows,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_vendor_request_templates(*, created_at: str | None = None) -> dict[str, Any]:
    return {
        "schema_id": "portfolio_research_os_vendor_request_templates",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P047",
        "created_at": created_at or _now(),
        "status": IMPORT_READY,
        "templates": {
            "price_data_request.md": _price_template(),
            "fundamental_data_request.md": _fundamental_template(),
            "universe_data_request.md": _universe_template(),
            "benchmark_data_request.md": _benchmark_template(),
            "risk_free_data_request.md": _risk_free_template(),
        },
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_data_file_naming_contract(*, created_at: str | None = None) -> dict[str, Any]:
    manifest = [
        {"required_file": name, "expected_relative_path": relative, "required": True, "status": DATA_REQUIRED}
        for name, (relative, _) in REQUIRED_FILES.items()
    ]
    schema_map = [{"required_file": name, "schema_contract": schema, "expected_relative_path": relative} for name, (relative, schema) in REQUIRED_FILES.items()]
    return {
        "schema_id": "portfolio_research_os_data_file_naming_contract",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P048",
        "created_at": created_at or _now(),
        "status": IMPORT_READY,
        "required_file_manifest": manifest,
        "file_schema_map": schema_map,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_raw_data_import_scanner(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(data_root)
    raw = root / "raw"
    expected = {relative for relative, _ in REQUIRED_FILES.values()}
    found: list[dict[str, Any]] = []
    unexpected: list[dict[str, Any]] = []
    empty: list[dict[str, Any]] = []
    malformed: list[dict[str, Any]] = []
    if raw.exists():
        for path in sorted(raw.rglob("*")):
            if not path.is_file():
                continue
            rel = str(path.relative_to(root))
            row = {"path": str(path), "relative_path": rel, "size_bytes": path.stat().st_size}
            if rel in expected:
                found.append(row)
            else:
                unexpected.append({**row, "reason": "File is not in the required naming contract."})
            if path.stat().st_size == 0:
                empty.append({**row, "reason": "File is empty."})
            elif path.suffix.lower() == ".csv" and _csv_header(path) is None:
                malformed.append({**row, "reason": "CSV header could not be read."})
    missing = [
        {"required_file": name, "expected_relative_path": relative, "status": DATA_REQUIRED}
        for name, (relative, _) in REQUIRED_FILES.items()
        if not (root / relative).exists()
    ]
    status = IMPORT_READY if not missing and not empty and not malformed else PARTIALLY_READY if found else DATA_REQUIRED
    return {
        "schema_id": "portfolio_research_os_raw_data_import_scanner",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P049",
        "created_at": created_at or _now(),
        "status": status,
        "found_files": found,
        "missing_files": missing,
        "unexpected_files": unexpected,
        "empty_files": empty,
        "malformed_files": malformed,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_universe_import_validator(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    path = Path(data_root) / REQUIRED_FILES["pit_universe.csv"][0]
    return _validate_csv(path, UNIVERSE_COLUMNS, "Portfolio Atlas P050", created_at, row_rules=[_reject_future_as_of_date])


def build_price_import_validator(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(data_root)
    path = root / REQUIRED_FILES["daily_prices.csv"][0]
    report = _validate_csv(path, PRICE_COLUMNS, "Portfolio Atlas P051", created_at)
    if report["status"] == IMPORT_READY and not _price_only_explicit(root):
        rows = _read_rows(path)[0]
        if _all_blank(rows, "dividend") or _all_blank(rows, "split_factor"):
            report["status"] = REJECTED
            report["rejections"].append({"file": str(path), "reason": "Dividend or split_factor values are blank without explicit price-only classification."})
    return report


def build_fundamental_import_validator(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    path = Path(data_root) / REQUIRED_FILES["fundamentals_pit.csv"][0]
    return _validate_csv(path, FUNDAMENTAL_COLUMNS, "Portfolio Atlas P052", created_at, row_rules=[_reject_future_filing])


def build_benchmark_import_validator(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(data_root)
    benchmark_path = root / REQUIRED_FILES["benchmark_returns.csv"][0]
    risk_free_path = root / REQUIRED_FILES["risk_free_rates.csv"][0]
    rejections = []
    missing = []
    rows: list[dict[str, str]] = []
    for path, required in [(benchmark_path, ["date", "benchmark_id", "total_return", "source", "assumption_flag"]), (risk_free_path, ["date", "rate", "source"])]:
        if not path.exists():
            missing.append({"file": str(path), "reason": "Missing required file."})
            continue
        data, fields = _read_rows(path)
        missing_columns = [column for column in required if column not in fields]
        if missing_columns:
            rejections.append({"file": str(path), "reason": f"Missing required columns: {', '.join(missing_columns)}"})
        rows.extend(data)
    present_ids = {row.get("benchmark_id", "") for row in rows}
    for benchmark_id in BENCHMARK_IDS:
        if benchmark_id not in present_ids:
            missing.append({"file": str(benchmark_path), "reason": f"Missing benchmark_id {benchmark_id}"})
    for row in rows:
        if row.get("benchmark_id") == "OAK_HARVEST_PROXY" and row.get("assumption_flag") != USER_ASSUMPTION and not row.get("source"):
            rejections.append({"file": str(benchmark_path), "reason": "OAK_HARVEST_PROXY must remain USER_ASSUMPTION unless source-backed return data is provided."})
    status = IMPORT_READY if not missing and not rejections else REJECTED if rejections else DATA_REQUIRED
    return _validator_report("Portfolio Atlas P053", created_at, status, missing, rejections, len(rows))


def build_sector_industry_map_validator(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    path = Path(data_root) / REQUIRED_FILES["sector_industry_map.csv"][0]
    return _validate_csv(path, SECTOR_COLUMNS, "Portfolio Atlas P054", created_at, row_rules=[_reject_future_as_of_date])


def build_portfolio_data_import_readiness(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    created = created_at or _now()
    scanner = build_raw_data_import_scanner(data_root=data_root, created_at=created)
    validators = [
        ("P049", "raw_data_import_scanner", scanner),
        ("P050", "universe_import_validator", build_universe_import_validator(data_root=data_root, created_at=created)),
        ("P051", "price_import_validator", build_price_import_validator(data_root=data_root, created_at=created)),
        ("P052", "fundamental_import_validator", build_fundamental_import_validator(data_root=data_root, created_at=created)),
        ("P053", "benchmark_import_validator", build_benchmark_import_validator(data_root=data_root, created_at=created)),
        ("P054", "sector_industry_map_validator", build_sector_industry_map_validator(data_root=data_root, created_at=created)),
    ]
    matrix = [{"phase": phase, "component": name, "classification": report["status"], "rows_loaded": report.get("rows_loaded", "")} for phase, name, report in validators]
    rejected = [item for _, _, report in validators for item in report.get("rejections", [])]
    missing = scanner["missing_files"] + [item for _, _, report in validators for item in report.get("missing_files", [])]
    if rejected:
        status = REJECTED
    elif missing:
        status = DATA_REQUIRED
    elif all(row["classification"] == IMPORT_READY for row in matrix):
        status = IMPORT_READY
    else:
        status = PARTIALLY_READY
    return {
        "schema_id": "portfolio_research_os_portfolio_data_import_readiness",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P055",
        "created_at": created,
        "status": status,
        "import_readiness_matrix": matrix,
        "rejected_files": rejected,
        "missing_files": _dedupe_rows(missing),
        "next_actions": _next_actions(status, rejected, missing),
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_normalized_data_cache(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(data_root)
    created = created_at or _now()
    readiness = build_portfolio_data_import_readiness(data_root=root, created_at=created)
    normalized = root / "normalized"
    outputs = [
        "normalized_universe.csv",
        "normalized_daily_prices.csv",
        "normalized_total_returns.csv",
        "normalized_fundamentals.csv",
        "normalized_benchmarks.csv",
        "normalized_risk_free.csv",
    ]
    written: list[dict[str, Any]] = []
    if readiness["status"] == IMPORT_READY:
        normalized.mkdir(parents=True, exist_ok=True)
        copy_map = {
            "normalized_universe.csv": root / REQUIRED_FILES["pit_universe.csv"][0],
            "normalized_daily_prices.csv": root / REQUIRED_FILES["daily_prices.csv"][0],
            "normalized_total_returns.csv": root / REQUIRED_FILES["daily_prices.csv"][0],
            "normalized_fundamentals.csv": root / REQUIRED_FILES["fundamentals_pit.csv"][0],
            "normalized_benchmarks.csv": root / REQUIRED_FILES["benchmark_returns.csv"][0],
            "normalized_risk_free.csv": root / REQUIRED_FILES["risk_free_rates.csv"][0],
        }
        for name, source in copy_map.items():
            target = normalized / name
            shutil.copyfile(source, target)
            written.append({"file": str(target), "source": str(source), "status": IMPORT_READY})
    return {
        "schema_id": "portfolio_research_os_normalized_data_cache",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P056-P060",
        "created_at": created,
        "status": IMPORT_READY if readiness["status"] == IMPORT_READY else DATA_REQUIRED,
        "readiness_status": readiness["status"],
        "expected_outputs": [{"file": str(normalized / name), "status": IMPORT_READY if written else DATA_REQUIRED} for name in outputs],
        "written_files": written,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def write_import_folder_structure(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "import_folder_structure")
    _write_json(out / "latest.json", report)
    (out / "latest_summary.md").write_text(_summary("Portfolio Atlas P046 - Import Folder Structure", report), encoding="utf-8")
    _write_csv(out / "folder_manifest.csv", ["folder", "exists", "purpose"], report["folder_manifest"])
    return {"latest_json": out / "latest.json"}


def write_vendor_request_templates(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "vendor_request_templates")
    _write_json(out / "latest.json", report)
    (out / "latest_summary.md").write_text(_summary("Portfolio Atlas P047 - Vendor Request Templates", report), encoding="utf-8")
    for name, content in report["templates"].items():
        (out / name).write_text(content, encoding="utf-8")
    return {"latest_json": out / "latest.json"}


def write_data_file_naming_contract(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "data_file_naming_contract")
    _write_json(out / "latest.json", report)
    (out / "latest_summary.md").write_text(_summary("Portfolio Atlas P048 - Data File Naming Contract", report), encoding="utf-8")
    _write_csv(out / "required_file_manifest.csv", ["required_file", "expected_relative_path", "required", "status"], report["required_file_manifest"])
    _write_csv(out / "file_schema_map.csv", ["required_file", "schema_contract", "expected_relative_path"], report["file_schema_map"])
    return {"latest_json": out / "latest.json"}


def write_raw_data_import_scanner(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "raw_data_import_scanner")
    _write_json(out / "latest.json", report)
    (out / "latest_summary.md").write_text(_summary("Portfolio Atlas P049 - Raw Data Import Scanner", report), encoding="utf-8")
    _write_csv(out / "found_files.csv", ["path", "relative_path", "size_bytes"], report["found_files"])
    _write_csv(out / "missing_files.csv", ["required_file", "expected_relative_path", "status"], report["missing_files"])
    _write_csv(out / "unexpected_files.csv", ["path", "relative_path", "size_bytes", "reason"], report["unexpected_files"])
    _write_csv(out / "empty_files.csv", ["path", "relative_path", "size_bytes", "reason"], report["empty_files"])
    _write_csv(out / "malformed_files.csv", ["path", "relative_path", "size_bytes", "reason"], report["malformed_files"])
    return {"latest_json": out / "latest.json"}


def write_simple_report(report: dict[str, Any], report_root: Path | str, key: str, title: str) -> dict[str, Path]:
    out = _out_dir(report_root, key)
    _write_json(out / "latest.json", report)
    (out / "latest_summary.md").write_text(_summary(title, report), encoding="utf-8")
    _write_csv(out / "missing_files.csv", ["file", "reason"], report.get("missing_files", []))
    _write_csv(out / "rejected_files.csv", ["file", "reason"], report.get("rejections", []))
    return {"latest_json": out / "latest.json"}


def write_portfolio_data_import_readiness(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "portfolio_data_import_readiness")
    _write_json(out / "latest.json", report)
    (out / "latest_summary.md").write_text(_summary("Portfolio Atlas P055 - Portfolio Data Import Readiness", report), encoding="utf-8")
    _write_csv(out / "import_readiness_matrix.csv", ["phase", "component", "classification", "rows_loaded"], report["import_readiness_matrix"])
    _write_csv(out / "rejected_files.csv", ["file", "reason"], report["rejected_files"])
    _write_csv(out / "missing_files.csv", ["required_file", "expected_relative_path", "status", "file", "reason"], report["missing_files"])
    _write_csv(out / "next_actions.csv", ["priority", "action", "reason"], report["next_actions"])
    return {"latest_json": out / "latest.json"}


def write_normalized_data_cache(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "normalized_data_cache")
    _write_json(out / "latest.json", report)
    (out / "latest_summary.md").write_text(_summary("Portfolio Atlas P056-P060 - Normalized Data Cache", report), encoding="utf-8")
    _write_csv(out / "expected_outputs.csv", ["file", "status"], report["expected_outputs"])
    _write_csv(out / "written_files.csv", ["file", "source", "status"], report["written_files"])
    return {"latest_json": out / "latest.json"}


def _validate_csv(path: Path, required: list[str], build: str, created_at: str | None, *, row_rules: list[Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return _validator_report(build, created_at, DATA_REQUIRED, [{"file": str(path), "reason": "Missing required file."}], [], 0)
    rows, fields = _read_rows(path)
    rejections = []
    missing_columns = [column for column in required if column not in fields]
    if missing_columns:
        rejections.append({"file": str(path), "reason": f"Missing required columns: {', '.join(missing_columns)}"})
    for rule in row_rules or []:
        rejections.extend(rule(path, rows))
    status = IMPORT_READY if not rejections else REJECTED
    return _validator_report(build, created_at, status, [], rejections, len(rows))


def _validator_report(build: str, created_at: str | None, status: str, missing: list[dict[str, Any]], rejections: list[dict[str, Any]], rows_loaded: int) -> dict[str, Any]:
    return {
        "schema_id": "portfolio_research_os_import_validator",
        "schema_version": "1.0",
        "build": build,
        "created_at": created_at or _now(),
        "status": status,
        "rows_loaded": rows_loaded,
        "missing_files": missing,
        "rejections": rejections,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _read_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = [{key: (value or "").strip() for key, value in row.items()} for row in reader]
            return rows, list(reader.fieldnames or [])
    except csv.Error:
        return [], []


def _csv_header(path: Path) -> list[str] | None:
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            return next(csv.reader(handle), None)
    except (csv.Error, UnicodeDecodeError):
        return None


def _reject_future_as_of_date(path: Path, rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rejections = []
    for index, row in enumerate(rows, start=2):
        as_of = _parse_date(row.get("as_of_date", ""))
        if as_of and as_of > TODAY:
            rejections.append({"file": str(path), "reason": f"Row {index} has future as_of_date {as_of.isoformat()}."})
    return rejections


def _reject_future_filing(path: Path, rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rejections = []
    for index, row in enumerate(rows, start=2):
        as_of = _parse_date(row.get("as_of_date", ""))
        filing = _parse_date(row.get("filing_date", ""))
        if as_of and filing and filing > as_of:
            rejections.append({"file": str(path), "reason": f"Row {index} has filing_date {filing.isoformat()} after as_of_date {as_of.isoformat()}."})
    return rejections


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _price_only_explicit(root: Path) -> bool:
    for name in ["PRICE_ONLY_CLASSIFICATION.txt", "price_only_classification.txt"]:
        path = root / "raw" / "prices" / name
        if path.exists():
            text = path.read_text(encoding="utf-8").strip().lower()
            return "price_only_explicit" in text or "price_only=true" in text
    return False


def _all_blank(rows: list[dict[str, str]], column: str) -> bool:
    return all(not row.get(column) for row in rows)


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    seen = set()
    for row in rows:
        marker = json.dumps(row, sort_keys=True)
        if marker not in seen:
            seen.add(marker)
            out.append(row)
    return out


def _next_actions(status: str, rejected: list[dict[str, Any]], missing: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if status == IMPORT_READY:
        return [{"priority": 1, "action": "Run normalized data cache builder.", "reason": "All import validators passed."}]
    actions = []
    if missing:
        actions.append({"priority": 1, "action": "Upload missing required files into data/portfolio_research_os/raw subfolders.", "reason": "Required source files are absent."})
    if rejected:
        actions.append({"priority": 2, "action": "Repair rejected files and rerun validators.", "reason": "One or more files failed schema or point-in-time checks."})
    if not actions:
        actions.append({"priority": 1, "action": "Rerun import readiness after source upload.", "reason": "Readiness is partial."})
    return actions


def _folder_purpose(path: Path) -> str:
    name = str(path)
    if "/raw/" in name:
        return "Vendor/source file drop zone."
    if name.endswith("/normalized"):
        return "Validated normalized CSV output."
    if name.endswith("/cache"):
        return "Local derived cache; no authority."
    return "Portfolio Research OS data root."


def _price_template() -> str:
    return "# Price Data Request\n\nProvide daily adjusted OHLCV, dividends, splits, delisted-security price history, and explicit corporate-action methodology.\n"


def _fundamental_template() -> str:
    return "# Fundamental Data Request\n\nProvide point-in-time fundamentals with report_date and filing_date. Future filings must not be visible before filing_date.\n"


def _universe_template() -> str:
    return "# Universe Data Request\n\nProvide point-in-time universe membership, delisted securities, sector/industry mappings, security_id, active/delisted flags, and eligibility fields.\n"


def _benchmark_template() -> str:
    return "# Benchmark Data Request\n\nProvide total-return series for VTI, 60_40_PROXY, SIMPLE_FACTOR_PROXY, OAK_HARVEST_PROXY, and CASH_OR_RISK_FREE. Label assumptions explicitly.\n"


def _risk_free_template() -> str:
    return "# Risk-Free Rate Request\n\nProvide daily or monthly risk-free rates with date, rate, source, tenor, and compounding convention.\n"


def _summary(title: str, report: dict[str, Any]) -> str:
    lines = [f"# {title}", "", f"- Status: {report.get('status')}", f"- Authority: {AUTHORITY_TEXT}", ""]
    if report.get("missing_files"):
        lines.append("## Missing Files")
        lines.extend(f"- {row.get('required_file') or row.get('file')}: {row.get('reason') or row.get('expected_relative_path')}" for row in report["missing_files"][:20])
        lines.append("")
    if report.get("rejections") or report.get("rejected_files"):
        lines.append("## Rejections")
        lines.extend(f"- {row.get('file')}: {row.get('reason')}" for row in (report.get("rejections") or report.get("rejected_files") or [])[:20])
        lines.append("")
    return "\n".join(lines)


def _out_dir(report_root: Path | str, key: str) -> Path:
    out = Path(report_root) / REPORT_DIRS[key]
    out.mkdir(parents=True, exist_ok=True)
    return out


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
