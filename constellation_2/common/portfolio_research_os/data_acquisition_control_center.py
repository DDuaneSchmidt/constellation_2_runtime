from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .data_import_package import (
    DEFAULT_DATA_ROOT,
    DEFAULT_REPORT_ROOT,
    FUNDAMENTAL_COLUMNS,
    PRICE_COLUMNS,
    REQUIRED_FILES,
    SECTOR_COLUMNS,
    UNIVERSE_COLUMNS,
    build_normalized_data_cache,
    build_portfolio_data_import_readiness,
    build_raw_data_import_scanner,
    run_import_folder_structure,
)
from .data_source_inventory import AUTHORITY_BOUNDARY, AUTHORITY_TEXT


MISSING = "MISSING"
RECEIVED = "RECEIVED"
VALIDATED = "VALIDATED"
REJECTED = "REJECTED"
NORMALIZED = "NORMALIZED"
READY = "READY"

READY_TO_BUY_DATA = "READY_TO_BUY_DATA"
READY_FOR_PRICE_ONLY_BASELINE = "READY_FOR_PRICE_ONLY_BASELINE"
DATA_REQUIREMENTS_INCOMPLETE = "DATA_REQUIREMENTS_INCOMPLETE"
BLOCKED = "BLOCKED"

PRICE_ONLY_LABEL = "PRICE_ONLY_BASELINE_NOT_FULL_PORTFOLIO_ATLAS"

REPORT_DIRS = {
    "data_acquisition_control_center": "data_acquisition_control_center",
    "vendor_evaluation_matrix": "vendor_evaluation_matrix",
    "data_purchase_decision": "data_purchase_decision",
    "minimum_viable_dataset": "minimum_viable_dataset",
    "price_only_baseline_path": "price_only_baseline_path",
    "full_model_data_path": "full_model_data_path",
    "data_import_checklist": "data_import_checklist",
    "data_dictionary": "data_dictionary",
    "data_acquisition_faq": "data_acquisition_faq",
    "data_acquisition_dry_run": "data_acquisition_dry_run",
    "data_acquisition_program_review": "data_acquisition_program_review",
}

REQUIRED_SEVEN_FILES = list(REQUIRED_FILES.keys())

TEMPLATE_COLUMNS = {
    "pit_universe_template.csv": UNIVERSE_COLUMNS,
    "daily_prices_template.csv": PRICE_COLUMNS,
    "fundamentals_pit_template.csv": FUNDAMENTAL_COLUMNS,
    "dividends_splits_template.csv": ["date", "ticker", "dividend", "split_factor", "source"],
    "benchmark_returns_template.csv": ["date", "benchmark_id", "return", "total_return", "fee_adjusted_return", "source", "assumption_flag"],
    "risk_free_rates_template.csv": ["date", "rate", "tenor", "compounding", "source"],
    "sector_industry_map_template.csv": SECTOR_COLUMNS,
}


def run_data_acquisition_control_center(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_acquisition_control_center(data_root=data_root, created_at=created_at)
    write_data_acquisition_control_center(report, report_root)
    return report


def run_vendor_evaluation_matrix(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_vendor_evaluation_matrix(created_at=created_at)
    write_vendor_evaluation_matrix(report, report_root)
    return report


def run_data_purchase_decision(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_purchase_decision(created_at=created_at)
    write_data_purchase_decision(report, report_root)
    return report


def run_minimum_viable_dataset(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_minimum_viable_dataset(created_at=created_at)
    write_minimum_viable_dataset(report, report_root)
    return report


def run_price_only_baseline_path(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_price_only_baseline_path(created_at=created_at)
    write_price_only_baseline_path(report, report_root)
    return report


def run_full_model_data_path(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_full_model_data_path(created_at=created_at)
    write_full_model_data_path(report, report_root)
    return report


def run_data_import_checklist(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_import_checklist(created_at=created_at)
    write_data_import_checklist(report, report_root)
    return report


def run_example_csv_templates(data_root: Path | str = DEFAULT_DATA_ROOT) -> dict[str, Any]:
    return write_example_csv_templates(data_root=data_root)


def run_data_dictionary(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_dictionary(created_at=created_at)
    write_data_dictionary(report, report_root)
    return report


def run_data_acquisition_faq(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_acquisition_faq(created_at=created_at)
    write_data_acquisition_faq(report, report_root)
    return report


def run_data_acquisition_dry_run(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_acquisition_dry_run(data_root=data_root, created_at=created_at)
    write_data_acquisition_dry_run(report, report_root)
    return report


def run_data_acquisition_program_review(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_acquisition_program_review(data_root=data_root, created_at=created_at)
    write_data_acquisition_program_review(report, report_root)
    return report


def run_data_acquisition_program(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    created = created_at or _now()
    reports = {
        "data_acquisition_control_center": run_data_acquisition_control_center(report_root, data_root, created_at=created),
        "vendor_evaluation_matrix": run_vendor_evaluation_matrix(report_root, created_at=created),
        "data_purchase_decision": run_data_purchase_decision(report_root, created_at=created),
        "minimum_viable_dataset": run_minimum_viable_dataset(report_root, created_at=created),
        "price_only_baseline_path": run_price_only_baseline_path(report_root, created_at=created),
        "full_model_data_path": run_full_model_data_path(report_root, created_at=created),
        "data_import_checklist": run_data_import_checklist(report_root, created_at=created),
        "example_csv_templates": run_example_csv_templates(data_root),
        "data_dictionary": run_data_dictionary(report_root, created_at=created),
        "data_acquisition_faq": run_data_acquisition_faq(report_root, created_at=created),
        "data_acquisition_dry_run": run_data_acquisition_dry_run(report_root, data_root, created_at=created),
        "data_acquisition_program_review": run_data_acquisition_program_review(report_root, data_root, created_at=created),
    }
    return {"status": reports["data_acquisition_program_review"]["decision"], "reports": reports, "authority_boundary": AUTHORITY_BOUNDARY}


def build_data_acquisition_control_center(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(data_root)
    readiness = build_portfolio_data_import_readiness(data_root=root, created_at=created_at)
    normalized = build_normalized_data_cache(data_root=root, created_at=created_at)
    rejected_paths = {Path(row.get("file", "")).name for row in readiness.get("rejected_files", [])}
    validated = readiness["status"] in {"IMPORT_READY"}
    normalized_names = {Path(row.get("source", "")).name for row in normalized.get("written_files", [])}
    dashboard = []
    for name in REQUIRED_SEVEN_FILES:
        relative = REQUIRED_FILES[name][0]
        path = root / relative
        if name in normalized_names:
            classification = NORMALIZED
        elif validated and path.exists():
            classification = VALIDATED
        elif name in rejected_paths:
            classification = REJECTED
        elif path.exists():
            classification = RECEIVED
        else:
            classification = MISSING
        dashboard.append({"required_file": name, "expected_relative_path": relative, "classification": classification, "exists": path.exists()})
    missing_count = sum(1 for row in dashboard if row["classification"] == MISSING)
    rejected_count = sum(1 for row in dashboard if row["classification"] == REJECTED)
    status = READY if all(row["classification"] in {VALIDATED, NORMALIZED} for row in dashboard) else REJECTED if rejected_count else MISSING
    return {
        "schema_id": "portfolio_research_os_data_acquisition_control_center",
        "schema_version": "1.0",
        "build": "Portfolio Atlas D001",
        "created_at": created_at or _now(),
        "status": status,
        "data_requirements_dashboard": dashboard,
        "acquisition_status": [{"classification": row["classification"], "required_file": row["required_file"], "path": row["expected_relative_path"]} for row in dashboard],
        "blocker_summary": _blockers(missing_count, rejected_count),
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_vendor_evaluation_matrix(*, created_at: str | None = None) -> dict[str, Any]:
    vendors = [
        ("EOD Historical Data", "partial", "partial", "yes", "yes", "partial", "ETF/proxy", "$", "API/export", "medium"),
        ("Nasdaq Data Link", "vendor-dependent", "vendor-dependent", "yes", "yes", "yes", "yes", "$$-$$$", "API/export", "low-medium"),
        ("Tiingo", "no", "partial", "yes", "yes", "limited", "ETF", "$", "API", "medium"),
        ("Polygon", "no", "partial", "yes", "yes", "limited", "ETF", "$$", "API", "medium"),
        ("Financial Modeling Prep", "limited", "partial", "yes", "yes", "yes", "ETF", "$", "API/export", "medium-high"),
        ("Alpha Vantage", "no", "limited", "yes", "limited", "limited", "ETF", "free-$", "API", "high"),
        ("Norgate Data", "no", "yes", "yes", "yes", "yes", "ETF/index", "$$", "export", "low"),
        ("Sharadar / Nasdaq Data Link", "yes", "yes", "yes", "yes", "yes", "ETF/proxy", "$$", "API/export", "low"),
        ("Koyfin export", "limited", "limited", "yes", "yes", "yes", "yes", "$$", "export", "medium"),
        ("Portfolio123 export", "yes", "yes", "yes", "yes", "yes", "yes", "$$", "export", "low-medium"),
    ]
    columns = ["vendor", "point_in_time_fundamentals", "delisted_securities", "adjusted_prices", "dividends_splits", "sector_mappings", "benchmark_returns", "cost", "api_export_support", "bias_risk"]
    matrix = [dict(zip(columns, row, strict=True)) for row in vendors]
    recommendation = [
        {"rank": 1, "vendor": "Sharadar / Nasdaq Data Link", "fit": "Best enough-for-V1 source-backed path", "reason": "Combines PIT fundamentals, delisted coverage, prices, actions, and export/API support."},
        {"rank": 2, "vendor": "Norgate Data", "fit": "Best survivorship-bias control for price/universe history", "reason": "Strong delisted and adjusted price coverage, weaker for fundamentals."},
        {"rank": 3, "vendor": "Portfolio123 export", "fit": "Practical low-friction export path", "reason": "Can stage PIT-style exports without building an API integration first."},
    ]
    return _report("portfolio_research_os_vendor_evaluation_matrix", "Portfolio Atlas D002", "READY_TO_BUY_DATA", created_at, vendor_matrix=matrix, vendor_recommendation=recommendation)


def build_data_purchase_decision(*, created_at: str | None = None) -> dict[str, Any]:
    options = [
        {"category": "Best low-friction option", "vendor_path": "Portfolio123 export or Koyfin export", "reason": "Fast manual export path for first source-backed staging."},
        {"category": "Best institutional-quality option", "vendor_path": "Sharadar / Nasdaq Data Link plus Norgate if needed", "reason": "Best PIT fundamentals and survivorship-bias controls."},
        {"category": "Best budget option", "vendor_path": "EOD Historical Data plus risk-free/benchmark exports", "reason": "Lower cost but requires explicit bias-risk documentation."},
        {"category": "Best enough-for-V1 option", "vendor_path": "Sharadar / Nasdaq Data Link", "reason": "Most complete single path for PIT fundamentals, delisted securities, actions, and exports."},
    ]
    recommended = [{"priority": 1, "recommended_path": "Buy/export Sharadar via Nasdaq Data Link first; supplement price/universe gaps only if validation rejects coverage.", "decision_status": "READY_TO_BUY_DATA"}]
    return _report("portfolio_research_os_data_purchase_decision", "Portfolio Atlas D003", "READY_TO_BUY_DATA", created_at, purchase_options=options, recommended_path=recommended)


def build_minimum_viable_dataset(*, created_at: str | None = None) -> dict[str, Any]:
    requirements = [
        {"data_category": "S&P 500 or Russell 1000 historical universe", "mvd_status": "REQUIRED", "file": "pit_universe.csv"},
        {"data_category": "daily adjusted prices", "mvd_status": "REQUIRED", "file": "daily_prices.csv"},
        {"data_category": "dividends/splits", "mvd_status": "REQUIRED", "file": "dividends_splits.csv"},
        {"data_category": "sector/industry mappings", "mvd_status": "REQUIRED", "file": "sector_industry_map.csv"},
        {"data_category": "benchmark returns", "mvd_status": "REQUIRED", "file": "benchmark_returns.csv"},
        {"data_category": "risk-free rates", "mvd_status": "REQUIRED", "file": "risk_free_rates.csv"},
        {"data_category": "PIT fundamentals", "mvd_status": "REQUIRED_FOR_FULL_MODEL", "file": "fundamentals_pit.csv"},
    ]
    baseline = [
        {"path": "price_only_baseline", "fundamentals": "OPTIONAL_FOR_PRICE_ONLY_BASELINE", "allowed": "momentum,risk,income_if_dividend_data_exists"},
        {"path": "full_model", "fundamentals": "REQUIRED_FOR_FULL_MODEL", "allowed": "growth,quality,valuation,PEG,PEGY after PIT validation"},
    ]
    return _report("portfolio_research_os_minimum_viable_dataset", "Portfolio Atlas D004", "READY_TO_BUY_DATA", created_at, mvd_requirements=requirements, baseline_vs_full_model=baseline)


def build_price_only_baseline_path(*, created_at: str | None = None) -> dict[str, Any]:
    allowed = [{"factor": factor, "status": "ALLOWED"} for factor in ["momentum", "risk", "income_if_dividend_data_exists"]]
    blocked = [{"factor": factor, "status": "BLOCKED_WITHOUT_PIT_FUNDAMENTALS"} for factor in ["growth", "quality", "valuation", "PEG", "PEGY"]]
    return _report(
        "portfolio_research_os_price_only_baseline_path",
        "Portfolio Atlas D005",
        PRICE_ONLY_LABEL,
        created_at,
        label=PRICE_ONLY_LABEL,
        purpose="Test Portfolio Atlas plumbing before full fundamentals arrive.",
        allowed_factors=allowed,
        blocked_factors=blocked,
    )


def build_full_model_data_path(*, created_at: str | None = None) -> dict[str, Any]:
    requirements = [
        {"requirement": "price history", "file": "daily_prices.csv", "status": "REQUIRED"},
        {"requirement": "dividends/splits", "file": "dividends_splits.csv", "status": "REQUIRED"},
        {"requirement": "PIT fundamentals with filing_date", "file": "fundamentals_pit.csv", "status": "REQUIRED"},
        {"requirement": "PIT universe membership", "file": "pit_universe.csv", "status": "REQUIRED"},
        {"requirement": "sector/industry mappings", "file": "sector_industry_map.csv", "status": "REQUIRED"},
        {"requirement": "benchmarks", "file": "benchmark_returns.csv", "status": "REQUIRED"},
        {"requirement": "risk-free rates", "file": "risk_free_rates.csv", "status": "REQUIRED"},
    ]
    blockers = [{"blocker": row["requirement"], "file": row["file"], "impact": "Full Portfolio Atlas V1 remains DATA_REQUIRED until source-backed and validated."} for row in requirements]
    return _report("portfolio_research_os_full_model_data_path", "Portfolio Atlas D006", "DATA_REQUIRED", created_at, full_model_requirements=requirements, full_model_blockers=blockers)


def build_data_import_checklist(*, created_at: str | None = None) -> dict[str, Any]:
    rows = []
    columns_by_file = _columns_by_file()
    for filename, (relative, _) in REQUIRED_FILES.items():
        rows.append({
            "where_to_put_file": f"data/portfolio_research_os/{relative}",
            "expected_file_name": filename,
            "required_columns": ",".join(columns_by_file[filename]),
            "validation_command": "python3 -m constellation_2.common.portfolio_research_os.cli --portfolio-data-import-readiness",
            "expected_success_output": "IMPORT_READY or READY_FOR_FIRST_BACKTEST after all validators and cache pass.",
            "common_failure_reasons": "missing columns; empty file; future filing leakage; missing dividends/splits; benchmark assumption not labeled",
        })
    return _report("portfolio_research_os_data_import_checklist", "Portfolio Atlas D007", "READY_TO_BUY_DATA", created_at, data_import_checklist=rows)


def write_example_csv_templates(data_root: Path | str = DEFAULT_DATA_ROOT) -> dict[str, Any]:
    out = Path(data_root) / "raw" / "templates"
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for filename, columns in TEMPLATE_COLUMNS.items():
        path = out / filename
        _write_csv(path, columns, [])
        written.append({"file": str(path), "columns": ",".join(columns), "status": "WRITTEN"})
    return {"schema_id": "portfolio_research_os_example_csv_templates", "schema_version": "1.0", "build": "Portfolio Atlas D008", "status": "WRITTEN", "templates": written, "authority_boundary": AUTHORITY_BOUNDARY}


def build_data_dictionary(*, created_at: str | None = None) -> dict[str, Any]:
    fields = []
    descriptions = {
        "as_of_date": "Date on which the row is visible to the backtest.",
        "filing_date": "Actual filing availability date; prevents future filing leakage.",
        "adjusted_close": "Corporate-action-adjusted close required for total-return comparison.",
        "assumption_flag": "Labels sourced versus user-assumption benchmark rows.",
    }
    for filename, columns in _columns_by_file().items():
        for column in columns:
            fields.append({"file": filename, "field": column, "definition": descriptions.get(column, f"{column} field required by the {filename} import contract.")})
    return _report("portfolio_research_os_data_dictionary", "Portfolio Atlas D009", "READY_TO_BUY_DATA", created_at, field_dictionary=fields)


def build_data_acquisition_faq(*, created_at: str | None = None) -> dict[str, Any]:
    questions = [
        ("What do I need to buy?", "A source-backed historical universe, adjusted daily prices, dividends/splits, benchmarks, risk-free rates, sector mappings, and PIT fundamentals for the full model."),
        ("Why do we need point-in-time data?", "It prevents the backtest from using information that was not available on the historical decision date."),
        ("Why do delisted stocks matter?", "They reduce survivorship bias by keeping failed or acquired securities in the historical universe."),
        ("Can we start with price-only data?", f"Yes, but it must be labeled {PRICE_ONLY_LABEL} and cannot use fundamental factors."),
        ("What happens if fundamentals are missing?", "The full model remains DATA_REQUIRED; missing fundamentals do not create fake scores."),
        ("What vendor should I use first?", "Sharadar / Nasdaq Data Link is the recommended enough-for-V1 path; Portfolio123 export is a low-friction alternative."),
    ]
    return _report("portfolio_research_os_data_acquisition_faq", "Portfolio Atlas D010", "READY_TO_BUY_DATA", created_at, faq=[{"question": q, "answer": a} for q, a in questions])


def build_data_acquisition_dry_run(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(data_root)
    run_import_folder_structure(data_root=root, report_root=Path("/tmp/portfolio_research_os_dry_run_reports"), created_at=created_at)
    templates = write_example_csv_templates(root)
    scanner = build_raw_data_import_scanner(data_root=root, created_at=created_at)
    rows = [
        {"check": "folder_structure", "status": "PASS", "detail": "Required folders exist."},
        {"check": "example_templates", "status": "PASS" if templates["templates"] else "FAIL", "detail": "Example CSV templates written."},
        {"check": "raw_required_files", "status": scanner["status"], "detail": "Dry run checks presence only; it does not claim data readiness."},
    ]
    return _report("portfolio_research_os_data_acquisition_dry_run", "Portfolio Atlas D011-D015", "DATA_REQUIRED", created_at, dry_run_status=rows)


def build_data_acquisition_program_review(data_root: Path | str = DEFAULT_DATA_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    control = build_data_acquisition_control_center(data_root=data_root, created_at=created_at)
    missing = [row for row in control["data_requirements_dashboard"] if row["classification"] == MISSING]
    rejected = [row for row in control["data_requirements_dashboard"] if row["classification"] == REJECTED]
    decision = READY_TO_BUY_DATA if missing and not rejected else BLOCKED if rejected else READY_FOR_PRICE_ONLY_BASELINE
    next_action = "Buy or export the seven required source files, starting with Sharadar / Nasdaq Data Link or Portfolio123 export."
    return _report(
        "portfolio_research_os_data_acquisition_program_review",
        "Portfolio Atlas D016-D020",
        decision,
        created_at,
        decision=decision,
        program_decision=[{"decision": decision, "missing_file_count": len(missing), "rejected_file_count": len(rejected)}],
        recommended_next_action=[{"priority": 1, "action": next_action, "reason": "Data requirements are defined and the raw drop-zone is ready."}],
    )


def write_data_acquisition_control_center(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = _out_dir(report_root, "data_acquisition_control_center")
    _write_json(out / "latest.json", report)
    _write_summary(out / "latest_summary.md", "Portfolio Atlas D001 - Data Acquisition Control Center", report)
    _write_csv(out / "data_requirements_dashboard.csv", ["required_file", "expected_relative_path", "classification", "exists"], report["data_requirements_dashboard"])
    _write_csv(out / "acquisition_status.csv", ["classification", "required_file", "path"], report["acquisition_status"])
    _write_csv(out / "blocker_summary.csv", ["blocker", "count", "action"], report["blocker_summary"])


def write_vendor_evaluation_matrix(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = _out_dir(report_root, "vendor_evaluation_matrix")
    _write_json(out / "latest.json", report)
    _write_summary(out / "latest_summary.md", "Portfolio Atlas D002 - Vendor Evaluation Matrix", report)
    _write_csv(out / "vendor_matrix.csv", list(report["vendor_matrix"][0].keys()), report["vendor_matrix"])
    _write_csv(out / "vendor_recommendation.csv", ["rank", "vendor", "fit", "reason"], report["vendor_recommendation"])


def write_data_purchase_decision(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = _out_dir(report_root, "data_purchase_decision")
    _write_json(out / "latest.json", report)
    _write_summary(out / "latest_summary.md", "Portfolio Atlas D003 - Data Purchase Decision", report)
    _write_csv(out / "purchase_options.csv", ["category", "vendor_path", "reason"], report["purchase_options"])
    _write_csv(out / "recommended_path.csv", ["priority", "recommended_path", "decision_status"], report["recommended_path"])


def write_minimum_viable_dataset(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = _out_dir(report_root, "minimum_viable_dataset")
    _write_json(out / "latest.json", report)
    _write_summary(out / "latest_summary.md", "Portfolio Atlas D004 - Minimum Viable Dataset", report)
    _write_csv(out / "mvd_requirements.csv", ["data_category", "mvd_status", "file"], report["mvd_requirements"])
    _write_csv(out / "baseline_vs_full_model.csv", ["path", "fundamentals", "allowed"], report["baseline_vs_full_model"])


def write_price_only_baseline_path(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = _out_dir(report_root, "price_only_baseline_path")
    _write_json(out / "latest.json", report)
    _write_summary(out / "latest_summary.md", "Portfolio Atlas D005 - Price-Only Baseline Path", report)
    _write_csv(out / "allowed_factors.csv", ["factor", "status"], report["allowed_factors"])
    _write_csv(out / "blocked_factors.csv", ["factor", "status"], report["blocked_factors"])


def write_full_model_data_path(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = _out_dir(report_root, "full_model_data_path")
    _write_json(out / "latest.json", report)
    _write_summary(out / "latest_summary.md", "Portfolio Atlas D006 - Full Model Data Path", report)
    _write_csv(out / "full_model_requirements.csv", ["requirement", "file", "status"], report["full_model_requirements"])
    _write_csv(out / "full_model_blockers.csv", ["blocker", "file", "impact"], report["full_model_blockers"])


def write_data_import_checklist(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = _out_dir(report_root, "data_import_checklist")
    _write_summary(out / "latest_summary.md", "Portfolio Atlas D007 - Data Import Checklist", report)
    _write_csv(out / "data_import_checklist.csv", ["where_to_put_file", "expected_file_name", "required_columns", "validation_command", "expected_success_output", "common_failure_reasons"], report["data_import_checklist"])


def write_data_dictionary(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = _out_dir(report_root, "data_dictionary")
    _write_json(out / "latest.json", report)
    _write_summary(out / "latest_summary.md", "Portfolio Atlas D009 - Data Dictionary", report)
    _write_csv(out / "field_dictionary.csv", ["file", "field", "definition"], report["field_dictionary"])


def write_data_acquisition_faq(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = _out_dir(report_root, "data_acquisition_faq")
    _write_summary(out / "latest_summary.md", "Portfolio Atlas D010 - Data Acquisition FAQ", report)


def write_data_acquisition_dry_run(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = _out_dir(report_root, "data_acquisition_dry_run")
    _write_json(out / "latest.json", report)
    _write_summary(out / "latest_summary.md", "Portfolio Atlas D011-D015 - Import Dry Run", report)
    _write_csv(out / "dry_run_status.csv", ["check", "status", "detail"], report["dry_run_status"])


def write_data_acquisition_program_review(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = _out_dir(report_root, "data_acquisition_program_review")
    _write_json(out / "latest.json", report)
    _write_summary(out / "latest_summary.md", "Portfolio Atlas D016-D020 - Acquisition Program Review", report)
    _write_csv(out / "program_decision.csv", ["decision", "missing_file_count", "rejected_file_count"], report["program_decision"])
    _write_csv(out / "recommended_next_action.csv", ["priority", "action", "reason"], report["recommended_next_action"])


def _report(schema_id: str, build: str, status: str, created_at: str | None, **payload: Any) -> dict[str, Any]:
    report = {"schema_id": schema_id, "schema_version": "1.0", "build": build, "created_at": created_at or _now(), "status": status, "authority_boundary": AUTHORITY_BOUNDARY}
    report.update(payload)
    return report


def _columns_by_file() -> dict[str, list[str]]:
    return {
        "pit_universe.csv": UNIVERSE_COLUMNS,
        "daily_prices.csv": PRICE_COLUMNS,
        "fundamentals_pit.csv": FUNDAMENTAL_COLUMNS,
        "dividends_splits.csv": ["date", "ticker", "dividend", "split_factor", "source"],
        "benchmark_returns.csv": ["date", "benchmark_id", "return", "total_return", "fee_adjusted_return", "source", "assumption_flag"],
        "risk_free_rates.csv": ["date", "rate", "tenor", "compounding", "source"],
        "sector_industry_map.csv": SECTOR_COLUMNS,
    }


def _blockers(missing_count: int, rejected_count: int) -> list[dict[str, Any]]:
    rows = []
    if missing_count:
        rows.append({"blocker": "missing_required_files", "count": missing_count, "action": "Acquire or export source-backed CSV files."})
    if rejected_count:
        rows.append({"blocker": "rejected_required_files", "count": rejected_count, "action": "Repair rejected data and rerun validators."})
    if not rows:
        rows.append({"blocker": "none", "count": 0, "action": "Proceed to import readiness and normalized cache validation."})
    return rows


def _write_summary(path: Path, title: str, report: dict[str, Any]) -> None:
    lines = [f"# {title}", "", f"- Status: {report.get('status')}", f"- Authority: {AUTHORITY_TEXT}", ""]
    if "decision" in report:
        lines.append(f"- Decision: {report['decision']}")
    if "label" in report:
        lines.append(f"- Label: {report['label']}")
    if "faq" in report:
        for row in report["faq"]:
            lines.extend([f"## {row['question']}", row["answer"], ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _out_dir(report_root: Path | str, key: str) -> Path:
    out = Path(report_root) / REPORT_DIRS[key]
    out.mkdir(parents=True, exist_ok=True)
    return out


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
