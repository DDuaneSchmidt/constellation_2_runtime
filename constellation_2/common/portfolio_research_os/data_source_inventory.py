from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable


DEFAULT_REPORT_ROOT = Path("reports/portfolio_research_os")
DATA_REQUIRED = "DATA_REQUIRED"
DATA_READY = "DATA_READY"
PARTIALLY_READY = "PARTIALLY_READY"
BIAS_RISK = "BIAS_RISK"
USER_ASSUMPTION = "USER_ASSUMPTION"

AUTHORITY_TEXT = "Research-only. No live portfolio, no replacement recommendation, no trades, no broker execution."
AUTHORITY_BOUNDARY = {
    "research_only": True,
    "live_portfolio_authorized": False,
    "replacement_recommendation_authorized": False,
    "trade_recommendation_authorized": False,
    "broker_execution_authorized": False,
    "performance_claim_authorized": False,
}

REPORT_DIRS = {
    "data_source_inventory": "data_source_inventory",
    "point_in_time_universe_contract": "point_in_time_universe_contract",
    "historical_price_return_contract": "historical_price_return_contract",
    "fundamental_data_contract": "fundamental_data_contract",
    "benchmark_data_contract": "benchmark_data_contract",
    "data_readiness_scorecard": "data_readiness_scorecard",
    "data_acquisition_plan": "data_acquisition_plan",
    "post_import_validation_plan": "post_import_validation_plan",
}

SOURCE_CATEGORIES = {
    "price_history": ("price", "prices", "ohlcv", "adjusted_close", "adj_close"),
    "monthly_returns": ("monthly_return", "monthly_returns"),
    "daily_returns": ("daily_return", "daily_returns", "total_return"),
    "fundamentals": ("fundamental", "fundamentals", "financials", "filing_date"),
    "dividends": ("dividend", "dividends"),
    "splits": ("split", "splits", "corporate_action", "corporate_actions"),
    "benchmark_returns": ("benchmark", "benchmarks", "benchmark_returns"),
    "ETF_returns": ("etf", "vti", "spy", "qqq"),
    "universe_membership": ("universe", "membership", "constituent", "constituents"),
    "sector_industry_mappings": ("sector", "industry", "gics", "sic", "naics"),
}

PIT_UNIVERSE_FIELDS = [
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

PRICE_RETURN_FIELDS = [
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume",
    "dividend",
    "split_factor",
    "total_return",
    "source",
]

FUNDAMENTAL_FIELDS = [
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
    "source",
]

BENCHMARK_FIELDS = [
    "date",
    "benchmark_id",
    "return",
    "total_return",
    "fee_adjusted_return",
    "source",
    "assumption_flag",
]

BENCHMARKS = ["VTI", "60/40 proxy", "simple factor portfolio", "Oak Harvest proxy", "cash / risk-free proxy"]


def run_data_source_inventory(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_source_inventory(report_root, created_at=created_at)
    write_data_source_inventory(report, report_root)
    return report


def run_point_in_time_universe_contract(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_point_in_time_universe_contract(created_at=created_at)
    write_point_in_time_universe_contract(report, report_root)
    return report


def run_historical_price_return_contract(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_historical_price_return_contract(created_at=created_at)
    write_historical_price_return_contract(report, report_root)
    return report


def run_fundamental_data_contract(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_fundamental_data_contract(created_at=created_at)
    write_fundamental_data_contract(report, report_root)
    return report


def run_benchmark_data_contract(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_benchmark_data_contract(created_at=created_at)
    write_benchmark_data_contract(report, report_root)
    return report


def run_data_readiness_scorecard(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_readiness_scorecard(report_root, created_at=created_at)
    write_data_readiness_scorecard(report, report_root)
    return report


def run_data_acquisition_plan(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_acquisition_plan(created_at=created_at)
    write_data_acquisition_plan(report, report_root)
    return report


def run_post_import_validation_plan(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_post_import_validation_plan(created_at=created_at)
    write_post_import_validation_plan(report, report_root)
    return report


def build_data_source_inventory(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(report_root)
    created = created_at or _now()
    candidates = _candidate_data_roots(root)
    files = _scan_source_files(candidates)
    available = []
    missing = []
    for category, hints in SOURCE_CATEGORIES.items():
        matches = [_file_row(path, category) for path in files if _matches_category(path, hints)]
        if matches:
            available.extend(matches)
        else:
            missing.append(
                {
                    "data_category": category,
                    "status": DATA_REQUIRED,
                    "required_for": _required_for(category),
                    "reason": f"No local source file matched hints: {', '.join(hints)}",
                }
            )
    priority = _priority_rows(missing)
    status = DATA_READY if not missing else PARTIALLY_READY if available else DATA_REQUIRED
    blockers = [row["data_category"] for row in missing if row["data_category"] in _critical_categories()]
    return {
        "schema_id": "portfolio_research_os_data_source_inventory",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P013",
        "created_at": created,
        "status": status,
        "scanned_roots": [str(path) for path in candidates],
        "available_sources": available,
        "missing_sources": missing,
        "data_source_priority": priority,
        "summary": {
            "available_category_count": len({row["data_category"] for row in available}),
            "missing_category_count": len(missing),
            "critical_blockers": blockers,
            "authority": AUTHORITY_TEXT,
        },
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_point_in_time_universe_contract(*, created_at: str | None = None) -> dict[str, Any]:
    rules = [
        _rule("as_of_date", "required", "Every row must represent the universe state known on this date.", "BLOCKER"),
        _rule("security_id", "required", "Ticker reuse must not collapse historical securities.", "BLOCKER"),
        _rule("is_delisted", "required", "Delisted securities must remain visible point-in-time.", "BLOCKER"),
        _rule("eligible_for_portfolio", "required", "Eligibility must be explicit and auditable.", "BLOCKER"),
        _rule("exclusion_reason", "conditional", "Required when eligible_for_portfolio is false.", "BLOCKER"),
    ]
    return {
        "schema_id": "portfolio_research_os_point_in_time_universe_contract",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P014",
        "created_at": created_at or _now(),
        "status": DATA_REQUIRED,
        "required_fields": PIT_UNIVERSE_FIELDS,
        "universe_schema": _schema_rows(PIT_UNIVERSE_FIELDS, _pit_types()),
        "universe_validation_rules": rules,
        "sample_universe_template": [{field: _sample_value(field) for field in PIT_UNIVERSE_FIELDS}],
        "critical_blockers": ["No point-in-time universe manifest", "Delisted membership not source-backed"],
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_historical_price_return_contract(*, created_at: str | None = None) -> dict[str, Any]:
    rules = [
        _rule("adjusted_close", "required", "Adjusted close is required for total-return comparison.", "BLOCKER"),
        _rule("dividend", "required_or_explicit_block", "Dividends must be included or explicitly blocked.", "BLOCKER"),
        _rule("split_factor", "required_or_explicit_block", "Splits must be handled or explicitly blocked.", "BLOCKER"),
        _rule("date", "calendar_check", "Missing expected trading dates must be reported by ticker.", "BLOCKER"),
        _rule("total_return", "derived_only", "Do not fabricate returns when adjusted_close/corporate actions are missing.", "BLOCKER"),
    ]
    return {
        "schema_id": "portfolio_research_os_historical_price_return_contract",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P015",
        "created_at": created_at or _now(),
        "status": DATA_REQUIRED,
        "required_fields": PRICE_RETURN_FIELDS,
        "price_schema": _schema_rows(PRICE_RETURN_FIELDS, _price_types()),
        "return_calculation_rules": rules,
        "data_quality_rules": [
            _rule("ohlc", "range_check", "high must be >= low; close/open must be non-negative.", "BLOCKER"),
            _rule("volume", "non_negative", "Volume must be zero or positive.", "WARNING"),
            _rule("source", "required", "Source vendor/file must be retained per row.", "BLOCKER"),
        ],
        "critical_blockers": ["No historical return data", "No dividend/split/corporate-action history"],
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_fundamental_data_contract(*, created_at: str | None = None) -> dict[str, Any]:
    rules = [
        _rule("filing_date", "required", "Filing date must be available for point-in-time gating.", "BLOCKER"),
        _rule("as_of_date", "no_future_filing", "as_of_date cannot use filings with filing_date after as_of_date.", "BLOCKER"),
        _rule("missing_fundamentals", "fail_closed", "Missing fundamentals produce DATA_REQUIRED, not fake scores.", "BLOCKER"),
    ]
    impacts = [
        {"field": field, "missing_impact": DATA_REQUIRED, "performance_allowed": False}
        for field in FUNDAMENTAL_FIELDS
        if field not in {"source"}
    ]
    return {
        "schema_id": "portfolio_research_os_fundamental_data_contract",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P016",
        "created_at": created_at or _now(),
        "status": DATA_REQUIRED,
        "required_fields": FUNDAMENTAL_FIELDS,
        "fundamental_schema": _schema_rows(FUNDAMENTAL_FIELDS, _fundamental_types()),
        "point_in_time_rules": rules,
        "missing_field_impact": impacts,
        "critical_blockers": ["No point-in-time fundamentals"],
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_benchmark_data_contract(*, created_at: str | None = None) -> dict[str, Any]:
    assumptions = []
    gaps = []
    for benchmark in BENCHMARKS:
        flag = USER_ASSUMPTION if benchmark == "Oak Harvest proxy" else DATA_REQUIRED
        assumptions.append(
            {
                "benchmark_id": benchmark,
                "assumption_flag": flag,
                "source_status": DATA_REQUIRED,
                "notes": "Oak Harvest remains USER_ASSUMPTION until source-backed data exists." if benchmark == "Oak Harvest proxy" else "Requires source-backed total-return series.",
            }
        )
        gaps.append({"benchmark_id": benchmark, "gap": "source-backed return history missing", "status": DATA_REQUIRED})
    return {
        "schema_id": "portfolio_research_os_benchmark_data_contract",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P017",
        "created_at": created_at or _now(),
        "status": DATA_REQUIRED,
        "required_fields": BENCHMARK_FIELDS,
        "benchmark_schema": _schema_rows(BENCHMARK_FIELDS, _benchmark_types()),
        "benchmark_assumptions": assumptions,
        "benchmark_gap_report": gaps,
        "critical_blockers": ["No benchmark return history"],
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_data_readiness_scorecard(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    created = created_at or _now()
    inventory = build_data_source_inventory(report_root, created_at=created)
    universe = build_point_in_time_universe_contract(created_at=created)
    prices = build_historical_price_return_contract(created_at=created)
    fundamentals = build_fundamental_data_contract(created_at=created)
    benchmarks = build_benchmark_data_contract(created_at=created)
    components = [
        _matrix_row("P013", "data_source_inventory", inventory["status"], "Local data discovery only."),
        _matrix_row("P014", "point_in_time_universe_contract", DATA_REQUIRED, "Contract exists; source manifest missing."),
        _matrix_row("P015", "historical_price_return_contract", DATA_REQUIRED, "Contract exists; price/corporate-action data missing."),
        _matrix_row("P016", "fundamental_data_contract", DATA_REQUIRED, "Contract exists; PIT fundamentals missing."),
        _matrix_row("P017", "benchmark_data_contract", DATA_REQUIRED, "Contract exists; benchmark returns missing."),
    ]
    blockers = [
        {"blocker": "No point-in-time universe manifest", "classification": DATA_REQUIRED, "required_action": "Import PIT universe file using P014 schema."},
        {"blocker": "No historical return data", "classification": DATA_REQUIRED, "required_action": "Import daily adjusted OHLCV and total-return data using P015 schema."},
        {"blocker": "No point-in-time fundamentals", "classification": DATA_REQUIRED, "required_action": "Import fundamentals with filing_date using P016 schema."},
        {"blocker": "No dividend/split/corporate-action history", "classification": DATA_REQUIRED, "required_action": "Import corporate action history or explicitly block total-return comparisons."},
        {"blocker": "No benchmark return history", "classification": DATA_REQUIRED, "required_action": "Import source-backed benchmark return series using P017 schema."},
        {"blocker": "Survivorship/lookahead bias risk remains", "classification": BIAS_RISK, "required_action": "Validate PIT membership, delistings, and filing-date gates before backtests."},
    ]
    actions = [
        {"priority": 1, "action": "Acquire point-in-time universe membership with delisted securities.", "unblocks": "P014"},
        {"priority": 2, "action": "Acquire adjusted daily OHLCV plus dividends and splits.", "unblocks": "P015"},
        {"priority": 3, "action": "Acquire fundamentals with report_date and filing_date.", "unblocks": "P016"},
        {"priority": 4, "action": "Acquire VTI, 60/40, factor, Oak Harvest, and risk-free benchmark return series.", "unblocks": "P017"},
    ]
    return {
        "schema_id": "portfolio_research_os_data_readiness_scorecard",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P018",
        "created_at": created,
        "status": DATA_REQUIRED,
        "readiness": DATA_REQUIRED,
        "readiness_matrix": components,
        "critical_blockers": blockers,
        "next_data_actions": actions,
        "source_reports": {
            "inventory_status": inventory["status"],
            "universe_contract_status": universe["status"],
            "price_contract_status": prices["status"],
            "fundamental_contract_status": fundamentals["status"],
            "benchmark_contract_status": benchmarks["status"],
        },
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_data_acquisition_plan(*, created_at: str | None = None) -> dict[str, Any]:
    categories = ["prices", "fundamentals", "dividends", "splits", "ETF returns", "benchmark returns", "risk-free rates"]
    requirements = [
        {
            "data_category": category,
            "priority": index,
            "required_history": "monthly and daily where available" if category != "fundamentals" else "quarterly/annual with filing dates",
            "required_fields_or_contract": _contract_for_category(category),
            "acceptance_criteria": "CSV/parquet files import locally; no external API call required.",
        }
        for index, category in enumerate(categories, start=1)
    ]
    return {
        "schema_id": "portfolio_research_os_data_acquisition_plan",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P019",
        "created_at": created_at or _now(),
        "status": DATA_REQUIRED,
        "vendor_requirements": requirements,
        "data_request_template": _vendor_request_template(),
        "import_folder_layout": _import_folder_layout(),
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_post_import_validation_plan(*, created_at: str | None = None) -> dict[str, Any]:
    commands = [
        "python3 -m constellation_2.common.portfolio_research_os.cli --data-source-inventory",
        "python3 -m constellation_2.common.portfolio_research_os.cli --data-readiness-scorecard",
        "python3 -m constellation_2.common.portfolio_research_os.cli --backtest-mvp",
        "python3 -m constellation_2.common.portfolio_research_os.cli --validation-framework",
    ]
    return {
        "schema_id": "portfolio_research_os_post_import_validation_plan",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P020",
        "created_at": created_at or _now(),
        "status": DATA_REQUIRED,
        "validation_sequence": [
            {"step": index, "command": command, "purpose": purpose}
            for index, (command, purpose) in enumerate(
                zip(
                    commands,
                    [
                        "Confirm local files are discoverable.",
                        "Classify remaining data blockers.",
                        "Run MVP only if readiness permits; otherwise fail closed.",
                        "Confirm validation framework and assumptions remain research-only.",
                    ],
                ),
                start=1,
            )
        ],
        "expected_outputs": [
            {"command": command, "expected_output": "latest.json plus latest_summary.md under reports/portfolio_research_os", "success_condition": "No fake performance; blockers remain explicit."}
            for command in commands
        ],
        "failure_handling": [
            {"failure": "DATA_REQUIRED", "handling": "Stop backtest interpretation and import missing source data."},
            {"failure": "BIAS_RISK", "handling": "Repair PIT membership, delisting, filing-date, or corporate-action coverage before comparisons."},
            {"failure": "USER_ASSUMPTION", "handling": "Do not compare against Oak Harvest as source-backed performance."},
        ],
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def write_data_source_inventory(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "data_source_inventory")
    paths = _standard_paths(out)
    _write_json(paths["latest_json"], report)
    paths["latest_summary"].write_text(_summary("Portfolio Atlas P013 - Data Source Inventory", report), encoding="utf-8")
    _write_csv(paths["available_sources"], ["data_category", "path", "file_name", "suffix", "size_bytes", "status"], report["available_sources"])
    _write_csv(paths["missing_sources"], ["data_category", "status", "required_for", "reason"], report["missing_sources"])
    _write_csv(paths["data_source_priority"], ["priority", "data_category", "classification", "next_action"], report["data_source_priority"])
    return paths


def write_point_in_time_universe_contract(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "point_in_time_universe_contract")
    paths = _standard_paths(out)
    _write_json(paths["latest_json"], report)
    paths["latest_summary"].write_text(_summary("Portfolio Atlas P014 - Point-in-Time Universe Contract", report), encoding="utf-8")
    _write_csv(out / "universe_schema.csv", ["field", "type", "required", "description"], report["universe_schema"])
    _write_csv(out / "universe_validation_rules.csv", ["field", "rule", "description", "severity"], report["universe_validation_rules"])
    _write_csv(out / "sample_universe_template.csv", PIT_UNIVERSE_FIELDS, report["sample_universe_template"])
    return paths


def write_historical_price_return_contract(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "historical_price_return_contract")
    paths = _standard_paths(out)
    _write_json(paths["latest_json"], report)
    paths["latest_summary"].write_text(_summary("Portfolio Atlas P015 - Historical Price Return Contract", report), encoding="utf-8")
    _write_csv(out / "price_schema.csv", ["field", "type", "required", "description"], report["price_schema"])
    _write_csv(out / "return_calculation_rules.csv", ["field", "rule", "description", "severity"], report["return_calculation_rules"])
    _write_csv(out / "data_quality_rules.csv", ["field", "rule", "description", "severity"], report["data_quality_rules"])
    return paths


def write_fundamental_data_contract(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "fundamental_data_contract")
    paths = _standard_paths(out)
    _write_json(paths["latest_json"], report)
    paths["latest_summary"].write_text(_summary("Portfolio Atlas P016 - Fundamental Data Contract", report), encoding="utf-8")
    _write_csv(out / "fundamental_schema.csv", ["field", "type", "required", "description"], report["fundamental_schema"])
    _write_csv(out / "point_in_time_rules.csv", ["field", "rule", "description", "severity"], report["point_in_time_rules"])
    _write_csv(out / "missing_field_impact.csv", ["field", "missing_impact", "performance_allowed"], report["missing_field_impact"])
    return paths


def write_benchmark_data_contract(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "benchmark_data_contract")
    paths = _standard_paths(out)
    _write_json(paths["latest_json"], report)
    paths["latest_summary"].write_text(_summary("Portfolio Atlas P017 - Benchmark Data Contract", report), encoding="utf-8")
    _write_csv(out / "benchmark_schema.csv", ["field", "type", "required", "description"], report["benchmark_schema"])
    _write_csv(out / "benchmark_assumptions.csv", ["benchmark_id", "assumption_flag", "source_status", "notes"], report["benchmark_assumptions"])
    _write_csv(out / "benchmark_gap_report.csv", ["benchmark_id", "gap", "status"], report["benchmark_gap_report"])
    return paths


def write_data_readiness_scorecard(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "data_readiness_scorecard")
    paths = _standard_paths(out)
    _write_json(paths["latest_json"], report)
    paths["latest_summary"].write_text(_summary("Portfolio Atlas P018 - Data Readiness Scorecard", report), encoding="utf-8")
    _write_csv(out / "readiness_matrix.csv", ["phase", "component", "classification", "evidence"], report["readiness_matrix"])
    _write_csv(out / "critical_blockers.csv", ["blocker", "classification", "required_action"], report["critical_blockers"])
    _write_csv(out / "next_data_actions.csv", ["priority", "action", "unblocks"], report["next_data_actions"])
    return paths


def write_data_acquisition_plan(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "data_acquisition_plan")
    paths = _standard_paths(out)
    _write_json(paths["latest_json"], report)
    paths["latest_summary"].write_text(_summary("Portfolio Atlas P019 - Vendor/Data Acquisition Plan", report), encoding="utf-8")
    _write_csv(out / "vendor_requirements.csv", ["data_category", "priority", "required_history", "required_fields_or_contract", "acceptance_criteria"], report["vendor_requirements"])
    (out / "data_request_template.md").write_text(report["data_request_template"], encoding="utf-8")
    (out / "import_folder_layout.md").write_text(report["import_folder_layout"], encoding="utf-8")
    return paths


def write_post_import_validation_plan(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = _out_dir(report_root, "post_import_validation_plan")
    paths = _standard_paths(out)
    _write_json(paths["latest_json"], report)
    paths["latest_summary"].write_text(_summary("Portfolio Atlas P020 - Post-Import Validation Plan", report), encoding="utf-8")
    _write_csv(out / "validation_sequence.csv", ["step", "command", "purpose"], report["validation_sequence"])
    _write_csv(out / "expected_outputs.csv", ["command", "expected_output", "success_condition"], report["expected_outputs"])
    _write_csv(out / "failure_handling.csv", ["failure", "handling"], report["failure_handling"])
    return paths


def _candidate_data_roots(report_root: Path) -> list[Path]:
    roots = [
        report_root / "input",
        report_root / "inputs",
        Path("data/portfolio_research_os"),
        Path("data/cache"),
        Path("data"),
    ]
    return [path for index, path in enumerate(roots) if path.exists() and path not in roots[:index]]


def _scan_source_files(roots: Iterable[Path]) -> list[Path]:
    files: list[Path] = []
    allowed = {".csv", ".json", ".jsonl", ".parquet", ".tsv"}
    for root in roots:
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in allowed:
                files.append(path)
    return sorted(set(files), key=lambda item: str(item))


def _matches_category(path: Path, hints: Iterable[str]) -> bool:
    text = str(path).lower()
    return any(hint.lower() in text for hint in hints)


def _file_row(path: Path, category: str) -> dict[str, Any]:
    return {
        "data_category": category,
        "path": str(path),
        "file_name": path.name,
        "suffix": path.suffix.lower(),
        "size_bytes": path.stat().st_size,
        "status": PARTIALLY_READY,
    }


def _critical_categories() -> set[str]:
    return {"price_history", "daily_returns", "fundamentals", "dividends", "splits", "benchmark_returns", "universe_membership"}


def _required_for(category: str) -> str:
    mapping = {
        "price_history": "historical return calculation",
        "monthly_returns": "backtest series aggregation",
        "daily_returns": "total-return and missing-date checks",
        "fundamentals": "factor scoring without fake scores",
        "dividends": "total-return adjustment",
        "splits": "total-return adjustment",
        "benchmark_returns": "benchmark comparison",
        "ETF_returns": "VTI/ETF benchmark comparison",
        "universe_membership": "survivorship-bias control",
        "sector_industry_mappings": "portfolio constraints and exposure audit",
    }
    return mapping.get(category, "data readiness")


def _priority_rows(missing: list[dict[str, Any]]) -> list[dict[str, Any]]:
    order = [
        "universe_membership",
        "price_history",
        "daily_returns",
        "dividends",
        "splits",
        "fundamentals",
        "benchmark_returns",
        "ETF_returns",
        "monthly_returns",
        "sector_industry_mappings",
    ]
    rows = []
    missing_by_category = {row["data_category"]: row for row in missing}
    for priority, category in enumerate(order, start=1):
        if category in missing_by_category:
            rows.append({"priority": priority, "data_category": category, "classification": DATA_REQUIRED, "next_action": f"Import source-backed {category.replace('_', ' ')}."})
    return rows


def _schema_rows(fields: Iterable[str], types: dict[str, str]) -> list[dict[str, str]]:
    return [{"field": field, "type": types.get(field, "string"), "required": "true", "description": _description(field)} for field in fields]


def _rule(field: str, rule: str, description: str, severity: str) -> dict[str, str]:
    return {"field": field, "rule": rule, "description": description, "severity": severity}


def _matrix_row(phase: str, component: str, classification: str, evidence: str) -> dict[str, str]:
    return {"phase": phase, "component": component, "classification": classification, "evidence": evidence}


def _pit_types() -> dict[str, str]:
    return {"as_of_date": "date", "market_cap": "float", "average_dollar_volume": "float", "is_active": "boolean", "is_delisted": "boolean", "delisting_date": "date", "eligible_for_portfolio": "boolean"}


def _price_types() -> dict[str, str]:
    return {"date": "date", "open": "float", "high": "float", "low": "float", "close": "float", "adjusted_close": "float", "volume": "integer", "dividend": "float", "split_factor": "float", "total_return": "float"}


def _fundamental_types() -> dict[str, str]:
    dates = {"as_of_date": "date", "report_date": "date", "filing_date": "date"}
    numbers = {field: "float" for field in FUNDAMENTAL_FIELDS if field not in dates and field not in {"ticker", "source"}}
    return {**dates, **numbers}


def _benchmark_types() -> dict[str, str]:
    return {"date": "date", "return": "float", "total_return": "float", "fee_adjusted_return": "float"}


def _description(field: str) -> str:
    return field.replace("_", " ")


def _sample_value(field: str) -> str:
    samples = {
        "as_of_date": "2020-01-31",
        "ticker": "ABC",
        "security_id": "ABC_US_001",
        "security_type": "EQUITY",
        "name": "Example Corp",
        "sector": "Technology",
        "industry": "Software",
        "market_cap": "1000000000",
        "average_dollar_volume": "25000000",
        "is_active": "true",
        "is_delisted": "false",
        "delisting_date": "",
        "eligible_for_portfolio": "true",
        "exclusion_reason": "",
    }
    return samples.get(field, "")


def _contract_for_category(category: str) -> str:
    if category in {"prices", "dividends", "splits", "ETF returns"}:
        return "P015 historical price return contract"
    if category == "fundamentals":
        return "P016 fundamental data contract"
    if category in {"benchmark returns", "risk-free rates"}:
        return "P017 benchmark data contract"
    return "P014 point-in-time universe contract"


def _vendor_request_template() -> str:
    return "\n".join(
        [
            "# Portfolio Atlas Data Request Template",
            "",
            "Please provide local file exports only; do not require this runtime to call external APIs.",
            "",
            "Required coverage:",
            "- Point-in-time universe membership including delisted securities.",
            "- Daily adjusted OHLCV, dividends, splits, and corporate actions.",
            "- Point-in-time fundamentals with report_date and filing_date.",
            "- Benchmark total-return series for VTI, 60/40 proxy, simple factor proxy, Oak Harvest proxy, and cash/risk-free proxy.",
            "",
            "Required metadata:",
            "- Source name, license/permission, extraction timestamp, field definitions, and survivorship-bias statement.",
            "",
        ]
    )


def _import_folder_layout() -> str:
    return "\n".join(
        [
            "# Portfolio Atlas Import Folder Layout",
            "",
            "reports/portfolio_research_os/input/",
            "- universe_manifest.csv",
            "- prices.csv",
            "- dividends.csv",
            "- splits.csv",
            "- fundamentals.csv",
            "- benchmark_returns.csv",
            "- data_vendor_manifest.csv",
            "",
            "All files should preserve source columns plus the contract fields generated by P014-P017.",
            "",
        ]
    )


def _summary(title: str, report: dict[str, Any]) -> str:
    blockers = report.get("critical_blockers") or report.get("summary", {}).get("critical_blockers") or []
    lines = [f"# {title}", "", f"- Status: {report.get('status')}", f"- Authority: {AUTHORITY_TEXT}", ""]
    if blockers:
        lines.append("## Critical Blockers")
        lines.append("")
        for item in blockers:
            lines.append(f"- {item if isinstance(item, str) else item.get('blocker')}")
        lines.append("")
    return "\n".join(lines)


def _out_dir(report_root: Path | str, key: str) -> Path:
    out = Path(report_root) / REPORT_DIRS[key]
    out.mkdir(parents=True, exist_ok=True)
    return out


def _standard_paths(out: Path) -> dict[str, Path]:
    return {
        "latest_json": out / "latest.json",
        "latest_summary": out / "latest_summary.md",
        "available_sources": out / "available_sources.csv",
        "missing_sources": out / "missing_sources.csv",
        "data_source_priority": out / "data_source_priority.csv",
    }


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
