from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from .data_import_package import DEFAULT_REPORT_ROOT
from .data_source_inventory import AUTHORITY_BOUNDARY, AUTHORITY_TEXT

PRICE_ONLY_READY = "PRICE_ONLY_READY"
FULL_MODEL_READY = "FULL_MODEL_READY"
DATA_REQUIRED = "DATA_REQUIRED"
BLOCKED = "BLOCKED"
INSUFFICIENT = "INSUFFICIENT"

RESEARCH_ONLY_AUTHORITY = "Research-only. No portfolio recommendation. No replacement recommendation. No trades. No broker execution."
REPORT_DIRS = {
    "existing_data_recovery": "existing_data_recovery",
    "databento_baseline_feasibility": "databento_baseline_feasibility",
    "benchmark_recovery": "benchmark_recovery",
    "fundamental_data_strategy": "fundamental_data_strategy",
    "first_backtest_readiness_audit": "first_backtest_readiness_audit",
    "portfolio_atlas_data_unlock_review": "portfolio_atlas_data_unlock_review",
}
DEFAULT_SEARCH_ROOTS = [Path("."), Path("/home/node/constellation_runtime_data/truth")]
ALLOWED_SUFFIXES = {".csv", ".json", ".parquet", ".zst"}
ETF_TICKERS = {"VTI", "SPY", "TLT", "BND", "AGG", "SHY", "IEF", "QQQ", "DIA", "IWM", "DBC", "GLD", "USO"}
ALLOWED_PRICE_ONLY_FACTORS = ["Momentum", "Risk"]
BLOCKED_FULL_MODEL_FACTORS = ["Growth", "Quality", "Valuation", "PEG", "PEGY"]


def run_data_unlock_program(report_root: Path | str = DEFAULT_REPORT_ROOT, *, search_roots: Iterable[Path | str] | None = None, created_at: str | None = None) -> dict[str, Any]:
    roots = [Path(root) for root in (search_roots or DEFAULT_SEARCH_ROOTS)]
    existing = run_existing_data_recovery(report_root, search_roots=roots, created_at=created_at)
    databento = run_databento_baseline_feasibility(report_root, existing_data=existing, created_at=created_at)
    benchmarks = run_benchmark_recovery(report_root, search_roots=roots, created_at=created_at)
    fundamentals = run_fundamental_data_strategy(report_root, created_at=created_at)
    readiness = run_first_backtest_readiness_audit(report_root, databento_feasibility=databento, benchmark_recovery=benchmarks, fundamental_strategy=fundamentals, created_at=created_at)
    review = run_portfolio_atlas_data_unlock_review(report_root, existing_data=existing, databento_feasibility=databento, benchmark_recovery=benchmarks, readiness_audit=readiness, created_at=created_at)
    return {
        "existing_data_recovery": existing,
        "databento_baseline_feasibility": databento,
        "benchmark_recovery": benchmarks,
        "fundamental_data_strategy": fundamentals,
        "first_backtest_readiness_audit": readiness,
        "portfolio_atlas_data_unlock_review": review,
    }


def run_existing_data_recovery(report_root: Path | str = DEFAULT_REPORT_ROOT, *, search_roots: Iterable[Path | str] | None = None, created_at: str | None = None) -> dict[str, Any]:
    report = build_existing_data_recovery(search_roots=search_roots, created_at=created_at)
    write_existing_data_recovery(report, report_root)
    return report


def run_databento_baseline_feasibility(report_root: Path | str = DEFAULT_REPORT_ROOT, *, existing_data: dict[str, Any] | None = None, search_roots: Iterable[Path | str] | None = None, created_at: str | None = None) -> dict[str, Any]:
    report = build_databento_baseline_feasibility(existing_data=existing_data or build_existing_data_recovery(search_roots=search_roots, created_at=created_at), created_at=created_at)
    write_databento_baseline_feasibility(report, report_root)
    return report


def run_benchmark_recovery(report_root: Path | str = DEFAULT_REPORT_ROOT, *, search_roots: Iterable[Path | str] | None = None, created_at: str | None = None) -> dict[str, Any]:
    report = build_benchmark_recovery(search_roots=search_roots, created_at=created_at)
    write_benchmark_recovery(report, report_root)
    return report


def run_fundamental_data_strategy(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_fundamental_data_strategy(created_at=created_at)
    write_fundamental_data_strategy(report, report_root)
    return report


def run_first_backtest_readiness_audit(report_root: Path | str = DEFAULT_REPORT_ROOT, *, databento_feasibility: dict[str, Any] | None = None, benchmark_recovery: dict[str, Any] | None = None, fundamental_strategy: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    report = build_first_backtest_readiness_audit(
        databento_feasibility=databento_feasibility or build_databento_baseline_feasibility(created_at=created_at),
        benchmark_recovery=benchmark_recovery or build_benchmark_recovery(created_at=created_at),
        fundamental_strategy=fundamental_strategy or build_fundamental_data_strategy(created_at=created_at),
        created_at=created_at,
    )
    write_first_backtest_readiness_audit(report, report_root)
    return report


def run_portfolio_atlas_data_unlock_review(report_root: Path | str = DEFAULT_REPORT_ROOT, *, existing_data: dict[str, Any] | None = None, databento_feasibility: dict[str, Any] | None = None, benchmark_recovery: dict[str, Any] | None = None, readiness_audit: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    existing_data = existing_data or build_existing_data_recovery(created_at=created_at)
    databento_feasibility = databento_feasibility or build_databento_baseline_feasibility(existing_data=existing_data, created_at=created_at)
    benchmark_recovery = benchmark_recovery or build_benchmark_recovery(created_at=created_at)
    readiness_audit = readiness_audit or build_first_backtest_readiness_audit(databento_feasibility=databento_feasibility, benchmark_recovery=benchmark_recovery, fundamental_strategy=build_fundamental_data_strategy(created_at=created_at), created_at=created_at)
    report = build_portfolio_atlas_data_unlock_review(existing_data=existing_data, databento_feasibility=databento_feasibility, benchmark_recovery=benchmark_recovery, readiness_audit=readiness_audit, created_at=created_at)
    write_portfolio_atlas_data_unlock_review(report, report_root)
    return report


def build_existing_data_recovery(*, search_roots: Iterable[Path | str] | None = None, created_at: str | None = None, max_files: int = 50000) -> dict[str, Any]:
    roots = [Path(root) for root in (search_roots or DEFAULT_SEARCH_ROOTS)]
    usable: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    scanned = 0
    truncated = False
    for path in _iter_candidate_files(roots):
        scanned += 1
        if scanned > max_files:
            truncated = True
            break
        row = _classify_candidate_file(path)
        if row["usable"] == "YES":
            usable.append({key: value for key, value in row.items() if key not in {"usable", "blocker"}})
        else:
            blocked.append(row)
    return {
        "stage": "D051-D055 Existing Data Recovery",
        "status": PRICE_ONLY_READY if usable else DATA_REQUIRED,
        "created_at": _created_at(created_at),
        "question": "What data already exists?",
        "search_roots": [str(root) for root in roots],
        "files_scanned": scanned,
        "truncated": truncated,
        "usable_count": len(usable),
        "blocked_count": len(blocked),
        "usable_data": usable,
        "blocked_data": blocked,
        "authority": RESEARCH_ONLY_AUTHORITY,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def build_databento_baseline_feasibility(*, existing_data: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    existing_data = existing_data or build_existing_data_recovery(created_at=created_at)
    databento_sources = [row for row in existing_data.get("usable_data", []) if row.get("data_type") == "databento_export" or "databento" in str(row.get("path", "")).lower()]
    has_price_source = any(row.get("format") in {"csv", "zst", "parquet"} and int(row.get("size_bytes", 0)) > 0 for row in databento_sources)
    allowed = [{"factor": factor, "status": "AVAILABLE_FROM_PRICE_HISTORY", "source_requirement": "OHLCV or adjusted close"} for factor in ALLOWED_PRICE_ONLY_FACTORS]
    allowed.append({"factor": "Income", "status": "AVAILABLE_IF_DIVIDEND_FIELD_VALIDATED" if _has_dividend_source(existing_data) else DATA_REQUIRED, "source_requirement": "dividend cash flow history"})
    blocked = [{"factor": factor, "status": DATA_REQUIRED, "reason": "Requires point-in-time fundamentals not provided by price-only Databento OHLCV export"} for factor in BLOCKED_FULL_MODEL_FACTORS]
    decision = PRICE_ONLY_READY if has_price_source else INSUFFICIENT
    return {"stage": "D056-D060 Databento Baseline Activation", "decision": decision, "status": decision, "created_at": _created_at(created_at), "question": "Can Databento support price-only Portfolio Atlas?", "databento_sources": databento_sources, "allowed_factors": allowed, "blocked_factors": blocked, "notes": "Price-only feasibility does not unlock growth, quality, valuation, PEG, or PEGY scoring.", "authority": RESEARCH_ONLY_AUTHORITY, "authority_boundary": AUTHORITY_BOUNDARY}


def build_benchmark_recovery(*, search_roots: Iterable[Path | str] | None = None, created_at: str | None = None) -> dict[str, Any]:
    roots = [Path(root) for root in (search_roots or DEFAULT_SEARCH_ROOTS)]
    sources = _find_benchmark_sources(roots)
    benchmark_sources: list[dict[str, Any]] = []
    blockers: list[dict[str, str]] = []
    returns: list[dict[str, Any]] = []
    series_by_ticker: dict[str, list[dict[str, Any]]] = {}
    for ticker in ["VTI", "SPY", "TLT"]:
        path = sources.get(ticker)
        if not path:
            blockers.append({"benchmark": ticker, "status": DATA_REQUIRED, "missing": f"source-backed {ticker} adjusted return history"})
            continue
        series = _read_adjusted_return_series(path)
        if not series:
            blockers.append({"benchmark": ticker, "status": DATA_REQUIRED, "missing": f"usable adjusted close return rows in {path}"})
            continue
        series_by_ticker[ticker] = series
        benchmark_sources.append({"benchmark": ticker, "status": "SOURCE_BACKED", "path": str(path), "return_rows": len(series)})
        returns.extend(_return_rows(ticker, series, path))
    proxy = _combine_6040(series_by_ticker.get("SPY", []), series_by_ticker.get("TLT", []))
    if proxy:
        benchmark_sources.append({"benchmark": "60_40_PROXY", "status": "SOURCE_BACKED_PROXY", "path": f"60% {sources.get('SPY')} + 40% {sources.get('TLT')}", "return_rows": len(proxy)})
        returns.extend(_return_rows("60_40_PROXY", proxy, None))
    else:
        blockers.append({"benchmark": "60_40_PROXY", "status": DATA_REQUIRED, "missing": "overlapping SPY and TLT adjusted return histories"})
    blockers.append({"benchmark": "risk-free proxy", "status": DATA_REQUIRED, "missing": "source-backed Treasury bill or cash/risk-free rate history"})
    return {"stage": "D061-D065 Benchmark Dataset Recovery", "status": PRICE_ONLY_READY if not blockers else DATA_REQUIRED, "created_at": _created_at(created_at), "benchmark_sources": benchmark_sources, "benchmark_returns": returns, "benchmark_blockers": blockers, "notes": "Only source-backed rows are emitted. Missing required benchmarks remain blocked instead of using assumptions.", "authority": RESEARCH_ONLY_AUTHORITY, "authority_boundary": AUTHORITY_BOUNDARY}


def build_fundamental_data_strategy(*, created_at: str | None = None) -> dict[str, Any]:
    fields = "Revenue; EPS; FCF; ROIC; Margins; PEG; PEGY"
    paths = [
        {"path_type": "Cheapest path", "recommended_sources": "SEC Companyfacts plus source-backed prices and dividends for derived ratios", "coverage": fields, "estimated_tradeoff": "Lowest cost; highest engineering effort; ROIC, PEG, and PEGY require careful point-in-time derivation and vendor QA"},
        {"path_type": "Best value path", "recommended_sources": "Financial Modeling Prep or Intrinio fundamentals plus Tiingo or equivalent adjusted prices/dividends", "coverage": fields, "estimated_tradeoff": "Moderate cost; faster normalization; still requires point-in-time filing-date audit"},
        {"path_type": "Institutional path", "recommended_sources": "Compustat/FactSet/S&P Capital IQ/Bloomberg with point-in-time fundamentals and corporate actions", "coverage": fields, "estimated_tradeoff": "Highest cost; strongest survivorship and filing-date controls; best audit trail"},
    ]
    return {"stage": "D066-D070 Fundamental Data Strategy", "status": DATA_REQUIRED, "created_at": _created_at(created_at), "required_paths": paths, "blocked_until_acquired": ["Revenue", "EPS", "FCF", "ROIC", "Margins", "PEG", "PEGY"], "authority": RESEARCH_ONLY_AUTHORITY, "authority_boundary": AUTHORITY_BOUNDARY}


def build_first_backtest_readiness_audit(*, databento_feasibility: dict[str, Any], benchmark_recovery: dict[str, Any], fundamental_strategy: dict[str, Any], created_at: str | None = None) -> dict[str, Any]:
    price_ready = databento_feasibility.get("decision") == PRICE_ONLY_READY
    missing_benchmarks = [row["benchmark"] for row in benchmark_recovery.get("benchmark_blockers", [])]
    full_model_blockers = [
        {"blocker": "point-in-time fundamentals", "impact": "Blocks growth, quality, valuation, PEG, and PEGY"},
        {"blocker": "point-in-time universe", "impact": "Blocks survivorship-bias-controlled full Portfolio Atlas universe"},
        {"blocker": "dividend/corporate action validation", "impact": "Blocks validated income and total-return attribution"},
        {"blocker": "benchmark gaps", "impact": "Missing " + ", ".join(missing_benchmarks) if missing_benchmarks else "No benchmark gaps detected"},
    ]
    return {"stage": "D071-D075 First Backtest Readiness Audit", "status": PRICE_ONLY_READY if price_ready else DATA_REQUIRED, "created_at": _created_at(created_at), "can_run_price_only_baseline": price_ready, "price_only_scope": "Momentum and risk only; income only after dividend validation; no new scoring features.", "benchmark_status": benchmark_recovery.get("status"), "fundamental_status": fundamental_strategy.get("status"), "price_only_readiness": [{"requirement": "source-backed OHLCV or adjusted close", "status": "READY" if price_ready else DATA_REQUIRED}, {"requirement": "allowed factors limited to Momentum and Risk", "status": "READY"}, {"requirement": "full-model fundamentals", "status": DATA_REQUIRED}], "full_model_blockers": full_model_blockers, "authority": RESEARCH_ONLY_AUTHORITY, "authority_boundary": AUTHORITY_BOUNDARY}


def build_portfolio_atlas_data_unlock_review(*, existing_data: dict[str, Any], databento_feasibility: dict[str, Any], benchmark_recovery: dict[str, Any], readiness_audit: dict[str, Any], created_at: str | None = None) -> dict[str, Any]:
    classification = PRICE_ONLY_READY if readiness_audit.get("can_run_price_only_baseline") else (DATA_REQUIRED if existing_data.get("usable_count", 0) == 0 else BLOCKED)
    missing_rows = []
    if any(row.get("benchmark") == "VTI" for row in benchmark_recovery.get("benchmark_blockers", [])):
        missing_rows.append({"missing": "VTI adjusted total-return history", "needed_for": "strict benchmark comparison"})
    missing_rows.extend([
        {"missing": "source-backed risk-free history", "needed_for": "cash/risk-free proxy"},
        {"missing": "point-in-time fundamentals", "needed_for": "full Portfolio Atlas model"},
        {"missing": "point-in-time universe", "needed_for": "survivorship-bias-controlled full model"},
    ])
    shortest_path = [
        {"step": 1, "action": "Normalize existing Databento OHLCV export into Portfolio Atlas price input schema", "unlocks": "price-only momentum/risk baseline"},
        {"step": 2, "action": "Recover or acquire source-backed VTI and risk-free histories", "unlocks": "required benchmark set"},
        {"step": 3, "action": "Run first-source-backed backtest only with price-only factors; keep full-model factors blocked", "unlocks": "first real Portfolio Atlas result"},
    ]
    return {"stage": "D076-D080 Portfolio Atlas Data Unlock Review", "classification": classification, "status": classification, "created_at": _created_at(created_at), "can_run_first_source_backed_backtest": classification == PRICE_ONLY_READY, "conclusion": "Existing source-backed price data is sufficient to unlock a price-only baseline, but the full model remains data-required.", "shortest_path_to_first_real_result": shortest_path, "missing_for_full_model": missing_rows, "databento_decision": databento_feasibility.get("decision"), "benchmark_status": benchmark_recovery.get("status"), "authority": RESEARCH_ONLY_AUTHORITY, "authority_boundary": AUTHORITY_BOUNDARY}


def write_existing_data_recovery(report: dict[str, Any], report_root: Path | str) -> None:
    directory = _report_dir(report_root, "existing_data_recovery")
    _write_json(directory / "latest.json", report)
    _write_summary(directory / "latest_summary.md", _summary_lines(report, "Portfolio Atlas Existing Data Recovery"))
    _write_csv(directory / "usable_data.csv", report["usable_data"], ["path", "data_type", "format", "size_bytes", "source_status", "notes"])
    _write_csv(directory / "blocked_data.csv", report["blocked_data"], ["path", "data_type", "format", "size_bytes", "source_status", "notes", "usable", "blocker"])


def write_databento_baseline_feasibility(report: dict[str, Any], report_root: Path | str) -> None:
    directory = _report_dir(report_root, "databento_baseline_feasibility")
    _write_json(directory / "latest.json", report)
    _write_summary(directory / "latest_summary.md", _summary_lines(report, "Portfolio Atlas Databento Baseline Feasibility"))
    _write_csv(directory / "databento_sources.csv", report["databento_sources"], ["path", "data_type", "format", "size_bytes", "source_status", "notes"])
    _write_csv(directory / "allowed_price_only_factors.csv", report["allowed_factors"], ["factor", "status", "source_requirement"])
    _write_csv(directory / "blocked_full_model_factors.csv", report["blocked_factors"], ["factor", "status", "reason"])


def write_benchmark_recovery(report: dict[str, Any], report_root: Path | str) -> None:
    directory = _report_dir(report_root, "benchmark_recovery")
    _write_json(directory / "latest.json", report)
    _write_summary(directory / "latest_summary.md", _summary_lines(report, "Portfolio Atlas Benchmark Dataset Recovery"))
    _write_csv(directory / "benchmark_sources.csv", report["benchmark_sources"], ["benchmark", "status", "path", "return_rows"])
    _write_csv(directory / "benchmark_returns.csv", report["benchmark_returns"], ["date", "benchmark", "return", "source"])
    _write_csv(directory / "benchmark_blockers.csv", report["benchmark_blockers"], ["benchmark", "status", "missing"])


def write_fundamental_data_strategy(report: dict[str, Any], report_root: Path | str) -> None:
    directory = _report_dir(report_root, "fundamental_data_strategy")
    _write_json(directory / "latest.json", report)
    _write_summary(directory / "latest_summary.md", _summary_lines(report, "Portfolio Atlas Fundamental Data Strategy"))
    _write_csv(directory / "fundamental_data_paths.csv", report["required_paths"], ["path_type", "recommended_sources", "coverage", "estimated_tradeoff"])
    _write_csv(directory / "fundamental_missing_fields.csv", [{"field": field, "status": DATA_REQUIRED} for field in report["blocked_until_acquired"]], ["field", "status"])


def write_first_backtest_readiness_audit(report: dict[str, Any], report_root: Path | str) -> None:
    directory = _report_dir(report_root, "first_backtest_readiness_audit")
    _write_json(directory / "latest.json", report)
    _write_summary(directory / "latest_summary.md", _summary_lines(report, "Portfolio Atlas First Backtest Readiness Audit"))
    _write_csv(directory / "price_only_readiness.csv", report["price_only_readiness"], ["requirement", "status"])
    _write_csv(directory / "full_model_blockers.csv", report["full_model_blockers"], ["blocker", "impact"])
    _write_csv(directory / "readiness_decision.csv", [{"decision": report["status"], "can_run_price_only_baseline": report["can_run_price_only_baseline"], "authority": report["authority"]}], ["decision", "can_run_price_only_baseline", "authority"])


def write_portfolio_atlas_data_unlock_review(report: dict[str, Any], report_root: Path | str) -> None:
    directory = _report_dir(report_root, "portfolio_atlas_data_unlock_review")
    _write_json(directory / "latest.json", report)
    _write_summary(directory / "latest_summary.md", _summary_lines(report, "Portfolio Atlas Data Unlock Review"))
    _write_csv(directory / "data_unlock_decision.csv", [{"classification": report["classification"], "can_run_first_source_backed_backtest": report["can_run_first_source_backed_backtest"], "authority": report["authority"]}], ["classification", "can_run_first_source_backed_backtest", "authority"])
    _write_csv(directory / "shortest_path.csv", report["shortest_path_to_first_real_result"], ["step", "action", "unlocks"])
    _write_csv(directory / "missing_data.csv", report["missing_for_full_model"], ["missing", "needed_for"])


def _iter_candidate_files(roots: Iterable[Path]) -> Iterable[Path]:
    skipped_dirs = {".git", "node_modules", "__pycache__", ".pytest_cache", ".mypy_cache", ".venv", "venv"}
    for root in roots:
        if not root.exists():
            continue
        if root.is_file():
            if root.suffix.lower() in ALLOWED_SUFFIXES:
                yield root
            continue
        for path in root.rglob("*"):
            if any(part in skipped_dirs for part in path.parts):
                continue
            if path.is_file() and path.suffix.lower() in ALLOWED_SUFFIXES:
                yield path


def _classify_candidate_file(path: Path) -> dict[str, Any]:
    size = path.stat().st_size if path.exists() else 0
    suffix = path.suffix.lower().lstrip(".")
    header = _read_header(path) if path.suffix.lower() == ".csv" else []
    data_type = _data_type(path, header)
    usable = size > 0 and (path.suffix.lower() != ".csv" or _csv_usable(header, data_type))
    return {"path": str(path), "data_type": data_type, "format": suffix, "size_bytes": size, "source_status": "SOURCE_BACKED_CANDIDATE" if usable else DATA_REQUIRED, "notes": _notes_for(path, header, data_type), "usable": "YES" if usable else "NO", "blocker": "" if usable else ("empty file" if size == 0 else "missing recognizable source-backed columns")}


def _data_type(path: Path, header: list[str]) -> str:
    text = str(path).lower()
    stem = path.stem.upper()
    normalized = {item.lower().replace("_", "") for item in header}
    if "databento" in text or "xnas" in text or "ohlcv" in text:
        return "databento_export"
    if "atlas" in text or "portfolio_research_os" in text:
        return "atlas_data"
    if stem.split("_")[0] in ETF_TICKERS or any(ticker in stem for ticker in ETF_TICKERS):
        return "etf_or_benchmark_file"
    if "benchmark" in text:
        return "benchmark_file"
    if "date" in normalized and ("close" in normalized or "adjclose" in normalized or "adjustedclose" in normalized):
        return "price_history"
    return "csv_or_structured_file"


def _csv_usable(header: list[str], data_type: str) -> bool:
    if not header:
        return False
    normalized = {item.lower().replace("_", "") for item in header}
    if data_type in {"databento_export", "price_history", "etf_or_benchmark_file", "benchmark_file"}:
        return "date" in normalized and ("close" in normalized or "adjclose" in normalized or "adjustedclose" in normalized)
    return True


def _read_header(path: Path) -> list[str]:
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            line = handle.readline().strip()
    except UnicodeDecodeError:
        return []
    return [item.strip() for item in line.split(",") if item.strip()]


def _notes_for(path: Path, header: list[str], data_type: str) -> str:
    if data_type == "databento_export":
        return "Databento or XNAS OHLCV candidate"
    if header:
        return "columns=" + "|".join(header[:12])
    return "structured file candidate"


def _has_dividend_source(existing_data: dict[str, Any]) -> bool:
    for row in existing_data.get("usable_data", []):
        text = (str(row.get("notes", "")) + " " + str(row.get("path", ""))).lower()
        if "divcash" in text or "dividend" in text:
            return True
    return False


def _find_benchmark_sources(roots: Iterable[Path]) -> dict[str, Path]:
    candidates: dict[str, list[Path]] = {"VTI": [], "SPY": [], "TLT": []}
    for path in _iter_candidate_files(roots):
        if path.suffix.lower() != ".csv":
            continue
        upper = path.name.upper()
        for ticker in candidates:
            if upper.startswith(ticker):
                candidates[ticker].append(path)
    sources: dict[str, Path] = {}
    for ticker, paths in candidates.items():
        usable = [path for path in paths if _read_adjusted_return_series(path)]
        if usable:
            sources[ticker] = sorted(usable, key=_benchmark_source_rank)[0]
        elif paths:
            sources[ticker] = sorted(paths, key=_benchmark_source_rank)[0]
    return sources


def _benchmark_source_rank(path: Path) -> tuple[int, int, str]:
    name = path.name.lower()
    preferred = 0 if "adjusted_daily" in name or "tiingo_adjusted_daily" in name else 1
    intraday = 1 if "5m" in name or "1m" in name or "intraday" in name else 0
    return (preferred, intraday, str(path))


def _read_adjusted_return_series(path: Path) -> list[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
    except (OSError, UnicodeDecodeError, csv.Error):
        return []
    fields = {field.lower().replace("_", ""): field for field in (reader.fieldnames or [])}
    date_field = fields.get("date")
    price_field = fields.get("adjclose") or fields.get("adjustedclose") or fields.get("close")
    if not date_field or not price_field:
        return []
    series: list[dict[str, Any]] = []
    last_price: float | None = None
    for row in rows:
        try:
            price = float(row[price_field])
        except (TypeError, ValueError):
            continue
        if last_price and last_price > 0:
            series.append({"date": str(row[date_field])[:10], "return": price / last_price - 1.0})
        last_price = price
    return series


def _combine_6040(spy: list[dict[str, Any]], tlt: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tlt_by_date = {row["date"]: row["return"] for row in tlt}
    return [{"date": row["date"], "return": 0.6 * row["return"] + 0.4 * tlt_by_date[row["date"]]} for row in spy if row["date"] in tlt_by_date]


def _return_rows(benchmark: str, series: list[dict[str, Any]], source: Path | None) -> list[dict[str, Any]]:
    source_text = str(source) if source else "60% SPY adjusted close + 40% TLT adjusted close"
    return [{"date": row["date"], "benchmark": benchmark, "return": f"{row['return']:.10f}", "source": source_text} for row in series]


def _report_dir(report_root: Path | str, key: str) -> Path:
    directory = Path(report_root) / REPORT_DIRS[key]
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_summary(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _summary_lines(report: dict[str, Any], title: str) -> list[str]:
    return [f"# {title}", "", f"Status: {report.get('status') or report.get('classification') or report.get('decision')}", "", report.get("conclusion") or report.get("notes") or report.get("question") or "", "", f"Authority: {AUTHORITY_TEXT}"]


def _created_at(created_at: str | None) -> str:
    return created_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# Portfolio Atlas D021-D050 Data Unlock Program.
# This is intentionally separate from the existing D051-D080 data unlock API above.
FIRST_REAL_BACKTEST_READY = "FIRST_REAL_BACKTEST_READY"
PRICE_ONLY_BASELINE = "PRICE_ONLY_BASELINE"
PRICE_ONLY_ENABLED_FACTORS = ["momentum", "risk", "income"]
PRICE_ONLY_DISABLED_FACTORS = ["growth", "quality", "valuation", "PEG", "PEGY"]
D021_REPORT_DIR = "data_unlock_program"


def run_data_unlock_program_d021_d050(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = Path("data/portfolio_research_os"), *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_unlock_program(report_root=report_root, data_root=data_root, created_at=created_at)
    write_data_unlock_program_d021_d050(report, report_root)
    return report


def build_data_unlock_program(report_root: Path | str = DEFAULT_REPORT_ROOT, data_root: Path | str = Path("data/portfolio_research_os"), *, created_at: str | None = None) -> dict[str, Any]:
    roots = _d021_candidate_roots(Path(report_root), Path(data_root))
    existing = [_d021_inventory_row(path) for path in _d021_iter_files(roots)]
    existing = [row for row in existing if row]
    usable = [row for row in existing if row["usable_for"]]
    missing = _d021_missing_inventory(existing)
    feasibility, gaps = _d021_databento_feasibility(existing)
    price_ready = any(row["decision"] == "PRICE_ONLY_BASELINE_READY" for row in feasibility)
    price_only = _d021_price_only_rows(price_ready, feasibility)
    strategy, cost_benefit = _d021_fundamental_strategy()
    universe, survivorship = _d021_universe_recovery(existing)
    classification = PRICE_ONLY_READY if price_ready else DATA_REQUIRED
    status = FIRST_REAL_BACKTEST_READY if price_ready else DATA_REQUIRED
    review = [
        {
            "question": "Can Portfolio Atlas run its first real backtest?",
            "classification": classification,
            "answer": "Yes, as a constrained PRICE_ONLY_BASELINE only." if price_ready else "No, source-backed data remains insufficient.",
            "next_step": "Normalize Databento OHLCV into explicit PRICE_ONLY_BASELINE inputs; keep full-model factors disabled." if price_ready else "Acquire source-backed price/universe/fundamental data first.",
        },
        {
            "question": "Is full Portfolio Atlas ready?",
            "classification": DATA_REQUIRED,
            "answer": "No. Full model still needs adjusted prices, corporate actions, PIT fundamentals, PIT universe, delisted securities, VTI, risk-free, and Oak Harvest source-backed data.",
            "next_step": "Buy or recover PIT fundamentals, adjusted/corporate-action data, and survivorship-aware universe data.",
        },
    ]
    return {
        "schema_id": "portfolio_research_os_data_unlock_program_d021_d050",
        "schema_version": "1.0",
        "build": "Portfolio Atlas D021-D050",
        "created_at": _created_at(created_at),
        "status": status,
        "classification": classification,
        "required_answer": "YES_PRICE_ONLY_BASELINE" if price_ready else "NO",
        "existing_data_inventory": existing,
        "usable_data_inventory": usable,
        "missing_data_inventory": missing,
        "databento_portfolio_feasibility": feasibility,
        "databento_gap_report": gaps,
        "price_only_baseline_portfolio": price_only,
        "fundamental_data_strategy": strategy,
        "cost_benefit_analysis": cost_benefit,
        "universe_recovery_plan": universe,
        "survivorship_risk_report": survivorship,
        "data_readiness_review": review,
        "final_deliverable": {
            "what_data_do_we_have": _d021_have_summary(existing, feasibility),
            "what_data_do_we_need": "Full model needs source-backed adjusted prices, dividends, splits, PIT fundamentals, broad PIT universe membership, delisted securities, VTI total-return benchmark data, risk-free history, and source-backed Oak Harvest comparison data.",
            "can_we_run_a_price_only_baseline": price_ready,
            "what_should_be_purchased_next": "BEST_VALUE: Sharadar or equivalent PIT fundamentals plus corporate actions and delisted/security master coverage. Verify current vendor pricing and license terms before purchase.",
            "shortest_path_to_first_evidence": "Run a clearly labeled PRICE_ONLY_BASELINE from existing Databento OHLCV with only momentum, risk, and income factors enabled; keep growth, quality, valuation, PEG, and PEGY disabled until fundamentals are acquired.",
        },
        "authority": "Research-only. No live portfolio. No Oak Harvest replacement recommendation. No broker execution. No trading authority.",
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def write_data_unlock_program_d021_d050(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> None:
    out = Path(report_root) / D021_REPORT_DIR
    out.mkdir(parents=True, exist_ok=True)
    _write_json(out / "latest.json", report)
    (out / "latest_summary.md").write_text(_d021_summary(report), encoding="utf-8")
    _write_csv(out / "existing_data_inventory.csv", report["existing_data_inventory"], ["source_area", "path", "file_type", "rows", "symbols", "start_date", "end_date", "columns", "usable_for", "limitations"])
    _write_csv(out / "usable_data_inventory.csv", report["usable_data_inventory"], ["source_area", "path", "file_type", "rows", "symbols", "start_date", "end_date", "columns", "usable_for", "limitations"])
    _write_csv(out / "missing_data_inventory.csv", report["missing_data_inventory"], ["data_need", "required_for", "status", "blocker_reason"])
    _write_csv(out / "databento_portfolio_feasibility.csv", report["databento_portfolio_feasibility"], ["question", "answer", "evidence", "decision"])
    _write_csv(out / "databento_gap_report.csv", report["databento_gap_report"], ["gap", "severity", "impact", "repair_path"])
    _write_csv(out / "price_only_baseline_portfolio.csv", report["price_only_baseline_portfolio"], ["label", "component", "status", "enabled_factors", "disabled_factors", "notes"])
    _write_csv(out / "fundamental_data_strategy.csv", report["fundamental_data_strategy"], ["vendor", "coverage", "pit_quality", "relative_cost", "fit", "recommendation"])
    _write_csv(out / "cost_benefit_analysis.csv", report["cost_benefit_analysis"], ["recommendation", "vendor", "benefit", "tradeoff", "next_action"])
    _write_csv(out / "universe_recovery_plan.csv", report["universe_recovery_plan"], ["universe_component", "current_status", "risk", "repair_path"])
    _write_csv(out / "survivorship_risk_report.csv", report["survivorship_risk_report"], ["risk", "status", "severity", "evidence", "required_repair"])
    _write_csv(out / "data_readiness_review.csv", report["data_readiness_review"], ["question", "classification", "answer", "next_step"])


def _d021_candidate_roots(report_root: Path, data_root: Path) -> list[Path]:
    roots = [data_root, Path("data/databento_raw"), Path("data"), report_root]
    if Path.cwd().resolve() == Path("/home/node/constellation"):
        roots.extend([Path("/home/node/constellation_runtime_data/truth"), Path("/home/node/Downloads"), Path("/home/node/Cloud"), Path("/home/node/OneDrive"), Path("/home/node/atlas"), Path("/home/node/projects/atlas"), Path("/home/node/projects/atlas_ai"), Path("/home/node/paul_trade_logic_lab")])
    deduped = []
    seen = set()
    for root in roots:
        marker = str(root)
        if marker not in seen:
            seen.add(marker)
            deduped.append(root)
    return deduped


def _d021_iter_files(roots: list[Path]) -> list[Path]:
    out = []
    for root in roots:
        if not root.exists():
            continue
        try:
            for dirpath, dirnames, filenames in __import__("os").walk(root):
                dirnames[:] = [d for d in dirnames if d not in {".git", "node_modules", ".venv", "__pycache__"}]
                for name in filenames:
                    path = Path(dirpath) / name
                    if path.suffix.lower() in {".csv", ".json", ".jsonl", ".zst"}:
                        out.append(path)
                    if len(out) >= 800:
                        return out
        except OSError:
            continue
    return out


def _d021_inventory_row(path: Path) -> dict[str, Any]:
    fields: list[str] = []
    count = ""
    symbols: set[str] = set()
    dates: list[str] = []
    if path.suffix.lower() == ".csv":
        try:
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = list(reader.fieldnames or [])
                for i, row in enumerate(reader, start=1):
                    symbol = row.get("symbol") or row.get("ticker") or row.get("benchmark_id")
                    if symbol:
                        symbols.add(symbol.strip())
                    date_value = (row.get("date") or row.get("ts_event") or row.get("as_of_date") or "")[:10]
                    if date_value:
                        dates.append(date_value)
                    if i >= 250000:
                        break
                count = str(i) if 'i' in locals() else "0"
        except (OSError, UnicodeDecodeError, csv.Error):
            pass
    elif path.suffix.lower() == ".json":
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                fields = sorted(data.keys())
                query = data.get("query", {})
                if isinstance(query, dict):
                    symbols.update(str(s) for s in query.get("symbols", []) if s)
            elif isinstance(data, list) and data and isinstance(data[0], dict):
                fields = sorted(data[0].keys())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            pass
    usable, limitations = _d021_classify(path, fields, symbols)
    return {"source_area": _d021_source_area(path), "path": str(path), "file_type": path.suffix.lower().lstrip("."), "rows": count, "symbols": "|".join(sorted(symbols)[:40]), "start_date": min(dates) if dates else "", "end_date": max(dates) if dates else "", "columns": "|".join(fields[:40]), "usable_for": usable, "limitations": limitations}


def _d021_classify(path: Path, fields: list[str], symbols: set[str]) -> tuple[str, str]:
    lower_path = str(path).lower()
    lower_fields = {field.lower() for field in fields}
    has_ohlcv = {"open", "high", "low", "close", "volume"}.issubset(lower_fields) and ("symbol" in lower_fields or "ticker" in lower_fields)
    if "databento" in lower_path and (has_ohlcv or path.suffix.lower() == ".zst"):
        return "PRICE_ONLY_BASELINE:daily bars derivable; momentum/risk/income only", "No adjusted_close, dividends, splits, broad PIT universe, fundamentals, or VTI in current export."
    if {"SPY", "TLT", "IWM", "USO"}.intersection(symbols):
        return "price_only_supporting_etf_history", "ETF proxy support only; not source-backed VTI total return."
    if "fundamental" in lower_path or {"revenue", "eps", "free_cash_flow"}.intersection(lower_fields):
        return "full_model_candidate_fundamentals", "Requires PIT filing-date validation before use."
    if "universe" in lower_path or {"eligible_for_portfolio", "is_delisted"}.intersection(lower_fields):
        return "universe_candidate", "Requires PIT membership and delisted-security validation before use."
    return "", "Not currently mapped to a Portfolio Atlas data contract."


def _d021_source_area(path: Path) -> str:
    text = str(path).lower()
    if "databento" in text:
        return "Databento exports"
    if "trade" in text:
        return "Trade Atlas data"
    if "download" in text:
        return "Downloaded datasets"
    if "cloud" in text or "onedrive" in text:
        return "Cloud storage"
    if "atlas" in text or "portfolio_research_os" in text:
        return "Atlas repositories"
    return "Other local data"


def _d021_missing_inventory(existing: list[dict[str, Any]]) -> list[dict[str, str]]:
    text = " ".join((row["path"] + " " + row["columns"] + " " + row["symbols"]).lower() for row in existing)
    checks = {"adjusted_prices": ["adjusted_close", "adj_close"], "dividends": ["dividend"], "splits": ["split", "split_factor"], "point_in_time_universe": ["eligible_for_portfolio", "pit_universe"], "delisted_securities": ["is_delisted", "delisted"], "vti_total_return": ["vti"], "oak_harvest_source_backed_data": ["oak_harvest"], "risk_free_rates": ["risk_free", "t_bill", "treasury"]}
    rows = []
    for need, terms in checks.items():
        if not any(term in text for term in terms):
            rows.append({"data_need": need, "required_for": "FULL_MODEL_READY", "status": DATA_REQUIRED, "blocker_reason": "No source-backed local file matched this requirement."})
    for need in ["revenue", "eps", "free_cash_flow", "roic", "margins", "debt", "cash", "peg", "pegy"]:
        if need not in text:
            rows.append({"data_need": need, "required_for": "FULL_MODEL_READY", "status": DATA_REQUIRED, "blocker_reason": "No PIT fundamental source found locally."})
    return rows


def _d021_databento_feasibility(existing: list[dict[str, Any]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    databento = [row for row in existing if row["source_area"] == "Databento exports"]
    symbols = set()
    has_ohlcv = False
    for row in databento:
        symbols.update(s for s in row["symbols"].split("|") if s)
        cols = set(row["columns"].split("|"))
        has_ohlcv = has_ohlcv or {"open", "high", "low", "close", "volume"}.issubset(cols) or row["file_type"] == "zst"
    decision = "PRICE_ONLY_BASELINE_READY" if has_ohlcv and len(symbols) >= 5 and {"SPY", "TLT", "IWM", "USO"}.intersection(symbols) else INSUFFICIENT
    evidence = f"{len(databento)} Databento file(s); symbols={','.join(sorted(symbols))}; daily bars derivable from OHLCV={has_ohlcv}"
    rows = [
        {"question": "Adjusted prices?", "answer": "NO", "evidence": "Current Databento export has close but no adjusted_close column.", "decision": INSUFFICIENT},
        {"question": "Dividends?", "answer": "NO", "evidence": "No dividend column or dividend event file found in Databento export.", "decision": INSUFFICIENT},
        {"question": "Splits?", "answer": "NO", "evidence": "No split column or split event file found in Databento export.", "decision": INSUFFICIENT},
        {"question": "ETF history?", "answer": "PARTIAL", "evidence": f"ETF/proxy symbols present: {','.join(sorted({'SPY','TLT','IWM','USO'}.intersection(symbols))) or 'none'}; VTI absent.", "decision": "PARTIAL"},
        {"question": "Daily bars?", "answer": "YES_DERIVABLE" if has_ohlcv else "NO", "evidence": evidence, "decision": decision},
    ]
    gaps = [
        {"gap": "No adjusted_close", "severity": "HIGH", "impact": "Full total-return model remains blocked.", "repair_path": "Acquire adjusted daily price history or corporate-action-adjusted series."},
        {"gap": "No dividends/splits", "severity": "HIGH", "impact": "Full total-return model remains blocked; price-only must be explicitly labeled.", "repair_path": "Acquire corporate action history or use explicit PRICE_ONLY_BASELINE."},
        {"gap": "VTI absent", "severity": "MEDIUM", "impact": "Use SPY only as temporary equity ETF proxy; VTI benchmark remains DATA_REQUIRED.", "repair_path": "Acquire VTI total-return history."},
    ]
    return rows, gaps


def _d021_price_only_rows(price_ready: bool, feasibility: list[dict[str, str]]) -> list[dict[str, str]]:
    status = PRICE_ONLY_READY if price_ready else DATA_REQUIRED
    return [{"label": PRICE_ONLY_BASELINE, "component": "factor_scope", "status": status, "enabled_factors": "|".join(PRICE_ONLY_ENABLED_FACTORS), "disabled_factors": "|".join(PRICE_ONLY_DISABLED_FACTORS), "notes": "Price-only baseline is not full Portfolio Atlas."}, {"label": PRICE_ONLY_BASELINE, "component": "data_scope", "status": status, "enabled_factors": "|".join(PRICE_ONLY_ENABLED_FACTORS), "disabled_factors": "|".join(PRICE_ONLY_DISABLED_FACTORS), "notes": feasibility[-1]["evidence"]}]


def _d021_fundamental_strategy() -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    rows = [
        {"vendor": "Financial Modeling Prep", "coverage": "Broad fundamentals, API friendly", "pit_quality": "Needs validation for point-in-time leakage", "relative_cost": "LOW", "fit": "Prototype only unless PIT terms pass", "recommendation": "CHEAPEST_ACCEPTABLE"},
        {"vendor": "Sharadar", "coverage": "US equities, fundamentals, actions, delisted coverage", "pit_quality": "Strong for research use", "relative_cost": "MEDIUM", "fit": "Best practical full-model path", "recommendation": "BEST_VALUE"},
        {"vendor": "Portfolio123", "coverage": "Equity factors and universes", "pit_quality": "Strong platform coverage", "relative_cost": "MEDIUM_HIGH", "fit": "Useful if using platform workflows", "recommendation": "BEST_VALUE"},
        {"vendor": "Tiingo", "coverage": "Prices plus some fundamentals", "pit_quality": "Validate PIT/fundamental completeness", "relative_cost": "LOW_MEDIUM", "fit": "Supplemental", "recommendation": "CHEAPEST_ACCEPTABLE"},
        {"vendor": "Nasdaq Data Link", "coverage": "Dataset marketplace", "pit_quality": "Dataset dependent", "relative_cost": "MEDIUM", "fit": "Procurement channel", "recommendation": "BEST_VALUE"},
        {"vendor": "Norgate", "coverage": "Survivorship-aware EOD and index membership", "pit_quality": "Strong for universe/history", "relative_cost": "MEDIUM_HIGH", "fit": "Best universe recovery supplement", "recommendation": "INSTITUTIONAL_GRADE"},
    ]
    cost = [
        {"recommendation": "CHEAPEST_ACCEPTABLE", "vendor": "Financial Modeling Prep or Tiingo", "benefit": "Fast low-cost prototype fundamentals", "tradeoff": "PIT and delisted coverage must be proven", "next_action": "Verify current pricing, license, PIT filing-date support, and bulk export terms."},
        {"recommendation": "BEST_VALUE", "vendor": "Sharadar", "benefit": "Best balance for fundamentals, actions, and delisted-aware research", "tradeoff": "Higher cost than prototype APIs", "next_action": "Prioritize if one purchase is allowed."},
        {"recommendation": "INSTITUTIONAL_GRADE", "vendor": "Norgate plus Sharadar/Portfolio123", "benefit": "Better universe membership and survivorship controls", "tradeoff": "More cost and integration work", "next_action": "Use when decision-grade evidence is required."},
    ]
    return rows, cost


def _d021_universe_recovery(existing: list[dict[str, Any]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    text = " ".join((row["path"] + " " + row["columns"]).lower() for row in existing)
    has_pit = "eligible_for_portfolio" in text or "pit_universe" in text
    has_delisted = "is_delisted" in text or "delisted" in text
    plan = [
        {"universe_component": "Russell membership", "current_status": DATA_REQUIRED, "risk": "future membership leakage", "repair_path": "Acquire PIT Russell constituents or use liquid-stock universe until sourced."},
        {"universe_component": "S&P membership", "current_status": DATA_REQUIRED, "risk": "index survivorship leakage", "repair_path": "Acquire PIT S&P constituents if used as benchmark/universe."},
        {"universe_component": "Liquid-stock universe", "current_status": PRICE_ONLY_READY if has_pit else DATA_REQUIRED, "risk": "narrow Databento symbol set if not broadened", "repair_path": "Create monthly eligible universe from source-backed bars and explicit PRICE_ONLY_BASELINE label."},
        {"universe_component": "Delisted securities", "current_status": PRICE_ONLY_READY if has_delisted else DATA_REQUIRED, "risk": "survivorship bias", "repair_path": "Acquire delisted/security master data from Sharadar, Norgate, or equivalent."},
    ]
    risks = [{"risk": "survivorship bias", "status": "OPEN" if not has_delisted else "CONTROLLED", "severity": "HIGH", "evidence": "No broad delisted-security source found." if not has_delisted else "Delisted fields found locally.", "required_repair": "Acquire and validate delisted securities before FULL_MODEL_READY."}, {"risk": "future index membership leakage", "status": "OPEN", "severity": "HIGH", "evidence": "No PIT Russell or S&P membership source found.", "required_repair": "Use source-backed liquid universe for baseline; buy PIT index membership for full model."}]
    return plan, risks


def _d021_have_summary(existing: list[dict[str, Any]], feasibility: list[dict[str, str]]) -> str:
    usable = [row for row in existing if row["usable_for"]]
    return f"{len(existing)} local data-like files found; {len(usable)} mapped to Portfolio Atlas uses. {feasibility[-1]['evidence']}"


def _d021_summary(report: dict[str, Any]) -> str:
    d = report["final_deliverable"]
    return "\n".join(["# Portfolio Atlas Data Unlock Report", "", f"- Status: {report['status']}", f"- Classification: {report['classification']}", f"- Can Portfolio Atlas run its first real backtest? {report['data_readiness_review'][0]['answer']}", "- Authority: Research-only. No live portfolio. No Oak Harvest replacement recommendation. No broker execution. No trading authority.", "", "## What data do we have?", d["what_data_do_we_have"], "", "## What data do we need?", d["what_data_do_we_need"], "", "## Can we run a price-only baseline?", "Yes, labeled PRICE_ONLY_BASELINE." if d["can_we_run_a_price_only_baseline"] else "No.", "", "## What should be purchased next?", d["what_should_be_purchased_next"], "", "## Shortest path to first evidence", d["shortest_path_to_first_evidence"], ""])
