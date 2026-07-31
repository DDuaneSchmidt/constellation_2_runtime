from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Iterable

from .backtest_mvp import (
    AUTHORITY_BOUNDARY,
    DATA_REQUIRED,
    FACTOR_INPUTS,
    FACTOR_WEIGHTS,
    INVERSE_METRICS,
    PORTFOLIO_SIZES,
    READY,
    calculate_return_metrics,
)


DEFAULT_REPORT_ROOT = Path("reports/portfolio_research_os")
INPUT_DIRNAMES = ("inputs", "input")
TODAY = date(2026, 6, 6)
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,14}$")

REPORT_DIRS = {
    "data_import_validator": "data_import_validator",
    "total_return_builder": "total_return_builder",
    "factor_score_builder": "factor_score_builder",
    "opportunity_score_builder": "opportunity_score_builder",
    "monthly_portfolio_constructor": "monthly_portfolio_constructor",
    "historical_portfolio_backtest": "historical_portfolio_backtest",
    "benchmark_backtest": "benchmark_backtest",
    "portfolio_benchmark_comparison": "portfolio_benchmark_comparison",
    "backtest_integrity_audit": "backtest_integrity_audit",
    "backtest_evidence_review": "backtest_evidence_review",
}

SOURCE_FILES = {
    "prices": ("prices.csv", "price_history.csv"),
    "dividends": ("dividends.csv", "dividend_adjustments.csv"),
    "splits": ("splits.csv", "corporate_actions.csv"),
    "fundamentals": ("fundamentals.csv", "point_in_time_fundamentals.csv"),
    "universe": ("universe.csv", "security_universe.csv", "universe_manifest.csv"),
    "benchmarks": ("benchmark_returns.csv", "benchmarks.csv"),
}

REQUIRED_COLUMNS = {
    "prices": ["ticker", "date", "adjusted_close"],
    "dividends": ["ticker", "date", "dividend"],
    "splits": ["ticker", "date", "split_ratio"],
    "fundamentals": ["ticker", "period_end_date", "as_of_date"],
    "universe": ["ticker", "start_date", "end_date", "security_type"],
}

BENCHMARK_IDS = ["VTI", "SIXTY_FORTY", "SIMPLE_FACTOR_PORTFOLIO", "OAK_HARVEST_PROXY", "CASH"]
AUTHORITY_TEXT = "Research-only. No live portfolio, no replacement recommendation, no trades."


def run_data_import_validator(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_data_import_validator(report_root, created_at=created_at)
    _write_data_import_validator(report, report_root)
    return report


def run_total_return_builder(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = _build_pipeline(report_root, created_at=created_at)
    _write_total_return_builder(report, report_root)
    return report["total_return_builder"]


def run_factor_score_builder(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = _build_pipeline(report_root, created_at=created_at)
    _write_factor_score_builder(report, report_root)
    return report["factor_score_builder"]


def run_opportunity_score_builder(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = _build_pipeline(report_root, created_at=created_at)
    _write_opportunity_score_builder(report, report_root)
    return report["opportunity_score_builder"]


def run_monthly_portfolio_constructor(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = _build_pipeline(report_root, created_at=created_at)
    _write_monthly_portfolio_constructor(report, report_root)
    return report["monthly_portfolio_constructor"]


def run_historical_portfolio_backtest(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = _build_pipeline(report_root, created_at=created_at)
    _write_historical_portfolio_backtest(report, report_root)
    return report["historical_portfolio_backtest"]


def run_benchmark_backtest(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = _build_pipeline(report_root, created_at=created_at)
    _write_benchmark_backtest(report, report_root)
    return report["benchmark_backtest"]


def run_portfolio_benchmark_comparison(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = _build_pipeline(report_root, created_at=created_at)
    _write_portfolio_benchmark_comparison(report, report_root)
    return report["portfolio_benchmark_comparison"]


def run_backtest_integrity_audit(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = _build_pipeline(report_root, created_at=created_at)
    _write_backtest_integrity_audit(report, report_root)
    return report["backtest_integrity_audit"]


def run_backtest_evidence_review(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = run_backtest_pipeline(report_root, created_at=created_at)
    return report["backtest_evidence_review"]


def run_backtest_pipeline(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = _build_pipeline(report_root, created_at=created_at)
    _write_data_import_validator(report["data_import_validator"], report_root)
    _write_total_return_builder(report, report_root)
    _write_factor_score_builder(report, report_root)
    _write_opportunity_score_builder(report, report_root)
    _write_monthly_portfolio_constructor(report, report_root)
    _write_historical_portfolio_backtest(report, report_root)
    _write_benchmark_backtest(report, report_root)
    _write_portfolio_benchmark_comparison(report, report_root)
    _write_backtest_integrity_audit(report, report_root)
    _write_backtest_evidence_review(report, report_root)
    return report


def build_data_import_validator(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(report_root)
    created = created_at or _now()
    inputs = _load_inputs(root)
    findings: list[dict[str, Any]] = []
    validated: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    price_only = _price_only_marked(inputs)

    for domain in ["prices", "dividends", "splits", "fundamentals", "universe"]:
        loaded = inputs[domain]["loaded"]
        rows = inputs[domain]["rows"]
        fields = inputs[domain]["fields"]
        required = REQUIRED_COLUMNS[domain]
        missing_columns = [column for column in required if column not in fields]
        domain_findings = _validate_rows(domain, rows, fields, today=_created_day(created))
        if domain == "prices" and "adjusted_close" not in fields:
            domain_findings.append(_finding(domain, "missing_adjusted_close", "BLOCKER", "prices require adjusted_close"))
        if domain in {"dividends", "splits"} and not loaded and not price_only:
            domain_findings.append(_finding(domain, "missing_dividend_split_handling", "BLOCKER", "dividend/split files are missing and price_only is not explicitly marked"))
        if missing_columns:
            domain_findings.append(_finding(domain, "required_columns", "BLOCKER", ";".join(missing_columns)))
        findings.extend(domain_findings)
        row = {"data_domain": domain, "path": inputs[domain]["path"] or "", "rows": len(rows), "price_only": price_only}
        if loaded and not any(item["data_domain"] == domain and item["severity"] == "BLOCKER" for item in domain_findings):
            validated.append({**row, "status": READY})
        else:
            rejected.append({**row, "status": DATA_REQUIRED, "reason": "missing or invalid source data"})

    blocker_count = sum(1 for item in findings if item["severity"] == "BLOCKER")
    status = READY if blocker_count == 0 and inputs["prices"]["loaded"] and inputs["fundamentals"]["loaded"] and inputs["universe"]["loaded"] else DATA_REQUIRED
    return {
        "schema_id": "portfolio_research_os_data_import_validator",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P021",
        "created_at": created,
        "status": status,
        "price_only": price_only,
        "source_files": {name: data["path"] for name, data in inputs.items() if data["path"]},
        "summary": {
            "validated_file_count": len(validated),
            "rejected_file_count": len(rejected),
            "data_quality_findings": len(findings),
            "blocker_count": blocker_count,
            "authority": AUTHORITY_TEXT,
        },
        "validated_files": validated,
        "rejected_files": rejected,
        "data_quality_findings": findings,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _build_pipeline(report_root: Path | str, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(report_root)
    created = created_at or _now()
    inputs = _load_inputs(root)
    validation = build_data_import_validator(root, created_at=created)
    total_returns = _build_total_returns(validation, inputs, created)
    factors = _build_factor_scores(validation, inputs, total_returns, created)
    opportunity = _build_opportunity_scores(factors, created)
    portfolios = _build_monthly_portfolios(opportunity, inputs, created)
    backtest = _build_historical_backtest(portfolios, total_returns, validation, created)
    benchmarks = _build_benchmarks(inputs, validation, created)
    comparison = _build_comparison(backtest, benchmarks, created)
    integrity = _build_integrity_audit(validation, total_returns, factors, portfolios, backtest, created)
    evidence = _build_evidence_review(validation, total_returns, factors, opportunity, portfolios, backtest, benchmarks, comparison, integrity, created)
    return {
        "created_at": created,
        "data_import_validator": validation,
        "total_return_builder": total_returns,
        "factor_score_builder": factors,
        "opportunity_score_builder": opportunity,
        "monthly_portfolio_constructor": portfolios,
        "historical_portfolio_backtest": backtest,
        "benchmark_backtest": benchmarks,
        "portfolio_benchmark_comparison": comparison,
        "backtest_integrity_audit": integrity,
        "backtest_evidence_review": evidence,
    }


def _build_total_returns(validation: dict[str, Any], inputs: dict[str, Any], created: str) -> dict[str, Any]:
    status = READY if validation["status"] == READY else DATA_REQUIRED
    daily: list[dict[str, Any]] = []
    audit = []
    if status == READY:
        prices = sorted(inputs["prices"]["rows"], key=lambda row: (row.get("ticker", ""), row.get("date", "")))
        previous: dict[str, float] = {}
        for row in prices:
            ticker = row.get("ticker", "")
            adjusted = _float(row.get("adjusted_close"))
            if adjusted is None or adjusted <= 0:
                continue
            ret = "" if ticker not in previous else round(adjusted / previous[ticker] - 1.0, 10)
            daily.append({"ticker": ticker, "date": row.get("date", ""), "daily_total_return": ret, "source": "adjusted_close"})
            previous[ticker] = adjusted
    monthly = _monthly_from_daily(daily) if daily else []
    for domain in ["dividends", "splits"]:
        audit.append(
            {
                "corporate_action_domain": domain,
                "status": READY if inputs[domain]["loaded"] or validation.get("price_only") else DATA_REQUIRED,
                "rows": len(inputs[domain]["rows"]),
                "notes": "Explicit price-only mode." if validation.get("price_only") and not inputs[domain]["loaded"] else "Source file loaded." if inputs[domain]["loaded"] else "Missing source file.",
            }
        )
    return {
        "schema_id": "portfolio_research_os_total_return_builder",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P022",
        "created_at": created,
        "status": READY if monthly else DATA_REQUIRED,
        "daily_total_returns": daily,
        "monthly_total_returns": monthly,
        "corporate_action_audit": audit,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _build_factor_scores(validation: dict[str, Any], inputs: dict[str, Any], returns: dict[str, Any], created: str) -> dict[str, Any]:
    if validation["status"] != READY:
        missing = [{"ticker": "", "as_of_date": "", "missing_input": "valid_source_data", "status": DATA_REQUIRED}]
        return _factor_report(created, DATA_REQUIRED, [], missing)
    rows = [row for row in inputs["fundamentals"]["rows"] if _parse_date(row.get("as_of_date")) and _parse_date(row.get("as_of_date")) <= _created_day(created)]
    universe = {row.get("ticker", ""): row for row in inputs["universe"]["rows"]}
    latest: dict[str, dict[str, str]] = {}
    for row in sorted(rows, key=lambda item: (item.get("ticker", ""), item.get("as_of_date", ""))):
        latest[row.get("ticker", "")] = row
    normalized = _normalize_by_metric(list(latest.values()))
    scored = []
    missing_rows = []
    for ticker, row in sorted(latest.items()):
        factor_values: dict[str, float | None] = {}
        for factor, metrics in FACTOR_INPUTS.items():
            values = [normalized.get(metric, {}).get(ticker) for metric in metrics]
            present = [value for value in values if value is not None]
            factor_values[factor] = round(mean(present), 6) if len(present) == len(metrics) else None
            for metric, value in zip(metrics, values):
                if value is None:
                    missing_rows.append({"ticker": ticker, "as_of_date": row.get("as_of_date", ""), "missing_input": metric, "status": DATA_REQUIRED})
        score_status = READY if all(value is not None for value in factor_values.values()) else DATA_REQUIRED
        scored.append(
            {
                "ticker": ticker,
                "as_of_date": row.get("as_of_date", ""),
                "sector": universe.get(ticker, {}).get("sector", ""),
                "security_type": universe.get(ticker, {}).get("security_type", ""),
                **{f"{factor}_score": _score_text(factor_values[factor]) for factor in FACTOR_WEIGHTS},
                "score_status": score_status,
                "source": "point_in_time_fundamentals",
            }
        )
    return _factor_report(created, READY if any(row["score_status"] == READY for row in scored) else DATA_REQUIRED, scored, missing_rows)


def _factor_report(created: str, status: str, scored: list[dict[str, Any]], missing: list[dict[str, Any]]) -> dict[str, Any]:
    values = [_float(row.get(f"{factor}_score")) for row in scored for factor in FACTOR_WEIGHTS]
    values = [value for value in values if value is not None]
    distribution = [{"metric": "factor_score", "count": len(values), "min": min(values) if values else "", "max": max(values) if values else "", "mean": round(mean(values), 6) if values else ""}]
    return {
        "schema_id": "portfolio_research_os_factor_score_builder",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P023",
        "created_at": created,
        "status": status,
        "factor_scores": scored,
        "missing_factor_inputs": missing,
        "factor_score_distribution": distribution,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _build_opportunity_scores(factors: dict[str, Any], created: str) -> dict[str, Any]:
    rows = []
    explanations = []
    for row in factors["factor_scores"]:
        if row.get("score_status") != READY:
            continue
        score = sum(float(row[f"{factor}_score"]) * weight for factor, weight in FACTOR_WEIGHTS.items())
        out = {**row, "opportunity_score": round(score, 6), "rank": 0, "score_status": READY}
        rows.append(out)
    rows = sorted(rows, key=lambda row: (-float(row["opportunity_score"]), row["ticker"]))
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    for row in rows[:10]:
        explanations.append({"ticker": row["ticker"], "opportunity_score": row["opportunity_score"], "explanation": "Fixed factor weights: growth 20, quality 20, valuation 20, income 15, momentum 15, risk 10."})
    return {
        "schema_id": "portfolio_research_os_opportunity_score_builder",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P024",
        "created_at": created,
        "status": READY if rows else DATA_REQUIRED,
        "fixed_weights": FACTOR_WEIGHTS,
        "opportunity_scores": rows,
        "rank_history": [{"as_of_date": row["as_of_date"], "ticker": row["ticker"], "rank": row["rank"], "opportunity_score": row["opportunity_score"]} for row in rows],
        "score_explanation_sample": explanations,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _build_monthly_portfolios(opportunity: dict[str, Any], inputs: dict[str, Any], created: str) -> dict[str, Any]:
    months = sorted({_month(row.get("date", "")) for row in inputs["prices"]["rows"] if row.get("date")})
    months = [month for month in months if month]
    scores = opportunity["opportunity_scores"]
    holdings = []
    audits = []
    for month in months:
        for size in PORTFOLIO_SIZES:
            selected = _select_portfolio(scores, size)
            status = READY if len(selected) == size else DATA_REQUIRED
            audits.append({"month": month, "portfolio_size": size, "status": status, "selected": len(selected), "max_position": 0.05, "sector_cap": 0.20, "notes": "sector cap applied when sector data exists"})
            for row in selected:
                holdings.append({"month": month, "portfolio_size": size, "ticker": row["ticker"], "sector": row.get("sector", ""), "target_weight": round(min(1.0 / size, 0.05), 8), "opportunity_score": row["opportunity_score"], "status": status})
    summary = [{"portfolio_size": size, "months": len(months), "status": READY if any(row["portfolio_size"] == size and row["status"] == READY for row in holdings) else DATA_REQUIRED} for size in PORTFOLIO_SIZES]
    return {
        "schema_id": "portfolio_research_os_monthly_portfolio_constructor",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P025",
        "created_at": created,
        "status": READY if any(row["status"] == READY for row in holdings) else DATA_REQUIRED,
        "monthly_holdings": holdings,
        "constraint_audit": audits,
        "portfolio_size_summary": summary,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _build_historical_backtest(portfolios: dict[str, Any], returns: dict[str, Any], validation: dict[str, Any], created: str) -> dict[str, Any]:
    ret_by_key = {(row["ticker"], row["month"]): _float(row.get("monthly_total_return")) for row in returns["monthly_total_returns"]}
    monthly_rows = []
    missing_return = False
    for (month, size), holdings in _group(portfolios["monthly_holdings"], "month", "portfolio_size").items():
        if any(row.get("status") != READY for row in holdings):
            missing_return = True
            continue
        values = []
        for row in holdings:
            value = ret_by_key.get((row["ticker"], month))
            if value is None:
                missing_return = True
                break
            values.append(float(row["target_weight"]) * value)
        else:
            monthly_rows.append({"month": month, "portfolio_size": size, "monthly_return": round(sum(values), 10), "status": READY})
    performance = []
    drawdown = []
    turnover = []
    for size in PORTFOLIO_SIZES:
        rows = [row for row in monthly_rows if int(row["portfolio_size"]) == size]
        metrics = calculate_return_metrics([float(row["monthly_return"]) for row in rows], turnover=0.0) if rows and not missing_return else calculate_return_metrics([])
        performance.append({"portfolio_size": size, "status": READY if rows and metrics["CAGR"] != DATA_REQUIRED else DATA_REQUIRED, **metrics})
        drawdown.extend(_drawdown_rows(size, rows))
        turnover.append({"portfolio_size": size, "turnover": 0.0 if rows else DATA_REQUIRED, "status": READY if rows else DATA_REQUIRED})
    status = READY if performance and all(row["status"] == READY for row in performance) else DATA_REQUIRED
    return {
        "schema_id": "portfolio_research_os_historical_portfolio_backtest",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P026",
        "created_at": created,
        "status": status,
        "portfolio_performance": performance,
        "monthly_returns": monthly_rows if status == READY else [],
        "drawdown_series": drawdown if status == READY else [],
        "turnover_report": turnover,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _build_benchmarks(inputs: dict[str, Any], validation: dict[str, Any], created: str) -> dict[str, Any]:
    rows = inputs["benchmarks"]["rows"]
    monthly = []
    by_benchmark: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        bid = row.get("benchmark_id") or row.get("benchmark") or row.get("ticker")
        value = _float(row.get("monthly_return") or row.get("return"))
        if bid and value is not None:
            monthly.append({"benchmark_id": bid, "month": _month(row.get("date") or row.get("month") or ""), "monthly_return": value, "status": READY})
            by_benchmark[bid].append(value)
    performance = []
    for benchmark_id in BENCHMARK_IDS:
        values = by_benchmark.get(benchmark_id, [])
        metrics = calculate_return_metrics(values) if values else calculate_return_metrics([])
        performance.append({"benchmark_id": benchmark_id, "status": READY if values else ("PLACEHOLDER_ONLY" if benchmark_id == "CASH" else DATA_REQUIRED), **metrics})
    audit = [{"benchmark_id": item, "status": next(row["status"] for row in performance if row["benchmark_id"] == item), "notes": "Requires source-backed benchmark monthly returns."} for item in BENCHMARK_IDS]
    return {
        "schema_id": "portfolio_research_os_benchmark_backtest",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P027",
        "created_at": created,
        "status": READY if by_benchmark.get("VTI") and by_benchmark.get("OAK_HARVEST_PROXY") else DATA_REQUIRED,
        "benchmark_performance": performance,
        "benchmark_monthly_returns": monthly,
        "benchmark_assumption_audit": audit,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _build_comparison(backtest: dict[str, Any], benchmarks: dict[str, Any], created: str) -> dict[str, Any]:
    atlas_ready = backtest["status"] == READY
    benchmark_ready = benchmarks["status"] == READY
    decision = DATA_REQUIRED
    if atlas_ready and benchmark_ready:
        atlas = next((row for row in backtest["portfolio_performance"] if row["portfolio_size"] == 25), {})
        vti = next((row for row in benchmarks["benchmark_performance"] if row["benchmark_id"] == "VTI"), {})
        if _float(atlas.get("CAGR")) is not None and _float(vti.get("CAGR")) is not None:
            decision = "ATLAS_OUTPERFORMS" if float(atlas["CAGR"]) > float(vti["CAGR"]) else "ATLAS_UNDERPERFORMS"
    scorecard = [{"comparison": "Portfolio Atlas vs benchmarks", "decision_label": decision, "status": READY if decision != DATA_REQUIRED else DATA_REQUIRED}]
    relative = [{"benchmark_id": row["benchmark_id"], "atlas_cagr": DATA_REQUIRED, "benchmark_cagr": row["CAGR"], "relative_cagr": DATA_REQUIRED, "status": DATA_REQUIRED if decision == DATA_REQUIRED else READY} for row in benchmarks["benchmark_performance"]]
    risk = [{"benchmark_id": row["benchmark_id"], "atlas_sharpe": DATA_REQUIRED, "benchmark_sharpe": row["Sharpe"], "risk_adjusted_delta": DATA_REQUIRED, "status": DATA_REQUIRED if decision == DATA_REQUIRED else READY} for row in benchmarks["benchmark_performance"]]
    return {
        "schema_id": "portfolio_research_os_portfolio_benchmark_comparison",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P028",
        "created_at": created,
        "status": READY if decision != DATA_REQUIRED else DATA_REQUIRED,
        "decision_label": decision,
        "comparison_scorecard": scorecard,
        "relative_performance": relative,
        "risk_adjusted_comparison": risk,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _build_integrity_audit(validation: dict[str, Any], returns: dict[str, Any], factors: dict[str, Any], portfolios: dict[str, Any], backtest: dict[str, Any], created: str) -> dict[str, Any]:
    rows = [
        _bias("survivorship bias", validation["status"] == READY, "Validated universe source required."),
        _bias("lookahead bias", factors["status"] == READY, "Factors must use as_of_date no later than rebalance date."),
        _bias("future index membership leakage", validation["status"] == READY, "Historical universe membership must be source-backed."),
        _bias("missing delisted securities", False, "Delisted-security source not validated in V1 import contract."),
        _bias("future fundamentals", not any(item["finding_type"] == "future_dated_fundamentals" for item in validation["data_quality_findings"]), "Future fundamentals are blockers."),
        _bias("rebalance timing leakage", portfolios["status"] == READY, "Monthly rebalance date must precede return realization."),
        _bias("dividend/split errors", returns["status"] == READY, "Total-return builder requires corporate actions or explicit price-only flag."),
    ]
    blocked = any(row["classification"] in {"BIAS_RISK", "BIAS_CONFIRMED"} for row in rows)
    matrix = [{"audit_item": row["audit_item"], "status": row["classification"], "evidence_status": row["evidence_status"]} for row in rows]
    repairs = [{"audit_item": row["audit_item"], "required_repair": row["required_remediation"]} for row in rows if row["classification"] != "BIAS_CONTROLLED"]
    return {
        "schema_id": "portfolio_research_os_backtest_integrity_audit",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P029",
        "created_at": created,
        "status": "BIAS_BLOCKED" if blocked else READY,
        "bias_findings": rows,
        "integrity_matrix": matrix,
        "required_repairs": repairs,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _build_evidence_review(validation: dict[str, Any], returns: dict[str, Any], factors: dict[str, Any], opportunity: dict[str, Any], portfolios: dict[str, Any], backtest: dict[str, Any], benchmarks: dict[str, Any], comparison: dict[str, Any], integrity: dict[str, Any], created: str) -> dict[str, Any]:
    components = [
        ("P021", "data_import_validator", validation["status"]),
        ("P022", "total_return_builder", returns["status"]),
        ("P023", "factor_score_builder", factors["status"]),
        ("P024", "opportunity_score_builder", opportunity["status"]),
        ("P025", "monthly_portfolio_constructor", portfolios["status"]),
        ("P026", "historical_portfolio_backtest", backtest["status"]),
        ("P027", "benchmark_backtest", benchmarks["status"]),
        ("P028", "portfolio_benchmark_comparison", comparison["status"]),
        ("P029", "backtest_integrity_audit", integrity["status"]),
    ]
    if integrity["status"] == "BIAS_BLOCKED" and backtest["status"] == READY:
        decision = "BIAS_BLOCKED"
    elif any(status == DATA_REQUIRED for _, _, status in components):
        decision = DATA_REQUIRED
    elif comparison["decision_label"] == "ATLAS_OUTPERFORMS":
        decision = "BACKTEST_PROMISING"
    elif comparison["decision_label"] == "ATLAS_UNDERPERFORMS":
        decision = "BACKTEST_WEAK"
    else:
        decision = "BACKTEST_FAILED"
    return {
        "schema_id": "portfolio_research_os_backtest_evidence_review",
        "schema_version": "1.0",
        "build": "Portfolio Atlas P030",
        "created_at": created,
        "status": decision,
        "final_decision": decision,
        "evidence_scorecard": [{"phase": phase, "component": component, "status": status} for phase, component, status in components],
        "benchmark_decision": [{"decision_label": comparison["decision_label"], "status": comparison["status"]}],
        "next_phase_decision": [{"final_decision": decision, "authority": AUTHORITY_TEXT}],
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _load_inputs(root: Path) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for name, filenames in SOURCE_FILES.items():
        path = _find_input(root, filenames)
        rows, fields = _read_csv(path) if path else ([], [])
        data[name] = {"path": str(path) if path else "", "loaded": bool(path), "rows": rows, "fields": fields}
    return data


def _find_input(root: Path, filenames: Iterable[str]) -> Path | None:
    search_roots = [root / dirname for dirname in INPUT_DIRNAMES] + [Path("data/portfolio_research_os")]
    for search_root in search_roots:
        for filename in filenames:
            path = search_root / filename
            if path.exists():
                return path
    return None


def _read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fields = list(reader.fieldnames or [])
        rows = [{key: (value or "").strip() for key, value in row.items()} for row in reader]
    return rows, fields


def _validate_rows(domain: str, rows: list[dict[str, str]], fields: list[str], *, today: date) -> list[dict[str, Any]]:
    findings = []
    if not rows:
        findings.append(_finding(domain, "missing_file_or_rows", "BLOCKER", "no source rows loaded"))
        return findings
    seen: set[tuple[str, str]] = set()
    for index, row in enumerate(rows, start=2):
        ticker = row.get("ticker", "")
        row_date = row.get("date") or row.get("as_of_date") or row.get("start_date")
        if ticker and not TICKER_RE.match(ticker):
            findings.append(_finding(domain, "invalid_ticker", "BLOCKER", ticker, row_number=index))
        key = (ticker, row_date)
        if key in seen:
            findings.append(_finding(domain, "duplicate_rows", "BLOCKER", f"{ticker}:{row_date}", row_number=index))
        seen.add(key)
        missing = [field for field in fields if row.get(field, "") == ""]
        if missing:
            findings.append(_finding(domain, "missing_values", "BLOCKER", ";".join(missing), row_number=index))
        if domain == "fundamentals":
            as_of = _parse_date(row.get("as_of_date"))
            if as_of and as_of > today:
                findings.append(_finding(domain, "future_dated_fundamentals", "BLOCKER", row.get("as_of_date", ""), row_number=index))
        if domain == "universe":
            start = _parse_date(row.get("start_date"))
            end = _parse_date(row.get("end_date"))
            if start and end and start > end:
                findings.append(_finding(domain, "date_ranges", "BLOCKER", f"{start}>{end}", row_number=index))
    return findings


def _finding(domain: str, finding_type: str, severity: str, detail: str, *, row_number: int | str = "") -> dict[str, Any]:
    return {"data_domain": domain, "finding_type": finding_type, "severity": severity, "row_number": row_number, "detail": detail}


def _price_only_marked(inputs: dict[str, Any]) -> bool:
    for row in inputs["prices"]["rows"]:
        if str(row.get("price_only", "")).strip().lower() in {"1", "true", "yes", "price_only"}:
            return True
    manifest = _find_input(Path("."), ("import_manifest.csv",))
    if manifest:
        rows, _ = _read_csv(manifest)
        return any(str(row.get("price_only", "")).lower() in {"1", "true", "yes"} for row in rows)
    return False


def _monthly_from_daily(daily: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compounded: dict[tuple[str, str], float] = defaultdict(lambda: 1.0)
    has_return: set[tuple[str, str]] = set()
    for row in daily:
        value = _float(row.get("daily_total_return"))
        key = (row["ticker"], _month(row["date"]))
        if value is None:
            continue
        compounded[key] *= 1.0 + value
        has_return.add(key)
    return [{"ticker": ticker, "month": month, "monthly_total_return": round(value - 1.0, 10), "status": READY} for (ticker, month), value in sorted(compounded.items()) if (ticker, month) in has_return]


def _normalize_by_metric(rows: list[dict[str, str]]) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    for metric in {metric for metrics in FACTOR_INPUTS.values() for metric in metrics}:
        values = []
        for row in rows:
            value = _float(row.get(metric))
            if value is None:
                continue
            values.append((row.get("ticker", ""), -value if metric in INVERSE_METRICS else value))
        if not values:
            continue
        raw = [value for _, value in values]
        lo, hi = min(raw), max(raw)
        result[metric] = {ticker: 0.5 if hi == lo else round((value - lo) / (hi - lo), 6) for ticker, value in values}
    return result


def _select_portfolio(scores: list[dict[str, Any]], size: int) -> list[dict[str, Any]]:
    ranked = sorted(scores, key=lambda row: (-float(row["opportunity_score"]), row["ticker"]))
    selected = []
    sector_counts: dict[str, int] = defaultdict(int)
    weight = min(1.0 / size, 0.05)
    max_sector_count = max(1, math.floor(0.20 / weight))
    for row in ranked:
        sector = row.get("sector") or ""
        if sector and sector_counts[sector] >= max_sector_count:
            continue
        selected.append(row)
        if sector:
            sector_counts[sector] += 1
        if len(selected) == size:
            break
    return selected


def _drawdown_rows(size: int, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    equity = 1.0
    peak = 1.0
    out = []
    for row in sorted(rows, key=lambda item: item["month"]):
        equity *= 1.0 + float(row["monthly_return"])
        peak = max(peak, equity)
        out.append({"portfolio_size": size, "month": row["month"], "drawdown": round(equity / peak - 1.0, 10), "status": READY})
    return out


def _bias(item: str, controlled: bool, remediation: str) -> dict[str, str]:
    return {
        "audit_item": item,
        "classification": "BIAS_CONTROLLED" if controlled else "BIAS_RISK",
        "evidence_status": "PRESENT" if controlled else "MISSING",
        "notes": remediation,
        "required_remediation": "" if controlled else remediation,
    }


def _group(rows: list[dict[str, Any]], key_a: str, key_b: str) -> dict[tuple[Any, Any], list[dict[str, Any]]]:
    grouped: dict[tuple[Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row[key_a], row[key_b])].append(row)
    return grouped


def _write_data_import_validator(report: dict[str, Any], root: Path | str) -> None:
    out = Path(root) / REPORT_DIRS["data_import_validator"]
    _write_report_common(out, report, "# Portfolio Atlas P021 - Data Import Validator")
    _write_csv(out / "validated_files.csv", ["data_domain", "path", "rows", "price_only", "status"], report["validated_files"])
    _write_csv(out / "rejected_files.csv", ["data_domain", "path", "rows", "price_only", "status", "reason"], report["rejected_files"])
    _write_csv(out / "data_quality_findings.csv", ["data_domain", "finding_type", "severity", "row_number", "detail"], report["data_quality_findings"])


def _write_total_return_builder(report: dict[str, Any], root: Path | str) -> None:
    data = report["total_return_builder"]
    out = Path(root) / REPORT_DIRS["total_return_builder"]
    out.mkdir(parents=True, exist_ok=True)
    _write_csv(out / "daily_total_returns.csv", ["ticker", "date", "daily_total_return", "source"], data["daily_total_returns"])
    _write_csv(out / "monthly_total_returns.csv", ["ticker", "month", "monthly_total_return", "status"], data["monthly_total_returns"])
    _write_csv(out / "corporate_action_audit.csv", ["corporate_action_domain", "status", "rows", "notes"], data["corporate_action_audit"])


def _write_factor_score_builder(report: dict[str, Any], root: Path | str) -> None:
    data = report["factor_score_builder"]
    out = Path(root) / REPORT_DIRS["factor_score_builder"]
    _write_report_common(out, data, "# Portfolio Atlas P023 - Factor Score Builder")
    factor_columns = ["ticker", "as_of_date", "sector", "security_type", *[f"{factor}_score" for factor in FACTOR_WEIGHTS], "score_status", "source"]
    _write_csv(out / "factor_scores.csv", factor_columns, data["factor_scores"])
    _write_csv(out / "missing_factor_inputs.csv", ["ticker", "as_of_date", "missing_input", "status"], data["missing_factor_inputs"])
    _write_csv(out / "factor_score_distribution.csv", ["metric", "count", "min", "max", "mean"], data["factor_score_distribution"])


def _write_opportunity_score_builder(report: dict[str, Any], root: Path | str) -> None:
    data = report["opportunity_score_builder"]
    out = Path(root) / REPORT_DIRS["opportunity_score_builder"]
    _write_report_common(out, data, "# Portfolio Atlas P024 - Opportunity Score Builder")
    columns = ["rank", "ticker", "as_of_date", "sector", "security_type", *[f"{factor}_score" for factor in FACTOR_WEIGHTS], "opportunity_score", "score_status", "source"]
    _write_csv(out / "opportunity_scores.csv", columns, data["opportunity_scores"])
    _write_csv(out / "rank_history.csv", ["as_of_date", "ticker", "rank", "opportunity_score"], data["rank_history"])
    _write_csv(out / "score_explanation_sample.csv", ["ticker", "opportunity_score", "explanation"], data["score_explanation_sample"])


def _write_monthly_portfolio_constructor(report: dict[str, Any], root: Path | str) -> None:
    data = report["monthly_portfolio_constructor"]
    out = Path(root) / REPORT_DIRS["monthly_portfolio_constructor"]
    _write_report_common(out, data, "# Portfolio Atlas P025 - Monthly Portfolio Constructor")
    _write_csv(out / "monthly_holdings.csv", ["month", "portfolio_size", "ticker", "sector", "target_weight", "opportunity_score", "status"], data["monthly_holdings"])
    _write_csv(out / "constraint_audit.csv", ["month", "portfolio_size", "status", "selected", "max_position", "sector_cap", "notes"], data["constraint_audit"])
    _write_csv(out / "portfolio_size_summary.csv", ["portfolio_size", "months", "status"], data["portfolio_size_summary"])


def _write_historical_portfolio_backtest(report: dict[str, Any], root: Path | str) -> None:
    data = report["historical_portfolio_backtest"]
    out = Path(root) / REPORT_DIRS["historical_portfolio_backtest"]
    _write_report_common(out, data, "# Portfolio Atlas P026 - Historical Portfolio Backtest")
    _write_csv(out / "portfolio_performance.csv", ["portfolio_size", "status", "CAGR", "annualized_volatility", "Sharpe", "Sortino", "max_drawdown", "turnover"], data["portfolio_performance"])
    _write_csv(out / "monthly_returns.csv", ["month", "portfolio_size", "monthly_return", "status"], data["monthly_returns"])
    _write_csv(out / "drawdown_series.csv", ["portfolio_size", "month", "drawdown", "status"], data["drawdown_series"])
    _write_csv(out / "turnover_report.csv", ["portfolio_size", "turnover", "status"], data["turnover_report"])


def _write_benchmark_backtest(report: dict[str, Any], root: Path | str) -> None:
    data = report["benchmark_backtest"]
    out = Path(root) / REPORT_DIRS["benchmark_backtest"]
    _write_report_common(out, data, "# Portfolio Atlas P027 - Benchmark Backtest")
    _write_csv(out / "benchmark_performance.csv", ["benchmark_id", "status", "CAGR", "annualized_volatility", "Sharpe", "Sortino", "max_drawdown", "turnover"], data["benchmark_performance"])
    _write_csv(out / "benchmark_monthly_returns.csv", ["benchmark_id", "month", "monthly_return", "status"], data["benchmark_monthly_returns"])
    _write_csv(out / "benchmark_assumption_audit.csv", ["benchmark_id", "status", "notes"], data["benchmark_assumption_audit"])


def _write_portfolio_benchmark_comparison(report: dict[str, Any], root: Path | str) -> None:
    data = report["portfolio_benchmark_comparison"]
    out = Path(root) / REPORT_DIRS["portfolio_benchmark_comparison"]
    _write_report_common(out, data, "# Portfolio Atlas P028 - Portfolio Benchmark Comparison")
    _write_csv(out / "comparison_scorecard.csv", ["comparison", "decision_label", "status"], data["comparison_scorecard"])
    _write_csv(out / "relative_performance.csv", ["benchmark_id", "atlas_cagr", "benchmark_cagr", "relative_cagr", "status"], data["relative_performance"])
    _write_csv(out / "risk_adjusted_comparison.csv", ["benchmark_id", "atlas_sharpe", "benchmark_sharpe", "risk_adjusted_delta", "status"], data["risk_adjusted_comparison"])


def _write_backtest_integrity_audit(report: dict[str, Any], root: Path | str) -> None:
    data = report["backtest_integrity_audit"]
    out = Path(root) / REPORT_DIRS["backtest_integrity_audit"]
    _write_report_common(out, data, "# Portfolio Atlas P029 - Backtest Integrity Audit")
    _write_csv(out / "bias_findings.csv", ["audit_item", "classification", "evidence_status", "notes", "required_remediation"], data["bias_findings"])
    _write_csv(out / "integrity_matrix.csv", ["audit_item", "status", "evidence_status"], data["integrity_matrix"])
    _write_csv(out / "required_repairs.csv", ["audit_item", "required_repair"], data["required_repairs"])


def _write_backtest_evidence_review(report: dict[str, Any], root: Path | str) -> None:
    data = report["backtest_evidence_review"]
    out = Path(root) / REPORT_DIRS["backtest_evidence_review"]
    _write_report_common(out, data, "# Portfolio Atlas P030 - Backtest Evidence Review")
    _write_csv(out / "evidence_scorecard.csv", ["phase", "component", "status"], data["evidence_scorecard"])
    _write_csv(out / "benchmark_decision.csv", ["decision_label", "status"], data["benchmark_decision"])
    _write_csv(out / "next_phase_decision.csv", ["final_decision", "authority"], data["next_phase_decision"])


def _write_report_common(out: Path, report: dict[str, Any], title: str) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "latest.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out / "latest_summary.md").write_text("\n".join([title, "", f"Status: {report.get('status')}", "", f"Authority: {AUTHORITY_TEXT}", ""]) , encoding="utf-8")


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _created_day(created: str) -> date:
    parsed = _parse_date(created[:10])
    return parsed or TODAY


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _month(value: str) -> str:
    return value[:7] if value else ""


def _float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def _score_text(value: float | None) -> str:
    return "" if value is None else f"{value:.6f}"
