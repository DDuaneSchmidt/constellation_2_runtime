from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .data_import_package import DEFAULT_REPORT_ROOT


REPORT_DIR = "full_model_data_transition"

FULL_MODEL_READY = "FULL_MODEL_READY"
PARTIALLY_READY = "PARTIALLY_READY"
DATA_REQUIRED = "DATA_REQUIRED"
BLOCKED = "BLOCKED"
USE_PORTFOLIO123_BRIDGE = "USE_PORTFOLIO123_BRIDGE"

AUTHORITY = "Research-only. No trades. No recommendations."

FULL_MODEL_FACTORS = ["growth", "quality", "valuation", "income", "momentum", "risk", "PEG", "PEGY"]


def run_full_model_transition(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_full_model_transition(created_at=created_at)
    write_full_model_transition(report, report_root)
    return report


def build_full_model_transition(*, created_at: str | None = None) -> dict[str, Any]:
    created = created_at or _now()
    gap_report = _full_model_gap_report()
    sharadar_contract = _sharadar_import_contract()
    p123_contract = _portfolio123_export_contract()
    p123_evidence = _portfolio123_evidence_expansion()
    small_cap_review = _small_cap_quality_review()
    test_plan = _portfolio_atlas_v1_test_plan()
    validators = _import_validators()
    skeleton = _full_model_backtest_skeleton()
    readiness = _readiness_decision(gap_report, validators)
    evidence_review = _evidence_review(readiness)
    return {
        "schema_id": "portfolio_research_os_full_model_data_transition",
        "schema_version": "1.0",
        "program_id": "PF001-PF030",
        "created_at": created,
        "status": readiness["classification"],
        "decision": evidence_review["decision"],
        "full_model_gap_report": gap_report,
        "sharadar_import_contract": sharadar_contract,
        "portfolio123_export_contract": p123_contract,
        "portfolio123_evidence": p123_evidence,
        "small_cap_quality_review": small_cap_review,
        "portfolio_atlas_v1_p123_test_plan": test_plan,
        "factor_import_validators": validators,
        "full_model_backtest_skeleton": skeleton,
        "full_model_readiness_decision": readiness,
        "full_model_evidence_review": evidence_review,
        "required_conclusions": [
            "PIT fundamentals, filing dates, dividends/splits, sector/industry, delisted securities, broad universe, VTI total returns, and risk-free rates remain required for the full model.",
            "Portfolio123 evidence is external/manual directional evidence, not proof of Portfolio Atlas future performance.",
            "Small Cap Quality is useful for hypothesis generation and comparison, not a template to copy blindly.",
            "Full-model factors remain blocked without point-in-time fundamentals and filing-date controls.",
        ],
        "authority": AUTHORITY,
        "authority_boundary": {
            "research_only": True,
            "recommendations_authorized": False,
            "capital_allocation_authorized": False,
            "trades_authorized": False,
            "broker_execution_authorized": False,
        },
    }


def write_full_model_transition(report: dict[str, Any], report_root: Path | str = DEFAULT_REPORT_ROOT) -> dict[str, Path]:
    out = Path(report_root) / REPORT_DIR
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out / "latest.json",
        "latest_summary": out / "latest_summary.md",
        "full_model_gap_report": out / "full_model_gap_report.csv",
        "sharadar_import_contract": out / "sharadar_import_contract.csv",
        "portfolio123_export_contract": out / "portfolio123_export_contract.csv",
        "small_cap_quality_review": out / "small_cap_quality_review.csv",
        "portfolio_atlas_v1_p123_test_plan": out / "portfolio_atlas_v1_p123_test_plan.csv",
        "full_model_readiness_decision": out / "full_model_readiness_decision.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(_summary(report), encoding="utf-8")
    _write_csv(paths["full_model_gap_report"], report["full_model_gap_report"], ["requirement", "status", "why_required", "blocks"])
    _write_csv(paths["sharadar_import_contract"], report["sharadar_import_contract"], ["dataset", "required_fields", "purpose", "validation_rule"])
    _write_csv(paths["portfolio123_export_contract"], report["portfolio123_export_contract"], ["export", "required_fields", "purpose", "evidence_label"])
    _write_csv(paths["small_cap_quality_review"], report["small_cap_quality_review"], ["dimension", "observed_value", "interpretation", "atlas_implication", "copy_risk"])
    _write_csv(paths["portfolio_atlas_v1_p123_test_plan"], report["portfolio_atlas_v1_p123_test_plan"], ["component", "specification", "metrics_to_capture"])
    _write_csv(paths["full_model_readiness_decision"], [report["full_model_readiness_decision"], report["full_model_evidence_review"]], ["classification", "decision", "reason", "recommended_next_action", "authority"])
    return paths


def _full_model_gap_report() -> list[dict[str, str]]:
    return [
        {"requirement": "PIT fundamentals", "status": DATA_REQUIRED, "why_required": "Needed for quality, growth, valuation, PEG, and PEGY without lookahead.", "blocks": "full factor model"},
        {"requirement": "filing_date", "status": DATA_REQUIRED, "why_required": "Controls when fundamentals became known.", "blocks": "point-in-time scoring"},
        {"requirement": "dividends/splits", "status": DATA_REQUIRED, "why_required": "Needed for income, total return, and corporate action adjustment.", "blocks": "income and total return"},
        {"requirement": "sector/industry", "status": DATA_REQUIRED, "why_required": "Needed for concentration controls and explainability.", "blocks": "risk controls"},
        {"requirement": "delisted securities", "status": DATA_REQUIRED, "why_required": "Needed to reduce survivorship bias.", "blocks": "decision-grade backtest"},
        {"requirement": "broad universe", "status": DATA_REQUIRED, "why_required": "Needed for realistic ranking breadth and small-cap tests.", "blocks": "Portfolio_Atlas_Full_v1"},
        {"requirement": "VTI/benchmark total returns", "status": DATA_REQUIRED, "why_required": "Needed for benchmark comparison.", "blocks": "evidence review"},
        {"requirement": "risk-free rates", "status": DATA_REQUIRED, "why_required": "Needed for Sharpe, excess return, and cash proxy.", "blocks": "risk-adjusted validation"},
    ]


def _sharadar_import_contract() -> list[dict[str, str]]:
    return [
        {"dataset": "prices", "required_fields": "date,ticker,open,high,low,close,volume,adjusted_close,dividend,split_factor", "purpose": "Adjusted price and total-return base", "validation_rule": "No duplicate date/ticker rows; adjusted_close present; split/dividend fields numeric"},
        {"dataset": "fundamentals", "required_fields": "ticker,dimension,calendardate,datekey,reportperiod,revenue,eps,fcf,roic,grossmargin,operatingmargin,netmargin,sharesbas,marketcap,ev,pe,pb,ps", "purpose": "Quality, growth, valuation, PEG, PEGY inputs", "validation_rule": "datekey/filing date must be on or before scoring as_of_date"},
        {"dataset": "tickers", "required_fields": "ticker,name,exchange,category,sector,industry,scalemarketcap,active,delisted,lastupdated", "purpose": "Broad universe, sector/industry, active and delisted status", "validation_rule": "Must include active and delisted securities"},
        {"dataset": "actions", "required_fields": "date,ticker,action,value,contraticker,contraname", "purpose": "Splits, dividends, ticker changes, corporate actions", "validation_rule": "Actions reconcile to price adjustment fields"},
        {"dataset": "indicators", "required_fields": "date,indicator,value", "purpose": "Risk-free rates, inflation, market regime indicators", "validation_rule": "Source and frequency must be documented"},
        {"dataset": "filing dates", "required_fields": "ticker,reportperiod,filing_date,datekey,accepted_timestamp", "purpose": "Point-in-time availability control", "validation_rule": "filing_date required for every fundamental row used in scoring"},
        {"dataset": "delisted status", "required_fields": "ticker,delisted,delisting_date,delisting_reason,last_price_date", "purpose": "Survivorship-bias control", "validation_rule": "Delisted securities must remain available historically"},
    ]


def _portfolio123_export_contract() -> list[dict[str, str]]:
    fields = {
        "model performance": "model_name,start_date,end_date,CAGR,benchmark_CAGR,Sharpe,Sortino,max_drawdown,turnover,win_rate",
        "holdings": "rebalance_date,ticker,name,weight,rank,sector,industry,market_cap",
        "ranking factors": "factor_name,formula,weight,direction,rank_bucket,score",
        "universe": "date,universe_name,ticker,eligible,exclusion_reason,market_cap,liquidity",
        "rebalance history": "rebalance_date,positions,buy_count,sell_count,cash,benchmark",
        "transactions": "date,ticker,side,shares_or_weight,price,commission,slippage,reason",
        "benchmark comparison": "date,model_return,benchmark_return,benchmark_name,excess_return,drawdown",
    }
    return [{"export": name, "required_fields": required, "purpose": "Portfolio123 bridge evidence and reproducibility", "evidence_label": "EXTERNAL_MANUAL_NOT_PROOF"} for name, required in fields.items()]


def _portfolio123_evidence_expansion() -> list[dict[str, str]]:
    return [
        {"model": "Small Cap Quality", "return": "21.7% 10Y annualized", "benchmark_return": "Russell 2000 10.5% 10Y annualized", "drawdown": "not captured", "Sharpe": "not captured", "Sortino": "not captured", "holdings": "not captured", "factor_philosophy": "quality, growth, reasonable valuation, small-cap exposure", "implication_for_portfolio_atlas": "Promising external direction; requires direct Portfolio_Atlas_v1 test"},
        {"model": "Profitable Leaders", "return": "roughly market-like", "benchmark_return": "not captured", "drawdown": "not captured", "Sharpe": "not captured", "Sortino": "not captured", "holdings": "not captured", "factor_philosophy": "profitability", "implication_for_portfolio_atlas": "Profitability alone is insufficient"},
        {"model": "Built for Stability", "return": "much lower return", "benchmark_return": "not captured", "drawdown": "lower drawdown", "Sharpe": "not captured", "Sortino": "not captured", "holdings": "not captured", "factor_philosophy": "stability/low volatility", "implication_for_portfolio_atlas": "Stability is not the leading return direction"},
        {"model": "CANSLIM", "return": "mixed", "benchmark_return": "not captured", "drawdown": "not captured", "Sharpe": "not captured", "Sortino": "not captured", "holdings": "not captured", "factor_philosophy": "growth, momentum, earnings strength", "implication_for_portfolio_atlas": "Needs controlled comparison before use"},
    ]


def _small_cap_quality_review() -> list[dict[str, str]]:
    return [
        {"dimension": "quality", "observed_value": "central philosophy", "interpretation": "Quality may improve small-cap selection.", "atlas_implication": "Prioritize quality inputs once PIT fundamentals exist.", "copy_risk": "Do not copy ranking system blindly."},
        {"dimension": "growth", "observed_value": "central philosophy", "interpretation": "Growth appears useful when paired with quality.", "atlas_implication": "Test growth as part of combined stack.", "copy_risk": "Need factor isolation."},
        {"dimension": "reasonable valuation", "observed_value": "central philosophy", "interpretation": "Valuation may control overpaying for growth.", "atlas_implication": "Include valuation/PEG/PEGY once data is ready.", "copy_risk": "Formula details unknown."},
        {"dimension": "small-cap exposure", "observed_value": "meaningful exposure", "interpretation": "Size exposure may explain part of excess return.", "atlas_implication": "Compare against Russell 2000 and IWM.", "copy_risk": "Do not mistake size beta for alpha."},
        {"dimension": "drawdown", "observed_value": "not captured", "interpretation": "Risk profile unknown from manual notes.", "atlas_implication": "Capture max drawdown and recovery.", "copy_risk": "Return alone is insufficient."},
        {"dimension": "holdings", "observed_value": "not captured", "interpretation": "Concentration and liquidity unknown.", "atlas_implication": "Export holdings by rebalance date.", "copy_risk": "Cannot assess implementability."},
        {"dimension": "sector concentration", "observed_value": "not captured", "interpretation": "Sector tilt may drive results.", "atlas_implication": "Export sector/industry exposure.", "copy_risk": "May be hidden regime bet."},
        {"dimension": "benchmark excess", "observed_value": "11.2% annualized excess vs Russell 2000", "interpretation": "Strong external observation.", "atlas_implication": "Use as comparison target, not proof.", "copy_risk": "Needs exact settings and reproducibility."},
    ]


def _portfolio_atlas_v1_test_plan() -> list[dict[str, str]]:
    metrics = "CAGR,Sharpe,Sortino,max_drawdown,turnover,win_rate"
    return [
        {"component": "universe", "specification": "Broad US equity universe with liquidity and small-cap eligibility documented", "metrics_to_capture": metrics},
        {"component": "ranking system", "specification": "Portfolio_Atlas_v1 factors: quality, growth, valuation, momentum, income, risk, PEG, PEGY", "metrics_to_capture": metrics},
        {"component": "positions", "specification": "25, 50, 100, and 150 holdings", "metrics_to_capture": metrics},
        {"component": "rebalance", "specification": "Monthly rebalance", "metrics_to_capture": metrics},
        {"component": "buy/sell rules", "specification": "Buy top-ranked eligible names; sell when rank falls below threshold or eligibility fails", "metrics_to_capture": metrics},
        {"component": "benchmark", "specification": "S&P 500, Russell 2000, SPY, IWM, and Small Cap Quality", "metrics_to_capture": metrics},
    ]


def _import_validators() -> list[dict[str, str]]:
    validators = []
    for factor in FULL_MODEL_FACTORS:
        if factor in {"momentum", "risk"}:
            status = PARTIALLY_READY
            required_columns = "date,ticker,adjusted_close,volume"
            blocker = "Needs full universe and benchmark context"
        elif factor == "income":
            status = DATA_REQUIRED
            required_columns = "date,ticker,dividend,split_factor,adjusted_close"
            blocker = "Needs dividends/splits"
        else:
            status = DATA_REQUIRED
            required_columns = "ticker,as_of_date,filing_date,revenue,eps,fcf,roic,margins,valuation_fields"
            blocker = "Needs PIT fundamentals with filing_date"
        validators.append({"factor": factor, "status": status, "required_columns": required_columns, "blocker": blocker})
    return validators


def _full_model_backtest_skeleton() -> dict[str, Any]:
    return {
        "model_name": "Portfolio_Atlas_Full_v1",
        "run_condition": "Do not run unless full_model_readiness_decision.classification == FULL_MODEL_READY",
        "factors": ["Quality", "Growth", "Value", "Momentum", "Income", "Risk", "PEG", "PEGY"],
        "portfolio_sizes": [25, 50, 100, 150],
        "rebalance": "monthly",
        "benchmark_comparison": ["VTI", "S&P 500", "Russell 2000", "SPY", "IWM", "60/40 proxy", "risk-free"],
        "status": DATA_REQUIRED,
    }


def _readiness_decision(gap_report: list[dict[str, str]], validators: list[dict[str, str]]) -> dict[str, str]:
    missing = [row["requirement"] for row in gap_report if row["status"] == DATA_REQUIRED]
    blocked_factors = [row["factor"] for row in validators if row["status"] == DATA_REQUIRED]
    return {
        "classification": DATA_REQUIRED,
        "decision": DATA_REQUIRED,
        "reason": "Full-model data is missing: " + ", ".join(missing) + ". Blocked factors: " + ", ".join(blocked_factors) + ".",
        "recommended_next_action": "Use Portfolio123 as an external bridge for Portfolio_Atlas_v1 tests while acquiring Sharadar-style PIT fundamentals.",
        "authority": AUTHORITY,
    }


def _evidence_review(readiness: dict[str, str]) -> dict[str, str]:
    return {
        "classification": readiness["classification"],
        "decision": USE_PORTFOLIO123_BRIDGE,
        "reason": "Full model is not ready for native backtest; Portfolio123 can bridge external tests, and Sharadar-style data remains the native import target.",
        "recommended_next_action": "Run Portfolio_Atlas_v1 in Portfolio123 if plan access allows; compare against Small Cap Quality, SPY, IWM, S&P 500, and Russell 2000.",
        "authority": AUTHORITY,
    }


def _summary(report: dict[str, Any]) -> str:
    readiness = report["full_model_readiness_decision"]
    return (
        "# Full Model Data Transition\n\n"
        f"Status: {report['status']}\n\n"
        "Full model blockers: PIT fundamentals, filing_date, dividends/splits, sector/industry, delisted securities, broad universe, VTI/benchmark total returns, and risk-free rates.\n\n"
        "Portfolio123 evidence: external/manual bridge evidence only. Small Cap Quality was strongest observed, but it is not proof of Portfolio Atlas.\n\n"
        "Small Cap Quality implication: quality + growth + reasonable valuation plus small-cap exposure is promising enough to test, not copy blindly.\n\n"
        f"Full model readiness: {readiness['classification']}.\n\n"
        f"Next action: {readiness['recommended_next_action']}\n\n"
        f"Authority: {AUTHORITY}\n"
    )


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
