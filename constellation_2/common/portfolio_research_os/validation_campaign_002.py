from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

DEFAULT_REPORT_ROOT = Path("reports/portfolio_research_os")
AUTHORITY_BOUNDARY = (
    "Research-only. No replacement recommendation, no live portfolio, no trades, "
    "no broker execution, and no real-capital recommendation."
)

WINDOW_COLUMNS = [
    "window_id",
    "window_type",
    "train_start",
    "train_end",
    "validation_start",
    "validation_end",
    "test_start",
    "test_end",
    "rebalance_frequency",
    "embargo_days",
]

REGIMES = [
    "bull market",
    "bear market",
    "high inflation",
    "low inflation",
    "high volatility",
    "low volatility",
    "rising rates",
    "falling rates",
]

REQUIRED_RETIREMENT_INPUTS = [
    "starting_assets",
    "annual_spending",
    "inflation_assumption",
    "healthcare_costs",
    "social_security",
    "annuity_lump_sum",
    "inheritance_assumption",
    "tax_assumptions",
    "portfolio_return_distribution",
]

REPORT_SPECS = {
    "walk_forward_portfolio_validation": ("P031", "Walk-Forward Portfolio Validation"),
    "oos_holdout_validation": ("P032", "Out-of-Sample Holdout Validation"),
    "regime_performance_analysis": ("P033", "Regime Performance Analysis"),
    "tax_turnover_proxy": ("P034", "Turnover and Tax Drag Proxy"),
    "behavioral_stress_test": ("P035", "Drawdown and Behavioral Stress Test"),
    "retirement_simulation_inputs": ("P036", "Retirement Simulation Inputs"),
    "monte_carlo_retirement_engine": ("P037", "Monte Carlo Retirement Engine MVP"),
    "oak_harvest_replacement_standard": ("P038", "Oak Harvest Replacement Standard"),
    "shadow_portfolio_design": ("P039", "Shadow Portfolio Design"),
    "weekly_review_package": ("P040", "Weekly Review Package Generator"),
    "portfolio_explainability": ("P041", "Portfolio Explainability Report"),
    "complexity_benefit_review": ("P042", "Complexity vs Benefit Review"),
    "portfolio_atlas_kill_test": ("P043", "Portfolio Atlas Kill Test"),
    "validation_campaign_002": ("P044", "Validation Campaign 002"),
    "implementation_funding_decision": ("P045", "Implementation Funding Decision"),
}


def run_validation_campaign_002(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    created = created_at or _now()
    root_path = Path(root)
    reports = {
        "walk_forward": run_walk_forward_portfolio_validation(root_path, created_at=created),
        "holdout": run_oos_holdout_validation(root_path, created_at=created),
        "regime": run_regime_performance_analysis(root_path, created_at=created),
        "tax_turnover": run_tax_turnover_proxy(root_path, created_at=created),
        "behavioral_stress": run_behavioral_stress_test(root_path, created_at=created),
        "retirement_inputs": run_retirement_simulation_inputs(root_path, created_at=created),
        "monte_carlo": run_monte_carlo_retirement_engine(root_path, created_at=created),
        "replacement_standard": run_oak_harvest_replacement_standard(root_path, created_at=created),
        "shadow_design": run_shadow_portfolio_design(root_path, created_at=created),
        "weekly_review": run_weekly_review_package(root_path, created_at=created),
        "explainability": run_portfolio_explainability(root_path, created_at=created),
        "complexity_benefit": run_complexity_benefit_review(root_path, created_at=created),
        "kill_test": run_portfolio_atlas_kill_test(root_path, created_at=created),
    }
    campaign = run_validation_campaign_002_summary(root_path, reports=reports, created_at=created)
    funding = run_implementation_funding_decision(root_path, campaign=campaign, created_at=created)
    return {**reports, "validation_campaign_002": campaign, "funding_decision": funding}


def run_walk_forward_portfolio_validation(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    inputs = _input_status(root_path)
    windows = _walk_forward_windows()
    data_ready = _performance_inputs_ready(inputs)
    performance = [
        {
            "window_id": row["window_id"],
            "atlas_return": "DATA_REQUIRED" if not data_ready else "",
            "vti_return": "DATA_REQUIRED" if not data_ready else "",
            "oak_harvest_proxy_return": "DATA_REQUIRED" if not data_ready else "",
            "stability_status": "DATA_REQUIRED" if not data_ready else "PENDING_SCORE",
            "notes": "Source-backed portfolio and benchmark returns are required before scoring.",
        }
        for row in windows
    ]
    stability = [
        {"metric": "return_consistency", "classification": "DATA_REQUIRED" if not data_ready else "WALK_FORWARD_MIXED", "notes": "No performance claim without source-backed returns."},
        {"metric": "drawdown_consistency", "classification": "DATA_REQUIRED" if not data_ready else "WALK_FORWARD_MIXED", "notes": "Requires comparable drawdown series."},
    ]
    classification = "DATA_REQUIRED" if not data_ready else "WALK_FORWARD_MIXED"
    report = _base_report("walk_forward_portfolio_validation", created, classification, inputs)
    report.update({"windows": windows, "performance": performance, "stability_report": stability})
    _write_report(
        root_path,
        "walk_forward_portfolio_validation",
        report,
        [
            ("walk_forward_windows.csv", WINDOW_COLUMNS, windows),
            ("walk_forward_performance.csv", list(performance[0]), performance),
            ("stability_report.csv", list(stability[0]), stability),
        ],
    )
    return report


def run_oos_holdout_validation(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    inputs = _input_status(root_path)
    data_ready = _performance_inputs_ready(inputs)
    results = [
        {
            "holdout_id": "H001",
            "train_start": "2010-01-01",
            "train_end": "2016-12-31",
            "validation_start": "2017-01-01",
            "validation_end": "2020-12-31",
            "holdout_start": "2021-01-01",
            "holdout_end": "2024-12-31",
            "tuning_after_holdout": "FALSE",
            "classification": "DATA_REQUIRED" if not data_ready else "PENDING_EVALUATION",
            "notes": "Holdout is strict; assumptions are locked before holdout scoring.",
        }
    ]
    audit = [
        {"assumption": "factor_weights", "locked_before_holdout": "TRUE", "holdout_used_for_tuning": "FALSE", "notes": "Fixed MVP factor weights only."},
        {"assumption": "benchmark_stack", "locked_before_holdout": "TRUE", "holdout_used_for_tuning": "FALSE", "notes": "VTI, 60/40, simple factor, and Oak Harvest proxy."},
    ]
    report = _base_report("oos_holdout_validation", created, "DATA_REQUIRED" if not data_ready else "PENDING_EVALUATION", inputs)
    report.update({"holdout_results": results, "holdout_assumption_audit": audit})
    _write_report(root_path, "oos_holdout_validation", report, [("holdout_results.csv", list(results[0]), results), ("holdout_assumption_audit.csv", list(audit[0]), audit)])
    return report


def run_regime_performance_analysis(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    inputs = _input_status(root_path)
    regime_available = inputs["regime_labels"]["loaded"]
    results = [
        {"regime": regime, "atlas_return": "DATA_REQUIRED", "vti_return": "DATA_REQUIRED", "oak_harvest_proxy_return": "DATA_REQUIRED", "classification": "DATA_REQUIRED", "notes": "Regime labels and aligned return series required."}
        for regime in REGIMES
    ]
    gaps = [
        {"regime": regime, "gap": "REGIME_DATA_UNAVAILABLE" if not regime_available else "RETURN_SERIES_UNAVAILABLE", "blocked": "TRUE", "required_input": "regime_labels.csv plus aligned portfolio and benchmark returns"}
        for regime in REGIMES
    ]
    report = _base_report("regime_performance_analysis", created, "DATA_REQUIRED", inputs)
    report.update({"regime_results": results, "regime_gap_report": gaps})
    _write_report(root_path, "regime_performance_analysis", report, [("regime_results.csv", list(results[0]), results), ("regime_gap_report.csv", list(gaps[0]), gaps)])
    return report


def run_tax_turnover_proxy(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    inputs = _input_status(root_path)
    ready = inputs["holdings_history"]["loaded"] and inputs["dividend_adjustments"]["loaded"]
    rows = [
        {
            "portfolio": "Portfolio Atlas",
            "annual_turnover": "DATA_REQUIRED" if not ready else "",
            "short_term_gain_proxy": "DATA_REQUIRED" if not ready else "",
            "long_term_gain_proxy": "DATA_REQUIRED" if not ready else "",
            "dividend_income": "DATA_REQUIRED" if not ready else "",
            "tax_drag_proxy": "DATA_REQUIRED" if not ready else "",
            "after_tax_return_proxy": "DATA_REQUIRED" if not ready else "",
            "proxy_label": "RESEARCH_ESTIMATE_ONLY_NOT_TAX_ADVICE",
        }
    ]
    audit = [
        {"assumption": "tax_drag_proxy", "status": "PROXY_ONLY", "notes": "Research estimate only. Not tax advice."},
        {"assumption": "lot_level_tax_accounting", "status": "NOT_AVAILABLE", "notes": "Requires lot-level buys, sells, holding periods, dividends, and tax assumptions."},
    ]
    report = _base_report("tax_turnover_proxy", created, "DATA_REQUIRED" if not ready else "PROXY_READY", inputs)
    report.update({"turnover_tax_proxy": rows, "tax_assumption_audit": audit, "not_tax_advice": True})
    _write_report(root_path, "tax_turnover_proxy", report, [("turnover_tax_proxy.csv", list(rows[0]), rows), ("tax_assumption_audit.csv", list(audit[0]), audit)])
    return report


def run_behavioral_stress_test(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    inputs = _input_status(root_path)
    ready = _performance_inputs_ready(inputs)
    drawdown = [
        {
            "portfolio": "Portfolio Atlas",
            "max_drawdown": "DATA_REQUIRED",
            "time_to_recover": "DATA_REQUIRED",
            "worst_12_month_return": "DATA_REQUIRED",
            "worst_36_month_return": "DATA_REQUIRED",
            "classification": "DATA_REQUIRED" if not ready else "PENDING_EVALUATION",
        }
    ]
    under = [
        {"comparison": "VTI", "underperformance_window": "DATA_REQUIRED", "magnitude": "DATA_REQUIRED"},
        {"comparison": "Oak Harvest proxy", "underperformance_window": "DATA_REQUIRED", "magnitude": "DATA_REQUIRED"},
    ]
    risk = [{"risk": "behavioral_capitulation_risk", "score": "DATA_REQUIRED", "notes": "Requires drawdown and underperformance duration evidence."}]
    report = _base_report("behavioral_stress_test", created, "DATA_REQUIRED" if not ready else "PENDING_EVALUATION", inputs)
    report.update({"drawdown_report": drawdown, "underperformance_windows": under, "behavioral_risk_score": risk})
    _write_report(root_path, "behavioral_stress_test", report, [("drawdown_report.csv", list(drawdown[0]), drawdown), ("underperformance_windows.csv", list(under[0]), under), ("behavioral_risk_score.csv", list(risk[0]), risk)])
    return report


def run_retirement_simulation_inputs(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    supplied = _retirement_inputs(root_path)
    required = [{"input": item, "required": "TRUE", "hard_coded": "FALSE"} for item in REQUIRED_RETIREMENT_INPUTS]
    available = [{"input": key, "status": "AVAILABLE"} for key in sorted(supplied)]
    missing = [{"input": item, "status": "MISSING"} for item in REQUIRED_RETIREMENT_INPUTS if item not in supplied]
    classification = "READY" if not missing else "DATA_REQUIRED"
    report = _base_report("retirement_simulation_inputs", created, classification, _input_status(root_path))
    report.update({"required_inputs": required, "available_inputs": available, "missing_inputs": missing, "hard_coded_personal_assumptions": False})
    _write_report(root_path, "retirement_simulation_inputs", report, [("required_inputs.csv", list(required[0]), required), ("available_inputs.csv", ["input", "status"], available), ("missing_inputs.csv", ["input", "status"], missing)])
    return report


def run_monte_carlo_retirement_engine(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    supplied = _retirement_inputs(root_path)
    ready = all(item in supplied for item in REQUIRED_RETIREMENT_INPUTS)
    portfolios = ["Portfolio Atlas", "VTI", "60/40", "Oak Harvest proxy"]
    rows = [
        {
            "portfolio": portfolio,
            "success_probability": "DATA_REQUIRED" if not ready else "",
            "failure_probability": "DATA_REQUIRED" if not ready else "",
            "median_ending_wealth": "DATA_REQUIRED" if not ready else "",
            "worst_10_percent": "DATA_REQUIRED" if not ready else "",
            "worst_1_percent": "DATA_REQUIRED" if not ready else "",
            "classification": "BLOCKED_INPUTS_MISSING" if not ready else "PENDING_SIMULATION",
        }
        for portfolio in portfolios
    ]
    scenario = [{"scenario": "base", "classification": "BLOCKED_INPUTS_MISSING" if not ready else "PENDING_SIMULATION", "notes": "Monte Carlo runs only when all required retirement inputs exist."}]
    report = _base_report("monte_carlo_retirement_engine", created, "DATA_REQUIRED" if not ready else "PENDING_SIMULATION", _input_status(root_path))
    report.update({"simulation_results": rows, "scenario_results": scenario, "blocked": not ready, "missing_inputs": [item for item in REQUIRED_RETIREMENT_INPUTS if item not in supplied]})
    _write_report(root_path, "monte_carlo_retirement_engine", report, [("simulation_results.csv", list(rows[0]), rows), ("scenario_results.csv", list(scenario[0]), scenario)])
    return report


def run_oak_harvest_replacement_standard(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    criteria = [
        {"criterion": "multi_period_outperformance", "required": "TRUE", "status": "DATA_REQUIRED"},
        {"criterion": "after_tax_advantage", "required": "TRUE", "status": "DATA_REQUIRED"},
        {"criterion": "behavioral_drawdown_acceptability", "required": "TRUE", "status": "DATA_REQUIRED"},
        {"criterion": "explainability_and_operational_simplicity", "required": "TRUE", "status": "DATA_REQUIRED"},
    ]
    matrix = [{"evidence": row["criterion"], "classification": row["status"], "notes": "No replacement evidence established."} for row in criteria]
    decision = [{"outcome": "INSUFFICIENT_EVIDENCE", "replacement_forbidden": "TRUE", "rationale": "Replacement remains forbidden until strict evidence exists.", "authority_boundary": AUTHORITY_BOUNDARY}]
    report = _base_report("oak_harvest_replacement_standard", created, "INSUFFICIENT_EVIDENCE", _input_status(root_path))
    report.update({"replacement_criteria": criteria, "current_evidence_matrix": matrix, "decision": decision})
    _write_report(root_path, "oak_harvest_replacement_standard", report, [("replacement_criteria.csv", list(criteria[0]), criteria), ("current_evidence_matrix.csv", list(matrix[0]), matrix), ("decision.csv", list(decision[0]), decision)])
    return report


def run_shadow_portfolio_design(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    schema = [
        {"field": "as_of_date", "required": "TRUE", "description": "Observation date"},
        {"field": "ticker", "required": "TRUE", "description": "Shadow holding ticker"},
        {"field": "target_weight", "required": "TRUE", "description": "Research-only target weight"},
        {"field": "benchmark_weight", "required": "FALSE", "description": "Benchmark comparator weight"},
        {"field": "research_note", "required": "FALSE", "description": "Non-recommendation rationale"},
    ]
    success = [{"criterion": "12_month_shadow_tracking_complete", "threshold": "TRUE", "capital_required": "FALSE"}]
    report = _base_report("shadow_portfolio_design", created, "READY_FOR_SHADOW_TRACKING", _input_status(root_path))
    report.update({"shadow_tracking_schema": schema, "success_criteria": success})
    extra = {"weekly_report_template.md": _weekly_template(shadow=True)}
    _write_report(root_path, "shadow_portfolio_design", report, [("shadow_tracking_schema.csv", list(schema[0]), schema), ("success_criteria.csv", list(success[0]), success)], extra_files=extra)
    return report


def run_weekly_review_package(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    template = [
        {"section": "current portfolio", "research_only": "TRUE"},
        {"section": "target portfolio", "research_only": "TRUE"},
        {"section": "changes", "research_only": "TRUE"},
        {"section": "benchmark comparison", "research_only": "TRUE"},
        {"section": "risk report", "research_only": "TRUE"},
        {"section": "factor explanation", "research_only": "TRUE"},
    ]
    report = _base_report("weekly_review_package", created, "RESEARCH_PACKAGE_READY", _input_status(root_path))
    report.update({"sections": template, "recommendation_language_forbidden": True})
    extra = {"weekly_review_sample.md": _weekly_template(shadow=False)}
    _write_report(root_path, "weekly_review_package", report, [("current_vs_target_template.csv", list(template[0]), template)], extra_files=extra)
    return report


def run_portfolio_explainability(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    holdings = [{"holding": "DATA_REQUIRED", "factor_scores": "DATA_REQUIRED", "opportunity_rank": "DATA_REQUIRED", "weight_rationale": "DATA_REQUIRED", "risk_contribution": "DATA_REQUIRED", "benchmark_comparison": "DATA_REQUIRED"}]
    scores = [{"score": "factor_score", "explanation": "Requires point-in-time factor inputs and holdings."}]
    report = _base_report("portfolio_explainability", created, "DATA_REQUIRED", _input_status(root_path))
    report.update({"holding_explanations": holdings, "score_explanations": scores})
    _write_report(root_path, "portfolio_explainability", report, [("holding_explanations.csv", list(holdings[0]), holdings), ("score_explanations.csv", list(scores[0]), scores)])
    return report


def run_complexity_benefit_review(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    portfolios = ["VTI", "60/40", "simple factor portfolio", "Portfolio Atlas"]
    complexity = [{"portfolio": item, "complexity": "LOW" if item in {"VTI", "60/40"} else "MEDIUM" if item == "simple factor portfolio" else "HIGH", "understandability": "HIGH" if item != "Portfolio Atlas" else "DATA_REQUIRED"} for item in portfolios]
    benefit = [{"portfolio": item, "performance": "DATA_REQUIRED", "turnover": "DATA_REQUIRED", "tax_drag_proxy": "DATA_REQUIRED", "benefit_classification": "DATA_REQUIRED"} for item in portfolios]
    report = _base_report("complexity_benefit_review", created, "DATA_REQUIRED", _input_status(root_path))
    report.update({"complexity_scorecard": complexity, "benefit_scorecard": benefit})
    _write_report(root_path, "complexity_benefit_review", report, [("complexity_scorecard.csv", list(complexity[0]), complexity), ("benefit_scorecard.csv", list(benefit[0]), benefit)])
    return report


def run_portfolio_atlas_kill_test(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None, evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    evidence = evidence or {}
    failure_modes = [
        _failure("cannot beat VTI", evidence.get("atlas_beats_vti")),
        _failure("cannot beat Oak Harvest proxy", evidence.get("atlas_beats_oak_harvest_proxy")),
        _failure("tax drag erodes advantage", evidence.get("tax_drag_advantage_survives")),
        _failure("turnover too high", evidence.get("turnover_acceptable")),
        _failure("drawdown too severe", evidence.get("drawdown_acceptable")),
        _failure("complexity not justified", evidence.get("complexity_justified")),
        _failure("bias risk unresolved", evidence.get("bias_controlled")),
    ]
    rejected = any(row["classification"] == "FAILURE_CONFIRMED" for row in failure_modes)
    data_required = any(row["classification"] == "DATA_REQUIRED" for row in failure_modes)
    decision_value = "PORTFOLIO_ATLAS_REJECTED" if rejected else "DATA_REQUIRED" if data_required else "PORTFOLIO_ATLAS_SURVIVED_KILL_TEST"
    decision = [{"decision": decision_value, "real_capital_recommendation": "FORBIDDEN", "rationale": "Adversarial kill test result; not a live allocation decision.", "authority_boundary": AUTHORITY_BOUNDARY}]
    survival = [{"reason": "No confirmed failure mode" if not rejected else "None", "classification": "DATA_REQUIRED" if data_required else "AVAILABLE"}]
    report = _base_report("portfolio_atlas_kill_test", created, decision_value, _input_status(root_path))
    report.update({"failure_modes": failure_modes, "survival_reasons": survival, "kill_decision": decision})
    _write_report(root_path, "portfolio_atlas_kill_test", report, [("failure_modes.csv", list(failure_modes[0]), failure_modes), ("survival_reasons.csv", list(survival[0]), survival), ("kill_decision.csv", list(decision[0]), decision)])
    return report


def run_validation_campaign_002_summary(root: str | Path = DEFAULT_REPORT_ROOT, *, reports: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    reports = reports or {}
    scorecard = [
        {"build": spec[0], "report": key, "classification": reports.get(_summary_key(key), {}).get("classification", "DATA_REQUIRED")}
        for key, spec in REPORT_SPECS.items()
        if key not in {"validation_campaign_002", "implementation_funding_decision"}
    ]
    any_bias = any(row["classification"] == "BIAS_BLOCKED" for row in scorecard)
    any_data = any(row["classification"] in {"DATA_REQUIRED", "INSUFFICIENT_EVIDENCE"} for row in scorecard)
    decision_value = "BIAS_BLOCKED" if any_bias else "DATA_REQUIRED" if any_data else "PORTFOLIO_ATLAS_PROMISING"
    decision = [{"decision": decision_value, "question_answered": "Is Portfolio Atlas worth continuing?", "replacement_recommendation": "FORBIDDEN", "rationale": "Campaign remains research-only and evidence-gated."}]
    next_phase = [{"next_step": "IMPROVE_DATA_FIRST" if decision_value == "DATA_REQUIRED" else "CONTINUE_SHADOW_MODE", "authority_boundary": AUTHORITY_BOUNDARY}]
    report = _base_report("validation_campaign_002", created, decision_value, _input_status(root_path))
    report.update({"validation_scorecard": scorecard, "decision_matrix": decision, "next_phase": next_phase})
    _write_report(root_path, "validation_campaign_002", report, [("validation_scorecard.csv", list(scorecard[0]), scorecard), ("decision_matrix.csv", list(decision[0]), decision), ("next_phase.csv", list(next_phase[0]), next_phase)])
    return report


def run_implementation_funding_decision(root: str | Path = DEFAULT_REPORT_ROOT, *, campaign: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    campaign_decision = (campaign or {}).get("classification", "DATA_REQUIRED")
    funding = "IMPROVE_DATA_FIRST" if campaign_decision == "DATA_REQUIRED" else "CONTINUE_DEVELOPMENT"
    rows = [{"decision": funding, "real_capital_recommendation": "FORBIDDEN", "rationale": "Continue only as research infrastructure; no live capital decision."}]
    evidence = [{"evidence": "validation_campaign_002", "classification": campaign_decision}]
    steps = [{"required_next_step": "Supply source-backed returns, benchmark series, regime labels, holdings history, and retirement inputs.", "priority": "P0"}]
    report = _base_report("implementation_funding_decision", created, funding, _input_status(root_path))
    report.update({"funding_decision": rows, "evidence_summary": evidence, "required_next_steps": steps})
    _write_report(root_path, "implementation_funding_decision", report, [("funding_decision.csv", list(rows[0]), rows), ("evidence_summary.csv", list(evidence[0]), evidence), ("required_next_steps.csv", list(steps[0]), steps)])
    return report


def _walk_forward_windows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    configs = [("rolling", date(2010, 1, 1), 6, 36), ("expanding", date(2010, 1, 1), 6, None)]
    for window_type, start, count, rolling_months in configs:
        for index in range(count):
            train_start = start if window_type == "expanding" else _add_months(start, index * 12)
            train_end = _add_months(start, 72 + index * 12) if rolling_months is None else _add_months(train_start, rolling_months)
            validation_start = _add_days(train_end, 1)
            validation_end = _add_months(validation_start, 12)
            test_start = _add_days(validation_end, 31)
            test_end = _add_months(test_start, 12)
            rows.append(
                {
                    "window_id": f"{window_type[:1].upper()}WF{index + 1:03d}",
                    "window_type": window_type,
                    "train_start": train_start.isoformat(),
                    "train_end": train_end.isoformat(),
                    "validation_start": validation_start.isoformat(),
                    "validation_end": validation_end.isoformat(),
                    "test_start": test_start.isoformat(),
                    "test_end": test_end.isoformat(),
                    "rebalance_frequency": "quarterly",
                    "embargo_days": 30,
                }
            )
    return rows


def _base_report(name: str, created_at: str, classification: str, inputs: dict[str, Any]) -> dict[str, Any]:
    build, title = REPORT_SPECS[name]
    return {
        "schema_id": f"portfolio_research_os_{name}",
        "schema_version": "1.0",
        "build": build,
        "report_title": title,
        "created_at": created_at,
        "day": created_at[:10],
        "classification": classification,
        "input_status": inputs,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _write_report(root: Path, dirname: str, report: dict[str, Any], csvs: list[tuple[str, list[str], list[dict[str, Any]]]], *, extra_files: dict[str, str] | None = None) -> None:
    out_dir = root / dirname
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "latest.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out_dir / "latest_summary.md").write_text(_summary(report), encoding="utf-8")
    for filename, columns, rows in csvs:
        _write_csv(out_dir / filename, columns, rows)
    for filename, text in (extra_files or {}).items():
        (out_dir / filename).write_text(text, encoding="utf-8")


def _summary(report: dict[str, Any]) -> str:
    return (
        f"# {report['build']} - {report['report_title']}\n\n"
        f"Classification: {report['classification']}\n\n"
        "Authority: Research-only. No replacement recommendation, no live portfolio, no trades, no broker execution.\n"
    )


def _input_status(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "portfolio_returns": root / "inputs" / "portfolio_returns.csv",
        "benchmark_returns": root / "inputs" / "benchmark_returns.csv",
        "holdings_history": root / "inputs" / "holdings_history.csv",
        "point_in_time_fundamentals": root / "inputs" / "point_in_time_fundamentals.csv",
        "delisted_security_master": root / "inputs" / "delisted_security_master.csv",
        "corporate_actions": root / "inputs" / "corporate_actions.csv",
        "dividend_adjustments": root / "inputs" / "dividend_adjustments.csv",
        "regime_labels": root / "inputs" / "regime_labels.csv",
    }
    return {name: {"path": str(path), "exists": path.exists(), "loaded": path.exists()} for name, path in paths.items()}


def _retirement_inputs(root: Path) -> dict[str, str]:
    path = root / "inputs" / "retirement_inputs.csv"
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    supplied: dict[str, str] = {}
    for row in rows:
        key = (row.get("input") or row.get("name") or "").strip()
        value = (row.get("value") or "").strip()
        if key and value:
            supplied[key] = value
    return supplied


def _performance_inputs_ready(inputs: dict[str, dict[str, Any]]) -> bool:
    return inputs["portfolio_returns"]["loaded"] and inputs["benchmark_returns"]["loaded"]


def _failure(name: str, passed: Any) -> dict[str, str]:
    if passed is None:
        classification = "DATA_REQUIRED"
    elif bool(passed):
        classification = "PASSED"
    else:
        classification = "FAILURE_CONFIRMED"
    return {"failure_mode": name, "classification": classification, "notes": "Adversarial test; rejection is research-only and does not imply live action."}


def _summary_key(dirname: str) -> str:
    mapping = {
        "walk_forward_portfolio_validation": "walk_forward",
        "oos_holdout_validation": "holdout",
        "regime_performance_analysis": "regime",
        "tax_turnover_proxy": "tax_turnover",
        "behavioral_stress_test": "behavioral_stress",
        "retirement_simulation_inputs": "retirement_inputs",
        "monte_carlo_retirement_engine": "monte_carlo",
        "oak_harvest_replacement_standard": "replacement_standard",
        "shadow_portfolio_design": "shadow_design",
        "weekly_review_package": "weekly_review",
        "portfolio_explainability": "explainability",
        "complexity_benefit_review": "complexity_benefit",
        "portfolio_atlas_kill_test": "kill_test",
    }
    return mapping.get(dirname, dirname)


def _weekly_template(*, shadow: bool) -> str:
    label = "Shadow Portfolio Weekly Report" if shadow else "Portfolio Atlas Weekly Research Package"
    return f"""# {label}

## Current Portfolio

Research-only current portfolio snapshot.

## Target Portfolio

Research-only target portfolio snapshot. This is not a recommendation.

## Changes

Research-only change log. No trade instruction.

## Benchmark Comparison

VTI, 60/40, simple factor portfolio, and Oak Harvest proxy comparison.

## Risk Report

Drawdown, volatility, turnover, and tax-drag proxy checks.

## Factor Explanation

Factor scores, opportunity rank, weight rationale, and benchmark comparison.

Authority: Research-only. No replacement recommendation, no live portfolio, no trades, no broker execution.
"""


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, min(value.day, 28))


def _add_days(value: date, days: int) -> date:
    return date.fromordinal(value.toordinal() + days)


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
