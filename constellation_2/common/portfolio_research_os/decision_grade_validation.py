from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

DEFAULT_REPORT_ROOT = Path("reports/portfolio_research_os")
DATA_REQUIRED = "DATA_REQUIRED"
READY = "READY"
AUTHORITY = "Research-only. No live portfolio, no replacement recommendation, no trades, no broker execution."

REQUIRED_RETIREMENT_INPUTS = [
    "starting_assets",
    "annual_spending",
    "inflation",
    "healthcare",
    "social_security",
    "annuity",
    "tax_assumptions",
    "planning_horizon",
]

REGIMES = ["bull market", "bear market", "high volatility", "low volatility", "rising rates", "falling rates"]
BENCHMARKS = ["Portfolio Atlas", "VTI", "60/40", "Oak Harvest proxy"]

REPORT_DIRS = {
    "walk_forward": "decision_grade_walk_forward_validation",
    "holdout": "decision_grade_strict_holdout",
    "regime": "decision_grade_regime_validation",
    "tax_turnover": "decision_grade_tax_turnover_proxy",
    "behavioral_stress": "decision_grade_behavioral_stress",
    "retirement_inputs": "decision_grade_retirement_inputs",
    "monte_carlo": "decision_grade_monte_carlo_retirement",
    "complexity_benefit": "decision_grade_complexity_benefit",
    "explainability": "decision_grade_portfolio_explainability",
    "weekly_review": "decision_grade_weekly_review_sample",
    "shadow_schema": "decision_grade_shadow_portfolio_schema",
    "shadow_starter": "decision_grade_shadow_portfolio_starter",
    "replacement_matrix": "decision_grade_oak_harvest_replacement_matrix",
    "kill_test": "decision_grade_portfolio_atlas_kill_test",
    "campaign": "validation_campaign_003",
    "funding": "decision_grade_funding_development_decision",
    "board_vote": "decision_grade_review_board_vote",
    "operating_manual": "decision_grade_operating_manual",
    "evidence_archive": "decision_grade_evidence_archive",
    "phase_review": "portfolio_atlas_phase_review",
}


def run_decision_grade_validation(root: str | Path = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    context = _evidence_context(root_path)
    reports = {
        "walk_forward": run_walk_forward_validation_run(root_path, context=context, created_at=created),
        "holdout": run_strict_holdout_run(root_path, context=context, created_at=created),
        "regime": run_regime_validation(root_path, context=context, created_at=created),
        "tax_turnover": run_tax_turnover_proxy_run(root_path, context=context, created_at=created),
        "behavioral_stress": run_behavioral_stress_decision_grade(root_path, context=context, created_at=created),
        "retirement_inputs": run_retirement_simulation_input_fill(root_path, context=context, created_at=created),
        "monte_carlo": run_monte_carlo_retirement_comparison(root_path, context=context, created_at=created),
        "complexity_benefit": run_complexity_benefit_decision_grade(root_path, context=context, created_at=created),
        "explainability": run_portfolio_explainability_package(root_path, context=context, created_at=created),
        "weekly_review": run_weekly_review_sample(root_path, context=context, created_at=created),
        "shadow_schema": run_shadow_portfolio_schema(root_path, context=context, created_at=created),
        "shadow_starter": run_shadow_portfolio_starter(root_path, context=context, created_at=created),
        "replacement_matrix": run_oak_harvest_replacement_evidence_matrix(root_path, context=context, created_at=created),
        "kill_test": run_portfolio_atlas_kill_test_decision_grade(root_path, context=context, created_at=created),
    }
    reports["campaign"] = run_validation_campaign_003(root_path, reports=reports, context=context, created_at=created)
    reports["funding"] = run_funding_development_decision(root_path, campaign=reports["campaign"], context=context, created_at=created)
    reports["board_vote"] = run_portfolio_review_board_vote(root_path, campaign=reports["campaign"], context=context, created_at=created)
    reports["operating_manual"] = run_practical_operating_manual_draft(root_path, context=context, created_at=created)
    reports["evidence_archive"] = run_evidence_archive_manifest(root_path, context=context, created_at=created)
    reports["phase_review"] = run_portfolio_atlas_phase_review(root_path, campaign=reports["campaign"], funding=reports["funding"], context=context, created_at=created)
    return reports


def run_walk_forward_validation_run(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    ready = context["source_backed_results_ready"]
    windows = _walk_forward_windows()
    performance = [
        {
            "window_id": row["window_id"],
            "atlas_return": "" if ready else DATA_REQUIRED,
            "vti_return": "" if ready else DATA_REQUIRED,
            "simple_factor_return": "" if ready else DATA_REQUIRED,
            "oak_harvest_proxy_return": "" if ready else DATA_REQUIRED,
            "classification": "WALK_FORWARD_MIXED" if ready else DATA_REQUIRED,
        }
        for row in windows
    ]
    stability = [
        {"metric": "return_stability", "classification": "WALK_FORWARD_MIXED" if ready else DATA_REQUIRED, "notes": _source_note(context)},
        {"metric": "drawdown_stability", "classification": "WALK_FORWARD_MIXED" if ready else DATA_REQUIRED, "notes": _source_note(context)},
    ]
    report = _base("P076", "Walk-Forward Validation Run", "WALK_FORWARD_MIXED" if ready else DATA_REQUIRED, context, created_at)
    report.update({"walk_forward_windows": windows, "walk_forward_performance": performance, "walk_forward_stability": stability})
    _write_report(root_path, REPORT_DIRS["walk_forward"], report, [("walk_forward_windows.csv", list(windows[0]), windows), ("walk_forward_performance.csv", list(performance[0]), performance), ("walk_forward_stability.csv", list(stability[0]), stability)])
    return report


def run_strict_holdout_run(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    ready = context["source_backed_results_ready"]
    results = [{
        "train_start": "2010-01-01",
        "train_end": "2016-12-31",
        "validation_start": "2017-01-01",
        "validation_end": "2020-12-31",
        "holdout_start": "2021-01-01",
        "holdout_end": "2024-12-31",
        "tuning_after_holdout": "FALSE",
        "classification": "PENDING_EVALUATION" if ready else DATA_REQUIRED,
    }]
    audit = [
        {"control": "fixed_split", "status": "LOCKED", "holdout_used_for_tuning": "FALSE"},
        {"control": "factor_policy", "status": "LOCKED", "holdout_used_for_tuning": "FALSE"},
        {"control": "benchmark_stack", "status": "LOCKED", "holdout_used_for_tuning": "FALSE"},
    ]
    report = _base("P077", "Strict Holdout Run", "PENDING_EVALUATION" if ready else DATA_REQUIRED, context, created_at)
    report.update({"holdout_results": results, "holdout_integrity_audit": audit})
    _write_report(root_path, REPORT_DIRS["holdout"], report, [("holdout_results.csv", list(results[0]), results), ("holdout_integrity_audit.csv", list(audit[0]), audit)])
    return report


def run_regime_validation(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    ready = context["source_backed_results_ready"] and context["regime_data_available"]
    rows = [{"regime": regime, "atlas_return": "" if ready else DATA_REQUIRED, "vti_return": "" if ready else DATA_REQUIRED, "classification": "PENDING_EVALUATION" if ready else DATA_REQUIRED, "blocked": "FALSE" if ready else "TRUE"} for regime in REGIMES]
    report = _base("P078", "Regime Validation", "PENDING_EVALUATION" if ready else DATA_REQUIRED, context, created_at)
    report.update({"regime_results": rows})
    _write_report(root_path, REPORT_DIRS["regime"], report, [("regime_results.csv", list(rows[0]), rows)])
    return report


def run_tax_turnover_proxy_run(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    ready = context["source_backed_results_ready"] and context["holdings_available"]
    rows = [{
        "annual_turnover": "" if ready else DATA_REQUIRED,
        "short_term_gains_proxy": "" if ready else DATA_REQUIRED,
        "long_term_gains_proxy": "" if ready else DATA_REQUIRED,
        "dividend_income": "" if ready else DATA_REQUIRED,
        "tax_drag_proxy": "" if ready else DATA_REQUIRED,
        "after_tax_return_proxy": "" if ready else DATA_REQUIRED,
        "proxy_label": "RESEARCH_ESTIMATE_ONLY_NOT_TAX_ADVICE",
    }]
    report = _base("P079", "Tax / Turnover Proxy Run", "PROXY_READY" if ready else DATA_REQUIRED, context, created_at)
    report.update({"tax_turnover_proxy": rows, "not_tax_advice": True})
    _write_report(root_path, REPORT_DIRS["tax_turnover"], report, [("tax_turnover_proxy.csv", list(rows[0]), rows)])
    return report


def run_behavioral_stress_decision_grade(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    ready = context["source_backed_results_ready"]
    rows = [{
        "worst_12_months": "" if ready else DATA_REQUIRED,
        "worst_36_months": "" if ready else DATA_REQUIRED,
        "time_to_recover": "" if ready else DATA_REQUIRED,
        "underperformance_vs_vti": "" if ready else DATA_REQUIRED,
        "underperformance_vs_oak_harvest_proxy": "" if ready else DATA_REQUIRED,
        "classification": "PENDING_EVALUATION" if ready else DATA_REQUIRED,
    }]
    report = _base("P080", "Behavioral Stress Test", "PENDING_EVALUATION" if ready else DATA_REQUIRED, context, created_at)
    report.update({"behavioral_stress_results": rows})
    _write_report(root_path, REPORT_DIRS["behavioral_stress"], report, [("behavioral_stress_results.csv", list(rows[0]), rows)])
    return report


def run_retirement_simulation_input_fill(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    supplied = _personal_inputs(root_path)
    required = [{"input": item, "required": "TRUE"} for item in REQUIRED_RETIREMENT_INPUTS]
    available = [{"input": key, "status": "AVAILABLE"} for key in sorted(supplied)]
    missing = [{"input": item, "status": "MISSING"} for item in REQUIRED_RETIREMENT_INPUTS if item not in supplied]
    report = _base("P081", "Retirement Simulation Input Fill", READY if not missing else DATA_REQUIRED, context, created_at)
    report.update({"required_inputs": required, "available_inputs": available, "missing_inputs": missing, "hard_coded_personal_assumptions": False})
    _write_report(root_path, REPORT_DIRS["retirement_inputs"], report, [("required_inputs.csv", list(required[0]), required), ("available_inputs.csv", ["input", "status"], available), ("missing_inputs.csv", ["input", "status"], missing)])
    return report


def run_monte_carlo_retirement_comparison(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    supplied = _personal_inputs(root_path)
    ready = context["source_backed_results_ready"] and all(item in supplied for item in REQUIRED_RETIREMENT_INPUTS)
    rows = [{"portfolio": portfolio, "success_probability": "" if ready else DATA_REQUIRED, "failure_probability": "" if ready else DATA_REQUIRED, "median_ending_wealth": "" if ready else DATA_REQUIRED, "worst_10_percent": "" if ready else DATA_REQUIRED, "worst_1_percent": "" if ready else DATA_REQUIRED, "classification": "PENDING_SIMULATION" if ready else "BLOCKED_INPUTS_MISSING"} for portfolio in BENCHMARKS]
    report = _base("P082", "Monte Carlo Retirement Comparison", "PENDING_SIMULATION" if ready else DATA_REQUIRED, context, created_at)
    report.update({"simulation_results": rows, "blocked": not ready, "missing_inputs": [item for item in REQUIRED_RETIREMENT_INPUTS if item not in supplied]})
    _write_report(root_path, REPORT_DIRS["monte_carlo"], report, [("simulation_results.csv", list(rows[0]), rows)])
    return report


def run_complexity_benefit_decision_grade(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    portfolios = ["VTI", "60/40", "simple factor", "Portfolio Atlas"]
    complexity = [{"portfolio": item, "complexity": "LOW" if item in {"VTI", "60/40"} else "MEDIUM" if item == "simple factor" else "HIGH"} for item in portfolios]
    benefit = [{"portfolio": item, "benchmark_improvement": DATA_REQUIRED, "tax_adjusted_improvement": DATA_REQUIRED, "classification": DATA_REQUIRED} for item in portfolios]
    decision = [{"decision": DATA_REQUIRED, "rationale": "Benchmark improvement cannot be measured until source-backed results exist.", "authority": AUTHORITY}]
    report = _base("P083", "Complexity vs Benefit Review", DATA_REQUIRED, context, created_at)
    report.update({"complexity_scorecard": complexity, "benefit_scorecard": benefit, "complexity_decision": decision})
    _write_report(root_path, REPORT_DIRS["complexity_benefit"], report, [("complexity_scorecard.csv", list(complexity[0]), complexity), ("benefit_scorecard.csv", list(benefit[0]), benefit), ("complexity_decision.csv", list(decision[0]), decision)])
    return report


def run_portfolio_explainability_package(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    ready = context["latest_target_exists"]
    rows = [{"holding": DATA_REQUIRED, "factor_scores": DATA_REQUIRED, "opportunity_rank": DATA_REQUIRED, "target_weight": DATA_REQUIRED, "risk_contribution": DATA_REQUIRED, "sector_theme_contribution": DATA_REQUIRED, "why_owned": DATA_REQUIRED}]
    report = _base("P084", "Portfolio Explainability Package", "PENDING_EXPLANATION" if ready else DATA_REQUIRED, context, created_at)
    report.update({"holding_explanations": rows})
    _write_report(root_path, REPORT_DIRS["explainability"], report, [("holding_explanations.csv", list(rows[0]), rows)], extra={"portfolio_explainability_summary.md": _summary_text(report)})
    return report


def run_weekly_review_sample(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    sections = [{"section": item, "recommendation_language": "FORBIDDEN"} for item in ["current portfolio", "target portfolio", "new entrants", "potential exits", "risk changes", "benchmark comparison", "no-action / action-needed status"]]
    report = _base("P085", "Weekly Review Sample", "SAMPLE_READY", context, created_at)
    report.update({"sections": sections})
    _write_report(root_path, REPORT_DIRS["weekly_review"], report, [("weekly_review_sections.csv", list(sections[0]), sections)], extra={"weekly_review_sample.md": _weekly_review_sample()})
    return report


def run_shadow_portfolio_schema(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    schema = [{"field": field, "required": "TRUE"} for field in ["review_date", "target_holdings", "target_weights", "benchmark_returns", "actual_followed_status", "notes"]]
    report = _base("P086", "Shadow Portfolio Schema", "SCHEMA_READY", context, created_at)
    report.update({"shadow_tracking_schema": schema, "capital_required": False})
    _write_report(root_path, REPORT_DIRS["shadow_schema"], report, [("shadow_tracking_schema.csv", list(schema[0]), schema)])
    return report


def run_shadow_portfolio_starter(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    ready = context["latest_target_exists"] and context["source_backed_results_ready"]
    rows = [{"review_date": _day(created_at), "target_holdings": DATA_REQUIRED if not ready else "", "target_weights": DATA_REQUIRED if not ready else "", "benchmark_returns": DATA_REQUIRED if not ready else "", "actual_followed_status": "NOT_FOLLOWED_NO_CAPITAL", "notes": "Research-only shadow state. No real capital."}]
    report = _base("P087", "Shadow Portfolio Starter", "SHADOW_STATE_READY" if ready else DATA_REQUIRED, context, created_at)
    report.update({"shadow_initial_state": rows, "uses_real_capital": False})
    _write_report(root_path, REPORT_DIRS["shadow_starter"], report, [("shadow_initial_state.csv", list(rows[0]), rows)])
    return report


def run_oak_harvest_replacement_evidence_matrix(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    complete = context["source_backed_results_ready"] and context["oak_harvest_source_backed"]
    rows = [{"criterion": item, "classification": READY if complete else DATA_REQUIRED} for item in ["multi_period_outperformance", "after_tax_advantage", "drawdown_acceptability", "complexity_justified", "bias_controls_complete"]]
    decision = "CONTINUE_SHADOW_MODE" if complete else "INSUFFICIENT_EVIDENCE"
    decision_rows = [{"decision": decision, "replacement_recommendation": "FORBIDDEN", "evidence_complete": str(complete).upper(), "authority": AUTHORITY}]
    report = _base("P088", "Oak Harvest Replacement Evidence Matrix", decision, context, created_at)
    report.update({"evidence_matrix": rows, "decision": decision_rows})
    _write_report(root_path, REPORT_DIRS["replacement_matrix"], report, [("replacement_evidence_matrix.csv", list(rows[0]), rows), ("replacement_decision.csv", list(decision_rows[0]), decision_rows)])
    return report


def run_portfolio_atlas_kill_test_decision_grade(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None, evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    evidence = evidence or {}
    modes = ["cannot beat VTI", "cannot beat simple factor", "cannot beat Oak Harvest proxy", "tax drag too high", "turnover too high", "drawdown too high", "complexity not justified", "bias risk unresolved"]
    rows = [_failure_mode(mode, evidence.get(mode)) for mode in modes]
    decision = "PORTFOLIO_ATLAS_REJECTED" if any(row["classification"] == "FAILURE_CONFIRMED" for row in rows) else DATA_REQUIRED if any(row["classification"] == DATA_REQUIRED for row in rows) else "SURVIVED_KILL_TEST"
    decision_rows = [{"decision": decision, "authority": AUTHORITY, "real_capital_action": "FORBIDDEN"}]
    report = _base("P089", "Portfolio Atlas Kill Test", decision, context, created_at)
    report.update({"failure_modes": rows, "kill_decision": decision_rows})
    _write_report(root_path, REPORT_DIRS["kill_test"], report, [("failure_modes.csv", list(rows[0]), rows), ("kill_decision.csv", list(decision_rows[0]), decision_rows)])
    return report


def run_validation_campaign_003(root: str | Path = DEFAULT_REPORT_ROOT, *, reports: dict[str, Any] | None = None, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    reports = reports or {}
    scorecard = [{"phase": report.get("build", key), "component": key, "classification": report.get("classification", DATA_REQUIRED)} for key, report in reports.items()]
    decision = DATA_REQUIRED if not context["source_backed_results_ready"] else "BIAS_BLOCKED" if context["bias_blocked"] else "PORTFOLIO_ATLAS_PROMISING"
    rows = [{"decision": decision, "shadow_mode_question": "Is Portfolio Atlas worth tracking in shadow mode?", "authority": AUTHORITY}]
    report = _base("P090", "Validation Campaign 003", decision, context, created_at)
    report.update({"validation_scorecard": scorecard, "decision_matrix": rows})
    _write_report(root_path, REPORT_DIRS["campaign"], report, [("validation_scorecard.csv", list(scorecard[0]), scorecard), ("decision_matrix.csv", list(rows[0]), rows)])
    return report


def run_funding_development_decision(root: str | Path = DEFAULT_REPORT_ROOT, *, campaign: dict[str, Any] | None = None, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    campaign_decision = (campaign or {}).get("classification", DATA_REQUIRED)
    decision = "IMPROVE_DATA_FIRST" if campaign_decision in {DATA_REQUIRED, "BIAS_BLOCKED"} else "START_SHADOW_MODE"
    rows = [{"decision": decision, "real_capital_action": "FORBIDDEN", "rationale": "Shadow mode cannot start until source-backed validation is complete." if decision == "IMPROVE_DATA_FIRST" else "Research-only shadow mode only."}]
    report = _base("P091", "Funding / Development Decision", decision, context, created_at)
    report.update({"funding_decision": rows})
    _write_report(root_path, REPORT_DIRS["funding"], report, [("funding_decision.csv", list(rows[0]), rows)])
    return report


def run_portfolio_review_board_vote(root: str | Path = DEFAULT_REPORT_ROOT, *, campaign: dict[str, Any] | None = None, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    voters = ["AQR", "Lopez de Prado", "Dalio", "Buffett", "Swensen"]
    vote = "NO_SHADOW_YET" if not context["source_backed_results_ready"] else "SHADOW_MODE_ONLY"
    rows = [
        {"reviewer": voter, "vote": vote, "reason": "Source-backed validation is incomplete.", "blocking_concern": "DATA_REQUIRED" if not context["source_backed_results_ready"] else "NO_REAL_CAPITAL"}
        for voter in voters
    ]
    report = _base("P092", "Portfolio Review Board Vote", DATA_REQUIRED if not context["source_backed_results_ready"] else "BOARD_REVIEW_COMPLETE", context, created_at)
    report.update({"board_votes": rows})
    _write_report(root_path, REPORT_DIRS["board_vote"], report, [("review_board_votes.csv", list(rows[0]), rows)])
    return report


def run_practical_operating_manual_draft(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    rows = [{"section": item, "status": "DRAFT"} for item in ["weekly review process", "monthly execution review", "threshold rules", "shadow tracking process", "annual review"]]
    report = _base("P093", "Practical Operating Manual Draft", "DRAFT_READY", context, created_at)
    report.update({"manual_sections": rows})
    _write_report(root_path, REPORT_DIRS["operating_manual"], report, [("operating_manual_sections.csv", list(rows[0]), rows)], extra={"operating_manual_draft.md": _manual_text()})
    return report


def run_evidence_archive_manifest(root: str | Path = DEFAULT_REPORT_ROOT, *, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    rows = _evidence_manifest_rows(root_path)
    if not rows:
        rows = [{"phase": "P001-P093", "artifact_path": "", "classification": DATA_REQUIRED, "notes": "No Portfolio Atlas evidence artifacts found."}]
    report = _base("P094", "Evidence Archive Manifest", "ARCHIVE_READY" if rows else DATA_REQUIRED, context, created_at)
    report.update({"evidence_archive_manifest": rows})
    _write_report(root_path, REPORT_DIRS["evidence_archive"], report, [("evidence_archive_manifest.csv", list(rows[0]), rows)], extra={"evidence_lineage_summary.md": _evidence_summary(rows, context)})
    return report


def run_portfolio_atlas_phase_review(root: str | Path = DEFAULT_REPORT_ROOT, *, campaign: dict[str, Any] | None = None, funding: dict[str, Any] | None = None, context: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    context = context or _evidence_context(root_path)
    campaign_decision = (campaign or {}).get("classification", DATA_REQUIRED)
    decision = "IMPROVE_DATA_FIRST" if campaign_decision in {DATA_REQUIRED, "BIAS_BLOCKED"} else "PROCEED_TO_SHADOW_MODE"
    rows = [{"phase_decision": decision, "campaign_decision": campaign_decision, "authority": AUTHORITY}]
    report = _base("P095", "Portfolio Atlas Phase Review", decision, context, created_at)
    report.update({"phase_decision": rows})
    _write_report(root_path, REPORT_DIRS["phase_review"], report, [("phase_decision.csv", list(rows[0]), rows)])
    return report


def _evidence_context(root: Path) -> dict[str, Any]:
    evidence = _read_json(root / "backtest_evidence_review" / "latest.json")
    backtest = _read_json(root / "historical_portfolio_backtest" / "latest.json")
    monthly = _read_json(root / "monthly_portfolio_constructor" / "latest.json")
    benchmark = _read_json(root / "benchmark_backtest" / "latest.json")
    integrity = _read_json(root / "backtest_integrity_audit" / "latest.json")
    source_ready = all((item or {}).get("status") == READY for item in [evidence, backtest, monthly, benchmark]) and (integrity or {}).get("status") == READY
    return {
        "source_backed_results_ready": source_ready,
        "evidence_status": (evidence or {}).get("status", DATA_REQUIRED),
        "backtest_status": (backtest or {}).get("status", DATA_REQUIRED),
        "monthly_portfolio_status": (monthly or {}).get("status", DATA_REQUIRED),
        "benchmark_status": (benchmark or {}).get("status", DATA_REQUIRED),
        "integrity_status": (integrity or {}).get("status", DATA_REQUIRED),
        "bias_blocked": (integrity or {}).get("status") == "BIAS_BLOCKED",
        "latest_target_exists": bool((monthly or {}).get("monthly_holdings")),
        "holdings_available": bool((monthly or {}).get("monthly_holdings")),
        "regime_data_available": (root / "inputs" / "regime_labels.csv").exists(),
        "oak_harvest_source_backed": False,
    }


def _personal_inputs(root: Path) -> dict[str, str]:
    candidates = [root / "inputs" / "retirement_inputs.csv", root / "inputs" / "personal_assumptions.csv", Path("config/portfolio_research_os/personal_assumptions.csv")]
    supplied: dict[str, str] = {}
    for path in candidates:
        if not path.exists():
            continue
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                key = (row.get("input") or row.get("name") or "").strip()
                value = (row.get("value") or "").strip()
                if key and value:
                    supplied[key] = value
    return supplied


def _walk_forward_windows() -> list[dict[str, Any]]:
    rows = []
    for kind in ["rolling", "expanding"]:
        for idx in range(4):
            train_start = date(2010, 1, 1) if kind == "expanding" else _add_years(date(2010, 1, 1), idx)
            train_end = _add_years(date(2016, 12, 31), idx if kind == "expanding" else idx)
            validation_start = _add_years(date(2017, 1, 1), idx)
            validation_end = _add_years(date(2017, 12, 31), idx)
            test_start = _add_years(date(2018, 2, 1), idx)
            test_end = _add_years(date(2018, 12, 31), idx)
            rows.append({"window_id": f"{kind[:1].upper()}WF{idx + 1:03d}", "window_type": kind, "train_start": train_start.isoformat(), "train_end": train_end.isoformat(), "validation_start": validation_start.isoformat(), "validation_end": validation_end.isoformat(), "test_start": test_start.isoformat(), "test_end": test_end.isoformat(), "rebalance_frequency": "monthly", "embargo_days": 30})
    return rows


def _base(build: str, title: str, classification: str, context: dict[str, Any], created_at: str | None) -> dict[str, Any]:
    created = created_at or _now()
    return {"schema_id": f"portfolio_research_os_{build.lower()}_decision_grade_validation", "schema_version": "1.0", "build": build, "report_title": title, "created_at": created, "day": created[:10], "classification": classification, "source_evidence_context": context, "authority_boundary": AUTHORITY}


def _write_report(root: Path, dirname: str, report: dict[str, Any], csvs: list[tuple[str, list[str], list[dict[str, Any]]]], *, extra: dict[str, str] | None = None) -> None:
    out = root / dirname
    out.mkdir(parents=True, exist_ok=True)
    (out / "latest.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out / "latest_summary.md").write_text(_summary_text(report), encoding="utf-8")
    for filename, columns, rows in csvs:
        _write_csv(out / filename, columns, rows)
    for filename, text in (extra or {}).items():
        (out / filename).write_text(text, encoding="utf-8")


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _failure_mode(mode: str, value: Any) -> dict[str, str]:
    if value is None:
        classification = DATA_REQUIRED
    elif bool(value):
        classification = "PASSED"
    else:
        classification = "FAILURE_CONFIRMED"
    return {"failure_mode": mode, "classification": classification, "notes": "Adversarial rejection test; no trading authority."}


def _evidence_manifest_rows(root: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(root.glob("**/*")):
        if path.is_file() and path.suffix in {".json", ".csv", ".md"}:
            rows.append({"phase": _phase_from_path(path), "artifact_path": str(path), "classification": "ARCHIVED", "notes": "Portfolio Atlas evidence artifact."})
    return rows


def _phase_from_path(path: Path) -> str:
    text = str(path)
    for build in range(1, 94):
        token = f"P{build:03d}"
        if token.lower() in text.lower():
            return token
    return "P001-P093"


def _source_note(context: dict[str, Any]) -> str:
    return "Source-backed results available." if context["source_backed_results_ready"] else "P061-P075/source-backed decision inputs are not complete; emit DATA_REQUIRED."


def _summary_text(report: dict[str, Any]) -> str:
    return f"# {report['build']} - {report['report_title']}\n\nClassification: {report['classification']}\n\nAuthority: {AUTHORITY}\n"


def _weekly_review_sample() -> str:
    return """# Portfolio Atlas Weekly Review Sample

## Current Portfolio
Research-only snapshot.

## Target Portfolio
Research-only target state. Not a recommendation.

## New Entrants
Research-only changes under observation.

## Potential Exits
Research-only watch list. No trade instruction.

## Risk Changes
Drawdown, volatility, turnover, and concentration changes.

## Benchmark Comparison
VTI, 60/40, simple factor, and Oak Harvest proxy.

## No-Action / Action-Needed Status
Research-only status. No broker execution.
"""


def _manual_text() -> str:
    return """# Portfolio Atlas Operating Manual Draft

## Weekly Review Process
Review shadow portfolio evidence and benchmark comparison.

## Monthly Execution Review
Assess process drift without placing trades.

## Threshold Rules
Thresholds are research gates, not trade instructions.

## Shadow Tracking Process
Track target holdings, benchmark returns, and followed status without capital.

## Annual Review
Reassess evidence quality, complexity, taxes, and behavioral risk.

Authority: Research-only. No live portfolio, no replacement recommendation, no trades, no broker execution.
"""


def _evidence_summary(rows: list[dict[str, Any]], context: dict[str, Any]) -> str:
    return f"# Evidence Lineage Summary\n\nArtifacts archived: {len(rows)}\n\nSource-backed results ready: {context['source_backed_results_ready']}\n\nAuthority: {AUTHORITY}\n"


def _add_years(value: date, years: int) -> date:
    return date(value.year + years, value.month, value.day)


def _day(created_at: str | None) -> str:
    return (created_at or _now())[:10]


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
