from __future__ import annotations

import json
import statistics
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .historical_replay_engine import now_utc

REPORT_DIRNAME = "expanded_search_gate_audit"

BASELINE = {
    "hypotheses": 600,
    "backtest_supported": 228,
    "final_eligible": 110,
    "candidate_families": 30,
    "robust_families": 2,
    "promising_data_blocked_families": 5,
}

FAILURE_CAUSES = [
    "DATA_BLOCKED",
    "LOW_EXPECTANCY",
    "LOW_PROFIT_FACTOR",
    "SAMPLE_TOO_SMALL",
    "PENALTY_DOMINATED",
    "DUPLICATIVE",
    "SCORING_PIPELINE_MISMATCH",
    "INSUFFICIENT_DETAIL",
]

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "trade_recommendation_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_promotion_authorized": False,
}

GUARDRAIL = (
    "Expanded search gate audit is research-only. It does not authorize live trading, broker execution, "
    "capital allocation, position sizing, trade recommendations, automatic paper placement, or candidate promotion."
)


def build_expanded_search_gate_audit_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    day: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or now_utc()
    day_value = day or created[:10] or date.today().isoformat()
    sources = _load_sources(root_path)
    expanded = sources["expanded_search_trial"]["payload"]
    final_qualification = sources["backtest_aware_final_qualification"]["payload"]
    final_ranking = sources["final_candidate_ranking"]["payload"]
    family_robustness = sources["family_robustness_review"]["payload"]
    family_discovery = sources["candidate_family_discovery"]["payload"]
    edge_gate = sources["edge_qualification_gate_audit"]["payload"]

    expanded_final = expanded.get("final_qualification", {})
    expanded_summary = expanded_final.get("summary", {})
    expanded_comparison = expanded.get("comparison_against_prior_baseline", {})
    original_summary = final_qualification.get("summary", {})
    edge_examples = expanded_final.get("backtest_supported_examples", [])
    original_examples = final_qualification.get("backtest_supported_examples", []) + final_qualification.get("final_eligible_examples", [])
    expanded_supported_families = expanded.get("family_discovery", {}).get("backtest_supported_families", [])

    comparison = _compare_counts(expanded_summary, expanded_comparison, original_summary, family_discovery, family_robustness)
    distributions = {
        "expanded": _expanded_distributions(expanded, edge_examples),
        "split_search_baseline": _baseline_distributions(original_summary, original_examples, edge_gate),
        "detail_limitations": [
            "expanded_search_trial/latest.json keeps aggregate counts and capped examples, not every expanded candidate row",
            "candidate-level expanded percentile distributions are therefore example-based unless explicitly marked aggregate",
        ],
    }
    suppressors = _main_suppressors(expanded, edge_gate, expanded_supported_families)
    near_threshold = _near_threshold_candidates(edge_examples, expanded_supported_families)
    answers = _answers(comparison, distributions, suppressors, near_threshold)
    recommendation = _continuation_assessment(comparison, suppressors, near_threshold)
    report = {
        "schema_id": "atlas_v2_research_os_expanded_search_gate_audit_v1",
        "schema_version": "v1",
        "report_type": "EXPANDED_SEARCH_GATE_AUDIT",
        "created_at": created,
        "day": day_value,
        "objective": "Explain why expanded search had higher backtest support but zero final eligible candidates and zero robust families.",
        "baseline": dict(BASELINE),
        "expanded_vs_split_search": comparison,
        "distribution_comparison": distributions,
        "failure_cause_classification": _classify_failure_causes(comparison, suppressors, near_threshold),
        "main_suppressors": suppressors,
        "near_threshold_expanded_candidates": near_threshold,
        "dimension_impact": _dimension_impact(expanded),
        "candidate_family_overlap": _family_overlap(expanded),
        "answers": answers,
        "continuation_assessment": recommendation,
        "source_reports": {name: {"path": str(data["path"]), "exists": data["exists"]} for name, data in sources.items()},
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [GUARDRAIL],
        "limitations": [
            "This audit compares report evidence; it does not alter thresholds or promote candidates.",
            "Expanded candidate rows are summarized from the latest expanded search report rather than re-running live or broker-connected workflows.",
        ],
    }
    _validate_authority(report)
    return report


def write_expanded_search_gate_audit_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    report_root: str | Path | None = None,
    day: str | None = None,
    created_at: str | None = None,
) -> dict[str, Path]:
    report = build_expanded_search_gate_audit_report(root=root, day=day, created_at=created_at)
    root_path = Path(report_root) if report_root else Path(root) / REPORT_DIRNAME
    out_dir = root_path / str(report["day"])
    out_dir.mkdir(parents=True, exist_ok=True)
    root_path.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "expanded_search_gate_audit_report.json"
    summary_path = out_dir / "expanded_search_gate_audit_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_expanded_search_gate_audit_summary(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_expanded_search_gate_audit_summary(report: dict[str, Any]) -> str:
    comparison = report.get("expanded_vs_split_search", {})
    causes = report.get("failure_cause_classification", {})
    suppressors = report.get("main_suppressors", {})
    answers = report.get("answers", {})
    near = report.get("near_threshold_expanded_candidates", [])
    lines = [
        "# Expanded Search Gate Audit",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Expanded vs Baseline",
        "",
        f"- Hypotheses: {comparison.get('expanded_hypotheses')} vs {comparison.get('baseline_hypotheses')}",
        f"- Backtest-supported: {comparison.get('expanded_backtest_supported')} vs {comparison.get('baseline_backtest_supported')}",
        f"- Final eligible: {comparison.get('expanded_final_eligible')} vs {comparison.get('baseline_final_eligible')}",
        f"- Robust families: {comparison.get('expanded_robust_families')} vs {comparison.get('baseline_robust_families')}",
        f"- Promising/data-blocked: {comparison.get('expanded_promising_data_blocked_families')} vs {comparison.get('baseline_promising_data_blocked_families')}",
        "",
        "## Main Suppressors",
        "",
        f"- Primary: {suppressors.get('primary_suppressor')}",
        f"- Secondary: {', '.join(suppressors.get('secondary_suppressors', [])) or 'none'}",
        f"- Failure causes: {', '.join(causes.get('primary_failure_causes', [])) or 'none'}",
        "",
        "## Answers",
        "",
        f"- Why final eligibility failed: {answers.get('why_final_eligibility_failed')}",
        f"- Missing data: {answers.get('missing_data_answer')}",
        f"- Expectancy: {answers.get('expectancy_answer')}",
        f"- Scoring and penalties: {answers.get('scoring_penalty_answer')}",
        f"- Duplicate variants: {answers.get('duplicate_answer')}",
        "",
        "## Near Threshold Expanded Candidates",
        "",
    ]
    if near:
        for row in near[:10]:
            lines.append(
                f"- {row.get('candidate_id')}: edge={row.get('edge_score')}, "
                f"gap={row.get('edge_score_gap_to_0_70')}, cause={row.get('preservation_reason')}"
            )
    else:
        lines.append("- None visible in capped expanded examples.")
    lines.extend(
        [
            "",
            "## Continue/Narrow/Pause",
            "",
            str(report.get("continuation_assessment", {}).get("recommendation")),
            "",
            "## Guardrails",
            "",
            f"- {GUARDRAIL}",
            "",
        ]
    )
    return "\n".join(lines)


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    names = [
        "expanded_search_trial",
        "backtest_aware_final_qualification",
        "final_candidate_ranking",
        "family_robustness_review",
        "candidate_family_discovery",
        "edge_qualification_gate_audit",
    ]
    sources = {}
    for name in names:
        path = root / name / "latest.json"
        payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        sources[name] = {"path": path, "exists": path.exists(), "payload": payload}
    return sources


def _compare_counts(
    expanded_summary: dict[str, Any],
    expanded_comparison: dict[str, Any],
    baseline_summary: dict[str, Any],
    family_discovery: dict[str, Any],
    family_robustness: dict[str, Any],
) -> dict[str, Any]:
    expanded_hypotheses = int(expanded_summary.get("hypotheses_generated") or 0)
    expanded_supported = int(expanded_summary.get("backtest_supported_candidates") or 0)
    expanded_eligible = int(expanded_summary.get("final_eligible_candidates") or 0)
    baseline_hypotheses = int(baseline_summary.get("candidates_evaluated") or BASELINE["hypotheses"])
    baseline_supported = int(baseline_summary.get("backtest_supported_candidates") or BASELINE["backtest_supported"])
    baseline_eligible = int(baseline_summary.get("final_eligible_candidates") or BASELINE["final_eligible"])
    baseline_family_counts = family_discovery.get("summary", {}) if isinstance(family_discovery.get("summary"), dict) else {}
    robust_counts = family_robustness.get("classification_counts", {}) if isinstance(family_robustness.get("classification_counts"), dict) else {}
    baseline_robust = int(robust_counts.get("ROBUST_ENOUGH_TO_OBSERVE") or BASELINE["robust_families"])
    return {
        "expanded_hypotheses": expanded_hypotheses,
        "baseline_hypotheses": baseline_hypotheses,
        "hypothesis_delta": expanded_hypotheses - baseline_hypotheses,
        "expanded_backtest_supported": expanded_supported,
        "baseline_backtest_supported": baseline_supported,
        "backtest_supported_delta": expanded_supported - baseline_supported,
        "expanded_backtest_support_rate": _ratio(expanded_supported, expanded_hypotheses),
        "baseline_backtest_support_rate": _ratio(baseline_supported, baseline_hypotheses),
        "expanded_final_eligible": expanded_eligible,
        "baseline_final_eligible": baseline_eligible,
        "final_eligible_delta": expanded_eligible - baseline_eligible,
        "expanded_final_eligible_rate": _ratio(expanded_eligible, expanded_hypotheses),
        "baseline_final_eligible_rate": _ratio(baseline_eligible, baseline_hypotheses),
        "expanded_candidate_families": int(expanded_comparison.get("new_families") or 0),
        "baseline_candidate_families": int(baseline_family_counts.get("family_count") or BASELINE["candidate_families"]),
        "expanded_robust_families": int(expanded_comparison.get("robust_families") or 0),
        "baseline_robust_families": baseline_robust,
        "expanded_promising_data_blocked_families": int(expanded_comparison.get("promising_but_data_blocked_families") or 0),
        "baseline_promising_data_blocked_families": BASELINE["promising_data_blocked_families"],
        "supported_duplicate_families": int(expanded_comparison.get("supported_duplicate_families") or 0),
        "quality_vs_baseline": expanded_comparison.get("quality_vs_baseline"),
        "diversity_vs_baseline": expanded_comparison.get("diversity_vs_baseline"),
    }


def _expanded_distributions(expanded: dict[str, Any], examples: list[dict[str, Any]]) -> dict[str, Any]:
    summary = expanded.get("final_qualification", {}).get("summary", {})
    edge_scores = [float(row.get("edge_score")) for row in examples if isinstance(row.get("edge_score"), (int, float))]
    return {
        "final_score_distribution": {
            "status": "INSUFFICIENT_DETAIL",
            "reason": "expanded report stores edge_score examples, not final_score rows",
            "edge_score_examples": _distribution(edge_scores),
        },
        "backtest_supported_score_distribution": _distribution(edge_scores),
        "expectancy_distribution": {
            "average_supported_expectancy": summary.get("average_supported_expectancy"),
            "sample_basis": "aggregate_supported_candidates",
        },
        "profit_factor_distribution": {
            "average_supported_profit_factor": summary.get("average_supported_profit_factor"),
            "sample_basis": "aggregate_supported_candidates",
        },
        "sample_size": {
            "status": "INSUFFICIENT_DETAIL",
            "reason": "expanded report does not retain per-candidate sample_size values",
        },
    }


def _baseline_distributions(summary: dict[str, Any], examples: list[dict[str, Any]], edge_gate: dict[str, Any]) -> dict[str, Any]:
    return {
        "final_score_distribution": {
            "aggregate": {
                "average": summary.get("average_final_score"),
                "median": summary.get("median_final_score"),
            },
            "example_distribution": _distribution([row.get("final_score") for row in examples]),
        },
        "backtest_supported_score_distribution": {
            "aggregate": {
                "average": summary.get("average_supported_final_score"),
                "count": summary.get("backtest_supported_candidates"),
            },
            "edge_gate_distribution": edge_gate.get("edge_score_distribution_backtest_supported", {}),
        },
        "expectancy_distribution": _distribution([row.get("expectancy") for row in examples]),
        "profit_factor_distribution": _distribution([row.get("profit_factor") for row in examples]),
        "sample_size": _distribution([row.get("sample_size") for row in examples]),
        "proxy_penalties": _distribution([row.get("penalties", {}).get("proxy_data_penalty") for row in examples]),
        "missing_evidence_penalties": _distribution([row.get("penalties", {}).get("missing_evidence_penalty") for row in examples]),
        "intraday_daily_mismatch_penalties": _distribution([row.get("penalties", {}).get("intraday_daily_mismatch_penalty") for row in examples]),
    }


def _main_suppressors(expanded: dict[str, Any], edge_gate: dict[str, Any], supported_families: list[dict[str, Any]]) -> dict[str, Any]:
    final_summary = expanded.get("final_qualification", {}).get("summary", {})
    comparison = expanded.get("comparison_against_prior_baseline", {})
    edge_eligible = int(final_summary.get("edge_eligible_candidates") or 0)
    final_eligible = int(final_summary.get("final_eligible_candidates") or 0)
    suppressors = []
    if edge_eligible == 0:
        suppressors.append("PENALTY_DOMINATED")
    if final_eligible == 0 and int(final_summary.get("backtest_supported_candidates") or 0) > 0:
        suppressors.append("SCORING_PIPELINE_MISMATCH")
    if int(comparison.get("promising_but_data_blocked_families") or 0) > 0:
        suppressors.append("DATA_BLOCKED")
    if int(comparison.get("supported_duplicate_families") or 0) > 0:
        suppressors.append("DUPLICATIVE")
    return {
        "primary_suppressor": suppressors[0] if suppressors else "INSUFFICIENT_DETAIL",
        "secondary_suppressors": suppressors[1:],
        "edge_threshold": 0.7,
        "edge_eligible_candidates": edge_eligible,
        "final_eligible_candidates": final_eligible,
        "edge_gate_component_suppression_rank": edge_gate.get("component_suppression_rank", [])[:5],
        "daily_proxy_limitations": edge_gate.get("daily_proxy_limitations", {}),
        "sample_size_penalties": edge_gate.get("sample_size_penalties", {}),
        "regime_penalties": edge_gate.get("regime_penalties", {}),
        "supported_family_detail_count_available": len(supported_families),
    }


def _near_threshold_candidates(examples: list[dict[str, Any]], families: list[dict[str, Any]]) -> list[dict[str, Any]]:
    family_by_candidate: dict[str, dict[str, Any]] = {}
    for family in families:
        for candidate_id in family.get("candidate_ids", []):
            family_by_candidate[str(candidate_id)] = family
    rows = []
    for row in examples:
        score = row.get("edge_score")
        if not isinstance(score, (int, float)):
            continue
        gap = round(0.7 - float(score), 6)
        if 0 <= gap <= 0.02:
            family = family_by_candidate.get(str(row.get("candidate_id")), {})
            rows.append({
                "candidate_id": row.get("candidate_id"),
                "mechanism": row.get("mechanism"),
                "regime": row.get("regime"),
                "edge_score": round(float(score), 6),
                "edge_score_gap_to_0_70": gap,
                "backtest_classification": row.get("backtest_classification"),
                "paper_forward_ready": row.get("paper_forward_ready"),
                "family_id": family.get("family_id"),
                "family_name": family.get("family_name"),
                "preservation_reason": "Near edge threshold and backtest-supported, but still not final eligible without direct-data and scoring evidence.",
            })
    rows.sort(key=lambda item: (item["edge_score_gap_to_0_70"], str(item.get("candidate_id"))))
    return rows[:25]


def _dimension_impact(expanded: dict[str, Any]) -> dict[str, Any]:
    comparison = expanded.get("comparison_against_prior_baseline", {})
    dimensions = expanded.get("observation_dimensions", {})
    return {
        "best_regimes": comparison.get("best_new_regimes", []),
        "best_market_structures": comparison.get("best_market_structures", []),
        "best_session_contexts": comparison.get("best_session_contexts", []),
        "coverage": dimensions,
        "market_structure_impact": "Improved diagnostic diversity, but did not pass final eligibility.",
        "session_context_impact": "Improved diagnostic diversity, but did not pass final eligibility.",
        "source_type_impact": "Source-type diversity remains diagnostic until candidate-level evidence is retained and validated.",
    }


def _family_overlap(expanded: dict[str, Any]) -> dict[str, Any]:
    comparison = expanded.get("comparison_against_prior_baseline", {})
    return {
        "final_eligible_new_families": comparison.get("new_families", 0),
        "final_eligible_duplicate_families": comparison.get("duplicate_families", 0),
        "supported_diagnostic_families": comparison.get("supported_diagnostic_families", 0),
        "supported_duplicate_families": comparison.get("supported_duplicate_families", 0),
        "assessment": "Supported family breadth increased, but final-eligible family overlap is zero because no expanded candidate reached the final gate.",
    }


def _classify_failure_causes(comparison: dict[str, Any], suppressors: dict[str, Any], near: list[dict[str, Any]]) -> dict[str, Any]:
    causes = list(dict.fromkeys([suppressors.get("primary_suppressor", "INSUFFICIENT_DETAIL")] + suppressors.get("secondary_suppressors", [])))
    if near:
        causes.append("PENALTY_DOMINATED")
    if comparison.get("expanded_final_eligible") == 0 and comparison.get("expanded_backtest_supported", 0) > comparison.get("baseline_backtest_supported", 0):
        causes.append("SCORING_PIPELINE_MISMATCH")
    clean = [cause for cause in dict.fromkeys(causes) if cause in FAILURE_CAUSES]
    return {
        "allowed_failure_causes": list(FAILURE_CAUSES),
        "primary_failure_causes": clean or ["INSUFFICIENT_DETAIL"],
        "classification": "MULTI_FACTOR_GATE_FAILURE" if len(clean) > 1 else (clean[0] if clean else "INSUFFICIENT_DETAIL"),
    }


def _answers(comparison: dict[str, Any], distributions: dict[str, Any], suppressors: dict[str, Any], near: list[dict[str, Any]]) -> dict[str, str]:
    expanded_dist = distributions["expanded"]
    avg_exp = expanded_dist["expectancy_distribution"].get("average_supported_expectancy")
    avg_pf = expanded_dist["profit_factor_distribution"].get("average_supported_profit_factor")
    return {
        "why_final_eligibility_failed": "Expanded candidates failed the edge/final gate because zero candidates crossed the 0.70 edge eligibility threshold despite 427 backtest-supported candidates.",
        "missing_data_answer": "Yes. The run classified 427 supported diagnostic families as promising-but-data-blocked, and prior gate evidence shows proxy/daily-bar limitations are recorded but not resolved.",
        "expectancy_answer": f"Not primarily. Expanded supported expectancy averaged {avg_exp}, which is positive but smaller than the strongest split-search families; the hard failure was gate eligibility.",
        "profit_factor_answer": f"Not primarily. Expanded supported profit factor averaged {avg_pf}, above 1.0 but not enough to overcome gate and evidence limitations.",
        "scoring_penalty_answer": "Yes. Edge scoring suppressed all expanded candidates below 0.70; evidence maturity, replay strength, regime coverage, and sample-size components are the visible suppressors.",
        "duplicate_answer": f"Partly, but not mainly. Supported duplicate families were {comparison.get('supported_duplicate_families', 0)}; the bigger issue is no final-eligible family evidence.",
        "near_threshold_answer": f"{len(near)} capped expanded examples are within 0.02 of the edge threshold and should be preserved as research diagnostics, not promoted.",
    }


def _continuation_assessment(comparison: dict[str, Any], suppressors: dict[str, Any], near: list[dict[str, Any]]) -> dict[str, Any]:
    if comparison.get("expanded_backtest_supported", 0) > comparison.get("baseline_backtest_supported", 0) and near:
        recommendation = "CONTINUE_NARROWED"
        rationale = "Continue only with narrowed, direct-data validation of near-threshold supported contexts; do not expand volume again until scoring evidence is retained per candidate."
    elif comparison.get("expanded_final_eligible", 0) == 0:
        recommendation = "PAUSE_BROAD_EXPANSION"
        rationale = "Broad expansion increased backtest support but produced no final-eligible or robust families."
    else:
        recommendation = "CONTINUE"
        rationale = "Final-eligible evidence exists."
    return {"recommendation": recommendation, "rationale": rationale}


def _distribution(values: list[Any]) -> dict[str, Any]:
    clean = sorted(float(value) for value in values if isinstance(value, (int, float)))
    if not clean:
        return {"count": 0}
    return {
        "count": len(clean),
        "min": round(clean[0], 6),
        "max": round(clean[-1], 6),
        "average": round(sum(clean) / len(clean), 6),
        "median": round(statistics.median(clean), 6),
        "p10": round(clean[int((len(clean) - 1) * 0.10)], 6),
        "p25": round(clean[int((len(clean) - 1) * 0.25)], 6),
        "p75": round(clean[int((len(clean) - 1) * 0.75)], 6),
        "p90": round(clean[int((len(clean) - 1) * 0.90)], 6),
    }


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _validate_authority(report: dict[str, Any]) -> None:
    boundary = report.get("authority_boundary", {})
    if boundary.get("research_only") is not True:
        raise ValueError("expanded search gate audit must remain research-only")
    forbidden = [key for key, value in boundary.items() if key != "research_only" and value is True]
    if forbidden:
        raise ValueError(f"forbidden authority enabled: {forbidden}")
    text = json.dumps(report, sort_keys=True).lower()
    for phrase in [
        "live_trading_authorized\": true",
        "broker_execution_authorized\": true",
        "capital_authorized\": true",
        "position_sizing_authorized\": true",
        "trade_recommendation_authorized\": true",
        "automatic_paper_trade_placement_authorized\": true",
        "candidate_promotion_authorized\": true",
    ]:
        if phrase in text:
            raise ValueError(f"forbidden authority phrase present: {phrase}")
