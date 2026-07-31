from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "search_overfit_guardrail"

RISK_CLASSIFICATIONS = (
    "LOW_OVERFIT_RISK",
    "MODERATE_OVERFIT_RISK",
    "HIGH_OVERFIT_RISK",
    "UNACCEPTABLE_OVERFIT_RISK",
)
RECOMMENDATIONS = (
    "PROCEED_TO_FORWARD_OBSERVATION",
    "REQUIRE_DIRECT_DATA",
    "REQUIRE_HOLDOUT_REPLAY",
    "REQUIRE_FAMILY_DEDUPLICATION",
    "PAUSE_PROFILE",
    "REJECT_PROFILE",
)

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "search_overfit_guardrail_allowed": True,
    "confidence_increase_from_search_selection_allowed": False,
    "trade_recommendation_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "broker_execution_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}

CONFIDENCE_INCREASE_ALLOWED_ONLY_THROUGH = [
    "direct-data validation",
    "forward observation",
    "holdout replay",
    "family-level repeatability",
    "net-of-cost evidence",
]

GUARDRAILS = [
    "Do not allow a candidate or family to gain confidence only because it was selected from a larger search.",
    "Confidence may increase only through direct-data validation, forward observation, holdout replay, family-level repeatability, or net-of-cost evidence.",
    "No trade recommendations.",
    "No capital allocation.",
    "No position sizing.",
    "No broker execution.",
    "No automatic paper placement.",
    "No candidate production promotion.",
]


def run_search_overfit_guardrail(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_search_overfit_guardrail_report(root=root, created_at=created_at)
    write_search_overfit_guardrail_report(report, root=root)
    return report


def build_search_overfit_guardrail_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    profile_reviews = _profile_reviews(sources)
    family_reviews = _family_reviews(sources)
    summary = _summary(profile_reviews, family_reviews)
    report = {
        "schema_id": "atlas_v2_research_os_search_overfit_guardrail_v1",
        "schema_version": "1.0",
        "report_type": "SEARCH_OVERFIT_MULTIPLE_TESTING_GUARDRAIL",
        "created_at": created,
        "day": created[:10],
        "source_reports": {
            name: {"path": source["path"], "exists": source["exists"], "report_type": source["payload"].get("report_type")}
            for name, source in sources.items()
        },
        "summary": summary,
        "trial_profile_reviews": profile_reviews,
        "candidate_family_reviews": family_reviews,
        "mandatory_rules": {
            "confidence_increase_from_search_selection_allowed": False,
            "confidence_increase_allowed_only_through": list(CONFIDENCE_INCREASE_ALLOWED_ONLY_THROUGH),
        },
        "methodology": {
            "estimated_false_discovery_risk": "Conservative ordinal estimate from hypotheses tested, finalist rate, near-threshold count, duplicate risk, and proxy/data blockers.",
            "data_snooping_risk": "Raised when ranking or narrowing is selected from the same search that generated candidates without holdout replay or forward evidence.",
            "family_recommendations": "Direct-data, holdout, and deduplication requirements take precedence over forward-observation permission.",
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": list(GUARDRAILS),
    }
    _validate_report(report)
    return report


def write_search_overfit_guardrail_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    out_dir = root_path / str(report.get("day"))
    out_dir.mkdir(parents=True, exist_ok=True)
    root_path.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "search_overfit_guardrail_report.json"
    summary_path = out_dir / "search_overfit_guardrail_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_search_overfit_guardrail_summary(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_search_overfit_guardrail_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Search Overfit Guardrail",
        "",
        f"Created: {report.get('created_at')}",
        f"Profiles analyzed: {summary.get('trial_profiles_analyzed', 0)}",
        f"Families analyzed: {summary.get('families_analyzed', 0)}",
        f"Highest risk source: {summary.get('highest_overfit_risk_source', 'none')}",
        "",
        "## Profile Risk",
        "",
    ]
    for row in report.get("trial_profile_reviews") or []:
        lines.append(f"- {row.get('profile_id')}: {row.get('risk_classification')} / {row.get('recommendation')}")
    lines.extend(["", "## Family Requirements", ""])
    lines.append(f"- Holdout/direct-data required: {', '.join(summary.get('families_requiring_holdout_or_direct_data', [])[:12]) or 'none'}")
    lines.append(f"- Forward observation allowed: {', '.join(summary.get('families_allowed_for_forward_observation', [])[:12]) or 'none'}")
    lines.append(f"- Profiles to pause/reject: {', '.join(summary.get('profiles_to_pause_or_reject', [])) or 'none'}")
    lines.extend(
        [
            "",
            "## Mandatory Rule",
            "",
            "- Search selection alone cannot increase confidence.",
            "- Confidence may increase only through direct data, forward observation, holdout replay, family-level repeatability, or net-of-cost evidence.",
            "",
            "## Authority",
            "",
            "- Research-only overfit guardrail.",
            "- No trade, capital, position-sizing, broker, automatic paper-placement, or candidate production-promotion authority.",
            "",
        ]
    )
    return "\n".join(lines)


def _profile_reviews(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    reviews = []
    cluster = sources["observation_cluster_split_experiment"]["payload"]
    if cluster:
        post = cluster.get("post_split") or {}
        pre = cluster.get("pre_split") or {}
        reviews.append(
            _profile_row(
                profile_id="OBSERVATION_CLUSTER_SPLIT_EXPERIMENT_POST_SPLIT",
                report_name="observation_cluster_split_experiment",
                hypotheses=_as_int(post.get("hypotheses") or pre.get("hypotheses")),
                families=0,
                eligible=_as_int(post.get("eligible_candidates")),
                near_threshold=0,
                selection_funnel={
                    "observations": post.get("observations"),
                    "clusters": post.get("clusters"),
                    "claims": post.get("claims"),
                    "hypotheses": post.get("hypotheses"),
                    "backtest_supported_candidates": post.get("backtest_supported_candidates"),
                    "eligible_candidates": post.get("eligible_candidates"),
                    "paper_forward_ready_candidates": post.get("paper_forward_ready_candidates"),
                },
                winner_selection_criteria="Split by mechanism/regime/timeframe/source/symbol-group, then evaluate edge and backtest support with unchanged gates.",
                duplicate_count=0,
                proxy_data_risk="HIGH" if cluster.get("data_source", {}).get("symbol") == "SPY" else "UNKNOWN",
                data_blocked=True,
            )
        )
    expanded = sources["expanded_search_trial"]["payload"]
    if expanded:
        final = expanded.get("final_qualification") or {}
        final_summary = final.get("summary") or {}
        comparison = expanded.get("comparison_against_prior_baseline") or {}
        reviews.append(
            _profile_row(
                profile_id=str(expanded.get("profile") or "EXPANDED_SEARCH_TRIAL"),
                report_name="expanded_search_trial",
                hypotheses=_as_int(final_summary.get("hypotheses_generated") or comparison.get("new_hypotheses")),
                families=_as_int(comparison.get("supported_diagnostic_families") or comparison.get("new_families")),
                eligible=_as_int(final_summary.get("final_eligible_candidates") or comparison.get("new_eligible_candidates")),
                near_threshold=len(sources["expanded_search_gate_audit"]["payload"].get("near_threshold_expanded_candidates") or []),
                selection_funnel={
                    "hypotheses": final_summary.get("hypotheses_generated") or comparison.get("new_hypotheses"),
                    "backtest_supported": final_summary.get("backtest_supported_candidates") or comparison.get("new_backtest_supported_candidates"),
                    "final_eligible": final_summary.get("final_eligible_candidates") or comparison.get("new_eligible_candidates"),
                    "supported_diagnostic_families": comparison.get("supported_diagnostic_families"),
                    "robust_families": comparison.get("robust_families"),
                },
                winner_selection_criteria="Expanded dimensions generated candidates; support came from replay/backtest examples, but final eligibility and robust-family gates remained unchanged.",
                duplicate_count=_as_int(comparison.get("supported_duplicate_families") or comparison.get("duplicate_families")),
                proxy_data_risk="HIGH",
                data_blocked=bool(comparison.get("promising_but_data_blocked_families")),
            )
        )
    focused = sources["focused_expanded_search_trial"]["payload"]
    if focused:
        summary = focused.get("summary") or {}
        reviews.append(
            _profile_row(
                profile_id=str(focused.get("profile") or "FOCUSED_EXPANDED_SEARCH_TRIAL"),
                report_name="focused_expanded_search_trial",
                hypotheses=_as_int(summary.get("hypotheses") or summary.get("hypotheses_tested")),
                families=_as_int(summary.get("candidate_families") or summary.get("families")),
                eligible=_as_int(summary.get("final_eligible") or summary.get("final_eligible_candidates")),
                near_threshold=_as_int(summary.get("near_threshold_candidates")),
                selection_funnel=dict(summary),
                winner_selection_criteria="Focused profile generated by prior narrowing; requires holdout or forward evidence before confidence increase.",
                duplicate_count=_as_int(summary.get("duplicate_families")),
                proxy_data_risk="UNKNOWN",
                data_blocked=bool(summary.get("data_blocked") or summary.get("needs_direct_data")),
            )
        )
    ranking = sources["final_candidate_ranking"]["payload"]
    if ranking:
        summary = ranking.get("summary") or {}
        reviews.append(
            _profile_row(
                profile_id="FINAL_CANDIDATE_RANKING",
                report_name="final_candidate_ranking",
                hypotheses=_as_int(summary.get("candidates_evaluated")),
                families=_as_int(sources["candidate_family_discovery"]["payload"].get("summary", {}).get("unique_candidate_families")),
                eligible=_as_int(summary.get("ranked_eligible_candidates") or summary.get("eligible_candidates_reviewed")),
                near_threshold=_near_threshold_from_candidates(ranking.get("excluded_candidates") or []),
                selection_funnel={
                    "candidates_evaluated": summary.get("candidates_evaluated"),
                    "ranked_eligible_candidates": summary.get("ranked_eligible_candidates"),
                    "top_20_count": summary.get("top_20_count"),
                    "campaign_candidate_count": summary.get("campaign_candidate_count"),
                    "classification_counts": summary.get("classification_counts"),
                },
                winner_selection_criteria="Ranked by final_score, expectancy, profit factor, sample size, drawdown inverse, replay consistency, diversity, and lower penalties.",
                duplicate_count=_as_int(sources["candidate_family_discovery"]["payload"].get("summary", {}).get("near_duplicate_group_count")),
                proxy_data_risk="HIGH" if "proxy" in str(summary.get("biggest_remaining_risk", "")).lower() else "UNKNOWN",
                data_blocked="proxy" in str(summary.get("biggest_remaining_risk", "")).lower(),
            )
        )
    narrowing = sources["expanded_search_narrowing"]["payload"]
    if narrowing:
        n_summary = narrowing.get("summary") or {}
        profile = narrowing.get("recommended_next_search_profile") or {}
        dimensions = profile.get("dimensions") or {}
        values = sum(len(v) for v in dimensions.values() if isinstance(v, list))
        reviews.append(
            _profile_row(
                profile_id=str(profile.get("profile_id") or "EXPANDED_OBSERVATION_TRIAL_FOCUSED"),
                report_name="expanded_search_narrowing",
                hypotheses=_as_int(n_summary.get("values_evaluated")),
                families=0,
                eligible=0,
                near_threshold=0,
                selection_funnel={"values_evaluated": n_summary.get("values_evaluated"), "selected_dimension_values": values, "recommendation_counts": n_summary.get("recommendation_counts")},
                winner_selection_criteria="Selected KEEP and strongest NARROW dimensions from prior expanded-search evidence.",
                duplicate_count=0,
                proxy_data_risk="MEDIUM_HIGH",
                data_blocked=bool(n_summary.get("quality_assessment")),
            )
        )
    return reviews


def _profile_row(
    *,
    profile_id: str,
    report_name: str,
    hypotheses: int,
    families: int,
    eligible: int,
    near_threshold: int,
    selection_funnel: dict[str, Any],
    winner_selection_criteria: str,
    duplicate_count: int,
    proxy_data_risk: str,
    data_blocked: bool,
) -> dict[str, Any]:
    finalist_rate = (eligible / hypotheses) if hypotheses else 0.0
    risk_score = 0
    if hypotheses >= 500:
        risk_score += 2
    elif hypotheses >= 100:
        risk_score += 1
    if finalist_rate == 0 and hypotheses:
        risk_score += 2
    elif finalist_rate < 0.05 and hypotheses:
        risk_score += 1
    if near_threshold >= 20:
        risk_score += 2
    elif near_threshold:
        risk_score += 1
    if duplicate_count >= 10:
        risk_score += 1
    if proxy_data_risk in {"HIGH", "MEDIUM_HIGH"} or data_blocked:
        risk_score += 1
    classification = _risk_classification(risk_score)
    recommendation = _profile_recommendation(classification, eligible, data_blocked, near_threshold)
    return {
        "profile_id": profile_id,
        "source_report": report_name,
        "hypotheses_tested": hypotheses,
        "candidate_families_tested": families,
        "final_eligible_candidates": eligible,
        "near_threshold_candidates": near_threshold,
        "selection_funnel": selection_funnel,
        "winner_selection_criteria": winner_selection_criteria,
        "estimated_false_discovery_risk": classification,
        "data_snooping_risk": _data_snooping_risk(hypotheses, near_threshold, eligible),
        "duplicate_family_risk": _duplicate_risk(duplicate_count, families),
        "proxy_data_risk": proxy_data_risk,
        "forward_validation_requirement": _forward_validation_requirement(classification, data_blocked, near_threshold),
        "recommendation": recommendation,
        "required_actions": _required_profile_actions(recommendation, data_blocked, near_threshold),
        "confidence_increase_from_search_selection_allowed": False,
    }


def _family_reviews(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    families: dict[str, dict[str, Any]] = {}
    for family in sources["candidate_family_discovery"]["payload"].get("families") or []:
        if family.get("family_id"):
            families[str(family["family_id"])] = dict(family)
    for family in sources["edge_magnitude_estimation"]["payload"].get("families") or []:
        if family.get("family_id"):
            families.setdefault(str(family["family_id"]), {}).update({"edge": family})
    rows = []
    for family_id, family in sorted(families.items(), key=lambda item: _family_sort_key(item[1])):
        edge = family.get("edge") or {}
        duplicate_risk = str(family.get("duplicate_or_distinct_assessment") or "").upper()
        direct_status = str(edge.get("direct_validation_status") or "").upper()
        proxy = str(edge.get("proxy_dependence") or "").upper()
        blockers = [str(item) for item in edge.get("data_blockers") or family.get("common_failure_risks") or []]
        requires_direct = bool(family.get("requires_direct_data_validation")) or any("direct" in b.lower() for b in blockers)
        needs_dedup = "REPEATED" in duplicate_risk or any("duplicate" in b.lower() for b in blockers)
        net_cost_ok = _net_cost_ok(edge)
        classification = _family_risk_classification(requires_direct, needs_dedup, direct_status, proxy, net_cost_ok)
        recommendation = _family_recommendation(classification, requires_direct, needs_dedup, direct_status, net_cost_ok)
        rows.append(
            {
                "family_id": family_id,
                "family_name": family.get("family_name") or edge.get("family_name") or family_id,
                "source_classification": family.get("classification"),
                "edge_classification": edge.get("classification"),
                "hypotheses_tested": None,
                "candidate_families_tested": 1,
                "final_eligible_candidates": _as_int((family.get("classification_counts") or {}).get("READY_FOR_PAPER_FORWARD_OBSERVATION")),
                "near_threshold_candidates": 0,
                "selection_funnel": {
                    "candidate_count": family.get("candidate_count") or edge.get("candidate_count"),
                    "best_rank": family.get("best_rank"),
                    "recommended_priority": family.get("recommended_priority"),
                    "direct_validation_status": edge.get("direct_validation_status"),
                    "edge_classification": edge.get("classification"),
                },
                "winner_selection_criteria": "Family inherited from finalist ranking; search selection alone is not confidence evidence.",
                "estimated_false_discovery_risk": classification,
                "data_snooping_risk": "HIGH" if direct_status not in {"CONFIRMED"} else "MODERATE",
                "duplicate_family_risk": "HIGH" if needs_dedup else "LOW",
                "proxy_data_risk": "HIGH" if "HIGH" in proxy or requires_direct else "MODERATE" if "PROXY" in proxy else "LOW",
                "forward_validation_requirement": _family_forward_requirement(requires_direct, needs_dedup, direct_status, net_cost_ok),
                "recommendation": recommendation,
                "required_actions": _family_required_actions(recommendation, requires_direct, needs_dedup, direct_status, net_cost_ok),
                "confidence_increase_from_search_selection_allowed": False,
            }
        )
    return rows


def _risk_classification(score: int) -> str:
    if score >= 6:
        return "UNACCEPTABLE_OVERFIT_RISK"
    if score >= 4:
        return "HIGH_OVERFIT_RISK"
    if score >= 2:
        return "MODERATE_OVERFIT_RISK"
    return "LOW_OVERFIT_RISK"


def _profile_recommendation(classification: str, eligible: int, data_blocked: bool, near_threshold: int) -> str:
    if classification == "UNACCEPTABLE_OVERFIT_RISK":
        return "REJECT_PROFILE"
    if classification == "HIGH_OVERFIT_RISK" and eligible == 0:
        return "PAUSE_PROFILE"
    if data_blocked:
        return "REQUIRE_DIRECT_DATA"
    if near_threshold:
        return "REQUIRE_HOLDOUT_REPLAY"
    return "PROCEED_TO_FORWARD_OBSERVATION"


def _family_recommendation(classification: str, requires_direct: bool, needs_dedup: bool, direct_status: str, net_cost_ok: bool) -> str:
    if needs_dedup:
        return "REQUIRE_FAMILY_DEDUPLICATION"
    if requires_direct or direct_status in {"MOSTLY_INSUFFICIENT", "NOT_RUN", ""}:
        return "REQUIRE_DIRECT_DATA"
    if direct_status != "CONFIRMED" or not net_cost_ok:
        return "REQUIRE_HOLDOUT_REPLAY"
    if classification == "UNACCEPTABLE_OVERFIT_RISK":
        return "REQUIRE_HOLDOUT_REPLAY"
    return "PROCEED_TO_FORWARD_OBSERVATION"


def _family_risk_classification(requires_direct: bool, needs_dedup: bool, direct_status: str, proxy: str, net_cost_ok: bool) -> str:
    score = 0
    if requires_direct:
        score += 2
    if needs_dedup:
        score += 2
    if direct_status in {"MOSTLY_INSUFFICIENT", "NOT_RUN", ""}:
        score += 2
    elif direct_status == "MIXED":
        score += 1
    if "HIGH" in proxy:
        score += 1
    if not net_cost_ok:
        score += 1
    return _risk_classification(score)


def _data_snooping_risk(hypotheses: int, near_threshold: int, eligible: int) -> str:
    if hypotheses >= 500 and (near_threshold or eligible == 0):
        return "HIGH"
    if hypotheses >= 100:
        return "MODERATE"
    return "LOW"


def _duplicate_risk(duplicate_count: int, families: int) -> str:
    if duplicate_count >= 10 or (families and duplicate_count / max(families, 1) >= 0.25):
        return "HIGH"
    if duplicate_count:
        return "MODERATE"
    return "LOW"


def _forward_validation_requirement(classification: str, data_blocked: bool, near_threshold: int) -> str:
    requirements = ["forward observation cannot increase confidence without independent evidence"]
    if data_blocked:
        requirements.append("direct-data validation required")
    if near_threshold or classification in {"HIGH_OVERFIT_RISK", "UNACCEPTABLE_OVERFIT_RISK"}:
        requirements.append("holdout replay required before confidence increase")
    return "; ".join(requirements)


def _required_profile_actions(recommendation: str, data_blocked: bool, near_threshold: int) -> list[str]:
    actions = [recommendation]
    if data_blocked and "REQUIRE_DIRECT_DATA" not in actions:
        actions.append("REQUIRE_DIRECT_DATA")
    if near_threshold and "REQUIRE_HOLDOUT_REPLAY" not in actions:
        actions.append("REQUIRE_HOLDOUT_REPLAY")
    return actions


def _family_forward_requirement(requires_direct: bool, needs_dedup: bool, direct_status: str, net_cost_ok: bool) -> str:
    requirements = []
    if requires_direct:
        requirements.append("direct-data validation")
    if needs_dedup:
        requirements.append("family-level deduplication")
    if direct_status != "CONFIRMED":
        requirements.append("holdout replay or confirmed forward observation")
    if not net_cost_ok:
        requirements.append("net-of-cost evidence")
    return ", ".join(requirements or ["forward observation allowed as research evidence only"])


def _family_required_actions(recommendation: str, requires_direct: bool, needs_dedup: bool, direct_status: str, net_cost_ok: bool) -> list[str]:
    actions = [recommendation]
    if requires_direct and "REQUIRE_DIRECT_DATA" not in actions:
        actions.append("REQUIRE_DIRECT_DATA")
    if needs_dedup and "REQUIRE_FAMILY_DEDUPLICATION" not in actions:
        actions.append("REQUIRE_FAMILY_DEDUPLICATION")
    if direct_status != "CONFIRMED" and "REQUIRE_HOLDOUT_REPLAY" not in actions:
        actions.append("REQUIRE_HOLDOUT_REPLAY")
    if not net_cost_ok and "REQUIRE_HOLDOUT_REPLAY" not in actions:
        actions.append("REQUIRE_HOLDOUT_REPLAY")
    return actions


def _summary(profile_reviews: list[dict[str, Any]], family_reviews: list[dict[str, Any]]) -> dict[str, Any]:
    profile_counts = Counter(row["estimated_false_discovery_risk"] for row in profile_reviews)
    family_counts = Counter(row["estimated_false_discovery_risk"] for row in family_reviews)
    ranked = profile_reviews + family_reviews
    highest = max(ranked, key=lambda row: RISK_CLASSIFICATIONS.index(row["estimated_false_discovery_risk"]), default={})
    return {
        "trial_profiles_analyzed": len(profile_reviews),
        "families_analyzed": len(family_reviews),
        "profile_risk_counts": {risk: profile_counts.get(risk, 0) for risk in RISK_CLASSIFICATIONS},
        "family_risk_counts": {risk: family_counts.get(risk, 0) for risk in RISK_CLASSIFICATIONS},
        "highest_overfit_risk_source": highest.get("profile_id") or highest.get("family_id") or "",
        "families_requiring_holdout_or_direct_data": [
            row["family_id"] for row in family_reviews if {"REQUIRE_DIRECT_DATA", "REQUIRE_HOLDOUT_REPLAY"} & set(row.get("required_actions") or [])
        ],
        "families_allowed_for_forward_observation": [
            row["family_id"] for row in family_reviews if row.get("recommendation") == "PROCEED_TO_FORWARD_OBSERVATION"
        ],
        "profiles_to_pause_or_reject": [
            row["profile_id"] for row in profile_reviews if row.get("recommendation") in {"PAUSE_PROFILE", "REJECT_PROFILE"}
        ],
        "confidence_increase_from_search_selection_allowed": False,
        "authority": "RESEARCH_ONLY_NO_TRADING_CAPITAL_BROKER_POSITION_SIZING_AUTOMATIC_PAPER_OR_PRODUCTION_PROMOTION",
    }


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    names = [
        "observation_cluster_split_experiment",
        "expanded_search_trial",
        "expanded_search_gate_audit",
        "expanded_search_narrowing",
        "focused_expanded_search_trial",
        "candidate_family_discovery",
        "final_candidate_ranking",
        "edge_magnitude_estimation",
        "portfolio_relevance_estimate",
    ]
    return {name: _load_latest(root, name) for name in names}


def _load_latest(root: Path, name: str) -> dict[str, Any]:
    path = root / name / "latest.json"
    if not path.exists():
        return {"path": str(path), "exists": False, "payload": {}}
    return {"path": str(path), "exists": True, "payload": json.loads(path.read_text(encoding="utf-8"))}


def _near_threshold_from_candidates(candidates: list[dict[str, Any]]) -> int:
    count = 0
    for row in candidates:
        score = row.get("final_score")
        if isinstance(score, (int, float)) and 0.68 <= float(score) < 0.70:
            count += 1
    return count


def _net_cost_ok(edge: dict[str, Any]) -> bool:
    estimates = edge.get("contribution_estimates") or {}
    conservative = estimates.get("conservative") or {}
    value = conservative.get("net_expectancy_estimate")
    return isinstance(value, (int, float)) and float(value) > 0


def _family_sort_key(family: dict[str, Any]) -> tuple[int, str]:
    best_rank = family.get("best_rank")
    return (_as_int(best_rank) if best_rank is not None else 10_000, str(family.get("family_id") or ""))


def _as_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _validate_report(report: dict[str, Any]) -> None:
    authority = report.get("authority_boundary") or {}
    if not authority.get("research_only"):
        raise ValueError("search overfit guardrail must remain research-only")
    for field in (
        "trade_recommendation_authorized",
        "capital_authorized",
        "position_sizing_authorized",
        "broker_execution_authorized",
        "automatic_paper_trade_placement_authorized",
        "candidate_production_promotion_authorized",
    ):
        if authority.get(field) is not False:
            raise ValueError(f"forbidden authority must remain false: {field}")
    bad_risks = [
        row for row in (report.get("trial_profile_reviews") or []) + (report.get("candidate_family_reviews") or []) if row.get("estimated_false_discovery_risk") not in RISK_CLASSIFICATIONS
    ]
    if bad_risks:
        raise ValueError("invalid overfit risk classification")
    bad_recommendations = [
        row for row in (report.get("trial_profile_reviews") or []) + (report.get("candidate_family_reviews") or []) if row.get("recommendation") not in RECOMMENDATIONS
    ]
    if bad_recommendations:
        raise ValueError("invalid overfit guardrail recommendation")
    if report.get("mandatory_rules", {}).get("confidence_increase_from_search_selection_allowed") is not False:
        raise ValueError("search selection cannot increase confidence")


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
