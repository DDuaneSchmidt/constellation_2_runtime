from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_family_discovery import AUTHORITY_BOUNDARY

REPORT_DIRNAME = "portfolio_relevance_estimate"


def run_portfolio_relevance_estimate(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_portfolio_relevance_estimate(root=root, created_at=created_at)
    write_portfolio_relevance_estimate(report, root=root)
    return report


def build_portfolio_relevance_estimate(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    family_report = sources["candidate_family_discovery"]["payload"]
    campaign_report = sources["focused_observation_campaign"]["payload"]
    families = list(family_report.get("families") or [])
    campaign_candidates = list(campaign_report.get("campaign_candidates") or [])
    family_by_id = {row.get("family_id"): row for row in families}
    top_family_ids = {row.get("family_id") for row in campaign_candidates if row.get("family_id")}
    top_families = [family_by_id[family_id] for family_id in top_family_ids if family_id in family_by_id]
    if not top_families:
        top_families = families[: max(1, min(8, len(families)))]

    edge_report = sources["edge_magnitude_estimation"]["payload"]
    robustness_report = sources["family_robustness_review"]["payload"]
    edge_inputs = _edge_inputs(edge_report=edge_report, families=families, campaign_candidates=campaign_candidates)
    robustness_inputs = _robustness_inputs(robustness_report)
    source_limitations = _source_limitations(sources)
    scenarios = _scenarios(
        family_report=family_report,
        robustness_inputs=robustness_inputs,
        families=families,
        top_families=top_families,
        campaign_candidates=campaign_candidates,
        edge_inputs=edge_inputs,
        source_limitations=source_limitations,
    )
    conclusion = _conclusion(scenarios=scenarios, source_limitations=source_limitations)

    return {
        "schema_id": "atlas_v2_research_os_portfolio_relevance_estimate",
        "schema_version": "1.0",
        "report_type": "PORTFOLIO_RELEVANCE_ESTIMATE",
        "created_at": created,
        "day": created[:10],
        "summary": {
            "independent_family_estimate_base": scenarios["base"]["number_of_independent_families"]["estimate"],
            "campaign_candidate_count": len(campaign_candidates),
            "source_family_count": len(families),
            "missing_required_input_count": sum(not source["exists"] for source in sources.values()),
            "base_expected_net_edge_after_haircut": scenarios["base"]["expected_net_edge_after_haircut"],
            "base_signal_magnitude_could_plausibly_matter": scenarios["base"]["whether_signal_magnitude_could_plausibly_matter"],
            "primary_overlap_risk": scenarios["base"]["correlation_overlap_risk"]["classification"],
            "required_conclusion": conclusion["could_this_eventually_matter_for_portfolio_returns"],
        },
        "required_inputs": {
            name: {"path": source["path"], "exists": source["exists"], "report_type": source["payload"].get("report_type")}
            for name, source in sources.items()
        },
        "source_limitations": source_limitations,
        "source_observations": _source_observations(family_report, campaign_report, edge_report, robustness_report, edge_inputs, robustness_inputs),
        "scenarios": scenarios,
        "required_conclusion": conclusion,
        "methodology": {
            "frequency_proxy": "Candidate sample sizes and campaign counts are used only as relative frequency proxies; no trade cadence or allocation is inferred.",
            "edge_proxy": "Expected net edge is derived from reported candidate expectancy after scenario haircuts; missing edge_magnitude_estimation and family_robustness_review inputs lower confidence.",
            "independence_proxy": "Independent family count is bounded by campaign family diversity, genuinely distinct family count, and repeated-variant/near-duplicate risk.",
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Hypothetical research only.",
            "No investment advice.",
            "No capital allocation.",
            "No trade recommendations.",
            "No position sizing.",
            "No portfolio construction authority.",
        ],
    }


def write_portfolio_relevance_estimate(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    report_root = Path(root) / REPORT_DIRNAME
    day_root = report_root / str(report.get("day") or _today())
    day_root.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_portfolio_relevance_summary(report)
    dated_json = day_root / "portfolio_relevance_estimate_report.json"
    dated_summary = day_root / "portfolio_relevance_estimate_summary.md"
    latest_json = report_root / "latest.json"
    latest_summary = report_root / "latest_summary.md"
    for path in (dated_json, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (dated_summary, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": dated_json, "summary": dated_summary, "latest_json": latest_json, "latest_summary": latest_summary}


def render_portfolio_relevance_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    conclusion = report.get("required_conclusion") or {}
    lines = [
        "# Portfolio Relevance Estimate",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Summary",
        "",
        f"- Independent family estimate, base: {summary.get('independent_family_estimate_base')}",
        f"- Campaign candidates: {summary.get('campaign_candidate_count')}",
        f"- Missing required inputs: {summary.get('missing_required_input_count')}",
        f"- Base expected net edge after haircut: {summary.get('base_expected_net_edge_after_haircut')}",
        f"- Base overlap risk: {summary.get('primary_overlap_risk')}",
        f"- Could this eventually matter: {conclusion.get('could_this_eventually_matter_for_portfolio_returns')}",
        "",
        "## Scenarios",
        "",
    ]
    for name in ("conservative", "base", "optimistic"):
        scenario = (report.get("scenarios") or {}).get(name, {})
        lines.extend(
            [
                f"### {name.title()}",
                "",
                f"- Independent families: {scenario.get('number_of_independent_families', {}).get('estimate')} ({scenario.get('number_of_independent_families', {}).get('rationale')})",
                f"- Frequency per family: {scenario.get('frequency_per_family')}",
                f"- Expected net edge after haircut: {scenario.get('expected_net_edge_after_haircut')}",
                f"- Correlation/overlap risk: {scenario.get('correlation_overlap_risk', {}).get('classification')}",
                f"- Small-account capacity relevance: {scenario.get('capacity_relevance_for_small_personal_account_scale')}",
                f"- Signal magnitude could plausibly matter: {scenario.get('whether_signal_magnitude_could_plausibly_matter')}",
                "",
            ]
        )
    lines.extend(
        [
            "## Required Conclusion",
            "",
            f"- Could this eventually matter for portfolio returns? {conclusion.get('could_this_eventually_matter_for_portfolio_returns')}",
            f"- What must be proven next? {conclusion.get('what_must_be_proven_next')}",
            f"- What evidence would invalidate the idea? {conclusion.get('what_evidence_would_invalidate_the_idea')}",
            "",
            "## Guardrails",
            "",
            "- Hypothetical research only.",
            "- No investment advice, capital allocation, trade recommendations, position sizing, or portfolio construction authority.",
            "",
        ]
    )
    return "\n".join(lines)


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "edge_magnitude_estimation": root / "edge_magnitude_estimation" / "latest.json",
        "family_robustness_review": root / "family_robustness_review" / "latest.json",
        "candidate_family_discovery": root / "candidate_family_discovery" / "latest.json",
        "focused_observation_campaign": root / "focused_observation_campaign" / "latest.json",
    }
    return {name: {"path": str(path), "exists": path.exists(), "payload": _read_json(path, {})} for name, path in paths.items()}


def _scenarios(
    *,
    family_report: dict[str, Any],
    robustness_inputs: dict[str, Any],
    families: list[dict[str, Any]],
    top_families: list[dict[str, Any]],
    campaign_candidates: list[dict[str, Any]],
    edge_inputs: dict[str, Any],
    source_limitations: list[str],
) -> dict[str, Any]:
    summary = family_report.get("summary") or {}
    top_8_family_count = _int(summary.get("top_8_family_count")) or len({row.get("family_id") for row in campaign_candidates if row.get("family_id")})
    genuinely_distinct = _int(summary.get("genuinely_distinct_family_count"))
    high_priority = sum(1 for row in families if row.get("classification") == "HIGH_PRIORITY_FAMILY")
    promising = sum(1 for row in families if row.get("classification") in {"HIGH_PRIORITY_FAMILY", "PROMISING_FAMILY"})
    robust_observable = robustness_inputs.get("robust_enough_to_observe_count", 0)
    base_independent = max(0, min(top_8_family_count or len(top_families), genuinely_distinct or len(top_families), max(1, high_priority + 2)))
    if robust_observable:
        base_independent = min(base_independent, max(1, robust_observable + 1))
    conservative_independent = max(1 if families else 0, min(base_independent, high_priority or robust_observable or 2))
    optimistic_independent = max(base_independent, min(genuinely_distinct or len(families), promising or len(families), top_8_family_count + 2))

    raw_edge = edge_inputs["average_expectancy"]
    scenarios = {
        "conservative": _scenario(
            independent=conservative_independent,
            families=top_families,
            scenario_name="conservative",
            edge_inputs=edge_inputs,
            raw_edge=raw_edge,
            haircut=0.75,
            frequency_label="low; require sparse confirmed observations per family before relevance can be inferred",
            overlap="HIGH",
            capacity="Likely sufficient at small personal-account scale if ever validated, but relevance is limited by sparse confirmed opportunity count.",
            matter_threshold=0.0010,
            limitations=source_limitations,
        ),
        "base": _scenario(
            independent=base_independent,
            families=top_families,
            scenario_name="base",
            edge_inputs=edge_inputs,
            raw_edge=raw_edge,
            haircut=0.55,
            frequency_label="moderate; campaign sample sizes imply repeated historical triggers, but true forward cadence is unproven",
            overlap="MEDIUM_HIGH",
            capacity="Plausibly relevant at small personal-account scale because candidates reference liquid ETFs/large caps; execution capacity is not the binding issue at this research stage.",
            matter_threshold=0.0015,
            limitations=source_limitations,
        ),
        "optimistic": _scenario(
            independent=optimistic_independent,
            families=top_families,
            scenario_name="optimistic",
            edge_inputs=edge_inputs,
            raw_edge=raw_edge,
            haircut=0.35,
            frequency_label="moderate to high if multiple distinct families survive direct data and paper-forward validation",
            overlap="MEDIUM",
            capacity="Likely capacity-relevant for small personal-account scale if validation confirms liquid symbols, stable fills, and non-overlapping opportunities.",
            matter_threshold=0.0020,
            limitations=source_limitations,
        ),
    }
    return scenarios


def _scenario(
    *,
    independent: int,
    families: list[dict[str, Any]],
    scenario_name: str,
    edge_inputs: dict[str, Any],
    raw_edge: float,
    haircut: float,
    frequency_label: str,
    overlap: str,
    capacity: str,
    matter_threshold: float,
    limitations: list[str],
) -> dict[str, Any]:
    source_net_edge = edge_inputs.get("scenario_net_expectancy", {}).get(scenario_name)
    net_edge = _float(source_net_edge) if source_net_edge is not None else max(0.0, raw_edge * (1.0 - haircut))
    source_frequency = edge_inputs.get("scenario_frequency_per_month", {}).get(scenario_name)
    if source_frequency is not None:
        frequency_label = f"{frequency_label}; edge_magnitude_estimation average={round(_float(source_frequency), 3)} observations/month"
    return {
        "number_of_independent_families": {
            "estimate": independent,
            "rationale": "Discounts reported family count for repeated variants, shared regimes, shared symbols, proxy-data dependence, and missing upstream robustness inputs.",
        },
        "frequency_per_family": frequency_label,
        "expected_net_edge_after_haircut": {
            "raw_expectancy_proxy": round(raw_edge, 6),
            "haircut": haircut,
            "net_expectancy_proxy": round(net_edge, 6),
            "basis": "Per-candidate expectancy proxy; not an allocation return estimate.",
        },
        "correlation_overlap_risk": {
            "classification": overlap,
            "drivers": [
                "Top families share BREAKOUT/CHOP variants across 5m/15m/30m.",
                "Several candidates use overlapping liquid ETF and mega-cap universes.",
                "Proxy-data dependence remains unresolved.",
            ],
        },
        "capacity_relevance_for_small_personal_account_scale": capacity,
        "whether_signal_magnitude_could_plausibly_matter": net_edge >= matter_threshold and independent >= 2 and not _severe_limitations(limitations),
        "limitations": limitations,
    }


def _edge_inputs(*, edge_report: dict[str, Any], families: list[dict[str, Any]], campaign_candidates: list[dict[str, Any]]) -> dict[str, Any]:
    edge_families = list(edge_report.get("families") or [])
    scenario_net: dict[str, list[float]] = {"conservative": [], "base": [], "optimistic": []}
    scenario_frequency: dict[str, list[float]] = {"conservative": [], "base": [], "optimistic": []}
    for family in edge_families:
        estimates = family.get("contribution_estimates") if isinstance(family.get("contribution_estimates"), dict) else {}
        for scenario in scenario_net:
            row = estimates.get(scenario) if isinstance(estimates.get(scenario), dict) else {}
            net = _float(row.get("net_expectancy_estimate"))
            freq = _float(row.get("expected_observations_per_month"))
            if net > 0:
                scenario_net[scenario].append(net)
            if freq > 0:
                scenario_frequency[scenario].append(freq)
    values = [_float(row.get("expectancy")) for row in campaign_candidates]
    if not any(values):
        values = [_float(row.get("average_expectancy")) for row in families]
    values = [value for value in values if value > 0]
    return {
        "average_expectancy": mean(values) if values else 0.0,
        "sample_count": len(values),
        "source": "edge_magnitude_estimation.families.contribution_estimates" if edge_families else ("campaign_candidates.expectancy" if campaign_candidates else "families.average_expectancy"),
        "scenario_net_expectancy": {scenario: round(mean(nums), 6) for scenario, nums in scenario_net.items() if nums},
        "scenario_frequency_per_month": {scenario: round(mean(nums), 3) for scenario, nums in scenario_frequency.items() if nums},
    }


def _robustness_inputs(robustness_report: dict[str, Any]) -> dict[str, Any]:
    reviews = list(robustness_report.get("family_reviews") or [])
    classifications = Counter(str(row.get("classification") or "UNKNOWN") for row in reviews)
    duplicate_levels = Counter(str((row.get("duplicate_risk") or {}).get("level") or "UNKNOWN") for row in reviews if isinstance(row.get("duplicate_risk"), dict))
    proxy_blocking = sum(1 for row in reviews if ((row.get("proxy_data_dependence") or {}).get("level") == "BLOCKING_FOR_CONFIDENCE_INCREASE"))
    return {
        "family_review_count": len(reviews),
        "classification_counts": dict(classifications),
        "duplicate_risk_counts": dict(duplicate_levels),
        "proxy_blocking_count": proxy_blocking,
        "robust_enough_to_observe_count": classifications.get("ROBUST_ENOUGH_TO_OBSERVE", 0),
    }


def _source_observations(
    family_report: dict[str, Any],
    campaign_report: dict[str, Any],
    edge_report: dict[str, Any],
    robustness_report: dict[str, Any],
    edge_inputs: dict[str, Any],
    robustness_inputs: dict[str, Any],
) -> dict[str, Any]:
    campaign_candidates = list(campaign_report.get("campaign_candidates") or [])
    return {
        "family_summary": family_report.get("summary") or {},
        "campaign_summary": campaign_report.get("summary") or {},
        "edge_magnitude_summary": edge_report.get("summary") or {},
        "family_robustness_summary": {
            "classification_counts": robustness_report.get("classification_counts") or robustness_inputs.get("classification_counts"),
            "evidence_summary": robustness_report.get("evidence_summary"),
            "missing_optional_inputs": robustness_report.get("missing_optional_inputs"),
        },
        "campaign_mechanisms": dict(Counter(row.get("mechanism") or "UNKNOWN" for row in campaign_candidates)),
        "campaign_regimes": dict(Counter(row.get("regime") or "UNKNOWN" for row in campaign_candidates)),
        "edge_proxy": edge_inputs,
        "robustness_proxy": robustness_inputs,
    }


def _source_limitations(sources: dict[str, dict[str, Any]]) -> list[str]:
    limitations = []
    for name, source in sources.items():
        if not source["exists"]:
            limitations.append(f"Required input missing: {name}/latest.json")
    if sources["focused_observation_campaign"]["payload"].get("summary", {}).get("biggest_remaining_risk"):
        limitations.append(str(sources["focused_observation_campaign"]["payload"]["summary"]["biggest_remaining_risk"]))
    limitations.append("No allocation, position sizing, trade recommendation, or portfolio construction inference is authorized.")
    return limitations


def _conclusion(*, scenarios: dict[str, Any], source_limitations: list[str]) -> dict[str, Any]:
    base = scenarios["base"]
    could_matter = "Plausibly yes, but only conditionally: the base case has enough family diversity and positive haircut-adjusted expectancy proxy to justify further validation, not allocation."
    if _severe_limitations(source_limitations):
        could_matter = "Possibly, but not yet evidenced strongly enough: required upstream magnitude or robustness inputs are missing, so the answer remains conditional research."
    return {
        "could_this_eventually_matter_for_portfolio_returns": could_matter,
        "what_must_be_proven_next": "Direct candidate data validation, paper-forward observation on one representative per independent family, realized frequency, net-of-cost expectancy, drawdown behavior, and cross-family correlation/overlap.",
        "what_evidence_would_invalidate_the_idea": "Direct-data replays or paper-forward observations showing zero/negative net expectancy after realistic costs, materially lower trigger frequency, high family correlation, regime fragility, proxy/backtest mismatch, or edge concentration in duplicate variants.",
        "base_case_reference": base,
    }


def _severe_limitations(limitations: list[str]) -> bool:
    return any("edge_magnitude_estimation" in item or "family_robustness_review" in item for item in limitations)


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
