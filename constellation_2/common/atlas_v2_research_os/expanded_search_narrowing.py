from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "expanded_search_narrowing"
PROFILE_ID = "EXPANDED_OBSERVATION_TRIAL_FOCUSED"
DIMENSIONS = ("regime", "market_structure", "session_context", "mechanism", "timeframe", "source_type", "symbol/universe")
RECOMMENDATIONS = ("KEEP", "NARROW", "PAUSE", "REJECT", "NEEDS_DATA")
ROBUST_CLASSIFICATION = "ROBUST_ENOUGH_TO_OBSERVE"

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "expanded_search_narrowing_allowed": True,
    "observation_profile_recommendation_allowed": True,
    "trade_recommendation_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "broker_execution_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_promotion_authorized": False,
    "production_promotion_authorized": False,
}

GUARDRAILS = [
    "Manual research narrowing only.",
    "No trading authority.",
    "No capital allocation authority.",
    "No broker execution authority.",
    "No position-sizing authority.",
    "No automatic paper placement or candidate production promotion authority.",
]


def run_expanded_search_narrowing(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_expanded_search_narrowing_report(root=root, created_at=created_at)
    write_expanded_search_narrowing_report(report, root=root)
    return report


def build_expanded_search_narrowing_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    row_evidence = _row_evidence(sources)
    failure_reasons = _failure_reasons(sources)
    rows: list[dict[str, Any]] = []
    for dimension in DIMENSIONS:
        values = _dimension_values(dimension, sources, row_evidence)
        for value in sorted(values):
            row = _evaluate_value(dimension, value, sources, row_evidence, failure_reasons)
            rows.append(row)
    profile = _focused_profile(rows)
    summary = _summary(rows, profile, sources)
    report = {
        "schema_id": "atlas_v2_research_os_expanded_search_narrowing_v1",
        "schema_version": "1.0",
        "report_type": "EXPANDED_SEARCH_NARROWING_RECOMMENDATION",
        "created_at": created,
        "day": created[:10],
        "source_reports": {
            name: {"path": source["path"], "exists": source["exists"], "report_type": source["payload"].get("report_type")}
            for name, source in sources.items()
        },
        "summary": summary,
        "dimension_reviews": rows,
        "recommended_next_search_profile": profile,
        "methodology": {
            "positive_replay_rate": "Uses expansion report rates when present; otherwise estimates from replay-positive candidate rows.",
            "backtest_supported_rate": "Counts backtest-supported examples and winner rows against all observed rows for the dimension value.",
            "final_eligibility_rate": "Uses explicit eligible counts by dimension where available, with candidate-row fallback.",
            "robust_family_rate": "Counts family robustness classifications among family reviews carrying the dimension value.",
            "data_blocked_interpretation": "Backtest support without final eligibility or robust-family confirmation is narrowed rather than promoted.",
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": list(GUARDRAILS),
    }
    _validate_authority(report)
    return report


def write_expanded_search_narrowing_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    out_dir = root_path / str(report.get("day"))
    out_dir.mkdir(parents=True, exist_ok=True)
    root_path.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "expanded_search_narrowing_report.json"
    summary_path = out_dir / "expanded_search_narrowing_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_expanded_search_narrowing_summary(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_expanded_search_narrowing_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    profile = report.get("recommended_next_search_profile", {})
    rows = report.get("dimension_reviews") or []
    by_rec: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        by_rec[str(row.get("recommendation"))].append(f"{row.get('dimension')}={row.get('value')}")
    lines = [
        "# Expanded Search Narrowing Recommendation",
        "",
        f"Created: {report.get('created_at')}",
        f"Profile: {profile.get('profile_id')}",
        f"Expanded search remains useful: {summary.get('expanded_search_remains_useful')}",
        "",
        "## Recommendation Counts",
        "",
    ]
    for recommendation in RECOMMENDATIONS:
        lines.append(f"- {recommendation}: {summary.get('recommendation_counts', {}).get(recommendation, 0)}")
    lines.extend(
        [
            "",
            "## Keep",
            "",
            f"- {', '.join(by_rec.get('KEEP', [])[:20]) or 'none'}",
            "",
            "## Narrow",
            "",
            f"- {', '.join(by_rec.get('NARROW', [])[:20]) or 'none'}",
            "",
            "## Pause Or Reject",
            "",
            f"- Pause: {', '.join(by_rec.get('PAUSE', [])[:20]) or 'none'}",
            f"- Reject: {', '.join(by_rec.get('REJECT', [])[:20]) or 'none'}",
            "",
            "## Focused Profile",
            "",
        ]
    )
    for dimension, values in (profile.get("dimensions") or {}).items():
        lines.append(f"- {dimension}: {', '.join(values) or 'none'}")
    lines.extend(
        [
            "",
            "## Authority",
            "",
            "- Research-only narrowing recommendation.",
            "- No trade, broker, capital, position-sizing, automatic paper-placement, or production-promotion authority.",
            "",
        ]
    )
    return "\n".join(lines)


def _evaluate_value(
    dimension: str,
    value: str,
    sources: dict[str, dict[str, Any]],
    row_evidence: dict[str, list[dict[str, Any]]],
    failure_reasons: dict[str, str],
) -> dict[str, Any]:
    hypotheses = _hypotheses_generated(dimension, value, sources)
    positive_rate = _positive_replay_rate(dimension, value, sources, row_evidence)
    backtest_rate = _backtest_supported_rate(dimension, value, row_evidence)
    final_rate = _final_eligibility_rate(dimension, value, sources, row_evidence)
    robust_rate = _robust_family_rate(dimension, value, sources)
    observed_rows = [row for row in row_evidence["all_candidates"] if value in _candidate_values(row, dimension)]
    robust_samples = _family_sample_count(dimension, value, sources)
    failure_cause = _failure_cause(dimension, value, failure_reasons, sources)
    recommendation, rationale = _recommendation(
        hypotheses=hypotheses,
        positive_rate=positive_rate,
        backtest_rate=backtest_rate,
        final_rate=final_rate,
        robust_rate=robust_rate,
        observed_count=len(observed_rows),
        robust_samples=robust_samples,
        failure_cause=failure_cause,
    )
    return {
        "dimension": dimension,
        "value": value,
        "hypotheses_generated": hypotheses,
        "positive_replay_rate": round(positive_rate, 6),
        "backtest_supported_rate": round(backtest_rate, 6),
        "final_eligibility_rate": round(final_rate, 6),
        "robust_family_rate": round(robust_rate, 6),
        "observed_candidate_rows": len(observed_rows),
        "robust_family_sample_size": robust_samples,
        "failure_cause": failure_cause,
        "recommendation": recommendation,
        "recommendation_rationale": rationale,
        "research_only": True,
    }


def _recommendation(
    *,
    hypotheses: int,
    positive_rate: float,
    backtest_rate: float,
    final_rate: float,
    robust_rate: float,
    observed_count: int,
    robust_samples: int,
    failure_cause: str,
) -> tuple[str, str]:
    evidence_count = max(hypotheses, observed_count, robust_samples)
    if evidence_count == 0:
        return "NEEDS_DATA", "No usable expansion, candidate, or robust-family evidence was available for this value."
    if robust_rate >= 0.25 and (final_rate > 0 or backtest_rate >= 0.25 or positive_rate >= 0.45):
        return "KEEP", "Robust-family evidence is present and at least one replay/backtest/eligibility signal supports continued observation."
    if final_rate >= 0.25 and (backtest_rate >= 0.25 or positive_rate >= 0.45):
        return "KEEP", "Final eligibility plus replay or backtest support is strong enough to keep this value in the focused profile."
    if positive_rate <= 0.1 and backtest_rate == 0 and final_rate == 0 and robust_rate == 0 and evidence_count >= 3:
        return "REJECT", "Enough evidence exists and it did not produce replay, eligibility, backtest, or robust-family support."
    if final_rate == 0 and robust_rate == 0 and ("INSUFFICIENT_DATA" in failure_cause or "direct data" in failure_cause.lower()):
        return "NEEDS_DATA", "Evidence is blocked by missing direct data or insufficient-data classifications."
    if positive_rate >= 0.45 or backtest_rate >= 0.2 or final_rate > 0:
        return "NARROW", "The value has some search signal but lacks enough final eligibility or robust-family confirmation."
    return "PAUSE", "Current evidence is weak or unfocused; keep out of the next focused profile unless new observations change it."


def _focused_profile(rows: list[dict[str, Any]]) -> dict[str, Any]:
    dimensions: dict[str, list[str]] = {}
    excluded: dict[str, list[str]] = {}
    for dimension in DIMENSIONS:
        candidates = [row for row in rows if row["dimension"] == dimension and row["recommendation"] in {"KEEP", "NARROW"}]
        candidates.sort(
            key=lambda row: (
                row["recommendation"] == "KEEP",
                row["robust_family_rate"],
                row["final_eligibility_rate"],
                row["backtest_supported_rate"],
                row["positive_replay_rate"],
                row["hypotheses_generated"],
            ),
            reverse=True,
        )
        dimensions[dimension] = [row["value"] for row in candidates[:5]]
        excluded[dimension] = [row["value"] for row in rows if row["dimension"] == dimension and row["recommendation"] not in {"KEEP", "NARROW"}]
    return {
        "profile_id": PROFILE_ID,
        "profile_type": "FOCUSED_EXPANDED_OBSERVATION_TRIAL",
        "dimensions": dimensions,
        "excluded_or_paused_dimensions": excluded,
        "selection_policy": "Include KEEP and strongest NARROW values only; keep data-blocked values out of confidence-increase paths until direct data exists.",
        "guardrails": list(GUARDRAILS),
    }


def _summary(rows: list[dict[str, Any]], profile: dict[str, Any], sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["recommendation"] for row in rows)
    comparison = sources["expanded_search_trial"]["payload"].get("comparison_against_prior_baseline") or {}
    useful = bool(
        comparison.get("expansion_increased_diversity")
        or comparison.get("new_backtest_supported_candidates", 0)
        or any(profile.get("dimensions", {}).get(dimension) for dimension in DIMENSIONS)
    )
    return {
        "dimensions_evaluated": len(DIMENSIONS),
        "values_evaluated": len(rows),
        "recommendation_counts": {recommendation: counts.get(recommendation, 0) for recommendation in RECOMMENDATIONS},
        "dimensions_to_keep": sorted({row["dimension"] for row in rows if row["recommendation"] == "KEEP"}),
        "dimensions_to_narrow": sorted({row["dimension"] for row in rows if row["recommendation"] == "NARROW"}),
        "dimensions_to_pause_or_reject": sorted({row["dimension"] for row in rows if row["recommendation"] in {"PAUSE", "REJECT"}}),
        "expanded_search_remains_useful": useful,
        "profile_id": PROFILE_ID,
        "quality_assessment": comparison.get("quality_assessment") or comparison.get("quality_vs_baseline") or "",
        "authority": "RESEARCH_ONLY_NO_TRADING_CAPITAL_BROKER_POSITION_SIZING_OR_AUTOMATIC_PAPER_AUTHORITY",
    }


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    names = [
        "expanded_search_trial",
        "expanded_search_gate_audit",
        "regime_expansion",
        "market_structure_expansion",
        "session_context_expansion",
        "winner_pattern_extraction",
        "candidate_failure_patterns",
        "family_robustness_review",
    ]
    return {name: _load_latest(root, name) for name in names}


def _load_latest(root: Path, name: str) -> dict[str, Any]:
    path = root / name / "latest.json"
    if not path.exists():
        return {"path": str(path), "exists": False, "payload": {}}
    return {"path": str(path), "exists": True, "payload": json.loads(path.read_text(encoding="utf-8"))}


def _row_evidence(sources: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    expanded = sources["expanded_search_trial"]["payload"]
    final = expanded.get("final_qualification") or {}
    winner = sources["winner_pattern_extraction"]["payload"]
    rows: list[dict[str, Any]] = []
    for row in final.get("backtest_supported_examples") or []:
        rows.append(dict(row) | {"_supported": True, "_eligible": bool(row.get("paper_forward_ready"))})
    for row in final.get("eligible_examples") or []:
        rows.append(dict(row) | {"_supported": True, "_eligible": True})
    for row in winner.get("top_20_candidates") or []:
        rows.append(dict(row) | {"_supported": True, "_eligible": True})
    for row in winner.get("top_8_campaign_candidates") or []:
        rows.append(dict(row) | {"_supported": True, "_eligible": True})
    return {"all_candidates": rows}


def _dimension_values(dimension: str, sources: dict[str, dict[str, Any]], row_evidence: dict[str, list[dict[str, Any]]]) -> set[str]:
    values: set[str] = set()
    if dimension == "regime":
        metrics = sources["regime_expansion"]["payload"].get("metrics") or {}
        values.update(_dict_keys(metrics, "hypotheses_by_regime"))
        values.update(_dict_keys(metrics, "positive_replay_rate_by_regime"))
    if dimension == "market_structure":
        metrics = sources["market_structure_expansion"]["payload"].get("required_metrics") or {}
        values.update(_dict_keys(metrics, "hypotheses_by_structure"))
        values.update(_dict_keys(metrics, "positive_replay_rate_by_structure"))
    if dimension == "session_context":
        metrics = sources["session_context_expansion"]["payload"].get("metrics") or {}
        values.update(_dict_keys(metrics, "hypotheses_by_session"))
        values.update(str(value) for value in sources["session_context_expansion"]["payload"].get("session_contexts") or [])
    for row in row_evidence["all_candidates"]:
        values.update(_candidate_values(row, dimension))
    for family in sources["family_robustness_review"]["payload"].get("family_reviews") or []:
        values.update(_family_values(family, dimension))
    failure_dist = sources["candidate_failure_patterns"]["payload"].get("preview_rejected_distributions") or {}
    failure_key = {
        "regime": "regimes",
        "mechanism": "mechanisms",
        "timeframe": "timeframes",
        "source_type": "source_types",
    }.get(dimension)
    if failure_key:
        values.update(str(value).upper() for value in (failure_dist.get(failure_key) or {}).keys())
    return {value for value in values if value}


def _hypotheses_generated(dimension: str, value: str, sources: dict[str, dict[str, Any]]) -> int:
    lookup = {
        "regime": (sources["regime_expansion"]["payload"].get("metrics") or {}, "hypotheses_by_regime"),
        "market_structure": (sources["market_structure_expansion"]["payload"].get("required_metrics") or {}, "hypotheses_by_structure"),
        "session_context": (sources["session_context_expansion"]["payload"].get("metrics") or {}, "hypotheses_by_session"),
    }
    if dimension in lookup:
        metrics, key = lookup[dimension]
        return int(_metric_number(metrics.get(key), value))
    return 0


def _positive_replay_rate(dimension: str, value: str, sources: dict[str, dict[str, Any]], row_evidence: dict[str, list[dict[str, Any]]]) -> float:
    lookup = {
        "regime": (sources["regime_expansion"]["payload"].get("metrics") or {}, "positive_replay_rate_by_regime"),
        "market_structure": (sources["market_structure_expansion"]["payload"].get("required_metrics") or {}, "positive_replay_rate_by_structure"),
    }
    if dimension in lookup:
        metrics, key = lookup[dimension]
        found = _metric_number(metrics.get(key), value, nested_key="positive_replay_rate", default=None)
        if found is not None:
            return float(found)
    rows = [row for row in row_evidence["all_candidates"] if value in _candidate_values(row, dimension)]
    if not rows:
        return 0.0
    return sum(1 for row in rows if float(row.get("replay_score") or 0) > 0) / len(rows)


def _backtest_supported_rate(dimension: str, value: str, row_evidence: dict[str, list[dict[str, Any]]]) -> float:
    rows = [row for row in row_evidence["all_candidates"] if value in _candidate_values(row, dimension)]
    if not rows:
        return 0.0
    supported = sum(1 for row in rows if row.get("_supported") or row.get("backtest_classification") == "BACKTEST_SUPPORTED" or float(row.get("profit_factor") or 0) > 1)
    return supported / len(rows)


def _final_eligibility_rate(dimension: str, value: str, sources: dict[str, dict[str, Any]], row_evidence: dict[str, list[dict[str, Any]]]) -> float:
    lookup = {
        "regime": (sources["regime_expansion"]["payload"].get("metrics") or {}, "eligible_candidates_by_regime", "hypotheses_by_regime"),
        "market_structure": (sources["market_structure_expansion"]["payload"].get("required_metrics") or {}, "eligible_candidates_by_structure", "hypotheses_by_structure"),
        "session_context": (sources["session_context_expansion"]["payload"].get("metrics") or {}, "eligible_candidates_by_session", "hypotheses_by_session"),
    }
    if dimension in lookup:
        metrics, eligible_key, hypo_key = lookup[dimension]
        eligible = _metric_number(metrics.get(eligible_key), value)
        hypotheses = _metric_number(metrics.get(hypo_key), value)
        if hypotheses:
            return min(1.0, float(eligible) / float(hypotheses))
    rows = [row for row in row_evidence["all_candidates"] if value in _candidate_values(row, dimension)]
    if not rows:
        return 0.0
    return sum(1 for row in rows if row.get("_eligible")) / len(rows)


def _robust_family_rate(dimension: str, value: str, sources: dict[str, dict[str, Any]]) -> float:
    families = [family for family in sources["family_robustness_review"]["payload"].get("family_reviews") or [] if value in _family_values(family, dimension)]
    if not families:
        return 0.0
    return sum(1 for family in families if str(family.get("classification")) == ROBUST_CLASSIFICATION) / len(families)


def _family_sample_count(dimension: str, value: str, sources: dict[str, dict[str, Any]]) -> int:
    return sum(1 for family in sources["family_robustness_review"]["payload"].get("family_reviews") or [] if value in _family_values(family, dimension))


def _failure_cause(dimension: str, value: str, failure_reasons: dict[str, str], sources: dict[str, dict[str, Any]]) -> str:
    failure_dist = sources["candidate_failure_patterns"]["payload"].get("preview_rejected_distributions") or {}
    key = {"regime": "regimes", "mechanism": "mechanisms", "timeframe": "timeframes", "source_type": "source_types"}.get(dimension)
    if key and str(value).upper() in {str(k).upper() for k in (failure_dist.get(key) or {}).keys()}:
        return failure_reasons.get("primary", "dimension appears in rejected preview sample")
    if dimension == "market_structure":
        failures = sources["market_structure_expansion"]["payload"].get("required_metrics", {}).get("top_structures_among_failures") or []
        for row in failures:
            if _norm(row.get("market_structure")) == value:
                return f"appears among top market-structure failures: {row.get('count', 0)} rejected/suppressed examples"
    return "no dimension-specific failure attribution available"


def _failure_reasons(sources: dict[str, dict[str, Any]]) -> dict[str, str]:
    reasons = sources["candidate_failure_patterns"]["payload"].get("aggregate_main_disqualification_reasons") or {}
    if not reasons:
        preview = sources["candidate_failure_patterns"]["payload"].get("preview_rejected_distributions", {}).get("reasons") or {}
        reasons = preview
    if not reasons:
        return {"primary": "no failure-pattern report available"}
    primary = max(reasons.items(), key=lambda item: item[1])[0]
    return {"primary": str(primary)}


def _candidate_values(row: dict[str, Any], dimension: str) -> set[str]:
    if dimension == "regime":
        return {_norm(row.get("regime"))}
    if dimension == "market_structure":
        return _norm_values(row.get("market_structures") or row.get("market_structure"))
    if dimension == "session_context":
        return _norm_values(row.get("session_contexts") or row.get("session_context"))
    if dimension == "mechanism":
        return {_norm(row.get("mechanism"))}
    if dimension == "timeframe":
        return _norm_values(row.get("timeframes") or row.get("timeframe"))
    if dimension == "source_type":
        return _norm_values(row.get("source_types") or row.get("source_type"))
    if dimension == "symbol/universe":
        return _norm_values(row.get("symbols") or row.get("symbol_or_universe") or row.get("symbol"))
    return set()


def _family_values(family: dict[str, Any], dimension: str) -> set[str]:
    parts = [_norm(part) for part in str(family.get("family_name") or "").split("/") if part.strip()]
    if dimension == "mechanism" and len(parts) >= 1:
        return {parts[0]}
    if dimension == "regime" and len(parts) >= 2:
        return {parts[1]}
    if dimension == "timeframe":
        values = _norm_values((family.get("timeframe_concentration") or {}).get("timeframes"))
        return values or ({parts[2]} if len(parts) >= 3 else set())
    if dimension == "source_type":
        values = _norm_values((family.get("source_type_dependence") or {}).get("source_types"))
        return values or ({parts[3]} if len(parts) >= 4 else set())
    if dimension == "symbol/universe":
        return _norm_values((family.get("symbol_universe_concentration") or {}).get("symbols"))
    return set()


def _dict_keys(payload: dict[str, Any], key: str) -> set[str]:
    values = payload.get(key) or {}
    if isinstance(values, dict):
        return {_norm(value) for value in values.keys()}
    return set()


def _metric_number(metrics: Any, value: str, *, nested_key: str | None = None, default: Any = 0) -> Any:
    if not isinstance(metrics, dict):
        return default
    match = None
    for key, raw in metrics.items():
        if _norm(key) == value:
            match = raw
            break
    if match is None:
        return default
    if nested_key and isinstance(match, dict):
        return match.get(nested_key, default)
    if isinstance(match, dict):
        for key in ("count", "value", "positive_replay_rate"):
            if key in match:
                return match[key]
        return default
    return match


def _norm_values(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        return {_norm(item) for item in value if _norm(item)}
    normalized = _norm(value)
    return {normalized} if normalized else set()


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _validate_authority(report: dict[str, Any]) -> None:
    authority = report.get("authority_boundary") or {}
    forbidden = [
        "trade_recommendation_authorized",
        "capital_authorized",
        "position_sizing_authorized",
        "broker_execution_authorized",
        "automatic_paper_trade_placement_authorized",
        "candidate_promotion_authorized",
        "production_promotion_authorized",
    ]
    if not authority.get("research_only"):
        raise ValueError("expanded search narrowing must remain research-only")
    for field in forbidden:
        if authority.get(field) is not False:
            raise ValueError(f"forbidden authority must remain false: {field}")
    invalid = [row for row in report.get("dimension_reviews", []) if row.get("recommendation") not in RECOMMENDATIONS]
    if invalid:
        raise ValueError("invalid recommendation in expanded search narrowing report")


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
