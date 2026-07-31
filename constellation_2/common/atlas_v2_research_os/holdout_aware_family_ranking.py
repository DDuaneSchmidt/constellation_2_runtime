from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_family_discovery import AUTHORITY_BOUNDARY

REPORT_DIRNAME = "holdout_aware_family_ranking"

HOLDOUT_SURVIVED = "HOLDOUT_SURVIVED"
HOLDOUT_WEAKENED = "HOLDOUT_WEAKENED"
HOLDOUT_FAILED = "HOLDOUT_FAILED"
INSUFFICIENT_HOLDOUT_DATA = "INSUFFICIENT_HOLDOUT_DATA"
DATA_BLOCKED = "DATA_BLOCKED"

BLOCKED_STATUSES = {INSUFFICIENT_HOLDOUT_DATA, DATA_BLOCKED}


def run_holdout_aware_family_ranking(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_holdout_aware_family_ranking(root=root, created_at=created_at)
    write_holdout_aware_family_ranking(report, root=root)
    return report


def build_holdout_aware_family_ranking(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    sources = _load_sources(root_path)
    family_report = sources["candidate_family_discovery"]["payload"]
    families = list(family_report.get("families") or [])
    if not families:
        families = list(sources["family_robustness_review"]["payload"].get("family_reviews") or [])

    robustness_by_id = _by_family_id(sources["family_robustness_review"]["payload"].get("family_reviews") or [])
    edge_by_id = _by_family_id(sources["edge_magnitude_estimation"]["payload"].get("families") or [])
    overfit_by_id = _by_family_id(sources["search_overfit_guardrail"]["payload"].get("candidate_family_reviews") or [])
    holdout_by_id = _holdout_by_family_id(sources["holdout_replay_validation"]["payload"])
    relevance = sources["portfolio_relevance_estimate"]["payload"]

    ranked = [_rank_family(row, index, sources, robustness_by_id, edge_by_id, overfit_by_id, holdout_by_id) for index, row in enumerate(families, start=1)]
    ranked.sort(key=_sort_key)
    _assign_new_ranks(ranked)

    worth_observing = [row for row in ranked if row["new_rank"] != "BLOCKED" and row["holdout_status"] in {HOLDOUT_SURVIVED, HOLDOUT_WEAKENED}]
    downgraded = [row for row in ranked if row["rank_direction"] == "DOWNGRADED"]
    direct_data_required = [row for row in ranked if row["requires_direct_data_before_confidence_increase"]]
    blocked = [row for row in ranked if row["new_rank"] == "BLOCKED"]
    upgraded = [row for row in ranked if row["rank_direction"] == "UPGRADED"]

    return {
        "schema_id": "atlas_v2_research_os_holdout_aware_family_ranking",
        "schema_version": "1.0",
        "report_type": "HOLDOUT_AWARE_FAMILY_RANKING",
        "created_at": created,
        "day": created[:10],
        "summary": {
            "families_ranked": len(ranked),
            "families_upgraded": len(upgraded),
            "families_downgraded": len(downgraded),
            "families_blocked": len(blocked),
            "families_worth_observing": len(worth_observing),
            "families_requiring_direct_data_before_confidence_increase": len(direct_data_required),
            "missing_required_input_count": sum(not source["exists"] for source in sources.values()),
            "confidence_increase_from_search_selection_allowed": False,
            "authority": "RESEARCH_ONLY_NO_TRADING_CAPITAL_BROKER_POSITION_SIZING",
        },
        "required_inputs": {
            name: {"path": source["path"], "exists": source["exists"], "report_type": source["payload"].get("report_type")}
            for name, source in sources.items()
        },
        "ranking_policy": {
            "holdout_survived": "May improve rank only when direct data is not blocked and proxy/duplicate/sample risks do not dominate.",
            "holdout_weakened": "Cannot improve rank; may hold or downgrade depending on degradation and risk.",
            "holdout_failed": "Must be downgraded.",
            "insufficient_holdout_data": "Blocked from observation-priority ranking.",
            "data_blocked": "Blocked from confidence increase and observation-priority ranking.",
            "selection_guardrail": "No confidence increase from search selection alone.",
        },
        "ranked_families": ranked,
        "required_conclusion": {
            "which_families_remain_worth_observing": _brief_rows(worth_observing),
            "which_families_are_downgraded": _brief_rows(downgraded),
            "which_families_require_direct_data_before_any_confidence_increase": _brief_rows(direct_data_required),
            "portfolio_relevance_context": (relevance.get("required_conclusion") or {}).get("could_this_eventually_matter_for_portfolio_returns"),
        },
        "authority_boundary": {
            **dict(AUTHORITY_BOUNDARY),
            "research_only": True,
            "capital_authorized": False,
            "broker_execution_authorized": False,
            "position_sizing_authorized": False,
            "trade_recommendation_authorized": False,
        },
        "guardrails": [
            "Research-only.",
            "No trading authority.",
            "No capital authority.",
            "No broker authority.",
            "No position-sizing authority.",
            "No confidence increase from selection alone.",
        ],
    }


def write_holdout_aware_family_ranking(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    report_root = Path(root) / REPORT_DIRNAME
    day_root = report_root / str(report.get("day") or _today())
    day_root.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_holdout_aware_family_ranking_summary(report)
    dated_json = day_root / "holdout_aware_family_ranking_report.json"
    dated_summary = day_root / "holdout_aware_family_ranking_summary.md"
    latest_json = report_root / "latest.json"
    latest_summary = report_root / "latest_summary.md"
    for path in (dated_json, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (dated_summary, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": dated_json, "summary": dated_summary, "latest_json": latest_json, "latest_summary": latest_summary}


def render_holdout_aware_family_ranking_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    conclusion = report.get("required_conclusion") or {}
    rows = report.get("ranked_families") or []
    table_rows = [
        f"| {row.get('family_id')} | {row.get('old_rank')} | {row.get('new_rank')} | {row.get('holdout_status')} | {row.get('direct_data_status')} | {row.get('rank_change_reason')} |"
        for row in rows[:30]
    ]
    if not table_rows:
        table_rows = ["| none | n/a | n/a | n/a | n/a | n/a |"]
    return "\n".join(
        [
            "# Holdout-Aware Family Ranking",
            "",
            f"Created: {report.get('created_at')}",
            "",
            "## Summary",
            "",
            f"- Families ranked: {summary.get('families_ranked')}",
            f"- Families upgraded: {summary.get('families_upgraded')}",
            f"- Families downgraded: {summary.get('families_downgraded')}",
            f"- Families blocked: {summary.get('families_blocked')}",
            f"- Worth observing: {summary.get('families_worth_observing')}",
            f"- Missing required inputs: {summary.get('missing_required_input_count')}",
            "",
            "## Ranking",
            "",
            "| family_id | old_rank | new_rank | holdout_status | direct_data_status | reason |",
            "| --- | ---: | --- | --- | --- | --- |",
            *table_rows,
            "",
            "## Required Conclusion",
            "",
            f"- Which families remain worth observing? {_family_names(conclusion.get('which_families_remain_worth_observing') or [])}",
            f"- Which families are downgraded? {_family_names(conclusion.get('which_families_are_downgraded') or [])}",
            f"- Which families require direct data before any confidence increase? {_family_names(conclusion.get('which_families_require_direct_data_before_any_confidence_increase') or [])}",
            "",
            "## Guardrails",
            "",
            "- Research-only. No trading, capital, broker, position-sizing, trade-recommendation, or portfolio-construction authority.",
            "- No confidence increase from search selection alone.",
            "",
        ]
    )


def _rank_family(
    family: dict[str, Any],
    fallback_rank: int,
    sources: dict[str, dict[str, Any]],
    robustness_by_id: dict[str, dict[str, Any]],
    edge_by_id: dict[str, dict[str, Any]],
    overfit_by_id: dict[str, dict[str, Any]],
    holdout_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    family_id = str(family.get("family_id") or "")
    robustness = robustness_by_id.get(family_id, {})
    edge = edge_by_id.get(family_id, {})
    overfit = overfit_by_id.get(family_id, {})
    holdout = holdout_by_id.get(family_id, {})
    old_rank = _int(family.get("best_rank")) or _int(robustness.get("best_rank")) or fallback_rank
    holdout_status = _holdout_status(holdout, sources["holdout_replay_validation"]["exists"])
    direct_data_status = _direct_data_status(family, edge, overfit)
    duplicate_risk = _risk_level((robustness.get("duplicate_risk") or {}).get("level") or overfit.get("duplicate_family_risk") or family.get("duplicate_or_distinct_assessment"))
    proxy_data_risk = _risk_level(edge.get("proxy_dependence") or overfit.get("proxy_data_risk") or (robustness.get("proxy_data_dependence") or {}).get("level"))
    sample_size = _float(edge.get("sample_size") if edge else None) or _sample_average(robustness) or _float(family.get("sample_size"))
    holdout_expectancy = _float(holdout.get("holdout_expectancy") or holdout.get("expectancy") or holdout.get("average_expectancy"))
    holdout_profit_factor = _float(holdout.get("holdout_profit_factor") or holdout.get("profit_factor") or holdout.get("average_profit_factor"))
    degradation = _degradation(holdout, family, edge)
    requires_direct_data = direct_data_status in {DATA_BLOCKED, "DIRECT_DATA_PARTIAL"} or proxy_data_risk in {"HIGH", "BLOCKING"}
    score = _score(old_rank, holdout_status, holdout_expectancy, holdout_profit_factor, degradation, sample_size, duplicate_risk, proxy_data_risk, direct_data_status)
    blocked = holdout_status in BLOCKED_STATUSES or direct_data_status == DATA_BLOCKED
    if holdout_status == HOLDOUT_FAILED:
        score -= 1000.0
    reason = _rank_change_reason(holdout_status, direct_data_status, duplicate_risk, proxy_data_risk, degradation, sources["holdout_replay_validation"]["exists"])
    return {
        "family_id": family_id,
        "family_name": family.get("family_name") or robustness.get("family_name") or edge.get("family_name") or overfit.get("family_name") or family_id,
        "old_rank": old_rank,
        "new_rank": "BLOCKED" if blocked else None,
        "prior_family_rank": old_rank,
        "holdout_status": holdout_status,
        "direct_data_status": direct_data_status,
        "holdout_classification": holdout_status,
        "holdout_expectancy": holdout_expectancy,
        "holdout_profit_factor": holdout_profit_factor,
        "performance_degradation": degradation,
        "sample_size": sample_size,
        "duplicate_risk": duplicate_risk,
        "proxy_data_risk": proxy_data_risk,
        "ranking_score": round(score, 6),
        "rank_change_reason": reason,
        "next_required_evidence": _next_required_evidence(holdout_status, direct_data_status, duplicate_risk, proxy_data_risk),
        "requires_direct_data_before_confidence_increase": requires_direct_data,
        "confidence_increase_from_search_selection_allowed": False,
        "rank_direction": "BLOCKED" if blocked else "PENDING_ASSIGNMENT",
    }


def _score(old_rank: int, holdout_status: str, holdout_expectancy: float, holdout_profit_factor: float, degradation: float | None, sample_size: float, duplicate_risk: str, proxy_data_risk: str, direct_data_status: str) -> float:
    score = max(0.0, 1000.0 - float(old_rank))
    if holdout_status == HOLDOUT_SURVIVED:
        score += 60.0
    elif holdout_status == HOLDOUT_WEAKENED:
        score -= 60.0
    elif holdout_status == HOLDOUT_FAILED:
        score -= 250.0
    if holdout_expectancy > 0:
        score += min(40.0, holdout_expectancy * 10000.0)
    if holdout_profit_factor >= 1.25:
        score += min(40.0, (holdout_profit_factor - 1.0) * 40.0)
    if degradation is not None:
        score -= max(0.0, degradation) * 100.0
    if sample_size < 30:
        score -= 80.0
    elif sample_size < 100:
        score -= 25.0
    if duplicate_risk in {"HIGH", "REPEATED"}:
        score -= 75.0
    elif duplicate_risk in {"MODERATE", "RELATED"}:
        score -= 35.0
    if proxy_data_risk in {"HIGH", "BLOCKING"}:
        score -= 90.0
    if direct_data_status == DATA_BLOCKED:
        score -= 200.0
    elif direct_data_status == "DIRECT_DATA_PARTIAL":
        score -= 50.0
    return score


def _assign_new_ranks(rows: list[dict[str, Any]]) -> None:
    eligible = [row for row in rows if row["new_rank"] != "BLOCKED"]
    for rank, row in enumerate(eligible, start=1):
        row["new_rank"] = rank
        if row["holdout_status"] == HOLDOUT_WEAKENED and rank < int(row["old_rank"]):
            row["new_rank"] = row["old_rank"]
        if row["holdout_status"] == HOLDOUT_FAILED and rank <= int(row["old_rank"]):
            row["new_rank"] = int(row["old_rank"]) + 10
        row["rank_direction"] = _rank_direction(int(row["old_rank"]), row["new_rank"])


def _sort_key(row: dict[str, Any]) -> tuple[int, float, int]:
    blocked = 1 if row["new_rank"] == "BLOCKED" else 0
    return (blocked, -float(row["ranking_score"]), int(row["old_rank"]))


def _rank_direction(old_rank: int, new_rank: int | str) -> str:
    if new_rank == "BLOCKED":
        return "BLOCKED"
    if int(new_rank) < old_rank:
        return "UPGRADED"
    if int(new_rank) > old_rank:
        return "DOWNGRADED"
    return "UNCHANGED"


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "holdout_replay_validation": root / "holdout_replay_validation" / "latest.json",
        "search_overfit_guardrail": root / "search_overfit_guardrail" / "latest.json",
        "candidate_family_discovery": root / "candidate_family_discovery" / "latest.json",
        "family_robustness_review": root / "family_robustness_review" / "latest.json",
        "edge_magnitude_estimation": root / "edge_magnitude_estimation" / "latest.json",
        "portfolio_relevance_estimate": root / "portfolio_relevance_estimate" / "latest.json",
    }
    return {name: {"path": str(path), "exists": path.exists(), "payload": _read_json(path, {})} for name, path in paths.items()}


def _holdout_by_family_id(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("family_holdout_results") or payload.get("holdout_family_results") or payload.get("families") or payload.get("family_results") or []
    by_id = _by_family_id(rows)
    summary = payload.get("summary") or {}
    status_lists = {
        DATA_BLOCKED: summary.get("families_data_blocked") or payload.get("families_data_blocked") or [],
        HOLDOUT_FAILED: summary.get("families_failed") or payload.get("families_failed") or [],
        INSUFFICIENT_HOLDOUT_DATA: summary.get("families_insufficient_holdout_data") or payload.get("families_insufficient_holdout_data") or [],
        HOLDOUT_SURVIVED: summary.get("families_survived_holdout") or payload.get("families_survived_holdout") or [],
        HOLDOUT_WEAKENED: summary.get("families_weakened") or payload.get("families_weakened") or [],
    }
    for status, family_ids in status_lists.items():
        for family_id in family_ids:
            by_id.setdefault(str(family_id), {"family_id": str(family_id), "classification": status})
    return by_id


def _holdout_status(holdout: dict[str, Any], exists: bool) -> str:
    if not exists:
        return INSUFFICIENT_HOLDOUT_DATA
    raw = str(holdout.get("holdout_status") or holdout.get("classification") or holdout.get("status") or "").upper()
    if "SURVIVED" in raw or raw in {"PASS", "PASSED"}:
        return HOLDOUT_SURVIVED
    if "WEAKENED" in raw:
        return HOLDOUT_WEAKENED
    if "FAILED" in raw or raw in {"FAIL", "REJECTED"}:
        return HOLDOUT_FAILED
    if "DATA_BLOCKED" in raw:
        return DATA_BLOCKED
    return INSUFFICIENT_HOLDOUT_DATA


def _direct_data_status(family: dict[str, Any], edge: dict[str, Any], overfit: dict[str, Any]) -> str:
    raw = str(edge.get("direct_validation_status") or overfit.get("selection_funnel", {}).get("direct_validation_status") or family.get("direct_validation_status") or "").lower()
    if raw in {"confirmed", "validated", "direct_data_confirmed", "sufficient"}:
        return "DIRECT_DATA_CONFIRMED"
    if raw in {"mixed", "partial", "mostly_sufficient"}:
        return "DIRECT_DATA_PARTIAL"
    return DATA_BLOCKED


def _rank_change_reason(holdout_status: str, direct_data_status: str, duplicate_risk: str, proxy_data_risk: str, degradation: float | None, holdout_exists: bool) -> str:
    if not holdout_exists:
        return "Blocked because holdout_replay_validation/latest.json is missing; in-sample/search-selected evidence cannot create observation priority."
    if direct_data_status == DATA_BLOCKED:
        return "Blocked because direct candidate data is insufficient or missing."
    if holdout_status == HOLDOUT_SURVIVED:
        return "Holdout survived; rank may improve only after direct-data and risk penalties."
    if holdout_status == HOLDOUT_WEAKENED:
        return "Holdout weakened; rank cannot improve and degradation/risk penalties apply."
    if holdout_status == HOLDOUT_FAILED:
        return "Holdout failed; family is downgraded."
    if holdout_status == DATA_BLOCKED:
        return "Holdout validation is data-blocked; family remains blocked."
    parts = ["Insufficient holdout evidence."]
    if duplicate_risk in {"HIGH", "REPEATED"}:
        parts.append("Duplicate-family risk is high.")
    if proxy_data_risk in {"HIGH", "BLOCKING"}:
        parts.append("Proxy-data dependence blocks confidence increase.")
    if degradation is not None and degradation > 0:
        parts.append("Performance degradation is present.")
    return " ".join(parts)


def _next_required_evidence(holdout_status: str, direct_data_status: str, duplicate_risk: str, proxy_data_risk: str) -> list[str]:
    evidence = []
    if holdout_status in {INSUFFICIENT_HOLDOUT_DATA, DATA_BLOCKED}:
        evidence.append("holdout replay validation or confirmed forward observation")
    if direct_data_status != "DIRECT_DATA_CONFIRMED":
        evidence.append("direct candidate data validation")
    if duplicate_risk in {"HIGH", "REPEATED", "RELATED", "MODERATE"}:
        evidence.append("one-representative family deduplication evidence")
    if proxy_data_risk in {"HIGH", "BLOCKING"}:
        evidence.append("proxy/backtest mismatch resolution")
    if holdout_status == HOLDOUT_FAILED:
        evidence.append("new independent evidence before reconsideration")
    return evidence or ["continued forward observation evidence"]


def _degradation(holdout: dict[str, Any], family: dict[str, Any], edge: dict[str, Any]) -> float | None:
    raw = holdout.get("performance_degradation")
    if raw is not None:
        return _float(raw)
    holdout_exp = _float(holdout.get("holdout_expectancy") or holdout.get("expectancy") or holdout.get("average_expectancy"))
    prior_exp = _float(edge.get("average_expectancy") or family.get("average_expectancy"))
    if prior_exp > 0 and holdout_exp:
        return round(max(0.0, (prior_exp - holdout_exp) / abs(prior_exp)), 6)
    return None


def _risk_level(value: Any) -> str:
    raw = str(value or "").upper()
    if "BLOCK" in raw:
        return "BLOCKING"
    if "REPEATED" in raw:
        return "REPEATED"
    if "RELATED" in raw:
        return "RELATED"
    if "HIGH" in raw:
        return "HIGH"
    if "MODERATE" in raw or "MEDIUM" in raw:
        return "MODERATE"
    if "LOW" in raw or "DISTINCT" in raw:
        return "LOW"
    return "UNKNOWN"


def _brief_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "family_id": row.get("family_id"),
            "family_name": row.get("family_name"),
            "old_rank": row.get("old_rank"),
            "new_rank": row.get("new_rank"),
            "holdout_status": row.get("holdout_status"),
            "direct_data_status": row.get("direct_data_status"),
            "rank_change_reason": row.get("rank_change_reason"),
            "next_required_evidence": row.get("next_required_evidence"),
        }
        for row in rows
    ]


def _family_names(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "none"
    return ", ".join(str(row.get("family_name") or row.get("family_id")) for row in rows)


def _by_family_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("family_id")): row for row in rows if row.get("family_id")}


def _sample_average(robustness: dict[str, Any]) -> float:
    summary = (robustness.get("sample_size_adequacy") or {}).get("summary") or {}
    return _float(summary.get("average"))


def _read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
