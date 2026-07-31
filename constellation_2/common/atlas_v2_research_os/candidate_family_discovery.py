from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .artifact_store import DEFAULT_STORE_ROOT
from .session_context import SESSION_CONTEXTS, normalize_session_context, session_context_from_row, session_distribution
from .regime_expansion import eligible_candidates_by_regime, family_count_by_regime, normalize_regime

REPORT_ROOT_NAME = "candidate_family_discovery"

AUTHORITY_BOUNDARY = {
    "paper_forward_observation_only": True,
    "research_only": True,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "candidate_production_promotion_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "portfolio_construction_authorized": False,
}

FAMILY_CLASSIFICATIONS = {
    "HIGH_PRIORITY_FAMILY",
    "PROMISING_FAMILY",
    "DUPLICATIVE_FAMILY",
    "WEAK_FAMILY",
    "INSUFFICIENT_DATA_FAMILY",
}


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_candidate_family_discovery(root: Path | str = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_candidate_family_discovery_report(root=Path(root), created_at=created_at)
    write_candidate_family_discovery_report(report, root=Path(root))
    return report


def build_candidate_family_discovery_report(root: Path | str = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or now_utc()
    sources = _load_sources(root_path)
    final_ranking = sources["final_candidate_ranking"]["payload"]
    focused_campaign = sources["focused_observation_campaign"]["payload"]
    symbol_attribution = sources["candidate_symbol_attribution"]["payload"]
    backtest_qualification = sources["backtest_aware_final_qualification"]["payload"]
    winner_patterns = sources["winner_pattern_extraction"]["payload"]

    symbol_rows = _index_by_candidate_id(symbol_attribution.get("candidate_symbol_attributions") or [])
    campaign_rows = _index_by_candidate_id(focused_campaign.get("campaign_candidates") or [])
    winner_rows = _index_by_candidate_id((winner_patterns.get("top_20_candidates") or []) + (winner_patterns.get("top_8_campaign_candidates") or []))
    backtest_rows = _index_by_candidate_id((backtest_qualification.get("final_eligible_examples") or []) + (backtest_qualification.get("backtest_supported_examples") or []))

    candidates = _normalize_candidates(
        final_ranking,
        symbol_rows=symbol_rows,
        campaign_rows=campaign_rows,
        winner_rows=winner_rows,
        backtest_rows=backtest_rows,
    )
    eligible_detailed = [row for row in candidates if row.get("candidate_review_status") != "REJECTED"]
    rejected_detailed = [row for row in candidates if row.get("candidate_review_status") == "REJECTED"]
    families = _build_families(eligible_detailed)
    family_lookup = {family["family_id"]: family for family in families}

    top_8_ids = _ordered_ids(focused_campaign.get("campaign_candidates") or [], rank_key="campaign_rank")[:8]
    if not top_8_ids:
        top_8_ids = [row["candidate_id"] for row in sorted(candidates, key=lambda row: _rank(row))[:8]]
    top_20_ids = _ordered_ids(final_ranking.get("top_20_robust_candidates") or [], rank_key="rank")[:20]
    top_8 = [row for candidate_id in top_8_ids for row in eligible_detailed if row["candidate_id"] == candidate_id]
    top_20 = [row for candidate_id in top_20_ids for row in eligible_detailed if row["candidate_id"] == candidate_id]

    top_8_families = [_top_family_entry(row, family_lookup) for row in top_8]
    overrepresented = _overrepresented_families(families, len(candidates))
    near_duplicates = _near_duplicate_groups(families)
    distinct_families = [family for family in families if family["duplicate_or_distinct_assessment"] == "GENUINELY_DISTINCT"]
    observation_first = _observation_first_families(families)
    cohort_comparisons = _cohort_comparisons(winner_patterns, top_8=top_8, top_20=top_20, eligible_detailed=eligible_detailed, rejected_detailed=rejected_detailed)

    eligible_reported = _int((final_ranking.get("summary") or {}).get("ranked_eligible_candidates")) or _int(((winner_patterns.get("cohorts") or {}).get("all_110_eligible") or {}).get("count"))
    rejected_reported = _int(((winner_patterns.get("cohorts") or {}).get("rejected_candidates") or {}).get("count")) or _int((final_ranking.get("summary") or {}).get("classification_counts", {}).get("REJECT_FOR_NOW"))
    top_20_family_counts = Counter(row["family_id"] for row in top_20)

    session_metrics = _session_metrics(candidates, families)
    summary = {
        "eligible_candidates_reported_by_source": eligible_reported,
        "rejected_candidates_reported_by_source": rejected_reported,
        "detailed_candidate_rows_reviewed": len(candidates),
        "detailed_non_rejected_rows_reviewed": len(eligible_detailed),
        "top_8_campaign_candidates_reviewed": len(top_8),
        "top_20_ranked_candidates_reviewed": len(top_20),
        "unique_candidate_families": len(families),
        "top_8_family_count": len({entry["family_id"] for entry in top_8_families}),
        "top_20_family_count": len(top_20_family_counts),
        "overrepresented_family_count": len(overrepresented),
        "near_duplicate_group_count": len(near_duplicates),
        "genuinely_distinct_family_count": len(distinct_families),
        "paper_forward_observation_first_family_count": len(observation_first),
        "classification_counts": dict(sorted(Counter(family["classification"] for family in families).items())),
        "eligible_candidates_by_session": session_metrics["eligible_candidates_by_session"],
        "family_count_by_session": session_metrics["family_count_by_session"],
        "best_session_contexts": session_metrics["best_session_contexts"],
        "worst_session_contexts": session_metrics["worst_session_contexts"],
        "eligible_candidates_by_regime": eligible_candidates_by_regime(eligible_detailed),
        "family_count_by_regime": family_count_by_regime(families),
        "eligible_candidates_by_session": session_metrics["eligible_candidates_by_session"],
        "family_count_by_session": session_metrics["family_count_by_session"],
        "best_session_contexts": session_metrics["best_session_contexts"],
        "worst_session_contexts": session_metrics["worst_session_contexts"],
        "main_conclusion": _main_conclusion(len(families), eligible_reported, near_duplicates),
        "main_concern": "Candidate-level detail is available for the top 20 and a 100-row excluded sample; full 110/490 comparisons rely on verified aggregate cohort distributions.",
    }

    answers = {
        "how_many_unique_candidate_families_exist": len(families),
        "top_5_families": [_family_brief(family) for family in families[:5]],
        "which_families_contain_the_top_8": top_8_families,
        "which_families_dominate_the_top_20": [_family_brief(family_lookup[family_id]) | {"top_20_candidate_count": count} for family_id, count in top_20_family_counts.most_common()],
        "which_families_are_overrepresented": [_family_brief(family) for family in overrepresented],
        "which_candidates_are_duplicates_or_near_duplicates": near_duplicates,
        "which_families_appear_genuinely_distinct": [_family_brief(family) for family in distinct_families[:20]],
        "which_families_should_be_observed_first": [_family_brief(family) for family in observation_first],
        "which_families_should_be_paper_forward_observed_first": [_family_brief(family) for family in observation_first],
        "families_requiring_direct_data_validation": [_family_brief(family) for family in families if family.get("requires_direct_data_validation")][:20],
        "recommended_next_research_cycle": "Direct-data validation and forward observation should cover one representative per high-priority or duplicative top-20 family before spending effort on same-family variants.",
        "distinct_or_repeated_assessment": summary["main_conclusion"],
    }

    return {
        "report_type": "CANDIDATE_FAMILY_DISCOVERY",
        "schema_id": "atlas_v2_research_os_candidate_family_discovery",
        "schema_version": "2.0",
        "created_at": created,
        "day": created[:10],
        "summary": summary,
        "answers": answers,
        "required_comparisons": cohort_comparisons,
        "session_context_metrics": session_metrics,
        "families": families,
        "candidate_family_assignments": [
            {
                "candidate_id": row["candidate_id"],
                "rank": row.get("rank"),
                "campaign_rank": row.get("campaign_rank"),
                "family_id": row["family_id"],
                "near_duplicate_key": row["near_duplicate_key"],
                "session_context": row["session_context"],
                "candidate_review_status": row["candidate_review_status"],
                "paper_forward_observation_candidate": row["paper_forward_observation_candidate"],
                "market_structures": row.get("market_structures", ["UNKNOWN"]),
                "regime": row["regime"],
            }
            for row in candidates
        ],
        "source_reports": {name: {"path": source["path"], "exists": source["exists"], "report_type": source["payload"].get("report_type")} for name, source in sources.items()},
        "methodology": {
            "cluster_dimensions": [
                "mechanism",
                "regime",
                "timeframe",
                "source_type",
                "session_context",
                "symbol/universe overlap",
                "entry rule similarity",
                "exit rule similarity",
                "invalidation rule similarity",
                "replay score similarity",
                "backtest expectancy similarity",
                "profit factor similarity",
                "market structure",
                "penalty profile",
            ],
            "candidate_level_scope": "Top 20 ranked candidates plus the detailed excluded rows exposed by final_candidate_ranking/latest.json are clustered directly.",
            "cohort_level_scope": "All 110 eligible candidates and 490 rejected candidates are compared through winner_pattern_extraction aggregate distributions; missing candidate rows are not inferred.",
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Paper-forward observation only.",
            "Research-only candidate family analysis.",
            "No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, or candidate production promotion.",
        ],
    }


def write_candidate_family_discovery_report(report: dict[str, Any], root: Path | str = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root)
    report_root = root_path / REPORT_ROOT_NAME
    day_root = report_root / str(report.get("day") or now_utc()[:10])
    day_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    latest_json = report_root / "latest.json"
    latest_summary = report_root / "latest_summary.md"
    dated_json = day_root / "candidate_family_report.json"
    dated_summary = day_root / "candidate_family_summary.md"
    summary_text = render_candidate_family_summary(report)

    for output_path in (latest_json, dated_json):
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for output_path in (latest_summary, dated_summary):
        output_path.write_text(summary_text, encoding="utf-8")
    return {"latest_json": latest_json, "latest_summary": latest_summary, "dated_json": dated_json, "dated_summary": dated_summary}


def render_candidate_family_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    answers = report.get("answers") or {}
    comparisons = report.get("required_comparisons") or {}
    lines = [
        "# Candidate Family Discovery",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Human Summary",
        "",
        f"- Number of unique candidate families: {summary.get('unique_candidate_families', 0)}",
        f"- Candidate-level rows reviewed: {summary.get('detailed_candidate_rows_reviewed', 0)}",
        f"- Source-reported eligible candidates: {summary.get('eligible_candidates_reported_by_source', 0)}",
        f"- Source-reported rejected candidates: {summary.get('rejected_candidates_reported_by_source', 0)}",
        f"- Eligible candidates by session: {summary.get('eligible_candidates_by_session', {})}",
        f"- Family count by session: {summary.get('family_count_by_session', {})}",
        f"- Best session contexts: {summary.get('best_session_contexts', [])}",
        f"- Worst session contexts: {summary.get('worst_session_contexts', [])}",
        f"- Eligible candidates by regime: {summary.get('eligible_candidates_by_regime', {})}",
        f"- Family count by regime: {summary.get('family_count_by_regime', {})}",
        f"- Main concern: {summary.get('main_concern')}",
        f"- Assessment: {summary.get('main_conclusion')}",
        "",
        "## Top 5 Families",
        "",
    ]
    for family in answers.get("top_5_families") or []:
        lines.append(_family_line(family))
    lines.extend(["", "## Families Containing Top 8 Campaign Candidates", ""])
    for entry in answers.get("which_families_contain_the_top_8") or []:
        lines.append(f"- campaign rank {entry.get('campaign_rank') or entry.get('rank')}: {entry.get('candidate_id')} -> {entry.get('family_id')} ({entry.get('family_classification')})")
    lines.extend(["", "## Top 20 Dominance", ""])
    for family in answers.get("which_families_dominate_the_top_20") or []:
        lines.append(f"- {family.get('family_id')} | top20_count={family.get('top_20_candidate_count')} | {family.get('family_name')} | {family.get('classification')}")
    lines.extend(["", "## Duplicative Families", ""])
    duplicate_groups = answers.get("which_candidates_are_duplicates_or_near_duplicates") or []
    if duplicate_groups:
        for group in duplicate_groups[:20]:
            lines.append(f"- {group.get('family_id')} | {group.get('near_duplicate_key')}: {', '.join(group.get('candidate_ids', []))}")
    else:
        lines.append("- none")
    lines.extend(["", "## Best Paper-Forward Observation Families", ""])
    for family in answers.get("which_families_should_be_observed_first") or []:
        lines.append(_family_line(family))
    lines.extend(["", "## Families Requiring Direct-Data Validation", ""])
    for family in answers.get("families_requiring_direct_data_validation") or []:
        lines.append(_family_line(family))
    lines.extend([
        "",
        "## Required Cohort Comparisons",
        "",
        f"- Top 8 campaign candidates: {comparisons.get('top_8_campaign_candidates', {}).get('summary', 'not available')}",
        f"- Top 20 ranked candidates: {comparisons.get('top_20_ranked_candidates', {}).get('summary', 'not available')}",
        f"- All 110 eligible candidates: {comparisons.get('all_110_eligible_candidates', {}).get('summary', 'not available')}",
        f"- 490 rejected candidates: {comparisons.get('rejected_490_candidates', {}).get('summary', 'not available')}",
        "",
        "## Recommended Next Research Cycle",
        "",
        f"{answers.get('recommended_next_research_cycle')}",
        "",
        "## Guardrails",
        "",
        "- Research-only; paper-forward observation only.",
        "- No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, or production promotion authority.",
        "",
    ])
    return "\n".join(lines)


def _load_sources(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "final_candidate_ranking": root / "final_candidate_ranking" / "latest.json",
        "focused_observation_campaign": root / "focused_observation_campaign" / "latest.json",
        "backtest_aware_final_qualification": root / "backtest_aware_final_qualification" / "latest.json",
        "winner_pattern_extraction": root / "winner_pattern_extraction" / "latest.json",
        "candidate_symbol_attribution": root / "candidate_symbol_attribution" / "latest.json",
    }
    return {name: {"path": str(path), "exists": path.exists(), "payload": _read_json(path)} for name, path in paths.items()}


def _normalize_candidates(
    final_ranking: dict[str, Any],
    *,
    symbol_rows: dict[str, dict[str, Any]],
    campaign_rows: dict[str, dict[str, Any]],
    winner_rows: dict[str, dict[str, Any]],
    backtest_rows: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    raw_rows = list(final_ranking.get("top_20_robust_candidates") or []) + list(final_ranking.get("excluded_candidates") or [])
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_rows, start=1):
        candidate_id = str(raw.get("candidate_id") or "")
        if not candidate_id or candidate_id in seen:
            continue
        seen.add(candidate_id)
        merged: dict[str, Any] = {}
        for source_row in (backtest_rows.get(candidate_id, {}), winner_rows.get(candidate_id, {}), raw, symbol_rows.get(candidate_id, {}), campaign_rows.get(candidate_id, {})):
            merged.update({key: value for key, value in source_row.items() if value not in (None, [], {})})
        row = _candidate_features(merged, fallback_rank=index)
        row["near_duplicate_key"] = _near_duplicate_key(row)
        row["family_key"] = _family_key(row)
        row["family_id"] = "family_" + _hash(row["family_key"])[:16]
        rows.append(row)
    return rows


def _candidate_features(row: dict[str, Any], *, fallback_rank: int) -> dict[str, Any]:
    metrics = row.get("candidate_backtest", {}).get("metrics") if isinstance(row.get("candidate_backtest"), dict) else {}
    final_qualification = row.get("final_qualification") if isinstance(row.get("final_qualification"), dict) else {}
    historical_replay = row.get("historical_replay") if isinstance(row.get("historical_replay"), dict) else {}
    penalties = row.get("penalties") if isinstance(row.get("penalties"), dict) else {}
    if not penalties:
        penalties = {
            "intraday_daily_mismatch_penalty": row.get("intraday_daily_mismatch_penalty"),
            "missing_evidence_penalty": row.get("missing_evidence_penalty"),
            "negative_backtest_penalty": row.get("negative_backtest_penalty"),
            "proxy_data_penalty": row.get("proxy_penalty") if row.get("proxy_penalty") is not None else row.get("proxy_data_penalty"),
            "sample_size_penalty": row.get("sample_size_penalty"),
        }
    split = row.get("split_dimensions") if isinstance(row.get("split_dimensions"), dict) else {}
    classification = str(row.get("classification") or final_qualification.get("classification") or "")
    backtest_classification = str(row.get("backtest_classification") or row.get("classification") or "UNKNOWN")
    source_types = _string_list(row.get("candidate_source_types") or row.get("source_types") or row.get("source_type") or split.get("source_type"))
    market_structures = _string_list(row.get("candidate_market_structures") or row.get("market_structures") or row.get("market_structure") or split.get("market_structure"))
    symbols = _string_list(row.get("candidate_universe_symbols") or row.get("candidate_symbols") or row.get("symbols") or row.get("candidate_symbol") or split.get("symbol_group"))
    timeframes = _string_list(row.get("candidate_timeframes") or row.get("timeframes") or split.get("timeframe"))
    session_context = session_context_from_row(row)
    final_score = _float(row.get("final_score") if row.get("final_score") is not None else final_qualification.get("final_score"))
    expectancy = _float(row.get("expectancy") if row.get("expectancy") is not None else metrics.get("expectancy"))
    profit_factor = _float(row.get("profit_factor") if row.get("profit_factor") is not None else metrics.get("profit_factor"))
    replay_score = _float(row.get("replay_score") if row.get("replay_score") is not None else historical_replay.get("replay_score"))
    return {
        "candidate_id": str(row.get("candidate_id")),
        "rank": _int(row.get("rank") or fallback_rank),
        "campaign_rank": _int(row.get("campaign_rank")),
        "mechanism": _upper(row.get("mechanism") or split.get("mechanism")),
        "regime": normalize_regime(row.get("regime") or split.get("regime")),
        "timeframes": timeframes,
        "source_types": source_types,
        "session_context": session_context,
        "market_structures": market_structures,
        "symbols": symbols,
        "classification": classification or "UNKNOWN",
        "backtest_classification": backtest_classification,
        "candidate_review_status": "REJECTED" if classification == "REJECT_FOR_NOW" else "REVIEWABLE",
        "paper_forward_observation_candidate": classification == "READY_FOR_PAPER_FORWARD_OBSERVATION" or _int(row.get("campaign_rank")) > 0,
        "final_score": final_score,
        "ranking_score": _float(row.get("ranking_score")),
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "sample_size": _int(row.get("sample_size") if row.get("sample_size") is not None else metrics.get("sample_size")),
        "max_drawdown": _float(row.get("max_drawdown") if row.get("max_drawdown") is not None else row.get("drawdown") if row.get("drawdown") is not None else metrics.get("max_drawdown")),
        "replay_score": replay_score,
        "replay_backtest_consistency": _float(row.get("replay_backtest_consistency") if row.get("replay_backtest_consistency") is not None else metrics.get("replay_backtest_consistency")),
        "backtest_evidence_score": _float(row.get("backtest_evidence_score")),
        "penalty_profile": {key: _float(value) for key, value in sorted(penalties.items()) if value is not None},
        "entry_rule_signature": _text_signature(row, ["entry_condition", "entry_observation_condition", "entry_rule"]),
        "exit_rule_signature": _text_signature(row, ["exit_condition", "exit_observation_condition", "exit_rule"]),
        "invalidation_rule_signature": _text_signature(row, ["invalidating_condition", "invalidation_condition", "invalidation_rule"]),
    }


def _build_families(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        groups[row["family_id"]].append(row)
    total = max(len(candidates), 1)
    families = [_family_from_rows(family_id, rows, total=total) for family_id, rows in groups.items()]
    return sorted(families, key=lambda family: (family["recommended_priority_sort"], family["best_rank"], -family["candidate_count"], family["family_id"]))


def _family_from_rows(family_id: str, rows: list[dict[str, Any]], *, total: int) -> dict[str, Any]:
    rows = sorted(rows, key=lambda row: (_rank(row), row["candidate_id"]))
    classifications = Counter(row["classification"] for row in rows)
    candidate_ids = [row["candidate_id"] for row in rows]
    mechanisms = Counter(row["mechanism"] for row in rows)
    regimes = Counter(row["regime"] for row in rows)
    timeframes = Counter(item for row in rows for item in row["timeframes"])
    source_types = Counter(item for row in rows for item in row["source_types"])
    sessions = Counter(normalize_session_context(row.get("session_context")) for row in rows)
    market_structures = Counter(item for row in rows for item in row["market_structures"])
    symbols = sorted({item for row in rows for item in row["symbols"]})
    best = rows[0]
    classification = _classify_family(rows)
    priority, priority_sort = _recommended_priority(rows, classification)
    duplicate_assessment = _duplicate_assessment(rows)
    family_name = _family_name(mechanisms, regimes, timeframes, source_types, market_structures)
    direct_data = any((row.get("penalty_profile") or {}).get("proxy_data_penalty", 0) for row in rows)
    family = {
        "family_id": family_id,
        "family_name": family_name,
        "classification": classification,
        "dominant_mechanism": _counter_top(mechanisms),
        "dominant_regime": _counter_top(regimes),
        "dominant_timeframe": _counter_top(timeframes),
        "dominant_source_type": _counter_top(source_types),
        "dominant_session_context": _counter_top(sessions),
        "session_context_distribution": {session: int(sessions.get(session, 0)) for session in SESSION_CONTEXTS},
        "dominant_market_structure": _counter_top(market_structures),
        "candidate_count": len(rows),
        "share_of_clustered_candidates": round(len(rows) / total, 6),
        "candidate_ids": candidate_ids,
        "top_candidate_ids": candidate_ids[:8],
        "symbols/universes": symbols,
        "symbols": symbols,
        "average_final_score": _average(row["final_score"] for row in rows),
        "average_expectancy": _average(row["expectancy"] for row in rows),
        "average_profit_factor": _average(row["profit_factor"] for row in rows),
        "sample_size_summary": _numeric_summary(row["sample_size"] for row in rows),
        "average_sample_size": _average(row["sample_size"] for row in rows),
        "average_replay_score": _average(row["replay_score"] for row in rows),
        "average_replay_backtest_consistency": _average(row["replay_backtest_consistency"] for row in rows),
        "common_supporting_evidence": _supporting_evidence(rows),
        "common_failure_risks": _failure_risks(rows, direct_data),
        "duplicate_or_distinct_assessment": duplicate_assessment,
        "recommended_priority": priority,
        "recommended_priority_sort": priority_sort,
        "best_rank": _rank(best),
        "best_candidate": _candidate_brief(best),
        "mechanism": best["mechanism"],
        "regime": best["regime"],
        "timeframes": sorted(timeframes),
        "source_types": sorted(source_types),
        "session_contexts": sorted(session for session, count in sessions.items() if count),
        "market_structures": sorted(market_structures),
        "classification_counts": dict(sorted(classifications.items())),
        "penalty_profiles": [dict(profile) for profile in {tuple(row["penalty_profile"].items()): row["penalty_profile"] for row in rows}.values()],
        "duplicate_candidate_ids": candidate_ids[1:] if len(rows) > 1 else [],
        "paper_forward_observation_rationale": _observation_rationale(rows, classification, duplicate_assessment),
        "requires_direct_data_validation": direct_data,
    }
    assert family["classification"] in FAMILY_CLASSIFICATIONS
    return family


def _classify_family(rows: list[dict[str, Any]]) -> str:
    reviewable = [row for row in rows if row["candidate_review_status"] != "REJECTED"]
    paper_ready = [row for row in rows if row["paper_forward_observation_candidate"]]
    if not reviewable:
        return "WEAK_FAMILY"
    if any(row["sample_size"] == 0 for row in rows):
        return "INSUFFICIENT_DATA_FAMILY"
    if len(rows) >= 4:
        return "DUPLICATIVE_FAMILY"
    if paper_ready and min(_rank(row) for row in paper_ready) <= 8:
        return "HIGH_PRIORITY_FAMILY"
    if paper_ready:
        return "PROMISING_FAMILY"
    return "WEAK_FAMILY"


def _recommended_priority(rows: list[dict[str, Any]], classification: str) -> tuple[str, int]:
    best_rank = min(_rank(row) for row in rows)
    has_campaign = any(_int(row.get("campaign_rank")) > 0 for row in rows)
    has_ready = any(row["paper_forward_observation_candidate"] for row in rows)
    if has_campaign:
        return "OBSERVE_FIRST_REPRESENTATIVE", 0
    if best_rank <= 20 and classification in {"HIGH_PRIORITY_FAMILY", "DUPLICATIVE_FAMILY", "PROMISING_FAMILY"}:
        return "OBSERVE_AFTER_TOP_8_REPRESENTATIVES", 1
    if has_ready:
        return "OBSERVE_IF_CAPACITY_REMAINS", 2
    if classification == "INSUFFICIENT_DATA_FAMILY":
        return "DIRECT_DATA_VALIDATION_BEFORE_OBSERVATION", 3
    return "DO_NOT_PRIORITIZE", 4


def _near_duplicate_groups(families: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    for family in families:
        if len(family["candidate_ids"]) > 1:
            groups.append(
                {
                    "family_id": family["family_id"],
                    "family_name": family["family_name"],
                    "near_duplicate_key": "|".join([family["dominant_mechanism"], family["dominant_regime"], family["dominant_timeframe"], family["dominant_source_type"], family.get("dominant_market_structure", "UNKNOWN")]),
                    "candidate_ids": family["candidate_ids"],
                    "representative_candidate_id": family["candidate_ids"][0],
                    "reason": "same normalized mechanism/regime/timeframe/source signature with similar replay/backtest/penalty profile",
                }
            )
    return groups


def _top_family_entry(row: dict[str, Any], family_lookup: dict[str, dict[str, Any]]) -> dict[str, Any]:
    family = family_lookup.get(row["family_id"], {})
    return {
        "rank": row.get("rank"),
        "campaign_rank": row.get("campaign_rank"),
        "candidate_id": row["candidate_id"],
        "family_id": row["family_id"],
        "family_name": family.get("family_name"),
        "family_classification": family.get("classification"),
        "family_candidate_count": family.get("candidate_count"),
        "duplicate_or_distinct_assessment": family.get("duplicate_or_distinct_assessment"),
        "mechanism": row["mechanism"],
        "regime": row["regime"],
        "timeframes": row["timeframes"],
        "market_structures": row["market_structures"],
        "symbols": row["symbols"],
        "session_context": row["session_context"],
    }


def _family_brief(family: dict[str, Any]) -> dict[str, Any]:
    return {
        "family_id": family.get("family_id"),
        "family_name": family.get("family_name"),
        "classification": family.get("classification"),
        "candidate_count": family.get("candidate_count"),
        "best_rank": family.get("best_rank"),
        "best_candidate_id": (family.get("best_candidate") or {}).get("candidate_id"),
        "dominant_mechanism": family.get("dominant_mechanism"),
        "dominant_regime": family.get("dominant_regime"),
        "dominant_timeframe": family.get("dominant_timeframe"),
        "dominant_market_structure": family.get("dominant_market_structure"),
        "symbols": family.get("symbols"),
        "average_final_score": family.get("average_final_score"),
        "average_expectancy": family.get("average_expectancy"),
        "average_profit_factor": family.get("average_profit_factor"),
        "duplicate_or_distinct_assessment": family.get("duplicate_or_distinct_assessment"),
        "recommended_priority": family.get("recommended_priority"),
        "rationale": family.get("paper_forward_observation_rationale"),
    }


def _candidate_brief(row: dict[str, Any]) -> dict[str, Any]:
    return {key: row.get(key) for key in ["candidate_id", "rank", "campaign_rank", "classification", "paper_forward_observation_candidate", "final_score", "expectancy", "profit_factor", "sample_size", "market_structures"]}


def _family_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            row["mechanism"],
            row["regime"],
            ",".join(row["timeframes"]),
            ",".join(row["source_types"]),
            row["session_context"],
            ",".join(row["market_structures"]),
            "WINNER_FAMILY",
        ]
    )


def _near_duplicate_key(row: dict[str, Any]) -> str:
    return "|".join([
        row["mechanism"],
        row["regime"],
        ",".join(row["timeframes"]),
        ",".join(row["source_types"]),
        row["session_context"],
        ",".join(row["market_structures"]),
        _symbol_overlap_bucket(row["symbols"]),
        _metric_bucket(row["expectancy"], 0.001),
        _metric_bucket(row["profit_factor"], 0.25),
    ])


def _text_signature(row: dict[str, Any], keys: list[str]) -> str:
    text = " ".join(str(row.get(key) or "") for key in keys)
    tokens = sorted({token.lower() for token in text.replace("/", " ").replace("-", " ").split() if len(token) > 4})
    return ",".join(tokens[:12]) if tokens else "NO_TEXT_CONDITIONS"


def _observation_rationale(rows: list[dict[str, Any]], classification: str, duplicate_assessment: str) -> str:
    if any(_int(row.get("campaign_rank")) > 0 for row in rows):
        return "Contains a focused campaign candidate; observe one representative before same-family variants."
    if classification == "HIGH_PRIORITY_FAMILY":
        return "Contains a top-ranked paper-forward observation candidate with supported backtest evidence."
    if classification == "PROMISING_FAMILY":
        return "Contains paper-forward-ready candidates outside the focused campaign."
    if classification == "DUPLICATIVE_FAMILY" or duplicate_assessment != "GENUINELY_DISTINCT":
        return "Multiple candidates share the same normalized idea; observe one representative before spending review time on variants."
    if classification == "INSUFFICIENT_DATA_FAMILY":
        return "Needs direct data or larger sample before observation priority."
    return "Weak or rejected family; revisit only if new evidence appears."


def _cohort_comparisons(
    winner_patterns: dict[str, Any],
    *,
    top_8: list[dict[str, Any]],
    top_20: list[dict[str, Any]],
    eligible_detailed: list[dict[str, Any]],
    rejected_detailed: list[dict[str, Any]],
) -> dict[str, Any]:
    cohorts = winner_patterns.get("cohorts") or {}
    return {
        "top_8_campaign_candidates": _comparison_from_rows(top_8, "Top 8 campaign candidates are candidate-level clustered and family-assigned."),
        "top_20_ranked_candidates": _comparison_from_rows(top_20, "Top 20 ranked candidates are candidate-level clustered and show direct family dominance."),
        "all_110_eligible_candidates": _comparison_from_cohort(cohorts.get("all_110_eligible") or {}, eligible_detailed, "All 110 eligible candidates are compared with aggregate cohort distributions plus available detailed eligible rows."),
        "rejected_490_candidates": _comparison_from_cohort(cohorts.get("rejected_candidates") or {}, rejected_detailed, "490 rejected candidates are compared with aggregate cohort distributions plus available detailed rejected rows."),
    }


def _comparison_from_rows(rows: list[dict[str, Any]], summary: str) -> dict[str, Any]:
    return {
        "summary": summary,
        "candidate_count": len(rows),
        "family_count": len({row["family_id"] for row in rows}),
        "family_distribution": dict(Counter(row["family_id"] for row in rows).most_common()),
        "mechanism_distribution": dict(Counter(row["mechanism"] for row in rows).most_common()),
        "regime_distribution": dict(Counter(row["regime"] for row in rows).most_common()),
        "timeframe_distribution": dict(Counter(item for row in rows for item in row["timeframes"]).most_common()),
        "source_type_distribution": dict(Counter(item for row in rows for item in row["source_types"]).most_common()),
        "market_structure_distribution": dict(Counter(item for row in rows for item in row["market_structures"]).most_common()),
        "symbol_distribution": dict(Counter(item for row in rows for item in row["symbols"]).most_common()),
    }


def _comparison_from_cohort(cohort: dict[str, Any], detailed_rows: list[dict[str, Any]], summary: str) -> dict[str, Any]:
    numeric = cohort.get("numeric") or {}
    return {
        "summary": summary,
        "source_reported_count": cohort.get("count"),
        "candidate_level_rows_available": len(detailed_rows),
        "candidate_level_family_count": len({row["family_id"] for row in detailed_rows}),
        "candidate_level_family_distribution": dict(Counter(row["family_id"] for row in detailed_rows).most_common()),
        "mechanism_distribution": cohort.get("mechanism_distribution") or dict(Counter(row["mechanism"] for row in detailed_rows).most_common()),
        "regime_distribution": cohort.get("regime_distribution") or dict(Counter(row["regime"] for row in detailed_rows).most_common()),
        "timeframe_distribution": cohort.get("timeframe_distribution") or dict(Counter(item for row in detailed_rows for item in row["timeframes"]).most_common()),
        "source_type_distribution": cohort.get("source_type_distribution") or dict(Counter(item for row in detailed_rows for item in row["source_types"]).most_common()),
        "market_structure_distribution": cohort.get("market_structure_distribution") or dict(Counter(item for row in detailed_rows for item in row["market_structures"]).most_common()),
        "symbol_universe_distribution": cohort.get("symbol_universe_distribution") or dict(Counter(item for row in detailed_rows for item in row["symbols"]).most_common()),
        "numeric_averages": {key: value.get("average") for key, value in numeric.items() if isinstance(value, dict) and "average" in value},
        "penalty_presence": cohort.get("penalty_presence") or {},
    }


def _session_metrics(candidates: list[dict[str, Any]], families: list[dict[str, Any]]) -> dict[str, Any]:
    eligible = [row for row in candidates if row.get("candidate_review_status") != "REJECTED"]
    rejected = [row for row in candidates if row.get("candidate_review_status") == "REJECTED"]
    family_count_by_session = {
        session: sum(1 for family in families if int((family.get("session_context_distribution") or {}).get(session, 0)) > 0)
        for session in SESSION_CONTEXTS
    }
    score_rows = []
    for session in SESSION_CONTEXTS:
        rows = [row for row in eligible if normalize_session_context(row.get("session_context")) == session]
        avg_score = _average(row.get("final_score") for row in rows)
        score_rows.append({
            "session_context": session,
            "eligible_candidate_count": len(rows),
            "family_count": family_count_by_session[session],
            "average_final_score": avg_score,
            "rejected_candidate_count": sum(1 for row in rejected if normalize_session_context(row.get("session_context")) == session),
        })
    best = sorted([row for row in score_rows if row["eligible_candidate_count"]], key=lambda row: (-(row["average_final_score"] or 0.0), -row["eligible_candidate_count"], row["session_context"]))[:3]
    worst = sorted([row for row in score_rows if row["eligible_candidate_count"]], key=lambda row: ((row["average_final_score"] or 0.0), row["eligible_candidate_count"], row["session_context"]))[:3]
    return {
        "observations_by_session": {},
        "claims_by_session": {},
        "hypotheses_by_session": {},
        "eligible_candidates_by_session": session_distribution(row.get("session_context") for row in eligible),
        "rejected_candidates_by_session": session_distribution(row.get("session_context") for row in rejected),
        "family_count_by_session": family_count_by_session,
        "best_session_contexts": best,
        "worst_session_contexts": worst,
    }


def _overrepresented_families(families: list[dict[str, Any]], total: int) -> list[dict[str, Any]]:
    threshold = max(3, round(total * 0.025))
    return [family for family in families if family["candidate_count"] >= threshold]


def _observation_first_families(families: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        family
        for family in families
        if family["recommended_priority"] in {"OBSERVE_FIRST_REPRESENTATIVE", "OBSERVE_AFTER_TOP_8_REPRESENTATIVES", "OBSERVE_IF_CAPACITY_REMAINS"}
        and (family["best_candidate"] or {}).get("paper_forward_observation_candidate")
    ][:10]


def _duplicate_assessment(rows: list[dict[str, Any]]) -> str:
    if len(rows) == 1:
        return "GENUINELY_DISTINCT"
    symbol_sets = {tuple(row["symbols"]) for row in rows}
    if len(rows) >= 4 and len(symbol_sets) <= 2:
        return "NEAR_DUPLICATE_VARIANTS"
    if len(rows) >= 3:
        return "REPEATED_FAMILY_VARIANTS"
    return "RELATED_BUT_DISTINCT_VARIANTS"


def _supporting_evidence(rows: list[dict[str, Any]]) -> list[str]:
    evidence = []
    if all(row.get("backtest_classification") in {"BACKTEST_SUPPORTED", "READY_FOR_PAPER_FORWARD_OBSERVATION"} for row in rows):
        evidence.append("backtest-supported candidate evidence")
    if any(row.get("paper_forward_observation_candidate") for row in rows):
        evidence.append("paper-forward observation readiness in source ranking or campaign")
    if _average(row.get("expectancy") for row in rows) is not None:
        evidence.append("positive expectancy/profit-factor metrics available")
    if all(row.get("sample_size", 0) > 0 for row in rows):
        evidence.append("non-zero sample size")
    return evidence or ["limited detailed evidence fields available"]


def _failure_risks(rows: list[dict[str, Any]], direct_data: bool) -> list[str]:
    risks = []
    if direct_data:
        risks.append("proxy data dependence remains; direct candidate data validation required")
    if any((row.get("penalty_profile") or {}).get("intraday_daily_mismatch_penalty", 0) for row in rows):
        risks.append("intraday/daily mismatch penalty appears in family")
    if any(row.get("sample_size", 0) < 100 for row in rows):
        risks.append("some variants have small sample size")
    if len(rows) > 1:
        risks.append("duplicate review effort risk from repeated variants")
    return risks or ["main risk is forward-regime generalization"]


def _main_conclusion(family_count: int, eligible_reported: int, near_duplicates: list[dict[str, Any]]) -> str:
    if eligible_reported and family_count <= max(20, eligible_reported // 4):
        return f"The winners are repeated variants more than 110 independent ideas: {family_count} candidate-level families were found, with {len(near_duplicates)} near-duplicate groups."
    return f"The winners cluster into a smaller set of recurring families rather than 110 independent ideas: {family_count} candidate-level winner families were found in available detailed rows, with {len(near_duplicates)} near-duplicate groups."


def _family_name(mechanisms: Counter[str], regimes: Counter[str], timeframes: Counter[str], source_types: Counter[str], market_structures: Counter[str]) -> str:
    return " / ".join([_counter_top(mechanisms), _counter_top(regimes), _counter_top(timeframes), _counter_top(source_types), _counter_top(market_structures)])


def _family_line(family: dict[str, Any]) -> str:
    return f"- {family.get('family_id')} | {family.get('classification')} | count={family.get('candidate_count')} | session={family.get('dominant_session_context')} | best={family.get('best_candidate_id')} | {family.get('family_name')} | priority={family.get('recommended_priority')}"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _index_by_candidate_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("candidate_id")): row for row in rows if row.get("candidate_id")}


def _ordered_ids(rows: list[dict[str, Any]], *, rank_key: str) -> list[str]:
    return [str(row.get("candidate_id")) for row in sorted(rows, key=lambda row: _int(row.get(rank_key)) or 999999) if row.get("candidate_id")]


def _string_list(value: Any) -> list[str]:
    if value is None:
        return ["UNKNOWN"]
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple, set)):
        values = list(value)
    else:
        values = [str(value)]
    cleaned = sorted({str(item).upper() for item in values if str(item).strip()})
    return cleaned or ["UNKNOWN"]


def _upper(value: Any) -> str:
    return str(value or "UNKNOWN").upper()


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _average(values: Iterable[Any]) -> float | None:
    nums = [float(value) for value in values if value is not None]
    if not nums:
        return None
    return round(sum(nums) / len(nums), 6)


def _numeric_summary(values: Iterable[Any]) -> dict[str, Any]:
    nums = sorted(float(value) for value in values if value is not None)
    if not nums:
        return {"count": 0, "min": None, "median": None, "max": None, "average": None}
    midpoint = len(nums) // 2
    median = nums[midpoint] if len(nums) % 2 else (nums[midpoint - 1] + nums[midpoint]) / 2
    return {"count": len(nums), "min": nums[0], "median": round(median, 6), "max": nums[-1], "average": round(sum(nums) / len(nums), 6)}


def _metric_bucket(value: float | None, width: float) -> str:
    if value is None:
        return "NONE"
    return str(round(round(float(value) / width) * width, 6))


def _symbol_overlap_bucket(symbols: list[str]) -> str:
    if not symbols or symbols == ["UNKNOWN"]:
        return "UNKNOWN"
    if len(symbols) >= 8:
        return "BROAD_UNIVERSE"
    return ",".join(symbols)


def _counter_top(counter: Counter[str]) -> str:
    if not counter:
        return "UNKNOWN"
    return counter.most_common(1)[0][0]


def _rank(row: dict[str, Any]) -> int:
    return _int(row.get("rank")) or _int(row.get("campaign_rank")) or 999999


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
