from __future__ import annotations

import json
import statistics
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .backtest_aware_final_qualification import build_backtest_aware_final_qualification_report
from .final_candidate_ranking import AUTHORITY_BOUNDARY
from .historical_replay_engine import now_utc
from .regime_expansion import positive_replay_rate_by_regime, normalize_regime

REPORT_DIRNAME = "winner_pattern_extraction"


def run_winner_pattern_extraction(root: str | Path = DEFAULT_STORE_ROOT, *, observation_count: int = 100000, created_at: str | None = None) -> dict[str, Any]:
    report = build_winner_pattern_report(root=root, observation_count=observation_count, created_at=created_at)
    write_winner_pattern_report(report, root=root)
    return report


def build_winner_pattern_report(root: str | Path = DEFAULT_STORE_ROOT, *, observation_count: int = 100000, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or now_utc()
    final_report = build_backtest_aware_final_qualification_report(root=root_path, observation_count=observation_count, created_at=created)
    ranking_latest = _read_json(root_path / "final_candidate_ranking" / "latest.json", {})
    campaign_latest = _read_json(root_path / "focused_observation_campaign" / "latest.json", {})
    split_latest = _read_json(root_path / "observation_cluster_split_experiment" / "latest.json", {})
    source_latest = _read_json(root_path / "observation_source_breakdown" / "latest.json", {})

    all_rows = [_normalize_trial(row) for row in final_report.get("trials", [])]
    eligible = [row for row in all_rows if row["final_eligible"]]
    rejected = [row for row in all_rows if not row["final_eligible"]]
    top20_ids = [row.get("candidate_id") for row in ranking_latest.get("top_20_robust_candidates", []) if row.get("candidate_id")]
    campaign_ids = [row.get("candidate_id") for row in campaign_latest.get("campaign_candidates", []) if row.get("candidate_id")]
    top20 = _rows_by_ids(all_rows, top20_ids) or _top_by_score(eligible, 20)
    campaign = _rows_by_ids(all_rows, campaign_ids) or _top_by_score(eligible, 8)

    cohorts = {
        "all_110_eligible": _cohort_summary(eligible),
        "top_20": _cohort_summary(top20),
        "top_8_campaign": _cohort_summary(campaign),
        "rejected_candidates": _cohort_summary(rejected),
    }
    report = {
        "schema_id": "atlas_v2_research_os_winner_pattern_extraction",
        "schema_version": "1.0",
        "report_type": "WINNER_PATTERN_EXTRACTION",
        "created_at": created,
        "day": created[:10] or date.today().isoformat(),
        "source_reports": {
            "backtest_aware_final_qualification": str(root_path / "backtest_aware_final_qualification" / "latest.json"),
            "final_candidate_ranking": str(root_path / "final_candidate_ranking" / "latest.json"),
            "focused_observation_campaign": str(root_path / "focused_observation_campaign" / "latest.json"),
            "observation_cluster_split_experiment": str(root_path / "observation_cluster_split_experiment" / "latest.json"),
            "observation_source_breakdown": str(root_path / "observation_source_breakdown" / "latest.json"),
        },
        "input_context": {
            "observation_count": observation_count,
            "split_diversity_hypotheses": len(all_rows),
            "backtest_supported_candidates": sum(row["backtest_classification"] == "BACKTEST_SUPPORTED" for row in all_rows),
            "final_eligible_candidates": len(eligible),
            "top_20_candidates": len(top20),
            "campaign_candidates": len(campaign),
            "ready_for_paper_forward_observation": (campaign_latest.get("summary") or {}).get("ready_for_observation_count"),
            "split_experiment_summary": split_latest.get("comparison", {}),
            "source_breakdown_summary": source_latest.get("summary", {}),
        },
        "cohorts": cohorts,
        "distinguishing_characteristics": _distinguish_winners(top20=top20, campaign=campaign, eligible=eligible, rejected=rejected),
        "top_20_candidates": _compact_candidates(top20),
        "top_8_campaign_candidates": _compact_candidates(campaign),
        "answers": _answers(cohorts),
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Pattern extraction is research-only.",
            "No live trading, broker execution, capital allocation, position sizing, portfolio construction, trade recommendations, automatic paper placement, or production promotion.",
        ],
    }
    _validate_authority(report)
    return report


def write_winner_pattern_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day_value = str(report.get("day") or date.today().isoformat())
    out_dir = root_path / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "winner_pattern_report.json"
    summary_path = out_dir / "winner_pattern_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_winner_pattern_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_winner_pattern_summary(report: dict[str, Any]) -> str:
    answers = report.get("answers", {})
    cohorts = report.get("cohorts", {})
    lines = [
        "# Winner Pattern Extraction",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Executive Answers",
        "",
        f"- What winners have in common: {answers.get('what_winners_have_in_common')}",
        f"- Dominant mechanisms: {answers.get('dominant_mechanisms')}",
        f"- Dominant regimes: {answers.get('dominant_regimes')}",
        f"- Top 20 positive replay rate by regime: {cohorts.get('top_20', {}).get('positive_replay_rate_by_regime', {})}",
        f"- Dominant source types: {answers.get('dominant_source_types')}",
        f"- Dominant timeframes: {answers.get('dominant_timeframes')}",
        f"- Least-present penalties: {answers.get('least_present_penalties')}",
        f"- Winner/rejected distinction: {answers.get('winner_rejected_distinction')}",
        "",
        "## Cohort Counts",
        "",
    ]
    for name in ["all_110_eligible", "top_20", "top_8_campaign", "rejected_candidates"]:
        cohort = cohorts.get(name, {})
        lines.append(
            f"- {name}: count={cohort.get('count')} avg_final={cohort.get('numeric', {}).get('final_score', {}).get('average')} avg_pf={cohort.get('numeric', {}).get('profit_factor', {}).get('average')} avg_expectancy={cohort.get('numeric', {}).get('expectancy', {}).get('average')}"
        )
    lines.extend(["", "## Top 8 Campaign Candidates", ""])
    for row in report.get("top_8_campaign_candidates", []):
        lines.append(
            f"- {row.get('candidate_id')} mechanism={row.get('mechanism')} regime={row.get('regime')} timeframes={','.join(row.get('timeframes') or [])} final={row.get('final_score')} pf={row.get('profit_factor')} n={row.get('sample_size')}"
        )
    lines.extend(["", "Authority: research-only pattern extraction. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, or candidate promotion.", ""])
    return "\n".join(lines)


def _normalize_trial(row: dict[str, Any]) -> dict[str, Any]:
    replay = row.get("historical_replay") or {}
    backtest = row.get("candidate_backtest") or {}
    metrics = backtest.get("metrics") or {}
    final = row.get("final_qualification") or {}
    penalties = final.get("penalties") or {}
    return {
        "candidate_id": row.get("candidate_id"),
        "mechanism": str(row.get("mechanism") or "UNKNOWN"),
        "regime": normalize_regime(row.get("regime")),
        "timeframes": _keys_or_values(row.get("candidate_timeframes"), row.get("timeframe_counts")),
        "source_types": _keys_or_values(row.get("candidate_source_types"), row.get("source_type_counts")),
        "symbols": _keys_or_values(row.get("candidate_universe_symbols") or row.get("candidate_symbols"), row.get("symbol_counts")),
        "replay_score": _round(replay.get("score") or metrics.get("historical_replay_score")),
        "replay_status": replay.get("status"),
        "final_score": _round(final.get("final_score")),
        "preliminary_edge_score": _round(final.get("preliminary_edge_score")),
        "backtest_evidence_score": _round(final.get("backtest_evidence_score")),
        "expectancy": _round(metrics.get("expectancy")),
        "profit_factor": _round(metrics.get("profit_factor")),
        "sample_size": int(metrics.get("sample_size") or 0),
        "drawdown": _round(metrics.get("max_drawdown")),
        "replay_backtest_consistency": _round(metrics.get("replay_backtest_consistency")),
        "penalties": {key: _round(value) for key, value in penalties.items()},
        "total_penalty": _round(final.get("total_penalty")),
        "final_eligible": bool(final.get("eligible")),
        "backtest_classification": backtest.get("classification"),
        "disqualification_reasons": list(final.get("disqualification_reasons") or []),
        "warnings": list(backtest.get("warnings") or []),
    }


def _cohort_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "count": len(rows),
        "mechanism_distribution": _distribution(row["mechanism"] for row in rows),
        "regime_distribution": _distribution(row["regime"] for row in rows),
        "timeframe_distribution": _distribution(value for row in rows for value in row["timeframes"]),
        "source_type_distribution": _distribution(value for row in rows for value in row["source_types"]),
        "symbol_universe_distribution": _distribution(value for row in rows for value in row["symbols"]),
        "penalty_presence": _penalty_presence(rows),
        "numeric": {
            "replay_score": _numeric_summary(row["replay_score"] for row in rows),
            "final_score": _numeric_summary(row["final_score"] for row in rows),
            "expectancy": _numeric_summary(row["expectancy"] for row in rows),
            "profit_factor": _numeric_summary(row["profit_factor"] for row in rows),
            "sample_size": _numeric_summary(row["sample_size"] for row in rows),
            "drawdown": _numeric_summary(row["drawdown"] for row in rows),
            "total_penalty": _numeric_summary(row["total_penalty"] for row in rows),
            "replay_backtest_consistency": _numeric_summary(row["replay_backtest_consistency"] for row in rows),
        },
        "backtest_classification_distribution": _distribution(row["backtest_classification"] for row in rows),
        "replay_status_distribution": _distribution(row["replay_status"] for row in rows),
        "positive_replay_rate_by_regime": positive_replay_rate_by_regime(rows),
        "main_disqualification_reasons": _distribution(reason for row in rows for reason in row["disqualification_reasons"]),
    }


def _distinguish_winners(*, top20: list[dict[str, Any]], campaign: list[dict[str, Any]], eligible: list[dict[str, Any]], rejected: list[dict[str, Any]]) -> dict[str, Any]:
    top = _cohort_summary(top20)
    camp = _cohort_summary(campaign)
    rej = _cohort_summary(rejected)
    return {
        "top20_vs_rejected": _delta_summary(top, rej),
        "campaign_vs_all_eligible": _delta_summary(camp, _cohort_summary(eligible)),
        "winner_markers": [
            "final_score clears the unchanged 0.70 threshold",
            "backtest classification is BACKTEST_SUPPORTED",
            "sample-size and missing-evidence penalties are absent",
            "proxy penalty remains but is small enough not to block final qualification",
            "profit factor and expectancy remain positive after backtest-aware scoring",
        ],
    }


def _delta_summary(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    return {
        "final_score_average_delta": _delta(left, right, "final_score"),
        "expectancy_average_delta": _delta(left, right, "expectancy"),
        "profit_factor_average_delta": _delta(left, right, "profit_factor"),
        "sample_size_average_delta": _delta(left, right, "sample_size"),
        "total_penalty_average_delta": _delta(left, right, "total_penalty"),
        "left_top_mechanisms": list(left.get("mechanism_distribution", {}).keys())[:5],
        "right_top_mechanisms": list(right.get("mechanism_distribution", {}).keys())[:5],
        "left_top_regimes": list(left.get("regime_distribution", {}).keys())[:5],
        "right_top_regimes": list(right.get("regime_distribution", {}).keys())[:5],
    }


def _answers(cohorts: dict[str, Any]) -> dict[str, str]:
    campaign = cohorts.get("top_8_campaign", {})
    top20 = cohorts.get("top_20", {})
    rejected = cohorts.get("rejected_candidates", {})
    top_mechs = list((campaign.get("mechanism_distribution") or top20.get("mechanism_distribution") or {}).keys())[:4]
    top_regimes = list((campaign.get("regime_distribution") or top20.get("regime_distribution") or {}).keys())[:4]
    top_sources = list((campaign.get("source_type_distribution") or top20.get("source_type_distribution") or {}).keys())[:4]
    top_timeframes = list((campaign.get("timeframe_distribution") or top20.get("timeframe_distribution") or {}).keys())[:4]
    least_penalties = _least_present_penalties(campaign.get("penalty_presence") or {})
    return {
        "what_winners_have_in_common": "They are backtest-supported, final-score eligible at the unchanged threshold, positive-expectancy, positive-profit-factor candidates with adequate sample size and no missing-evidence or sample-size penalties.",
        "dominant_mechanisms": ", ".join(top_mechs) if top_mechs else "No dominant mechanism detected.",
        "dominant_regimes": ", ".join(top_regimes) if top_regimes else "No dominant regime detected.",
        "dominant_source_types": ", ".join(top_sources) if top_sources else "Source type was not preserved on the winning rows.",
        "dominant_timeframes": ", ".join(top_timeframes) if top_timeframes else "No dominant timeframe detected.",
        "least_present_penalties": ", ".join(least_penalties) if least_penalties else "No penalty was fully absent.",
        "winner_rejected_distinction": "Rejected candidates are mostly separated by lower final scores, unsupported or weaker backtests, and explicit disqualification reasons; winners retain proxy limitations but avoid missing-evidence, negative-backtest, and sample-size blockers.",
        "rejected_main_reasons": ", ".join(list((rejected.get("main_disqualification_reasons") or {}).keys())[:5]),
    }


def _compact_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": row["candidate_id"],
            "mechanism": row["mechanism"],
            "regime": row["regime"],
            "timeframes": row["timeframes"],
            "source_types": row["source_types"],
            "symbols": row["symbols"],
            "replay_score": row["replay_score"],
            "final_score": row["final_score"],
            "expectancy": row["expectancy"],
            "profit_factor": row["profit_factor"],
            "sample_size": row["sample_size"],
            "drawdown": row["drawdown"],
            "penalties": row["penalties"],
        }
        for row in rows
    ]


def _rows_by_ids(rows: list[dict[str, Any]], ids: list[str]) -> list[dict[str, Any]]:
    by_id = {row["candidate_id"]: row for row in rows}
    return [by_id[candidate_id] for candidate_id in ids if candidate_id in by_id]


def _top_by_score(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (-row["final_score"], row["candidate_id"] or ""))[:limit]


def _distribution(values) -> dict[str, int]:
    counter = Counter(str(value or "UNKNOWN") for value in values if value not in {None, ""})
    return dict(counter.most_common())


def _penalty_presence(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    keys = sorted({key for row in rows for key in row["penalties"]})
    return {
        key: {
            "count_present": sum(row["penalties"].get(key, 0.0) > 0 for row in rows),
            "average": _average(row["penalties"].get(key, 0.0) for row in rows),
        }
        for key in keys
    }


def _least_present_penalties(presence: dict[str, dict[str, Any]]) -> list[str]:
    return [key for key, value in sorted(presence.items(), key=lambda item: (item[1].get("count_present", 0), item[1].get("average", 0))) if value.get("count_present", 0) == 0]


def _numeric_summary(values) -> dict[str, float | int | None]:
    clean = [float(value) for value in values if value not in {None, ""}]
    if not clean:
        return {"count": 0, "average": None, "median": None, "min": None, "max": None}
    return {
        "count": len(clean),
        "average": round(sum(clean) / len(clean), 6),
        "median": round(statistics.median(clean), 6),
        "min": round(min(clean), 6),
        "max": round(max(clean), 6),
    }


def _keys_or_values(values: Any, counts: Any) -> list[str]:
    if isinstance(values, list) and values:
        return sorted(str(value) for value in values if value not in {None, ""})
    if isinstance(counts, dict) and counts:
        return sorted(str(key) for key in counts if key not in {None, ""})
    return []


def _delta(left: dict[str, Any], right: dict[str, Any], metric: str) -> float | None:
    left_value = ((left.get("numeric") or {}).get(metric) or {}).get("average")
    right_value = ((right.get("numeric") or {}).get(metric) or {}).get("average")
    if left_value is None or right_value is None:
        return None
    return round(float(left_value) - float(right_value), 6)


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _round(value: Any) -> float:
    try:
        return round(float(value or 0.0), 6)
    except (TypeError, ValueError):
        return 0.0


def _average(values) -> float:
    clean = [float(value or 0.0) for value in values]
    return round(sum(clean) / len(clean), 6) if clean else 0.0


def _validate_authority(report: dict[str, Any]) -> None:
    authority = report.get("authority_boundary", {})
    forbidden_true = [key for key, value in authority.items() if key != "paper_forward_observation_only" and value is True]
    if authority.get("paper_forward_observation_only") is not True or forbidden_true:
        raise ValueError(f"winner pattern extraction authority boundary failed: {forbidden_true}")
