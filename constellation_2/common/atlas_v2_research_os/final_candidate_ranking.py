from __future__ import annotations

import json
import math
import statistics
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .backtest_aware_final_qualification import build_backtest_aware_final_qualification_report
from .candidate_symbol_attribution import apply_symbol_attribution
from .edge_qualification_models import PAPER_CANDIDATE_LIMITATION
from .historical_replay_engine import now_utc

RANKING_DIRNAME = "final_candidate_ranking"
CAMPAIGN_DIRNAME = "focused_observation_campaign"
DEFAULT_CAMPAIGN_SIZE = 8
TOP_REVIEW_COUNT = 20

AUTHORITY_BOUNDARY = {
    "paper_forward_observation_only": True,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "portfolio_construction_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}

STRONGLY_INTRADAY_MECHANISMS = {
    "OPENING_RANGE",
    "SESSION_TIMING",
    "VWAP_OR_AVERAGE_RECLAIM",
    "LIQUIDITY_SWEEP",
}


def run_final_candidate_ranking(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    observation_count: int | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    ranking = build_final_candidate_ranking_report(root=root, observation_count=observation_count, created_at=created_at)
    ranking_paths = write_final_candidate_ranking_report(ranking, root=root)
    campaign = build_focused_observation_campaign_report(ranking, created_at=ranking["created_at"])
    campaign_paths = write_focused_observation_campaign_report(campaign, root=root)
    return {"ranking": ranking, "campaign": campaign, "paths": {"ranking": _stringify_paths(ranking_paths), "campaign": _stringify_paths(campaign_paths)}}


def build_final_candidate_ranking_report(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    observation_count: int | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    final_latest = _read_json(root_path / "backtest_aware_final_qualification" / "latest.json", {})
    source_count = observation_count or int(((final_latest.get("source") or {}).get("observation_count") or 100000))
    created = created_at or now_utc()
    final_report = build_backtest_aware_final_qualification_report(root=root_path, observation_count=source_count, created_at=created)
    candidates = [_rankable_candidate(row) for row in _extract_full_trials(final_report)]
    eligible = [row for row in candidates if row["final_eligible"]]
    ranked = _rank_candidates(eligible)
    top_20 = _diversified_selection(ranked, target=TOP_REVIEW_COUNT, max_per_mechanism=4, max_per_regime=5, allow_data_improvement=True)
    campaign_candidates = _campaign_candidates(ranked, target=DEFAULT_CAMPAIGN_SIZE)
    excluded = _excluded_candidates(candidates, selected_ids={row["candidate_id"] for row in top_20})
    report = {
        "schema_id": "atlas_v2_research_os_final_candidate_ranking",
        "schema_version": "1.0",
        "report_type": "FINAL_CANDIDATE_RANKING",
        "created_at": created,
        "day": created[:10] or date.today().isoformat(),
        "source_reports": _source_report_paths(root_path),
        "ranking_policy": {
            "rank_by": [
                "final_score",
                "expectancy",
                "profit_factor",
                "sample_size",
                "max_drawdown_inverse",
                "replay_backtest_consistency",
                "mechanism diversity",
                "regime diversity",
                "lower proxy penalty",
                "lower intraday_daily_mismatch penalty",
                "lower missing_evidence penalty",
            ],
            "threshold_unchanged": final_report.get("threshold"),
            "automatic_exclusions": [
                "sample_size < 30",
                "missing evidence penalty",
                "unsupported backtest",
                "large unresolved drawdown warning",
                "strongly intraday mechanism with daily proxy mismatch unless explicitly flagged",
            ],
        },
        "summary": _summary(candidates, eligible, ranked, top_20, campaign_candidates, excluded),
        "top_20_robust_candidates": top_20,
        "campaign_candidate_preview": campaign_candidates,
        "excluded_candidates": excluded[:100],
        "mechanism_distribution": dict(Counter(row["mechanism"] for row in ranked)),
        "regime_distribution": dict(Counter(row["regime"] for row in ranked)),
        "main_remaining_evidence_weaknesses": _weaknesses(candidates),
        "recommended_human_review_order": _human_review_order(top_20),
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "limitations": [
            PAPER_CANDIDATE_LIMITATION,
            "Ranking is for paper-forward observation review only.",
            "Local SPY daily proxy limitations remain material for all ranked candidates.",
        ],
    }
    _validate_authority(report)
    return report


def build_focused_observation_campaign_report(ranking_report: dict[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
    campaign = list(ranking_report.get("campaign_candidate_preview") or [])
    for index, row in enumerate(campaign, start=1):
        row["campaign_rank"] = index
        row["human_review_required"] = True
        row["recommended_human_action"] = (
            "APPROVE_FOR_PAPER_FORWARD_OBSERVATION"
            if row["classification"] == "READY_FOR_PAPER_FORWARD_OBSERVATION"
            else "REQUEST_DATA_IMPROVEMENT_BEFORE_OBSERVATION"
        )
        row["paper_forward_observation_only"] = True
    return {
        "schema_id": "atlas_v2_research_os_focused_observation_campaign",
        "schema_version": "1.0",
        "report_type": "FOCUSED_OBSERVATION_CAMPAIGN",
        "created_at": created_at or ranking_report.get("created_at") or now_utc(),
        "day": str((created_at or ranking_report.get("created_at") or now_utc()))[:10],
        "source_report": "reports/atlas_v2_research_os/final_candidate_ranking/latest.json",
        "selection_policy": {
            "target_size": DEFAULT_CAMPAIGN_SIZE,
            "paper_forward_observation_only": True,
            "requires_human_review": True,
            "excludes_unsupported_backtests": True,
            "excludes_missing_evidence_penalty": True,
            "excludes_intraday_daily_mismatch_for_strong_intraday_mechanisms": True,
            "max_per_mechanism": 2,
            "max_per_regime": 3,
        },
        "summary": {
            "campaign_candidate_count": len(campaign),
            "ready_for_observation_count": sum(row["classification"] == "READY_FOR_PAPER_FORWARD_OBSERVATION" for row in campaign),
            "mechanism_distribution": dict(Counter(row["mechanism"] for row in campaign)),
            "regime_distribution": dict(Counter(row["regime"] for row in campaign)),
            "biggest_remaining_risk": _biggest_risk(campaign),
        },
        "campaign_candidates": campaign,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Paper-forward observation only.",
            "No live trading, broker execution, capital allocation, position sizing, portfolio construction, trade recommendations, automatic paper placement, or production promotion.",
        ],
    }


def write_final_candidate_ranking_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    return _write_report(
        report,
        root=Path(root) / RANKING_DIRNAME,
        filename="final_candidate_ranking_report.json",
        summary_filename="final_candidate_ranking_summary.md",
        summary=render_final_candidate_ranking_summary(report),
    )


def write_focused_observation_campaign_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    return _write_report(
        report,
        root=Path(root) / CAMPAIGN_DIRNAME,
        filename="focused_observation_campaign.json",
        summary_filename="focused_observation_campaign_summary.md",
        summary=render_focused_observation_campaign_summary(report),
    )


def render_final_candidate_ranking_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Final Candidate Ranking",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Summary",
        "",
        f"- Eligible candidates reviewed: {summary.get('eligible_candidates_reviewed')}",
        f"- Top review candidates: {summary.get('top_20_count')}",
        f"- Focused campaign candidates: {summary.get('campaign_candidate_count')}",
        f"- Excluded candidates: {summary.get('excluded_candidate_count')}",
        f"- Biggest remaining risk: {summary.get('biggest_remaining_risk')}",
        "",
        "## Top 20 Robust Candidates",
        "",
    ]
    for row in report.get("top_20_robust_candidates", []):
        lines.append(
            "- rank={rank} candidate={candidate_id} mechanism={mechanism} regime={regime} class={classification} final={final_score} exp={expectancy} pf={profit_factor} n={sample_size}".format(
                **row
            )
        )
    lines.extend(["", "## Focused Campaign Preview", ""])
    for row in report.get("campaign_candidate_preview", []):
        lines.append(f"- campaign_rank={row.get('campaign_rank')} candidate={row.get('candidate_id')} mechanism={row.get('mechanism')} regime={row.get('regime')} final={row.get('final_score')}")
    lines.extend(["", "Authority: paper-forward observation only. No live trading, broker execution, capital allocation, position sizing, portfolio construction, trade recommendations, automatic paper placement, or production promotion.", ""])
    return "\n".join(lines)


def render_focused_observation_campaign_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Focused Observation Campaign",
        "",
        f"Created: {report.get('created_at')}",
        f"Campaign candidates: {summary.get('campaign_candidate_count')}",
        f"Ready for observation: {summary.get('ready_for_observation_count')}",
        f"Biggest remaining risk: {summary.get('biggest_remaining_risk')}",
        "",
        "## Campaign",
        "",
    ]
    for row in report.get("campaign_candidates", []):
        lines.append(
            "- rank={campaign_rank} candidate={candidate_id} mechanism={mechanism} regime={regime} final={final_score} action={recommended_human_action}".format(
                **row
            )
        )
    lines.extend(["", "Authority: paper-forward observation only; human review required before any paper-forward observation. No live/capital/broker/position-sizing authority.", ""])
    return "\n".join(lines)


def _extract_full_trials(final_report: dict[str, Any]) -> list[dict[str, Any]]:
    trials = final_report.get("trials") or []
    if trials:
        return list(trials)
    return list(final_report.get("sample_trials", []))


def _rankable_candidate(row: dict[str, Any]) -> dict[str, Any]:
    final = row.get("final_qualification") or {}
    backtest = row.get("candidate_backtest") or {}
    metrics = backtest.get("metrics") or {}
    penalties = final.get("penalties") or {}
    warnings = list(backtest.get("warnings") or [])
    max_drawdown = _float(metrics.get("max_drawdown"))
    sample_size = int(metrics.get("sample_size") or 0)
    classification, reasons = _classify_candidate(row, final=final, backtest=backtest, metrics=metrics, penalties=penalties, warnings=warnings)
    score = _ranking_score(final, metrics, penalties, row.get("mechanism"), row.get("regime"), classification)
    ranked_row = {
        "candidate_id": row.get("candidate_id"),
        "mechanism": str(row.get("mechanism") or "UNKNOWN"),
        "regime": str(row.get("regime") or "UNKNOWN"),
        "symbols": list(row.get("symbols", [])),
        "timeframes": list(row.get("timeframes", [])),
        "source_types": list(row.get("source_types", [])),
        "source_observation_ids": list(row.get("source_observation_ids", [])),
        "source_lineage": dict(row.get("source_lineage", {}) if isinstance(row.get("source_lineage", {}), dict) else {}),
        "final_score": _round(final.get("final_score")),
        "preliminary_edge_score": _round(final.get("preliminary_edge_score")),
        "backtest_evidence_score": _round(final.get("backtest_evidence_score")),
        "expectancy": _round(metrics.get("expectancy")),
        "profit_factor": _round(metrics.get("profit_factor")),
        "sample_size": sample_size,
        "max_drawdown": _round(max_drawdown),
        "max_drawdown_inverse": _round(max(0.0, 1.0 - abs(max_drawdown) / 0.35)),
        "replay_backtest_consistency": _round(metrics.get("replay_backtest_consistency")),
        "proxy_penalty": _round(penalties.get("proxy_data_penalty")),
        "intraday_daily_mismatch_penalty": _round(penalties.get("intraday_daily_mismatch_penalty")),
        "missing_evidence_penalty": _round(penalties.get("missing_evidence_penalty")),
        "sample_size_penalty": _round(penalties.get("sample_size_penalty")),
        "backtest_classification": backtest.get("classification"),
        "final_eligible": bool(final.get("eligible")),
        "ranking_score": score,
        "classification": classification,
        "classification_reasons": reasons,
        "warnings": warnings,
        "missing_data": list(backtest.get("missing_data") or []),
        "candidate_symbols": list(row.get("candidate_symbols") or []),
        "candidate_universe_symbols": list(row.get("candidate_universe_symbols") or []),
        "candidate_timeframes": list(row.get("candidate_timeframes") or []),
        "candidate_source_observation_ids": list(row.get("candidate_source_observation_ids") or []),
        "candidate_source_types": list(row.get("candidate_source_types") or []),
        "symbol_attribution_confidence": row.get("symbol_attribution_confidence"),
        "symbol_attribution_method": row.get("symbol_attribution_method"),
        "recommended_human_action": _human_action(classification),
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
    }
    return apply_symbol_attribution(ranked_row)


def _rank_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = sorted(
        candidates,
        key=lambda row: (
            -float(row["ranking_score"]),
            -float(row["final_score"] or 0),
            -float(row["expectancy"] or 0),
            -float(row["profit_factor"] or 0),
            -int(row["sample_size"] or 0),
            row["candidate_id"] or "",
        ),
    )
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    return ranked


def _campaign_candidates(ranked: list[dict[str, Any]], *, target: int) -> list[dict[str, Any]]:
    ready = [row for row in ranked if row["classification"] == "READY_FOR_PAPER_FORWARD_OBSERVATION"]
    selected = _diversified_selection(ready, target=target, max_per_mechanism=2, max_per_regime=3, allow_data_improvement=False)
    if len(selected) < 5:
        backups = [row for row in ranked if row["classification"] in {"NEEDS_DATA_IMPROVEMENT", "NEEDS_RULE_CLARIFICATION"}]
        selected.extend(_diversified_selection(backups, target=5 - len(selected), max_per_mechanism=2, max_per_regime=3, existing=selected, allow_data_improvement=True))
    for index, row in enumerate(selected, start=1):
        row["campaign_rank"] = index
    return selected


def _diversified_selection(
    ranked: list[dict[str, Any]],
    *,
    target: int,
    max_per_mechanism: int,
    max_per_regime: int,
    allow_data_improvement: bool,
    existing: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    selected = list(existing or [])
    selected_ids = {row["candidate_id"] for row in selected}
    mech_counts = Counter(row["mechanism"] for row in selected)
    regime_counts = Counter(row["regime"] for row in selected)
    allowed = {"READY_FOR_PAPER_FORWARD_OBSERVATION"}
    if allow_data_improvement:
        allowed.update({"NEEDS_DATA_IMPROVEMENT", "NEEDS_RULE_CLARIFICATION"})
    for row in ranked:
        if len(selected) >= target:
            break
        if row["candidate_id"] in selected_ids or row["classification"] not in allowed:
            continue
        if mech_counts[row["mechanism"]] >= max_per_mechanism or regime_counts[row["regime"]] >= max_per_regime:
            continue
        selected.append(dict(row))
        selected_ids.add(row["candidate_id"])
        mech_counts[row["mechanism"]] += 1
        regime_counts[row["regime"]] += 1
    if len(selected) < target:
        for row in ranked:
            if len(selected) >= target:
                break
            if row["candidate_id"] in selected_ids or row["classification"] not in allowed:
                continue
            selected.append(dict(row))
            selected_ids.add(row["candidate_id"])
    return selected[:target]


def _classify_candidate(
    row: dict[str, Any],
    *,
    final: dict[str, Any],
    backtest: dict[str, Any],
    metrics: dict[str, Any],
    penalties: dict[str, Any],
    warnings: list[str],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    mechanism = str(row.get("mechanism") or "UNKNOWN")
    sample_size = int(metrics.get("sample_size") or 0)
    max_drawdown = abs(_float(metrics.get("max_drawdown")))
    if not final.get("eligible"):
        return "REJECT_FOR_NOW", list(final.get("disqualification_reasons") or ["final qualification failed"])
    if backtest.get("classification") != "BACKTEST_SUPPORTED":
        return "REJECT_FOR_NOW", [f"unsupported backtest: {backtest.get('classification')}"]
    if sample_size < 30 or _float(penalties.get("sample_size_penalty")) > 0:
        return "NEEDS_DATA_IMPROVEMENT", [f"sample_size below requirement: {sample_size}"]
    if _float(penalties.get("missing_evidence_penalty")) > 0:
        return "NEEDS_DATA_IMPROVEMENT", ["missing evidence penalty present"]
    if max_drawdown > 0.20 or any("drawdown" in warning.lower() and "unresolved" in warning.lower() for warning in warnings):
        return "TOO_FRAGILE", [f"max_drawdown too large: {round(max_drawdown, 6)}"]
    if mechanism in STRONGLY_INTRADAY_MECHANISMS and _float(penalties.get("intraday_daily_mismatch_penalty")) > 0:
        return "NEEDS_DATA_IMPROVEMENT", ["strongly intraday mechanism is still tested with daily proxy data"]
    if _float(penalties.get("proxy_data_penalty")) > 0 and _float(final.get("final_score")) < 0.705:
        return "TOO_PROXY_DEPENDENT", ["thin margin above threshold with SPY proxy penalty still present"]
    if not row.get("candidate_id"):
        return "NEEDS_RULE_CLARIFICATION", ["candidate id missing"]
    reasons.append("final qualification passed at unchanged threshold")
    reasons.append("backtest classification BACKTEST_SUPPORTED")
    reasons.append("sample size and drawdown filters passed")
    return "READY_FOR_PAPER_FORWARD_OBSERVATION", reasons


def _ranking_score(final: dict[str, Any], metrics: dict[str, Any], penalties: dict[str, Any], mechanism: Any, regime: Any, classification: str) -> float:
    final_score = _float(final.get("final_score"))
    expectancy = _clamp((_float(metrics.get("expectancy")) + 0.01) / 0.05)
    profit_factor = _clamp((_float(metrics.get("profit_factor")) - 0.8) / 1.7)
    sample = _clamp(_float(metrics.get("sample_size")) / 120.0)
    drawdown_inverse = _clamp(1.0 - abs(_float(metrics.get("max_drawdown"))) / 0.35)
    consistency = _clamp(metrics.get("replay_backtest_consistency"))
    penalty = _float(penalties.get("proxy_data_penalty")) + _float(penalties.get("intraday_daily_mismatch_penalty")) + _float(penalties.get("missing_evidence_penalty"))
    class_adjustment = {
        "READY_FOR_PAPER_FORWARD_OBSERVATION": 0.04,
        "NEEDS_DATA_IMPROVEMENT": -0.01,
        "NEEDS_RULE_CLARIFICATION": -0.02,
        "TOO_PROXY_DEPENDENT": -0.04,
        "TOO_FRAGILE": -0.08,
        "REJECT_FOR_NOW": -0.20,
    }.get(classification, -0.1)
    raw = (
        0.34 * final_score
        + 0.18 * expectancy
        + 0.14 * profit_factor
        + 0.12 * sample
        + 0.10 * drawdown_inverse
        + 0.08 * consistency
        + _diversity_hint(mechanism, regime)
        + class_adjustment
        - penalty
    )
    return round(raw, 6)


def _excluded_candidates(candidates: list[dict[str, Any]], *, selected_ids: set[str]) -> list[dict[str, Any]]:
    rows = []
    for row in candidates:
        if row["candidate_id"] in selected_ids:
            continue
        if row["classification"] == "READY_FOR_PAPER_FORWARD_OBSERVATION":
            reason = "not selected due to diversity/campaign narrowing"
        else:
            reason = "; ".join(row["classification_reasons"])
        rows.append(
            {
                "candidate_id": row["candidate_id"],
                "mechanism": row["mechanism"],
                "regime": row["regime"],
                "classification": row["classification"],
                "reason": reason,
                "final_score": row["final_score"],
                "sample_size": row["sample_size"],
                "max_drawdown": row["max_drawdown"],
                "candidate_symbols": list(row.get("candidate_symbols", [])),
                "candidate_universe_symbols": list(row.get("candidate_universe_symbols", [])),
                "candidate_timeframes": list(row.get("candidate_timeframes", [])),
                "candidate_source_observation_ids": list(row.get("candidate_source_observation_ids", [])),
                "symbol_attribution_method": row.get("symbol_attribution_method", "UNKNOWN"),
                "symbol_attribution_confidence": row.get("symbol_attribution_confidence", 0.0),
            }
        )
    return sorted(rows, key=lambda value: (value["classification"], value["candidate_id"] or ""))


def _summary(candidates: list[dict[str, Any]], eligible: list[dict[str, Any]], ranked: list[dict[str, Any]], top_20: list[dict[str, Any]], campaign: list[dict[str, Any]], excluded: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "candidates_evaluated": len(candidates),
        "eligible_candidates_reviewed": len(eligible),
        "ranked_eligible_candidates": len(ranked),
        "top_20_count": len(top_20),
        "campaign_candidate_count": len(campaign),
        "excluded_candidate_count": len(excluded),
        "classification_counts": dict(Counter(row["classification"] for row in candidates)),
        "mechanism_distribution_top_20": dict(Counter(row["mechanism"] for row in top_20)),
        "regime_distribution_top_20": dict(Counter(row["regime"] for row in top_20)),
        "mechanism_distribution_campaign": dict(Counter(row["mechanism"] for row in campaign)),
        "regime_distribution_campaign": dict(Counter(row["regime"] for row in campaign)),
        "average_final_score_top_20": _average([row["final_score"] for row in top_20]),
        "average_expectancy_top_20": _average([row["expectancy"] for row in top_20]),
        "biggest_remaining_risk": _biggest_risk(campaign or top_20),
        "symbols_attributed_campaign": sum(bool(row.get("candidate_symbols")) for row in campaign),
        "symbol_attribution_method_counts_campaign": dict(Counter(row.get("symbol_attribution_method", "UNKNOWN") for row in campaign)),
    }


def _weaknesses(candidates: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "proxy_dependency": sum(row["proxy_penalty"] > 0 for row in candidates),
        "intraday_daily_mismatch": sum(row["intraday_daily_mismatch_penalty"] > 0 for row in candidates),
        "missing_evidence": sum(row["missing_evidence_penalty"] > 0 for row in candidates),
        "low_sample_size": sum(row["sample_size"] < 30 for row in candidates),
        "fragile_drawdown": sum(abs(row["max_drawdown"] or 0) > 0.20 for row in candidates),
    }


def _human_review_order(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "rank": index,
            "candidate_id": row["candidate_id"],
            "recommended_human_action": row["recommended_human_action"],
            "classification": row["classification"],
            "why": row["classification_reasons"],
        }
        for index, row in enumerate(rows, start=1)
    ]


def _human_action(classification: str) -> str:
    return {
        "READY_FOR_PAPER_FORWARD_OBSERVATION": "APPROVE_FOR_PAPER_FORWARD_OBSERVATION",
        "NEEDS_DATA_IMPROVEMENT": "REQUEST_SYMBOL_OR_INTRADAY_DATA_IMPROVEMENT",
        "NEEDS_RULE_CLARIFICATION": "REQUEST_RULE_CLARIFICATION",
        "TOO_PROXY_DEPENDENT": "REQUEST_DIRECT_SYMBOL_DATA_BEFORE_OBSERVATION",
        "TOO_FRAGILE": "REJECT_FOR_NOW",
        "REJECT_FOR_NOW": "REJECT_FOR_NOW",
    }.get(classification, "REQUEST_CLARIFICATION")


def _biggest_risk(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No campaign candidates selected."
    if any(row["proxy_penalty"] > 0 for row in rows):
        return "All selected candidates still depend on SPY daily proxy evidence until candidate-specific data is supplied."
    return "Remaining risk is ordinary paper-forward observation uncertainty."


def _source_report_paths(root: Path) -> dict[str, str]:
    names = [
        "backtest_aware_final_qualification",
        "observation_cluster_split_experiment",
        "top_candidate_deep_backtest",
        "observation_campaign_selection",
        "candidate_data_requirements",
    ]
    return {name: str(root / name / "latest.json") for name in names}


def _write_report(report: dict[str, Any], *, root: Path, filename: str, summary_filename: str, summary: str) -> dict[str, Path]:
    day_value = str(report.get("day") or date.today().isoformat())
    out_dir = root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / filename
    summary_path = out_dir / summary_filename
    latest_json = root / "latest.json"
    latest_summary = root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def _stringify_paths(paths: dict[str, Path]) -> dict[str, str]:
    return {key: str(value) for key, value in paths.items()}


def _validate_authority(report: dict[str, Any]) -> None:
    authority = report.get("authority_boundary", {})
    forbidden_true = [key for key, value in authority.items() if key != "paper_forward_observation_only" and value is True]
    if authority.get("paper_forward_observation_only") is not True or forbidden_true:
        raise ValueError(f"final candidate ranking authority boundary failed: {forbidden_true}")


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _diversity_hint(mechanism: Any, regime: Any) -> float:
    material = f"{mechanism}|{regime}"
    return (sum(ord(char) for char in material) % 17) / 1000.0


def _clamp(value: Any) -> float:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 0.0
    return min(1.0, max(0.0, _float(value)))


def _float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _round(value: Any) -> float:
    return round(_float(value), 6)


def _average(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return round(sum(clean) / len(clean), 6) if clean else None


def _median(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return round(statistics.median(clean), 6) if clean else None
