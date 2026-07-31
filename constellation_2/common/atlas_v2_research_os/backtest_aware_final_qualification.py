from __future__ import annotations

import copy
import json
import statistics
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import SUPPORTED_CLASSIFICATION, _load_local_spy_data
from .edge_qualification_models import EDGE_ELIGIBILITY_THRESHOLD
from .historical_replay_engine import now_utc
from .observation_cluster_splitting import (
    DEFAULT_OBSERVATION_COUNT,
    _cached_backtest,
    build_coarse_clusters,
    generate_observation_records,
    split_over_merged_clusters,
)
from .observation_to_claim import convert_observation_cluster_to_claim
from .observation_trial import (
    AUTHORITY_BOUNDARY,
    AUTHORITY_STATEMENT,
    _backtest_spec_from_trial,
    _edge_input_from_hypothesis,
    _hypothesis_from_claim,
    _run_replay,
)
from .edge_qualification import qualify_edge
from .historical_replay_results import edge_input_with_historical_replay
from .paper_trade_candidate_reports import build_paper_trade_candidate_report

REPORT_DIRNAME = "backtest_aware_final_qualification"
FINAL_QUALIFICATION_SCHEMA = "atlas_v2_research_os_backtest_aware_final_qualification"


def run_backtest_aware_final_qualification(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    observation_count: int = DEFAULT_OBSERVATION_COUNT,
    created_at: str | None = None,
) -> dict[str, Any]:
    report = build_backtest_aware_final_qualification_report(root=root, observation_count=observation_count, created_at=created_at)
    write_backtest_aware_final_qualification_report(report, root=root)
    return report


def build_backtest_aware_final_qualification_report(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    observation_count: int = DEFAULT_OBSERVATION_COUNT,
    created_at: str | None = None,
) -> dict[str, Any]:
    created = created_at or now_utc()
    records = generate_observation_records(observation_count, created_at=created)
    coarse_clusters = build_coarse_clusters(records)
    split_clusters = split_over_merged_clusters(records)
    data_rows, data_meta, data_limitations = _load_local_spy_data(None)
    backtest_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    final_trials = [
        _run_final_qualification_trial(cluster, index=index, created_at=created, data_rows=data_rows, data_meta=data_meta, backtest_cache=backtest_cache)
        for index, cluster in enumerate(split_clusters)
    ]
    supported = [row for row in final_trials if row["candidate_backtest"]["classification"] == SUPPORTED_CLASSIFICATION]
    final_passes = [row for row in final_trials if row["final_qualification"]["eligible"]]
    supported_passes = [row for row in supported if row["final_qualification"]["eligible"]]
    summary = _summary(final_trials, supported=supported, supported_passes=supported_passes, final_passes=final_passes)
    report = {
        "schema_id": FINAL_QUALIFICATION_SCHEMA,
        "schema_version": "1.0",
        "report_type": "BACKTEST_AWARE_FINAL_QUALIFICATION",
        "created_at": created,
        "day": created[:10] or date.today().isoformat(),
        "objective": "Apply a second-stage Research OS qualification after candidate backtest evidence, without changing the 0.70 eligibility threshold.",
        "threshold": EDGE_ELIGIBILITY_THRESHOLD,
        "source": {
            "observation_count": observation_count,
            "coarse_cluster_count": len(coarse_clusters),
            "split_cluster_count": len(split_clusters),
            "source_profile": f"OBSERVATION_IMPORT_{observation_count}_RECONSTRUCTED_LOCAL_STRUCTURED_SOURCE",
        },
        "qualification_flow": [
            "preliminary qualification after historical replay",
            "candidate backtest",
            "backtest-aware final qualification at unchanged 0.70 threshold",
        ],
        "summary": summary,
        "component_suppression": _component_suppression(final_trials),
        "threshold_sensitivity": _threshold_sensitivity(final_trials),
        "backtest_supported_examples": _rank_trials(supported)[:25],
        "final_eligible_examples": _rank_trials(final_passes)[:25],
        "trials": final_trials,
        "sample_trials": final_trials[:30],
        "backtest_cache_entries": len(backtest_cache),
        "data_source": data_meta,
        "data_limitations": data_limitations,
        "answer": _answer(summary),
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": {
            "live_trading_authorized": False,
            "broker_execution_authorized": False,
            "capital_authorized": False,
            "position_sizing_authorized": False,
            "portfolio_construction_authorized": False,
            "trade_recommendation_authorized": False,
            "candidate_production_promotion_authorized": False,
            "automatic_paper_trade_placement_authorized": False,
        },
        "limitations": [
            AUTHORITY_STATEMENT,
            "Final qualification is research-only and authorizes at most human-reviewed paper-forward observation consideration.",
            "Backtests use local SPY adjusted daily proxy data when candidate-specific local data is absent.",
            "Daily proxy limitations and intraday/daily mismatch are explicit score penalties.",
        ],
    }
    _validate_authority(report)
    return report


def compute_backtest_aware_final_qualification(preliminary_qualification: dict[str, Any], candidate_backtest: dict[str, Any]) -> dict[str, Any]:
    preliminary_score = float((preliminary_qualification.get("score") or {}).get("edge_score") or 0.0)
    backtest_score = compute_backtest_evidence_score(candidate_backtest)
    penalties = compute_backtest_penalties(candidate_backtest)
    gross_credit = round(0.08 * backtest_score["backtest_evidence_score"], 6)
    total_penalty = round(sum(penalties.values()), 6)
    final_score = round(_clamp(preliminary_score + gross_credit - total_penalty), 6)
    inherited_disqualifications = [
        reason
        for reason in preliminary_qualification.get("disqualification_reasons", [])
        if str(reason) != f"edge_score below {EDGE_ELIGIBILITY_THRESHOLD}"
    ]
    disqualifications = list(inherited_disqualifications)
    reasons = [
        "preliminary qualification evaluated after historical replay",
        "candidate backtest evaluated before final qualification",
        "backtest evidence included in final score",
        "proxy and intraday/daily limitations penalized explicitly",
    ]
    if final_score >= EDGE_ELIGIBILITY_THRESHOLD:
        reasons.append(f"final_score >= {EDGE_ELIGIBILITY_THRESHOLD}")
    else:
        disqualifications.append(f"final_score below {EDGE_ELIGIBILITY_THRESHOLD}")
    if candidate_backtest.get("classification") == SUPPORTED_CLASSIFICATION:
        reasons.append("backtest classification BACKTEST_SUPPORTED")
    else:
        disqualifications.append(f"backtest classification {candidate_backtest.get('classification') or 'UNKNOWN'}")
    eligible = not disqualifications
    return {
        "stage": "FINAL_AFTER_BACKTEST",
        "threshold": EDGE_ELIGIBILITY_THRESHOLD,
        "preliminary_edge_score": round(preliminary_score, 6),
        "backtest_evidence_score": backtest_score["backtest_evidence_score"],
        "backtest_components": backtest_score["components"],
        "backtest_component_weights": backtest_score["weights"],
        "gross_backtest_credit": gross_credit,
        "penalties": penalties,
        "total_penalty": total_penalty,
        "final_score": final_score,
        "eligible": eligible,
        "qualification_reasons": reasons,
        "disqualification_reasons": disqualifications,
        "research_only": True,
        "authority_level": "HUMAN_REVIEWED_PAPER_TESTING_CONSIDERATION",
    }


def compute_backtest_evidence_score(candidate_backtest: dict[str, Any]) -> dict[str, Any]:
    metrics = candidate_backtest.get("metrics") or {}
    sample_size = int(metrics.get("sample_size") or 0)
    expectancy = _float(metrics.get("expectancy"))
    profit_factor = _float(metrics.get("profit_factor"))
    max_drawdown = abs(_float(metrics.get("max_drawdown")))
    components = {
        "expectancy": _clamp((expectancy + 0.01) / 0.05),
        "profit_factor": _clamp((profit_factor - 0.8) / 1.7),
        "sample_size": _clamp(sample_size / 120.0),
        "max_drawdown_inverse": _clamp(1.0 - (max_drawdown / 0.35)),
        "backtest_consistency": _clamp(metrics.get("replay_backtest_consistency")),
        "mechanism_regime_consistency": _mechanism_regime_consistency(metrics, candidate_backtest),
    }
    weights = {
        "expectancy": 0.25,
        "profit_factor": 0.20,
        "sample_size": 0.15,
        "max_drawdown_inverse": 0.15,
        "backtest_consistency": 0.15,
        "mechanism_regime_consistency": 0.10,
    }
    score = round(sum(weights[key] * components[key] for key in weights), 6)
    return {"backtest_evidence_score": score, "components": components, "weights": weights}


def compute_backtest_penalties(candidate_backtest: dict[str, Any]) -> dict[str, float]:
    metrics = candidate_backtest.get("metrics") or {}
    warnings = [str(value).lower() for value in candidate_backtest.get("warnings", [])]
    missing = candidate_backtest.get("missing_data", []) or []
    sample_size = int(metrics.get("sample_size") or 0)
    penalties = {
        "proxy_data_penalty": 0.025 if any("spy adjusted daily data as local proxy" in warning for warning in warnings) else 0.0,
        "intraday_daily_mismatch_penalty": 0.015 if any("daily bars cannot fully test intraday" in warning for warning in warnings) else 0.0,
        "sample_size_penalty": round(max(0.0, (30.0 - float(sample_size)) / 30.0) * 0.02, 6),
        "missing_evidence_penalty": 0.015 if missing else 0.0,
        "negative_backtest_penalty": 0.02 if candidate_backtest.get("classification") != SUPPORTED_CLASSIFICATION else 0.0,
    }
    return penalties


def write_backtest_aware_final_qualification_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day_value = str(report.get("day") or date.today().isoformat())
    out_dir = root_path / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "backtest_aware_final_qualification_report.json"
    summary_path = out_dir / "backtest_aware_final_qualification_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_backtest_aware_final_qualification_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_backtest_aware_final_qualification_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    suppression = report.get("component_suppression", {})
    lines = [
        "# Backtest-Aware Final Qualification",
        "",
        f"Created: {report.get('created_at')}",
        f"Threshold: {report.get('threshold')}",
        "",
        "## Result",
        "",
        f"- Candidates evaluated: {summary.get('candidates_evaluated')}",
        f"- Backtest-supported candidates: {summary.get('backtest_supported_candidates')}",
        f"- Final eligible candidates: {summary.get('final_eligible_candidates')}",
        f"- Backtest-supported final passes: {summary.get('backtest_supported_final_passes')}",
        f"- Preliminary eligible candidates: {summary.get('preliminary_eligible_candidates')}",
        f"- Average preliminary edge score: {summary.get('average_preliminary_edge_score')}",
        f"- Average final score: {summary.get('average_final_score')}",
        f"- Median final score: {summary.get('median_final_score')}",
        "",
        "## Suppressors",
        "",
        f"- Average backtest evidence score: {suppression.get('average_backtest_evidence_score')}",
        f"- Average gross backtest credit: {suppression.get('average_gross_backtest_credit')}",
        f"- Average total penalty: {suppression.get('average_total_penalty')}",
        f"- Proxy penalty count: {suppression.get('proxy_penalty_count')}",
        f"- Intraday/daily mismatch penalty count: {suppression.get('intraday_daily_mismatch_penalty_count')}",
        f"- Sample-size penalty count: {suppression.get('sample_size_penalty_count')}",
        "",
        "## Threshold Sensitivity",
        "",
    ]
    for row in report.get("threshold_sensitivity", []):
        lines.append(f"- {row.get('threshold')}: all={row.get('all_candidates_pass')}, backtest_supported={row.get('backtest_supported_pass')}")
    lines.extend([
        "",
        "## Answer",
        "",
        str(report.get("answer")),
        "",
        "## Guardrails",
        "",
        "Research-only final qualification. No live trading, broker execution, capital allocation, position sizing, portfolio construction, trade recommendations, automatic paper placement, or candidate production promotion authority is added.",
        "",
    ])
    return "\n".join(lines)


def _run_final_qualification_trial(
    cluster: dict[str, Any],
    *,
    index: int,
    created_at: str,
    data_rows: list[dict[str, Any]],
    data_meta: dict[str, Any],
    backtest_cache: dict[tuple[Any, ...], dict[str, Any]],
) -> dict[str, Any]:
    claim = convert_observation_cluster_to_claim(cluster)
    hypothesis = _hypothesis_from_claim(claim, index=index, created_at=created_at)
    replay = _run_replay(hypothesis, claim=claim, created_at=created_at)
    edge_input = _edge_input_from_hypothesis(hypothesis, claim=claim, replay=replay)
    replay_edge_input = edge_input_with_historical_replay(edge_input, replay)
    preliminary = qualify_edge(replay_edge_input, created_at=created_at)
    candidate_id = _stable_candidate_id(hypothesis["hypothesis_id"], replay["replay_id"])
    candidate_report = build_paper_trade_candidate_report(replay_edge_input, candidate_id=candidate_id, created_at=created_at)
    backtest_spec = _backtest_spec_from_trial(hypothesis, candidate_report, replay, data_meta=data_meta)
    backtest = _cached_backtest(backtest_spec, data_rows=data_rows, created_at=created_at, cache=backtest_cache)
    final = compute_backtest_aware_final_qualification(preliminary, backtest)
    return {
        "cluster_id": cluster.get("cluster_id"),
        "candidate_id": candidate_id,
        "mechanism": hypothesis["mechanism"],
        "regime": hypothesis["regime"],
        "split_dimensions": (cluster.get("metadata") or {}).get("split_dimensions", {}),
        "candidate_symbols": list(cluster.get("symbols", [])),
        "candidate_universe_symbols": list(cluster.get("symbols", [])) if len(cluster.get("symbols", [])) > 1 else [],
        "candidate_timeframes": list(cluster.get("timeframes", [])),
        "candidate_source_types": list(cluster.get("source_types", [])),
        "candidate_source_observation_ids": list(cluster.get("source_observation_ids", [])),
        "symbol_counts": dict(cluster.get("symbol_counts", {}) if isinstance(cluster.get("symbol_counts", {}), dict) else {}),
        "timeframe_counts": dict(cluster.get("timeframe_counts", {}) if isinstance(cluster.get("timeframe_counts", {}), dict) else {}),
        "source_type_counts": dict(cluster.get("source_type_counts", {}) if isinstance(cluster.get("source_type_counts", {}), dict) else {}),
        "preliminary_qualification": {
            "qualification_id": preliminary["qualification_id"],
            "eligible": preliminary["eligible"],
            "edge_score": preliminary["score"]["edge_score"],
            "disqualification_reasons": preliminary["disqualification_reasons"],
        },
        "historical_replay": {
            "replay_id": replay["replay_id"],
            "status": replay["certification"]["status"],
            "score": replay["metrics"]["historical_replay_score"],
            "sample_size": replay["sample_size"],
            "expectancy": replay["metrics"].get("expectancy"),
        },
        "candidate_backtest": {
            "classification": backtest["classification"],
            "metrics": backtest["metrics"],
            "warnings": backtest["warnings"],
            "missing_data": backtest["missing_data"],
            "paper_forward_recommendation": backtest["paper_forward_recommendation"],
        },
        "final_qualification": final,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
    }


def _summary(
    trials: list[dict[str, Any]],
    *,
    supported: list[dict[str, Any]],
    supported_passes: list[dict[str, Any]],
    final_passes: list[dict[str, Any]],
) -> dict[str, Any]:
    preliminary_scores = [row["preliminary_qualification"]["edge_score"] for row in trials]
    final_scores = [row["final_qualification"]["final_score"] for row in trials]
    supported_final_scores = [row["final_qualification"]["final_score"] for row in supported]
    return {
        "candidates_evaluated": len(trials),
        "preliminary_eligible_candidates": sum(row["preliminary_qualification"]["eligible"] for row in trials),
        "backtest_supported_candidates": len(supported),
        "final_eligible_candidates": len(final_passes),
        "backtest_supported_final_passes": len(supported_passes),
        "backtest_supported_pass_rate": _rate(len(supported_passes), len(supported)),
        "average_preliminary_edge_score": _average(preliminary_scores),
        "median_preliminary_edge_score": _median(preliminary_scores),
        "average_final_score": _average(final_scores),
        "median_final_score": _median(final_scores),
        "average_supported_final_score": _average(supported_final_scores),
        "main_final_disqualification_reasons": dict(Counter(reason for row in trials for reason in row["final_qualification"]["disqualification_reasons"])),
        "backtest_classifications": dict(Counter(row["candidate_backtest"]["classification"] for row in trials)),
        "mechanism_final_pass_counts": dict(Counter(row["mechanism"] for row in final_passes)),
    }


def _component_suppression(trials: list[dict[str, Any]]) -> dict[str, Any]:
    evidence_scores = [row["final_qualification"]["backtest_evidence_score"] for row in trials]
    gross_credits = [row["final_qualification"]["gross_backtest_credit"] for row in trials]
    penalties = [row["final_qualification"]["total_penalty"] for row in trials]
    return {
        "average_backtest_evidence_score": _average(evidence_scores),
        "average_gross_backtest_credit": _average(gross_credits),
        "average_total_penalty": _average(penalties),
        "proxy_penalty_count": sum(row["final_qualification"]["penalties"]["proxy_data_penalty"] > 0 for row in trials),
        "intraday_daily_mismatch_penalty_count": sum(row["final_qualification"]["penalties"]["intraday_daily_mismatch_penalty"] > 0 for row in trials),
        "sample_size_penalty_count": sum(row["final_qualification"]["penalties"]["sample_size_penalty"] > 0 for row in trials),
        "missing_evidence_penalty_count": sum(row["final_qualification"]["penalties"]["missing_evidence_penalty"] > 0 for row in trials),
        "component_average": _component_averages(trials),
    }


def _threshold_sensitivity(trials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for threshold in [0.70, 0.65, 0.60, 0.55]:
        all_pass = [row for row in trials if row["final_qualification"]["final_score"] >= threshold]
        supported_pass = [row for row in all_pass if row["candidate_backtest"]["classification"] == SUPPORTED_CLASSIFICATION]
        rows.append({"threshold": threshold, "all_candidates_pass": len(all_pass), "backtest_supported_pass": len(supported_pass)})
    return rows


def _rank_trials(trials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in trials:
        metrics = row["candidate_backtest"]["metrics"]
        rows.append(
            {
                "candidate_id": row["candidate_id"],
                "mechanism": row["mechanism"],
                "regime": row["regime"],
                "preliminary_edge_score": row["preliminary_qualification"]["edge_score"],
                "final_score": row["final_qualification"]["final_score"],
                "backtest_evidence_score": row["final_qualification"]["backtest_evidence_score"],
                "classification": row["candidate_backtest"]["classification"],
                "expectancy": metrics.get("expectancy"),
                "profit_factor": metrics.get("profit_factor"),
                "sample_size": metrics.get("sample_size"),
                "max_drawdown": metrics.get("max_drawdown"),
                "penalties": copy.deepcopy(row["final_qualification"]["penalties"]),
                "disqualification_reasons": list(row["final_qualification"]["disqualification_reasons"]),
            }
        )
    return sorted(rows, key=lambda value: (value["final_score"], value["backtest_evidence_score"]), reverse=True)


def _component_averages(trials: list[dict[str, Any]]) -> dict[str, float | None]:
    keys = ["expectancy", "profit_factor", "sample_size", "max_drawdown_inverse", "backtest_consistency", "mechanism_regime_consistency"]
    return {key: _average([row["final_qualification"]["backtest_components"][key] for row in trials]) for key in keys}


def _answer(summary: dict[str, Any]) -> str:
    passes = int(summary.get("backtest_supported_final_passes") or 0)
    supported = int(summary.get("backtest_supported_candidates") or 0)
    if passes:
        return f"{passes} of {supported} backtest-supported candidates pass final qualification after including backtest evidence and proxy penalties."
    return f"0 of {supported} backtest-supported candidates pass final qualification; backtest evidence is still outweighed by proxy, mismatch, or base-score limitations."


def _mechanism_regime_consistency(metrics: dict[str, Any], candidate_backtest: dict[str, Any]) -> float:
    regime_perf = metrics.get("regime_specific_performance") or {}
    if not isinstance(regime_perf, dict) or not regime_perf:
        return 0.0
    positive = 0
    total = 0
    for row in regime_perf.values():
        if not isinstance(row, dict):
            continue
        total += 1
        if _float(row.get("expectancy")) > 0 and _float(row.get("win_rate")) >= 0.5:
            positive += 1
    base = _rate(positive, total)
    if candidate_backtest.get("classification") == SUPPORTED_CLASSIFICATION:
        base = max(base, 0.55)
    return _clamp(base)


def _stable_candidate_id(hypothesis_id: str, replay_id: str) -> str:
    from .edge_qualification import stable_id

    return stable_id("ptc_backtest_final", [hypothesis_id, replay_id])


def _validate_authority(report: dict[str, Any]) -> None:
    authority = report.get("authority_boundary", {})
    guardrails = report.get("guardrails", {})
    forbidden_true = [key for key, value in {**authority, **guardrails}.items() if key != "research_only" and value is True]
    if authority.get("research_only") is not True or forbidden_true:
        raise ValueError(f"final qualification authority boundary failed: {forbidden_true}")


def _clamp(value: Any) -> float:
    return min(1.0, max(0.0, _float(value)))


def _float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _average(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _median(values: list[float]) -> float | None:
    return round(statistics.median(values), 6) if values else None


def _rate(numerator: int | float, denominator: int | float) -> float:
    return round(float(numerator) / float(denominator), 6) if denominator else 0.0
