from __future__ import annotations

import copy
import json
import statistics
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .bulk_observation_import import observation_record_from_raw, stable_id, structured_observation_rows
from .candidate_backtests import SUPPORTED_CLASSIFICATION, _load_local_spy_data, run_candidate_backtest_spec
from .edge_qualification import qualify_edge
from .historical_replay_engine import now_utc
from .historical_replay_results import edge_input_with_historical_replay
from .observation_deduplication import normalize_observation_text
from .observation_to_claim import convert_observation_cluster_to_claim
from .observation_trial import (
    AUTHORITY_BOUNDARY,
    AUTHORITY_STATEMENT,
    _backtest_spec_from_trial,
    _edge_input_from_hypothesis,
    _hypothesis_from_claim,
    _run_replay,
)
from .paper_trade_candidate_reports import build_paper_trade_candidate_report
from .regime_expansion import regime_metric_block, normalize_regime

REPORT_DIRNAME = "observation_cluster_split_experiment"
DEFAULT_OBSERVATION_COUNT = 100000
SPLIT_DIMENSIONS = ["mechanism", "regime", "timeframe", "source_type", "symbol_group"]
INDEX_ETFS = {"SPY", "QQQ", "IWM", "DIA"}
MEGA_CAPS = {"AAPL", "MSFT", "NVDA", "TSLA", "AMD", "META", "GOOGL", "AMZN", "NFLX", "JPM", "BAC"}
SECTOR_ETFS = {"XLE", "USO", "GLD", "TLT", "DBC"}


def symbol_group(symbol: str | None) -> str:
    value = str(symbol or "").upper().strip()
    if not value:
        return "unknown"
    if value in INDEX_ETFS:
        return "index_etf"
    if value in MEGA_CAPS:
        return "mega_cap"
    if value in SECTOR_ETFS:
        return "sector_etf"
    return "single_name"


def coarse_cluster_key(record: dict[str, Any]) -> tuple[str, str, str]:
    normalized = normalize_observation_text(record.get("observation_text", ""))
    signature = " ".join(normalized.split()[:6])
    return (str(record.get("mechanism", "UNKNOWN")).upper(), normalize_regime(record.get("regime")), signature)


def split_cluster_key(record: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    mechanism, regime, signature = coarse_cluster_key(record)
    source_type = str((record.get("metadata") or {}).get("source_type") or "UNKNOWN")
    return (mechanism, regime, str(record.get("timeframe") or "UNKNOWN").lower(), source_type, symbol_group(record.get("symbol")), signature)


def generate_observation_records(count: int = DEFAULT_OBSERVATION_COUNT, *, created_at: str | None = None) -> list[dict[str, Any]]:
    created = created_at or now_utc()
    rows = structured_observation_rows(count=count, profile_name=f"OBSERVATION_IMPORT_{count}")
    return [observation_record_from_raw(row, index=index, created_at=created) for index, row in enumerate(rows)]


def build_coarse_clusters(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[coarse_cluster_key(record)].append(record)
    return [_cluster_from_records(rows, key=key, split=False) for key, rows in sorted(groups.items())]


def split_over_merged_clusters(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[split_cluster_key(record)].append(record)
    return [_cluster_from_records(rows, key=key, split=True) for key, rows in sorted(groups.items())]


def run_observation_cluster_split_experiment(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    observation_count: int = DEFAULT_OBSERVATION_COUNT,
    created_at: str | None = None,
) -> dict[str, Any]:
    report = build_observation_cluster_split_experiment(root=root, observation_count=observation_count, created_at=created_at)
    write_observation_cluster_split_experiment(report, root=root)
    return report


def build_observation_cluster_split_experiment(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    observation_count: int = DEFAULT_OBSERVATION_COUNT,
    created_at: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or now_utc()
    records = generate_observation_records(observation_count, created_at=created)
    coarse_clusters = build_coarse_clusters(records)
    split_clusters = split_over_merged_clusters(records)
    data_rows, data_meta, data_limitations = _load_local_spy_data(None)
    backtest_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    split_trials = [
        _run_split_cluster_trial(cluster, index=index, created_at=created, data_rows=data_rows, data_meta=data_meta, backtest_cache=backtest_cache)
        for index, cluster in enumerate(split_clusters)
    ]
    pre_metrics = _pre_split_metrics(root_path, coarse_clusters=coarse_clusters, observation_count=observation_count)
    post_metrics = _metrics(split_clusters, split_trials, observation_count=observation_count)
    comparison = _comparison(pre_metrics, post_metrics)
    report = {
        "schema_id": "atlas_v2_research_os_observation_cluster_split_experiment",
        "schema_version": "1.0",
        "report_type": "OBSERVATION_CLUSTER_SPLIT_EXPERIMENT",
        "created_at": created,
        "day": created[:10] or date.today().isoformat(),
        "objective": "Split over-merged observation clusters by mechanism, regime, timeframe, source_type, and symbol_group, then compare candidate funnel quality.",
        "split_dimensions": list(SPLIT_DIMENSIONS),
        "source": {
            "observation_count": observation_count,
            "coarse_cluster_count": len(coarse_clusters),
            "split_cluster_count": len(split_clusters),
            "source_profile": f"OBSERVATION_IMPORT_{observation_count}_RECONSTRUCTED_LOCAL_STRUCTURED_SOURCE",
        },
        "pre_split": pre_metrics,
        "post_split": post_metrics,
        "comparison": comparison,
        "top_split_mechanisms": _rank_mechanisms(split_trials, reverse=True),
        "weak_split_mechanisms": _rank_mechanisms(split_trials, reverse=False),
        "sample_split_clusters": split_clusters[:30],
        "sample_split_trials": split_trials[:30],
        "backtest_cache_entries": len(backtest_cache),
        "data_source": data_meta,
        "data_limitations": data_limitations,
        "answer": _answer(comparison),
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
            "Split experiment uses reconstructed deterministic local observation rows and local SPY proxy backtests.",
            "Positive evidence remains paper-forward observation research only.",
        ],
    }
    _validate_authority(report)
    return report


def write_observation_cluster_split_experiment(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day_value = str(report.get("day") or date.today().isoformat())
    out_dir = root_path / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "observation_cluster_split_experiment.json"
    summary_path = out_dir / "observation_cluster_split_experiment_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_observation_cluster_split_experiment_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_observation_cluster_split_experiment_summary(report: dict[str, Any]) -> str:
    pre = report.get("pre_split", {})
    post = report.get("post_split", {})
    comparison = report.get("comparison", {})
    lines = [
        "# Observation Cluster Split Experiment",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Before vs After",
        "",
        f"- Pre-split clusters: {pre.get('clusters')}",
        f"- Post-split clusters: {post.get('clusters')}",
        f"- Claim increase: {comparison.get('claim_increase')}",
        f"- Hypothesis increase: {comparison.get('hypothesis_increase')}",
        f"- Positive replay rate change: {comparison.get('positive_replay_rate_change')}",
        f"- Eligible candidate change: {comparison.get('eligible_candidate_change')}",
        f"- Backtest-supported candidate change: {comparison.get('backtest_supported_candidate_change')}",
        f"- Paper-forward-ready candidate change: {comparison.get('paper_forward_ready_candidate_change')}",
        f"- Average edge score change: {comparison.get('average_edge_score_change')}",
        "",
        "## Post-Split Metrics",
        "",
        f"- Clusters: {post.get('clusters')}",
        f"- Claims: {post.get('claims')}",
        f"- Hypotheses: {post.get('hypotheses')}",
        f"- Historical replays: {post.get('historical_replays')}",
        f"- Positive replay rate: {post.get('positive_replay_rate')}",
        f"- Claims by regime: {post.get('regime_metrics', {}).get('claims_by_regime', {})}",
        f"- Hypotheses by regime: {post.get('regime_metrics', {}).get('hypotheses_by_regime', {})}",
        f"- Eligible candidates by regime: {post.get('regime_metrics', {}).get('eligible_candidates_by_regime', {})}",
        f"- Eligible candidates: {post.get('eligible_candidates')}",
        f"- Backtest-supported candidates: {post.get('backtest_supported_candidates')}",
        f"- Paper-forward-ready candidates: {post.get('paper_forward_ready_candidates')}",
        f"- Average edge score: {post.get('average_edge_score')}",
        f"- Median edge score: {post.get('median_edge_score')}",
        "",
        "## Answer",
        "",
        str(report.get("answer")),
        "",
        "## Top Split Mechanisms",
        "",
    ]
    for row in report.get("top_split_mechanisms", [])[:10]:
        lines.append(f"- {row.get('mechanism')}: ready={row.get('paper_forward_ready_candidates')}, eligible={row.get('eligible_candidates')}, supported={row.get('backtest_supported_candidates')}, edge={row.get('average_edge_score')}")
    lines.extend(["", "## Guardrails", "", "Research-only split experiment. No live trading, broker execution, capital allocation, position sizing, portfolio construction, trade recommendations, automatic paper placement, or candidate production promotion authority is added.", ""])
    return "\n".join(lines)


def _cluster_from_records(rows: list[dict[str, Any]], *, key: tuple[Any, ...], split: bool) -> dict[str, Any]:
    if split:
        mechanism, regime, timeframe, source_type, group, signature = key
        cluster_id = stable_id("obs_split_cluster", [mechanism, regime, timeframe, source_type, group, signature, sorted(row["observation_id"] for row in rows)])
        metadata = {
            "measurement_only": True,
            "normalized_signature": signature,
            "split_cluster": True,
            "split_dimensions": {"mechanism": mechanism, "regime": regime, "timeframe": timeframe, "source_type": source_type, "symbol_group": group},
        }
    else:
        mechanism, regime, signature = key
        cluster_id = stable_id("obs_cluster", [mechanism, regime, signature, sorted(row["observation_id"] for row in rows)])
        metadata = {"measurement_only": True, "normalized_signature": signature, "split_cluster": False}
    confidences = [float(row.get("confidence") or 0.0) for row in rows]
    symbol_counts = Counter(str(row.get("symbol") or "UNKNOWN").upper() for row in rows)
    timeframe_counts = Counter(str(row.get("timeframe") or "UNKNOWN").lower() for row in rows)
    source_type_counts = Counter(str((row.get("metadata") or {}).get("source_type") or "UNKNOWN") for row in rows)
    return {
        "cluster_id": cluster_id,
        "parent_cluster_key": "|".join([str(mechanism), str(regime), str(signature)]),
        "mechanism": str(mechanism).upper(),
        "regime": str(regime).upper(),
        "symbols": sorted(symbol_counts),
        "timeframes": sorted(timeframe_counts),
        "source_types": sorted(source_type_counts),
        "symbol_counts": dict(symbol_counts),
        "timeframe_counts": dict(timeframe_counts),
        "source_type_counts": dict(source_type_counts),
        "symbol_groups": sorted({symbol_group(row.get("symbol")) for row in rows}),
        "source_observation_ids": sorted(row["observation_id"] for row in rows),
        "cluster_summary": f"{len(rows)} {mechanism} observations in {regime} regime split by {', '.join(SPLIT_DIMENSIONS) if split else 'coarse key'}: {signature}",
        "confidence": round(sum(confidences) / len(confidences), 6) if confidences else 0.0,
        "observation_count": len(rows),
        "metadata": metadata,
        "example_observations": [
            {
                "observation_id": row.get("observation_id"),
                "symbol": row.get("symbol"),
                "timeframe": row.get("timeframe"),
                "source_type": (row.get("metadata") or {}).get("source_type"),
                "confidence": row.get("confidence"),
                "observation_text": row.get("observation_text"),
            }
            for row in rows[:3]
        ],
    }


def _run_split_cluster_trial(
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
    edge_input.metadata.update(_source_lineage_from_cluster(cluster, claim))
    replay_edge_input = edge_input_with_historical_replay(edge_input, replay)
    qualification = qualify_edge(replay_edge_input, created_at=created_at)
    candidate_id = stable_id("ptc_obs_split", [hypothesis["hypothesis_id"], replay["replay_id"]])
    candidate_report = build_paper_trade_candidate_report(replay_edge_input, candidate_id=candidate_id, created_at=created_at)
    backtest_spec = _backtest_spec_from_trial(hypothesis, candidate_report, replay, data_meta=data_meta)
    backtest = _cached_backtest(backtest_spec, data_rows=data_rows, created_at=created_at, cache=backtest_cache)
    paper_forward_ready = candidate_report.get("paper_trade_eligible") is True and backtest.get("classification") == SUPPORTED_CLASSIFICATION
    return {
        "cluster_id": cluster.get("cluster_id"),
        "parent_cluster_key": cluster.get("parent_cluster_key"),
        "claim_seed_id": claim.get("claim_seed_id"),
        "hypothesis_id": hypothesis["hypothesis_id"],
        "candidate_id": candidate_id,
        "mechanism": hypothesis["mechanism"],
        "regime": hypothesis["regime"],
        "symbols": list(claim.get("symbols", [])),
        "timeframes": list(claim.get("timeframes", [])),
        "source_types": list(cluster.get("source_types", [])),
        "source_observation_ids": list(claim.get("source_observation_ids", [])),
        "source_lineage": _source_lineage_from_cluster(cluster, claim),
        "split_dimensions": (cluster.get("metadata") or {}).get("split_dimensions", {}),
        "claim": claim,
        "historical_replay": {
            "replay_id": replay["replay_id"],
            "status": replay["certification"]["status"],
            "score": replay["metrics"]["historical_replay_score"],
            "sample_size": replay["sample_size"],
            "expectancy": replay["metrics"].get("expectancy"),
        },
        "edge_qualification": {
            "qualification_id": qualification["qualification_id"],
            "eligible": qualification["eligible"],
            "edge_score": qualification["score"]["edge_score"],
            "disqualification_reasons": qualification["disqualification_reasons"],
        },
        "paper_trade_candidate": {
            "candidate_id": candidate_report["candidate"]["candidate_id"],
            "paper_trade_eligible": candidate_report["paper_trade_eligible"],
            "certification_status": candidate_report["certification"]["status"],
            "human_review_required": candidate_report["human_review_required"],
            "authority_level": candidate_report["authority_level"],
            "candidate_symbols": candidate_report["candidate"].get("candidate_symbols", []),
            "candidate_universe_symbols": candidate_report["candidate"].get("candidate_universe_symbols", []),
            "candidate_timeframes": candidate_report["candidate"].get("candidate_timeframes", []),
            "candidate_source_observation_ids": candidate_report["candidate"].get("candidate_source_observation_ids", []),
            "symbol_attribution_method": candidate_report["candidate"].get("symbol_attribution_method", "UNKNOWN"),
            "symbol_attribution_confidence": candidate_report["candidate"].get("symbol_attribution_confidence", 0.0),
        },
        "candidate_backtest": {
            "classification": backtest["classification"],
            "metrics": backtest["metrics"],
            "missing_data": backtest["missing_data"],
            "warnings": backtest["warnings"],
            "paper_forward_recommendation": backtest["paper_forward_recommendation"],
        },
        "paper_forward_ready": paper_forward_ready,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
    }


def _source_lineage_from_cluster(cluster: dict[str, Any], claim: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbols": list(claim.get("symbols", []) or cluster.get("symbols", [])),
        "timeframes": list(claim.get("timeframes", []) or cluster.get("timeframes", [])),
        "source_types": list(cluster.get("source_types", [])),
        "source_observation_ids": list(claim.get("source_observation_ids", []) or cluster.get("source_observation_ids", [])),
        "symbol_counts": dict(cluster.get("symbol_counts", {})),
        "timeframe_counts": dict(cluster.get("timeframe_counts", {})),
        "source_type_counts": dict(cluster.get("source_type_counts", {})),
        "mechanism": claim.get("mechanism") or cluster.get("mechanism"),
        "regime": claim.get("regime") or cluster.get("regime"),
        "source_cluster_id": cluster.get("cluster_id"),
        "lineage_path": "ObservationRecord->ObservationCluster->Claim->ResearchHypothesis->HistoricalReplay->EdgeQualification->PaperTradeCandidate",
    }


def _cached_backtest(backtest_spec: dict[str, Any], *, data_rows: list[dict[str, Any]], created_at: str, cache: dict[tuple[Any, ...], dict[str, Any]]) -> dict[str, Any]:
    key = (
        backtest_spec.get("mechanism"),
        backtest_spec.get("primary_regime"),
        tuple(backtest_spec.get("allowed_regimes") or []),
        backtest_spec.get("horizon_days"),
    )
    if key not in cache:
        cache[key] = run_candidate_backtest_spec(backtest_spec, data_rows, created_at=created_at)
    return copy.deepcopy(cache[key])


def _pre_split_metrics(root: Path, *, coarse_clusters: list[dict[str, Any]], observation_count: int) -> dict[str, Any]:
    stress = _read_json(root / "observation_scale_stress" / "latest.json", {})
    stress_result = None
    if isinstance(stress.get("results"), list):
        stress_result = next((row for row in stress["results"] if row.get("level") == observation_count), None)
    if stress_result:
        return {
            "observations": int(stress_result.get("observations_imported") or observation_count),
            "clusters": int(stress_result.get("clusters_created") or len(coarse_clusters)),
            "claims": int(stress_result.get("claims_generated") or len(coarse_clusters)),
            "hypotheses": int(stress_result.get("hypotheses_generated") or len(coarse_clusters)),
            "historical_replays": int(stress_result.get("historical_replays_executed") or len(coarse_clusters)),
            "positive_replay_rate": _nullable_float(stress_result.get("positive_replay_rate")),
            "eligible_candidates": int(stress_result.get("eligible_candidates") or 0),
            "backtest_supported_candidates": int(stress_result.get("backtest_supported_candidates") or 0),
            "paper_forward_ready_candidates": int(stress_result.get("paper_forward_ready_candidates") or 0),
            "average_edge_score": None,
            "median_edge_score": None,
            "source": "observation_scale_stress/latest.json",
        }
    return {
        "observations": observation_count,
        "clusters": len(coarse_clusters),
        "claims": len(coarse_clusters),
        "hypotheses": len(coarse_clusters),
        "historical_replays": len(coarse_clusters),
        "positive_replay_rate": None,
        "eligible_candidates": 0,
        "backtest_supported_candidates": 0,
        "paper_forward_ready_candidates": 0,
        "average_edge_score": None,
        "median_edge_score": None,
        "source": "reconstructed_coarse_clusters",
    }


def _metrics(clusters: list[dict[str, Any]], trials: list[dict[str, Any]], *, observation_count: int) -> dict[str, Any]:
    positive = [row for row in trials if row.get("historical_replay", {}).get("status") == "REPLAY_POSITIVE"]
    eligible = [row for row in trials if row.get("paper_trade_candidate", {}).get("paper_trade_eligible") is True]
    supported = [row for row in trials if row.get("candidate_backtest", {}).get("classification") == SUPPORTED_CLASSIFICATION]
    ready = [row for row in trials if row.get("paper_forward_ready") is True]
    edge_scores = [_nullable_float(row.get("edge_qualification", {}).get("edge_score")) for row in trials]
    edge_values = [value for value in edge_scores if value is not None]
    return {
        "observations": observation_count,
        "clusters": len(clusters),
        "claims": len(clusters),
        "hypotheses": len(trials),
        "historical_replays": len(trials),
        "positive_replays": len(positive),
        "positive_replay_rate": _rate(len(positive), len(trials)),
        "eligible_candidates": len(eligible),
        "backtest_supported_candidates": len(supported),
        "paper_forward_ready_candidates": len(ready),
        "average_edge_score": _average(edge_values),
        "median_edge_score": round(statistics.median(edge_values), 6) if edge_values else None,
        "mechanism_counts": dict(Counter(row.get("mechanism") for row in trials)),
        "split_dimension_counts": {
            "timeframes": dict(Counter(str((row.get("split_dimensions") or {}).get("timeframe") or "UNKNOWN") for row in trials)),
            "source_types": dict(Counter(str((row.get("split_dimensions") or {}).get("source_type") or "UNKNOWN") for row in trials)),
            "symbol_groups": dict(Counter(str((row.get("split_dimensions") or {}).get("symbol_group") or "UNKNOWN") for row in trials)),
        },
        "conversion_rates": {
            "observations_to_clusters": _rate(len(clusters), observation_count),
            "clusters_to_claims": 1.0 if clusters else 0.0,
            "claims_to_hypotheses": _rate(len(trials), len(clusters)),
            "hypotheses_to_positive_replay": _rate(len(positive), len(trials)),
            "hypotheses_to_eligible_candidate": _rate(len(eligible), len(trials)),
            "hypotheses_to_backtest_supported": _rate(len(supported), len(trials)),
            "hypotheses_to_paper_forward_ready": _rate(len(ready), len(trials)),
        },
        "main_rejection_reasons": _rejection_reasons(trials),
        "regime_metrics": regime_metric_block(
            observations=[],
            claims=trials,
            hypotheses=trials,
            replay_rows=trials,
            candidates=trials,
        ),
    }


def _comparison(pre: dict[str, Any], post: dict[str, Any]) -> dict[str, Any]:
    return {
        "cluster_increase": int(post.get("clusters") or 0) - int(pre.get("clusters") or 0),
        "claim_increase": int(post.get("claims") or 0) - int(pre.get("claims") or 0),
        "hypothesis_increase": int(post.get("hypotheses") or 0) - int(pre.get("hypotheses") or 0),
        "candidate_increase": int(post.get("eligible_candidates") or 0) - int(pre.get("eligible_candidates") or 0),
        "eligible_candidate_change": int(post.get("eligible_candidates") or 0) - int(pre.get("eligible_candidates") or 0),
        "paper_forward_ready_candidate_change": int(post.get("paper_forward_ready_candidates") or 0) - int(pre.get("paper_forward_ready_candidates") or 0),
        "backtest_supported_candidate_change": int(post.get("backtest_supported_candidates") or 0) - int(pre.get("backtest_supported_candidates") or 0),
        "positive_replay_rate_change": _diff(post.get("positive_replay_rate"), pre.get("positive_replay_rate")),
        "average_edge_score_change": _diff(post.get("average_edge_score"), pre.get("average_edge_score")),
    }


def _rank_mechanisms(trials: list[dict[str, Any]], *, reverse: bool) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trials:
        groups[str(row.get("mechanism") or "UNKNOWN")].append(row)
    rows = []
    for mechanism, values in groups.items():
        edge_scores = [_nullable_float(row.get("edge_qualification", {}).get("edge_score")) for row in values]
        rows.append({
            "mechanism": mechanism,
            "trials": len(values),
            "positive_replay_rate": _rate(sum(row.get("historical_replay", {}).get("status") == "REPLAY_POSITIVE" for row in values), len(values)),
            "eligible_candidates": sum(row.get("paper_trade_candidate", {}).get("paper_trade_eligible") is True for row in values),
            "backtest_supported_candidates": sum(row.get("candidate_backtest", {}).get("classification") == SUPPORTED_CLASSIFICATION for row in values),
            "paper_forward_ready_candidates": sum(row.get("paper_forward_ready") is True for row in values),
            "average_edge_score": _average([value for value in edge_scores if value is not None]),
        })
    return sorted(rows, key=lambda row: (row["paper_forward_ready_candidates"], row["eligible_candidates"], row["backtest_supported_candidates"], row["average_edge_score"] or 0.0), reverse=reverse)


def _answer(comparison: dict[str, Any]) -> str:
    if comparison.get("paper_forward_ready_candidate_change", 0) > 0 or comparison.get("eligible_candidate_change", 0) > 0:
        return "Diversity preservation improved downstream candidate quality and should replace coarse-only claim generation."
    if comparison.get("backtest_supported_candidate_change", 0) > 0:
        return "Diversity preservation improved evidence breadth but did not yet clear candidate eligibility; edge qualification remains the bottleneck."
    return "Diversity preservation increased claim/hypothesis breadth but did not improve candidate quality in this run; review split dimensions and edge scoring inputs."


def _rejection_reasons(trials: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in trials:
        reasons = row.get("edge_qualification", {}).get("disqualification_reasons") or []
        if row.get("paper_trade_candidate", {}).get("paper_trade_eligible") is not True and not reasons:
            reasons = ["candidate gate not eligible"]
        counts.update(str(reason) for reason in reasons)
    return dict(counts)


def _validate_authority(report: dict[str, Any]) -> None:
    authority = report.get("authority_boundary", {})
    guardrails = report.get("guardrails", {})
    forbidden_true = [key for key, value in {**authority, **guardrails}.items() if key != "research_only" and value is True]
    if authority.get("research_only") is not True or forbidden_true:
        raise ValueError(f"split experiment authority boundary failed: {forbidden_true}")


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rate(numerator: int | float, denominator: int | float) -> float:
    return round(float(numerator) / float(denominator), 6) if denominator else 0.0


def _average(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _diff(after: Any, before: Any) -> float | None:
    if after is None or before is None:
        return None
    return round(float(after) - float(before), 6)
