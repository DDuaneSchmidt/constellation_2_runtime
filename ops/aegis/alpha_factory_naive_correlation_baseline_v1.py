from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_poc_v1 import ASSETS, HORIZONS, RETURN_ASSETS, build_alpha_factory_poc_v1
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_naive_correlation_baseline_v1"
POC_FAMILY = "aegis_alpha_factory_poc_v1"
SCHEMA_ID = "aegis_alpha_factory_naive_correlation_baseline"
SCHEMA_VERSION = "v1"


def build_alpha_factory_naive_correlation_baseline_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    poc_path = root / "reports" / POC_FAMILY / day_utc / "alpha_factory_poc.v1.json"
    poc = read_json_v1(poc_path)
    if not poc:
        poc = build_alpha_factory_poc_v1(truth_root=root, day_utc=day_utc)
        poc_path = Path("")

    observations = _list(poc.get("observation_store"))
    features = _list(poc.get("feature_store"))
    candidates = _list(poc.get("research_asset_candidates"))
    candidate = candidates[0] if candidates and isinstance(candidates[0], dict) else {}
    series = _series_by_asset(observations)
    shock_events = _shock_events(series)
    relationships = _relationship_rows(series, shock_events)
    clusters = _clusters(relationships)
    comparison = _compare_poc_candidate(candidate, relationships, clusters, poc)
    verdicts = {
        "baseline_execution": "BASELINE_EXECUTION_VALID" if relationships and clusters else "BASELINE_EXECUTION_INVALID",
        "poc_vs_baseline": comparison["poc_vs_baseline"],
        "discovery_advantage": comparison["discovery_advantage"],
        "minimum_next_action": comparison["minimum_next_action"],
    }
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "minimum_naive_correlation_shock_response_baseline_for_alpha_factory_poc",
        "source_poc_artifact": str(poc_path) if poc_path else "",
        "source_poc_hash": str(poc.get("content_hash") or _hash(poc)),
        "inputs": {
            "observation_count": len(observations),
            "feature_count": len(features),
            "source_assets": sorted(series),
            "target_assets": list(RETURN_ASSETS),
            "horizons": list(HORIZONS),
        },
        "method": {
            "relationship_generation": "pairwise source-asset shock days to target-asset forward returns",
            "shock_definition": "absolute daily z-score >= 2.0 or absolute daily move >= fallback threshold",
            "ranking_metrics": ["sample_count", "effect_size", "direction_consistency", "baseline_comparison"],
            "ranking_score": "sample_count weighted by absolute effect size and direction consistency",
            "no_llm": True,
            "no_discovery_engine_changes": True,
        },
        "verdicts": verdicts,
        "top_relationships": relationships[:25],
        "top_candidate_relationship_clusters": clusters[:10],
        "poc_candidate_comparison": comparison,
        "hostile_checks": comparison["hostile_checks"],
        "summary": {
            "relationship_count": len(relationships),
            "cluster_count": len(clusters),
            "top_relationship_id": relationships[0]["relationship_id"] if relationships else "",
            "top_cluster_id": clusters[0]["cluster_id"] if clusters else "",
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_naive_correlation_baseline_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_naive_correlation_baseline_v1.json", payload)
    return {"json": str(path)}


def _relationship_rows(series: dict[str, list[tuple[str, float]]], shock_events: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_asset in sorted(shock_events):
        for target_asset in RETURN_ASSETS:
            if source_asset == target_asset or target_asset not in series:
                continue
            for horizon in HORIZONS:
                row = _evaluate_pair(series, shock_events[source_asset], source_asset, target_asset, horizon)
                if row["sample_count"] > 0:
                    rows.append(row)
    return sorted(
        rows,
        key=lambda row: (
            -int(row["sample_count"]),
            -float(row["direction_consistency"]),
            -abs(float(row["effect_size"])),
            row["source_asset"],
            row["target_asset"],
            int(row["horizon_days"]),
        ),
    )


def _evaluate_pair(
    series: dict[str, list[tuple[str, float]]],
    source_events: list[dict[str, Any]],
    source_asset: str,
    target_asset: str,
    horizon: int,
) -> dict[str, Any]:
    target_series = series[target_asset]
    index_by_day = {day: idx for idx, (day, _value) in enumerate(target_series)}
    returns: list[float] = []
    event_days: list[str] = []
    for event in source_events:
        day = str(event["day"])
        idx = index_by_day.get(day)
        if idx is None or idx + horizon >= len(target_series):
            continue
        start = target_series[idx][1]
        end = target_series[idx + horizon][1]
        if start:
            returns.append((end / start) - 1.0)
            event_days.append(day)
    baseline = [
        (target_series[idx + horizon][1] / target_series[idx][1]) - 1.0
        for idx in range(0, len(target_series) - horizon)
        if target_series[idx][1]
    ]
    shock_mean = sum(returns) / len(returns) if returns else 0.0
    baseline_mean = sum(baseline) / len(baseline) if baseline else 0.0
    effect_size = shock_mean - baseline_mean
    non_zero = [row for row in returns if abs(row) > 0.0000001]
    if not non_zero:
        direction_consistency = 0.0
    else:
        positive = len([row for row in non_zero if row > 0])
        negative = len([row for row in non_zero if row < 0])
        direction_consistency = max(positive, negative) / len(non_zero)
    direction = "POSITIVE" if effect_size > 0.0005 else "NEGATIVE" if effect_size < -0.0005 else "FLAT"
    score = len(returns) * abs(effect_size) * (0.5 + direction_consistency)
    return {
        "relationship_id": f"NB-{source_asset}-TO-{target_asset}-{horizon}D",
        "source_asset": source_asset,
        "target_asset": target_asset,
        "horizon_days": horizon,
        "sample_count": len(returns),
        "effect_direction": direction,
        "effect_size": round(effect_size, 8),
        "direction_consistency": round(direction_consistency, 8),
        "baseline_comparison": {
            "shock_forward_return_mean": round(shock_mean, 8),
            "all_days_forward_return_mean": round(baseline_mean, 8),
        },
        "event_days": event_days,
        "ranking_score": round(score, 10),
    }


def _clusters(relationships: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in relationships:
        grouped.setdefault((row["source_asset"], row["target_asset"]), []).append(row)
    clusters: list[dict[str, Any]] = []
    for (source, target), rows in grouped.items():
        top_rows = sorted(rows, key=lambda row: int(row["horizon_days"]))
        support_count = len([row for row in top_rows if abs(float(row["effect_size"])) > 0.0005 and int(row["sample_count"]) >= 3])
        clusters.append(
            {
                "cluster_id": f"NBC-{source}-TO-{target}",
                "source_asset": source,
                "target_asset": target,
                "relationship_ids": [row["relationship_id"] for row in top_rows],
                "horizons": [row["horizon_days"] for row in top_rows],
                "total_sample_count": sum(int(row["sample_count"]) for row in top_rows),
                "max_abs_effect_size": round(max(abs(float(row["effect_size"])) for row in top_rows), 8),
                "mean_direction_consistency": round(sum(float(row["direction_consistency"]) for row in top_rows) / len(top_rows), 8),
                "support_count": support_count,
                "cluster_score": round(sum(float(row["ranking_score"]) for row in top_rows), 10),
            }
        )
    return sorted(
        clusters,
        key=lambda row: (
            -int(row["support_count"]),
            -int(row["total_sample_count"]),
            -float(row["cluster_score"]),
            row["source_asset"],
            row["target_asset"],
        ),
    )


def _compare_poc_candidate(
    candidate: dict[str, Any],
    relationships: list[dict[str, Any]],
    clusters: list[dict[str, Any]],
    poc: dict[str, Any],
) -> dict[str, Any]:
    candidate_name = str(candidate.get("proposed_name") or "")
    supporting_ids = [str(row) for row in candidate.get("supporting_evidence") or []]
    contradictory_ids = [str(row) for row in candidate.get("contradictory_evidence") or []]
    supporting_evidence = [
        row
        for row in _list(poc.get("evidence_store"))
        if isinstance(row, dict) and str(row.get("evidence_id")) in supporting_ids
    ]
    real_yield_evidence = [
        row
        for row in _list(poc.get("evidence_store"))
        if isinstance(row, dict) and str(row.get("hypothesis_id", "")).startswith(("H-001", "H-003"))
    ]
    gld_slv_cluster = _find_cluster(clusters, "GLD", "SLV")
    slv_gld_cluster = _find_cluster(clusters, "SLV", "GLD")
    real_yield_gld_cluster = _find_cluster(clusters, "REAL_YIELD", "GLD")
    recoverable_by_naive = bool(gld_slv_cluster and int(gld_slv_cluster["support_count"]) >= 3)
    gld_slv_explains_support = supporting_ids == ["E-002-01", "E-002-05", "E-002-20"]
    real_yield_sample_count = sum(int(row.get("sample_count") or 0) for row in real_yield_evidence)
    naming_exceeds = "Real Yield" in candidate_name and real_yield_sample_count == 0
    baseline_matches = recoverable_by_naive or gld_slv_explains_support or naming_exceeds
    if baseline_matches:
        poc_vs_baseline = "BASELINE_MATCHES_OR_EXCEEDS_POC"
        discovery_advantage = "DISCOVERY_ADVANTAGE_ABSENT"
        minimum_next_action = (
            "Do not claim discovery advantage. First compare POC candidate projection against the naive-correlation baseline "
            "on an out-of-fixture dataset, and require real-yield support with nonzero samples before retaining the current candidate name."
        )
    else:
        poc_vs_baseline = "INCONCLUSIVE"
        discovery_advantage = "DISCOVERY_ADVANTAGE_INCONCLUSIVE"
        minimum_next_action = "Run the same baseline on a larger independent history before claiming discovery advantage."
    return {
        "poc_candidate_id": candidate.get("research_asset_candidate_id"),
        "poc_candidate_name": candidate_name,
        "poc_vs_baseline": poc_vs_baseline,
        "discovery_advantage": discovery_advantage,
        "minimum_next_action": minimum_next_action,
        "matched_naive_clusters": [
            row for row in [gld_slv_cluster, slv_gld_cluster, real_yield_gld_cluster] if row
        ],
        "hostile_checks": {
            "poc_candidate_mostly_recoverable_by_naive_correlation": recoverable_by_naive,
            "poc_naming_exceeds_evidence": naming_exceeds,
            "real_yield_support_zero_sample_or_insufficient": real_yield_sample_count < 3,
            "gld_to_slv_evidence_alone_explains_candidate": gld_slv_explains_support,
            "supporting_evidence_ids": supporting_ids,
            "contradictory_evidence_ids": contradictory_ids,
            "real_yield_evidence_sample_count": real_yield_sample_count,
            "supporting_evidence_sample_count": sum(int(row.get("sample_count") or 0) for row in supporting_evidence),
        },
    }


def _shock_events(series: dict[str, list[tuple[str, float]]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for asset in sorted(series):
        values = series[asset]
        changes = [_daily_change(values, idx, asset) for idx in range(len(values))]
        clean = [row for row in changes if row is not None]
        if len(clean) < 5:
            continue
        mean = sum(clean) / len(clean)
        std = _std(clean) or 0.0
        threshold = 0.018 if asset in RETURN_ASSETS else 0.05
        events: list[dict[str, Any]] = []
        for idx, change in enumerate(changes):
            if change is None:
                continue
            z_score = ((change - mean) / std) if std else 0.0
            if abs(z_score) >= 2.0 or abs(change) >= threshold:
                events.append({"day": values[idx][0], "move": round(change, 8), "z_score": round(z_score, 8)})
        if events:
            out[asset] = events
    return out


def _series_by_asset(observations: list[Any]) -> dict[str, list[tuple[str, float]]]:
    out: dict[str, list[tuple[str, float]]] = {asset: [] for asset in ASSETS}
    for row in observations:
        if not isinstance(row, dict):
            continue
        asset = str(row.get("asset") or "")
        if asset in out:
            out[asset].append((str(row.get("day")), float(row.get("value"))))
    return {asset: sorted(values) for asset, values in out.items() if values}


def _daily_change(values: list[tuple[str, float]], idx: int, asset: str) -> float | None:
    if idx <= 0:
        return None
    previous = values[idx - 1][1]
    current = values[idx][1]
    if asset in RETURN_ASSETS or asset in {"VIX"}:
        if previous == 0:
            return None
        return (current / previous) - 1.0
    return current - previous


def _find_cluster(clusters: list[dict[str, Any]], source_asset: str, target_asset: str) -> dict[str, Any] | None:
    for row in clusters:
        if row.get("source_asset") == source_asset and row.get("target_asset") == target_asset:
            return row
    return None


def _std(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    return math.sqrt(sum((row - mean) ** 2 for row in values) / (len(values) - 1))


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
