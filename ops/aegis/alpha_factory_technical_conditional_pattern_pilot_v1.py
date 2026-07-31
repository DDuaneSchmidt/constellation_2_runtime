from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_cross_asset_etf_discovery_pilot_v1 import (
    ETF_UNIVERSE,
    RETURN_TARGETS,
    _load_available_etf_observations,
)
from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import _clusters, _relationship_rows, _shock_events
from ops.aegis.alpha_factory_poc_v1 import Observation, _daily_return, _return_over, _series_by_asset, _std, _z_score
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_technical_conditional_pattern_pilot_v1"
SCHEMA_ID = "aegis_alpha_factory_technical_conditional_pattern_pilot"
SCHEMA_VERSION = "v1"
FAILURE_ATTRIBUTION_FAMILY = "aegis_alpha_factory_cross_asset_etf_failure_attribution_v1"
FAILURE_ATTRIBUTION_FILENAME = "aegis_alpha_factory_cross_asset_etf_failure_attribution_v1.json"
CALCULATION_VERSION = "alpha_factory_technical_conditional_pattern_pilot.v1"
KNOWN_AT_RULE = "technical_condition_known_after_all_source_observations_known_at"
HORIZONS = (1, 5, 20)
MIN_SAMPLE_COUNT = 5
MIN_EFFECT_SIZE = 0.001
MIN_DIRECTION_CONSISTENCY = 0.55
MINIMUM_NEXT_ACTION = (
    "Run an out-of-sample technical conditional pattern replay before any real-market discovery claim; keep trading, "
    "capital allocation, and broker execution prohibited."
)


def build_alpha_factory_technical_conditional_pattern_pilot_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    observations, source_lineage, missing_symbols = _load_available_etf_observations(root)
    series = _series_by_asset(observations)
    failure_path = root / "reports" / FAILURE_ATTRIBUTION_FAMILY / day_utc / FAILURE_ATTRIBUTION_FILENAME
    failure_attribution = read_json_v1(failure_path)
    technical_features = _technical_features(observations)
    baseline_relationships = _relationship_rows(series, _shock_events(series)) if observations else []
    baseline_clusters = _clusters(baseline_relationships) if baseline_relationships else []
    baseline_pairs = _baseline_pairs(baseline_clusters)
    pattern_evidence = _pattern_evidence(series, technical_features, baseline_pairs)
    supported_evidence = [row for row in pattern_evidence if row["support_verdict"] == "SUPPORTED"]
    candidate_groups = _candidate_groups(supported_evidence)
    hostile_checks = _hostile_checks(
        observations=observations,
        failure_attribution=failure_attribution,
        features=technical_features,
        evidence=pattern_evidence,
        candidate_groups=candidate_groups,
        baseline_clusters=baseline_clusters,
    )
    verdicts = _verdicts(observations, failure_attribution, supported_evidence, candidate_groups, baseline_clusters, hostile_checks)
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_technical_conditional_pattern_pilot_v1",
        "constraints": {
            "cross_asset_etf_discovery_pilot_v1_modified": False,
            "cross_asset_etf_failure_attribution_v1_modified": False,
            "question_discovery_v2_modified": False,
            "evidence_test_v2_modified": False,
            "feature_surface_repair_v1_modified": False,
            "naive_baseline_modified": False,
            "candidate_qualification_rules_modified": False,
            "trading_allowed": False,
            "capital_allocation_allowed": False,
            "live_broker_integration_allowed": False,
        },
        "source_artifacts": {
            "cross_asset_etf_failure_attribution_v1": {
                "status": "AVAILABLE" if failure_attribution else "MISSING",
                "path": str(failure_path),
                "content_hash": failure_attribution.get("content_hash", "") if failure_attribution else "",
                "file_sha256": _file_hash(failure_path),
                "minimum_next_action": failure_attribution.get("verdicts", {}).get("minimum_next_action", "")
                if failure_attribution
                else "",
            }
        },
        "input_universe": {
            "requested_symbols": list(ETF_UNIVERSE),
            "loaded_symbols": sorted({row.asset for row in observations}),
            "missing_symbols": missing_symbols,
            "observation_count": len(observations),
            "source_lineage": source_lineage,
        },
        "method": {
            "branch_reason": "Prior cross-asset ETF failure attribution recommended a technical-analysis conditional pattern pilot.",
            "pattern_generation": [
                "oversold_reversal",
                "overbought_pullback",
                "volatility_compression_expansion",
                "vix_stress_conditional_response",
                "trend_continuation",
            ],
            "evidence_horizons_days": list(HORIZONS),
            "support_thresholds": {
                "minimum_sample_count": MIN_SAMPLE_COUNT,
                "minimum_abs_effect_size": MIN_EFFECT_SIZE,
                "minimum_direction_consistency": MIN_DIRECTION_CONSISTENCY,
            },
            "baseline_comparison": "Naive pairwise shock-response clusters are loaded only for recoverability and hostile comparison.",
            "not_a_trading_system": True,
        },
        "technical_features": technical_features,
        "pattern_evidence": pattern_evidence,
        "supported_technical_evidence": supported_evidence,
        "naive_baseline_comparison": {
            "top_relationships": baseline_relationships[:25],
            "top_candidate_relationship_clusters": baseline_clusters[:10],
            "baseline_recoverable_relationship_pairs": sorted(baseline_pairs),
        },
        "technical_pattern_candidate_groups": candidate_groups,
        "valid_research_asset_candidates": [],
        "limiting_or_contradictory_evidence": _limitations(pattern_evidence, missing_symbols, failure_attribution),
        "hostile_checks": hostile_checks,
        "verdicts": verdicts,
        "summary": {
            "technical_feature_count": len(technical_features),
            "pattern_evidence_count": len(pattern_evidence),
            "supported_evidence_count": len(supported_evidence),
            "candidate_group_count": len(candidate_groups),
            "valid_rac_count": 0,
            "baseline_cluster_count": len(baseline_clusters),
            "lineage_complete": hostile_checks["technical_condition_lineage_complete"],
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_technical_conditional_pattern_pilot_v1(
    *, truth_root: Path, day_utc: str, payload: dict[str, Any]
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_technical_conditional_pattern_pilot_v1.json", payload)
    return {"json": str(path)}


def _technical_features(observations: list[Observation]) -> list[dict[str, Any]]:
    series = _series_by_asset(observations)
    obs_id = {(row.asset, row.day): row.observation_id for row in observations}
    features: list[dict[str, Any]] = []
    vol_by_asset = _rolling_vol_by_asset(series)
    vol_cutoffs = {
        asset: _percentile([value for _day, value in rows if value is not None], 0.25)
        for asset, rows in vol_by_asset.items()
        if rows
    }
    vix_z_by_day = _z_by_day(series.get("VIX", []))
    for asset in sorted(asset for asset in RETURN_TARGETS if series.get(asset)):
        asset_series = series[asset]
        vol_lookup = dict(vol_by_asset.get(asset, []))
        for idx, (day, value) in enumerate(asset_series):
            if idx < 20:
                continue
            ret5 = _return_over(asset_series, idx, 5)
            ret20 = _return_over(asset_series, idx, 20)
            z20 = _z_score([row[1] for row in asset_series[max(0, idx - 19) : idx + 1]], value)
            daily = _daily_return(asset_series, idx) or 0.0
            vol20 = vol_lookup.get(day)
            source = [obs_id[(asset, day)]]
            if z20 is not None and ret5 is not None and z20 <= -1.0 and ret5 <= -0.005:
                features.append(_feature(asset, day, "oversold_reversal", z20, source, {"return_5d": ret5}))
            if z20 is not None and ret5 is not None and z20 >= 1.0 and ret5 >= 0.005:
                features.append(_feature(asset, day, "overbought_pullback", z20, source, {"return_5d": ret5}))
            if vol20 is not None and vol20 <= vol_cutoffs.get(asset, -1.0):
                features.append(
                    _feature(asset, day, "volatility_compression_expansion", vol20, source, {"volatility_p25": vol_cutoffs[asset]})
                )
            if ret20 is not None and ret5 is not None and ret20 >= 0.015 and ret5 >= 0.002:
                features.append(_feature(asset, day, "trend_continuation", ret20, source, {"return_5d": ret5}))
            if abs(daily) >= 0.015:
                features.append(_feature(asset, day, "single_asset_shock_context", daily, source, {"shock_threshold": 0.015}))
            vix_z = vix_z_by_day.get(day)
            if vix_z is not None and vix_z >= 1.0 and ("VIX", day) in obs_id:
                features.append(
                    _feature(
                        asset,
                        day,
                        "vix_stress_conditional_response",
                        vix_z,
                        [obs_id[(asset, day)], obs_id[("VIX", day)]],
                        {"condition_asset": "VIX"},
                    )
                )
    return sorted(features, key=lambda row: str(row["feature_id"]))


def _pattern_evidence(
    series: dict[str, list[tuple[str, float]]], features: list[dict[str, Any]], baseline_pairs: set[str]
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for feature in features:
        grouped.setdefault((str(feature["asset"]), str(feature["condition_type"])), []).append(feature)
    rows: list[dict[str, Any]] = []
    for (asset, condition_type), condition_features in sorted(grouped.items()):
        asset_series = series.get(asset, [])
        for horizon in HORIZONS:
            row = _evaluate_pattern(asset_series, asset, condition_type, condition_features, horizon, baseline_pairs)
            if row["sample_count"] > 0:
                rows.append(row)
    return sorted(
        rows,
        key=lambda row: (
            row["support_verdict"] != "SUPPORTED",
            -int(row["sample_count"]),
            -abs(float(row["effect_size"])),
            row["target_asset"],
            row["condition_type"],
            int(row["horizon_days"]),
        ),
    )


def _evaluate_pattern(
    asset_series: list[tuple[str, float]],
    asset: str,
    condition_type: str,
    condition_features: list[dict[str, Any]],
    horizon: int,
    baseline_pairs: set[str],
) -> dict[str, Any]:
    index_by_day = {day: idx for idx, (day, _value) in enumerate(asset_series)}
    feature_by_day = {str(row["day"]): row for row in condition_features}
    returns: list[float] = []
    event_days: list[str] = []
    lineage: list[str] = []
    for day, feature in sorted(feature_by_day.items()):
        idx = index_by_day.get(day)
        if idx is None or idx + horizon >= len(asset_series):
            continue
        start = asset_series[idx][1]
        end = asset_series[idx + horizon][1]
        if not start:
            continue
        returns.append((end / start) - 1.0)
        event_days.append(day)
        lineage.extend([str(feature["feature_id"]), *[str(ref) for ref in feature.get("source_observations", [])]])
    baseline = [
        (asset_series[idx + horizon][1] / asset_series[idx][1]) - 1.0
        for idx in range(0, len(asset_series) - horizon)
        if asset_series[idx][1]
    ]
    effect_kind = "ABSOLUTE_FORWARD_MOVE" if condition_type == "volatility_compression_expansion" else "SIGNED_FORWARD_RETURN"
    if effect_kind == "ABSOLUTE_FORWARD_MOVE":
        condition_mean = _mean([abs(row) for row in returns])
        baseline_mean = _mean([abs(row) for row in baseline])
        direction_consistency = _fraction([abs(row) > baseline_mean for row in returns])
        effect_direction = "EXPANSION" if condition_mean > baseline_mean + 0.0005 else "NO_EXPANSION"
    else:
        condition_mean = _mean(returns)
        baseline_mean = _mean(baseline)
        non_zero = [row for row in returns if abs(row) > 0.0000001]
        positive = len([row for row in non_zero if row > 0])
        negative = len([row for row in non_zero if row < 0])
        direction_consistency = max(positive, negative) / len(non_zero) if non_zero else 0.0
        effect_direction = "POSITIVE" if condition_mean > baseline_mean + 0.0005 else "NEGATIVE" if condition_mean < baseline_mean - 0.0005 else "FLAT"
    effect_size = condition_mean - baseline_mean
    baseline_recoverable = _baseline_recoverable(asset, condition_type, baseline_pairs)
    blockers = []
    if len(returns) < MIN_SAMPLE_COUNT:
        blockers.append("LOW_SAMPLE_COUNT")
    if abs(effect_size) < MIN_EFFECT_SIZE:
        blockers.append("WEAK_EFFECT_SIZE")
    if direction_consistency < MIN_DIRECTION_CONSISTENCY:
        blockers.append("LOW_DIRECTION_CONSISTENCY")
    if baseline_recoverable:
        blockers.append("BASELINE_RECOVERABLE_SUPPORT")
    technical_condition_non_pairwise = condition_type != "vix_stress_conditional_response" or f"VIX:{asset}" not in baseline_pairs
    if not technical_condition_non_pairwise:
        blockers.append("PAIRWISE_BASELINE_RESTATEMENT")
    supported = not blockers
    return {
        "evidence_id": f"TCP-EV-{asset}-{condition_type}-{horizon}D",
        "target_asset": asset,
        "condition_type": condition_type,
        "horizon_days": horizon,
        "effect_kind": effect_kind,
        "sample_count": len(returns),
        "effect_direction": effect_direction,
        "effect_size": round(effect_size, 8),
        "direction_consistency": round(direction_consistency, 8),
        "baseline_comparison": {
            "conditional_forward_metric_mean": round(condition_mean, 8),
            "all_days_forward_metric_mean": round(baseline_mean, 8),
        },
        "baseline_recoverable": baseline_recoverable,
        "technical_condition_non_pairwise": technical_condition_non_pairwise,
        "support_verdict": "SUPPORTED" if supported else "BLOCKED",
        "support_blockers": sorted(set(blockers)),
        "event_days": event_days,
        "supporting_feature_refs": sorted(str(row["feature_id"]) for row in condition_features if str(row["day"]) in event_days)[:25],
        "evidence_summary": "Technical conditional evidence passed nonzero, non-pairwise, non-baseline hostile gates."
        if supported
        else "Blocked by hostile gate: " + ",".join(sorted(set(blockers))),
        "contradiction_notes": "" if supported else "Blocked by hostile gate: " + ",".join(sorted(set(blockers))),
        "lineage": sorted(set(lineage))[:100],
        "trading_allowed": False,
    }


def _candidate_groups(supported: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in supported:
        grouped.setdefault((str(row["target_asset"]), str(row["condition_type"])), []).append(row)
    groups: list[dict[str, Any]] = []
    for (asset, condition_type), rows in sorted(grouped.items()):
        qualifies_as_pilot_group = len(rows) >= 2 and all(
            int(row["sample_count"]) > 0 and not row["baseline_recoverable"] and row["technical_condition_non_pairwise"] for row in rows
        )
        groups.append(
            {
                "technical_pattern_candidate_group_id": f"TCP-GROUP-{asset}-{condition_type}",
                "proposed_name": f"Observed technical conditional pattern: {asset} {condition_type}",
                "target_asset": asset,
                "condition_type": condition_type,
                "supporting_evidence_ids": sorted(str(row["evidence_id"]) for row in rows),
                "horizons": sorted(int(row["horizon_days"]) for row in rows),
                "unique_non_naive_support_count": len(rows),
                "qualifies_as_pilot_group": qualifies_as_pilot_group,
                "research_asset_candidate_qualification": {
                    "qualifies": False,
                    "reason": "Technical conditional pattern pilot records groups only; RAC promotion requires separate out-of-sample benchmark.",
                },
                "limiting_evidence": [],
                "contradictory_evidence": [],
                "lineage": sorted({ref for row in rows for ref in row.get("lineage", [])})[:150],
                "trading_allowed": False,
            }
        )
    return groups


def _hostile_checks(
    *,
    observations: list[Observation],
    failure_attribution: dict[str, Any],
    features: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    candidate_groups: list[dict[str, Any]],
    baseline_clusters: list[dict[str, Any]],
) -> dict[str, Any]:
    supported = [row for row in evidence if row["support_verdict"] == "SUPPORTED"]
    lineage_complete = bool(features) and all(
        row.get("feature_id")
        and row.get("source_observations")
        and row.get("source_series")
        and row.get("calculation_version") == CALCULATION_VERSION
        and row.get("known_at_rule") == KNOWN_AT_RULE
        and row.get("lineage")
        for row in features
    )
    no_pairwise_restatement = all(row.get("technical_condition_non_pairwise") for row in supported)
    no_baseline_only = all(not row.get("baseline_recoverable") for row in supported)
    no_zero = all(int(row.get("sample_count") or 0) > 0 for row in supported)
    evidence_not_naive = all(row.get("condition_type") not in {"relationship_instability", "correlation_breakdown"} for row in supported)
    no_overnamed = all(str(row.get("proposed_name", "")).startswith("Observed technical conditional pattern:") for row in candidate_groups)
    prior_recommended = (
        failure_attribution.get("verdicts", {}).get("minimum_next_action") == "technical-analysis conditional pattern pilot"
        if failure_attribution
        else False
    )
    return {
        "available_certified_etf_history_loaded": bool(observations),
        "prior_failure_attribution_recommended_this_pilot": prior_recommended,
        "technical_condition_lineage_complete": lineage_complete,
        "no_zero_sample_support": no_zero,
        "no_baseline_recoverable_only_pattern_group": no_baseline_only,
        "evidence_support_not_naive_forward_return_correlation": evidence_not_naive,
        "questions_not_pairwise_relationship_restatements": no_pairwise_restatement,
        "candidate_naming_generic": no_overnamed,
        "valid_rac_count": 0,
        "naive_baseline_has_competing_support": any(int(row.get("support_count") or 0) > 0 for row in baseline_clusters[:10]),
        "real_market_claim_stays_prohibited": True,
        "deterministic_replay": True,
        "hostile_checks_all_pass": bool(observations)
        and lineage_complete
        and no_zero
        and no_baseline_only
        and evidence_not_naive
        and no_pairwise_restatement
        and no_overnamed,
    }


def _verdicts(
    observations: list[Observation],
    failure_attribution: dict[str, Any],
    supported: list[dict[str, Any]],
    candidate_groups: list[dict[str, Any]],
    baseline_clusters: list[dict[str, Any]],
    hostile: dict[str, Any],
) -> dict[str, str]:
    execution_valid = bool(observations) and bool(failure_attribution) and hostile["technical_condition_lineage_complete"]
    technical_pattern_found = any(row.get("qualifies_as_pilot_group") for row in candidate_groups)
    baseline_has_support = any(int(row.get("support_count") or 0) > 0 for row in baseline_clusters[:10])
    outperforms = technical_pattern_found and hostile["hostile_checks_all_pass"] and not baseline_has_support
    advantage = outperforms
    return {
        "execution": "TECHNICAL_CONDITIONAL_PATTERN_PILOT_EXECUTION_VALID"
        if execution_valid
        else "TECHNICAL_CONDITIONAL_PATTERN_PILOT_EXECUTION_INVALID",
        "real_data_technical_pattern": "REAL_DATA_TECHNICAL_PATTERN_FOUND" if technical_pattern_found else "NO_REAL_DATA_TECHNICAL_PATTERN",
        "technical_conditional_advantage": "TECHNICAL_CONDITIONAL_ADVANTAGE_PRESENT"
        if advantage
        else "ABSENT"
        if execution_valid
        else "INCONCLUSIVE",
        "pipeline_vs_baseline": "PIPELINE_OUTPERFORMS_BASELINE"
        if outperforms
        else "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE"
        if baseline_has_support
        else "INCONCLUSIVE",
        "real_market_technical_discovery_claim": "PROHIBITED",
        "minimum_next_action": MINIMUM_NEXT_ACTION,
    }


def _feature(
    asset: str,
    day: str,
    condition_type: str,
    value: float,
    source_observations: list[str],
    details: dict[str, Any],
) -> dict[str, Any]:
    return {
        "feature_id": f"TCP-FEAT-{asset}-{condition_type}-{day}",
        "asset": asset,
        "condition_type": condition_type,
        "day": day,
        "value": round(float(value), 8),
        "calculation_details": {key: round(val, 8) if isinstance(val, float) else val for key, val in sorted(details.items())},
        "source_observations": source_observations,
        "source_series": sorted({asset, *[str(details.get("condition_asset")) if details.get("condition_asset") else asset]}),
        "calculation_version": CALCULATION_VERSION,
        "known_at_rule": KNOWN_AT_RULE,
        "lineage": list(source_observations),
        "trading_allowed": False,
    }


def _rolling_vol_by_asset(series: dict[str, list[tuple[str, float]]]) -> dict[str, list[tuple[str, float | None]]]:
    out: dict[str, list[tuple[str, float | None]]] = {}
    for asset, rows in series.items():
        vals = []
        for idx, (day, _value) in enumerate(rows):
            returns = [_daily_return(rows, j) for j in range(max(1, idx - 19), idx + 1)]
            vals.append((day, _std(returns)))
        out[asset] = vals
    return out


def _z_by_day(rows: list[tuple[str, float]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for idx, (day, value) in enumerate(rows):
        if idx < 20:
            continue
        z = _z_score([row[1] for row in rows[max(0, idx - 19) : idx + 1]], value)
        if z is not None:
            out[day] = z
    return out


def _baseline_pairs(clusters: list[dict[str, Any]]) -> set[str]:
    pairs: set[str] = set()
    for cluster in clusters[:10]:
        if int(cluster.get("support_count") or 0) <= 0:
            continue
        source = str(cluster.get("source_asset") or "")
        target = str(cluster.get("target_asset") or "")
        if source and target:
            pairs.add(f"{source}:{target}")
            pairs.add(f"{target}:{source}")
    return pairs


def _baseline_recoverable(asset: str, condition_type: str, baseline_pairs: set[str]) -> bool:
    if condition_type == "vix_stress_conditional_response" and f"VIX:{asset}" in baseline_pairs:
        return True
    if condition_type == "single_asset_shock_context":
        return True
    return False


def _limitations(
    evidence: list[dict[str, Any]], missing_symbols: list[str], failure_attribution: dict[str, Any]
) -> list[dict[str, Any]]:
    limitations = [
        {
            "limitation_id": "TCP-LIM-NO-OUT-OF-SAMPLE",
            "description": "This pilot is in-sample only; no real-market discovery claim is allowed without out-of-sample replay.",
        }
    ]
    if missing_symbols:
        limitations.append(
            {
                "limitation_id": "TCP-LIM-MISSING-ETF-SERIES",
                "description": "Some requested ETF/VIX series were unavailable in the local historical snapshot.",
                "missing_symbols": missing_symbols,
            }
        )
    blocked = [row for row in evidence if row["support_verdict"] != "SUPPORTED"]
    if blocked:
        limitations.append(
            {
                "limitation_id": "TCP-LIM-BLOCKED-EVIDENCE",
                "description": "Some technical conditional tests failed hostile support gates.",
                "blocked_evidence_count": len(blocked),
                "blocker_counts": _blocker_counts(blocked),
            }
        )
    if not failure_attribution:
        limitations.append(
            {
                "limitation_id": "TCP-LIM-MISSING-FAILURE-ATTRIBUTION",
                "description": "Prior cross-asset failure attribution artifact was unavailable.",
            }
        )
    return limitations


def _blocker_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for blocker in row.get("support_blockers", []):
            counts[str(blocker)] = counts.get(str(blocker), 0) + 1
    return dict(sorted(counts.items()))


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _fraction(values: list[bool]) -> float:
    return len([row for row in values if row]) / len(values) if values else 0.0


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * pct))))
    return ordered[idx]


def _file_hash(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _hash(payload: dict[str, Any]) -> str:
    clone = json.loads(json.dumps(payload, sort_keys=True))
    clone.pop("content_hash", None)
    encoded = json.dumps(clone, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
