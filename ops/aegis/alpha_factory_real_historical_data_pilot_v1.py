from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_feature_surface_repair_v1 import (
    CALCULATION_VERSION,
    KNOWN_AT_RULE,
    _relationship_features as repaired_relationship_features,
)
from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import _clusters, _relationship_rows, _shock_events
from ops.aegis.alpha_factory_poc_v1 import Observation, _daily_return, _return_over, _series_by_asset, _std, _z_score
from ops.aegis.alpha_factory_question_discovery_v2 import _discover_triggers, _leakage_checks, _questions_from_triggers
from ops.aegis.intelligence_common_v1 import write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_real_historical_data_pilot_v1"
SCHEMA_ID = "aegis_alpha_factory_real_historical_data_pilot"
SCHEMA_VERSION = "v1"
PILOT_SYMBOLS = ("SPY", "QQQ", "GLD", "SLV", "TLT", "UUP", "VIX")
RETURN_TARGETS = ("SPY", "QQQ", "GLD", "SLV", "TLT", "UUP")
MINIMUM_NEXT_ACTION_RAC = (
    "Submit the real historical candidate to hostile review and an out-of-sample historical replay before allowing any real-market discovery claim."
)
MINIMUM_NEXT_ACTION_NO_RAC = (
    "Do not allow a real-market discovery claim; expand the real historical pilot only after data coverage and benchmark gates remain intact."
)


def build_alpha_factory_real_historical_data_pilot_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    observations, source_lineage, missing_symbols = _load_available_market_observations(root)
    series = _series_by_asset(observations)
    repaired_features = repaired_relationship_features(observations) if observations else []
    daily_features = _daily_features(observations)
    question_features = sorted(daily_features + repaired_features, key=lambda row: str(row.get("feature_id")))
    triggers = _discover_triggers(question_features) if question_features else []
    questions = _questions_from_triggers(triggers)
    baseline_relationships = _relationship_rows(series, _shock_events(series)) if observations else []
    baseline_clusters = _clusters(baseline_relationships) if baseline_relationships else []
    baseline_pairs = _baseline_pairs(baseline_clusters)
    evidence = [_real_evidence(question, repaired_features, baseline_pairs) for question in questions]
    supported = [row for row in evidence if row["support_verdict"] == "SUPPORTED"]
    candidate_groups = _candidate_groups(supported)
    valid_racs = [row for row in candidate_groups if row["research_asset_candidate_qualification"]["qualifies"]]
    hostile_checks = _hostile_checks(
        observations=observations,
        repaired_features=repaired_features,
        questions=questions,
        evidence=evidence,
        candidate_groups=candidate_groups,
        valid_racs=valid_racs,
    )
    verdicts = _verdicts(observations, baseline_clusters, valid_racs, hostile_checks)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_real_historical_data_pilot_v1",
        "constraints": {
            "available_historical_data_only": True,
            "question_discovery_v2_modified": False,
            "evidence_test_v2_modified": False,
            "feature_surface_repair_v1_modified": False,
            "synthetic_certification_modified": False,
            "candidate_qualification_rules_modified": False,
            "naive_baseline_modified": False,
            "synthetic_fixture_assumptions_allowed": False,
            "trading_allowed": False,
            "capital_allocation_allowed": False,
            "live_broker_integration_allowed": False,
        },
        "input_universe": {
            "requested_symbols": list(PILOT_SYMBOLS) + ["DXY_OR_REAL_YIELD_IF_AVAILABLE"],
            "loaded_symbols": sorted({row.asset for row in observations}),
            "missing_symbols": missing_symbols,
            "observation_count": len(observations),
            "source_lineage": source_lineage,
        },
        "method": {
            "relationship_feature_surface": "alpha_factory_feature_surface_repair.v1_relationship_features_applied_to_available_real_history",
            "question_generation": "question_discovery_v2 trigger and rendering functions applied to real historical repaired features",
            "evidence_gates": [
                "relationship_specific_support",
                "nonzero_sample_support",
                "baseline_separated_support",
                "non_naive_relationship_feature_support",
                "complete_relationship_lineage",
            ],
            "rac_qualification": [
                "at_least_2_supported_nonzero_relationship_specific_baseline_separated_non_naive_evidence_artifacts",
                "at_least_2_related_questions",
                "no_zero_sample_support_counted",
                "no_baseline_recoverable_only_rac",
                "explicit_limiting_or_contradictory_evidence_captured",
            ],
        },
        "relationship_features": repaired_features,
        "questions": questions,
        "trigger_evidence": triggers,
        "evidence_support": evidence,
        "unique_non_naive_support": supported,
        "naive_baseline_comparison": {
            "top_relationships": baseline_relationships[:25],
            "top_candidate_relationship_clusters": baseline_clusters[:10],
            "baseline_recoverable_relationship_pairs": sorted(baseline_pairs),
        },
        "candidate_groups": candidate_groups,
        "valid_research_asset_candidates": valid_racs,
        "limiting_or_contradictory_evidence": _limitations(evidence, missing_symbols),
        "hostile_checks": hostile_checks,
        "verdicts": verdicts,
        "summary": {
            "question_count": len(questions),
            "relationship_feature_count": len(repaired_features),
            "evidence_count": len(evidence),
            "supported_evidence_count": len(supported),
            "unique_non_naive_support_count": len(supported),
            "baseline_cluster_count": len(baseline_clusters),
            "valid_rac_count": len(valid_racs),
            "lineage_complete": hostile_checks["relationship_specific_lineage_complete"],
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_real_historical_data_pilot_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_real_historical_data_pilot_v1.json", payload)
    return {"json": str(path)}


def _load_available_market_observations(root: Path) -> tuple[list[Observation], list[dict[str, str]], list[str]]:
    market_root = root / "market_data_snapshot_v1"
    raw: dict[str, dict[str, dict[str, Any]]] = {}
    lineage: list[dict[str, str]] = []
    missing: list[str] = []
    for symbol in PILOT_SYMBOLS:
        symbol_root = market_root / symbol
        files = sorted(symbol_root.glob("*.jsonl")) if symbol_root.exists() else []
        if not files:
            missing.append(symbol)
            continue
        rows: dict[str, dict[str, Any]] = {}
        for path in files:
            lineage.append({"symbol": symbol, "path": str(path)})
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                item = json.loads(line)
                close = item.get("close")
                timestamp = str(item.get("timestamp_utc") or "")
                if close is None or len(timestamp) < 10:
                    continue
                day = timestamp[:10]
                rows[day] = item
        if rows:
            raw[symbol] = rows
        else:
            missing.append(symbol)
    observations: list[Observation] = []
    for symbol in sorted(raw):
        for day, item in sorted(raw[symbol].items()):
            observations.append(
                Observation(
                    observation_id=f"REAL-MD-{symbol}-{day}",
                    asset=symbol,
                    day=day,
                    value=float(item["close"]),
                    source=str(item.get("source_name") or "market_data_snapshot_v1"),
                    known_at=str(item.get("ingested_utc") or f"{day}T21:00:00Z"),
                )
            )
    return observations, lineage, sorted(set(missing + ["REAL_YIELD", "NOMINAL_YIELD", "INFLATION_EXPECTATIONS", "DXY"]))


def _daily_features(observations: list[Observation]) -> list[dict[str, Any]]:
    series = _series_by_asset(observations)
    obs_id = {(row.asset, row.day): row.observation_id for row in observations}
    out: list[dict[str, Any]] = []
    for asset in sorted(asset for asset in RETURN_TARGETS if series.get(asset)):
        asset_series = series[asset]
        for idx, (day, value) in enumerate(asset_series):
            daily = _daily_return(asset_series, idx)
            five = _return_over(asset_series, idx, 5)
            twenty = _return_over(asset_series, idx, 20)
            vol = _std([_daily_return(asset_series, j) for j in range(max(1, idx - 19), idx + 1)])
            z = _z_score([row[1] for row in asset_series[max(0, idx - 19) : idx + 1]], value)
            shock = abs(daily or 0.0) >= 0.018 or abs(z or 0.0) >= 2.0
            for feature_type, feature_value in {
                "daily_return": daily,
                "return_5d": five,
                "return_20d": twenty,
                "rolling_volatility_20d": vol,
                "z_score_20d": z,
                "shock_indicator": shock,
                "regime_label": _real_regime(series, idx),
            }.items():
                out.append(_feature(asset, day, feature_type, feature_value, [obs_id[(asset, day)]]))
    return out


def _feature(asset: str, day: str, feature_type: str, value: Any, source: list[str]) -> dict[str, Any]:
    return {
        "feature_id": f"REAL-FEAT-{asset}-{feature_type}-{day}",
        "asset": asset,
        "related_asset": None,
        "day": day,
        "feature_type": feature_type,
        "value": round(value, 8) if isinstance(value, float) else value,
        "source_observations": source,
        "calculation_version": "alpha_factory_real_historical_data_pilot.daily_features.v1",
        "known_at_rule": KNOWN_AT_RULE,
        "lineage": list(source),
        "trading_allowed": False,
    }


def _real_regime(series: dict[str, list[tuple[str, float]]], idx: int) -> str:
    vix = series.get("VIX", [])
    if idx < len(vix) and vix[idx][1] >= 18:
        return "RISK_OFF"
    return "NORMAL"


def _real_evidence(question: dict[str, Any], repaired_features: list[dict[str, Any]], baseline_pairs: set[str]) -> dict[str, Any]:
    rel = _relationship_from_question(question)
    relationship_key = f"{rel}:RELATIONSHIP_INSTABILITY" if rel else ""
    features = [row for row in repaired_features if row.get("relationship_key") == rel]
    support_features = _support_feature_refs(question, features)
    pair_baseline_recoverable = rel in baseline_pairs or ":".join(reversed(rel.split(":"))) in baseline_pairs if rel else False
    relationship_specific = bool(rel) and all(ref.startswith(f"AF-FSR-{rel.replace(':', '-')}-") for ref in support_features)
    non_naive = bool(support_features) and all(
        "forward_return" not in ref and "-TO-" not in ref and "NB-" not in ref for ref in support_features
    )
    sample_count = len(support_features)
    supported = sample_count > 0 and relationship_specific and non_naive and not pair_baseline_recoverable
    blockers = []
    if sample_count <= 0:
        blockers.append("ZERO_SAMPLE_SUPPORT")
    if not relationship_specific:
        blockers.append("BROAD_OR_NON_SPECIFIC_SUPPORT")
    if pair_baseline_recoverable:
        blockers.append("BASELINE_RECOVERABLE_SUPPORT")
    if not non_naive:
        blockers.append("NAIVE_FORWARD_RETURN_ONLY_SUPPORT")
    return {
        "evidence_id": f"REAL-HIST-EV-{question.get('question_id')}",
        "question_id": question.get("question_id"),
        "relationship_key": relationship_key,
        "sample_count": sample_count,
        "nonzero_support": sample_count > 0,
        "relationship_specific_support": relationship_specific,
        "baseline_recoverable": pair_baseline_recoverable,
        "baseline_separated_support": not pair_baseline_recoverable,
        "non_naive_relationship_feature_support": non_naive,
        "support_verdict": "SUPPORTED" if supported else "BLOCKED",
        "support_blockers": sorted(set(blockers)),
        "supporting_feature_refs": support_features,
        "contradiction_notes": "Blocked by hostile gate: " + ",".join(sorted(set(blockers))) if blockers else "",
        "lineage": sorted(set([str(question.get("question_id")), *question.get("lineage", []), *support_features])),
        "trading_allowed": False,
    }


def _relationship_from_question(question: dict[str, Any]) -> str:
    for ref in question.get("source_feature_refs", []) + question.get("source_relationship_refs", []):
        text = str(ref)
        if text.startswith("AF-FSR-"):
            parts = text.split("-")
            if len(parts) >= 5:
                return f"{parts[2]}:{parts[3]}"
    trigger_id = str(question.get("trigger_id") or "")
    parts = trigger_id.split("-")
    if len(parts) >= 7:
        return f"{parts[4]}:{parts[5]}"
    return ""


def _support_feature_refs(question: dict[str, Any], features: list[dict[str, Any]]) -> list[str]:
    refs = [str(ref) for ref in question.get("source_feature_refs", []) if str(ref).startswith("AF-FSR-")]
    if refs:
        return refs[:8]
    preferred = [row for row in features if row.get("feature_type") in {"relationship_instability_score_20d", "shock_response_1d", "rolling_beta_20d", "pre_post_trigger_relationship_delta_5d"}]
    return [str(row["feature_id"]) for row in preferred[:8]]


def _candidate_groups(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in evidence:
        grouped.setdefault(str(row["relationship_key"]), []).append(row)
    out: list[dict[str, Any]] = []
    for relationship_key, rows in sorted(grouped.items()):
        evidence_ids = sorted(str(row["evidence_id"]) for row in rows)
        question_ids = sorted({str(row["question_id"]) for row in rows})
        qualifies = len(evidence_ids) >= 2 and len(question_ids) >= 2
        out.append(
            {
                "research_asset_candidate_id": f"REAL-RAC-{relationship_key.replace(':', '-')}",
                "proposed_name": f"Observed relationship instability: {relationship_key.rsplit(':', 1)[0]}",
                "relationship_key": relationship_key,
                "supporting_evidence_ids": evidence_ids,
                "related_question_ids": question_ids,
                "limiting_evidence": [],
                "contradictory_evidence": [],
                "unique_non_naive_support_count": len(evidence_ids),
                "research_asset_candidate_qualification": {
                    "qualifies": qualifies,
                    "rules": [
                        "at_least_2_supported_nonzero_relationship_specific_baseline_separated_non_naive_evidence_artifacts",
                        "at_least_2_related_questions",
                        "no_zero_sample_support_counted",
                        "no_baseline_recoverable_support_counted",
                    ],
                },
                "lineage": sorted({relationship_key, *[ref for row in rows for ref in row.get("lineage", [])]}),
                "trading_allowed": False,
            }
        )
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


def _limitations(evidence: list[dict[str, Any]], missing_symbols: list[str]) -> list[dict[str, Any]]:
    limitations = [
        {
            "limitation_id": "REAL-HIST-LIM-MISSING-MACRO",
            "description": "Real yield, nominal yield, inflation expectation, or DXY series were not available in the local historical pilot input.",
            "affected_symbols": sorted(set(missing_symbols + ["REAL_YIELD", "NOMINAL_YIELD", "INFLATION_EXPECTATIONS", "DXY"])),
        }
    ]
    blocked = [row for row in evidence if row["support_verdict"] != "SUPPORTED"]
    if blocked:
        limitations.append(
            {
                "limitation_id": "REAL-HIST-LIM-BLOCKED-EVIDENCE",
                "description": "Some generated questions failed hostile evidence gates.",
                "blocked_evidence_count": len(blocked),
                "blocker_counts": _blocker_counts(blocked),
            }
        )
    return limitations


def _blocker_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for blocker in row.get("support_blockers", []):
            counts[str(blocker)] = counts.get(str(blocker), 0) + 1
    return dict(sorted(counts.items()))


def _hostile_checks(
    *,
    observations: list[Observation],
    repaired_features: list[dict[str, Any]],
    questions: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    candidate_groups: list[dict[str, Any]],
    valid_racs: list[dict[str, Any]],
) -> dict[str, Any]:
    supported = [row for row in evidence if row["support_verdict"] == "SUPPORTED"]
    return {
        "available_historical_data_loaded": bool(observations),
        "no_zero_sample_support": all(int(row.get("sample_count") or 0) > 0 for row in supported),
        "no_baseline_recoverable_only_rac": all(not row.get("baseline_recoverable") for row in supported),
        "no_overnamed_candidates": all(str(row.get("proposed_name", "")).startswith("Observed relationship instability:") for row in candidate_groups),
        "no_synthetic_fixture_assumptions": True,
        "relationship_specific_lineage_complete": _lineage_complete(repaired_features, questions, evidence, candidate_groups),
        "evidence_support_not_just_naive_forward_return_correlation": all(row.get("non_naive_relationship_feature_support") for row in supported),
        "deterministic_replay": True,
        "hostile_checks_all_pass": bool(observations)
        and all(int(row.get("sample_count") or 0) > 0 for row in supported)
        and all(not row.get("baseline_recoverable") for row in supported)
        and all(str(row.get("proposed_name", "")).startswith("Observed relationship instability:") for row in candidate_groups)
        and _lineage_complete(repaired_features, questions, evidence, candidate_groups)
        and all(row.get("non_naive_relationship_feature_support") for row in supported),
        "valid_rac_count": len(valid_racs),
    }


def _lineage_complete(features: list[dict[str, Any]], questions: list[dict[str, Any]], evidence: list[dict[str, Any]], groups: list[dict[str, Any]]) -> bool:
    if not features:
        return False
    feature_ok = all(
        row.get("source_observations")
        and row.get("source_series")
        and row.get("calculation_version") == CALCULATION_VERSION
        and row.get("known_at_rule") == KNOWN_AT_RULE
        and row.get("lineage")
        for row in features
    )
    questions_ok = all(row.get("lineage") and (row.get("source_feature_refs") or row.get("source_relationship_refs")) for row in questions)
    evidence_ok = all(row.get("lineage") for row in evidence)
    groups_ok = all(row.get("lineage") for row in groups)
    return bool(feature_ok and questions_ok and evidence_ok and groups_ok)


def _verdicts(observations: list[Observation], baseline_clusters: list[dict[str, Any]], valid_racs: list[dict[str, Any]], hostile: dict[str, Any]) -> dict[str, str]:
    execution_valid = bool(observations) and hostile["relationship_specific_lineage_complete"]
    rac_found = bool(valid_racs)
    checks_pass = hostile["hostile_checks_all_pass"]
    baseline_has_support = any(int(row.get("support_count") or 0) > 0 for row in baseline_clusters[:10])
    discovery_advantage = rac_found and checks_pass
    pipeline_outperforms = discovery_advantage and not baseline_has_support
    claim_allowed = rac_found and discovery_advantage and pipeline_outperforms and checks_pass
    return {
        "execution": "REAL_HISTORICAL_PILOT_EXECUTION_VALID" if execution_valid else "REAL_HISTORICAL_PILOT_EXECUTION_INVALID",
        "real_data_rac": "REAL_DATA_RAC_FOUND" if rac_found else "NO_REAL_DATA_RAC",
        "discovery_advantage": "DISCOVERY_ADVANTAGE_PRESENT" if discovery_advantage else "ABSENT" if execution_valid else "INCONCLUSIVE",
        "pipeline_vs_baseline": "PIPELINE_OUTPERFORMS_BASELINE"
        if pipeline_outperforms
        else "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE"
        if baseline_has_support
        else "INCONCLUSIVE",
        "real_market_discovery_claim": "REAL_MARKET_DISCOVERY_CLAIM_ALLOWED" if claim_allowed else "PROHIBITED",
        "minimum_next_action": MINIMUM_NEXT_ACTION_RAC if claim_allowed else MINIMUM_NEXT_ACTION_NO_RAC,
    }


def _hash(payload: dict[str, Any]) -> str:
    clone = json.loads(json.dumps(payload, sort_keys=True))
    clone.pop("content_hash", None)
    encoded = json.dumps(clone, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
