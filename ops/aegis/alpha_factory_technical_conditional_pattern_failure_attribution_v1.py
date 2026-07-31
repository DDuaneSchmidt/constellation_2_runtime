from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_technical_conditional_pattern_failure_attribution_v1"
SCHEMA_ID = "aegis_alpha_factory_technical_conditional_pattern_failure_attribution"
SCHEMA_VERSION = "v1"

PILOT_FAMILY = "aegis_alpha_factory_technical_conditional_pattern_pilot_v1"
PILOT_FILENAME = "aegis_alpha_factory_technical_conditional_pattern_pilot_v1.json"

FAILURE_MODES = {
    "CONDITION_TOO_WEAK",
    "CONDITION_TOO_CLOSE_TO_PATTERN",
    "BASELINE_TOO_STRONG",
    "FEATURE_SURFACE_TOO_NARROW",
    "PATTERN_DEFINITION_TOO_SIMPLE",
    "UNIVERSE_TOO_SMALL",
    "SAMPLE_INSUFFICIENT",
    "INCONCLUSIVE",
}

NEXT_ACTIONS = {
    "stronger regime features",
    "broader universe",
    "better technical pattern definitions",
    "out-of-period/walk-forward validation",
    "stop technical branch",
}

SIMPLE_PATTERN_TYPES = {
    "oversold_reversal",
    "overbought_pullback",
    "volatility_compression_expansion",
    "vix_stress_conditional_response",
    "trend_continuation",
    "single_asset_shock_context",
}


def build_alpha_factory_technical_conditional_pattern_failure_attribution_v1(
    *, truth_root: Path, day_utc: str
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    pilot_path = root / "reports" / PILOT_FAMILY / day_utc / PILOT_FILENAME
    pilot = read_json_v1(pilot_path)

    inspected = _inspect_pilot(pilot)
    hostile_checks = _hostile_checks(inspected, pilot)
    failure_mode = _primary_failure_mode(inspected, hostile_checks, pilot)
    branch_decision = _branch_decision(failure_mode, hostile_checks)
    minimum_next_action = _minimum_next_action(failure_mode, hostile_checks)
    execution_valid = bool(pilot) and pilot.get("verdicts", {}).get("execution") == "TECHNICAL_CONDITIONAL_PATTERN_PILOT_EXECUTION_VALID"

    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_technical_conditional_pattern_failure_attribution_v1",
        "constraints": {
            "technical_conditional_pattern_pilot_v1_modified": False,
            "existing_technical_features_modified": False,
            "naive_technical_baselines_modified": False,
            "candidate_qualification_rules_modified": False,
            "trading_allowed": False,
            "capital_allocation_allowed": False,
            "live_broker_integration_allowed": False,
        },
        "source_artifacts": {
            "technical_conditional_pattern_pilot_v1": {
                "status": "AVAILABLE" if pilot else "MISSING",
                "path": str(pilot_path),
                "content_hash": pilot.get("content_hash", "") if pilot else "",
                "file_sha256": _file_hash(pilot_path),
            }
        },
        "input_pilot_verdicts": pilot.get("verdicts", {}) if pilot else {},
        "inspected_surfaces": inspected,
        "hostile_checks": hostile_checks,
        "failure_attribution": {
            "primary_technical_failure_mode": failure_mode,
            "branch_decision": branch_decision,
            "minimum_next_action": minimum_next_action,
            "rationale": _rationale(failure_mode, hostile_checks),
        },
        "verdicts": {
            "execution": "TECHNICAL_ATTRIBUTION_EXECUTION_VALID"
            if execution_valid
            else "TECHNICAL_ATTRIBUTION_EXECUTION_INVALID",
            "primary_technical_failure_mode": failure_mode,
            "branch_decision": branch_decision,
            "minimum_next_action": minimum_next_action,
        },
        "summary": {
            "technical_pattern_count": inspected["counts"]["technical_pattern_count"],
            "technical_feature_count": inspected["counts"]["technical_feature_count"],
            "pattern_evidence_count": inspected["counts"]["pattern_evidence_count"],
            "supported_evidence_count": inspected["counts"]["supported_evidence_count"],
            "candidate_group_count": inspected["counts"]["candidate_group_count"],
            "valid_rac_count": inspected["counts"]["valid_rac_count"],
            "baseline_cluster_count": inspected["counts"]["baseline_cluster_count"],
            "primary_technical_failure_mode": failure_mode,
            "output_reproducible": hostile_checks["deterministic_replay"],
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_technical_conditional_pattern_failure_attribution_v1(
    *,
    truth_root: Path,
    day_utc: str,
    payload: dict[str, Any],
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(
        out_dir / "aegis_alpha_factory_technical_conditional_pattern_failure_attribution_v1.json", payload
    )
    return {"json": str(path)}


def _inspect_pilot(pilot: dict[str, Any]) -> dict[str, Any]:
    features = _list(pilot.get("technical_features")) if pilot else []
    evidence = _list(pilot.get("pattern_evidence")) if pilot else []
    supported = _list(pilot.get("supported_technical_evidence")) if pilot else []
    groups = _list(pilot.get("technical_pattern_candidate_groups")) if pilot else []
    valid_racs = _list(pilot.get("valid_research_asset_candidates")) if pilot else []
    limitations = _list(pilot.get("limiting_or_contradictory_evidence")) if pilot else []
    baseline = pilot.get("naive_baseline_comparison") if isinstance(pilot.get("naive_baseline_comparison"), dict) else {}
    baseline_clusters = _list(baseline.get("top_candidate_relationship_clusters"))
    loaded_symbols = _list(pilot.get("input_universe", {}).get("loaded_symbols")) if pilot else []

    condition_counts = Counter(str(row.get("condition_type")) for row in features if isinstance(row, dict))
    evidence_by_condition = _condition_evidence_analysis(evidence)
    candidate_group_analysis = [_candidate_group(row) for row in groups if isinstance(row, dict)]
    support_sample_counts = [int(row.get("sample_count") or 0) for row in supported if isinstance(row, dict)]
    support_effects = [abs(float(row.get("effect_size") or 0.0)) for row in supported if isinstance(row, dict)]
    blocked = [row for row in evidence if isinstance(row, dict) and row.get("support_verdict") != "SUPPORTED"]

    return {
        "counts": {
            "technical_pattern_count": len(condition_counts),
            "technical_feature_count": len(features),
            "pattern_evidence_count": len(evidence),
            "supported_evidence_count": len(supported),
            "blocked_evidence_count": len(blocked),
            "candidate_group_count": len(groups),
            "valid_rac_count": len(valid_racs),
            "baseline_cluster_count": len(baseline_clusters),
            "loaded_symbol_count": len(loaded_symbols),
            "limiting_or_contradictory_evidence_count": len(limitations),
        },
        "technical_patterns_found": sorted(condition_counts.keys()),
        "technical_pattern_feature_counts": dict(sorted(condition_counts.items())),
        "conditional_features_used": _condition_feature_analysis(features),
        "naive_baseline_results": {
            "top_candidate_relationship_clusters": baseline_clusters[:10],
            "baseline_recoverable_relationship_pairs": _list(baseline.get("baseline_recoverable_relationship_pairs")),
            "max_support_count": max([int(row.get("support_count") or 0) for row in baseline_clusters] or [0]),
            "max_total_sample_count": max([int(row.get("total_sample_count") or 0) for row in baseline_clusters] or [0]),
            "has_competing_support": any(int(row.get("support_count") or 0) > 0 for row in baseline_clusters),
        },
        "conditional_pipeline_results": {
            "supported_evidence_ids": sorted(str(row.get("evidence_id")) for row in supported if isinstance(row, dict)),
            "qualifying_group_ids": sorted(
                str(row.get("technical_pattern_candidate_group_id")) for row in groups if row.get("qualifies_as_pilot_group")
            ),
            "valid_research_asset_candidates": valid_racs,
            "pilot_verdicts": pilot.get("verdicts", {}) if pilot else {},
        },
        "evidence_support": {
            "supported_by_condition_type": evidence_by_condition,
            "median_supported_sample_count": _median(support_sample_counts),
            "median_abs_supported_effect_size": _median(support_effects),
            "blocked_by_reason": _blocker_counts(blocked),
            "baseline_recoverable_supported_count": len([row for row in supported if row.get("baseline_recoverable")]),
            "non_pairwise_supported_count": len([row for row in supported if row.get("technical_condition_non_pairwise")]),
            "weak_effect_supported_count": len([row for row in supported if abs(float(row.get("effect_size") or 0.0)) < 0.002]),
        },
        "candidate_groups": candidate_group_analysis,
        "valid_or_invalid_candidate_groups": candidate_group_analysis,
        "limiting_or_contradictory_evidence": limitations,
        "lineage": {
            "feature_lineage_complete": _feature_lineage_complete(features),
            "evidence_lineage_complete": _lineage_complete(evidence),
            "candidate_group_lineage_complete": _lineage_complete(groups),
            "source_summary_lineage_complete": bool(pilot.get("summary", {}).get("lineage_complete")) if pilot else False,
        },
    }


def _condition_feature_analysis(features: list[Any]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in features:
        if isinstance(row, dict):
            grouped.setdefault(str(row.get("condition_type")), []).append(row)
    out = []
    for condition, rows in sorted(grouped.items()):
        source_series = sorted({str(series) for row in rows for series in _list(row.get("source_series"))})
        out.append(
            {
                "condition_type": condition,
                "feature_count": len(rows),
                "source_series": source_series,
                "calculation_versions": sorted({str(row.get("calculation_version")) for row in rows}),
                "known_at_rules": sorted({str(row.get("known_at_rule")) for row in rows}),
                "looks_like_pattern_definition": condition in SIMPLE_PATTERN_TYPES,
            }
        )
    return out


def _condition_evidence_analysis(evidence: list[Any]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in evidence:
        if isinstance(row, dict):
            grouped.setdefault(str(row.get("condition_type")), []).append(row)
    out = []
    for condition, rows in sorted(grouped.items()):
        supported = [row for row in rows if row.get("support_verdict") == "SUPPORTED"]
        out.append(
            {
                "condition_type": condition,
                "evidence_count": len(rows),
                "supported_count": len(supported),
                "blocked_count": len(rows) - len(supported),
                "median_sample_count": _median([int(row.get("sample_count") or 0) for row in rows]),
                "median_abs_effect_size": _median([abs(float(row.get("effect_size") or 0.0)) for row in rows]),
                "baseline_recoverable_count": len([row for row in rows if row.get("baseline_recoverable")]),
                "non_pairwise_count": len([row for row in rows if row.get("technical_condition_non_pairwise")]),
            }
        )
    return out


def _candidate_group(row: dict[str, Any]) -> dict[str, Any]:
    name = str(row.get("proposed_name") or "")
    generic = name.startswith("Observed technical conditional pattern:")
    return {
        "technical_pattern_candidate_group_id": row.get("technical_pattern_candidate_group_id"),
        "proposed_name": name,
        "target_asset": row.get("target_asset"),
        "condition_type": row.get("condition_type"),
        "supporting_evidence_ids": _list(row.get("supporting_evidence_ids")),
        "unique_non_naive_support_count": int(row.get("unique_non_naive_support_count") or 0),
        "qualifies_as_pilot_group": bool(row.get("qualifies_as_pilot_group")),
        "qualifies_as_rac": bool(row.get("research_asset_candidate_qualification", {}).get("qualifies")),
        "candidate_overnamed": not generic,
        "lineage_ref_count": len(_list(row.get("lineage"))),
        "limiting_evidence_count": len(_list(row.get("limiting_evidence"))),
        "contradictory_evidence_count": len(_list(row.get("contradictory_evidence"))),
    }


def _hostile_checks(inspected: dict[str, Any], pilot: dict[str, Any]) -> dict[str, Any]:
    counts = inspected["counts"]
    verdicts = pilot.get("verdicts", {}) if isinstance(pilot, dict) else {}
    evidence = inspected["evidence_support"]
    condition_features = inspected["conditional_features_used"]
    condition_types = {row["condition_type"] for row in condition_features}
    simple_condition_ratio = (
        len([row for row in condition_features if row["looks_like_pattern_definition"]]) / len(condition_features)
        if condition_features
        else 0.0
    )
    baseline_dominates = (
        verdicts.get("pipeline_vs_baseline") == "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE"
        and counts["baseline_cluster_count"] >= counts["candidate_group_count"]
        and inspected["naive_baseline_results"]["has_competing_support"]
    )
    sample_too_small = counts["supported_evidence_count"] == 0 or float(evidence["median_supported_sample_count"]) < 5.0
    universe_too_narrow = counts["loaded_symbol_count"] <= 7
    overnamed = any(row["candidate_overnamed"] for row in inspected["candidate_groups"])
    lineages = inspected["lineage"]
    condition_adds_info = (
        counts["supported_evidence_count"] > 0
        and evidence["non_pairwise_supported_count"] == counts["supported_evidence_count"]
        and len(condition_types - {"single_asset_shock_context"}) >= 2
    )
    condition_restates_pattern = simple_condition_ratio >= 0.9
    pattern_alone_sufficient = baseline_dominates and counts["valid_rac_count"] == 0
    feature_surface_narrow = len(condition_types) <= 2 or condition_restates_pattern

    return {
        "conditional_features_add_information_beyond_pattern": condition_adds_info and not condition_restates_pattern,
        "condition_merely_restates_pattern": condition_restates_pattern,
        "baseline_dominates_because_pattern_alone_sufficient": pattern_alone_sufficient,
        "sample_count_too_small": sample_too_small,
        "universe_too_narrow": universe_too_narrow,
        "candidate_was_overnamed": overnamed,
        "deterministic_replay": bool(pilot.get("summary", {}).get("output_reproducible")) if isinstance(pilot, dict) else False,
        "complete_lineage": bool(
            lineages["feature_lineage_complete"]
            and lineages["evidence_lineage_complete"]
            and lineages["candidate_group_lineage_complete"]
            and lineages["source_summary_lineage_complete"]
        ),
        "naive_baseline_matches_or_exceeds_pipeline": verdicts.get("pipeline_vs_baseline")
        == "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE",
        "technical_advantage_absent": verdicts.get("technical_conditional_advantage") == "ABSENT",
        "valid_rac_absent": counts["valid_rac_count"] == 0,
        "feature_surface_too_conventional": feature_surface_narrow,
        "pattern_definition_too_simple": condition_restates_pattern,
        "limiting_or_contradictory_evidence_captured": counts["limiting_or_contradictory_evidence_count"] > 0,
    }


def _primary_failure_mode(inspected: dict[str, Any], hostile: dict[str, Any], pilot: dict[str, Any]) -> str:
    if not pilot or pilot.get("verdicts", {}).get("execution") != "TECHNICAL_CONDITIONAL_PATTERN_PILOT_EXECUTION_VALID":
        return "INCONCLUSIVE"
    if hostile["sample_count_too_small"]:
        return "SAMPLE_INSUFFICIENT"
    if hostile["baseline_dominates_because_pattern_alone_sufficient"]:
        return "BASELINE_TOO_STRONG"
    if hostile["condition_merely_restates_pattern"]:
        return "CONDITION_TOO_CLOSE_TO_PATTERN"
    if hostile["feature_surface_too_conventional"]:
        return "FEATURE_SURFACE_TOO_NARROW"
    if inspected["evidence_support"]["weak_effect_supported_count"] >= inspected["counts"]["supported_evidence_count"] * 0.5:
        return "CONDITION_TOO_WEAK"
    if hostile["universe_too_narrow"]:
        return "UNIVERSE_TOO_SMALL"
    return "INCONCLUSIVE"


def _branch_decision(failure_mode: str, hostile: dict[str, Any]) -> str:
    if not hostile["complete_lineage"] or hostile["candidate_was_overnamed"]:
        return "STOP_BRANCH"
    if failure_mode in {"BASELINE_TOO_STRONG", "CONDITION_TOO_CLOSE_TO_PATTERN", "PATTERN_DEFINITION_TOO_SIMPLE"}:
        return "PIVOT_RECOMMENDED"
    if failure_mode in {"UNIVERSE_TOO_SMALL", "SAMPLE_INSUFFICIENT", "FEATURE_SURFACE_TOO_NARROW", "CONDITION_TOO_WEAK"}:
        return "TECHNICAL_BRANCH_CONTINUE"
    return "PIVOT_RECOMMENDED"


def _minimum_next_action(failure_mode: str, hostile: dict[str, Any]) -> str:
    if not hostile["complete_lineage"] or hostile["candidate_was_overnamed"]:
        return "stop technical branch"
    if failure_mode == "BASELINE_TOO_STRONG":
        return "out-of-period/walk-forward validation"
    if failure_mode in {"CONDITION_TOO_CLOSE_TO_PATTERN", "PATTERN_DEFINITION_TOO_SIMPLE"}:
        return "better technical pattern definitions"
    if failure_mode in {"FEATURE_SURFACE_TOO_NARROW", "CONDITION_TOO_WEAK"}:
        return "stronger regime features"
    if failure_mode in {"UNIVERSE_TOO_SMALL", "SAMPLE_INSUFFICIENT"}:
        return "broader universe"
    return "out-of-period/walk-forward validation"


def _rationale(failure_mode: str, hostile: dict[str, Any]) -> list[str]:
    reasons = []
    if hostile["naive_baseline_matches_or_exceeds_pipeline"]:
        reasons.append("The source pilot verdict says the naive technical baseline matched or exceeded the conditional pipeline.")
    if hostile["baseline_dominates_because_pattern_alone_sufficient"]:
        reasons.append("Naive baseline clusters had competing support while the pilot produced no valid RAC.")
    if hostile["condition_merely_restates_pattern"]:
        reasons.append("The conditional feature set is composed almost entirely of the same simple pattern labels being tested.")
    if hostile["universe_too_narrow"]:
        reasons.append("The pilot universe is limited to the seven certified ETF/VIX symbols.")
    if hostile["complete_lineage"]:
        reasons.append("Lineage is complete, so the failure is evidential rather than traceability-driven.")
    if not reasons:
        reasons.append(f"Primary failure mode is {failure_mode} based on deterministic hostile checks.")
    return reasons


def _feature_lineage_complete(features: list[Any]) -> bool:
    return bool(features) and all(
        isinstance(row, dict)
        and row.get("feature_id")
        and row.get("source_observations")
        and row.get("source_series")
        and row.get("calculation_version")
        and row.get("known_at_rule")
        and row.get("lineage")
        for row in features
    )


def _lineage_complete(rows: list[Any]) -> bool:
    return all(isinstance(row, dict) and row.get("lineage") for row in rows)


def _blocker_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        for blocker in _list(row.get("support_blockers")):
            counts[str(blocker)] += 1
    return dict(sorted(counts.items()))


def _median(values: list[int] | list[float]) -> float:
    return round(float(median(values)), 8) if values else 0.0


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _file_hash(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _hash(payload: dict[str, Any]) -> str:
    clone = dict(payload)
    clone.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clone, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
