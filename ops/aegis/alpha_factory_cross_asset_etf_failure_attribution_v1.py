from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_cross_asset_etf_failure_attribution_v1"
SCHEMA_ID = "aegis_alpha_factory_cross_asset_etf_failure_attribution"
SCHEMA_VERSION = "v1"

PILOT_FAMILY = "aegis_alpha_factory_cross_asset_etf_discovery_pilot_v1"
PILOT_FILENAME = "aegis_alpha_factory_cross_asset_etf_discovery_pilot_v1.json"

PAIRWISE_FEATURE_TYPES = {
    "rolling_correlation",
    "rolling_correlation_20d",
    "rolling_beta",
    "rolling_beta_20d",
    "relationship_instability_score",
    "shock_response",
    "shock_response_1d",
    "pre_post_trigger_relationship_delta",
}

FAILURE_MODES = {
    "BASELINE_REDUNDANCY",
    "QUESTION_TOO_CLOSE_TO_BASELINE",
    "EVIDENCE_TEST_TOO_CLOSE_TO_BASELINE",
    "FEATURE_SURFACE_TOO_CONVENTIONAL",
    "CANDIDATE_GROUPING_TOO_PERMISSIVE",
    "DATA_UNIVERSE_TOO_SMALL",
    "INCONCLUSIVE",
}

MINIMUM_NEXT_ACTIONS = {
    "broader ETF universe",
    "technical-analysis conditional pattern pilot",
    "long-term fundamental thesis branch",
    "alternative feature representation",
    "stop current cross-asset branch",
}


def build_alpha_factory_cross_asset_etf_failure_attribution_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    pilot_path = root / "reports" / PILOT_FAMILY / day_utc / PILOT_FILENAME
    pilot = read_json_v1(pilot_path)

    inspected = _inspect_pilot(pilot)
    hostile_checks = _hostile_checks(inspected, pilot)
    failure_mode = _primary_failure_mode(inspected, hostile_checks, pilot)
    fault_class = _fault_class(failure_mode, hostile_checks)
    branch_decision = _branch_decision(failure_mode)
    minimum_next_action = _minimum_next_action(failure_mode, fault_class)
    execution_valid = bool(pilot) and pilot.get("verdicts", {}).get("execution") == "CROSS_ASSET_ETF_PILOT_EXECUTION_VALID"

    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_cross_asset_etf_failure_attribution_v1",
        "constraints": {
            "cross_asset_etf_discovery_pilot_v1_modified": False,
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
            "cross_asset_etf_discovery_pilot_v1": {
                "status": "AVAILABLE" if pilot else "MISSING",
                "path": str(pilot_path),
                "content_hash": pilot.get("content_hash", "") if pilot else "",
                "file_sha256": _file_hash(pilot_path),
            }
        },
        "input_pilot_verdicts": pilot.get("verdicts", {}),
        "inspected_surfaces": inspected,
        "hostile_checks": hostile_checks,
        "failure_attribution": {
            "primary_failure_mode": failure_mode,
            "fault_class": fault_class,
            "branch_decision": branch_decision,
            "minimum_next_action": minimum_next_action,
            "rationale": _rationale(failure_mode, hostile_checks),
        },
        "verdicts": {
            "execution": "CROSS_ASSET_FAILURE_ATTRIBUTION_EXECUTION_VALID"
            if execution_valid
            else "CROSS_ASSET_FAILURE_ATTRIBUTION_EXECUTION_INVALID",
            "primary_failure_mode": failure_mode,
            "fault_class": fault_class,
            "branch_decision": branch_decision,
            "minimum_next_action": minimum_next_action,
        },
        "summary": {
            "valid_rac_count": inspected["counts"]["valid_rac_count"],
            "relationship_group_count": inspected["counts"]["relationship_group_count"],
            "supporting_evidence_count": inspected["counts"]["supporting_evidence_count"],
            "unique_non_naive_support_count": inspected["counts"]["unique_non_naive_support_count"],
            "baseline_recoverable_support_count": inspected["counts"]["baseline_recoverable_support_count"],
            "baseline_cluster_count": inspected["counts"]["baseline_cluster_count"],
            "primary_failure_mode": failure_mode,
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_cross_asset_etf_failure_attribution_v1(
    *,
    truth_root: Path,
    day_utc: str,
    payload: dict[str, Any],
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_cross_asset_etf_failure_attribution_v1.json", payload)
    return {"json": str(path)}


def _inspect_pilot(pilot: dict[str, Any]) -> dict[str, Any]:
    valid_racs = _list(pilot.get("valid_research_asset_candidates"))
    candidate_groups = _list(pilot.get("candidate_groups"))
    evidence = _list(pilot.get("evidence_support"))
    unique_support = _list(pilot.get("unique_non_naive_support"))
    questions = _list(pilot.get("generated_questions"))
    features = _list(pilot.get("relationship_features"))
    baseline = pilot.get("naive_baseline_comparison") if isinstance(pilot.get("naive_baseline_comparison"), dict) else {}
    baseline_pairs = {_canonical_pair(str(pair)) for pair in _list(baseline.get("baseline_recoverable_relationship_pairs"))}
    baseline_clusters = _list(baseline.get("top_candidate_relationship_clusters"))
    baseline_cluster_pairs = {_canonical_pair(str(row.get("relationship_pair") or row.get("pair") or row.get("cluster_id"))) for row in baseline_clusters if isinstance(row, dict)}
    baseline_all_pairs = sorted(pair for pair in baseline_pairs | baseline_cluster_pairs if pair and pair != "UNKNOWN")

    support_by_id = {str(row.get("evidence_id")): row for row in evidence if isinstance(row, dict)}
    group_analysis = [_relationship_group(row, support_by_id, baseline_all_pairs) for row in candidate_groups if isinstance(row, dict)]
    rac_analysis = [_relationship_group(row, support_by_id, baseline_all_pairs) for row in valid_racs if isinstance(row, dict)]
    question_analysis = _question_analysis(questions)
    evidence_test_types = _evidence_test_type_analysis(evidence)
    feature_surface = _feature_surface_analysis(features)

    return {
        "counts": {
            "valid_rac_count": len(valid_racs),
            "relationship_group_count": len(candidate_groups),
            "question_count": len(questions),
            "evidence_count": len(evidence),
            "supporting_evidence_count": len([row for row in evidence if row.get("support_verdict") == "SUPPORTED"]),
            "unique_non_naive_support_count": len(unique_support),
            "baseline_recoverable_support_count": len([row for row in evidence if bool(row.get("baseline_recoverable"))]),
            "baseline_cluster_count": len(baseline_clusters),
            "relationship_feature_count": len(features),
            "loaded_symbol_count": len(_list(pilot.get("input_universe", {}).get("loaded_symbols"))),
        },
        "valid_rac_analysis": rac_analysis,
        "relationship_group_analysis": group_analysis,
        "support_analysis": {
            "supporting_evidence_ids": sorted(str(row.get("evidence_id")) for row in evidence if row.get("support_verdict") == "SUPPORTED"),
            "unique_non_naive_support_ids": sorted(str(row.get("evidence_id")) for row in unique_support if isinstance(row, dict)),
            "baseline_recoverable_support_ids": sorted(str(row.get("evidence_id")) for row in evidence if row.get("baseline_recoverable")),
            "blocked_support_count": len([row for row in evidence if row.get("support_verdict") == "BLOCKED"]),
            "non_naive_support_count": len([row for row in evidence if row.get("non_naive_relationship_feature_support")]),
        },
        "baseline_analysis": {
            "baseline_recoverable_relationship_pairs": baseline_all_pairs,
            "top_candidate_relationship_clusters": baseline_clusters[:10],
        },
        "question_analysis": question_analysis,
        "evidence_test_type_analysis": evidence_test_types,
        "feature_surface_analysis": feature_surface,
        "candidate_naming_analysis": _candidate_naming_analysis(valid_racs + candidate_groups),
    }


def _relationship_group(row: dict[str, Any], support_by_id: dict[str, dict[str, Any]], baseline_pairs: list[str]) -> dict[str, Any]:
    relationship_key = str(row.get("relationship_key") or "")
    pair = _canonical_pair(relationship_key)
    evidence_ids = [str(item) for item in _list(row.get("supporting_evidence_ids"))]
    support_rows = [support_by_id[eid] for eid in evidence_ids if eid in support_by_id]
    baseline_recoverable_rows = [support for support in support_rows if bool(support.get("baseline_recoverable"))]
    return {
        "research_asset_candidate_id": row.get("research_asset_candidate_id"),
        "proposed_name": row.get("proposed_name"),
        "relationship_key": relationship_key,
        "relationship_pair": pair,
        "qualifies": bool(row.get("research_asset_candidate_qualification", {}).get("qualifies")),
        "related_question_ids": _list(row.get("related_question_ids")),
        "supporting_evidence_ids": evidence_ids,
        "unique_non_naive_support_count": int(row.get("unique_non_naive_support_count") or 0),
        "support_rows_all_baseline_recoverable": bool(support_rows) and len(baseline_recoverable_rows) == len(support_rows),
        "support_rows_baseline_recoverable_count": len(baseline_recoverable_rows),
        "relationship_pair_recovered_by_naive_baseline": pair in set(baseline_pairs),
        "limiting_evidence_count": len(_list(row.get("limiting_evidence"))),
        "contradictory_evidence_count": len(_list(row.get("contradictory_evidence"))),
        "lineage_ref_count": len(_list(row.get("lineage"))),
    }


def _hostile_checks(inspected: dict[str, Any], pilot: dict[str, Any]) -> dict[str, Any]:
    racs = inspected["valid_rac_analysis"]
    groups = inspected["relationship_group_analysis"]
    question = inspected["question_analysis"]
    feature_surface = inspected["feature_surface_analysis"]
    pilot_verdicts = pilot.get("verdicts", {}) if isinstance(pilot, dict) else {}
    valid_rac_count = inspected["counts"]["valid_rac_count"]
    baseline_matches_or_exceeds = pilot_verdicts.get("pipeline_vs_baseline") == "BASELINE_MATCHES_OR_EXCEEDS_PIPELINE"
    rac_relationship_recoverable = bool(racs) and all(row["relationship_pair_recovered_by_naive_baseline"] for row in racs)
    rac_support_entirely_recoverable = bool(racs) and all(row["support_rows_all_baseline_recoverable"] for row in racs)
    non_naive_rows = inspected["support_analysis"]["non_naive_support_count"]
    unique_rows = inspected["counts"]["unique_non_naive_support_count"]
    material_non_naive_info = unique_rows > 0 and not (baseline_matches_or_exceeds and rac_relationship_recoverable)
    one_relationship_rac = bool(racs) and all(len(row["related_question_ids"]) <= 2 and len(row["supporting_evidence_ids"]) <= 2 for row in racs)
    small_universe = inspected["counts"]["loaded_symbol_count"] <= 7
    many_baseline_clusters = inspected["counts"]["baseline_cluster_count"] >= max(10, len(groups))

    return {
        "rac_support_entirely_baseline_recoverable": rac_support_entirely_recoverable,
        "rac_relationship_recoverable_by_naive_baseline": rac_relationship_recoverable,
        "non_naive_evidence_adds_any_information": non_naive_rows > 0 and unique_rows > 0,
        "non_naive_evidence_adds_material_information_beyond_baseline": material_non_naive_info,
        "questions_are_mostly_restated_pairwise_relationships": question["pairwise_relationship_question_ratio"] >= 0.75,
        "feature_surface_only_obvious_pairwise_relationships": feature_surface["pairwise_conventional_feature_ratio"] >= 0.80,
        "universe_size_prevents_discovery_advantage": small_universe and many_baseline_clusters and baseline_matches_or_exceeds,
        "candidate_grouping_may_be_too_permissive": valid_rac_count > 0 and one_relationship_rac and baseline_matches_or_exceeds,
        "candidate_naming_remains_valid": inspected["candidate_naming_analysis"]["over_named_count"] == 0,
        "baseline_matches_or_exceeds_pipeline": baseline_matches_or_exceeds,
        "pipeline_found_rac_without_discovery_advantage": valid_rac_count > 0
        and pilot_verdicts.get("discovery_advantage") == "ABSENT",
        "lineage_complete_in_source_pilot": bool(pilot.get("summary", {}).get("lineage_complete")),
        "deterministic_replay_in_source_pilot": bool(pilot.get("summary", {}).get("output_reproducible")),
    }


def _primary_failure_mode(inspected: dict[str, Any], hostile: dict[str, Any], pilot: dict[str, Any]) -> str:
    if not pilot:
        return "INCONCLUSIVE"
    if hostile["rac_relationship_recoverable_by_naive_baseline"] and hostile["baseline_matches_or_exceeds_pipeline"]:
        return "BASELINE_REDUNDANCY"
    if hostile["questions_are_mostly_restated_pairwise_relationships"]:
        return "QUESTION_TOO_CLOSE_TO_BASELINE"
    if not hostile["non_naive_evidence_adds_material_information_beyond_baseline"]:
        return "EVIDENCE_TEST_TOO_CLOSE_TO_BASELINE"
    if hostile["feature_surface_only_obvious_pairwise_relationships"]:
        return "FEATURE_SURFACE_TOO_CONVENTIONAL"
    if hostile["candidate_grouping_may_be_too_permissive"]:
        return "CANDIDATE_GROUPING_TOO_PERMISSIVE"
    if hostile["universe_size_prevents_discovery_advantage"]:
        return "DATA_UNIVERSE_TOO_SMALL"
    if inspected["counts"]["valid_rac_count"] == 0:
        return "INCONCLUSIVE"
    return "INCONCLUSIVE"


def _fault_class(failure_mode: str, hostile: dict[str, Any]) -> str:
    if failure_mode == "QUESTION_TOO_CLOSE_TO_BASELINE":
        return "QUESTION_GENERATION_FAULT"
    if failure_mode in {"EVIDENCE_TEST_TOO_CLOSE_TO_BASELINE", "BASELINE_REDUNDANCY"}:
        if hostile["feature_surface_only_obvious_pairwise_relationships"]:
            return "FEATURE_SURFACE_FAULT"
        return "EVIDENCE_TEST_FAULT"
    if failure_mode == "FEATURE_SURFACE_TOO_CONVENTIONAL":
        return "FEATURE_SURFACE_FAULT"
    if failure_mode == "CANDIDATE_GROUPING_TOO_PERMISSIVE":
        return "CANDIDATE_GROUPING_FAULT"
    if failure_mode == "DATA_UNIVERSE_TOO_SMALL":
        return "UNIVERSE_FAULT"
    return "INCONCLUSIVE"


def _branch_decision(failure_mode: str) -> str:
    if failure_mode in {"BASELINE_REDUNDANCY", "FEATURE_SURFACE_TOO_CONVENTIONAL", "EVIDENCE_TEST_TOO_CLOSE_TO_BASELINE"}:
        return "PIVOT_RECOMMENDED"
    if failure_mode == "DATA_UNIVERSE_TOO_SMALL":
        return "CURRENT_BRANCH_CONTINUE"
    if failure_mode == "CANDIDATE_GROUPING_TOO_PERMISSIVE":
        return "PIVOT_RECOMMENDED"
    if failure_mode == "QUESTION_TOO_CLOSE_TO_BASELINE":
        return "PIVOT_RECOMMENDED"
    return "STOP_BRANCH"


def _minimum_next_action(failure_mode: str, fault_class: str) -> str:
    if failure_mode == "DATA_UNIVERSE_TOO_SMALL":
        return "broader ETF universe"
    if fault_class == "FEATURE_SURFACE_FAULT":
        return "alternative feature representation"
    if fault_class == "EVIDENCE_TEST_FAULT":
        return "technical-analysis conditional pattern pilot"
    if fault_class == "CANDIDATE_GROUPING_FAULT":
        return "alternative feature representation"
    if fault_class == "QUESTION_GENERATION_FAULT":
        return "technical-analysis conditional pattern pilot"
    return "stop current cross-asset branch"


def _rationale(failure_mode: str, hostile: dict[str, Any]) -> str:
    if failure_mode == "BASELINE_REDUNDANCY":
        return (
            "The pilot produced a RAC while the source verdict says the naive baseline matches or exceeds the pipeline; "
            "the RAC relationship is recoverable as a simple pairwise relationship even though qualifying support rows "
            "were marked baseline-separated."
        )
    if failure_mode == "FEATURE_SURFACE_TOO_CONVENTIONAL":
        return "The inspected relationship features are dominated by rolling correlation, beta, instability, and shock-response pair features."
    if failure_mode == "EVIDENCE_TEST_TOO_CLOSE_TO_BASELINE":
        return "Non-naive rows exist, but the hostile checks do not show material information beyond the baseline comparison."
    if failure_mode == "DATA_UNIVERSE_TOO_SMALL":
        return "The pilot uses a seven-symbol ETF/VIX universe and the naive baseline finds broad pairwise relationship coverage."
    if failure_mode == "CANDIDATE_GROUPING_TOO_PERMISSIVE":
        return "The RAC qualifies on narrow relationship breadth while the same branch fails discovery advantage."
    if failure_mode == "QUESTION_TOO_CLOSE_TO_BASELINE":
        return "Generated questions mostly restate pairwise relationship instability/correlation surfaces."
    return "The source pilot does not provide enough separable evidence to assign a sharper failure mode."


def _question_analysis(questions: list[Any]) -> dict[str, Any]:
    rows = [row for row in questions if isinstance(row, dict)]
    pairwise = [
        row
        for row in rows
        if row.get("source_relationship_refs")
        or str(row.get("source_type") or "") in {"RELATIONSHIP_INSTABILITY", "CORRELATION_BREAKDOWN"}
    ]
    source_type_counts: dict[str, int] = {}
    for row in rows:
        source_type = str(row.get("source_type") or "UNKNOWN")
        source_type_counts[source_type] = source_type_counts.get(source_type, 0) + 1
    return {
        "question_count": len(rows),
        "source_type_counts": dict(sorted(source_type_counts.items())),
        "pairwise_relationship_question_count": len(pairwise),
        "pairwise_relationship_question_ratio": _ratio(len(pairwise), len(rows)),
        "sample_questions": [
            {
                "question_id": row.get("question_id"),
                "source_type": row.get("source_type"),
                "question": row.get("question"),
                "source_relationship_ref_count": len(_list(row.get("source_relationship_refs"))),
                "source_feature_ref_count": len(_list(row.get("source_feature_refs"))),
            }
            for row in rows[:5]
        ],
    }


def _evidence_test_type_analysis(evidence: list[Any]) -> dict[str, Any]:
    rows = [row for row in evidence if isinstance(row, dict)]
    counts = {
        "relationship_specific_support": 0,
        "non_naive_relationship_feature_support": 0,
        "baseline_separated_support": 0,
        "baseline_recoverable": 0,
        "nonzero_support": 0,
        "blocked_by_baseline_recoverability": 0,
    }
    for row in rows:
        for key in ("relationship_specific_support", "non_naive_relationship_feature_support", "baseline_separated_support", "baseline_recoverable", "nonzero_support"):
            if row.get(key):
                counts[key] += 1
        if "BASELINE_RECOVERABLE_SUPPORT" in _list(row.get("support_blockers")):
            counts["blocked_by_baseline_recoverability"] += 1
    return {
        "evidence_count": len(rows),
        "test_type_counts": counts,
        "supported_evidence_test_types": sorted(
            {
                key
                for row in rows
                if row.get("support_verdict") == "SUPPORTED"
                for key in ("relationship_specific_support", "non_naive_relationship_feature_support", "baseline_separated_support", "nonzero_support")
                if row.get(key)
            }
        ),
    }


def _feature_surface_analysis(features: list[Any]) -> dict[str, Any]:
    rows = [row for row in features if isinstance(row, dict)]
    type_counts: dict[str, int] = {}
    pairwise_count = 0
    for row in rows:
        feature_type = str(row.get("feature_type") or "UNKNOWN")
        type_counts[feature_type] = type_counts.get(feature_type, 0) + 1
        if feature_type in PAIRWISE_FEATURE_TYPES or row.get("relationship_key"):
            pairwise_count += 1
    return {
        "relationship_feature_count": len(rows),
        "feature_type_counts": dict(sorted(type_counts.items())),
        "pairwise_conventional_feature_count": pairwise_count,
        "pairwise_conventional_feature_ratio": _ratio(pairwise_count, len(rows)),
        "sample_relationship_keys": sorted({str(row.get("relationship_key")) for row in rows if row.get("relationship_key")})[:10],
    }


def _candidate_naming_analysis(candidates: list[Any]) -> dict[str, Any]:
    overnamed_terms = ("alpha", "trade", "real yield", "inflation", "nominal", "macro", "dxy")
    rows = [row for row in candidates if isinstance(row, dict)]
    overnamed = []
    for row in rows:
        name = str(row.get("proposed_name") or "")
        lower = name.lower()
        if any(term in lower for term in overnamed_terms):
            overnamed.append({"research_asset_candidate_id": row.get("research_asset_candidate_id"), "proposed_name": name})
    return {
        "candidate_name_count": len(rows),
        "over_named_count": len(overnamed),
        "over_named_candidates": overnamed,
    }


def _canonical_pair(value: str) -> str:
    if not value:
        return "UNKNOWN"
    parts = [part for part in value.replace("|", ":").split(":") if part]
    if len(parts) >= 2:
        return ":".join(sorted((parts[0], parts[1])))
    return value


def _ratio(numerator: int, denominator: int) -> float:
    return round(float(numerator) / float(denominator), 6) if denominator else 0.0


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _file_hash(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _hash(payload: dict[str, Any]) -> str:
    clone = json.loads(json.dumps(payload, sort_keys=True))
    clone.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clone, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
