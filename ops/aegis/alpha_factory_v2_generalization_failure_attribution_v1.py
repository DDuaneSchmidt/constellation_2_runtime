from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import build_alpha_factory_naive_correlation_baseline_v1
from ops.aegis.alpha_factory_non_naive_evidence_test_v1 import build_alpha_factory_non_naive_evidence_test_v1
from ops.aegis.alpha_factory_question_discovery_v2 import build_alpha_factory_question_discovery_v2
from ops.aegis.alpha_factory_v2_non_naive_rebenchmark_v1 import build_alpha_factory_v2_non_naive_rebenchmark_v1
from ops.aegis.alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1 import (
    build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1,
)
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_v2_generalization_failure_attribution_v1"
SCHEMA_ID = "aegis_alpha_factory_v2_generalization_failure_attribution"
SCHEMA_VERSION = "v1"

V2_REBENCHMARK_FAMILY = "aegis_alpha_factory_v2_non_naive_rebenchmark_v1"
OOF_REBENCHMARK_FAMILY = "aegis_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1"
NON_NAIVE_FAMILY = "aegis_alpha_factory_non_naive_evidence_test_v1"
QDV2_FAMILY = "aegis_alpha_factory_question_discovery_v2"
BASELINE_FAMILY = "aegis_alpha_factory_naive_correlation_baseline_v1"


def build_alpha_factory_v2_generalization_failure_attribution_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    in_rebenchmark = _load_or_build(
        root,
        day_utc,
        V2_REBENCHMARK_FAMILY,
        "aegis_alpha_factory_v2_non_naive_rebenchmark_v1.json",
        build_alpha_factory_v2_non_naive_rebenchmark_v1,
    )
    out_rebenchmark = _load_or_build(
        root,
        day_utc,
        OOF_REBENCHMARK_FAMILY,
        "aegis_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1.json",
        build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1,
    )
    non_naive = _load_or_build(
        root,
        day_utc,
        NON_NAIVE_FAMILY,
        "aegis_alpha_factory_non_naive_evidence_test_v1.json",
        build_alpha_factory_non_naive_evidence_test_v1,
    )
    qdv2 = _load_or_build(
        root,
        day_utc,
        QDV2_FAMILY,
        "aegis_alpha_factory_question_discovery_v2.json",
        build_alpha_factory_question_discovery_v2,
    )
    baseline = _load_or_build(
        root,
        day_utc,
        BASELINE_FAMILY,
        "aegis_alpha_factory_naive_correlation_baseline_v1.json",
        build_alpha_factory_naive_correlation_baseline_v1,
    )

    comparison = _comparison(in_rebenchmark, out_rebenchmark, non_naive, qdv2, baseline)
    hostile_checks = _hostile_checks(comparison, in_rebenchmark, out_rebenchmark)
    primary_mode = _primary_failure_mode(comparison, hostile_checks)
    fault_class = _fault_class(primary_mode)
    recommendation = _recommendation(primary_mode)
    minimum_next_action = _minimum_next_action(primary_mode, recommendation)
    execution_valid = bool(in_rebenchmark) and bool(out_rebenchmark) and bool(non_naive) and bool(qdv2)

    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_v2_generalization_failure_attribution_v1",
        "constraints": {
            "question_discovery_v2_modified": False,
            "non_naive_evidence_test_v1_modified": False,
            "in_fixture_rebenchmark_modified": False,
            "out_of_fixture_rebenchmark_modified": False,
            "naive_baseline_modified": False,
            "v3_built": False,
            "trading_allowed": False,
        },
        "source_artifacts": {
            "v2_non_naive_rebenchmark_v1_hash": in_rebenchmark.get("content_hash", ""),
            "v2_out_of_fixture_non_naive_rebenchmark_v1_hash": out_rebenchmark.get("content_hash", ""),
            "non_naive_evidence_test_v1_hash": non_naive.get("content_hash", ""),
            "question_discovery_v2_hash": qdv2.get("content_hash", ""),
            "naive_baseline_v1_hash": baseline.get("content_hash", ""),
        },
        "verdicts": {
            "execution": "ATTRIBUTION_EXECUTION_VALID" if execution_valid else "ATTRIBUTION_EXECUTION_INVALID",
            "primary_generalization_failure_mode": primary_mode,
            "fault_class": fault_class,
            "recommendation": recommendation,
            "minimum_next_action": minimum_next_action,
        },
        "comparison": comparison,
        "hostile_checks": hostile_checks,
        "summary": {
            "in_fixture_unique_support_count": comparison["support_counts"]["in_fixture"]["unique_support_count"],
            "out_of_fixture_unique_support_count": comparison["support_counts"]["out_of_fixture"]["unique_support_count"],
            "in_fixture_qualified_candidate_count": comparison["candidate_group_formation"]["in_fixture"]["qualified_count"],
            "out_of_fixture_qualified_candidate_count": comparison["candidate_group_formation"]["out_of_fixture"]["qualified_count"],
            "primary_failure_mode": primary_mode,
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_v2_generalization_failure_attribution_v1(
    *,
    truth_root: Path,
    day_utc: str,
    payload: dict[str, Any],
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_v2_generalization_failure_attribution_v1.json", payload)
    return {"json": str(path)}


def _comparison(
    in_rebenchmark: dict[str, Any],
    out_rebenchmark: dict[str, Any],
    non_naive: dict[str, Any],
    qdv2: dict[str, Any],
    baseline: dict[str, Any],
) -> dict[str, Any]:
    in_evidence = _list(in_rebenchmark.get("evidence_artifacts"))
    out_evidence = _list(out_rebenchmark.get("evidence_artifacts"))
    in_groups = _list(in_rebenchmark.get("candidate_groups"))
    out_groups = _list(out_rebenchmark.get("candidate_groups"))
    return {
        "question_trigger_distribution": {
            "in_fixture_all_v2_questions": _count_by(qdv2.get("questions"), "source_type"),
            "in_fixture_supported_questions": _count_by(non_naive.get("question_evidence"), "trigger_type", only_supported=True),
            "out_of_fixture_supported_questions": _count_by(out_evidence, "trigger_type"),
            "note": "Out-of-fixture prior artifact exposes supported evidence trigger distribution, not the full generated question list.",
        },
        "support_counts": {
            "in_fixture": {
                "non_naive_supported_count": int(in_rebenchmark.get("summary", {}).get("non_naive_supported_evidence_count") or 0),
                "unique_support_count": int(in_rebenchmark.get("summary", {}).get("unique_supported_evidence_count") or 0),
                "baseline_recoverable_support_count": int(in_rebenchmark.get("summary", {}).get("baseline_recoverable_supported_evidence_count") or 0),
            },
            "out_of_fixture": {
                "non_naive_supported_count": int(out_rebenchmark.get("summary", {}).get("non_naive_supported_evidence_count") or 0),
                "unique_support_count": int(out_rebenchmark.get("summary", {}).get("unique_supported_evidence_count") or 0),
                "baseline_recoverable_support_count": int(out_rebenchmark.get("summary", {}).get("baseline_recoverable_supported_evidence_count") or 0),
            },
        },
        "candidate_group_formation": {
            "in_fixture": _group_summary(in_groups),
            "out_of_fixture": _group_summary(out_groups),
        },
        "relationship_feature_coverage": {
            "in_fixture_relationship_keys": sorted({str(row.get("relationship_key")) for row in in_groups if isinstance(row, dict)}),
            "out_of_fixture_relationship_keys": sorted({str(row.get("relationship_key")) for row in out_groups if isinstance(row, dict)}),
            "in_fixture_unique_relationship_keys": sorted({str(row.get("relationship_key")) for row in in_groups if _qualifies(row)}),
            "out_of_fixture_unique_relationship_keys": sorted({str(row.get("relationship_key")) for row in out_groups if _qualifies(row)}),
            "baseline_top_cluster_ids": [str(row.get("cluster_id")) for row in _list(baseline.get("top_candidate_relationship_clusters"))[:5] if isinstance(row, dict)],
        },
        "supporting_test_types": {
            "in_fixture": _test_type_counts(in_evidence),
            "out_of_fixture": _test_type_counts(out_evidence),
            "in_fixture_unique": _test_type_counts([row for row in in_evidence if row.get("unique_support_vs_naive_baseline")]),
            "out_of_fixture_unique": _test_type_counts([row for row in out_evidence if row.get("unique_support_vs_naive_baseline")]),
        },
    }


def _hostile_checks(
    comparison: dict[str, Any],
    in_rebenchmark: dict[str, Any],
    out_rebenchmark: dict[str, Any],
) -> dict[str, Any]:
    in_unique_keys = comparison["relationship_feature_coverage"]["in_fixture_unique_relationship_keys"]
    out_keys = comparison["relationship_feature_coverage"]["out_of_fixture_relationship_keys"]
    in_groups = _list(in_rebenchmark.get("candidate_groups"))
    qualified_groups = [row for row in in_groups if _qualifies(row)]
    return {
        "in_fixture_unique_support_came_from_one_relationship_only": len(in_unique_keys) == 1,
        "in_fixture_unique_relationship_keys": in_unique_keys,
        "out_of_fixture_lacked_same_relationship": bool(in_unique_keys) and not set(in_unique_keys).issubset(set(out_keys)),
        "evidence_tests_produced_support_but_no_unique_support_out_of_fixture": comparison["support_counts"]["out_of_fixture"]["non_naive_supported_count"] > 0
        and comparison["support_counts"]["out_of_fixture"]["unique_support_count"] == 0,
        "all_out_of_fixture_support_baseline_recoverable": comparison["support_counts"]["out_of_fixture"]["non_naive_supported_count"]
        == comparison["support_counts"]["out_of_fixture"]["baseline_recoverable_support_count"]
        and comparison["support_counts"]["out_of_fixture"]["non_naive_supported_count"] > 0,
        "candidate_grouping_required_in_fixture_specific_structure": len(qualified_groups) == 1
        and len(qualified_groups[0].get("supporting_evidence") or []) == 2
        if qualified_groups
        else False,
        "out_of_fixture_support_mostly_baseline_recoverable": bool(out_rebenchmark.get("hostile_checks", {}).get("support_mostly_baseline_recoverable")),
        "out_of_fixture_insufficient_unique_non_naive_evidence_count": int(out_rebenchmark.get("summary", {}).get("unique_supported_evidence_count") or 0) < 2,
        "lineage_complete": in_rebenchmark.get("verdicts", {}).get("lineage") == "LINEAGE_COMPLETE"
        and out_rebenchmark.get("verdicts", {}).get("lineage") == "LINEAGE_COMPLETE",
        "deterministic_replay": bool(in_rebenchmark.get("summary", {}).get("output_reproducible"))
        and bool(out_rebenchmark.get("summary", {}).get("output_reproducible")),
    }


def _primary_failure_mode(comparison: dict[str, Any], hostile: dict[str, Any]) -> str:
    if (
        hostile["in_fixture_unique_support_came_from_one_relationship_only"]
        and hostile["out_of_fixture_lacked_same_relationship"]
        and hostile["all_out_of_fixture_support_baseline_recoverable"]
    ):
        return "FIXTURE_SPECIFIC_RELATIONSHIP"
    if hostile["all_out_of_fixture_support_baseline_recoverable"]:
        return "BASELINE_REDUNDANCY"
    if hostile["evidence_tests_produced_support_but_no_unique_support_out_of_fixture"]:
        return "NON_NAIVE_TEST_OVERFIT"
    if hostile["candidate_grouping_required_in_fixture_specific_structure"]:
        return "CANDIDATE_GROUPING_OVERFIT"
    if not comparison["question_trigger_distribution"]["out_of_fixture_supported_questions"]:
        return "WEAK_QUESTION_GENERALIZATION"
    return "INCONCLUSIVE"


def _fault_class(primary_mode: str) -> str:
    if primary_mode in {"FIXTURE_SPECIFIC_RELATIONSHIP", "INSUFFICIENT_FIXTURE_DIVERSITY"}:
        return "FIXTURE_FAULT"
    if primary_mode == "WEAK_QUESTION_GENERALIZATION":
        return "QUESTION_GENERATION_FAULT"
    if primary_mode in {"NON_NAIVE_TEST_OVERFIT", "BASELINE_REDUNDANCY"}:
        return "EVIDENCE_TEST_FAULT"
    if primary_mode == "CANDIDATE_GROUPING_OVERFIT":
        return "CANDIDATE_GROUPING_FAULT"
    return "INCONCLUSIVE"


def _recommendation(primary_mode: str) -> str:
    if primary_mode in {"FIXTURE_SPECIFIC_RELATIONSHIP", "INSUFFICIENT_FIXTURE_DIVERSITY"}:
        return "FIXTURE_SUITE_RECOMMENDED"
    if primary_mode in {"NON_NAIVE_TEST_OVERFIT", "BASELINE_REDUNDANCY"}:
        return "EVIDENCE_TEST_V2_RECOMMENDED"
    if primary_mode == "CANDIDATE_GROUPING_OVERFIT":
        return "CANDIDATE_GROUPING_V2_RECOMMENDED"
    if primary_mode == "WEAK_QUESTION_GENERALIZATION":
        return "V3_RECOMMENDED"
    return "STOP_AND_REDESIGN"


def _minimum_next_action(primary_mode: str, recommendation: str) -> str:
    if recommendation == "FIXTURE_SUITE_RECOMMENDED":
        return "Do not build v3 yet. Build a deterministic fixture suite to test whether the QQQ-REAL_YIELD relationship was fixture-specific."
    if recommendation == "EVIDENCE_TEST_V2_RECOMMENDED":
        return "Do not build v3 yet. Repair evidence tests so out-of-fixture support must exceed naive baseline recoverability."
    if recommendation == "CANDIDATE_GROUPING_V2_RECOMMENDED":
        return "Do not build v3 yet. Repair candidate grouping breadth before changing question generation."
    if recommendation == "V3_RECOMMENDED":
        return "Question generation is the likely fault; only then consider v3 question discovery."
    return f"Stop the Alpha Factory POC line and redesign; attribution remained {primary_mode}."


def _group_summary(groups: list[Any]) -> dict[str, Any]:
    group_rows = [row for row in groups if isinstance(row, dict)]
    qualified = [row for row in group_rows if _qualifies(row)]
    return {
        "group_count": len(group_rows),
        "qualified_count": len(qualified),
        "relationship_keys": sorted({str(row.get("relationship_key")) for row in group_rows}),
        "qualified_relationship_keys": sorted({str(row.get("relationship_key")) for row in qualified}),
        "qualified_supporting_evidence_counts": [len(row.get("supporting_evidence") or []) for row in qualified],
    }


def _qualifies(row: dict[str, Any]) -> bool:
    qual = row.get("research_asset_candidate_qualification")
    return isinstance(qual, dict) and bool(qual.get("qualifies"))


def _count_by(rows: Any, field: str, *, only_supported: bool = False) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in _list(rows):
        if not isinstance(row, dict):
            continue
        if only_supported and not row.get("supported_tests"):
            continue
        key = str(row.get(field) or "")
        if not key:
            continue
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items()))


def _test_type_counts(rows: list[Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        test = str(row.get("non_naive_test") or "")
        if not test:
            continue
        out[test] = out.get(test, 0) + 1
    return dict(sorted(out.items()))


def _load_or_build(root: Path, day_utc: str, family: str, filename: str, builder: Any) -> dict[str, Any]:
    payload = read_json_v1(root / "reports" / family / day_utc / filename)
    return payload if isinstance(payload, dict) and payload else builder(truth_root=root, day_utc=day_utc)


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
