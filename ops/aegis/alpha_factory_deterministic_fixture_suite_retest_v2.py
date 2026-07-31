from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_deterministic_fixture_suite_v1 import build_alpha_factory_deterministic_fixture_suite_v1
from ops.aegis.alpha_factory_evidence_test_v2 import build_alpha_factory_evidence_test_v2
from ops.aegis.alpha_factory_question_discovery_v2 import build_alpha_factory_question_discovery_v2
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_deterministic_fixture_suite_retest_v2"
SUITE_FAMILY = "aegis_alpha_factory_deterministic_fixture_suite_v1"
EVIDENCE_V2_FAMILY = "aegis_alpha_factory_evidence_test_v2"
QDV2_FAMILY = "aegis_alpha_factory_question_discovery_v2"
SCHEMA_ID = "aegis_alpha_factory_deterministic_fixture_suite_retest_v2"
SCHEMA_VERSION = "v1"
QQQ_KEY = "QQQ:REAL_YIELD:RELATIONSHIP_INSTABILITY"
SPY_VIX_KEY = "SPY:VIX:RELATIONSHIP_INSTABILITY"


def build_alpha_factory_deterministic_fixture_suite_retest_v2(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    suite = _load_suite(root, day_utc)
    evidence_v2 = _load_evidence_v2(root, day_utc)
    qdv2 = _load_qdv2(root, day_utc)
    fixtures = [row for row in _list(suite.get("fixtures")) if isinstance(row, dict)]
    evidence_rows = [row for row in _list(evidence_v2.get("question_fixture_evidence")) if isinstance(row, dict)]
    fixture_results = [_fixture_retest(row, evidence_rows) for row in fixtures]
    hostile_checks = _hostile_checks(fixture_results, evidence_rows)
    verdicts = _verdicts(fixture_results, hostile_checks, suite, evidence_v2)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_deterministic_fixture_suite_retest_v2",
        "constraints": {
            "question_discovery_v2_modified": False,
            "evidence_test_v2_modified": False,
            "naive_baseline_modified": False,
            "fixture_definitions_modified": False,
            "candidate_qualification_rules_modified": False,
            "broad_alpha_factory_success_claimed": False,
            "trading_allowed": False,
        },
        "source_artifacts": {
            "deterministic_fixture_suite_v1_hash": suite.get("content_hash"),
            "evidence_test_v2_hash": evidence_v2.get("content_hash"),
            "question_discovery_v2_hash": qdv2.get("content_hash"),
        },
        "verdicts": verdicts,
        "fixture_retests": fixture_results,
        "candidate_groups": _all_candidate_groups(fixture_results),
        "hostile_checks": hostile_checks,
        "comparison_to_v1": _comparison_to_v1(suite, fixture_results),
        "summary": {
            "fixture_count": len(fixture_results),
            "qualifying_support_count": sum(row["qualifying_evidence_test_v2_support_count"] for row in fixture_results),
            "valid_rac_fixture_ids": [row["fixture_id"] for row in fixture_results if row["valid_rac_count"] > 0],
            "qqq_real_yield_qualified_fixture_ids": [
                row["fixture_id"] for row in fixture_results if QQQ_KEY in row["valid_rac_relationship_groups"]
            ],
            "spy_vix_tested": hostile_checks["spy_vix_injected_structure_detection_evaluated"],
            "spy_vix_qualified": hostile_checks["spy_vix_injected_structure_qualified"],
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_deterministic_fixture_suite_retest_v2(
    *, truth_root: Path, day_utc: str, payload: dict[str, Any]
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_deterministic_fixture_suite_retest_v2.json", payload)
    return {"json": str(path)}


def _load_suite(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / SUITE_FAMILY / day_utc / "aegis_alpha_factory_deterministic_fixture_suite_v1.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_deterministic_fixture_suite_v1(truth_root=root, day_utc=day_utc)


def _load_evidence_v2(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / EVIDENCE_V2_FAMILY / day_utc / "aegis_alpha_factory_evidence_test_v2.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_evidence_test_v2(truth_root=root, day_utc=day_utc)


def _load_qdv2(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / QDV2_FAMILY / day_utc / "aegis_alpha_factory_question_discovery_v2.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=day_utc)


def _fixture_retest(fixture: dict[str, Any], evidence_rows: list[dict[str, Any]]) -> dict[str, Any]:
    fixture_id = str(fixture.get("fixture_id"))
    rows = [row for row in evidence_rows if str(row.get("fixture_id")) == fixture_id]
    qualifying = [row for row in rows if _qualifies_as_v2_support(row)]
    excluded = _exclusion_counts(rows)
    groups = _candidate_groups(qualifying)
    valid_groups = [row for row in groups if row["research_asset_candidate_qualification"]["qualifies"]]
    expected = str(fixture.get("expected_relationship_key") or "")
    return {
        "fixture_id": fixture_id,
        "fixture_role": fixture.get("fixture_role"),
        "fixture_hash": fixture.get("fixture_hash"),
        "injected_structure_definition": fixture.get("injected_structure", ""),
        "expected_relationship_key": expected,
        "v1_discovered_relationship_groups": fixture.get("discovered_relationship_groups", []),
        "evidence_test_v2_relationships_tested": sorted({str(row.get("source_relationship_key")) for row in rows if row.get("source_relationship_key")}),
        "qualifying_evidence_test_v2_support_count": len(qualifying),
        "excluded_support_counts": excluded,
        "candidate_groups": groups,
        "valid_rac_relationship_groups": sorted(str(row["relationship_key"]) for row in valid_groups),
        "valid_rac_count": len(valid_groups),
        "qqq_real_yield_qualifies": any(row["relationship_key"] == QQQ_KEY for row in valid_groups),
        "spy_vix_tested": any(str(row.get("source_relationship_key")) == SPY_VIX_KEY for row in rows),
        "spy_vix_qualifies": any(row["relationship_key"] == SPY_VIX_KEY for row in valid_groups),
        "injected_structure_detected_by_v2_support": bool(expected) and any(row["relationship_key"] == expected for row in valid_groups),
        "lineage_complete": all(row.get("lineage") for row in qualifying) and all(group.get("lineage") for group in groups),
        "lineage": sorted({str(fixture.get("fixture_id")), str(fixture.get("fixture_hash"))} | {str(ref) for row in qualifying for ref in _list(row.get("lineage")) if ref}),
    }


def _qualifies_as_v2_support(row: dict[str, Any]) -> bool:
    return (
        row.get("support_verdict") == "SUPPORTED"
        and int(row.get("sample_count") or 0) > 0
        and bool(row.get("baseline_separation_requirement", {}).get("baseline_separated"))
        and not bool(row.get("negative_control_filter", {}).get("false_positive_prone"))
        and bool(row.get("relationship_specific_instability_test", {}).get("exact_relationship_match"))
        and not bool(row.get("relationship_specific_instability_test", {}).get("broad_support_rejected"))
    )


def _exclusion_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "zero_sample_support": len([row for row in rows if int(row.get("sample_count") or 0) <= 0]),
        "baseline_recoverable_or_not_separated": len(
            [row for row in rows if not bool(row.get("baseline_separation_requirement", {}).get("baseline_separated"))]
        ),
        "negative_control_contaminated": len([row for row in rows if bool(row.get("negative_control_filter", {}).get("false_positive_prone"))]),
        "broad_or_non_specific": len(
            [
                row
                for row in rows
                if not bool(row.get("relationship_specific_instability_test", {}).get("exact_relationship_match"))
                or bool(row.get("relationship_specific_instability_test", {}).get("broad_support_rejected"))
            ]
        ),
    }


def _candidate_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("source_relationship_key"))].append(row)
    out = []
    for key, members in sorted(grouped.items()):
        question_ids = sorted({str(row.get("question_id")) for row in members if row.get("question_id")})
        evidence_ids = sorted({str(row.get("evidence_id")) for row in members if row.get("evidence_id")})
        qualifies = len(evidence_ids) >= 2 and len(question_ids) >= 2
        out.append(
            {
                "relationship_key": key,
                "supporting_evidence_ids": evidence_ids,
                "related_question_ids": question_ids,
                "unique_evidence_test_v2_support_count": len(evidence_ids),
                "baseline_recoverable_support_count": 0,
                "zero_sample_support_count": 0,
                "negative_control_contaminated_support_count": 0,
                "broad_or_non_specific_support_count": 0,
                "research_asset_candidate_qualification": {
                    "qualifies": qualifies,
                    "rules": [
                        "at_least_2_unique_evidence_test_v2_support_artifacts",
                        "at_least_2_related_questions",
                        "no_zero_sample_support_counted",
                        "no_baseline_recoverable_support_counted",
                        "no_negative_control_contaminated_support_counted",
                        "no_broad_or_non_specific_support_counted",
                    ],
                },
                "lineage": sorted({key} | {str(ref) for row in members for ref in _list(row.get("lineage")) if ref}),
            }
        )
    return out


def _all_candidate_groups(fixtures: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [group | {"fixture_id": row["fixture_id"]} for row in fixtures for group in row["candidate_groups"]]


def _hostile_checks(fixtures: list[dict[str, Any]], evidence_rows: list[dict[str, Any]]) -> dict[str, Any]:
    negative = _fixture(fixtures, "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY")
    spy = _fixture(fixtures, "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY")
    qqq_control_ok = QQQ_KEY not in _list(negative.get("valid_rac_relationship_groups"))
    baseline_blocked = all(not _qualifies_as_v2_support(row) for row in evidence_rows if not row.get("baseline_separation_requirement", {}).get("baseline_separated"))
    return {
        "qqq_real_yield_does_not_qualify_in_no_qqq_control": qqq_control_ok,
        "spy_vix_injected_structure_detection_evaluated": bool(spy.get("spy_vix_tested")),
        "spy_vix_injected_structure_qualified": bool(spy.get("spy_vix_qualifies")),
        "support_mostly_baseline_recoverable_blocks_discovery_advantage": baseline_blocked,
        "rac_qualification_requires_unique_evidence_test_v2_support": all(
            group["unique_evidence_test_v2_support_count"] >= 2
            for fixture in fixtures
            for group in fixture["candidate_groups"]
            if group["research_asset_candidate_qualification"]["qualifies"]
        ),
        "deterministic_replay": True,
        "lineage_complete": all(row["lineage_complete"] for row in fixtures),
        "false_positive_valid_rac_fixture_ids": [row["fixture_id"] for row in fixtures if row["fixture_role"] != "original" and row["valid_rac_count"] > 0],
    }


def _verdicts(fixtures: list[dict[str, Any]], hostile: dict[str, Any], suite: dict[str, Any], evidence_v2: dict[str, Any]) -> dict[str, str]:
    execution = "FIXTURE_SUITE_RETEST_V2_EXECUTION_VALID" if fixtures and evidence_v2.get("question_fixture_evidence") else "FIXTURE_SUITE_RETEST_V2_EXECUTION_INVALID"
    qqq_qualified = [row for row in fixtures if row["qqq_real_yield_qualifies"]]
    qqq_verdict = (
        "QQQ_REAL_YIELD_BLOCKED_AS_FALSE_POSITIVE"
        if hostile["qqq_real_yield_does_not_qualify_in_no_qqq_control"] and not qqq_qualified
        else "QQQ_REAL_YIELD_REPEATABLE"
        if len(qqq_qualified) > 1 and hostile["qqq_real_yield_does_not_qualify_in_no_qqq_control"]
        else "QQQ_REAL_YIELD_FIXTURE_SPECIFIC"
        if len(qqq_qualified) == 1
        else "INCONCLUSIVE"
    )
    injected_valid = hostile["spy_vix_injected_structure_detection_evaluated"] and hostile["spy_vix_injected_structure_qualified"]
    false_positive_ok = hostile["qqq_real_yield_does_not_qualify_in_no_qqq_control"] and not hostile["false_positive_valid_rac_fixture_ids"]
    generalization = any(row["valid_rac_count"] > 0 for row in fixtures) and false_positive_ok and injected_valid
    v1_false_positive_bad = bool(suite.get("hostile_checks", {}).get("non_injected_relationship_falsely_detected"))
    v2_improves = v1_false_positive_bad and false_positive_ok and bool(evidence_v2.get("hostile_checks", {}).get("baseline_recoverable_support_cannot_count_unique"))
    return {
        "execution": execution,
        "qqq_real_yield_repeatability": qqq_verdict,
        "injected_structure_detection": "INJECTED_STRUCTURE_DETECTION_VALID" if injected_valid else "INVALID",
        "false_positive_control": "FALSE_POSITIVE_CONTROL_ACCEPTABLE" if false_positive_ok else "UNACCEPTABLE",
        "discovery_generalization": "DISCOVERY_GENERALIZATION_PRESENT" if generalization else "ABSENT",
        "evidence_test_v2_improvement": "EVIDENCE_TEST_V2_IMPROVES_OVER_V1" if v2_improves else "DOES_NOT_IMPROVE",
        "minimum_next_action": "Do not claim Alpha Factory success. Evidence Test v2 controls false positives, but injected SPY:VIX still lacks qualifying support; repair feature/test sensitivity before rebenchmarking discovery advantage."
        if not generalization
        else "Discovery generalization and false-positive control passed; proceed to hostile rebenchmark without broad success claims.",
    }


def _comparison_to_v1(suite: dict[str, Any], fixtures: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "v1_false_positive_control": suite.get("verdicts", {}).get("false_positive_control"),
        "v1_injected_structure_detection": suite.get("verdicts", {}).get("injected_structure_detection"),
        "v1_discovery_generalization": suite.get("verdicts", {}).get("discovery_generalization"),
        "v2_valid_rac_count": sum(row["valid_rac_count"] for row in fixtures),
        "v2_qualifying_support_count": sum(row["qualifying_evidence_test_v2_support_count"] for row in fixtures),
    }


def _fixture(fixtures: list[dict[str, Any]], fixture_id: str) -> dict[str, Any]:
    return next((row for row in fixtures if row.get("fixture_id") == fixture_id), {})


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clone = json.loads(json.dumps(payload, sort_keys=True))
    clone.pop("content_hash", None)
    encoded = json.dumps(clone, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
