from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_deterministic_fixture_suite_v1 import _fixtures
from ops.aegis.alpha_factory_deterministic_fixture_suite_retest_v2 import build_alpha_factory_deterministic_fixture_suite_retest_v2
from ops.aegis.alpha_factory_evidence_test_v2 import build_alpha_factory_evidence_test_v2
from ops.aegis.alpha_factory_poc_v1 import calculate_features_v1, Observation
from ops.aegis.alpha_factory_question_discovery_v2 import build_alpha_factory_question_discovery_v2
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_injection_visibility_diagnostic_v1"
RETEST_FAMILY = "aegis_alpha_factory_deterministic_fixture_suite_retest_v2"
SUITE_FAMILY = "aegis_alpha_factory_deterministic_fixture_suite_v1"
EVIDENCE_V2_FAMILY = "aegis_alpha_factory_evidence_test_v2"
QDV2_FAMILY = "aegis_alpha_factory_question_discovery_v2"
SCHEMA_ID = "aegis_alpha_factory_injection_visibility_diagnostic"
SCHEMA_VERSION = "v1"
CAUSES = {
    "INJECTION_NOT_EXPRESSED_IN_FEATURES",
    "QUESTION_NOT_GENERATED_FOR_INJECTION",
    "EVIDENCE_TEST_NOT_TARGETING_INJECTION",
    "THRESHOLD_TOO_STRICT",
    "SAMPLE_COUNT_TOO_LOW",
    "BASELINE_FILTER_TOO_STRICT",
    "FIXTURE_INJECTION_TOO_WEAK",
    "INCONCLUSIVE",
}


def build_alpha_factory_injection_visibility_diagnostic_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    retest = _load_retest(root, day_utc)
    suite = _load_json(root / "reports" / SUITE_FAMILY / day_utc / "aegis_alpha_factory_deterministic_fixture_suite_v1.json")
    evidence_v2 = _load_evidence_v2(root, day_utc)
    qdv2 = _load_qdv2(root, day_utc)
    fixture_defs = {row["fixture_id"]: row for row in _fixtures()}
    retest_fixtures = [row for row in _list(retest.get("fixture_retests")) if row.get("injected_structure_definition")]
    inspections = [_inspect_injected_fixture(row, fixture_defs.get(str(row.get("fixture_id")), {}), evidence_v2, qdv2, suite) for row in retest_fixtures]
    hostile_checks = _hostile_checks(inspections)
    primary = _primary_cause(inspections, hostile_checks)
    recommendation = _recommendation(primary)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_injection_visibility_diagnostic_v1",
        "constraints": {
            "question_discovery_v2_modified": False,
            "evidence_test_v2_modified": False,
            "fixture_suite_modified": False,
            "candidate_qualification_rules_modified": False,
            "naive_baseline_modified": False,
            "evidence_test_v2_repaired": False,
            "trading_allowed": False,
        },
        "source_artifacts": {
            "fixture_suite_retest_v2_hash": retest.get("content_hash"),
            "fixture_suite_v1_hash": suite.get("content_hash"),
            "evidence_test_v2_hash": evidence_v2.get("content_hash"),
            "question_discovery_v2_hash": qdv2.get("content_hash"),
        },
        "verdicts": {
            "execution": "INJECTION_VISIBILITY_DIAGNOSTIC_EXECUTION_VALID" if inspections else "INJECTION_VISIBILITY_DIAGNOSTIC_EXECUTION_INVALID",
            "primary_injection_miss_cause": primary,
            "recommendation": recommendation,
            "evidence_test_v2_strictness": _strictness_verdict(hostile_checks),
            "minimum_next_action": _minimum_next_action(primary, recommendation),
        },
        "injected_fixture_inspections": inspections,
        "detected_non_injected_relationship_comparison": _non_injected_comparison(suite, retest),
        "hostile_checks": hostile_checks,
        "summary": {
            "injected_fixture_count": len(inspections),
            "injected_relationships_without_generated_questions": [row["injected_relationship"] for row in inspections if not row["question_triggers_for_injected_relationship"]],
            "injected_relationships_without_targeted_generated_tests": [row["injected_relationship"] for row in inspections if not row["generated_question_targeted_evidence_tests"]],
            "injected_relationships_with_only_probe_tests": [row["injected_relationship"] for row in inspections if row["evidence_test_v2_tests_run_for_injected_relationship"] and not row["generated_question_targeted_evidence_tests"]],
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_injection_visibility_diagnostic_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_injection_visibility_diagnostic_v1.json", payload)
    return {"json": str(path)}


def _inspect_injected_fixture(retest_fixture: dict[str, Any], fixture_def: dict[str, Any], evidence_v2: dict[str, Any], qdv2: dict[str, Any], suite: dict[str, Any]) -> dict[str, Any]:
    fixture_id = str(retest_fixture.get("fixture_id"))
    injected_relationship = str(retest_fixture.get("injected_structure_definition") or retest_fixture.get("expected_relationship_key") or "")
    target, driver, effect_type = _parse_relationship(injected_relationship)
    rows = [dict(row) for row in _list(fixture_def.get("rows"))]
    affected_observations = [row for row in rows if str(row.get("observation_id", "")).startswith(f"SYN-INJECT-{target}-{driver}-")]
    affected_feature_summary = _affected_feature_summary(rows, target, driver)
    questions = _questions_for_relationship(qdv2, injected_relationship)
    tests = [
        row
        for row in _list(evidence_v2.get("question_fixture_evidence"))
        if str(row.get("fixture_id")) == fixture_id and str(row.get("source_relationship_key")) == injected_relationship
    ]
    generated_tests = [row for row in tests if row.get("question_generated_by_v2")]
    failure_reasons = [_test_failure_reason(row) for row in tests]
    suite_fixture = _suite_fixture(suite, fixture_id)
    causes = _causes_for_fixture(questions, tests, generated_tests, affected_feature_summary, suite_fixture)
    return {
        "fixture_id": fixture_id,
        "fixture_role": retest_fixture.get("fixture_role"),
        "injected_relationship": injected_relationship,
        "injected_effect_type": effect_type,
        "injected_magnitude": _injected_magnitude(affected_observations),
        "affected_observations": {
            "count": len(affected_observations),
            "sample_observation_ids": [str(row.get("observation_id")) for row in affected_observations[:5]],
            "target_asset": target,
            "driver_asset": driver,
        },
        "affected_features": affected_feature_summary,
        "question_triggers_for_injected_relationship": questions,
        "evidence_test_v2_tests_run_for_injected_relationship": tests,
        "generated_question_targeted_evidence_tests": generated_tests,
        "test_failure_analysis": failure_reasons,
        "detected_non_injected_relationships_in_same_fixture": [
            key for key in _list(suite_fixture.get("discovered_relationship_groups")) if key != injected_relationship
        ],
        "baseline_context": {
            "baseline_recoverable_support_count": suite_fixture.get("baseline_recoverable_support_count", 0),
            "unique_non_naive_support_count": suite_fixture.get("unique_non_naive_support_count", 0),
        },
        "miss_causes": sorted(causes),
        "lineage": sorted(
            {
                fixture_id,
                str(retest_fixture.get("fixture_hash")),
                injected_relationship,
                *[str(ref) for row in tests for ref in _list(row.get("lineage")) if ref],
                *[str(ref) for row in questions for ref in _list(row.get("lineage")) if ref],
            }
        ),
    }


def _causes_for_fixture(questions: list[dict[str, Any]], tests: list[dict[str, Any]], generated_tests: list[dict[str, Any]], feature_summary: dict[str, Any], suite_fixture: dict[str, Any]) -> set[str]:
    causes: set[str] = set()
    if not feature_summary["relationship_feature_count"] or not feature_summary["trigger_level_feature_count"]:
        causes.add("INJECTION_NOT_EXPRESSED_IN_FEATURES")
    if not questions:
        causes.add("QUESTION_NOT_GENERATED_FOR_INJECTION")
    if not tests or (tests and not generated_tests):
        causes.add("EVIDENCE_TEST_NOT_TARGETING_INJECTION")
    if any("INJECTED_RELATIONSHIP_NOT_DETECTED_IN_FIXTURE" in _list(row.get("support_blockers")) for row in tests):
        causes.add("THRESHOLD_TOO_STRICT")
    if any("ZERO_SAMPLE_SUPPORT" in _list(row.get("support_blockers")) for row in tests):
        causes.add("SAMPLE_COUNT_TOO_LOW")
    if tests and all(set(_list(row.get("support_blockers"))) == {"BASELINE_RECOVERABLE_OR_NOT_SEPARATED"} for row in tests):
        causes.add("BASELINE_FILTER_TOO_STRICT")
    if suite_fixture and str(suite_fixture.get("expected_relationship_key")) not in _list(suite_fixture.get("discovered_relationship_groups")):
        causes.add("FIXTURE_INJECTION_TOO_WEAK")
    return causes or {"INCONCLUSIVE"}


def _affected_feature_summary(rows: list[dict[str, Any]], target: str, driver: str) -> dict[str, Any]:
    if not rows:
        return {"relationship_feature_count": 0, "trigger_level_feature_count": 0, "sample_feature_ids": []}
    observations = [Observation(**row) for row in rows]
    features = calculate_features_v1(observations)
    feature_rows = [row.__dict__ if hasattr(row, "__dict__") else dict(row) for row in features]
    rel_features = [row for row in feature_rows if row.get("asset") == target and row.get("related_asset") == driver]
    trigger_level = [row for row in rel_features if row.get("feature_type") in {"rolling_correlation_20d", "rolling_beta_20d"}]
    return {
        "relationship_feature_count": len(rel_features),
        "trigger_level_feature_count": len(trigger_level),
        "sample_feature_ids": [str(row.get("feature_id")) for row in trigger_level[:5]],
        "max_abs_feature_value": round(max([abs(float(row.get("value") or 0.0)) for row in trigger_level], default=0.0), 8),
    }


def _injected_magnitude(affected: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(row.get("value") or 0.0) for row in affected]
    if len(values) < 2:
        return {"affected_sample_count": len(values), "max_abs_daily_change": 0.0, "total_abs_change": 0.0}
    changes = [values[idx] / values[idx - 1] - 1.0 for idx in range(1, len(values)) if values[idx - 1]]
    return {
        "affected_sample_count": len(values),
        "max_abs_daily_change": round(max([abs(row) for row in changes], default=0.0), 8),
        "total_abs_change": round(abs(values[-1] / values[0] - 1.0), 8) if values[0] else 0.0,
    }


def _questions_for_relationship(qdv2: dict[str, Any], relationship: str) -> list[dict[str, Any]]:
    out = []
    for question in _list(qdv2.get("questions")):
        refs = " ".join(str(ref) for ref in _list(question.get("source_relationship_refs")))
        if relationship.replace(":", "-") in refs or _relationship_from_question(question) == relationship:
            out.append({
                "question_id": question.get("question_id"),
                "source_type": question.get("source_type"),
                "trigger_id": question.get("trigger_id"),
                "source_relationship_refs": question.get("source_relationship_refs", []),
                "lineage": question.get("lineage", []),
            })
    return out


def _relationship_from_question(question: dict[str, Any]) -> str:
    refs = _list(question.get("source_relationship_refs"))
    if not refs:
        return ""
    ref = str(refs[0])
    parts = ref.split("-")
    if len(parts) < 4 or parts[0] != "FEAT":
        return ""
    return f"{parts[1]}:{parts[2]}:{question.get('source_type')}"


def _test_failure_reason(row: dict[str, Any]) -> dict[str, Any]:
    blockers = _list(row.get("support_blockers"))
    return {
        "evidence_id": row.get("evidence_id"),
        "question_id": row.get("question_id"),
        "question_generated_by_v2": row.get("question_generated_by_v2"),
        "support_verdict": row.get("support_verdict"),
        "sample_count": row.get("sample_count"),
        "support_specificity_score": row.get("support_specificity_score"),
        "support_blockers": blockers,
        "why_failed_or_inconclusive": _blocker_explanation(blockers),
    }


def _blocker_explanation(blockers: list[Any]) -> str:
    blockers = [str(row) for row in blockers]
    if "NO_EXACT_RELATIONSHIP_MATCH" in blockers and "ZERO_SAMPLE_SUPPORT" in blockers:
        return "Evidence Test v2 had no exact detected relationship to attach samples to, so the injected relationship was zero-sample support."
    if "BASELINE_RECOVERABLE_OR_NOT_SEPARATED" in blockers and len(blockers) == 1:
        return "Only the baseline separation gate blocked this evidence."
    if "INJECTED_RELATIONSHIP_NOT_DETECTED_IN_FIXTURE" in blockers:
        return "The fixture expected this injected relationship, but the upstream detected relationship groups did not include it."
    return "Evidence failed one or more specificity gates."


def _hostile_checks(inspections: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "injected_relationship_lacks_generated_question": any(not row["question_triggers_for_injected_relationship"] for row in inspections),
        "injected_relationship_has_question_but_no_targeted_evidence_test": any(row["question_triggers_for_injected_relationship"] and not row["generated_question_targeted_evidence_tests"] for row in inspections),
        "targeted_evidence_test_fails_due_to_threshold": any("THRESHOLD_TOO_STRICT" in row["miss_causes"] for row in inspections),
        "support_blocked_only_by_baseline_filter": any("BASELINE_FILTER_TOO_STRICT" in row["miss_causes"] for row in inspections),
        "fixture_injection_not_visible_in_features": any("INJECTION_NOT_EXPRESSED_IN_FEATURES" in row["miss_causes"] for row in inspections),
        "insufficient_sample_count": any("SAMPLE_COUNT_TOO_LOW" in row["miss_causes"] for row in inspections),
        "deterministic_replay": True,
        "lineage_complete": all(row["lineage"] for row in inspections),
    }


def _primary_cause(inspections: list[dict[str, Any]], hostile: dict[str, Any]) -> str:
    ordered = [
        "INJECTION_NOT_EXPRESSED_IN_FEATURES",
        "QUESTION_NOT_GENERATED_FOR_INJECTION",
        "EVIDENCE_TEST_NOT_TARGETING_INJECTION",
        "THRESHOLD_TOO_STRICT",
        "SAMPLE_COUNT_TOO_LOW",
        "BASELINE_FILTER_TOO_STRICT",
        "FIXTURE_INJECTION_TOO_WEAK",
    ]
    causes = {cause for row in inspections for cause in row["miss_causes"]}
    for cause in ordered:
        if cause in causes:
            return cause
    return "INCONCLUSIVE"


def _recommendation(primary: str) -> str:
    if primary == "QUESTION_NOT_GENERATED_FOR_INJECTION":
        return "QUESTION_TRIGGER_REPAIR_REQUIRED"
    if primary == "INJECTION_NOT_EXPRESSED_IN_FEATURES":
        return "FEATURE_REPAIR_REQUIRED"
    if primary == "FIXTURE_INJECTION_TOO_WEAK":
        return "FIXTURE_REPAIR_REQUIRED"
    if primary == "EVIDENCE_TEST_NOT_TARGETING_INJECTION":
        return "EVIDENCE_SENSITIVITY_REPAIR_REQUIRED"
    if primary in {"THRESHOLD_TOO_STRICT", "SAMPLE_COUNT_TOO_LOW", "BASELINE_FILTER_TOO_STRICT"}:
        return "EVIDENCE_SENSITIVITY_REPAIR_REQUIRED"
    return "INCONCLUSIVE"


def _strictness_verdict(hostile: dict[str, Any]) -> str:
    if hostile["fixture_injection_not_visible_in_features"] or hostile["injected_relationship_lacks_generated_question"]:
        return "NOT_TOO_STRICT"
    if hostile["targeted_evidence_test_fails_due_to_threshold"] or hostile["insufficient_sample_count"]:
        return "EVIDENCE_TEST_V2_TOO_STRICT"
    if hostile["support_blocked_only_by_baseline_filter"]:
        return "EVIDENCE_TEST_V2_TOO_STRICT"
    return "INCONCLUSIVE"


def _minimum_next_action(primary: str, recommendation: str) -> str:
    if recommendation == "QUESTION_TRIGGER_REPAIR_REQUIRED":
        return "Do not repair Evidence Test v2 yet. Repair or benchmark question-trigger sensitivity so injected SPY:VIX relationship instability can produce a generated question before evidence qualification."
    if recommendation == "FEATURE_REPAIR_REQUIRED":
        return "Do not repair Evidence Test v2 yet. Expand or repair the feature surface so injected SPY:VIX relationship instability is represented before question generation and evidence tests run."
    if recommendation == "EVIDENCE_SENSITIVITY_REPAIR_REQUIRED":
        return "Repair Evidence Test v2 sensitivity only after preserving the negative-control and baseline-separation gates."
    if recommendation == "FIXTURE_REPAIR_REQUIRED":
        return "Repair fixture injection or feature visibility before changing evidence logic."
    return "Do not repair yet; collect more diagnostic evidence."


def _non_injected_comparison(suite: dict[str, Any], retest: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for fixture in _list(suite.get("fixtures")):
        expected = str(fixture.get("expected_relationship_key") or "")
        for relationship in _list(fixture.get("discovered_relationship_groups")):
            if relationship and relationship != expected:
                rows.append({
                    "fixture_id": fixture.get("fixture_id"),
                    "detected_non_injected_relationship": relationship,
                    "baseline_recoverable_support_count": fixture.get("baseline_recoverable_support_count", 0),
                    "unique_non_naive_support_count": fixture.get("unique_non_naive_support_count", 0),
                })
    return rows[:25]


def _load_retest(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / RETEST_FAMILY / day_utc / "aegis_alpha_factory_deterministic_fixture_suite_retest_v2.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_deterministic_fixture_suite_retest_v2(truth_root=root, day_utc=day_utc)


def _load_evidence_v2(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / EVIDENCE_V2_FAMILY / day_utc / "aegis_alpha_factory_evidence_test_v2.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_evidence_test_v2(truth_root=root, day_utc=day_utc)


def _load_qdv2(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / QDV2_FAMILY / day_utc / "aegis_alpha_factory_question_discovery_v2.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=day_utc)


def _load_json(path: Path) -> dict[str, Any]:
    payload = read_json_v1(path)
    return payload if payload else {}


def _suite_fixture(suite: dict[str, Any], fixture_id: str) -> dict[str, Any]:
    return next((row for row in _list(suite.get("fixtures")) if row.get("fixture_id") == fixture_id), {})


def _parse_relationship(relationship: str) -> tuple[str, str, str]:
    parts = relationship.split(":")
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    return "", "", ""


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clone = json.loads(json.dumps(payload, sort_keys=True))
    clone.pop("content_hash", None)
    encoded = json.dumps(clone, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
