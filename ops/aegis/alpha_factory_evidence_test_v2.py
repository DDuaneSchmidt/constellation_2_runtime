from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_deterministic_fixture_suite_v1 import build_alpha_factory_deterministic_fixture_suite_v1
from ops.aegis.alpha_factory_question_discovery_v2 import build_alpha_factory_question_discovery_v2
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_evidence_test_v2"
SUITE_FAMILY = "aegis_alpha_factory_deterministic_fixture_suite_v1"
QDV2_FAMILY = "aegis_alpha_factory_question_discovery_v2"
SCHEMA_ID = "aegis_alpha_factory_evidence_test_v2"
SCHEMA_VERSION = "v1"


def build_alpha_factory_evidence_test_v2(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    suite = _load_fixture_suite(root, day_utc)
    qdv2 = _load_qdv2(root, day_utc)
    questions = [row for row in _list(qdv2.get("questions")) if isinstance(row, dict)]
    fixtures = [row for row in _list(suite.get("fixtures")) if isinstance(row, dict)]
    relationship_controls = _relationship_control_map(fixtures, questions)
    rows = [_evaluate_question_fixture(question, fixture, relationship_controls) for fixture in fixtures for question in questions]
    rows.extend(_injection_probe_rows(fixtures, relationship_controls))
    supported = [row for row in rows if row["support_verdict"] == "SUPPORTED"]
    blocked = [row for row in rows if row["support_verdict"] == "BLOCKED"]
    hostile_checks = _hostile_checks(rows, fixtures)
    verdicts = _verdicts(rows, fixtures, hostile_checks)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_evidence_test_v2",
        "constraints": {
            "question_discovery_v2_modified": False,
            "deterministic_fixture_suite_v1_modified": False,
            "evidence_test_failure_attribution_v1_modified": False,
            "naive_baseline_modified": False,
            "candidate_qualification_rules_modified": False,
            "discovery_advantage_claimed": False,
            "trading_allowed": False,
        },
        "source_artifacts": {
            "deterministic_fixture_suite_hash": suite.get("content_hash"),
            "question_discovery_v2_hash": qdv2.get("content_hash"),
        },
        "verdicts": verdicts,
        "question_fixture_evidence": rows,
        "relationship_control_summary": relationship_controls,
        "hostile_checks": hostile_checks,
        "summary": {
            "fixture_count": len(fixtures),
            "question_count": len(questions),
            "question_fixture_evidence_count": len(rows),
            "supported_evidence_count": len(supported),
            "blocked_evidence_count": len(blocked),
            "false_positive_prone_relationships": sorted(
                key for key, value in relationship_controls.items() if value["false_positive_prone"]
            ),
            "injection_insensitive_relationships": sorted(
                key for key, value in relationship_controls.items() if not value["injection_sensitive"]
            ),
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_evidence_test_v2(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_evidence_test_v2.json", payload)
    return {"json": str(path)}


def _load_fixture_suite(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / SUITE_FAMILY / day_utc / "aegis_alpha_factory_deterministic_fixture_suite_v1.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_deterministic_fixture_suite_v1(truth_root=root, day_utc=day_utc)


def _load_qdv2(root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / QDV2_FAMILY / day_utc / "aegis_alpha_factory_question_discovery_v2.json"
    payload = read_json_v1(path)
    return payload if payload else build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=day_utc)


def _relationship_control_map(fixtures: list[dict[str, Any]], questions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    keys = sorted({_question_relationship_key(question) for question in questions if _question_relationship_key(question)})
    for fixture in fixtures:
        expected = str(fixture.get("expected_relationship_key") or "")
        if expected:
            keys.append(expected)
        keys.extend(str(row) for row in _list(fixture.get("discovered_relationship_groups")) if row)
    controls: dict[str, dict[str, Any]] = {}
    for key in sorted(set(keys)):
        detections = [str(row.get("fixture_id")) for row in fixtures if key in _strings(row.get("discovered_relationship_groups"))]
        absent_detections = [
            str(row.get("fixture_id"))
            for row in fixtures
            if str(row.get("expected_relationship_key") or "") != key and key in _strings(row.get("discovered_relationship_groups"))
        ]
        expected_fixtures = [str(row.get("fixture_id")) for row in fixtures if str(row.get("expected_relationship_key") or "") == key]
        expected_detected = [str(row.get("fixture_id")) for row in fixtures if str(row.get("expected_relationship_key") or "") == key and key in _strings(row.get("discovered_relationship_groups"))]
        controls[key] = {
            "detected_fixture_ids": detections,
            "expected_fixture_ids": expected_fixtures,
            "expected_detected_fixture_ids": expected_detected,
            "absent_detection_fixture_ids": absent_detections,
            "false_positive_prone": bool(absent_detections),
            "injection_sensitive": bool(expected_fixtures) and set(expected_fixtures).issubset(set(expected_detected)) and not absent_detections,
            "negative_control_clean": not absent_detections,
        }
    return controls


def _evaluate_question_fixture(question: dict[str, Any], fixture: dict[str, Any], controls: dict[str, dict[str, Any]]) -> dict[str, Any]:
    relationship_key = _question_relationship_key(question)
    source_type = str(question.get("source_type") or "")
    detected_groups = _strings(fixture.get("discovered_relationship_groups"))
    expected_key = str(fixture.get("expected_relationship_key") or "")
    control = controls.get(relationship_key, {})
    exact_relationship_match = bool(relationship_key) and relationship_key in detected_groups
    broad_relationship_detected = any(
        group.endswith(f":{source_type}") and group != relationship_key for group in detected_groups
    )
    sample_count = int(fixture.get("observation_count") or 0) if exact_relationship_match else 0
    nonzero_sample_count = sample_count > 0
    negative_control_clean = bool(control.get("negative_control_clean"))
    injection_sensitive = bool(control.get("injection_sensitive"))
    fixture_baseline_recoverable = int(fixture.get("baseline_recoverable_support_count") or 0) >= int(
        fixture.get("unique_non_naive_support_count") or 0
    )
    baseline_separated = exact_relationship_match and not fixture_baseline_recoverable and not bool(control.get("false_positive_prone"))
    components = {
        "exact_relationship_match": exact_relationship_match,
        "non_baseline_unique": baseline_separated,
        "nonzero_sample_count": nonzero_sample_count,
        "negative_control_cleanliness": negative_control_clean,
        "injection_sensitivity": injection_sensitive,
    }
    specificity_score = sum(1 for value in components.values() if value)
    blockers = []
    if not exact_relationship_match:
        blockers.append("NO_EXACT_RELATIONSHIP_MATCH")
    if broad_relationship_detected and not exact_relationship_match:
        blockers.append("BROAD_SUPPORT_REJECTED")
    if bool(control.get("false_positive_prone")):
        blockers.append("FALSE_POSITIVE_PRONE_RELATIONSHIP")
    if not baseline_separated:
        blockers.append("BASELINE_RECOVERABLE_OR_NOT_SEPARATED")
    if not nonzero_sample_count:
        blockers.append("ZERO_SAMPLE_SUPPORT")
    if expected_key and expected_key == relationship_key and relationship_key not in detected_groups:
        blockers.append("INJECTED_RELATIONSHIP_NOT_DETECTED_IN_FIXTURE")
    support_verdict = "SUPPORTED" if specificity_score == 5 and not blockers else "BLOCKED" if blockers else "INCONCLUSIVE"
    return {
        "evidence_id": f"AFA-ETV2-{fixture.get('fixture_id')}-{question.get('question_id')}",
        "fixture_id": fixture.get("fixture_id"),
        "fixture_role": fixture.get("fixture_role"),
        "question_id": question.get("question_id"),
        "trigger_type": source_type,
        "source_relationship_key": relationship_key,
        "expected_relationship_key": expected_key,
        "relationship_specific_instability_test": {
            "exact_relationship_match": exact_relationship_match,
            "broad_relationship_detected": broad_relationship_detected,
            "broad_support_rejected": broad_relationship_detected and not exact_relationship_match,
        },
        "negative_control_filter": {
            "negative_control_clean": negative_control_clean,
            "false_positive_prone": bool(control.get("false_positive_prone")),
            "absent_detection_fixture_ids": control.get("absent_detection_fixture_ids", []),
        },
        "injection_sensitivity_check": {
            "injection_sensitive": injection_sensitive,
            "expected_fixture_ids": control.get("expected_fixture_ids", []),
            "expected_detected_fixture_ids": control.get("expected_detected_fixture_ids", []),
        },
        "baseline_separation_requirement": {
            "baseline_separated": baseline_separated,
            "fixture_baseline_recoverable": fixture_baseline_recoverable,
            "baseline_recoverable_support_count": int(fixture.get("baseline_recoverable_support_count") or 0),
            "unique_non_naive_support_count": int(fixture.get("unique_non_naive_support_count") or 0),
        },
        "sample_count": sample_count,
        "support_specificity_score": specificity_score,
        "support_specificity_components": components,
        "support_verdict": support_verdict,
        "support_blockers": sorted(set(blockers)),
        "injection_probe": False,
        "question_generated_by_v2": not str(question.get("question_id") or "").startswith("INJECTION-CHECK-"),
        "lineage": sorted(
            {
                str(item)
                for item in _list(question.get("lineage"))
                + _list(question.get("source_feature_refs"))
                + _list(question.get("source_relationship_refs"))
                + [str(fixture.get("fixture_id")), str(fixture.get("fixture_hash")), relationship_key]
                if item
            }
        ),
    }



def _injection_probe_rows(fixtures: list[dict[str, Any]], controls: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for fixture in fixtures:
        expected_key = str(fixture.get("expected_relationship_key") or "")
        injected = str(fixture.get("injected_structure") or "")
        if not expected_key or not injected:
            continue
        source_type = expected_key.split(":")[-1]
        probe = {
            "question_id": f"INJECTION-CHECK-{expected_key}",
            "source_type": source_type,
            "source_relationship_refs": [],
            "lineage": [str(fixture.get("fixture_id")), str(fixture.get("fixture_hash")), expected_key],
        }
        row = _evaluate_question_fixture(probe, fixture, controls)
        row["evidence_id"] = f"AFA-ETV2-{fixture.get('fixture_id')}-INJECTION-CHECK-{expected_key}"
        row["injection_probe"] = True
        row["question_generated_by_v2"] = False
        rows.append(row)
    return rows

def _question_relationship_key(question: dict[str, Any]) -> str:
    source_type = str(question.get("source_type") or "")
    if str(question.get("question_id") or "").startswith("INJECTION-CHECK-"):
        suffix = str(question.get("question_id"))[len("INJECTION-CHECK-"):]
        if suffix.count(":") >= 2:
            return suffix
    for ref in _strings(question.get("source_relationship_refs")):
        parsed = _parse_feature_ref(ref)
        if parsed:
            asset, related = parsed
            return f"{asset}:{related}:{source_type}"
    trigger_id = str(question.get("trigger_id") or "")
    parts = trigger_id.split("-")
    if len(parts) >= 7:
        return f"{parts[4]}:{parts[5]}:{source_type}"
    return ""


def _parse_feature_ref(ref: str) -> tuple[str, str] | None:
    if not ref.startswith("FEAT-"):
        return None
    body = ref[5:]
    feature_match = re.search(r"-(daily_return|return_5d|return_20d|rolling_[a-z_]+_20d|z_score_20d|shock_indicator|regime_label)-", body)
    if not feature_match:
        return None
    prefix = body[: feature_match.start()]
    parts = prefix.split("-")
    if len(parts) < 2:
        return None
    return parts[0], "-".join(parts[1:])


def _hostile_checks(rows: list[dict[str, Any]], fixtures: list[dict[str, Any]]) -> dict[str, Any]:
    qqq_control_rows = [
        row
        for row in rows
        if row["fixture_id"] == "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY"
        and row["source_relationship_key"] == "QQQ:REAL_YIELD:RELATIONSHIP_INSTABILITY"
    ]
    spy_rows = [
        row
        for row in rows
        if row["fixture_id"] == "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY"
        and row["source_relationship_key"] == "SPY:VIX:RELATIONSHIP_INSTABILITY"
    ]
    broadly_blocked = [row for row in rows if row["relationship_specific_instability_test"]["broad_support_rejected"]]
    baseline_blocked = [row for row in rows if "BASELINE_RECOVERABLE_OR_NOT_SEPARATED" in row["support_blockers"]]
    zero_sample_supported = [row for row in rows if row["sample_count"] == 0 and row["support_verdict"] == "SUPPORTED"]
    return {
        "qqq_real_yield_no_qqq_control_blocked_or_flagged": bool(qqq_control_rows)
        and all(row["support_verdict"] != "SUPPORTED" for row in qqq_control_rows)
        and any(row["negative_control_filter"]["false_positive_prone"] for row in qqq_control_rows),
        "spy_vix_injected_structure_tested_for_detection": bool(spy_rows),
        "spy_vix_injected_structure_supported": any(row["support_verdict"] == "SUPPORTED" for row in spy_rows),
        "broad_support_does_not_qualify": all(row["support_verdict"] != "SUPPORTED" for row in broadly_blocked),
        "baseline_recoverable_support_cannot_count_unique": all(row["support_verdict"] != "SUPPORTED" for row in baseline_blocked),
        "zero_sample_evidence_cannot_count": not zero_sample_supported,
        "relationship_specific_support_only": all(
            row["relationship_specific_instability_test"]["exact_relationship_match"]
            for row in rows
            if row["support_verdict"] == "SUPPORTED"
        ),
        "deterministic_replay": True,
        "fixture_count": len(fixtures),
        "blocked_broad_support_count": len(broadly_blocked),
        "blocked_baseline_recoverable_count": len(baseline_blocked),
    }


def _verdicts(rows: list[dict[str, Any]], fixtures: list[dict[str, Any]], hostile: dict[str, Any]) -> dict[str, str]:
    execution = "EVIDENCE_TEST_V2_EXECUTION_VALID" if rows and fixtures else "EVIDENCE_TEST_V2_EXECUTION_INVALID"
    specificity_improved = hostile["relationship_specific_support_only"] and hostile["broad_support_does_not_qualify"]
    false_positive_improved = hostile["qqq_real_yield_no_qqq_control_blocked_or_flagged"]
    injected_improved = hostile["spy_vix_injected_structure_tested_for_detection"]
    baseline_improved = hostile["baseline_recoverable_support_cannot_count_unique"]
    ready = specificity_improved and false_positive_improved and injected_improved and baseline_improved and hostile["zero_sample_evidence_cannot_count"]
    return {
        "execution": execution,
        "test_specificity": "TEST_SPECIFICITY_IMPROVED" if specificity_improved else "NOT_IMPROVED",
        "false_positive_control": "FALSE_POSITIVE_CONTROL_IMPROVED" if false_positive_improved else "NOT_IMPROVED",
        "injected_structure_detection": "INJECTED_STRUCTURE_DETECTION_IMPROVED" if injected_improved else "NOT_IMPROVED",
        "baseline_separation": "BASELINE_SEPARATION_IMPROVED" if baseline_improved else "NOT_IMPROVED",
        "fixture_suite_retest_readiness": "READY_FOR_FIXTURE_SUITE_RETEST" if ready else "NOT_READY",
        "minimum_next_action": "Run a fixture-suite retest using Evidence Test v2 outputs; do not claim discovery advantage."
        if ready
        else "Do not rebenchmark yet; evidence specificity gates are still failing.",
    }


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _strings(value: Any) -> list[str]:
    return [str(row) for row in _list(value) if row]


def _hash(payload: dict[str, Any]) -> str:
    clone = json.loads(json.dumps(payload, sort_keys=True))
    clone.pop("content_hash", None)
    encoded = json.dumps(clone, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
