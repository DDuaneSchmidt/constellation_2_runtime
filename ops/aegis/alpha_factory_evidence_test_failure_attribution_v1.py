from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.alpha_factory_deterministic_fixture_suite_v1 import (
    build_alpha_factory_deterministic_fixture_suite_v1,
)
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_alpha_factory_evidence_test_failure_attribution_v1"
FIXTURE_SUITE_FAMILY = "aegis_alpha_factory_deterministic_fixture_suite_v1"
SCHEMA_ID = "aegis_alpha_factory_evidence_test_failure_attribution"
SCHEMA_VERSION = "v1"
QQQ_KEY = "QQQ:REAL_YIELD:RELATIONSHIP_INSTABILITY"
SPY_VIX_KEY = "SPY:VIX:RELATIONSHIP_INSTABILITY"


def build_alpha_factory_evidence_test_failure_attribution_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    fixture_suite = _load_or_build_fixture_suite(root, day_utc)
    fixture_rows = [row for row in _list(fixture_suite.get("fixtures")) if isinstance(row, dict)]
    fixture_inspections = [_inspect_fixture(row) for row in fixture_rows]
    hostile_checks = _hostile_checks(fixture_inspections, fixture_suite)
    failure_modes = _failure_modes(hostile_checks, fixture_inspections)
    primary = _primary_failure_mode(failure_modes)
    qqq_cause = _qqq_false_positive_cause(hostile_checks)
    spy_cause = _spy_vix_miss_cause(hostile_checks)
    recommendation = _recommendation(primary, hostile_checks)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "scope": "alpha_factory_evidence_test_failure_attribution_v1",
        "constraints": {
            "question_discovery_v2_modified": False,
            "deterministic_fixture_suite_v1_modified": False,
            "candidate_qualification_rules_modified": False,
            "naive_baseline_modified": False,
            "evidence_tests_repaired": False,
            "trading_allowed": False,
        },
        "source_artifacts": {
            "deterministic_fixture_suite_v1_hash": fixture_suite.get("content_hash", ""),
        },
        "verdicts": {
            "execution": "EVIDENCE_FAILURE_ATTRIBUTION_EXECUTION_VALID"
            if fixture_inspections
            else "EVIDENCE_FAILURE_ATTRIBUTION_EXECUTION_INVALID",
            "primary_evidence_failure_mode": primary,
            "qqq_false_positive_cause": qqq_cause,
            "spy_vix_miss_cause": spy_cause,
            "recommendation": recommendation,
            "minimum_next_action": _minimum_next_action(recommendation, primary),
        },
        "fixture_inspections": fixture_inspections,
        "expected_vs_detected": _expected_vs_detected(fixture_inspections),
        "failure_mode_evidence": failure_modes,
        "explanations": {
            "why_qqq_real_yield_appears_in_no_qqq_control": _explain_qqq_false_positive(hostile_checks),
            "why_spy_vix_injected_structure_was_not_detected": _explain_spy_miss(hostile_checks),
            "candidate_grouping_creates_false_racs_from_weak_support": _explain_grouping(hostile_checks),
            "non_naive_tests_collapse_back_into_naive_correlation_behavior": _explain_naive_collapse(hostile_checks),
        },
        "hostile_checks": hostile_checks,
        "summary": {
            "fixture_count": len(fixture_inspections),
            "false_positive_fixture_count": len([row for row in fixture_inspections if row["false_positive_structures"]]),
            "missed_injected_fixture_count": len(
                [row for row in fixture_inspections if row["injected_structure_definition"] and not row["injected_structure_detected"]]
            ),
            "primary_evidence_failure_mode": primary,
            "output_reproducible": True,
        },
    }
    payload["content_hash"] = _hash(payload)
    return payload


def write_alpha_factory_evidence_test_failure_attribution_v1(
    *,
    truth_root: Path,
    day_utc: str,
    payload: dict[str, Any],
) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "aegis_alpha_factory_evidence_test_failure_attribution_v1.json", payload)
    return {"json": str(path)}


def _inspect_fixture(row: dict[str, Any]) -> dict[str, Any]:
    detected = [str(item) for item in _list(row.get("discovered_relationship_groups"))]
    valid = [str(item) for item in _list(row.get("valid_rac_relationship_groups"))]
    support_count = int(row.get("unique_non_naive_support_count") or 0)
    baseline_count = int(row.get("baseline_recoverable_support_count") or 0)
    return {
        "fixture_id": row.get("fixture_id", ""),
        "fixture_role": row.get("fixture_role", ""),
        "injected_structure_definition": row.get("injected_structure", ""),
        "expected_relationship_key": row.get("expected_relationship_key", ""),
        "detected_relationship_groups": detected,
        "non_naive_test_support": {
            "unique_non_naive_support_count": support_count,
            "supporting_test_types": row.get("supporting_test_types", {}),
        },
        "baseline_recoverability": {
            "baseline_recoverable_support_count": baseline_count,
            "baseline_recoverable_dominates_unique_support": baseline_count >= support_count,
        },
        "valid_racs": valid,
        "valid_rac_count": int(row.get("valid_rac_count") or 0),
        "qqq_real_yield_detected": bool(row.get("qqq_real_yield_detected")),
        "injected_structure_detected": bool(row.get("injected_structure_detected")),
        "false_positive_structures": [str(item) for item in _list(row.get("false_positive_structures"))],
    }


def _hostile_checks(inspections: list[dict[str, Any]], fixture_suite: dict[str, Any]) -> dict[str, Any]:
    negative = _fixture(inspections, "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY")
    spy = _fixture(inspections, "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY")
    false_positive_fixtures = [row for row in inspections if row["false_positive_structures"]]
    valid_false_racs = [
        row for row in false_positive_fixtures if set(row["false_positive_structures"]).intersection(row["valid_racs"])
    ]
    baseline_dominant = [
        row for row in inspections if row["baseline_recoverability"]["baseline_recoverable_dominates_unique_support"]
    ]
    return {
        "false_positive_appears_in_negative_control": QQQ_KEY in negative.get("detected_relationship_groups", []),
        "negative_control_false_positive_valid_rac": QQQ_KEY in negative.get("valid_racs", []),
        "injected_structure_missed": SPY_VIX_KEY not in spy.get("detected_relationship_groups", []),
        "support_mostly_baseline_recoverable": len(baseline_dominant) == len(inspections),
        "baseline_dominant_fixture_ids": [row["fixture_id"] for row in baseline_dominant],
        "rac_created_from_weak_or_non_unique_support": bool(valid_false_racs)
        or any(row["valid_rac_count"] > 0 and row["non_naive_test_support"]["unique_non_naive_support_count"] < 2 for row in inspections),
        "false_positive_valid_rac_fixture_ids": [row["fixture_id"] for row in valid_false_racs],
        "evidence_test_lacks_specificity": bool(false_positive_fixtures)
        and SPY_VIX_KEY not in spy.get("detected_relationship_groups", []),
        "non_naive_tests_collapse_toward_naive_behavior": bool(
            fixture_suite.get("hostile_checks", {}).get("naive_baseline_explains_detected_structures")
        ),
        "deterministic_replay": bool(fixture_suite.get("summary", {}).get("output_reproducible")),
    }


def _failure_modes(hostile: dict[str, Any], inspections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    modes: list[dict[str, Any]] = []
    if hostile["evidence_test_lacks_specificity"]:
        modes.append(
            {
                "failure_mode": "TEST_TOO_BROAD",
                "evidence": "The same QQQ:REAL_YIELD structure appears in a negative control while SPY:VIX injection is missed.",
            }
        )
    if hostile["false_positive_appears_in_negative_control"]:
        modes.append(
            {
                "failure_mode": "THRESHOLD_TOO_LOOSE",
                "evidence": "A no-QQQ-instability control still generated QQQ:REAL_YIELD detection.",
            }
        )
    if hostile["rac_created_from_weak_or_non_unique_support"]:
        modes.append(
            {
                "failure_mode": "GROUPING_TOO_PERMISSIVE",
                "evidence": "A false-positive structure reached valid RAC grouping in at least one control fixture.",
            }
        )
    if hostile["injected_structure_missed"]:
        modes.append(
            {
                "failure_mode": "INJECTION_NOT_VISIBLE_TO_FEATURES",
                "evidence": "SPY:VIX injection did not surface as a detected relationship group under existing feature/test logic.",
            }
        )
    if hostile["support_mostly_baseline_recoverable"]:
        modes.append(
            {
                "failure_mode": "BASELINE_DOMINANCE",
                "evidence": "Baseline-recoverable support is greater than or equal to unique support in every fixture.",
            }
        )
    if not inspections:
        modes.append({"failure_mode": "INCONCLUSIVE", "evidence": "No fixture inspections were available."})
    return modes


def _primary_failure_mode(modes: list[dict[str, Any]]) -> str:
    order = ["TEST_TOO_BROAD", "GROUPING_TOO_PERMISSIVE", "BASELINE_DOMINANCE", "THRESHOLD_TOO_LOOSE", "INJECTION_NOT_VISIBLE_TO_FEATURES"]
    present = {str(row.get("failure_mode")) for row in modes}
    for mode in order:
        if mode in present:
            return mode
    return "INCONCLUSIVE"


def _qqq_false_positive_cause(hostile: dict[str, Any]) -> str:
    if hostile["negative_control_false_positive_valid_rac"]:
        return "GROUPING_TOO_PERMISSIVE"
    if hostile["false_positive_appears_in_negative_control"]:
        return "TEST_TOO_BROAD"
    return "INCONCLUSIVE"


def _spy_vix_miss_cause(hostile: dict[str, Any]) -> str:
    if hostile["injected_structure_missed"] and hostile["evidence_test_lacks_specificity"]:
        return "TEST_TOO_BROAD"
    if hostile["injected_structure_missed"]:
        return "INJECTION_NOT_VISIBLE_TO_FEATURES"
    return "INCONCLUSIVE"


def _recommendation(primary: str, hostile: dict[str, Any]) -> str:
    if primary in {"TEST_TOO_BROAD", "THRESHOLD_TOO_LOOSE", "BASELINE_DOMINANCE"}:
        return "EVIDENCE_TEST_V2_REQUIRED"
    if primary == "GROUPING_TOO_PERMISSIVE":
        return "GROUPING_REPAIR_REQUIRED"
    if primary == "INJECTION_NOT_VISIBLE_TO_FEATURES":
        return "FIXTURE_REPAIR_REQUIRED"
    if hostile["negative_control_false_positive_valid_rac"] and hostile["injected_structure_missed"]:
        return "STOP_ALPHA_FACTORY_POC"
    return "STOP_ALPHA_FACTORY_POC"


def _minimum_next_action(recommendation: str, primary: str) -> str:
    if recommendation == "EVIDENCE_TEST_V2_REQUIRED":
        return f"Do not repair yet in this step. Specify Evidence Test v2 to narrow non-naive tests and baseline-recoverability gates; primary mode is {primary}."
    if recommendation == "GROUPING_REPAIR_REQUIRED":
        return "Do not repair yet in this step. Specify grouping v2 so valid RACs cannot form from false-positive controls."
    if recommendation == "FIXTURE_REPAIR_REQUIRED":
        return "Do not repair yet in this step. Verify injected structures are visible to feature stores before changing evidence tests."
    return "Stop the Alpha Factory POC line and redesign the evidence/gating stack."


def _expected_vs_detected(inspections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in inspections:
        expected = str(row.get("expected_relationship_key") or "")
        detected = set(row["detected_relationship_groups"])
        rows.append(
            {
                "fixture_id": row["fixture_id"],
                "expected_relationship_key": expected,
                "expected_detected": bool(expected and expected in detected),
                "qqq_real_yield_detected": row["qqq_real_yield_detected"],
                "false_positive_structures": row["false_positive_structures"],
                "valid_racs": row["valid_racs"],
            }
        )
    return rows


def _explain_qqq_false_positive(hostile: dict[str, Any]) -> str:
    if hostile["negative_control_false_positive_valid_rac"]:
        return "The no-QQQ control still produced QQQ:REAL_YIELD as a valid RAC, which indicates grouping accepts broad relationship-instability support as structure-specific evidence."
    if hostile["false_positive_appears_in_negative_control"]:
        return "The no-QQQ control still produced QQQ:REAL_YIELD detection, so the evidence tests are too broad or thresholds too loose."
    return "No QQQ false positive was present in the negative control."


def _explain_spy_miss(hostile: dict[str, Any]) -> str:
    if hostile["injected_structure_missed"]:
        return "The SPY:VIX injected fixture did not produce the expected detected relationship group; existing non-naive tests did not distinguish the injected structure from broader fixture behavior."
    return "The SPY:VIX injected structure was detected."


def _explain_grouping(hostile: dict[str, Any]) -> str:
    if hostile["rac_created_from_weak_or_non_unique_support"]:
        return "Candidate grouping can create valid RACs from false-positive controls, so grouping is not sufficiently guarded by expected-structure or anti-control evidence."
    return "Candidate grouping did not create a false RAC from weak or non-unique support."


def _explain_naive_collapse(hostile: dict[str, Any]) -> str:
    if hostile["non_naive_tests_collapse_toward_naive_behavior"]:
        return "Detected structures are still explained by naive baseline behavior at the fixture-suite level, so non-naive tests are not sufficiently independent."
    return "The fixture suite did not show naive-baseline dominance."


def _load_or_build_fixture_suite(root: Path, day_utc: str) -> dict[str, Any]:
    payload = read_json_v1(
        root
        / "reports"
        / FIXTURE_SUITE_FAMILY
        / day_utc
        / "aegis_alpha_factory_deterministic_fixture_suite_v1.json"
    )
    return payload if isinstance(payload, dict) and payload else build_alpha_factory_deterministic_fixture_suite_v1(
        truth_root=root,
        day_utc=day_utc,
    )


def _fixture(rows: list[dict[str, Any]], fixture_id: str) -> dict[str, Any]:
    for row in rows:
        if row.get("fixture_id") == fixture_id:
            return row
    return {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any]) -> str:
    clean = dict(payload)
    clean.pop("content_hash", None)
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
