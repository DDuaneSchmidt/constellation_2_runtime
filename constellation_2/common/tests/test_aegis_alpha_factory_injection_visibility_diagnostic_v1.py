from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_deterministic_fixture_suite_v1 import build_alpha_factory_deterministic_fixture_suite_v1, write_alpha_factory_deterministic_fixture_suite_v1
from ops.aegis.alpha_factory_deterministic_fixture_suite_retest_v2 import build_alpha_factory_deterministic_fixture_suite_retest_v2, write_alpha_factory_deterministic_fixture_suite_retest_v2
from ops.aegis.alpha_factory_evidence_test_v2 import build_alpha_factory_evidence_test_v2, write_alpha_factory_evidence_test_v2
from ops.aegis.alpha_factory_injection_visibility_diagnostic_v1 import build_alpha_factory_injection_visibility_diagnostic_v1, write_alpha_factory_injection_visibility_diagnostic_v1
from ops.aegis.alpha_factory_question_discovery_v2 import build_alpha_factory_question_discovery_v2, write_alpha_factory_question_discovery_v2


DAY = "2026-06-03"


def _seed_inputs(root: Path) -> None:
    qdv2 = build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY)
    write_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY, payload=qdv2)
    suite = build_alpha_factory_deterministic_fixture_suite_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_deterministic_fixture_suite_v1(truth_root=root, day_utc=DAY, payload=suite)
    evidence = build_alpha_factory_evidence_test_v2(truth_root=root, day_utc=DAY)
    write_alpha_factory_evidence_test_v2(truth_root=root, day_utc=DAY, payload=evidence)
    retest = build_alpha_factory_deterministic_fixture_suite_retest_v2(truth_root=root, day_utc=DAY)
    write_alpha_factory_deterministic_fixture_suite_retest_v2(truth_root=root, day_utc=DAY, payload=retest)


def test_injection_visibility_artifact_generation(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_injection_visibility_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_injection_visibility_diagnostic"
    assert payload["injected_fixture_inspections"]
    paths = write_alpha_factory_injection_visibility_diagnostic_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_injection_visibility_is_deterministic(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    first = build_alpha_factory_injection_visibility_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_injection_visibility_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_injection_visibility_required_verdicts(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    verdicts = build_alpha_factory_injection_visibility_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["execution"] == "INJECTION_VISIBILITY_DIAGNOSTIC_EXECUTION_VALID"
    assert verdicts["primary_injection_miss_cause"] in {
        "INJECTION_NOT_EXPRESSED_IN_FEATURES",
        "QUESTION_NOT_GENERATED_FOR_INJECTION",
        "EVIDENCE_TEST_NOT_TARGETING_INJECTION",
        "THRESHOLD_TOO_STRICT",
        "SAMPLE_COUNT_TOO_LOW",
        "BASELINE_FILTER_TOO_STRICT",
        "FIXTURE_INJECTION_TOO_WEAK",
        "INCONCLUSIVE",
    }
    assert verdicts["recommendation"] in {
        "FIXTURE_REPAIR_REQUIRED",
        "FEATURE_REPAIR_REQUIRED",
        "EVIDENCE_SENSITIVITY_REPAIR_REQUIRED",
        "QUESTION_TRIGGER_REPAIR_REQUIRED",
        "INCONCLUSIVE",
    }
    assert verdicts["evidence_test_v2_strictness"] in {"EVIDENCE_TEST_V2_TOO_STRICT", "NOT_TOO_STRICT", "INCONCLUSIVE"}
    assert verdicts["minimum_next_action"]


def test_injection_visibility_spy_vix_diagnosis(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_injection_visibility_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)
    spy = next(row for row in payload["injected_fixture_inspections"] if row["injected_relationship"] == "SPY:VIX:RELATIONSHIP_INSTABILITY")

    assert spy["affected_observations"]["count"] > 0
    assert spy["affected_features"]["relationship_feature_count"] == 0
    assert not spy["question_triggers_for_injected_relationship"]
    assert spy["evidence_test_v2_tests_run_for_injected_relationship"]
    assert not spy["generated_question_targeted_evidence_tests"]
    assert "INJECTION_NOT_EXPRESSED_IN_FEATURES" in spy["miss_causes"]
    assert "QUESTION_NOT_GENERATED_FOR_INJECTION" in spy["miss_causes"]
    assert "SAMPLE_COUNT_TOO_LOW" in spy["miss_causes"]


def test_injection_visibility_hostile_checks(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    checks = build_alpha_factory_injection_visibility_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)["hostile_checks"]

    assert checks["injected_relationship_lacks_generated_question"] is True
    assert "injected_relationship_has_question_but_no_targeted_evidence_test" in checks
    assert checks["targeted_evidence_test_fails_due_to_threshold"] is True
    assert checks["support_blocked_only_by_baseline_filter"] is False
    assert checks["fixture_injection_not_visible_in_features"] is True
    assert checks["insufficient_sample_count"] is True
