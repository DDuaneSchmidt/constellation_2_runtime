from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_deterministic_fixture_suite_v1 import (
    build_alpha_factory_deterministic_fixture_suite_v1,
    write_alpha_factory_deterministic_fixture_suite_v1,
)
from ops.aegis.alpha_factory_evidence_test_failure_attribution_v1 import (
    build_alpha_factory_evidence_test_failure_attribution_v1,
    write_alpha_factory_evidence_test_failure_attribution_v1,
)


DAY = "2026-06-03"


def _seed_fixture_suite(root: Path) -> None:
    suite = build_alpha_factory_deterministic_fixture_suite_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_deterministic_fixture_suite_v1(truth_root=root, day_utc=DAY, payload=suite)


def test_evidence_test_failure_attribution_artifact_generation(tmp_path: Path) -> None:
    _seed_fixture_suite(tmp_path)
    payload = build_alpha_factory_evidence_test_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_evidence_test_failure_attribution"
    assert payload["fixture_inspections"]
    assert payload["expected_vs_detected"]
    paths = write_alpha_factory_evidence_test_failure_attribution_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        payload=payload,
    )
    assert Path(paths["json"]).exists()


def test_evidence_test_failure_attribution_is_deterministic(tmp_path: Path) -> None:
    _seed_fixture_suite(tmp_path)
    first = build_alpha_factory_evidence_test_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_evidence_test_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_evidence_test_failure_attribution_required_verdicts(tmp_path: Path) -> None:
    _seed_fixture_suite(tmp_path)
    payload = build_alpha_factory_evidence_test_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]

    assert verdicts["execution"] == "EVIDENCE_FAILURE_ATTRIBUTION_EXECUTION_VALID"
    allowed_modes = {
        "TEST_TOO_BROAD",
        "THRESHOLD_TOO_LOOSE",
        "GROUPING_TOO_PERMISSIVE",
        "INJECTION_NOT_VISIBLE_TO_FEATURES",
        "FIXTURE_LABELING_ERROR",
        "BASELINE_DOMINANCE",
        "INCONCLUSIVE",
    }
    assert verdicts["primary_evidence_failure_mode"] in allowed_modes
    assert verdicts["qqq_false_positive_cause"] in allowed_modes
    assert verdicts["spy_vix_miss_cause"] in allowed_modes
    assert verdicts["recommendation"] in {
        "EVIDENCE_TEST_V2_REQUIRED",
        "FIXTURE_REPAIR_REQUIRED",
        "GROUPING_REPAIR_REQUIRED",
        "STOP_ALPHA_FACTORY_POC",
    }
    assert verdicts["minimum_next_action"]


def test_evidence_test_failure_attribution_hostile_checks(tmp_path: Path) -> None:
    _seed_fixture_suite(tmp_path)
    payload = build_alpha_factory_evidence_test_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert "false_positive_appears_in_negative_control" in checks
    assert "injected_structure_missed" in checks
    assert "support_mostly_baseline_recoverable" in checks
    assert "rac_created_from_weak_or_non_unique_support" in checks
    assert "evidence_test_lacks_specificity" in checks
    assert checks["deterministic_replay"] is True


def test_evidence_test_failure_attribution_explanations(tmp_path: Path) -> None:
    _seed_fixture_suite(tmp_path)
    payload = build_alpha_factory_evidence_test_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)

    explanations = payload["explanations"]
    assert explanations["why_qqq_real_yield_appears_in_no_qqq_control"]
    assert explanations["why_spy_vix_injected_structure_was_not_detected"]
    assert explanations["candidate_grouping_creates_false_racs_from_weak_support"]
    assert explanations["non_naive_tests_collapse_back_into_naive_correlation_behavior"]
    assert payload["constraints"]["evidence_tests_repaired"] is False
