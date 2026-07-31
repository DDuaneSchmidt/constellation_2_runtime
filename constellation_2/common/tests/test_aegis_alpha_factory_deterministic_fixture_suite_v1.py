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


DAY = "2026-06-03"


def test_fixture_suite_artifact_generation(tmp_path: Path) -> None:
    payload = build_alpha_factory_deterministic_fixture_suite_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_deterministic_fixture_suite"
    assert len(payload["fixtures"]) >= 5
    assert payload["detection_matrix"]
    paths = write_alpha_factory_deterministic_fixture_suite_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_fixture_suite_is_deterministic(tmp_path: Path) -> None:
    first = build_alpha_factory_deterministic_fixture_suite_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_deterministic_fixture_suite_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_fixture_suite_required_verdicts(tmp_path: Path) -> None:
    payload = build_alpha_factory_deterministic_fixture_suite_v1(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]

    assert verdicts["execution"] == "FIXTURE_SUITE_EXECUTION_VALID"
    assert verdicts["qqq_real_yield_repeatability"] in {
        "QQQ_REAL_YIELD_REPEATABLE",
        "QQQ_REAL_YIELD_FIXTURE_SPECIFIC",
        "INCONCLUSIVE",
    }
    assert verdicts["injected_structure_detection"] in {
        "INJECTED_STRUCTURE_DETECTION_VALID",
        "INJECTED_STRUCTURE_DETECTION_INVALID",
        "INCONCLUSIVE",
    }
    assert verdicts["false_positive_control"] in {
        "FALSE_POSITIVE_CONTROL_ACCEPTABLE",
        "FALSE_POSITIVE_CONTROL_UNACCEPTABLE",
        "INCONCLUSIVE",
    }
    assert verdicts["discovery_generalization"] in {
        "DISCOVERY_GENERALIZATION_PRESENT",
        "DISCOVERY_GENERALIZATION_ABSENT",
        "INCONCLUSIVE",
    }
    assert verdicts["minimum_next_action"]


def test_fixture_suite_records_required_fixture_fields(tmp_path: Path) -> None:
    payload = build_alpha_factory_deterministic_fixture_suite_v1(truth_root=tmp_path, day_utc=DAY)

    fixture_ids = {row["fixture_id"] for row in payload["fixtures"]}
    assert "ORIGINAL_FIXTURE" in fixture_ids
    assert "EXISTING_OUT_OF_FIXTURE_ALTERNATE" in fixture_ids
    assert "SYNTHETIC_INJECTED_QQQ_REAL_YIELD_INSTABILITY" in fixture_ids
    assert "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY" in fixture_ids
    assert "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY" in fixture_ids
    for row in payload["fixtures"]:
        assert "discovered_relationship_groups" in row
        assert "unique_non_naive_support_count" in row
        assert "baseline_recoverable_support_count" in row
        assert "valid_rac_count" in row
        assert "qqq_real_yield_detected" in row
        assert "injected_structure_detected" in row
        assert "false_positive_structures" in row


def test_fixture_suite_hostile_checks(tmp_path: Path) -> None:
    payload = build_alpha_factory_deterministic_fixture_suite_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert "qqq_real_yield_appears_only_in_original_fixture" in checks
    assert "injected_relationship_not_detected" in checks
    assert "non_injected_relationship_falsely_detected" in checks
    assert "naive_baseline_explains_detected_structures" in checks
    assert "rac_qualification_passes_only_on_seeded_or_injected_structures" in checks
    assert checks["deterministic_replay_across_fixture_suite"] is True
    assert payload["constraints"]["v3_built"] is False
