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
from ops.aegis.alpha_factory_deterministic_fixture_suite_retest_v2 import (
    build_alpha_factory_deterministic_fixture_suite_retest_v2,
    write_alpha_factory_deterministic_fixture_suite_retest_v2,
)
from ops.aegis.alpha_factory_evidence_test_v2 import build_alpha_factory_evidence_test_v2, write_alpha_factory_evidence_test_v2
from ops.aegis.alpha_factory_question_discovery_v2 import (
    build_alpha_factory_question_discovery_v2,
    write_alpha_factory_question_discovery_v2,
)


DAY = "2026-06-03"


def _seed_inputs(root: Path) -> None:
    qdv2 = build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY)
    write_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY, payload=qdv2)
    suite = build_alpha_factory_deterministic_fixture_suite_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_deterministic_fixture_suite_v1(truth_root=root, day_utc=DAY, payload=suite)
    evidence = build_alpha_factory_evidence_test_v2(truth_root=root, day_utc=DAY)
    write_alpha_factory_evidence_test_v2(truth_root=root, day_utc=DAY, payload=evidence)


def test_fixture_suite_retest_v2_artifact_generation(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_deterministic_fixture_suite_retest_v2(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_deterministic_fixture_suite_retest_v2"
    assert payload["fixture_retests"]
    paths = write_alpha_factory_deterministic_fixture_suite_retest_v2(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_fixture_suite_retest_v2_is_deterministic(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    first = build_alpha_factory_deterministic_fixture_suite_retest_v2(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_deterministic_fixture_suite_retest_v2(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_fixture_suite_retest_v2_required_verdicts(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    verdicts = build_alpha_factory_deterministic_fixture_suite_retest_v2(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["execution"] == "FIXTURE_SUITE_RETEST_V2_EXECUTION_VALID"
    assert verdicts["qqq_real_yield_repeatability"] in {
        "QQQ_REAL_YIELD_REPEATABLE",
        "QQQ_REAL_YIELD_FIXTURE_SPECIFIC",
        "QQQ_REAL_YIELD_BLOCKED_AS_FALSE_POSITIVE",
        "INCONCLUSIVE",
    }
    assert verdicts["injected_structure_detection"] in {"INJECTED_STRUCTURE_DETECTION_VALID", "INVALID", "INCONCLUSIVE"}
    assert verdicts["false_positive_control"] in {"FALSE_POSITIVE_CONTROL_ACCEPTABLE", "UNACCEPTABLE", "INCONCLUSIVE"}
    assert verdicts["discovery_generalization"] in {"DISCOVERY_GENERALIZATION_PRESENT", "ABSENT", "INCONCLUSIVE"}
    assert verdicts["evidence_test_v2_improvement"] in {"EVIDENCE_TEST_V2_IMPROVES_OVER_V1", "DOES_NOT_IMPROVE", "INCONCLUSIVE"}
    assert verdicts["minimum_next_action"]


def test_fixture_suite_retest_v2_hostile_checks(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_deterministic_fixture_suite_retest_v2(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert checks["qqq_real_yield_does_not_qualify_in_no_qqq_control"] is True
    assert checks["spy_vix_injected_structure_detection_evaluated"] is True
    assert checks["support_mostly_baseline_recoverable_blocks_discovery_advantage"] is True
    assert checks["rac_qualification_requires_unique_evidence_test_v2_support"] is True
    assert checks["deterministic_replay"] is True
    assert checks["lineage_complete"] is True


def test_fixture_suite_retest_v2_excludes_invalid_support(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_deterministic_fixture_suite_retest_v2(truth_root=tmp_path, day_utc=DAY)
    negative = next(row for row in payload["fixture_retests"] if row["fixture_id"] == "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY")
    spy = next(row for row in payload["fixture_retests"] if row["fixture_id"] == "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY")

    assert "QQQ:REAL_YIELD:RELATIONSHIP_INSTABILITY" not in negative["valid_rac_relationship_groups"]
    assert spy["spy_vix_tested"] is True
    assert all(group["research_asset_candidate_qualification"]["qualifies"] is False for group in payload["candidate_groups"])
