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
from ops.aegis.alpha_factory_evidence_test_v2 import (
    build_alpha_factory_evidence_test_v2,
    write_alpha_factory_evidence_test_v2,
)
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


def test_evidence_test_v2_artifact_generation(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_evidence_test_v2(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_evidence_test_v2"
    assert payload["question_fixture_evidence"]
    assert payload["relationship_control_summary"]
    paths = write_alpha_factory_evidence_test_v2(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_evidence_test_v2_is_deterministic(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    first = build_alpha_factory_evidence_test_v2(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_evidence_test_v2(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_evidence_test_v2_required_verdicts(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    verdicts = build_alpha_factory_evidence_test_v2(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["execution"] == "EVIDENCE_TEST_V2_EXECUTION_VALID"
    assert verdicts["test_specificity"] in {"TEST_SPECIFICITY_IMPROVED", "NOT_IMPROVED", "INCONCLUSIVE"}
    assert verdicts["false_positive_control"] in {"FALSE_POSITIVE_CONTROL_IMPROVED", "NOT_IMPROVED", "INCONCLUSIVE"}
    assert verdicts["injected_structure_detection"] in {"INJECTED_STRUCTURE_DETECTION_IMPROVED", "NOT_IMPROVED", "INCONCLUSIVE"}
    assert verdicts["baseline_separation"] in {"BASELINE_SEPARATION_IMPROVED", "NOT_IMPROVED", "INCONCLUSIVE"}
    assert verdicts["fixture_suite_retest_readiness"] in {"READY_FOR_FIXTURE_SUITE_RETEST", "NOT_READY"}
    assert verdicts["minimum_next_action"]


def test_evidence_test_v2_hostile_gates(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_evidence_test_v2(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert checks["qqq_real_yield_no_qqq_control_blocked_or_flagged"] is True
    assert checks["spy_vix_injected_structure_tested_for_detection"] is True
    assert checks["broad_support_does_not_qualify"] is True
    assert checks["baseline_recoverable_support_cannot_count_unique"] is True
    assert checks["zero_sample_evidence_cannot_count"] is True


def test_evidence_test_v2_support_specificity_and_blockers(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_evidence_test_v2(truth_root=tmp_path, day_utc=DAY)
    rows = payload["question_fixture_evidence"]

    assert all(0 <= row["support_specificity_score"] <= 5 for row in rows)
    assert all(row["sample_count"] > 0 for row in rows if row["support_verdict"] == "SUPPORTED")
    qqq_control = [
        row
        for row in rows
        if row["fixture_id"] == "SYNTHETIC_NO_QQQ_REAL_YIELD_INSTABILITY"
        and row["source_relationship_key"] == "QQQ:REAL_YIELD:RELATIONSHIP_INSTABILITY"
    ]
    assert qqq_control
    assert all("FALSE_POSITIVE_PRONE_RELATIONSHIP" in row["support_blockers"] for row in qqq_control)
    spy_injected = [
        row
        for row in rows
        if row["fixture_id"] == "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY"
        and row["source_relationship_key"] == "SPY:VIX:RELATIONSHIP_INSTABILITY"
    ]
    assert spy_injected
    assert any("INJECTED_RELATIONSHIP_NOT_DETECTED_IN_FIXTURE" in row["support_blockers"] for row in spy_injected)
