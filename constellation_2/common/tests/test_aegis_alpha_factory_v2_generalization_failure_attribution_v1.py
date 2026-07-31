from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_naive_correlation_baseline_v1 import (
    build_alpha_factory_naive_correlation_baseline_v1,
    write_alpha_factory_naive_correlation_baseline_v1,
)
from ops.aegis.alpha_factory_non_naive_evidence_test_v1 import (
    build_alpha_factory_non_naive_evidence_test_v1,
    write_alpha_factory_non_naive_evidence_test_v1,
)
from ops.aegis.alpha_factory_poc_v1 import build_alpha_factory_poc_v1, write_alpha_factory_poc_v1
from ops.aegis.alpha_factory_question_discovery_v2 import (
    build_alpha_factory_question_discovery_v2,
    write_alpha_factory_question_discovery_v2,
)
from ops.aegis.alpha_factory_v2_generalization_failure_attribution_v1 import (
    build_alpha_factory_v2_generalization_failure_attribution_v1,
    write_alpha_factory_v2_generalization_failure_attribution_v1,
)
from ops.aegis.alpha_factory_v2_non_naive_rebenchmark_v1 import (
    build_alpha_factory_v2_non_naive_rebenchmark_v1,
    write_alpha_factory_v2_non_naive_rebenchmark_v1,
)
from ops.aegis.alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1 import (
    build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1,
    write_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1,
)


DAY = "2026-06-03"


def _seed_inputs(root: Path) -> None:
    poc = build_alpha_factory_poc_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_poc_v1(truth_root=root, day_utc=DAY, payload=poc)
    baseline = build_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY, payload=baseline)
    qdv2 = build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY)
    write_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY, payload=qdv2)
    non_naive = build_alpha_factory_non_naive_evidence_test_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_non_naive_evidence_test_v1(truth_root=root, day_utc=DAY, payload=non_naive)
    in_rebenchmark = build_alpha_factory_v2_non_naive_rebenchmark_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_v2_non_naive_rebenchmark_v1(truth_root=root, day_utc=DAY, payload=in_rebenchmark)
    out_rebenchmark = build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1(
        truth_root=root,
        day_utc=DAY,
        payload=out_rebenchmark,
    )


def test_generalization_failure_attribution_artifact_generation(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_v2_generalization_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_v2_generalization_failure_attribution"
    assert payload["comparison"]["support_counts"]["in_fixture"]["unique_support_count"] >= 0
    assert payload["comparison"]["support_counts"]["out_of_fixture"]["unique_support_count"] >= 0
    paths = write_alpha_factory_v2_generalization_failure_attribution_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        payload=payload,
    )
    assert Path(paths["json"]).exists()


def test_generalization_failure_attribution_is_deterministic(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    first = build_alpha_factory_v2_generalization_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_v2_generalization_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_generalization_failure_attribution_required_verdicts(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_v2_generalization_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]

    assert verdicts["execution"] == "ATTRIBUTION_EXECUTION_VALID"
    assert verdicts["primary_generalization_failure_mode"] in {
        "FIXTURE_SPECIFIC_RELATIONSHIP",
        "WEAK_QUESTION_GENERALIZATION",
        "NON_NAIVE_TEST_OVERFIT",
        "CANDIDATE_GROUPING_OVERFIT",
        "INSUFFICIENT_FIXTURE_DIVERSITY",
        "BASELINE_REDUNDANCY",
        "INCONCLUSIVE",
    }
    assert verdicts["fault_class"] in {
        "QUESTION_GENERATION_FAULT",
        "EVIDENCE_TEST_FAULT",
        "CANDIDATE_GROUPING_FAULT",
        "FIXTURE_FAULT",
        "INCONCLUSIVE",
    }
    assert verdicts["recommendation"] in {
        "V3_RECOMMENDED",
        "EVIDENCE_TEST_V2_RECOMMENDED",
        "CANDIDATE_GROUPING_V2_RECOMMENDED",
        "FIXTURE_SUITE_RECOMMENDED",
        "STOP_AND_REDESIGN",
    }
    assert verdicts["minimum_next_action"]


def test_generalization_failure_attribution_hostile_checks(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_v2_generalization_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert "in_fixture_unique_support_came_from_one_relationship_only" in checks
    assert "out_of_fixture_lacked_same_relationship" in checks
    assert "evidence_tests_produced_support_but_no_unique_support_out_of_fixture" in checks
    assert "all_out_of_fixture_support_baseline_recoverable" in checks
    assert "candidate_grouping_required_in_fixture_specific_structure" in checks
    assert checks["lineage_complete"] is True
    assert checks["deterministic_replay"] is True


def test_generalization_failure_attribution_blocks_v3_until_primary_mode(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_v2_generalization_failure_attribution_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["constraints"]["v3_built"] is False
    assert payload["verdicts"]["primary_generalization_failure_mode"] != "INCONCLUSIVE"
    if payload["verdicts"]["recommendation"] != "V3_RECOMMENDED":
        assert "Do not build v3 yet" in payload["verdicts"]["minimum_next_action"]
