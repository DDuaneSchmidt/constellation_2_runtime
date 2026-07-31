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
    TEST_NAMES,
    build_alpha_factory_non_naive_evidence_test_v1,
    write_alpha_factory_non_naive_evidence_test_v1,
)
from ops.aegis.alpha_factory_poc_v1 import build_alpha_factory_poc_v1, write_alpha_factory_poc_v1
from ops.aegis.alpha_factory_question_discovery_v2 import (
    build_alpha_factory_question_discovery_v2,
    write_alpha_factory_question_discovery_v2,
)


DAY = "2026-06-03"


def _seed_inputs(root: Path) -> None:
    poc = build_alpha_factory_poc_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_poc_v1(truth_root=root, day_utc=DAY, payload=poc)
    baseline = build_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY, payload=baseline)
    v2 = build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY)
    write_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY, payload=v2)


def test_non_naive_evidence_artifact_generation(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_non_naive_evidence_test_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_non_naive_evidence_test"
    assert payload["question_evidence"]
    paths = write_alpha_factory_non_naive_evidence_test_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_non_naive_evidence_output_is_deterministic(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    first = build_alpha_factory_non_naive_evidence_test_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_non_naive_evidence_test_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_non_naive_evidence_required_verdict_fields(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_non_naive_evidence_test_v1(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]

    assert verdicts["execution"] == "NON_NAIVE_EVIDENCE_EXECUTION_VALID"
    assert verdicts["unique_support"] in {"UNIQUE_SUPPORT_FOUND", "NO_UNIQUE_SUPPORT", "INCONCLUSIVE"}
    assert verdicts["question_productivity"] in {"QUESTION_PRODUCTIVITY_IMPROVED", "NOT_IMPROVED", "INCONCLUSIVE"}
    assert verdicts["rebenchmark_readiness"] in {"READY_FOR_V2_REBENCHMARK_WITH_NON_NAIVE_EVIDENCE", "NOT_READY"}
    assert verdicts["minimum_next_action"]


def test_non_naive_evidence_runs_required_tests(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_non_naive_evidence_test_v1(truth_root=tmp_path, day_utc=DAY)

    for row in payload["question_evidence"]:
        assert set(TEST_NAMES[:3]).issubset(set(row["tests_run"]))
        for supported in row["supported_tests"]:
            assert supported["test_name"] in TEST_NAMES
            assert supported["sample_count"] > 0
            assert supported["non_naive_measure"] is True
        assert row["lineage"]


def test_non_naive_evidence_hostile_checks(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_non_naive_evidence_test_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert checks["evidence_reduces_to_pairwise_forward_return"] is False
    assert checks["support_claims_identify_non_naive_test"] is True
    assert checks["zero_sample_tests_support_claims"] is False
    assert checks["baseline_recoverable_evidence_marked"] is True
