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
from ops.aegis.alpha_factory_poc_v1 import build_alpha_factory_poc_v1, write_alpha_factory_poc_v1
from ops.aegis.alpha_factory_question_discovery_v2 import (
    build_alpha_factory_question_discovery_v2,
    write_alpha_factory_question_discovery_v2,
)
from ops.aegis.alpha_factory_question_discovery_v2_rebenchmark_v1 import (
    build_alpha_factory_question_discovery_v2_rebenchmark_v1,
    write_alpha_factory_question_discovery_v2_rebenchmark_v1,
)
from ops.aegis.alpha_factory_question_productivity_diagnostic_v1 import (
    build_alpha_factory_question_productivity_diagnostic_v1,
    write_alpha_factory_question_productivity_diagnostic_v1,
)


DAY = "2026-06-03"


def _seed_inputs(root: Path) -> None:
    poc = build_alpha_factory_poc_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_poc_v1(truth_root=root, day_utc=DAY, payload=poc)
    baseline = build_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY, payload=baseline)
    v2 = build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY)
    write_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY, payload=v2)
    rebenchmark = build_alpha_factory_question_discovery_v2_rebenchmark_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_question_discovery_v2_rebenchmark_v1(truth_root=root, day_utc=DAY, payload=rebenchmark)


def test_question_productivity_diagnostic_artifact_generation(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_question_productivity_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_question_productivity_diagnostic"
    assert payload["question_diagnostics"]
    assert payload["trigger_type_aggregates"]
    paths = write_alpha_factory_question_productivity_diagnostic_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_question_productivity_diagnostic_output_is_deterministic(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    first = build_alpha_factory_question_productivity_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_question_productivity_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_question_productivity_diagnostic_required_verdict_fields(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_question_productivity_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]

    assert verdicts["execution"] == "DIAGNOSTIC_EXECUTION_VALID"
    assert verdicts["question_productivity"] in {
        "QUESTION_PRODUCTIVITY_SUFFICIENT",
        "QUESTION_PRODUCTIVITY_INSUFFICIENT",
    }
    assert verdicts["primary_failure_mode"]
    assert verdicts["v3_decision"] in {"V3_REQUIRED", "DO_NOT_BUILD_V3_YET"}
    assert verdicts["minimum_next_action"]


def test_question_productivity_diagnostic_per_question_fields(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_question_productivity_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)
    allowed = {
        "PRODUCTIVE_UNIQUE",
        "PRODUCTIVE_BASELINE_RECOVERABLE",
        "UNPRODUCTIVE_NO_EVIDENCE",
        "UNPRODUCTIVE_WEAK_EVIDENCE",
        "REDUNDANT_WITH_BASELINE",
    }

    for row in payload["question_diagnostics"]:
        assert row["trigger_type"]
        assert row["source_feature_refs"]
        assert row["downstream_hypothesis_count"] >= 0
        assert row["downstream_evidence_count"] >= 0
        assert row["classification"] in allowed


def test_question_productivity_diagnostic_hostile_checks(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_question_productivity_diagnostic_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert isinstance(checks["questions_that_generate_no_evidence"], list)
    assert isinstance(checks["questions_whose_evidence_is_fully_baseline_recoverable"], list)
    assert checks["trigger_types_with_zero_unique_productivity"]
    assert isinstance(checks["candidate_groups_unsupported_by_evidence"], list)
    assert checks["evidence_tests_too_similar_to_naive_baseline"] is True
