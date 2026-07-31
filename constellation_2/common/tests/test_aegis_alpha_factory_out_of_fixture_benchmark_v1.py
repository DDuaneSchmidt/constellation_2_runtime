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
from ops.aegis.alpha_factory_out_of_fixture_benchmark_v1 import (
    build_alpha_factory_out_of_fixture_benchmark_v1,
    write_alpha_factory_out_of_fixture_benchmark_v1,
)
from ops.aegis.alpha_factory_poc_v1 import build_alpha_factory_poc_v1, write_alpha_factory_poc_v1


DAY = "2026-06-03"


def _seed_in_fixture_outputs(root: Path) -> None:
    poc = build_alpha_factory_poc_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_poc_v1(truth_root=root, day_utc=DAY, payload=poc)
    baseline = build_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY, payload=baseline)


def test_out_of_fixture_benchmark_artifact_generation(tmp_path: Path) -> None:
    _seed_in_fixture_outputs(tmp_path)
    payload = build_alpha_factory_out_of_fixture_benchmark_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_out_of_fixture_benchmark"
    assert payload["alternate_fixture"]["fixture_id"] == "alpha_factory_alternate_deterministic_daily_history_v1"
    assert payload["out_of_fixture_results"]["poc"]["summary"]["research_asset_candidate_count"] >= 1
    assert payload["out_of_fixture_results"]["naive_baseline"]["relationship_count"] > 0
    paths = write_alpha_factory_out_of_fixture_benchmark_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_out_of_fixture_benchmark_output_is_deterministic(tmp_path: Path) -> None:
    _seed_in_fixture_outputs(tmp_path)
    first = build_alpha_factory_out_of_fixture_benchmark_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_out_of_fixture_benchmark_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_out_of_fixture_benchmark_required_verdict_fields(tmp_path: Path) -> None:
    _seed_in_fixture_outputs(tmp_path)
    payload = build_alpha_factory_out_of_fixture_benchmark_v1(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]

    assert verdicts["out_of_fixture_execution"] == "OUT_OF_FIXTURE_EXECUTION_VALID"
    assert verdicts["poc_generalization"] in {"POC_GENERALIZES", "POC_DOES_NOT_GENERALIZE", "INCONCLUSIVE"}
    assert verdicts["poc_vs_baseline"] in {
        "BASELINE_MATCHES_OR_EXCEEDS_POC",
        "POC_OUTPERFORMS_BASELINE",
        "INCONCLUSIVE",
    }
    assert verdicts["discovery_advantage"] in {
        "DISCOVERY_ADVANTAGE_PRESENT",
        "DISCOVERY_ADVANTAGE_ABSENT",
        "DISCOVERY_ADVANTAGE_INCONCLUSIVE",
    }
    assert verdicts["candidate_naming"] in {"CANDIDATE_NAMING_VALID", "OVERNAMED", "INCONCLUSIVE"}
    assert verdicts["minimum_next_action"]


def test_out_of_fixture_benchmark_compares_poc_and_baseline_outputs(tmp_path: Path) -> None:
    _seed_in_fixture_outputs(tmp_path)
    payload = build_alpha_factory_out_of_fixture_benchmark_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["in_fixture_reference"]["poc"]["content_hash"]
    assert payload["in_fixture_reference"]["naive_baseline"]["top_cluster"]
    assert payload["out_of_fixture_results"]["poc"]["content_hash"]
    assert payload["out_of_fixture_results"]["naive_baseline"]["top_cluster"]
    assert payload["comparison"]["relationship_recovery"]["out_of_fixture_candidate_clusters"]
    checks = payload["hostile_checks"]
    assert checks["fixed_template_question_leakage"] is True
    assert checks["candidate_name_unsupported_by_evidence"] is True
    assert checks["zero_sample_feature_support"] is True
