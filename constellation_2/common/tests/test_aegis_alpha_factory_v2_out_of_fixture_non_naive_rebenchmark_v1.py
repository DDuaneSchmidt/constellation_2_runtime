from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1 import (
    build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1,
    write_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1,
)


DAY = "2026-06-03"


def test_v2_out_of_fixture_non_naive_rebenchmark_artifact_generation(tmp_path: Path) -> None:
    payload = build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark"
    assert payload["alternate_fixture"]["fixture_id"] == "alpha_factory_alternate_deterministic_daily_history_v1"
    assert payload["source_artifacts"]["out_of_fixture_question_discovery_v2_hash"]
    assert payload["source_artifacts"]["out_of_fixture_non_naive_evidence_test_v1_hash"]
    assert payload["source_artifacts"]["out_of_fixture_naive_baseline_v1_hash"]
    paths = write_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        payload=payload,
    )
    assert Path(paths["json"]).exists()


def test_v2_out_of_fixture_non_naive_rebenchmark_is_deterministic(tmp_path: Path) -> None:
    first = build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_v2_out_of_fixture_non_naive_rebenchmark_required_verdicts(tmp_path: Path) -> None:
    payload = build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]

    assert verdicts["execution"] == "OUT_OF_FIXTURE_NON_NAIVE_EXECUTION_VALID"
    assert verdicts["generalization"] in {"V2_NON_NAIVE_GENERALIZES", "V2_NON_NAIVE_DOES_NOT_GENERALIZE", "INCONCLUSIVE"}
    assert verdicts["v2_non_naive_vs_baseline"] in {
        "V2_NON_NAIVE_OUTPERFORMS_BASELINE",
        "BASELINE_MATCHES_OR_EXCEEDS_V2_NON_NAIVE",
        "INCONCLUSIVE",
    }
    assert verdicts["discovery_advantage"] in {
        "DISCOVERY_ADVANTAGE_PRESENT",
        "DISCOVERY_ADVANTAGE_ABSENT",
        "DISCOVERY_ADVANTAGE_INCONCLUSIVE",
    }
    assert verdicts["research_asset_candidate"] in {
        "RESEARCH_ASSET_CANDIDATE_VALID",
        "RESEARCH_ASSET_CANDIDATE_INVALID",
        "RESEARCH_ASSET_CANDIDATE_INCONCLUSIVE",
    }
    assert verdicts["lineage"] in {"LINEAGE_COMPLETE", "LINEAGE_INCOMPLETE"}
    assert verdicts["minimum_next_action"]


def test_v2_out_of_fixture_non_naive_success_standard(tmp_path: Path) -> None:
    payload = build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)

    if payload["verdicts"]["generalization"] == "V2_NON_NAIVE_GENERALIZES":
        assert payload["verdicts"]["discovery_advantage"] == "DISCOVERY_ADVANTAGE_PRESENT"
        assert payload["verdicts"]["research_asset_candidate"] == "RESEARCH_ASSET_CANDIDATE_VALID"
        assert payload["summary"]["qualified_unique_nonzero_candidate_count"] >= 1
        assert payload["summary"]["naive_baseline_fully_explains_qualified_candidates"] is False


def test_v2_out_of_fixture_non_naive_hostile_checks(tmp_path: Path) -> None:
    payload = build_alpha_factory_v2_out_of_fixture_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert isinstance(checks["support_mostly_baseline_recoverable"], bool)
    assert "zero_sample_support_count" in checks
    assert checks["zero_sample_support_counted"] is False
    assert isinstance(checks["candidate_overnaming_detected"], bool)
    assert isinstance(checks["insufficient_related_question_breadth_group_ids"], list)
    assert isinstance(checks["insufficient_unique_non_naive_evidence_group_ids"], list)
    assert checks["deterministic_replay"] is True
    assert isinstance(checks["lineage_complete"], bool)
