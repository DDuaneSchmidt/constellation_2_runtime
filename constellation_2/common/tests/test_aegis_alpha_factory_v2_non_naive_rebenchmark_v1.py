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
from ops.aegis.alpha_factory_v2_non_naive_rebenchmark_v1 import (
    build_alpha_factory_v2_non_naive_rebenchmark_v1,
    write_alpha_factory_v2_non_naive_rebenchmark_v1,
)


DAY = "2026-06-03"


def _seed_inputs(root: Path) -> None:
    poc = build_alpha_factory_poc_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_poc_v1(truth_root=root, day_utc=DAY, payload=poc)
    baseline = build_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_naive_correlation_baseline_v1(truth_root=root, day_utc=DAY, payload=baseline)
    v2 = build_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY)
    write_alpha_factory_question_discovery_v2(truth_root=root, day_utc=DAY, payload=v2)
    non_naive = build_alpha_factory_non_naive_evidence_test_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_non_naive_evidence_test_v1(truth_root=root, day_utc=DAY, payload=non_naive)


def test_v2_non_naive_rebenchmark_artifact_generation(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_v2_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_v2_non_naive_rebenchmark"
    assert payload["evidence_artifacts"]
    assert payload["candidate_groups"]
    paths = write_alpha_factory_v2_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_v2_non_naive_rebenchmark_output_is_deterministic(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    first = build_alpha_factory_v2_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_v2_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_v2_non_naive_rebenchmark_required_verdict_fields(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_v2_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)
    verdicts = payload["verdicts"]

    assert verdicts["execution"] == "V2_NON_NAIVE_REBENCHMARK_EXECUTION_VALID"
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
    if payload["summary"]["qualified_research_asset_candidate_count"]:
        assert verdicts["research_asset_candidate"] == "RESEARCH_ASSET_CANDIDATE_VALID"


def test_v2_non_naive_rebenchmark_qualification_rules(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_v2_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)

    for group in payload["candidate_groups"]:
        qualification = group["research_asset_candidate_qualification"]
        if qualification["qualifies"]:
            assert qualification["unique_non_naive_evidence_count"] >= 2
            assert qualification["related_question_count"] >= 2
            assert qualification["related_hypothesis_evidence_path_count"] >= 2
            assert qualification["zero_sample_support_counted"] is False
            assert qualification["candidate_name_generic_or_supported"] is True
            assert qualification["explicit_limiting_evidence_captured"] is True
            assert group["lineage"]


def test_v2_non_naive_rebenchmark_hostile_checks(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    payload = build_alpha_factory_v2_non_naive_rebenchmark_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert checks["zero_sample_support_counted"] is False
    assert isinstance(checks["support_mostly_baseline_recoverable"], bool)
    assert isinstance(checks["candidate_overnaming_detected"], bool)
    assert checks["deterministic_replay"] is True
    assert payload["source_artifacts"]["question_discovery_v2_hash"]
    assert payload["source_artifacts"]["non_naive_evidence_test_v1_hash"]
    assert payload["source_artifacts"]["naive_correlation_baseline_v1_hash"]
