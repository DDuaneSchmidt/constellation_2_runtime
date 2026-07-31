from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_feature_surface_repair_v1 import build_alpha_factory_feature_surface_repair_v1, write_alpha_factory_feature_surface_repair_v1
from ops.aegis.alpha_factory_fixture_suite_retest_with_repaired_features_v1 import (
    build_alpha_factory_fixture_suite_retest_with_repaired_features_v1,
    write_alpha_factory_fixture_suite_retest_with_repaired_features_v1,
)


DAY = "2026-06-03"


def _seed_feature_surface(root: Path) -> None:
    payload = build_alpha_factory_feature_surface_repair_v1(truth_root=root, day_utc=DAY)
    write_alpha_factory_feature_surface_repair_v1(truth_root=root, day_utc=DAY, payload=payload)


def test_repaired_feature_retest_artifact_generation(tmp_path: Path) -> None:
    _seed_feature_surface(tmp_path)
    payload = build_alpha_factory_fixture_suite_retest_with_repaired_features_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_fixture_suite_retest_with_repaired_features"
    assert payload["fixture_retests"]
    paths = write_alpha_factory_fixture_suite_retest_with_repaired_features_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_repaired_feature_retest_is_deterministic(tmp_path: Path) -> None:
    _seed_feature_surface(tmp_path)
    first = build_alpha_factory_fixture_suite_retest_with_repaired_features_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_fixture_suite_retest_with_repaired_features_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_repaired_feature_retest_required_verdicts(tmp_path: Path) -> None:
    _seed_feature_surface(tmp_path)
    verdicts = build_alpha_factory_fixture_suite_retest_with_repaired_features_v1(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["execution"] == "REPAIRED_FEATURE_FIXTURE_RETEST_EXECUTION_VALID"
    assert verdicts["spy_vix_injected_structure_detection"] in {"SPY_VIX_INJECTED_STRUCTURE_DETECTED", "NOT_DETECTED", "INCONCLUSIVE"}
    assert verdicts["qqq_real_yield_false_positive"] in {"QQQ_REAL_YIELD_FALSE_POSITIVE_BLOCKED", "NOT_BLOCKED", "INCONCLUSIVE"}
    assert verdicts["false_positive_control"] in {"FALSE_POSITIVE_CONTROL_ACCEPTABLE", "UNACCEPTABLE", "INCONCLUSIVE"}
    assert verdicts["discovery_generalization"] in {"DISCOVERY_GENERALIZATION_PRESENT", "ABSENT", "INCONCLUSIVE"}
    assert verdicts["research_asset_candidate"] in {"RESEARCH_ASSET_CANDIDATE_VALID", "INVALID", "INCONCLUSIVE"}
    assert verdicts["minimum_next_action"]


def test_repaired_feature_retest_detects_spy_vix_only_where_injected(tmp_path: Path) -> None:
    _seed_feature_surface(tmp_path)
    payload = build_alpha_factory_fixture_suite_retest_with_repaired_features_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["hostile_checks"]["spy_vix_detected_only_where_injected"] is True
    detected = payload["summary"]["spy_vix_detected_fixture_ids"]
    assert detected == ["SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY"]


def test_repaired_feature_retest_hostile_checks(tmp_path: Path) -> None:
    _seed_feature_surface(tmp_path)
    checks = build_alpha_factory_fixture_suite_retest_with_repaired_features_v1(truth_root=tmp_path, day_utc=DAY)["hostile_checks"]

    assert checks["qqq_real_yield_does_not_qualify_in_no_qqq_control"] is True
    assert checks["no_rac_from_baseline_recoverable_only_support"] is True
    assert checks["no_zero_sample_support_counted"] is True
    assert checks["repaired_features_used_in_question_evidence_lineage"] is True
    assert checks["deterministic_replay"] is True
    assert checks["lineage_complete"] is True


def test_repaired_feature_retest_candidate_support_rules(tmp_path: Path) -> None:
    _seed_feature_surface(tmp_path)
    payload = build_alpha_factory_fixture_suite_retest_with_repaired_features_v1(truth_root=tmp_path, day_utc=DAY)
    spy_fixture = next(row for row in payload["fixture_retests"] if row["fixture_id"] == "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY")

    assert "SPY:VIX:RELATIONSHIP_INSTABILITY" in spy_fixture["valid_rac_relationship_groups"]
    supported = [row for row in spy_fixture["evidence_test_v2_repaired_feature_run"] if row["support_verdict"] == "SUPPORTED"]
    assert supported
    assert all(row["nonzero_support"] and row["relationship_specific_support"] and row["baseline_separated_support"] and row["negative_control_clean"] for row in supported)
    assert any(any(str(ref).startswith("AF-FSR-SPY-VIX-") for ref in row["lineage"]) for row in supported)
