from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_feature_surface_repair_v1 import (
    REQUIRED_FEATURE_TYPES,
    build_alpha_factory_feature_surface_repair_v1,
    write_alpha_factory_feature_surface_repair_v1,
)


DAY = "2026-06-03"


def test_feature_surface_repair_artifact_generation(tmp_path: Path) -> None:
    payload = build_alpha_factory_feature_surface_repair_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["schema_id"] == "aegis_alpha_factory_feature_surface_repair"
    assert payload["fixture_feature_surfaces"]
    paths = write_alpha_factory_feature_surface_repair_v1(truth_root=tmp_path, day_utc=DAY, payload=payload)
    assert Path(paths["json"]).exists()


def test_feature_surface_repair_is_deterministic(tmp_path: Path) -> None:
    first = build_alpha_factory_feature_surface_repair_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_feature_surface_repair_v1(truth_root=tmp_path, day_utc=DAY)

    assert first["content_hash"] == second["content_hash"]


def test_feature_surface_repair_required_verdicts(tmp_path: Path) -> None:
    verdicts = build_alpha_factory_feature_surface_repair_v1(truth_root=tmp_path, day_utc=DAY)["verdicts"]

    assert verdicts["execution"] == "FEATURE_SURFACE_REPAIR_EXECUTION_VALID"
    assert verdicts["spy_vix_features"] in {"SPY_VIX_FEATURES_GENERATED", "NOT_GENERATED"}
    assert verdicts["relationship_feature_lineage"] in {"RELATIONSHIP_FEATURE_LINEAGE_COMPLETE", "INCOMPLETE"}
    assert verdicts["injection_visibility"] in {"INJECTION_VISIBLE_TO_FEATURES", "NOT_VISIBLE", "INCONCLUSIVE"}
    assert verdicts["fixture_suite_retest_readiness"] in {"READY_FOR_FIXTURE_SUITE_RETEST_WITH_REPAIRED_FEATURES", "NOT_READY"}
    assert verdicts["minimum_next_action"]


def test_spy_vix_features_generated_in_injected_fixture(tmp_path: Path) -> None:
    payload = build_alpha_factory_feature_surface_repair_v1(truth_root=tmp_path, day_utc=DAY)
    fixture = next(row for row in payload["fixture_feature_surfaces"] if row["fixture_id"] == "SYNTHETIC_INJECTED_SPY_VIX_INSTABILITY")
    spy_vix = [row for row in fixture["relationship_features"] if row["relationship_key"] == "SPY:VIX"]

    assert spy_vix
    assert REQUIRED_FEATURE_TYPES.issubset({row["feature_type"] for row in spy_vix})
    assert fixture["spy_vix_feature_summary"]["lineage_complete"] is True


def test_feature_surface_repair_hostile_checks(tmp_path: Path) -> None:
    payload = build_alpha_factory_feature_surface_repair_v1(truth_root=tmp_path, day_utc=DAY)
    checks = payload["hostile_checks"]

    assert checks["spy_vix_features_exist_in_injected_fixture"] is True
    assert checks["no_zero_sample_relationship_feature_artifacts"] is True
    assert checks["features_are_relationship_specific_not_broad_proxies"] is True
    assert checks["known_at_rule_exists"] is True
    assert checks["lineage_complete"] is True
    assert checks["deterministic_replay"] is True


def test_feature_surface_lineage_contract(tmp_path: Path) -> None:
    payload = build_alpha_factory_feature_surface_repair_v1(truth_root=tmp_path, day_utc=DAY)
    features = [feature for fixture in payload["fixture_feature_surfaces"] for feature in fixture["relationship_features"][:25]]

    assert features
    for feature in features:
        assert feature["source_observations"]
        assert feature["source_series"] == [feature["asset"], feature["related_asset"]]
        assert feature["calculation_version"] == "alpha_factory_feature_surface_repair.v1"
        assert feature["known_at_rule"]
        assert feature["lineage"]
        assert feature["sample_count"] > 0
