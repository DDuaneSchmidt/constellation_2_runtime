from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.alpha_factory_poc_v1 import build_alpha_factory_poc_v1, write_alpha_factory_poc_v1


DAY = "2026-06-03"


def test_alpha_factory_core_loop_produces_candidate_with_complete_lineage(tmp_path: Path) -> None:
    payload = build_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary"]["observation_count"] > 0
    assert payload["summary"]["feature_count"] > 0
    assert payload["summary"]["question_count"] >= 5
    assert payload["summary"]["hypothesis_count"] >= 15
    assert payload["summary"]["evidence_count"] >= 15
    assert payload["summary"]["research_asset_candidate_count"] >= 1
    candidate = payload["research_asset_candidates"][0]
    assert len(candidate["supporting_evidence"]) >= 3
    assert len(candidate["hypothesis_family"]) >= 2
    assert any(str(item).startswith("OBS-") for item in candidate["lineage"])
    assert any(str(item).startswith("FEAT-") for item in candidate["lineage"])
    assert candidate["trading_allowed"] is False
    assert candidate["capital_allocation_allowed"] is False


def test_features_include_required_metadata_and_known_at_rule(tmp_path: Path) -> None:
    payload = build_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY)

    feature_types = {row["feature_type"] for row in payload["feature_store"]}
    assert {
        "daily_return",
        "return_5d",
        "return_20d",
        "rolling_volatility_20d",
        "rolling_correlation_20d",
        "rolling_beta_20d",
        "z_score_20d",
        "shock_indicator",
        "regime_label",
    }.issubset(feature_types)
    assert all(row["feature_id"] for row in payload["feature_store"])
    assert all(row["source_observations"] for row in payload["feature_store"])
    assert all(row["calculation_version"] for row in payload["feature_store"])
    assert all(row["known_at_rule"] for row in payload["feature_store"])


def test_event_log_contains_all_required_append_only_actions(tmp_path: Path) -> None:
    payload = build_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY)
    event_types = {row["event_type"] for row in payload["event_log"]}

    assert {
        "OBSERVATION_LOADED",
        "FEATURE_CALCULATED",
        "QUESTION_GENERATED",
        "HYPOTHESIS_GENERATED",
        "EXPERIMENT_RUN",
        "EVIDENCE_CREATED",
        "RESEARCH_ASSET_CANDIDATE_CREATED",
    }.issubset(event_types)
    assert all(row["append_only"] is True for row in payload["event_log"])


def test_output_is_reproducible_and_writes_artifacts(tmp_path: Path) -> None:
    first = build_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY)
    assert first["content_hash"] == second["content_hash"]

    paths = write_alpha_factory_poc_v1(truth_root=tmp_path, day_utc=DAY, payload=first)
    assert Path(paths["json"]).exists()
    assert Path(paths["events"]).exists()
    assert Path(paths["summary"]).exists()
