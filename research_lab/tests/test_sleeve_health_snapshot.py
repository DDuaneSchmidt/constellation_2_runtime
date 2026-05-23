from __future__ import annotations

from pathlib import Path

from research_lab.sleeves.sleeve_definition import build_sleeve_definition, build_sleeve_version
from research_lab.sleeves.sleeve_health import build_sleeve_health_snapshot
from research_lab.sleeves.sleeve_registry import store_sleeve_definition, store_sleeve_version
from research_lab.storage.manifest_io import write_json


def _write_health_fixture(store: Path) -> tuple[str, str]:
    sleeve_id = "slv_fixture"
    write_json(store / "evidence_packages" / "ev_event" / "evidence_manifest.json", {"evidence_package_id": "ev_event", "hypothesis_id": "hyp_fixture", "research_plan_id": "rp", "dataset_snapshot_id": "ds_fixture", "universe_snapshot_id": "us", "runner_name": "event_study_v1", "runner_version": "v1", "runner_input_hash": "a" * 12, "runner_output_hash": "b" * 12, "artifact_uris": [], "summary_uri": "research://evidence/ev_event/summary.json", "manifest_hash": "c" * 12, "created_at": "2024-01-01T00:00:00Z", "created_by": "pytest", "schema_version": "evidence_package.v1"}, overwrite=False)
    write_json(store / "evidence_packages" / "ev_bt" / "evidence_manifest.json", {"evidence_package_id": "ev_bt", "hypothesis_id": "hyp_fixture", "research_plan_id": "btp", "dataset_snapshot_id": "ds_fixture", "universe_snapshot_id": "us", "runner_name": "holding_period_backtest_v1", "runner_version": "v1", "runner_input_hash": "a" * 12, "runner_output_hash": "b" * 12, "artifact_uris": [], "summary_uri": "research://evidence/ev_bt/performance_summary.json", "manifest_hash": "c" * 12, "created_at": "2024-01-01T00:00:00Z", "created_by": "pytest", "schema_version": "evidence_package.v1", "evidence_quality": "research_simulation"}, overwrite=False)
    write_json(store / "evidence_packages" / "ev_bt" / "performance_summary.json", {"post_cost": {"total_return": 0.10}, "excess_return_vs_benchmark": -0.50}, overwrite=False)
    write_json(store / "candidate_batches" / "cb_fixture" / "candidate_batch.json", {"candidate_batch_id": "cb_fixture"}, overwrite=False)
    write_json(store / "candidate_batches" / "cb_fixture" / "generation_summary.json", {"candidate_count": 0}, overwrite=False)
    write_json(store / "candidate_batches" / "cb_fixture" / "attribution_report.json", {"attribution_report_id": "attr_fixture", "measured_candidate_count": 0}, overwrite=False)
    write_json(store / "datasets" / "ds_fixture" / "dataset_snapshot.json", {"dataset_snapshot_id": "ds_fixture"}, overwrite=False)
    write_json(store / "regimes" / "rs_fixture" / "regime_snapshot.json", {"regime_snapshot_id": "rs_fixture"}, overwrite=False)
    write_json(store / "cost_models" / "cm_fixture" / "cost_model_snapshot.json", {"cost_model_snapshot_id": "cm_fixture"}, overwrite=False)
    store_sleeve_definition(build_sleeve_definition(sleeve_id=sleeve_id, name="Fixture", hypothesis_id="hyp_fixture", created_at="2024-01-01T00:00:00Z"), store_root=store)
    version = build_sleeve_version(sleeve_id=sleeve_id, version="v1", hypothesis_id="hyp_fixture", event_study_evidence_package_id="ev_event", backtest_evidence_package_id="ev_bt", candidate_batch_id="cb_fixture", dataset_snapshot_id="ds_fixture", regime_snapshot_id="rs_fixture", cost_model_snapshot_id="cm_fixture", created_at="2024-01-01T00:00:00Z")
    store_sleeve_version(version, store_root=store)
    return sleeve_id, version["sleeve_version_id"]


def test_sleeve_health_marks_current_weaknesses_as_watch(tmp_path: Path) -> None:
    store = tmp_path / "store"
    sleeve_id, version_id = _write_health_fixture(store)

    health = build_sleeve_health_snapshot(sleeve_id=sleeve_id, sleeve_version_id=version_id, as_of_date="2024-01-02", store_root=store, created_at="2024-01-02T00:00:00Z")

    assert health["backtest_health"] == "watch"
    assert health["candidate_health"] == "watch"
    assert health["attribution_health"] == "watch"
    assert health["overall_health"] == "watch"
    assert health["overall_health"] != "healthy"

