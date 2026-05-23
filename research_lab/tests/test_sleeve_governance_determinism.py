from __future__ import annotations

from research_lab.sleeves.sleeve_definition import build_sleeve_definition, build_sleeve_version
from research_lab.sleeves.sleeve_health import build_sleeve_health_snapshot
from research_lab.tests.test_sleeve_health_snapshot import _write_health_fixture


def test_sleeve_inputs_produce_same_hashes() -> None:
    first = build_sleeve_definition(sleeve_id="slv_fixture", name="Fixture", hypothesis_id="hyp_fixture", created_at="2024-01-01T00:00:00Z")
    second = build_sleeve_definition(sleeve_id="slv_fixture", name="Fixture", hypothesis_id="hyp_fixture", created_at="2024-01-02T00:00:00Z")
    assert first["content_hash"] == second["content_hash"]

    v1 = build_sleeve_version(sleeve_id="slv_fixture", version="v1", hypothesis_id="hyp_fixture", event_study_evidence_package_id="ev_event", backtest_evidence_package_id="ev_bt", candidate_batch_id="cb_fixture", dataset_snapshot_id="ds_fixture", regime_snapshot_id="rs_fixture", cost_model_snapshot_id="cm_fixture", created_at="2024-01-01T00:00:00Z")
    v2 = build_sleeve_version(sleeve_id="slv_fixture", version="v1", hypothesis_id="hyp_fixture", event_study_evidence_package_id="ev_event", backtest_evidence_package_id="ev_bt", candidate_batch_id="cb_fixture", dataset_snapshot_id="ds_fixture", regime_snapshot_id="rs_fixture", cost_model_snapshot_id="cm_fixture", created_at="2024-01-02T00:00:00Z")
    assert v1["content_hash"] == v2["content_hash"]


def test_health_snapshot_deterministic_except_created_at(tmp_path) -> None:
    store = tmp_path / "store"
    sleeve_id, version_id = _write_health_fixture(store)
    first = build_sleeve_health_snapshot(sleeve_id=sleeve_id, sleeve_version_id=version_id, as_of_date="2024-01-02", store_root=store, created_at="2024-01-01T00:00:00Z")
    second = build_sleeve_health_snapshot(sleeve_id=sleeve_id, sleeve_version_id=version_id, as_of_date="2024-01-02", store_root=store, created_at="2024-01-02T00:00:00Z")
    assert first["content_hash"] == second["content_hash"]

