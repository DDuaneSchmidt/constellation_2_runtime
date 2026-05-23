from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.sleeves.sleeve_definition import build_sleeve_definition, build_sleeve_version
from research_lab.sleeves.sleeve_registry import store_sleeve_definition, store_sleeve_version
from research_lab.storage.manifest_io import write_json


def _write_refs(store: Path) -> None:
    for ev in ["ev_event", "ev_bt"]:
        write_json(store / "evidence_packages" / ev / "evidence_manifest.json", {"evidence_package_id": ev, "hypothesis_id": "hyp_fixture", "research_plan_id": "rp", "dataset_snapshot_id": "ds_fixture", "universe_snapshot_id": "us", "runner_name": "fixture", "runner_version": "v1", "runner_input_hash": "a" * 12, "runner_output_hash": "b" * 12, "artifact_uris": [], "summary_uri": f"research://evidence/{ev}/summary.json", "manifest_hash": "c" * 12, "created_at": "2024-01-01T00:00:00Z", "created_by": "pytest", "schema_version": "evidence_package.v1"}, overwrite=False)
    write_json(store / "candidate_batches" / "cb_fixture" / "candidate_batch.json", {"candidate_batch_id": "cb_fixture"}, overwrite=False)
    write_json(store / "datasets" / "ds_fixture" / "dataset_snapshot.json", {"dataset_snapshot_id": "ds_fixture"}, overwrite=False)
    write_json(store / "regimes" / "rs_fixture" / "regime_snapshot.json", {"regime_snapshot_id": "rs_fixture"}, overwrite=False)
    write_json(store / "cost_models" / "cm_fixture" / "cost_model_snapshot.json", {"cost_model_snapshot_id": "cm_fixture"}, overwrite=False)


def test_sleeve_version_cannot_overwrite_existing_version(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _write_refs(store)
    store_sleeve_definition(build_sleeve_definition(sleeve_id="slv_fixture", name="Fixture", hypothesis_id="hyp_fixture", created_at="2024-01-01T00:00:00Z"), store_root=store)
    version = build_sleeve_version(sleeve_id="slv_fixture", version="v1", hypothesis_id="hyp_fixture", event_study_evidence_package_id="ev_event", backtest_evidence_package_id="ev_bt", candidate_batch_id="cb_fixture", dataset_snapshot_id="ds_fixture", regime_snapshot_id="rs_fixture", cost_model_snapshot_id="cm_fixture", created_at="2024-01-01T00:00:00Z")
    store_sleeve_version(version, store_root=store)

    with pytest.raises(FileExistsError):
        store_sleeve_version(version, store_root=store)

