from __future__ import annotations

from pathlib import Path

import pytest

from ops.aegis.research_lab.research_lab_routes import research_lab_challenger_track_v1, research_lab_challenger_tracks_v1
from research_lab.challengers.challenger_track import (
    build_challenger_research_track,
    deterministic_challenger_hypotheses,
    write_challenger_research_track,
)
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.manifest_io import read_jsonl, write_json


SLEEVE_ID = "slv_fixture"
VERSION_ID = "slvv_fixture"
STABILITY_ID = "ssr_fixture"
DRIFT_ID = "edr_fixture"
FRAGILITY_ID = "rfr_fixture"


def _write_source(path: Path, artifact_id: str) -> None:
    write_json(path, {"artifact_id": artifact_id, "content_hash": f"hash_{artifact_id}"}, overwrite=False)


def _write_fixture(store: Path, *, stability_id: str = STABILITY_ID) -> None:
    write_json(
        store / "sleeves" / SLEEVE_ID / "versions" / f"{VERSION_ID}.json",
        {
            "sleeve_id": SLEEVE_ID,
            "sleeve_version_id": VERSION_ID,
            "dataset_snapshot_id": "ds_fixture",
            "regime_snapshot_id": "rs_fixture",
            "cost_model_snapshot_id": "cm_fixture",
            "linked_event_study_evidence_package_id": "ev_event_fixture",
            "linked_backtest_evidence_package_id": "ev_backtest_fixture",
            "linked_candidate_batch_ids": ["cb_fixture"],
            "linked_evidence_package_ids": ["ev_event_fixture", "ev_backtest_fixture"],
            "content_hash": "hash_version",
            "schema_version": "sleeve_version.v1",
        },
        overwrite=False,
    )
    write_json(
        store / "stability_reports" / "expectancy_drift" / f"{DRIFT_ID}.json",
        {
            "expectancy_drift_report_id": DRIFT_ID,
            "sleeve_id": SLEEVE_ID,
            "sleeve_version_id": VERSION_ID,
            "longitudinal_run_id": "lcr_fixture",
            "drift_status": "degrading",
            "recommended_action": "challenge_sleeve",
            "research_label": "Research governance view only. No broker execution.",
            "content_hash": "hash_drift",
        },
        overwrite=False,
    )
    write_json(
        store / "stability_reports" / "regime_fragility" / f"{FRAGILITY_ID}.json",
        {
            "regime_fragility_report_id": FRAGILITY_ID,
            "sleeve_id": SLEEVE_ID,
            "sleeve_version_id": VERSION_ID,
            "longitudinal_run_id": "lcr_fixture",
            "fragility_status": "watch",
            "recommended_action": "review_sleeve",
            "research_label": "Research governance view only. No broker execution.",
            "content_hash": "hash_fragility",
        },
        overwrite=False,
    )
    write_json(
        store / "stability_reports" / "sleeve_stability" / f"{stability_id}.json",
        {
            "sleeve_stability_report_id": stability_id,
            "sleeve_id": SLEEVE_ID,
            "sleeve_version_id": VERSION_ID,
            "expectancy_drift_report_id": DRIFT_ID,
            "regime_fragility_report_id": FRAGILITY_ID,
            "overall_stability_status": "degrading",
            "recommended_action": "challenge_sleeve",
            "key_findings": ["expectancy_drift_status=degrading", "regime_fragility_status=watch"],
            "research_label": "Research governance view only. No broker execution.",
            "content_hash": "hash_stability",
        },
        overwrite=False,
    )
    _write_source(store / "datasets" / "ds_fixture" / "dataset_snapshot.json", "ds_fixture")
    _write_source(store / "regimes" / "rs_fixture" / "regime_snapshot.json", "rs_fixture")
    _write_source(store / "cost_models" / "cm_fixture" / "cost_model_snapshot.json", "cm_fixture")
    _write_source(store / "evidence_packages" / "ev_event_fixture" / "evidence_manifest.json", "ev_event_fixture")
    _write_source(store / "evidence_packages" / "ev_backtest_fixture" / "evidence_manifest.json", "ev_backtest_fixture")
    _write_source(store / "candidate_batches" / "cb_fixture" / "candidate_batch.json", "cb_fixture")


def _file_listing(store: Path, rel: str) -> list[str]:
    root = store / rel
    if not root.exists():
        return []
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file())


def test_challenger_track_creation_preserves_triggers_and_research_label(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _write_fixture(store)

    track = build_challenger_research_track(sleeve_id=SLEEVE_ID, sleeve_stability_report_id=STABILITY_ID, store_root=store)

    validate_contract("challenger_research_track", track)
    assert track["trigger_report_ids"]["sleeve_stability_report_id"] == STABILITY_ID
    assert track["trigger_report_ids"]["expectancy_drift_report_id"] == DRIFT_ID
    assert track["trigger_report_ids"]["regime_fragility_report_id"] == FRAGILITY_ID
    assert track["research_label"] == "RESEARCH_ONLY"
    assert track["status"] == "evidence_pending"
    assert track["recommended_next_action"] == "generate_challenger_evidence"
    assert len(track["challenger_hypothesis_set"]) == 7


def test_missing_trigger_artifacts_block_track(tmp_path: Path) -> None:
    track = build_challenger_research_track(sleeve_id=SLEEVE_ID, sleeve_stability_report_id="missing", store_root=tmp_path / "store")

    assert track["status"] == "blocked"
    assert track["recommended_next_action"] == "investigate_blocker"
    assert track["audit_refs"]["missing_required_sources"]


def test_deterministic_hypothesis_generation_and_exclusion_rules(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _write_fixture(store)

    first = build_challenger_research_track(sleeve_id=SLEEVE_ID, sleeve_stability_report_id=STABILITY_ID, store_root=store)
    second = build_challenger_research_track(sleeve_id=SLEEVE_ID, sleeve_stability_report_id=STABILITY_ID, store_root=store)

    assert first == second
    assert deterministic_challenger_hypotheses(SLEEVE_ID) == deterministic_challenger_hypotheses(SLEEVE_ID)
    assert "negative post-cost expectancy" in first["exclusion_rules"]
    assert "any artifact mutation detected" in first["exclusion_rules"]


def test_write_is_immutable_append_only_and_does_not_mutate_incumbent_or_trials_or_candidates(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _write_fixture(store)
    sleeve_before = _file_listing(store, "sleeves")
    paper_before = _file_listing(store, "paper_trials")
    candidate_before = _file_listing(store, "candidate_batches")

    result = write_challenger_research_track(sleeve_id=SLEEVE_ID, sleeve_stability_report_id=STABILITY_ID, store_root=store)
    rows = read_jsonl(store / "registries" / "challenger_research_tracks.jsonl")

    assert rows[-1]["challenger_track_id"] == result["track"]["challenger_track_id"]
    assert _file_listing(store, "sleeves") == sleeve_before
    assert _file_listing(store, "paper_trials") == paper_before
    assert _file_listing(store, "candidate_batches") == candidate_before
    with pytest.raises(FileExistsError):
        write_challenger_research_track(sleeve_id=SLEEVE_ID, sleeve_stability_report_id=STABILITY_ID, store_root=store)


def test_read_only_routes_expose_projection_without_trading_or_capital_allocation(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _write_fixture(store)
    result = write_challenger_research_track(sleeve_id=SLEEVE_ID, sleeve_stability_report_id=STABILITY_ID, store_root=store)

    listing = research_lab_challenger_tracks_v1(store_root=store)
    detail = research_lab_challenger_track_v1(challenger_track_id=result["track"]["challenger_track_id"], store_root=store)

    assert listing["read_only"] is True
    assert listing["challenger_tracks"][0]["challenger_track_id"] == result["track"]["challenger_track_id"]
    assert detail["read_only"] is True
    assert detail["challenger_track"]["status"] == "evidence_pending"
    text = str(detail).lower()
    assert "canonical_ohlcv" not in text
    assert "trading_instructions" not in text
    assert "capital_allocation_details" not in text


def test_no_broker_order_trading_capital_allocation_objects_introduced(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _write_fixture(store)

    track = build_challenger_research_track(sleeve_id=SLEEVE_ID, sleeve_stability_report_id=STABILITY_ID, store_root=store)

    keys = {str(key).lower() for key in track}
    assert track["governance_constraints"]["broker_execution_allowed"] is False
    assert track["governance_constraints"]["order_management_allowed"] is False
    assert track["governance_constraints"]["capital_allocation_allowed"] is False
    assert "order_management_object" not in keys
    assert "broker_order" not in keys
    assert "capital_allocation_object" not in keys
