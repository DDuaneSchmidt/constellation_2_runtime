from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.research_lab.research_store_reader import (
    DEFAULT_SLEEVE_ID,
    FORBIDDEN_CANONICAL_MARKET_DATA_TABLES,
    build_edge_lab_projection,
    build_evidence_chain_view,
)
from ops.aegis.research_lab.research_store_refs_v1 import as_reference_model_manifest
from ops.aegis.research_lab.research_lab_routes import (
    research_lab_edge_lab_projection_v1,
    research_lab_evidence_chain_v1,
    research_lab_health_v1,
)


def test_research_store_reader_loads_current_sleeve_and_evidence_chain() -> None:
    payload = build_evidence_chain_view(DEFAULT_SLEEVE_ID)

    assert payload["read_only"] is True
    assert payload["sleeve_id"] == "slv_etf_drop_reversion_v1"
    assert payload["sleeve_version_id"] == "slvv_slv_etf_drop_reversion_v1_v1_1c36b58d9b"
    assert payload["dataset_snapshot_id"] == "ds_ohlcv_1d_local_etf_minimum_viable_20150101_20251231_9fde16"
    assert payload["event_study_evidence_id"] == "ev_hyp_etf_drop_reversion_v1_20260518_c116a70295"
    assert payload["backtest_evidence_id"] == "ev_bt_hyp_etf_drop_reversion_v1_20260518_19251c7cf9"
    assert "cb_hyp_etf_drop_reversion_v1_20251231_5252eb333a" in payload["candidate_batch_ids"]
    assert "lcr_slv_etf_drop_reversion_v1_20200101_20251231_weekly_c8b1fd8b29" in payload["longitudinal_run_ids"]
    assert "ptr_slv_etf_drop_reversion_v1_2b7b327cde71" in payload["paper_trial_ids"]
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False
    assert payload["automatic_sleeve_mutation_allowed"] is False


def test_missing_artifact_is_reported_not_fabricated(tmp_path: Path) -> None:
    store = tmp_path / "research_store"
    (store / "sleeves").mkdir(parents=True)

    payload = build_evidence_chain_view("missing_sleeve", store_root=store)

    assert payload["read_only"] is True
    assert payload["sleeve_id"] == "missing_sleeve"
    assert payload["missing_artifacts"]
    assert all(row["status"] == "missing" for row in payload["artifact_refs"])
    assert "Research / paper-trial information only" in payload["research_label"]


def test_hash_mismatch_is_reported(tmp_path: Path) -> None:
    sleeve_dir = tmp_path / "research_store" / "sleeves" / "slv_bad"
    sleeve_dir.mkdir(parents=True)
    (sleeve_dir / "sleeve_definition.json").write_text(
        json.dumps(
            {
                "sleeve_id": "slv_bad",
                "name": "Bad Hash Sleeve",
                "hypothesis_id": "hyp_bad",
                "status": "draft",
                "content_hash": "not-a-real-hash",
            }
        ),
        encoding="utf-8",
    )

    payload = build_evidence_chain_view("slv_bad", store_root=tmp_path / "research_store")

    assert payload["hash_mismatches"]
    assert payload["errors"][0]["error"] == "artifact_hash_mismatch"


def test_edge_lab_projection_maps_active_paper_trial_to_lane_and_preserves_watch_state() -> None:
    projection = build_edge_lab_projection(DEFAULT_SLEEVE_ID)

    assert projection["lane"] == "Paper Trial Active"
    assert projection["health"] == "watch"
    assert projection["challenge_type"] == "underperformance"
    assert projection["recommended_action"] == "continue_research"
    assert projection["paper_trial_status"] == "active"
    assert projection["latest_review_decision"] == "continue"
    assert projection["read_only"] is True
    assert projection["automatic_promotion_allowed"] is False


def test_research_lab_routes_are_read_only_views() -> None:
    health = research_lab_health_v1()
    chain = research_lab_evidence_chain_v1(sleeve_id=DEFAULT_SLEEVE_ID)
    projection = research_lab_edge_lab_projection_v1(sleeve_id=DEFAULT_SLEEVE_ID)

    assert health["read_only"] is True
    assert health["research_store_available"] is True
    assert chain["read_only"] is True
    assert projection["read_only"] is True
    assert projection["evidence_chain"]["sleeve_id"] == DEFAULT_SLEEVE_ID
    assert "No broker execution" in projection["research_label"]


def test_aegis_reference_boundary_still_has_no_canonical_ohlcv_tables() -> None:
    manifest = as_reference_model_manifest()

    assert set(manifest["forbidden_canonical_market_data_tables"]) == FORBIDDEN_CANONICAL_MARKET_DATA_TABLES
    assert all(row["stores_canonical_ohlcv_rows"] is False for row in manifest["reference_tables"])
