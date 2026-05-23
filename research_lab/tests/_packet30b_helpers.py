
from pathlib import Path

from research_lab.event_intake.event_intake_registry import (
    create_event_cluster,
    generate_hypothesis_proposal,
    generate_intent_candidate,
    seed_event_families,
    store_event_observation,
)
from research_lab.event_intake.event_observation import build_event_observation
from research_lab.storage.manifest_io import write_json, append_jsonl


def seed_oil_proposal(store: Path) -> dict:
    seed_event_families(store_root=store, actor="Tester")
    observation = build_event_observation(
        event_family_id="oil_shock",
        title="Viral claim about large crude oil short before geopolitical headline",
        description="Social media claim alleging large crude short before headline-driven oil drop and rebound.",
        source_type="social_media_claim",
        symbols_mentioned=["USO", "XLE", "DBC", "SPY"],
        suspected_mechanism="geopolitical oil shock / headline overreaction / possible information asymmetry claim",
        confidence_level="low",
        research_priority="watchlist",
        notes="unverified_claim; do not treat as factual; research only public-event market behavior",
        observed_at="2026-05-20T00:00:00Z",
    )
    store_event_observation(observation, store_root=store, actor="Tester")
    cluster_result = create_event_cluster(event_family_id="oil_shock", observation_ids=[observation["event_observation_id"]], cluster_title="Oil shock headline dislocation observations", store_root=store, actor="Tester")
    intent_result = generate_intent_candidate(event_cluster_id=cluster_result["event_cluster"]["event_cluster_id"], store_root=store, actor="Tester")
    proposal_result = generate_hypothesis_proposal(intent_candidate_id=intent_result["intent_candidate"]["intent_candidate_id"], store_root=store, actor="Tester")
    return {"observation": observation, "cluster": cluster_result["event_cluster"], "intent": intent_result["intent_candidate"], "proposal": proposal_result["hypothesis_proposal"]}


def seed_minimum_dataset_and_cost_model(store: Path) -> None:
    dataset_id = "ds_minimum"
    manifest = {"dataset_snapshot_id": dataset_id, "symbols_loaded": ["SPY"], "row_count": 10, "quality_status": "pass", "schema_version": "ohlcv_dataset_manifest.v1"}
    write_json(store / "datasets" / dataset_id / "manifest.json", manifest, overwrite=False)
    append_jsonl(store / "registries" / "dataset_snapshots.jsonl", {"dataset_snapshot_id": dataset_id, "quality_status": "pass", "row_count": 10, "symbol_count": 1, "schema_version": "dataset_snapshot.v1"})
    append_jsonl(store / "registries" / "cost_model_snapshots.jsonl", {"cost_model_snapshot_id": "cm_default", "name": "Default", "content_hash": "abc123abc123", "schema_version": "cost_model_snapshot.v1"})
