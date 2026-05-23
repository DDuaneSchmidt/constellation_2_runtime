from __future__ import annotations

from pathlib import Path

from ops.aegis.research_lab.research_lab_routes import (
    research_lab_observation_cluster_batch_latest_v1,
    research_lab_observation_cluster_batch_v1,
    research_lab_observation_cluster_batches_v1,
    research_lab_observation_cluster_latest_v1,
    research_lab_observation_cluster_v1,
    research_lab_observation_clusters_v1,
)
from research_lab.contracts.schemas import validate_contract
from research_lab.observations.observation_batch import write_observation_candidate_batch
from research_lab.observations.observation_cluster_batch import build_observation_cluster_batch, write_observation_cluster_batch
from research_lab.observations.observation_clustering import cluster_observations
from research_lab.storage.manifest_io import read_jsonl
from research_lab.tests.test_observation_candidates import _counts
from research_lab.tests.test_research_os_status_report import _failed_integrity_fixture
from research_lab.status.research_os_status import write_research_os_status_report


def _seed_observations(store: Path) -> dict:
    _failed_integrity_fixture(store)
    write_research_os_status_report(store_root=store, generated_at="2026-05-19T00:00:00Z")
    return write_observation_candidate_batch(store_root=store, scanners=["all"], generated_at="2026-05-19T00:10:00Z")


def _cluster_counts(store: Path) -> dict[str, int]:
    counts = _counts(store)
    counts.update(
        {
            "clusters": len(list((store / "observation_clusters").glob("*.json"))) if (store / "observation_clusters").exists() else 0,
            "cluster_batches": len(list((store / "observation_cluster_batches").glob("*.json"))) if (store / "observation_cluster_batches").exists() else 0,
        }
    )
    return counts


def test_cluster_latest_packet25_batch_has_expected_families(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _seed_observations(store)

    clusters, _ = cluster_observations(store_root=store, latest_only=True, generated_at="2026-05-19T00:20:00Z")
    families = {row["cluster_family"] for row in clusters}

    assert len(clusters) >= 3
    assert "research_store_integrity" in families
    assert "evidence_quality" in families
    assert "regime_behavior" in families
    assert any(row["cluster_status"] == "recurring" for row in clusters)
    for row in clusters:
        validate_contract("observation_cluster", row)
        assert row["actionability_status"] == "non_actionable_research_only"
        assert "buy" not in row
        assert "sell" not in row
        assert "hold" not in row


def test_market_data_unavailable_creates_blocked_cluster(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _seed_observations(store)

    clusters, _ = cluster_observations(store_root=store, latest_only=True, generated_at="2026-05-19T00:20:00Z")
    market = [row for row in clusters if row["cluster_family"] == "market_price_volume"]

    assert market
    assert market[0]["cluster_status"] == "blocked"
    assert any(blocker.get("blocker_code") == "market_data_unavailable" for blocker in market[0]["blockers"])


def test_cluster_batch_persistence_registry_audit_api_and_no_lifecycle_mutation(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _seed_observations(store)
    before = _cluster_counts(store)

    result = write_observation_cluster_batch(store_root=store, latest_only=True, generated_at="2026-05-19T00:20:00Z")
    after = _cluster_counts(store)
    batch = result["batch"]

    validate_contract("observation_cluster_batch", batch)
    assert batch["cluster_count"] >= 4
    assert batch["non_actionable_research_only_assertion"]["hypothesis_created"] is False
    assert after["hypotheses"] == before["hypotheses"]
    assert after["challengers"] == before["challengers"]
    assert after["sleeves"] == before["sleeves"]
    assert after["paper_trials"] == before["paper_trials"]
    assert after["paper_trial_proposals"] == before["paper_trial_proposals"]
    assert after["candidate_batches"] == before["candidate_batches"]
    assert after["clusters"] == before.get("clusters", 0) + batch["cluster_count"]
    assert after["cluster_batches"] == before.get("cluster_batches", 0) + 1
    assert after["audit"] == before["audit"] + batch["cluster_count"] + 1
    assert read_jsonl(store / "registries" / "observation_cluster_batch_registry.json")[-1]["observation_cluster_batch_id"] == batch["observation_cluster_batch_id"]
    assert len(read_jsonl(store / "registries" / "observation_cluster_registry.json")) == batch["cluster_count"]

    listing = research_lab_observation_clusters_v1(store_root=store)
    latest_cluster = research_lab_observation_cluster_latest_v1(store_root=store)
    cluster_detail = research_lab_observation_cluster_v1(observation_cluster_id=latest_cluster["observation_cluster"]["observation_cluster_id"], store_root=store)
    batch_listing = research_lab_observation_cluster_batches_v1(store_root=store)
    latest_batch = research_lab_observation_cluster_batch_latest_v1(store_root=store)
    batch_detail = research_lab_observation_cluster_batch_v1(observation_cluster_batch_id=batch["observation_cluster_batch_id"], store_root=store)
    assert listing["read_only"] is True
    assert cluster_detail["read_only"] is True
    assert batch_listing["read_only"] is True
    assert latest_batch["observation_cluster_batch"]["observation_cluster_batch_id"] == batch["observation_cluster_batch_id"]
    assert batch_detail["observation_cluster_batch"]["observation_cluster_batch_id"] == batch["observation_cluster_batch_id"]


def test_cluster_batch_fingerprint_is_deterministic_except_generated_ids(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _seed_observations(store)

    first_clusters, first_batches = cluster_observations(store_root=store, latest_only=True, generated_at="2026-05-19T00:20:00Z")
    second_clusters, second_batches = cluster_observations(store_root=store, latest_only=True, generated_at="2026-05-19T00:21:00Z")
    first = build_observation_cluster_batch(clusters=first_clusters, source_batches=first_batches, generated_at="2026-05-19T00:20:00Z", clustering_run_id="run_a")
    second = build_observation_cluster_batch(clusters=second_clusters, source_batches=second_batches, generated_at="2026-05-19T00:21:00Z", clustering_run_id="run_b")

    assert first["observation_cluster_batch_id"] != second["observation_cluster_batch_id"]
    assert first["immutable_hash"] == second["immutable_hash"]

