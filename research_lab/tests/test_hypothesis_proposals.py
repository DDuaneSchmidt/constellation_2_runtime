from __future__ import annotations

from pathlib import Path

from ops.aegis.research_lab.research_lab_routes import (
    research_lab_hypothesis_proposal_batch_latest_v1,
    research_lab_hypothesis_proposal_batch_v1,
    research_lab_hypothesis_proposal_batches_v1,
    research_lab_hypothesis_proposal_latest_v1,
    research_lab_hypothesis_proposal_v1,
    research_lab_hypothesis_proposals_v1,
    research_lab_observation_cluster_hypothesis_proposals_v1,
)
from research_lab.contracts.schemas import validate_contract
from research_lab.hypotheses.hypothesis_proposal_batch import build_hypothesis_proposal_batch, write_hypothesis_proposal_batch
from research_lab.hypotheses.hypothesis_proposal_engine import generate_hypothesis_proposals
from research_lab.observations.observation_cluster_batch import write_observation_cluster_batch
from research_lab.storage.manifest_io import read_jsonl
from research_lab.tests.test_observation_candidates import _counts
from research_lab.tests.test_observation_clusters import _seed_observations


def _seed_cluster_batch(store: Path) -> dict:
    _seed_observations(store)
    return write_observation_cluster_batch(store_root=store, latest_only=True, generated_at="2026-05-19T00:20:00Z")


def _proposal_counts(store: Path) -> dict[str, int]:
    counts = _counts(store)
    counts.update(
        {
            "hypothesis_proposals": len(list((store / "hypothesis_proposals").glob("*.json"))) if (store / "hypothesis_proposals").exists() else 0,
            "hypothesis_proposal_batches": len(list((store / "hypothesis_proposal_batches").glob("*.json"))) if (store / "hypothesis_proposal_batches").exists() else 0,
            "active_hypotheses": len(list((store / "hypotheses").glob("*.json"))) if (store / "hypotheses").exists() else 0,
        }
    )
    return counts


def test_generates_proposal_batch_from_latest_cluster_batch(tmp_path: Path) -> None:
    store = tmp_path / "store"
    cluster_result = _seed_cluster_batch(store)

    result = write_hypothesis_proposal_batch(store_root=store, generated_at="2026-05-19T00:30:00Z")
    batch = result["batch"]
    proposals = [row["proposal"] for row in result["proposals"]]

    validate_contract("hypothesis_proposal_batch", batch)
    assert batch["proposal_count"] == cluster_result["batch"]["cluster_count"]
    assert batch["source_observation_cluster_batch_id"]
    assert len(batch["hypothesis_proposal_ids"]) == batch["proposal_count"]
    for proposal in proposals:
        validate_contract("hypothesis_proposal", proposal)
        assert proposal["source_observation_cluster_id"]
        assert proposal["source_observation_cluster_batch_id"] == batch["source_observation_cluster_batch_id"]
        assert proposal["actionability_status"] == "non_actionable_research_only"


def test_market_data_blocked_proposal_and_red_status_handling(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _seed_cluster_batch(store)

    result = write_hypothesis_proposal_batch(store_root=store, generated_at="2026-05-19T00:30:00Z")
    proposals = [row["proposal"] for row in result["proposals"]]
    market = [row for row in proposals if row["cluster_family"] == "market_price_volume"][0]
    regime_rows = [row for row in proposals if row["cluster_family"] == "regime_behavior"]

    assert market["proposal_status"] == "blocked"
    assert any(blocker.get("blocker_code") == "market_data_unavailable" for blocker in market["blockers"])
    assert market["non_actionable_research_only_assertion"]["active_hypothesis_created"] is False
    if regime_rows:
        regime = regime_rows[0]
        assert regime["proposal_status"] in {"blocked", "needs_more_observation"}
        assert any(blocker.get("blocker_code") in {"research_os_status_red", "integrity_status_fail"} for blocker in regime["blockers"])


def test_evidence_quality_and_integrity_are_system_research_only(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _seed_cluster_batch(store)

    result = write_hypothesis_proposal_batch(store_root=store, generated_at="2026-05-19T00:30:00Z")
    proposals = [row["proposal"] for row in result["proposals"]]
    evidence = [row for row in proposals if row["proposal_family"] == "evidence_quality"][0]
    integrity = [row for row in proposals if row["proposal_family"] == "research_store_integrity"][0]

    assert evidence["proposal_status"] == "system_research_only"
    assert evidence["proposed_test_design"]["test_type"] == "evidence_quality_review"
    assert "trading" not in evidence["proposed_hypothesis_statement"].lower()
    assert integrity["proposal_status"] == "system_research_only"
    assert integrity["proposed_test_design"]["test_type"] == "integrity_resolution_review"


def test_append_only_registry_audit_api_and_no_lifecycle_mutation(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _seed_cluster_batch(store)
    before = _proposal_counts(store)

    result = write_hypothesis_proposal_batch(store_root=store, generated_at="2026-05-19T00:30:00Z")
    batch = result["batch"]
    after = _proposal_counts(store)

    assert after["active_hypotheses"] == before["active_hypotheses"]
    assert after["hypotheses"] == before["hypotheses"]
    assert after["challengers"] == before["challengers"]
    assert after["sleeves"] == before["sleeves"]
    assert after["paper_trials"] == before["paper_trials"]
    assert after["paper_trial_proposals"] == before["paper_trial_proposals"]
    assert after["candidate_batches"] == before["candidate_batches"]
    assert after["hypothesis_proposals"] == before["hypothesis_proposals"] + batch["proposal_count"]
    assert after["hypothesis_proposal_batches"] == before["hypothesis_proposal_batches"] + 1
    assert after["audit"] == before["audit"] + batch["proposal_count"] + 1
    assert read_jsonl(store / "registries" / "hypothesis_proposal_batch_registry.json")[-1]["hypothesis_proposal_batch_id"] == batch["hypothesis_proposal_batch_id"]
    assert len(read_jsonl(store / "registries" / "hypothesis_proposal_registry.json")) == batch["proposal_count"]

    listing = research_lab_hypothesis_proposals_v1(store_root=store)
    latest_proposal = research_lab_hypothesis_proposal_latest_v1(store_root=store)
    proposal_detail = research_lab_hypothesis_proposal_v1(hypothesis_proposal_id=latest_proposal["hypothesis_proposal"]["hypothesis_proposal_id"], store_root=store)
    batch_listing = research_lab_hypothesis_proposal_batches_v1(store_root=store)
    latest_batch = research_lab_hypothesis_proposal_batch_latest_v1(store_root=store)
    batch_detail = research_lab_hypothesis_proposal_batch_v1(hypothesis_proposal_batch_id=batch["hypothesis_proposal_batch_id"], store_root=store)
    cluster_link = research_lab_observation_cluster_hypothesis_proposals_v1(observation_cluster_id=latest_proposal["hypothesis_proposal"]["source_observation_cluster_id"], store_root=store)
    assert listing["read_only"] is True
    assert proposal_detail["read_only"] is True
    assert batch_listing["read_only"] is True
    assert latest_batch["hypothesis_proposal_batch"]["hypothesis_proposal_batch_id"] == batch["hypothesis_proposal_batch_id"]
    assert batch_detail["hypothesis_proposal_batch"]["hypothesis_proposal_batch_id"] == batch["hypothesis_proposal_batch_id"]
    assert cluster_link["count"] >= 1


def test_proposal_batch_fingerprint_is_deterministic_except_generated_ids(tmp_path: Path) -> None:
    store = tmp_path / "store"
    cluster_result = _seed_cluster_batch(store)
    cluster_batch_id = cluster_result["batch"]["observation_cluster_batch_id"]

    first_proposals, first_source, first_integrity, first_status = generate_hypothesis_proposals(
        store_root=store,
        observation_cluster_batch_id=cluster_batch_id,
        generated_at="2026-05-19T00:30:00Z",
    )
    second_proposals, second_source, second_integrity, second_status = generate_hypothesis_proposals(
        store_root=store,
        observation_cluster_batch_id=cluster_batch_id,
        generated_at="2026-05-19T00:31:00Z",
    )
    first = build_hypothesis_proposal_batch(
        proposals=first_proposals,
        source_observation_cluster_batch_id=cluster_batch_id,
        latest_integrity_report_id=first_integrity["integrity_report_id"],
        latest_research_os_status_report_id=first_status["research_os_status_report_id"],
        generated_at="2026-05-19T00:30:00Z",
    )
    second = build_hypothesis_proposal_batch(
        proposals=second_proposals,
        source_observation_cluster_batch_id=second_source["observation_cluster_batch_id"],
        latest_integrity_report_id=second_integrity["integrity_report_id"],
        latest_research_os_status_report_id=second_status["research_os_status_report_id"],
        generated_at="2026-05-19T00:31:00Z",
    )

    assert first["hypothesis_proposal_batch_id"] != second["hypothesis_proposal_batch_id"]
    assert first["immutable_hash"] == second["immutable_hash"]
