from __future__ import annotations

from pathlib import Path

import pytest

from ops.aegis.research_lab.research_lab_routes import (
    research_lab_hypothesis_proposal_review_batch_latest_v1,
    research_lab_hypothesis_proposal_review_batch_v1,
    research_lab_hypothesis_proposal_review_batches_v1,
    research_lab_hypothesis_proposal_review_latest_v1,
    research_lab_hypothesis_proposal_review_v1,
    research_lab_hypothesis_proposal_reviews_for_proposal_v1,
    research_lab_hypothesis_proposal_reviews_v1,
)
from research_lab.contracts.schemas import validate_contract
from research_lab.hypotheses.hypothesis_proposal import build_hypothesis_proposal
from research_lab.hypotheses.hypothesis_proposal_review import build_hypothesis_proposal_review
from research_lab.hypotheses.hypothesis_proposal_review_batch import (
    build_hypothesis_proposal_review_batch,
    build_reviews_for_batch,
    write_hypothesis_proposal_review_batch,
)
from research_lab.storage.manifest_io import read_jsonl
from research_lab.tests.test_hypothesis_proposals import _proposal_counts, _seed_cluster_batch
from research_lab.hypotheses.hypothesis_proposal_batch import write_hypothesis_proposal_batch


def _seed_proposal_batch(store: Path) -> dict:
    _seed_cluster_batch(store)
    return write_hypothesis_proposal_batch(store_root=store, generated_at="2026-05-19T00:30:00Z")


def _review_counts(store: Path) -> dict[str, int]:
    counts = _proposal_counts(store)
    counts.update(
        {
            "hypothesis_proposal_reviews": len(list((store / "hypothesis_proposal_reviews").glob("*.json"))) if (store / "hypothesis_proposal_reviews").exists() else 0,
            "hypothesis_proposal_review_batches": len(list((store / "hypothesis_proposal_review_batches").glob("*.json"))) if (store / "hypothesis_proposal_review_batches").exists() else 0,
        }
    )
    return counts


def test_auto_review_latest_packet27_batch(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _seed_proposal_batch(store)

    result = write_hypothesis_proposal_review_batch(store_root=store, generated_at="2026-05-19T00:40:00Z")
    batch = result["batch"]
    reviews = [row["review"] for row in result["reviews"]]

    validate_contract("hypothesis_proposal_review_batch", batch)
    assert batch["review_count"] == 4
    assert batch["review_decision_counts"]["request_data_enablement"] == 1
    assert batch["review_decision_counts"]["defer_until_more_observation"] == 1
    assert batch["review_decision_counts"]["mark_system_research_item"] == 2
    assert batch["review_status_counts"]["recorded"] == 4
    for review in reviews:
        validate_contract("hypothesis_proposal_review", review)
        assert review["actionability_status"] == "non_actionable_research_only"
        assert review["no_lifecycle_mutation_assertion"]["active_hypothesis_created"] is False


def test_blocked_proposal_cannot_be_approved(tmp_path: Path) -> None:
    store = tmp_path / "store"
    result = _seed_proposal_batch(store)
    market_id = next(
        proposal_id
        for proposal_id in result["batch"]["hypothesis_proposal_ids"]
        if "market_behavior" in (store / "hypothesis_proposals" / f"{proposal_id}.json").read_text(encoding="utf-8")
    )

    review_result = write_hypothesis_proposal_review_batch(
        store_root=store,
        proposal_id=market_id,
        decision="approve_for_future_hypothesis_activation",
        rationale="Attempted approval should be blocked.",
        reviewer_id="test",
        generated_at="2026-05-19T00:40:00Z",
    )
    review = review_result["reviews"][0]["review"]

    assert review["review_status"] == "blocked"
    assert any(row.get("blocker_code") == "review_decision_not_allowed" for row in review["blockers"])
    assert review["no_lifecycle_mutation_assertion"]["active_hypothesis_created"] is False


def test_system_research_only_cannot_be_activated_by_default(tmp_path: Path) -> None:
    store = tmp_path / "store"
    result = _seed_proposal_batch(store)
    system_id = next(
        proposal_id
        for proposal_id in result["batch"]["hypothesis_proposal_ids"]
        if "research_store_integrity" in (store / "hypothesis_proposals" / f"{proposal_id}.json").read_text(encoding="utf-8")
    )

    review_result = write_hypothesis_proposal_review_batch(
        store_root=store,
        proposal_id=system_id,
        decision="approve_for_future_hypothesis_activation",
        rationale="System research item cannot be activated by default.",
        reviewer_id="test",
        generated_at="2026-05-19T00:40:00Z",
    )
    review = review_result["reviews"][0]["review"]

    assert review["review_status"] == "blocked"
    assert any(row.get("blocker_code") == "review_decision_not_allowed" for row in review["blockers"])


def test_proposed_for_review_fixture_can_be_marked_future_activation_without_lifecycle_mutation() -> None:
    proposal = build_hypothesis_proposal(
        generated_at="2026-05-19T00:30:00Z",
        proposal_status="proposed_for_review",
        proposal_family="regime_behavior",
        proposal_label="Fixture proposal",
        proposed_hypothesis_statement="Fixture research-only proposal for future review.",
        proposed_research_question="Does the fixture warrant later formal research activation?",
        source_observation_cluster_id="ocl_fixture",
        source_observation_cluster_batch_id="oclb_fixture",
        source_observation_candidate_ids=["obc_fixture"],
        cluster_family="regime_behavior",
        cluster_status="recurring",
        research_priority_score=0.8,
        eligibility_status="eligible_for_review",
        eligibility_checks={"fixture": True},
        blockers=[],
        required_next_evidence=["human review"],
        proposed_test_design={"test_type": "regime_fragility_review", "required_data": ["fixture"], "minimum_observation_count": 1, "event_window": "fixture", "benchmark": "SPY", "expected_artifacts_if_approved": ["research_plan"]},
        proposed_event_window="fixture",
        proposed_universe="fixture",
        proposed_benchmark="SPY",
        latest_integrity_report_id="rsir_fixture",
        latest_research_os_status_report_id="rsos_fixture",
        source_artifact_ids={},
        source_artifact_hashes={},
    )
    review = build_hypothesis_proposal_review(
        proposal=proposal,
        generated_at="2026-05-19T00:40:00Z",
        reviewer_id="test",
        review_decision="approve_for_future_hypothesis_activation",
        review_rationale="Fixture is eligible for a future activation packet only.",
    )

    assert review["review_status"] == "recorded"
    assert review["allowed_next_research_actions"] == ["eligible_for_hypothesis_activation_packet"]
    assert review["no_lifecycle_mutation_assertion"]["active_hypothesis_created"] is False


def test_source_hash_validation_blocks_mismatch(tmp_path: Path) -> None:
    store = tmp_path / "store"
    result = _seed_proposal_batch(store)
    proposal_id = result["batch"]["hypothesis_proposal_ids"][0]
    proposal = __import__("research_lab.storage.manifest_io", fromlist=["read_json"]).read_json(store / "hypothesis_proposals" / f"{proposal_id}.json")

    with pytest.raises(ValueError, match="source_proposal_hash_mismatch"):
        build_hypothesis_proposal_review(
            proposal=proposal,
            generated_at="2026-05-19T00:40:00Z",
            reviewer_id="test",
            review_decision="reject_proposal",
            review_rationale="Mismatch should fail.",
            expected_source_proposal_hash="bad_hash",
        )


def test_append_only_registry_audit_api_determinism_and_no_lifecycle_mutation(tmp_path: Path) -> None:
    store = tmp_path / "store"
    proposal_result = _seed_proposal_batch(store)
    before = _review_counts(store)

    result = write_hypothesis_proposal_review_batch(store_root=store, generated_at="2026-05-19T00:40:00Z")
    batch = result["batch"]
    after = _review_counts(store)

    assert after["active_hypotheses"] == before["active_hypotheses"]
    assert after["hypotheses"] == before["hypotheses"]
    assert after["challengers"] == before["challengers"]
    assert after["sleeves"] == before["sleeves"]
    assert after["paper_trials"] == before["paper_trials"]
    assert after["paper_trial_proposals"] == before["paper_trial_proposals"]
    assert after["candidate_batches"] == before["candidate_batches"]
    assert after["hypothesis_proposal_reviews"] == before["hypothesis_proposal_reviews"] + batch["review_count"]
    assert after["hypothesis_proposal_review_batches"] == before["hypothesis_proposal_review_batches"] + 1
    assert after["audit"] == before["audit"] + batch["review_count"] + 1
    assert read_jsonl(store / "registries" / "hypothesis_proposal_review_batch_registry.json")[-1]["hypothesis_proposal_review_batch_id"] == batch["hypothesis_proposal_review_batch_id"]
    assert len(read_jsonl(store / "registries" / "hypothesis_proposal_review_registry.json")) == batch["review_count"]

    listing = research_lab_hypothesis_proposal_reviews_v1(store_root=store)
    latest_review = research_lab_hypothesis_proposal_review_latest_v1(store_root=store)
    review_detail = research_lab_hypothesis_proposal_review_v1(hypothesis_proposal_review_id=latest_review["hypothesis_proposal_review"]["hypothesis_proposal_review_id"], store_root=store)
    batch_listing = research_lab_hypothesis_proposal_review_batches_v1(store_root=store)
    latest_batch = research_lab_hypothesis_proposal_review_batch_latest_v1(store_root=store)
    batch_detail = research_lab_hypothesis_proposal_review_batch_v1(hypothesis_proposal_review_batch_id=batch["hypothesis_proposal_review_batch_id"], store_root=store)
    proposal_link = research_lab_hypothesis_proposal_reviews_for_proposal_v1(hypothesis_proposal_id=proposal_result["batch"]["hypothesis_proposal_ids"][0], store_root=store)
    assert listing["read_only"] is True
    assert review_detail["read_only"] is True
    assert batch_listing["read_only"] is True
    assert latest_batch["hypothesis_proposal_review_batch"]["hypothesis_proposal_review_batch_id"] == batch["hypothesis_proposal_review_batch_id"]
    assert batch_detail["hypothesis_proposal_review_batch"]["hypothesis_proposal_review_batch_id"] == batch["hypothesis_proposal_review_batch_id"]
    assert proposal_link["count"] >= 1

    first_reviews, first_source = build_reviews_for_batch(store_root=store, source_batch_id=proposal_result["batch"]["hypothesis_proposal_batch_id"], generated_at="2026-05-19T00:41:00Z", reviewer_id="test")
    second_reviews, second_source = build_reviews_for_batch(store_root=store, source_batch_id=proposal_result["batch"]["hypothesis_proposal_batch_id"], generated_at="2026-05-19T00:42:00Z", reviewer_id="test")
    first = build_hypothesis_proposal_review_batch(
        reviews=first_reviews,
        source_hypothesis_proposal_batch_id=first_source["hypothesis_proposal_batch_id"],
        latest_integrity_report_id=first_source["latest_integrity_report_id"],
        latest_research_os_status_report_id=first_source["latest_research_os_status_report_id"],
        generated_at="2026-05-19T00:41:00Z",
        review_run_id="run_a",
    )
    second = build_hypothesis_proposal_review_batch(
        reviews=second_reviews,
        source_hypothesis_proposal_batch_id=second_source["hypothesis_proposal_batch_id"],
        latest_integrity_report_id=second_source["latest_integrity_report_id"],
        latest_research_os_status_report_id=second_source["latest_research_os_status_report_id"],
        generated_at="2026-05-19T00:42:00Z",
        review_run_id="run_b",
    )
    assert first["hypothesis_proposal_review_batch_id"] != second["hypothesis_proposal_review_batch_id"]
    assert first["immutable_hash"] == second["immutable_hash"]

