from __future__ import annotations

from pathlib import Path

from ops.aegis.research_lab.research_lab_routes import (
    research_lab_hypothesis_intake_batch_latest_v1,
    research_lab_hypothesis_intake_batch_v1,
    research_lab_hypothesis_intake_batches_v1,
    research_lab_hypothesis_intake_decision_latest_v1,
    research_lab_hypothesis_intake_decision_v1,
    research_lab_hypothesis_intake_decisions_v1,
    research_lab_research_hypothesis_latest_v1,
    research_lab_research_hypothesis_v1,
    research_lab_research_hypotheses_v1,
)
from research_lab.contracts.schemas import validate_contract
from research_lab.hypotheses.hypothesis_intake import (
    build_hypothesis_intake_for_review_id,
    latest_hypothesis_intake_decision,
)
from research_lab.hypotheses.hypothesis_intake_batch import (
    build_hypothesis_intake_batch,
    build_intake_for_review_batch,
    write_hypothesis_intake_batch,
)
from research_lab.hypotheses.hypothesis_proposal import build_hypothesis_proposal, hypothesis_proposal_path, hypothesis_proposal_registry_path
from research_lab.hypotheses.hypothesis_proposal_batch import build_hypothesis_proposal_batch, hypothesis_proposal_batch_path, hypothesis_proposal_batch_registry_path
from research_lab.hypotheses.hypothesis_proposal_review import build_hypothesis_proposal_review, hypothesis_proposal_review_path, hypothesis_proposal_review_registry_path
from research_lab.hypotheses.hypothesis_proposal_review_batch import (
    build_hypothesis_proposal_review_batch,
    hypothesis_proposal_review_batch_path,
    hypothesis_proposal_review_batch_registry_path,
    write_hypothesis_proposal_review_batch,
)
from research_lab.hypotheses.research_hypothesis import latest_research_hypothesis
from research_lab.storage.hashing import content_hash
from research_lab.storage.manifest_io import append_jsonl, read_jsonl, write_json
from research_lab.tests.test_hypothesis_proposal_reviews import _review_counts, _seed_proposal_batch
from research_lab.tests.test_hypothesis_proposals import _seed_cluster_batch
from research_lab.observations.observation_cluster_batch import latest_observation_cluster_batch
from research_lab.observations.observation_cluster import load_observation_cluster


def _intake_counts(store: Path) -> dict[str, int]:
    counts = _review_counts(store)
    counts.update(
        {
            "hypothesis_intake_decisions": len(list((store / "hypothesis_intake_decisions").glob("*.json"))) if (store / "hypothesis_intake_decisions").exists() else 0,
            "hypothesis_intake_batches": len(list((store / "hypothesis_intake_batches").glob("*.json"))) if (store / "hypothesis_intake_batches").exists() else 0,
            "research_hypotheses": len(list((store / "research_hypotheses").glob("*.json"))) if (store / "research_hypotheses").exists() else 0,
        }
    )
    return counts


def _write_clean_status(store: Path) -> tuple[str, str]:
    integrity_id = "rsir_clean_fixture"
    integrity = {
        "integrity_report_id": integrity_id,
        "generated_at": "2026-05-20T00:00:00Z",
        "overall_status": "PASS",
        "missing_references": [],
        "hash_mismatches": [],
        "schema_validation_failures": [],
        "lineage_breaks": [],
        "duplicate_registry_entries": [],
        "mutation_boundary_violations": [],
        "warnings": [],
        "unresolved_blockers": [],
        "immutable_hash": "clean_integrity_hash_123456",
    }
    write_json(store / "integrity_reports" / f"{integrity_id}.json", integrity, overwrite=False)
    append_jsonl(store / "registries" / "integrity_report_registry.json", {"integrity_report_id": integrity_id, "generated_at": integrity["generated_at"], "overall_status": "PASS", "error_count": 0, "warning_count": 0, "immutable_hash": integrity["immutable_hash"]})
    status_id = "rsos_clean_fixture"
    status = {
        "research_os_status_report_id": status_id,
        "generated_at": "2026-05-20T00:00:01Z",
        "overall_status": "GREEN",
        "readiness_level": "research_ready",
        "integrity_status": "PASS",
        "immutable_hash": "clean_status_hash_123456",
    }
    write_json(store / "status_reports" / f"{status_id}.json", status, overwrite=False)
    append_jsonl(store / "registries" / "research_os_status_report_registry.json", {"research_os_status_report_id": status_id, "generated_at": status["generated_at"], "overall_status": "GREEN", "readiness_level": "research_ready", "latest_integrity_report_id": integrity_id, "immutable_hash": status["immutable_hash"]})
    return integrity_id, status_id


def _write_approved_fixture(store: Path, *, proposal_hash_override: str | None = None) -> dict:
    cluster_batch = _seed_cluster_batch(store)["batch"]
    cluster = load_observation_cluster(cluster_batch["observation_cluster_ids"][0], store_root=store)
    integrity_id, status_id = _write_clean_status(store)
    proposal = build_hypothesis_proposal(
        generated_at="2026-05-20T00:10:00Z",
        proposal_status="proposed_for_review",
        proposal_family="regime_behavior",
        proposal_label="Fixture inactive hypothesis intake",
        proposed_hypothesis_statement="Fixture research-only proposal for inactive hypothesis intake.",
        proposed_research_question="Does this fixture merit later formal research as an inactive hypothesis?",
        source_observation_cluster_id=cluster["observation_cluster_id"],
        source_observation_cluster_batch_id=cluster_batch["observation_cluster_batch_id"],
        source_observation_candidate_ids=cluster["observation_candidate_ids"],
        cluster_family=cluster["cluster_family"],
        cluster_status=cluster["cluster_status"],
        research_priority_score=0.85,
        eligibility_status="eligible_for_review",
        eligibility_checks={"fixture": True},
        blockers=[],
        required_next_evidence=["human review"],
        proposed_test_design={"test_type": "regime_fragility_review", "required_data": ["observation_cluster"], "minimum_observation_count": 1, "event_window": "fixture", "benchmark": "SPY", "expected_artifacts_if_approved": ["research_plan"]},
        proposed_event_window="fixture",
        proposed_universe="fixture_universe",
        proposed_benchmark="SPY",
        latest_integrity_report_id=integrity_id,
        latest_research_os_status_report_id=status_id,
        source_artifact_ids={"source_observation_cluster_batch_id": cluster_batch["observation_cluster_batch_id"]},
        source_artifact_hashes={"source_cluster_hash": cluster["immutable_hash"]},
    )
    write_json(hypothesis_proposal_path(store, proposal["hypothesis_proposal_id"]), proposal, overwrite=False)
    append_jsonl(hypothesis_proposal_registry_path(store), {"hypothesis_proposal_id": proposal["hypothesis_proposal_id"], "generated_at": proposal["generated_at"], "proposal_status": proposal["proposal_status"], "proposal_family": proposal["proposal_family"], "source_observation_cluster_id": proposal["source_observation_cluster_id"], "source_observation_cluster_batch_id": proposal["source_observation_cluster_batch_id"], "latest_integrity_report_id": integrity_id, "latest_research_os_status_report_id": status_id, "immutable_hash": proposal["immutable_hash"]})
    proposal_batch = build_hypothesis_proposal_batch(proposals=[proposal], source_observation_cluster_batch_id=cluster_batch["observation_cluster_batch_id"], latest_integrity_report_id=integrity_id, latest_research_os_status_report_id=status_id, generated_at="2026-05-20T00:11:00Z")
    write_json(hypothesis_proposal_batch_path(store, proposal_batch["hypothesis_proposal_batch_id"]), proposal_batch, overwrite=False)
    append_jsonl(hypothesis_proposal_batch_registry_path(store), {"hypothesis_proposal_batch_id": proposal_batch["hypothesis_proposal_batch_id"], "generated_at": proposal_batch["generated_at"], "proposal_engine_version": proposal_batch["proposal_engine_version"], "source_observation_cluster_batch_id": proposal_batch["source_observation_cluster_batch_id"], "proposal_count": proposal_batch["proposal_count"], "blocked_count": proposal_batch["blocked_count"], "proposed_for_review_count": proposal_batch["proposed_for_review_count"], "system_research_only_count": proposal_batch["system_research_only_count"], "immutable_hash": proposal_batch["immutable_hash"]})
    review = build_hypothesis_proposal_review(
        proposal=proposal | {"source_hypothesis_proposal_batch_id": proposal_batch["hypothesis_proposal_batch_id"]},
        generated_at="2026-05-20T00:12:00Z",
        reviewer_id="test",
        review_decision="approve_for_future_hypothesis_activation",
        review_rationale="Fixture approval for inactive research hypothesis intake only.",
    )
    if proposal_hash_override:
        review["source_proposal_hash"] = proposal_hash_override
        review["immutable_hash"] = content_hash(review, exclude={"hypothesis_proposal_review_id", "generated_at", "immutable_hash", "content_hash"}, sort_lists=True)
        review["content_hash"] = review["immutable_hash"]
    write_json(hypothesis_proposal_review_path(store, review["hypothesis_proposal_review_id"]), review, overwrite=False)
    append_jsonl(hypothesis_proposal_review_registry_path(store), {"hypothesis_proposal_review_id": review["hypothesis_proposal_review_id"], "generated_at": review["generated_at"], "hypothesis_proposal_id": review["hypothesis_proposal_id"], "hypothesis_proposal_batch_id": review["hypothesis_proposal_batch_id"], "review_decision": review["review_decision"], "review_status": review["review_status"], "reviewer_id": review["reviewer_id"], "latest_integrity_report_id": integrity_id, "latest_research_os_status_report_id": status_id, "immutable_hash": review["immutable_hash"]})
    review_batch = build_hypothesis_proposal_review_batch(reviews=[review], source_hypothesis_proposal_batch_id=proposal_batch["hypothesis_proposal_batch_id"], latest_integrity_report_id=integrity_id, latest_research_os_status_report_id=status_id, generated_at="2026-05-20T00:13:00Z", review_run_id="fixture_review_run")
    write_json(hypothesis_proposal_review_batch_path(store, review_batch["hypothesis_proposal_review_batch_id"]), review_batch, overwrite=False)
    append_jsonl(hypothesis_proposal_review_batch_registry_path(store), {"hypothesis_proposal_review_batch_id": review_batch["hypothesis_proposal_review_batch_id"], "generated_at": review_batch["generated_at"], "review_run_id": review_batch["review_run_id"], "review_version": review_batch["review_version"], "source_hypothesis_proposal_batch_id": review_batch["source_hypothesis_proposal_batch_id"], "review_count": review_batch["review_count"], "immutable_hash": review_batch["immutable_hash"]})
    return {"proposal": proposal, "review": review, "review_batch": review_batch}


def test_current_packet28_reviews_create_zero_hypotheses(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _seed_proposal_batch(store)
    write_hypothesis_proposal_review_batch(store_root=store, generated_at="2026-05-19T00:40:00Z")

    result = write_hypothesis_intake_batch(store_root=store, generated_at="2026-05-19T00:50:00Z")
    batch = result["batch"]

    validate_contract("hypothesis_intake_batch", batch)
    assert batch["intake_count"] == 4
    assert batch["accepted_count"] == 0
    assert batch["created_research_hypothesis_ids"] == []
    assert not list((store / "research_hypotheses").glob("*.json"))
    for row in result["decisions"]:
        decision = row["decision"]
        validate_contract("hypothesis_intake_decision", decision)
        assert decision["created_research_hypothesis_id"] is None
        assert decision["no_lifecycle_mutation_assertion"]["active_hypothesis_created"] is False


def test_approved_clean_fixture_creates_inactive_research_hypothesis(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _write_approved_fixture(store)

    result = write_hypothesis_intake_batch(store_root=store, generated_at="2026-05-20T00:20:00Z")
    batch = result["batch"]
    hypothesis = result["research_hypotheses"][0]["research_hypothesis"]

    assert batch["accepted_count"] == 1
    assert batch["created_research_hypothesis_ids"] == [hypothesis["research_hypothesis_id"]]
    assert hypothesis["hypothesis_status"] == "inactive_research_only"
    assert hypothesis["no_challenger_created_assertion"] is True
    assert hypothesis["no_sleeve_created_assertion"] is True
    assert hypothesis["no_capital_allocation_assertion"] is True
    validate_contract("research_hypothesis", hypothesis)
    assert latest_research_hypothesis(store_root=store)["research_hypothesis_id"] == hypothesis["research_hypothesis_id"]


def test_red_status_and_integrity_fail_block_intake(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _seed_proposal_batch(store)
    review_result = write_hypothesis_proposal_review_batch(store_root=store, generated_at="2026-05-19T00:40:00Z")
    review_id = review_result["batch"]["hypothesis_proposal_review_ids"][0]

    decision, hypothesis = build_hypothesis_intake_for_review_id(hypothesis_proposal_review_id=review_id, store_root=store, generated_at="2026-05-19T00:50:00Z")

    assert hypothesis is None
    assert decision["intake_status"] in {"blocked", "deferred"}
    codes = {row["blocker_code"] for row in decision["blockers"]}
    assert "research_os_status_red" in codes
    assert "integrity_status_fail" in codes


def test_source_hash_mismatch_blocks_intake(tmp_path: Path) -> None:
    store = tmp_path / "store"
    fixture = _write_approved_fixture(store, proposal_hash_override="bad_source_hash_123456")

    result = write_hypothesis_intake_batch(store_root=store, source_batch_id=fixture["review_batch"]["hypothesis_proposal_review_batch_id"], generated_at="2026-05-20T00:20:00Z")
    decision = result["decisions"][0]["decision"]

    assert decision["intake_status"] == "blocked"
    assert any(row["blocker_code"] == "source_proposal_hash_mismatch" for row in decision["blockers"])
    assert result["batch"]["created_research_hypothesis_ids"] == []


def test_append_only_registry_audit_determinism_and_no_lifecycle_mutation(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _seed_proposal_batch(store)
    review_result = write_hypothesis_proposal_review_batch(store_root=store, generated_at="2026-05-19T00:40:00Z")
    before = _intake_counts(store)

    result = write_hypothesis_intake_batch(store_root=store, generated_at="2026-05-19T00:50:00Z")
    batch = result["batch"]
    after = _intake_counts(store)

    assert after["active_hypotheses"] == before["active_hypotheses"]
    assert after["hypotheses"] == before["hypotheses"]
    assert after["challengers"] == before["challengers"]
    assert after["sleeves"] == before["sleeves"]
    assert after["paper_trials"] == before["paper_trials"]
    assert after["paper_trial_proposals"] == before["paper_trial_proposals"]
    assert after["candidate_batches"] == before["candidate_batches"]
    assert after["research_hypotheses"] == before["research_hypotheses"]
    assert after["hypothesis_intake_decisions"] == before["hypothesis_intake_decisions"] + batch["intake_count"]
    assert after["hypothesis_intake_batches"] == before["hypothesis_intake_batches"] + 1
    assert after["audit"] == before["audit"] + batch["intake_count"] + 1
    assert read_jsonl(store / "registries" / "hypothesis_intake_batch_registry.json")[-1]["hypothesis_intake_batch_id"] == batch["hypothesis_intake_batch_id"]
    assert len(read_jsonl(store / "registries" / "hypothesis_intake_decision_registry.json")) == batch["intake_count"]
    assert latest_hypothesis_intake_decision(store_root=store)["hypothesis_intake_decision_id"] == batch["hypothesis_intake_decision_ids"][-1]

    first_decisions, _, source = build_intake_for_review_batch(store_root=store, source_batch_id=review_result["batch"]["hypothesis_proposal_review_batch_id"], generated_at="2026-05-19T00:51:00Z")
    second_decisions, _, _ = build_intake_for_review_batch(store_root=store, source_batch_id=review_result["batch"]["hypothesis_proposal_review_batch_id"], generated_at="2026-05-19T00:52:00Z")
    first = build_hypothesis_intake_batch(decisions=first_decisions, source_hypothesis_proposal_review_batch_id=source["hypothesis_proposal_review_batch_id"], created_research_hypothesis_ids=[], latest_integrity_report_id=first_decisions[0]["latest_integrity_report_id"], latest_research_os_status_report_id=first_decisions[0]["latest_research_os_status_report_id"], generated_at="2026-05-19T00:51:00Z", intake_run_id="run_a")
    second = build_hypothesis_intake_batch(decisions=second_decisions, source_hypothesis_proposal_review_batch_id=source["hypothesis_proposal_review_batch_id"], created_research_hypothesis_ids=[], latest_integrity_report_id=second_decisions[0]["latest_integrity_report_id"], latest_research_os_status_report_id=second_decisions[0]["latest_research_os_status_report_id"], generated_at="2026-05-19T00:52:00Z", intake_run_id="run_b")
    assert first["hypothesis_intake_batch_id"] != second["hypothesis_intake_batch_id"]
    assert first["immutable_hash"] == second["immutable_hash"]


def test_api_safe_empty_and_intake_routes(tmp_path: Path) -> None:
    empty_store = tmp_path / "empty_store"
    assert research_lab_hypothesis_intake_decisions_v1(store_root=empty_store)["count"] == 0
    assert research_lab_hypothesis_intake_decision_latest_v1(store_root=empty_store)["ok"] is False
    assert research_lab_hypothesis_intake_batches_v1(store_root=empty_store)["count"] == 0
    assert research_lab_hypothesis_intake_batch_latest_v1(store_root=empty_store)["ok"] is False
    assert research_lab_research_hypotheses_v1(store_root=empty_store)["count"] == 0
    assert research_lab_research_hypothesis_latest_v1(store_root=empty_store)["ok"] is False

    store = tmp_path / "store"
    _seed_proposal_batch(store)
    write_hypothesis_proposal_review_batch(store_root=store, generated_at="2026-05-19T00:40:00Z")
    result = write_hypothesis_intake_batch(store_root=store, generated_at="2026-05-19T00:50:00Z")
    batch = result["batch"]
    decision_id = batch["hypothesis_intake_decision_ids"][0]

    listing = research_lab_hypothesis_intake_decisions_v1(store_root=store)
    latest_decision = research_lab_hypothesis_intake_decision_latest_v1(store_root=store)
    decision_detail = research_lab_hypothesis_intake_decision_v1(hypothesis_intake_decision_id=decision_id, store_root=store)
    batch_listing = research_lab_hypothesis_intake_batches_v1(store_root=store)
    latest_batch = research_lab_hypothesis_intake_batch_latest_v1(store_root=store)
    batch_detail = research_lab_hypothesis_intake_batch_v1(hypothesis_intake_batch_id=batch["hypothesis_intake_batch_id"], store_root=store)
    hypotheses = research_lab_research_hypotheses_v1(store_root=store)

    assert listing["read_only"] is True
    assert latest_decision["hypothesis_intake_decision"]["hypothesis_intake_decision_id"] == batch["hypothesis_intake_decision_ids"][-1]
    assert decision_detail["hypothesis_intake_decision"]["hypothesis_intake_decision_id"] == decision_id
    assert batch_listing["read_only"] is True
    assert latest_batch["hypothesis_intake_batch"]["hypothesis_intake_batch_id"] == batch["hypothesis_intake_batch_id"]
    assert batch_detail["hypothesis_intake_batch"]["hypothesis_intake_batch_id"] == batch["hypothesis_intake_batch_id"]
    assert hypotheses["count"] == 0

    accepted_store = tmp_path / "accepted_store"
    _write_approved_fixture(accepted_store)
    accepted = write_hypothesis_intake_batch(store_root=accepted_store, generated_at="2026-05-20T00:20:00Z")
    hypothesis_id = accepted["batch"]["created_research_hypothesis_ids"][0]
    latest_hypothesis = research_lab_research_hypothesis_latest_v1(store_root=accepted_store)
    hypothesis_detail = research_lab_research_hypothesis_v1(research_hypothesis_id=hypothesis_id, store_root=accepted_store)
    assert latest_hypothesis["research_hypothesis"]["research_hypothesis_id"] == hypothesis_id
    assert hypothesis_detail["research_hypothesis"]["hypothesis_status"] == "inactive_research_only"
