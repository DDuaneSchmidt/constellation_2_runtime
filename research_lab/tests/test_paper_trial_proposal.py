from __future__ import annotations

from pathlib import Path

from ops.aegis.research_lab.research_lab_routes import (
    research_lab_human_review_decision_paper_trial_proposals_v1,
    research_lab_paper_trial_proposal_latest_v1,
    research_lab_paper_trial_proposal_v1,
    research_lab_paper_trial_proposals_v1,
)
from research_lab.challengers.human_review_decision import write_human_review_decision
from research_lab.challengers.human_review_dossier import load_human_review_dossier
from research_lab.integrity.research_store_integrity import write_research_store_integrity_report
from research_lab.paper_trials.paper_trial_proposal import build_paper_trial_proposal, write_paper_trial_proposal
from research_lab.status.research_os_status import write_research_os_status_report
from research_lab.storage.hashing import content_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.tests.test_research_store_integrity import _fixture_store


def _counts(store: Path) -> dict[str, int]:
    return {
        "paper_trials": len(list((store / "paper_trials").rglob("*.json"))) if (store / "paper_trials").exists() else 0,
        "candidates": len(list((store / "candidate_batches").rglob("*.json"))) if (store / "candidate_batches").exists() else 0,
        "sleeves": len(list((store / "sleeves").rglob("*.json"))) if (store / "sleeves").exists() else 0,
        "proposals": len(list((store / "paper_trial_proposals").glob("*.json"))) if (store / "paper_trial_proposals").exists() else 0,
        "audit": len(read_jsonl(store / "audit_log" / "audit_events.jsonl")),
    }


def _candidate_id(store: Path, dossier_id: str) -> str:
    dossier = load_human_review_dossier(dossier_id, store_root=store)
    return str((dossier.get("review_candidates") or [{}])[0].get("challenger_hypothesis_id") or "")


def _open_decision_fixture(store: Path, *, red: bool = False) -> dict[str, str]:
    ids = _fixture_store(store)
    candidate_id = _candidate_id(store, ids["dossier_id"])
    decision = write_human_review_decision(
        human_review_dossier_id=ids["dossier_id"],
        challenger_id=candidate_id,
        decision="open_challenger_paper_trial",
        decided_by="david",
        rationale="Evidence is complete enough to record intent for a paper-trial proposal gate only.",
        decided_at="2026-05-19T13:00:00Z",
        store_root=store,
    )["decision"]
    if red:
        paper_trial_id = "ptr_duplicate_fixture"
        paper_trial = {
            "paper_trial_id": paper_trial_id,
            "sleeve_id": "slv_etf_drop_reversion_v1",
            "sleeve_version_id": "slvv_slv_etf_drop_reversion_v1_v1_1c36b58d9b",
            "hypothesis_id": "hyp_fixture",
            "dataset_snapshot_id": "ds_fixture",
            "regime_snapshot_id": "rs_fixture",
            "cost_model_snapshot_id": "cm_fixture",
            "source_evidence_package_ids": [],
            "candidate_generator_name": "fixture",
            "candidate_generator_version": "v1",
            "ranking_policy_version": "fixture",
            "threshold": 0.0,
            "observation_frequency": "weekly",
            "outcome_windows": [5],
            "status": "active",
            "started_at": "2026-05-19T00:00:00Z",
            "created_at": "2026-05-19T00:00:00Z",
            "created_by": "test",
            "schema_version": "paper_trial.v1",
            "content_hash": "",
        }
        paper_trial["content_hash"] = content_hash(paper_trial, exclude={"content_hash"}, sort_lists=True)
        write_json(store / "paper_trials" / paper_trial_id / "paper_trial.json", paper_trial, overwrite=False)
        row = {"paper_trial_id": paper_trial_id}
        append_jsonl(store / "registries" / "paper_trials.jsonl", row)
        append_jsonl(store / "registries" / "paper_trials.jsonl", row)
    write_research_store_integrity_report(store_root=store)
    write_research_os_status_report(store_root=store, generated_at="2026-05-19T14:00:00Z")
    ids["open_decision_id"] = decision["human_review_decision_id"]
    ids["open_challenger_id"] = decision["challenger_id"]
    return ids


def test_reject_decision_blocks_proposal_and_creates_no_paper_trial(tmp_path: Path) -> None:
    store = tmp_path / "store"
    ids = _fixture_store(store)
    candidate_id = _candidate_id(store, ids["dossier_id"])
    decision = write_human_review_decision(
        human_review_dossier_id=ids["dossier_id"],
        challenger_id=candidate_id,
        decision="reject_challenger",
        decided_by="david",
        rationale="Reject for proposal gate test only.",
        decided_at="2026-05-19T13:00:00Z",
        store_root=store,
    )["decision"]
    write_research_store_integrity_report(store_root=store)
    write_research_os_status_report(store_root=store, generated_at="2026-05-19T14:00:00Z")
    before = _counts(store)

    proposal = build_paper_trial_proposal(human_review_decision_id=decision["human_review_decision_id"], store_root=store)

    assert proposal["proposal_status"] == "blocked"
    assert "decision_not_open_challenger_paper_trial" in [row["blocker_code"] for row in proposal["blockers"]]
    assert _counts(store)["paper_trials"] == before["paper_trials"]


def test_open_decision_with_red_status_blocks_and_override_needs_review(tmp_path: Path) -> None:
    store = tmp_path / "store"
    ids = _open_decision_fixture(store, red=True)

    blocked = build_paper_trial_proposal(human_review_decision_id=ids["open_decision_id"], store_root=store)
    override = build_paper_trial_proposal(
        human_review_decision_id=ids["open_decision_id"],
        allow_red_status=True,
        override_reason="Human acknowledges RED integrity status for proposal review only.",
        store_root=store,
    )

    assert blocked["proposal_status"] == "blocked"
    codes = [row["blocker_code"] for row in blocked["blockers"]]
    assert "research_os_status_red" in codes
    assert "integrity_status_fail" in codes
    assert "duplicate_paper_trial_registry_entry" in codes
    assert override["proposal_status"] == "needs_review"
    assert [row["blocker_code"] for row in override["blockers"]]


def test_complete_evidence_and_non_red_status_is_eligible(tmp_path: Path) -> None:
    store = tmp_path / "store"
    ids = _open_decision_fixture(store, red=False)

    proposal = build_paper_trial_proposal(human_review_decision_id=ids["open_decision_id"], store_root=store)

    assert proposal["proposal_status"] == "eligible"
    assert proposal["eligibility_status"] == "passed"
    assert proposal["blockers"] == []


def test_missing_evidence_and_source_hash_mismatch_block(tmp_path: Path) -> None:
    store = tmp_path / "store"
    ids = _open_decision_fixture(store, red=False)
    dossier_path = store / "human_review_dossiers" / f"{ids['dossier_id']}.json"
    dossier = read_json(dossier_path)
    dossier["review_candidates"][0]["artifact_lineage"]["event_study_evidence_id"] = ""
    write_json(dossier_path, dossier, overwrite=True)

    proposal = build_paper_trial_proposal(human_review_decision_id=ids["open_decision_id"], store_root=store)
    codes = [row["blocker_code"] for row in proposal["blockers"]]

    assert proposal["proposal_status"] == "blocked"
    assert "missing_event_study_evidence_id" in codes
    assert "source_dossier_hash_mismatch" in codes


def test_proposal_persistence_registry_audit_api_and_no_lifecycle_mutation(tmp_path: Path) -> None:
    store = tmp_path / "store"
    ids = _open_decision_fixture(store, red=True)
    before = _counts(store)

    first = write_paper_trial_proposal(human_review_decision_id=ids["open_decision_id"], store_root=store, generated_at="2026-05-19T15:00:00Z")
    second = write_paper_trial_proposal(human_review_decision_id=ids["open_decision_id"], store_root=store, generated_at="2026-05-19T15:00:01Z")
    after = _counts(store)

    assert after["paper_trials"] == before["paper_trials"]
    assert after["candidates"] == before["candidates"]
    assert after["sleeves"] == before["sleeves"]
    assert after["proposals"] == before.get("proposals", 0) + 2
    assert after["audit"] == before["audit"] + 2
    assert first["proposal"]["immutable_hash"] == second["proposal"]["immutable_hash"]
    assert first["proposal"]["paper_trial_proposal_id"] != second["proposal"]["paper_trial_proposal_id"]
    assert read_jsonl(store / "registries" / "paper_trial_proposal_registry.json")[-1]["paper_trial_proposal_id"] == second["proposal"]["paper_trial_proposal_id"]
    assert second["audit_event"]["action"] == "paper_trial_proposal_generated"

    listing = research_lab_paper_trial_proposals_v1(store_root=store)
    latest = research_lab_paper_trial_proposal_latest_v1(store_root=store)
    detail = research_lab_paper_trial_proposal_v1(paper_trial_proposal_id=second["proposal"]["paper_trial_proposal_id"], store_root=store)
    by_decision = research_lab_human_review_decision_paper_trial_proposals_v1(human_review_decision_id=ids["open_decision_id"], store_root=store)
    assert listing["read_only"] is True
    assert latest["paper_trial_proposal"]["paper_trial_proposal_id"] == second["proposal"]["paper_trial_proposal_id"]
    assert detail["paper_trial_proposal"]["paper_trial_proposal_id"] == second["proposal"]["paper_trial_proposal_id"]
    assert by_decision["count"] == 2

