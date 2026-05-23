from __future__ import annotations

from pathlib import Path

import pytest

from ops.aegis.research_lab.research_lab_routes import (
    research_lab_human_review_decision_latest_v1,
    research_lab_human_review_decision_read_model_v1,
    research_lab_human_review_decision_v1,
    research_lab_human_review_decisions_v1,
    research_lab_human_review_dossier_decisions_v1,
)
from research_lab.challengers.challenger_comparison import write_challenger_comparison_report
from research_lab.challengers.challenger_evidence import write_challenger_evidence_batch
from research_lab.challengers.human_review_decision import (
    build_human_review_decision,
    write_human_review_decision,
)
from research_lab.challengers.human_review_dossier import write_human_review_dossier
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash
from research_lab.storage.manifest_io import read_json, read_jsonl, write_json
from research_lab.tests.test_challenger_evidence_batch import _write_fixture


def _write_dossier(store: Path) -> str:
    track_id = _write_fixture(store)
    batch = write_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)["batch"]
    report = write_challenger_comparison_report(challenger_evidence_batch_id=batch["challenger_evidence_batch_id"], store_root=store)["report"]
    dossier = write_human_review_dossier(challenger_comparison_report_id=report["challenger_comparison_report_id"], store_root=store)["dossier"]
    return dossier["human_review_dossier_id"]


def _files(store: Path, rel: str) -> list[str]:
    root = store / rel
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()) if root.exists() else []


def test_allowed_and_rejected_decisions(tmp_path: Path) -> None:
    store = tmp_path / "store"
    dossier_id = _write_dossier(store)

    for decision in ["request_more_challenger_evidence", "reject_challenger", "open_challenger_paper_trial"]:
        payload = build_human_review_decision(
            human_review_dossier_id=dossier_id,
            decision=decision,
            decided_by="david",
            rationale="Human research decision rationale with enough context.",
            decided_at=f"2026-05-19T00:0{len(decision) % 9}:00Z",
            store_root=store,
        )
        validate_contract("human_review_decision", payload)
        assert payload["decision"] == decision

    for decision in ["promote_challenger", "allocate_capital", "execute_trade", "mutate_sleeve"]:
        with pytest.raises(ValueError):
            build_human_review_decision(
                human_review_dossier_id=dossier_id,
                decision=decision,
                decided_by="david",
                rationale="Not allowed.",
                store_root=store,
            )


def test_append_only_second_decision_and_registry(tmp_path: Path) -> None:
    store = tmp_path / "store"
    dossier_id = _write_dossier(store)

    first = write_human_review_decision(
        human_review_dossier_id=dossier_id,
        decision="request_more_challenger_evidence",
        decided_by="david",
        decided_at="2026-05-19T12:00:00Z",
        rationale="Need more regime-specific evidence before opening paper trial.",
        store_root=store,
    )
    second = write_human_review_decision(
        human_review_dossier_id=dossier_id,
        decision="reject_challenger",
        decided_by="david",
        decided_at="2026-05-19T13:00:00Z",
        rationale="Rejected for research review after manual assessment.",
        store_root=store,
    )

    assert first["decision"]["human_review_decision_id"] != second["decision"]["human_review_decision_id"]
    rows = read_jsonl(store / "registries" / "human_review_decision_registry.json")
    assert len(rows) == 2
    assert rows[-1]["human_review_decision_id"] == second["decision"]["human_review_decision_id"]


def test_dossier_integrity_and_no_mutation(tmp_path: Path) -> None:
    store = tmp_path / "store"
    dossier_id = _write_dossier(store)
    dossier_path = store / "human_review_dossiers" / f"{dossier_id}.json"
    dossier_before = read_json(dossier_path)
    sleeve_before = _files(store, "sleeves")
    paper_before = _files(store, "paper_trials")
    candidate_before = _files(store, "candidate_batches")

    result = write_human_review_decision(
        human_review_dossier_id=dossier_id,
        decision="open_challenger_paper_trial",
        decided_by="david",
        rationale="Evidence is complete; record paper-trial intent only, without creating a trial.",
        store_root=store,
    )

    assert result["decision"]["source_dossier_hash"] == dossier_before["content_hash"]
    assert read_json(dossier_path) == dossier_before
    assert _files(store, "sleeves") == sleeve_before
    assert _files(store, "paper_trials") == paper_before
    assert _files(store, "candidate_batches") == candidate_before
    assert result["decision"]["governance_constraints"]["paper_trial_created"] is False


def test_open_paper_trial_requires_complete_evidence(tmp_path: Path) -> None:
    store = tmp_path / "store"
    dossier_id = _write_dossier(store)
    dossier_path = store / "human_review_dossiers" / f"{dossier_id}.json"
    dossier = read_json(dossier_path)
    dossier["review_candidates"][0]["evidence_completeness"]["complete"] = False
    dossier["review_candidates"][0]["confidence_classification"] = "insufficient"
    dossier["content_hash"] = content_hash(dossier, exclude={"generated_at"}, sort_lists=True)
    write_json(dossier_path, dossier, overwrite=True)

    with pytest.raises(RuntimeError):
        build_human_review_decision(
            human_review_dossier_id=dossier_id,
            decision="open_challenger_paper_trial",
            decided_by="david",
            rationale="Attempt must fail because evidence is incomplete.",
            store_root=store,
        )


def test_audit_event_and_api_read_models(tmp_path: Path) -> None:
    store = tmp_path / "store"
    dossier_id = _write_dossier(store)
    result = write_human_review_decision(
        human_review_dossier_id=dossier_id,
        decision="request_more_challenger_evidence",
        decided_by="david",
        rationale="Need more regime-specific evidence before opening paper trial.",
        store_root=store,
    )

    audit_rows = read_jsonl(store / "audit_log" / "audit_events.jsonl")
    assert any(row["action"] == "human_review_decision_recorded" and row["entity_id"] == result["decision"]["human_review_decision_id"] for row in audit_rows)

    listing = research_lab_human_review_decisions_v1(store_root=store)
    latest = research_lab_human_review_decision_latest_v1(human_review_dossier_id=dossier_id, store_root=store)
    detail = research_lab_human_review_decision_v1(human_review_decision_id=result["decision"]["human_review_decision_id"], store_root=store)
    dossier_decisions = research_lab_human_review_dossier_decisions_v1(human_review_dossier_id=dossier_id, store_root=store)
    read_model = research_lab_human_review_decision_read_model_v1(store_root=store)

    assert listing["read_only"] is True
    assert latest["latest_decision"]["human_review_decision_id"] == result["decision"]["human_review_decision_id"]
    assert detail["human_review_decision"]["decision"] == "request_more_challenger_evidence"
    assert dossier_decisions["count"] == 1
    assert read_model["dossiers"][0]["decision_history_count"] == 1


def test_deterministic_hash_and_no_execution_objects(tmp_path: Path) -> None:
    store = tmp_path / "store"
    dossier_id = _write_dossier(store)

    first = build_human_review_decision(
        human_review_dossier_id=dossier_id,
        decision="reject_challenger",
        decided_by="david",
        rationale="Rejected after human review of research evidence.",
        decided_at="2026-05-19T12:00:00Z",
        store_root=store,
    )
    second = build_human_review_decision(
        human_review_dossier_id=dossier_id,
        decision="reject_challenger",
        decided_by="david",
        rationale="Rejected after human review of research evidence.",
        decided_at="2026-05-19T12:00:00Z",
        store_root=store,
    )

    assert first["immutable_hash"] == second["immutable_hash"]
    assert first["human_review_decision_id"] == second["human_review_decision_id"]
    text = str(first).lower()
    assert "broker_order" not in text
    assert "capital_allocation_object" not in text
    assert "execution_order" not in text
    assert first["governance_constraints"]["broker_execution_allowed"] is False
    assert first["governance_constraints"]["capital_allocation_allowed"] is False
