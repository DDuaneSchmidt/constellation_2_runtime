from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.candidates.candidate_generator import generate_drop_reversion_candidates
from research_lab.candidates.candidate_registry import candidate_ledger_summary, candidates_with_latest_status, store_candidate_batch
from research_lab.candidates.operator_decision import append_operator_decision, build_operator_decision
from research_lab.tests.test_candidate_generation import _candidate_context


def _store_batch(store: Path) -> str:
    evidence_id, dataset_id, regime_id, cost_id = _candidate_context(store)
    result = generate_drop_reversion_candidates(
        source_evidence_package_id=evidence_id,
        dataset_snapshot_id=dataset_id,
        regime_snapshot_id=regime_id,
        cost_model_snapshot_id=cost_id,
        as_of_date="2024-01-02",
        threshold=-0.01,
        store_root=store,
    )
    store_candidate_batch(
        candidate_batch=result["candidate_batch"],
        candidates=result["candidates"],
        generation_summary=result["generation_summary"],
        store_root=store,
        actor="pytest",
        allow_json_fallback=True,
    )
    return result["candidate_batch"]["candidate_batch_id"]


def test_candidate_batch_is_immutable(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id = _store_batch(store)
    evidence_id, dataset_id, regime_id, cost_id = _candidate_context(tmp_path / "other" / "store")
    result = generate_drop_reversion_candidates(
        source_evidence_package_id=evidence_id,
        dataset_snapshot_id=dataset_id,
        regime_snapshot_id=regime_id,
        cost_model_snapshot_id=cost_id,
        as_of_date="2024-01-02",
        threshold=-0.01,
        store_root=tmp_path / "other" / "store",
    )
    result["candidate_batch"]["candidate_batch_id"] = batch_id

    with pytest.raises(FileExistsError):
        store_candidate_batch(
            candidate_batch=result["candidate_batch"],
            candidates=result["candidates"],
            generation_summary=result["generation_summary"],
            store_root=store,
            actor="pytest",
            allow_json_fallback=True,
        )


def test_candidate_ledger_summary_and_derived_status(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id = _store_batch(store)
    candidate = candidates_with_latest_status(batch_id, store_root=store)[0]
    decision = build_operator_decision(
        candidate_id=candidate["candidate_id"],
        candidate_batch_id=batch_id,
        decision="ignore",
        decision_reason="research observation only",
        decided_by="operator",
    )
    append_operator_decision(decision, store_root=store, actor="operator")

    rows = candidates_with_latest_status(batch_id, store_root=store)
    summary = candidate_ledger_summary(batch_id, store_root=store)

    assert rows[0]["derived_candidate_status"] == "ignored"
    assert summary["decision_count"] == 1
    assert summary["status_counts"] == {"ignored": 1}

