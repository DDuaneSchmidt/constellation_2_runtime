from __future__ import annotations

from pathlib import Path

from research_lab.candidates.candidate_generator import generate_drop_reversion_candidates
from research_lab.tests.test_candidate_generation import _candidate_context


def test_candidate_generation_is_deterministic(tmp_path: Path) -> None:
    store_a = tmp_path / "a" / "store"
    store_b = tmp_path / "b" / "store"
    args_a = _candidate_context(store_a)
    args_b = _candidate_context(store_b)

    result_a = generate_drop_reversion_candidates(
        source_evidence_package_id=args_a[0],
        dataset_snapshot_id=args_a[1],
        regime_snapshot_id=args_a[2],
        cost_model_snapshot_id=args_a[3],
        as_of_date="2024-01-02",
        threshold=-0.01,
        store_root=store_a,
    )
    result_b = generate_drop_reversion_candidates(
        source_evidence_package_id=args_b[0],
        dataset_snapshot_id=args_b[1],
        regime_snapshot_id=args_b[2],
        cost_model_snapshot_id=args_b[3],
        as_of_date="2024-01-02",
        threshold=-0.01,
        store_root=store_b,
    )

    assert result_a["candidate_batch"]["candidate_batch_id"] == result_b["candidate_batch"]["candidate_batch_id"]
    assert result_a["candidates"][0]["candidate_id"] == result_b["candidates"][0]["candidate_id"]

