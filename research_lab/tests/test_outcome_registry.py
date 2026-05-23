from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.outcomes.measurement_runner import run_candidate_outcome_measurement
from research_lab.outcomes.outcome_registry import build_candidate_outcome_summary, load_outcomes
from research_lab.storage.manifest_io import read_jsonl
from research_lab.tests.test_outcome_measurement import _stored_candidate_batch


def test_outcome_registry_appends_and_summary_loads(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id, dataset_id, cost_id = _stored_candidate_batch(store)

    run_candidate_outcome_measurement(
        candidate_batch_id=batch_id,
        dataset_snapshot_id=dataset_id,
        cost_model_snapshot_id=cost_id,
        benchmark_symbol="SPY",
        windows=[1],
        store_root=store,
        actor="pytest",
        allow_json_fallback=True,
    )

    assert load_outcomes(batch_id, store_root=store)
    assert build_candidate_outcome_summary(batch_id, store_root=store)["candidate_count"] == 1
    assert read_jsonl(store / "registries" / "outcome_records.jsonl")
    assert read_jsonl(store / "registries" / "attribution_reports.jsonl")


def test_outcome_artifacts_are_immutable(tmp_path: Path) -> None:
    store = tmp_path / "store"
    batch_id, dataset_id, cost_id = _stored_candidate_batch(store)
    kwargs = {
        "candidate_batch_id": batch_id,
        "dataset_snapshot_id": dataset_id,
        "cost_model_snapshot_id": cost_id,
        "benchmark_symbol": "SPY",
        "windows": [1],
        "store_root": store,
        "actor": "pytest",
        "allow_json_fallback": True,
    }
    run_candidate_outcome_measurement(**kwargs)

    with pytest.raises(FileExistsError):
        run_candidate_outcome_measurement(**kwargs)

