from __future__ import annotations

import pytest

from research_lab.paper_trials.paper_trial import build_paper_trial
from research_lab.paper_trials.paper_trial_registry import change_paper_trial_status
from research_lab.tests.test_paper_trial_observation import _paper_trial_fixture


def test_same_paper_trial_inputs_produce_same_hash() -> None:
    kwargs = dict(sleeve_id="slv", sleeve_version_id="slvv", hypothesis_id="hyp", dataset_snapshot_id="ds", regime_snapshot_id="rs", cost_model_snapshot_id="cm", source_evidence_package_ids=["b", "a"], threshold=-0.02)
    first = build_paper_trial(**kwargs, created_at="2024-01-01T00:00:00Z")
    second = build_paper_trial(**kwargs, created_at="2024-01-02T00:00:00Z")

    assert first["paper_trial_id"] == second["paper_trial_id"]
    assert first["content_hash"] == second["content_hash"]


def test_invalid_status_transition_fails(tmp_path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)

    with pytest.raises(RuntimeError, match="Invalid paper trial status transition"):
        change_paper_trial_status(trial_id, "completed", store_root=store)

