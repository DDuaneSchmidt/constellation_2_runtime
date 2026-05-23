from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.paper_trials.observation import record_paper_observation, validate_paper_trial_observation
from research_lab.paper_trials.paper_trial import build_paper_trial
from research_lab.paper_trials.paper_trial_registry import change_paper_trial_status, store_paper_trial
from research_lab.sleeves.sleeve_definition import build_sleeve_definition, build_sleeve_version
from research_lab.sleeves.sleeve_registry import store_sleeve_definition, store_sleeve_version
from research_lab.tests.test_candidate_generation import _candidate_context
from research_lab.tests.test_sleeve_versioning import _write_refs


def _paper_trial_fixture(store: Path) -> str:
    evidence_id, dataset_id, regime_id, cost_id = _candidate_context(store)
    _write_refs(store)
    store_sleeve_definition(build_sleeve_definition(sleeve_id="slv_fixture", name="Fixture", hypothesis_id="hyp_fixture", created_at="2024-01-01T00:00:00Z"), store_root=store)
    version = build_sleeve_version(sleeve_id="slv_fixture", version="v1", hypothesis_id="hyp_fixture", event_study_evidence_package_id="ev_event", backtest_evidence_package_id=evidence_id, candidate_batch_id="cb_fixture", dataset_snapshot_id=dataset_id, regime_snapshot_id=regime_id, cost_model_snapshot_id=cost_id, created_at="2024-01-01T00:00:00Z")
    store_sleeve_version(version, store_root=store)
    trial = build_paper_trial(sleeve_id="slv_fixture", sleeve_version_id=version["sleeve_version_id"], hypothesis_id="hyp_fixture", dataset_snapshot_id=dataset_id, regime_snapshot_id=regime_id, cost_model_snapshot_id=cost_id, source_evidence_package_ids=["ev_event", evidence_id], threshold=-0.90, created_at="2024-01-01T00:00:00Z")
    store_paper_trial(trial, store_root=store)
    return trial["paper_trial_id"]


def test_observation_requires_active_paper_trial(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)

    with pytest.raises(RuntimeError, match="must be active"):
        record_paper_observation(paper_trial_id=trial_id, as_of_date="2024-01-04", operator_note="", store_root=store, allow_json_fallback=True)


def test_zero_candidate_observation_is_valid(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)

    observation = record_paper_observation(paper_trial_id=trial_id, as_of_date="2024-01-04", operator_note="fixture", store_root=store, allow_json_fallback=True)

    validate_paper_trial_observation(observation)
    assert observation["status"] == "zero_candidates"
    assert observation["candidate_count"] == 0

