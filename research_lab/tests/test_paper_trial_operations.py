from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.audit.audit_log import audit_events
from research_lab.paper_trials.operations import measure_due_paper_outcomes, record_next_paper_observation
from research_lab.paper_trials.paper_trial_registry import change_paper_trial_status
from research_lab.storage.manifest_io import read_json
from research_lab.tests.test_paper_trial_observation import _paper_trial_fixture


def test_record_next_observation_creates_due_outcomes(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)

    result = record_next_paper_observation(
        paper_trial_id=trial_id,
        as_of_date="2024-01-02",
        operator_note="fixture",
        store_root=store,
        allow_json_fallback=True,
    )

    assert result["paper_trial_observation"]["candidate_count"] == 0
    assert len(result["due_outcomes"]) == 5
    assert result["operations_report"]["recommended_next_action"] == "record_more_observations"


def test_duplicate_observation_for_same_as_of_date_is_blocked(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)
    record_next_paper_observation(paper_trial_id=trial_id, as_of_date="2024-01-02", operator_note="fixture", store_root=store, allow_json_fallback=True)

    with pytest.raises(RuntimeError, match="Duplicate paper trial observation"):
        record_next_paper_observation(paper_trial_id=trial_id, as_of_date="2024-01-02", operator_note="again", store_root=store, allow_json_fallback=True)

    assert any(event["action"] == "paper_trial_duplicate_observation_blocked" for event in audit_events(store_root=store))


def test_measure_due_outcomes_handles_zero_candidate_observations(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)
    result = record_next_paper_observation(
        paper_trial_id=trial_id,
        as_of_date="2024-01-04",
        operator_note="zero",
        store_root=store,
        allow_json_fallback=True,
    )

    measured = measure_due_paper_outcomes(paper_trial_id=trial_id, as_of_date="2024-01-31", store_root=store, allow_json_fallback=True)

    assert result["paper_trial_observation"]["candidate_count"] == 0
    assert measured["operations_report"]["unavailable_due_outcomes"] == 5
    assert measured["operations_report"]["measured_candidate_count"] == 0


def test_measure_due_outcomes_does_not_overwrite_measured_outcome_file(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)
    record = record_next_paper_observation(paper_trial_id=trial_id, as_of_date="2024-01-02", operator_note="fixture", store_root=store, allow_json_fallback=True)
    first = measure_due_paper_outcomes(paper_trial_id=trial_id, as_of_date="2024-01-03", store_root=store, allow_json_fallback=True)
    outcome_id = first["measured_due_outcomes"][0]["measured_outcome_id"]
    path = store / "paper_trials" / trial_id / "outcomes" / f"{outcome_id}.json"
    before = read_json(path)

    second = measure_due_paper_outcomes(paper_trial_id=trial_id, as_of_date="2024-01-03", store_root=store, allow_json_fallback=True)
    after = read_json(path)

    assert record["paper_trial_observation"]["candidate_count"] == 0
    assert before == after
    assert second["measured_due_outcomes"] == []
