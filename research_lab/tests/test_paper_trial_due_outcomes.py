from __future__ import annotations

from pathlib import Path

from research_lab.audit.audit_log import audit_events
from research_lab.paper_trials.due_outcomes import (
    compute_due_date,
    create_due_outcomes_for_observation,
    list_paper_due_outcomes,
    validate_paper_trial_due_outcome,
)
from research_lab.paper_trials.observation import record_paper_observation
from research_lab.paper_trials.paper_trial_registry import change_paper_trial_status
from research_lab.tests.test_paper_trial_observation import _paper_trial_fixture


def test_paper_trial_due_outcome_schema_validates(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)
    observation = record_paper_observation(paper_trial_id=trial_id, as_of_date="2024-01-02", operator_note="fixture", store_root=store, allow_json_fallback=True)

    rows = create_due_outcomes_for_observation(
        paper_trial_id=trial_id,
        paper_trial_observation_id=observation["paper_trial_observation_id"],
        store_root=store,
    )

    validate_paper_trial_due_outcome(rows[0])
    assert {row["outcome_window"] for row in rows} == {1, 2, 5, 10, 20}
    assert any(event["action"] == "paper_trial_due_outcomes_created" for event in audit_events(store_root=store))


def test_due_date_uses_dataset_trading_day_calendar(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    from research_lab.paper_trials.paper_trial_registry import load_paper_trial

    trial = load_paper_trial(trial_id, store_root=store)

    assert compute_due_date(trial["dataset_snapshot_id"], "2024-01-02", 1, store_root=store) == "2024-01-03"
    assert compute_due_date(trial["dataset_snapshot_id"], "2024-01-02", 2, store_root=store) == "2024-01-04"
    assert compute_due_date(trial["dataset_snapshot_id"], "2024-01-02", 5, store_root=store) == ""


def test_list_paper_due_outcomes_identifies_due_pending_and_unavailable(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)
    observation = record_paper_observation(paper_trial_id=trial_id, as_of_date="2024-01-02", operator_note="fixture", store_root=store, allow_json_fallback=True)
    create_due_outcomes_for_observation(paper_trial_id=trial_id, paper_trial_observation_id=observation["paper_trial_observation_id"], store_root=store)

    payload = list_paper_due_outcomes(trial_id, as_of_date="2024-01-03", store_root=store)

    assert payload["due_count"] == 1
    assert payload["pending_count"] == 1
    assert payload["unavailable_count"] == 3
