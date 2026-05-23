from __future__ import annotations

from pathlib import Path

from research_lab.paper_trials.operations import measure_due_paper_outcomes, record_next_paper_observation
from research_lab.paper_trials.operations_report import (
    validate_paper_trial_operations_report,
    write_paper_trial_operations_report,
)
from research_lab.paper_trials.paper_trial_registry import change_paper_trial_status
from research_lab.tests.test_paper_trial_observation import _paper_trial_fixture


def test_operations_report_recommends_record_more_observations_when_sample_small(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)

    report = write_paper_trial_operations_report(trial_id, as_of_date="2024-01-01", store_root=store)

    validate_paper_trial_operations_report(report)
    assert report["observation_count"] == 0
    assert report["recommended_next_action"] == "record_more_observations"
    assert "no broker execution" in report["research_label"]


def test_operations_report_recommends_measure_due_outcomes_when_due_exists(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)
    record_next_paper_observation(paper_trial_id=trial_id, as_of_date="2024-01-02", operator_note="fixture", store_root=store, allow_json_fallback=True)

    report = write_paper_trial_operations_report(trial_id, as_of_date="2024-01-03", store_root=store)

    assert report["due_outcomes"] == 1
    assert report["recommended_next_action"] == "measure_due_outcomes"


def test_operations_report_updates_after_due_measurement(tmp_path: Path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)
    record_next_paper_observation(paper_trial_id=trial_id, as_of_date="2024-01-02", operator_note="fixture", store_root=store, allow_json_fallback=True)

    measured = measure_due_paper_outcomes(paper_trial_id=trial_id, as_of_date="2024-01-03", store_root=store, allow_json_fallback=True)

    assert measured["operations_report"]["measured_due_outcomes"] == 1
    assert measured["operations_report"]["due_outcomes"] == 0
    assert (store / "paper_trials" / trial_id / "paper_trial_operations_report.json").exists()
    assert (store / "paper_trials" / trial_id / "paper_trial_operations_report.md").exists()
