from __future__ import annotations

from research_lab.paper_trials.observation import record_paper_observation
from research_lab.paper_trials.outcome import measure_paper_outcomes, validate_paper_trial_outcome
from research_lab.paper_trials.paper_trial_registry import change_paper_trial_status
from research_lab.tests.test_paper_trial_observation import _paper_trial_fixture


def test_paper_trial_outcome_uses_existing_outcome_engine(tmp_path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)
    observation = record_paper_observation(paper_trial_id=trial_id, as_of_date="2024-01-04", operator_note="fixture", store_root=store, allow_json_fallback=True)

    outcome = measure_paper_outcomes(paper_trial_id=trial_id, paper_trial_observation_id=observation["paper_trial_observation_id"], store_root=store, allow_json_fallback=True)

    validate_paper_trial_outcome(outcome)
    assert outcome["status"] == "measured"
    assert outcome["measured_candidate_count"] == 0

