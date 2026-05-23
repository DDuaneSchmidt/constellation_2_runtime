from __future__ import annotations

from research_lab.paper_trials.observation import record_paper_observation
from research_lab.paper_trials.paper_trial_registry import change_paper_trial_status, build_paper_trial_summary
from research_lab.paper_trials.review import record_paper_trial_review, validate_paper_trial_review
from research_lab.tests.test_paper_trial_observation import _paper_trial_fixture


def test_paper_trial_review_append_only_and_summary_label(tmp_path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)
    record_paper_observation(paper_trial_id=trial_id, as_of_date="2024-01-04", operator_note="fixture", store_root=store, allow_json_fallback=True)

    review = record_paper_trial_review(paper_trial_id=trial_id, review_decision="continue", review_reason="Continue observation.", reviewed_by="operator", store_root=store)
    summary = build_paper_trial_summary(trial_id, store_root=store)

    validate_paper_trial_review(review)
    assert summary["latest_review_decision"] == "continue"
    assert "no broker execution" in summary["research_label"]


def test_promotion_review_does_not_mutate_sleeve(tmp_path) -> None:
    store = tmp_path / "store"
    trial_id = _paper_trial_fixture(store)
    change_paper_trial_status(trial_id, "active", store_root=store)

    review = record_paper_trial_review(paper_trial_id=trial_id, review_decision="promote_to_sleeve_review_candidate", review_reason="record only", reviewed_by="operator", store_root=store)

    assert review["next_state"] == "active"
    assert (store / "sleeves" / "slv_fixture" / "sleeve_definition.json").exists()

