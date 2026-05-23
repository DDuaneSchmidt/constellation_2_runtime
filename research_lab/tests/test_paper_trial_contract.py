from __future__ import annotations

from research_lab.paper_trials.paper_trial import build_paper_trial, validate_paper_trial


def test_paper_trial_schema_validates() -> None:
    trial = build_paper_trial(
        sleeve_id="slv_fixture",
        sleeve_version_id="slvv_fixture",
        hypothesis_id="hyp_fixture",
        dataset_snapshot_id="ds_fixture",
        regime_snapshot_id="rs_fixture",
        cost_model_snapshot_id="cm_fixture",
        source_evidence_package_ids=["ev_event", "ev_bt"],
        threshold=-0.02,
        created_at="2024-01-01T00:00:00Z",
    )

    validate_paper_trial(trial)
    assert trial["status"] == "draft"
    assert trial["observation_frequency"] == "manual"

