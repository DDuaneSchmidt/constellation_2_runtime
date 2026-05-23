from __future__ import annotations

from research_lab.longitudinal.candidate_run import build_longitudinal_candidate_run


def test_longitudinal_run_hash_is_deterministic_except_created_at() -> None:
    kwargs = dict(
        sleeve_id="slv_fixture",
        sleeve_version_id="slvv_fixture",
        hypothesis_id="hyp_fixture",
        source_evidence_package_id="ev_fixture",
        dataset_snapshot_id="ds_fixture",
        regime_snapshot_id="rs_fixture",
        cost_model_snapshot_id="cm_fixture",
        start_date="2024-01-01",
        end_date="2024-01-31",
        frequency="weekly",
        threshold=-0.02,
        outcome_windows=[1, 2],
    )
    first = build_longitudinal_candidate_run(**kwargs, created_at="2024-01-01T00:00:00Z")
    second = build_longitudinal_candidate_run(**kwargs, created_at="2024-01-02T00:00:00Z")

    assert first["longitudinal_run_id"] == second["longitudinal_run_id"]
    assert first["content_hash"] == second["content_hash"]

