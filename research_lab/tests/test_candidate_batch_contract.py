from __future__ import annotations

from research_lab.candidates.candidate_batch import build_candidate_batch, validate_candidate, validate_candidate_batch


def test_candidate_batch_schema_validates() -> None:
    batch = build_candidate_batch(
        hypothesis_id="hyp_fixture",
        source_evidence_package_id="ev_fixture",
        dataset_snapshot_id="ds_fixture",
        regime_snapshot_id="rs_fixture",
        cost_model_snapshot_id="cm_fixture",
        symbols=["SPY"],
        as_of_date="2024-01-03",
        threshold=-0.02,
        created_at="2024-01-03T00:00:00Z",
        created_by="pytest",
        ranking_policy_version="candidate_ranking_policy_v1",
    )

    validate_candidate_batch(batch)
    assert batch["candidate_batch_id"].startswith("cb_hyp_fixture_20240103_")


def test_candidate_schema_validates() -> None:
    candidate = {
        "candidate_id": "cand_spy_20240103_fixture",
        "candidate_batch_id": "cb_fixture",
        "symbol": "SPY",
        "sleeve_id": "research_drop_reversion_v1",
        "hypothesis_id": "hyp_fixture",
        "source_evidence_package_id": "ev_fixture",
        "signal_date": "2024-01-03",
        "as_of_date": "2024-01-03",
        "signal_type": "daily_return_below_threshold",
        "signal_value": -0.03,
        "candidate_direction": "long",
        "candidate_status": "generated",
        "ranking_score": 4.0,
        "ranking_bucket": "top",
        "why_now": "Fixture drop breached threshold.",
        "risk_regime": "risk_on",
        "trend_regime": "bull_trend",
        "vol_regime": "normal_vol",
        "drawdown_regime": "normal_drawdown",
        "expected_holding_period": "5 sessions",
        "evidence_quality": "moderate",
        "post_cost_expectancy_reference": {"evidence_type": "backtest"},
        "dataset_snapshot_id": "ds_fixture",
        "regime_snapshot_id": "rs_fixture",
        "cost_model_snapshot_id": "cm_fixture",
        "created_at": "2024-01-03T00:00:00Z",
        "schema_version": "candidate.v1",
    }

    validate_candidate(candidate)

