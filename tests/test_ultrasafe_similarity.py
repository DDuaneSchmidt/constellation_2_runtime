from __future__ import annotations

from src.ultrasafe_similarity import (
    UltraSafeProfile,
    classify_similarity_score,
    default_ultrasafe_profile,
    evaluate_ultrasafe_similarity,
)


def test_classification_thresholds_match_requested_scale() -> None:
    assert classify_similarity_score(0.00) == "highly differentiated"
    assert classify_similarity_score(0.30) == "highly differentiated"
    assert classify_similarity_score(0.31) == "partially differentiated"
    assert classify_similarity_score(0.60) == "partially differentiated"
    assert classify_similarity_score(0.61) == "likely redundant"
    assert classify_similarity_score(1.00) == "likely redundant"


def test_near_clone_is_likely_redundant() -> None:
    ultrasafe = default_ultrasafe_profile()
    candidate = UltraSafeProfile(
        candidate_id="near_clone",
        factor_families=ultrasafe.factor_families,
        rules=ultrasafe.rules,
        concepts=ultrasafe.concepts,
        sectors=ultrasafe.sectors,
        market_cap=ultrasafe.market_cap,
        returns=ultrasafe.returns,
        drawdowns=ultrasafe.drawdowns,
    )

    result = evaluate_ultrasafe_similarity(candidate, ultrasafe)

    assert result.ultrasafe_similarity_score == 1.0
    assert result.differentiation_classification == "likely redundant"
    assert result.factor_family_overlap == 1.0
    assert result.rule_overlap == 1.0
    assert result.concept_overlap == 1.0


def test_distinct_candidate_is_highly_differentiated() -> None:
    ultrasafe = default_ultrasafe_profile()
    candidate = UltraSafeProfile(
        candidate_id="small_cap_energy_event",
        factor_families=("event_dislocation", "commodity_supply", "short_interest"),
        rules=("earnings_gap_entry", "single_name_stop", "short_horizon_exit"),
        concepts=("supply_shock", "idiosyncratic_repricing"),
        sectors=("energy", "materials"),
        market_cap=2_000_000_000.0,
        returns=tuple(-value for value in ultrasafe.returns),
        drawdowns=(-0.35, -0.31, -0.27, -0.22, -0.18, -0.29, -0.25, -0.20, -0.16, -0.12, -0.19, -0.14),
    )

    result = evaluate_ultrasafe_similarity(candidate, ultrasafe)

    assert result.differentiation_classification == "highly differentiated"
    assert result.ultrasafe_similarity_score <= 0.30
    assert result.factor_family_overlap == 0.0
    assert result.rule_overlap == 0.0
    assert result.return_behavior_similarity == 0.0
    assert any("No shared factor families" in item.reason for item in result.explanations)


def test_partial_overlap_explains_shared_and_candidate_only_dimensions() -> None:
    ultrasafe = default_ultrasafe_profile()
    candidate = UltraSafeProfile(
        candidate_id="quality_plus_event_overlay",
        factor_families=("quality", "event_dislocation"),
        rules=("monthly_rebalance", "earnings_gap_entry"),
        concepts=("capital_preservation", "idiosyncratic_repricing"),
        sectors=("technology", "energy"),
        market_cap=50_000_000_000.0,
        returns=(0.010, 0.006, -0.010, 0.018, 0.003, -0.008),
        drawdowns=(-0.03, -0.04, -0.06, -0.04, -0.03, -0.05),
    )

    result = evaluate_ultrasafe_similarity(candidate, ultrasafe)
    payload = result.as_dict()

    assert result.differentiation_classification == "partially differentiated"
    assert 0.31 <= result.ultrasafe_similarity_score <= 0.60
    assert payload["ultrasafe_similarity_score"] == result.ultrasafe_similarity_score
    assert any("Shares factor families: quality" in item.reason for item in result.explanations)
    assert any("candidate-only factor families: event_dislocation" in item.reason for item in result.explanations)


def test_missing_behavior_inputs_fail_closed_to_different_not_similar() -> None:
    candidate = UltraSafeProfile(candidate_id="metadata_only")

    result = evaluate_ultrasafe_similarity(candidate)

    assert result.market_cap_similarity == 0.0
    assert result.return_behavior_similarity == 0.0
    assert result.drawdown_similarity == 0.0
    assert result.ultrasafe_similarity_score < 0.30
