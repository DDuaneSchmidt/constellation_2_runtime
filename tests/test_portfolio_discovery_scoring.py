from __future__ import annotations

import pytest

from src.portfolio_discovery_scoring import challenger_score, rank_challengers


def _candidate(**overrides: float | str) -> dict[str, float | str]:
    candidate: dict[str, float | str] = {
        "candidate_id": "baseline",
        "performance": 0.70,
        "drawdown": 0.16,
        "stability": 0.75,
        "diversification": 0.70,
        "correlation_to_ultrasafe": 0.25,
        "ultrasafe_similarity_score": 0.20,
        "overfit_risk": 0.20,
    }
    candidate.update(overrides)
    return candidate


def test_challenger_score_rewards_diversified_stable_low_correlation_candidate() -> None:
    result = challenger_score(_candidate(candidate_id="diversifier"))

    assert result["candidate_id"] == "diversifier"
    assert result["score"] > 0.70
    assert result["components"]["correlation_to_ultrasafe"] == 0.75
    assert result["penalties"]["high_similarity"] == 0.0
    assert "strengths=" in result["explanation"]
    assert "penalties=no threshold penalties" in result["explanation"]


def test_rank_challengers_does_not_rank_solely_by_return() -> None:
    high_return_lookalike = _candidate(
        candidate_id="high_return_lookalike",
        performance=0.96,
        drawdown=0.42,
        stability=0.42,
        diversification=0.20,
        correlation_to_ultrasafe=0.84,
        ultrasafe_similarity_score=0.88,
        overfit_risk=0.72,
    )
    lower_return_diversifier = _candidate(
        candidate_id="lower_return_diversifier",
        performance=0.66,
        drawdown=0.12,
        stability=0.86,
        diversification=0.92,
        correlation_to_ultrasafe=0.08,
        ultrasafe_similarity_score=0.12,
        overfit_risk=0.18,
    )

    ranked = rank_challengers([high_return_lookalike, lower_return_diversifier])

    assert [row["candidate_id"] for row in ranked] == ["lower_return_diversifier", "high_return_lookalike"]
    assert ranked[0]["rank"] == 1
    assert "rank 1" in ranked[0]["explanation"]
    assert ranked[1]["penalties"]["high_similarity"] > 0.0
    assert ranked[1]["penalties"]["high_overfit_risk"] > 0.0


def test_drawdown_accepts_negative_fraction_and_percent_inputs() -> None:
    negative_fraction = challenger_score(_candidate(drawdown=-0.25))
    positive_percent = challenger_score(_candidate(drawdown=25))

    assert negative_fraction["components"]["drawdown"] == 0.75
    assert positive_percent["components"]["drawdown"] == 0.75


def test_unstable_behavior_is_explained_and_penalized() -> None:
    result = challenger_score(_candidate(candidate_id="unstable", stability=0.25, overfit_risk=0.80))

    assert result["penalties"]["unstable_behavior"] > 0.0
    assert result["penalties"]["high_overfit_risk"] > 0.0
    assert "unstable_behavior" in result["explanation"]


def test_missing_metric_fails_closed() -> None:
    candidate = _candidate()
    del candidate["overfit_risk"]

    with pytest.raises(ValueError, match="overfit_risk"):
        challenger_score(candidate)
