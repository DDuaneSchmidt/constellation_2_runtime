from __future__ import annotations

import pytest

import src.ai_factor_generator as ai_factor_generator
from src.ai_factor_generator import (
    FACTOR_FAMILIES,
    generate_anti_ultrasafe_candidates,
    generate_by_family,
    generate_candidates,
)


REQUIRED_FIELDS = {
    "candidate_id",
    "name",
    "hypothesis",
    "factor_family",
    "economic_rationale",
    "expected_behavior",
    "why_it_may_diversify_ultrasafe",
    "complexity_score",
    "overfit_risk_score",
    "novelty_score",
}


def test_generate_candidates_returns_one_candidate_per_required_family() -> None:
    candidates = generate_candidates()

    assert [candidate["factor_family"] for candidate in candidates] == list(FACTOR_FAMILIES)
    assert len(candidates) == 16
    assert {candidate["candidate_id"] for candidate in candidates} == {
        f"aifg_{family}_v1" for family in FACTOR_FAMILIES
    }


def test_every_candidate_contains_required_contract_fields_and_scores() -> None:
    for candidate in generate_candidates():
        assert set(candidate) == REQUIRED_FIELDS
        for field in REQUIRED_FIELDS - {"complexity_score", "overfit_risk_score", "novelty_score"}:
            assert isinstance(candidate[field], str)
            assert candidate[field]
        for score_field in ("complexity_score", "overfit_risk_score", "novelty_score"):
            assert isinstance(candidate[score_field], int)
            assert 1 <= candidate[score_field] <= 10


def test_generation_is_deterministic() -> None:
    assert generate_candidates() == generate_candidates()
    assert generate_candidates(["quality", "value"]) == generate_candidates(["quality", "value"])


def test_generate_by_family_returns_only_requested_family() -> None:
    candidates = generate_by_family("quality")

    assert len(candidates) == 1
    assert candidates[0]["candidate_id"] == "aifg_quality_v1"
    assert candidates[0]["factor_family"] == "quality"


def test_generate_by_family_normalizes_case_and_spacing() -> None:
    assert generate_by_family("  QUALITY  ") == generate_by_family("quality")


def test_generate_by_family_rejects_unknown_family() -> None:
    with pytest.raises(ValueError, match="Unknown factor family"):
        generate_by_family("portfolio123")


def test_generate_anti_ultrasafe_candidates_is_explicitly_generation_only() -> None:
    candidates = generate_anti_ultrasafe_candidates()

    assert candidates == generate_by_family("anti_ultrasafe")
    assert candidates[0]["factor_family"] == "anti_ultrasafe"
    assert "UltraSafe" in candidates[0]["why_it_may_diversify_ultrasafe"]


def test_no_portfolio123_or_api_surface_is_exposed() -> None:
    module_candidates = generate_candidates()
    joined_values = " ".join(str(value) for candidate in module_candidates for value in candidate.values())
    module_source = ai_factor_generator.__loader__.get_source(ai_factor_generator.__name__) or ""

    assert "Portfolio123" not in joined_values
    assert "requests" not in module_source
    assert "urllib" not in module_source
