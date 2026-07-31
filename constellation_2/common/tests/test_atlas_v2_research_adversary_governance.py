from __future__ import annotations

import pytest

from constellation_2.common.atlas_v2_research_os.research_adversary import (
    ResearchAdversaryGovernanceError,
    validate_research_adversary_review,
)
from constellation_2.common.tests.test_atlas_v2_research_adversary_contracts import valid_review


@pytest.mark.parametrize(
    "phrase",
    [
        "buy",
        "sell",
        "position size",
        "allocate capital",
        "promote candidate",
        "override replay",
        "override qualification",
    ],
)
def test_rejects_forbidden_authority_language(phrase: str) -> None:
    review = valid_review()
    review["mechanism_proposal"]["proposal"] = f"Research adversary should {phrase} after this review."

    with pytest.raises(ResearchAdversaryGovernanceError, match="forbidden authority phrase"):
        validate_research_adversary_review(review)


def test_requires_null_explanation() -> None:
    review = valid_review()
    review["null_explanation"] = {}

    with pytest.raises(ResearchAdversaryGovernanceError, match="null explanation is required"):
        validate_research_adversary_review(review)


def test_requires_at_least_one_competing_explanation() -> None:
    review = valid_review()
    review["competing_explanations"] = []

    with pytest.raises(ResearchAdversaryGovernanceError, match="at least one competing explanation"):
        validate_research_adversary_review(review)


def test_requires_at_least_one_falsification_test() -> None:
    review = valid_review()
    review["falsification_tests"] = []

    with pytest.raises(ResearchAdversaryGovernanceError, match="at least one falsification test"):
        validate_research_adversary_review(review)


def test_rejects_authority_boundary_override() -> None:
    review = valid_review()
    review["authority_boundary"]["replay_override_authorized"] = True

    with pytest.raises(ResearchAdversaryGovernanceError, match="authority boundary mismatch"):
        validate_research_adversary_review(review)


def test_taxonomy_integration_rejects_authority_language() -> None:
    review = valid_review()
    review["taxonomy_reasoning"] = ["Taxonomy lookup should override replay results."]

    with pytest.raises(ResearchAdversaryGovernanceError, match="forbidden authority phrase"):
        validate_research_adversary_review(review)


def test_taxonomy_confidence_must_be_bounded() -> None:
    review = valid_review()
    review["taxonomy_confidence"] = 1.7

    with pytest.raises(ResearchAdversaryGovernanceError, match="taxonomy confidence must be between 0 and 1"):
        validate_research_adversary_review(review)
