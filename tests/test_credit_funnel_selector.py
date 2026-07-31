from __future__ import annotations

from src.credit_funnel_selector import (
    CreditFunnelCandidate,
    discovery_score,
    explain_rejection,
    explain_selection,
    rank_candidates,
    select_candidates_for_testing,
)


def _candidate(
    candidate_id: str,
    *,
    title: str,
    thesis: str,
    mechanism: str = "credit_mean_reversion",
    novelty: float = 0.7,
    economic: float = 0.7,
    diversification: float = 0.6,
    non_redundancy: float = 0.7,
    complexity: float = 0.1,
    overfit: float = 0.1,
    universe: tuple[str, ...] = ("LQD", "HYG"),
    tags: tuple[str, ...] = ("credit",),
) -> CreditFunnelCandidate:
    return CreditFunnelCandidate(
        candidate_id=candidate_id,
        title=title,
        thesis=thesis,
        mechanism=mechanism,
        universe=universe,
        tags=tags,
        novelty_score=novelty,
        economic_logic_score=economic,
        diversification_score=diversification,
        non_redundancy_score=non_redundancy,
        complexity_penalty=complexity,
        overfit_risk_penalty=overfit,
    )


def test_discovery_score_combines_positive_components_and_penalties() -> None:
    score = discovery_score(
        {
            "candidate_id": "credit_001",
            "novelty_score": 0.8,
            "economic_logic_score": 0.7,
            "diversification_score": 0.6,
            "non_redundancy_score": 0.5,
            "complexity_penalty": 0.2,
            "overfit_risk_penalty": 0.1,
        }
    )

    assert score.discovery_score == 2.3
    assert score.to_dict()["complexity_penalty"] == 0.2


def test_rank_candidates_removes_exact_duplicates_and_sends_rejection_to_graveyard() -> None:
    stronger = _candidate(
        "credit_001",
        title="Credit spread widening reversal",
        thesis="Buy credit after spread widening when liquidity stabilizes",
        economic=0.9,
    )
    weaker_duplicate = _candidate(
        "credit_002",
        title="Credit spread widening reversal",
        thesis="Buy credit after spread widening when liquidity stabilizes",
        economic=0.6,
    )

    result = rank_candidates([weaker_duplicate, stronger])

    assert [row.candidate_id for row in result.ranked_candidates] == ["credit_001"]
    assert result.rejected_candidates[0].candidate_id == "credit_002"
    assert result.rejected_candidates[0].rejection_reason == "DUPLICATE_CANDIDATE_LOWER_SCORE"
    assert result.candidate_graveyard[0]["archive"] == "Candidate Graveyard"
    assert result.candidate_graveyard[0]["candidate_id"] == "credit_002"


def test_rank_candidates_removes_highly_similar_lower_ranked_candidate() -> None:
    stronger = _candidate(
        "credit_001",
        title="HYG liquidity stress reversal",
        thesis="Buy HYG credit after liquidity stress and spread widening",
        economic=0.9,
        tags=("credit", "liquidity"),
    )
    similar = _candidate(
        "credit_002",
        title="HYG liquidity stress reversal",
        thesis="Buy HYG credit after liquidity stress and spread widening stabilizes",
        economic=0.7,
        tags=("credit", "liquidity"),
    )

    result = rank_candidates([similar, stronger], similarity_threshold=0.8)

    assert [row.candidate_id for row in result.ranked_candidates] == ["credit_001"]
    rejection = result.rejected_candidates[0]
    assert rejection.rejection_reason == "HIGHLY_SIMILAR_TO_HIGHER_RANKED_CANDIDATE"
    assert rejection.similar_to_candidate_id == "credit_001"
    assert "Candidate Graveyard" == result.candidate_graveyard[0]["archive"]


def test_select_candidates_for_testing_keeps_only_top_budget_and_graveyards_rest() -> None:
    candidates = [
        _candidate("credit_001", title="Best", thesis="Credit rates momentum after spread compression", economic=0.95),
        _candidate("credit_002", title="Second", thesis="Credit risk premium reversal after volatility shock", economic=0.85),
        _candidate("credit_003", title="Third", thesis="Macro credit carry filter with rates and liquidity", economic=0.75),
    ]

    result = select_candidates_for_testing(candidates, max_candidates=2)

    assert [row.candidate_id for row in result.selected_candidates] == ["credit_001", "credit_002"]
    assert result.rejected_candidates[-1].candidate_id == "credit_003"
    assert result.rejected_candidates[-1].rejection_reason == "NOT_IN_TOP_CREDIT_BUDGET"
    assert {row.candidate_id for row in result.rejected_candidates} == {
        record["candidate_id"] for record in result.candidate_graveyard
    }


def test_select_candidates_for_testing_rejects_below_score_floor() -> None:
    weak = _candidate(
        "credit_weak",
        title="Weak credit idea",
        thesis="Credit threshold optimized best in-sample",
        novelty=0.1,
        economic=0.1,
        diversification=0.1,
        non_redundancy=0.1,
        complexity=0.7,
        overfit=0.7,
    )

    result = select_candidates_for_testing([weak], min_discovery_score=0.0)

    assert result.selected_candidates == ()
    assert result.rejected_candidates[0].rejection_reason == "DISCOVERY_SCORE_BELOW_TESTING_FLOOR"
    assert result.candidate_graveyard[0]["failed_criteria"] == ["discovery_score"]


def test_explanations_include_selection_and_rejection_reasons() -> None:
    result = select_candidates_for_testing(
        [
            _candidate("credit_001", title="Strong", thesis="Credit momentum after spread compression", economic=0.9),
            _candidate("credit_002", title="Weak", thesis="Credit threshold optimized best in-sample", economic=0.2),
        ],
        max_candidates=1,
    )

    selection = explain_selection(result.selected_candidates[0])
    rejection = explain_rejection(result.rejected_candidates[0])

    assert "advances because discovery_score=" in selection
    assert "rejected at CREDIT_FUNNEL_CAPACITY_CUTOFF" in rejection
