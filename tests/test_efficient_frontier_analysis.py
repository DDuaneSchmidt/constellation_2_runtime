from __future__ import annotations

from src.main import (
    FrontierCandidate,
    _best_challenger,
    benchmark_relation,
    benchmark_dominated_by,
    benchmark_dominators,
    build_efficient_frontier_markdown,
    complete_frontier_candidates,
    incomplete_frontier_candidates,
    ultrasafe_benchmark,
)
from src.efficient_frontier_report import build_portfolio_discovery_report_markdown


def _candidate(candidate_id: str, *, cagr: float | None, sharpe: float | None, max_drawdown: float | None) -> FrontierCandidate:
    return FrontierCandidate(
        candidate_id=candidate_id,
        label=candidate_id,
        mechanism="test",
        classification="test",
        source_bucket="test",
        source="test",
        final_score=0.0,
        max_drawdown=max_drawdown,
        sample_size=1,
        expectancy=None,
        cagr=cagr,
        sharpe=sharpe,
    )


def test_ultrasafe_benchmark_included_when_not_present_in_db() -> None:
    markdown, summary = build_efficient_frontier_markdown(top_n=50)

    assert "UltraSafe benchmark" in markdown
    assert "benchmark_override" in markdown
    assert summary["ultrasafe_present"] is True
    assert summary["ultrasafe_source"] == "benchmark_override"
    assert "Incomplete Frontier Candidates" in markdown
    assert summary["complete_frontier_candidate_count"] == 0
    assert summary["incomplete_frontier_candidate_count"] == 50
    assert summary["best_challenger"] is None


def test_candidate_with_higher_cagr_but_worse_sharpe_and_drawdown_does_not_dominate_ultrasafe() -> None:
    ultrasafe = ultrasafe_benchmark()
    higher_return_weaker_risk = _candidate("higher_return_weaker_risk", cagr=0.25, sharpe=0.8, max_drawdown=-0.50)

    assert benchmark_dominators(ultrasafe, [higher_return_weaker_risk]) == []
    assert benchmark_relation(higher_return_weaker_risk, ultrasafe) == "different_tradeoff"


def test_candidate_with_lower_cagr_but_better_sharpe_and_drawdown_is_different_tradeoff() -> None:
    ultrasafe = ultrasafe_benchmark()
    lower_return_better_risk = _candidate("lower_return_better_risk", cagr=0.12, sharpe=1.4, max_drawdown=-0.10)

    assert benchmark_dominators(ultrasafe, [lower_return_better_risk]) == []
    assert benchmark_dominated_by(ultrasafe, [lower_return_better_risk]) == []
    assert benchmark_relation(lower_return_better_risk, ultrasafe) == "different_tradeoff"


def test_candidate_with_all_three_better_dominates_ultrasafe() -> None:
    ultrasafe = ultrasafe_benchmark()
    dominant = _candidate("dominant", cagr=0.20, sharpe=1.1, max_drawdown=-0.20)

    assert benchmark_dominators(ultrasafe, [dominant]) == [dominant]
    assert benchmark_relation(dominant, ultrasafe) == "dominates"


def test_incomplete_candidate_is_excluded_from_benchmark_dominance() -> None:
    ultrasafe = ultrasafe_benchmark()
    missing_sharpe = _candidate("missing_sharpe", cagr=0.30, sharpe=None, max_drawdown=-0.10)

    assert complete_frontier_candidates([missing_sharpe]) == []
    assert incomplete_frontier_candidates([missing_sharpe]) == [missing_sharpe]
    assert benchmark_dominators(ultrasafe, [missing_sharpe]) == []
    assert benchmark_relation(missing_sharpe, ultrasafe) == "insufficient_metrics"


def test_best_challenger_uses_only_complete_candidates() -> None:
    incomplete_high_score = _candidate("incomplete_high_score", cagr=0.40, sharpe=None, max_drawdown=-0.20)
    expected = _candidate("test_5730b64ae8ba", cagr=0.2024, sharpe=0.85, max_drawdown=-0.4125)

    assert _best_challenger([incomplete_high_score, expected]) == expected


def test_known_complete_challenger_is_tradeoff_not_dominator() -> None:
    ultrasafe = ultrasafe_benchmark()
    challenger = _candidate("test_5730b64ae8ba", cagr=0.2024, sharpe=0.85, max_drawdown=-0.4125)

    assert benchmark_dominators(ultrasafe, [challenger]) == []
    assert benchmark_relation(challenger, ultrasafe) == "different_tradeoff"


def test_portfolio_discovery_report_answers_complement_question() -> None:
    markdown, summary = build_portfolio_discovery_report_markdown(top_n=50)

    assert "## Executive Summary" in markdown
    assert "## Best New Challenger" in markdown
    assert "## Most Different Candidate" in markdown
    assert "## Best Risk Adjusted Candidate" in markdown
    assert "## Failure Pattern Summary" in markdown
    assert "## Candidate Graveyard Summary" in markdown
    assert "## UltraSafe Similarity Findings" in markdown
    assert "## Recommended Next Tests" in markdown
    assert "Did we find anything that could complement UltraSafe?" in markdown
    assert "not whether anything beat UltraSafe" in markdown
    assert summary["could_complement_ultrasafe"] is True
    assert summary["ready_for_paper_forward_observation_count"] > 0
