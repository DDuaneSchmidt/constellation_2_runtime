from __future__ import annotations

from src.short_lab_go_no_go import (
    DecisionCriterion,
    DecisionOutcome,
    build_short_lab_go_no_go_markdown,
    decide_short_lab_go_no_go,
)


def test_default_decision_is_go_later_before_spending_portfolio123_credits() -> None:
    decision = decide_short_lab_go_no_go()

    assert decision.outcome is DecisionOutcome.GO_LATER
    assert decision.confidence == "MEDIUM_HIGH"
    assert decision.estimated_probability_of_success == 0.32
    assert "Do not spend Portfolio123 credits yet" in decision.recommendation
    assert "No API calls" in decision.authority_boundary


def test_markdown_contains_required_outputs_and_criteria() -> None:
    markdown, summary = build_short_lab_go_no_go_markdown()

    assert "# Short Lab Go / No-Go" in markdown
    assert "Decision: `GO_LATER`" in markdown
    assert "Confidence:" in markdown
    assert "Exact next action:" in markdown
    assert "Estimated probability of success:" in markdown
    assert "expected_value" in markdown
    assert "implementation_complexity" in markdown
    assert "probability_of_finding_usable_edge" in markdown
    assert "opportunity_cost_versus_ultrasafe" in markdown
    assert summary["outcome"] == "GO_LATER"


def test_strong_short_lab_inputs_can_recommend_go() -> None:
    decision = decide_short_lab_go_no_go(
        [
            DecisionCriterion("expected_value", 0.9, 0.35, "high"),
            DecisionCriterion("implementation_complexity", 0.7, 0.25, "manageable"),
            DecisionCriterion("probability_of_finding_usable_edge", 0.65, 0.25, "good"),
            DecisionCriterion("opportunity_cost_versus_ultrasafe", 0.75, 0.15, "acceptable"),
        ]
    )

    assert decision.outcome is DecisionOutcome.GO
    assert "capped Portfolio123 credit budget" in decision.recommendation


def test_weak_short_lab_inputs_recommend_no_go() -> None:
    decision = decide_short_lab_go_no_go(
        [
            DecisionCriterion("expected_value", 0.2, 0.35, "low"),
            DecisionCriterion("implementation_complexity", 0.2, 0.25, "hard"),
            DecisionCriterion("probability_of_finding_usable_edge", 0.15, 0.25, "poor"),
            DecisionCriterion("opportunity_cost_versus_ultrasafe", 0.2, 0.15, "bad"),
        ]
    )

    assert decision.outcome is DecisionOutcome.NO_GO
    assert "Do not spend Portfolio123 credits" in decision.recommendation
