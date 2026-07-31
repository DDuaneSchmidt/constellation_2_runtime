from __future__ import annotations

from constellation_2.common.atlas_v2_research_os.research_adversary import (
    GENERATED_ONLY_STATUS,
    build_research_adversary_report,
    create_research_adversary_review,
    validate_research_adversary_review,
)


def valid_review() -> dict:
    return create_research_adversary_review(
        review_id="research-adversary-contract-test",
        created_at="2026-06-05T00:00:00Z",
        mechanism_proposal={
            "mechanism_id": "mech-breakout-volume",
            "mechanism": "BREAKOUT",
            "proposal": "Breakout continuation may be driven by volume expansion after compression.",
        },
        competing_explanations=[
            {
                "explanation_id": "competing-regime-beta",
                "summary": "The effect may be broad market beta during trending regimes.",
            }
        ],
        null_explanation={
            "explanation_id": "null-data-mining",
            "summary": "The apparent effect may be data-mined noise from repeated generated-only trials.",
        },
        assumptions=[
            {"assumption_id": "assumption-volume", "statement": "Volume expansion is measured consistently."},
        ],
        falsification_tests=[
            {
                "test_id": "falsify-with-regime-control",
                "description": "Replay against a regime-matched null and reject if the volume term adds no lift.",
            }
        ],
        supporting_evidence=["Generated observation clusters show continuation after compression."],
        weakening_evidence=["The same pattern may disappear after regime controls."],
        suspected_taxonomy_categories=["REGIME_DEPENDENCY", "WARNING_RECURRENCE"],
        taxonomy_confidence=0.68,
        related_failure_patterns=[
            {
                "pattern_id": "atlas-failure-regime-dependency",
                "category": "REGIME_DEPENDENCY",
                "summary": "Historical reviews weakened after regime controls were added.",
            }
        ],
        taxonomy_reasoning=["The weakening evidence points to regime sensitivity as a likely failure mode."],
    )


def test_valid_review_contract_contains_required_adversarial_sections() -> None:
    review = valid_review()

    assert validate_research_adversary_review(review) is True
    assert review["mechanism_proposal"]["mechanism"] == "BREAKOUT"
    assert review["competing_explanations"]
    assert review["null_explanation"]["explanation_id"] == "null-data-mining"
    assert review["assumptions"]
    assert review["falsification_tests"]
    assert review["evidence_status"] == GENERATED_ONLY_STATUS
    assert review["suspected_taxonomy_categories"] == ["REGIME_DEPENDENCY", "WARNING_RECURRENCE"]
    assert review["taxonomy_confidence"] == 0.68
    assert review["related_failure_patterns"][0]["pattern_id"] == "atlas-failure-regime-dependency"
    assert review["taxonomy_reasoning"]
    assert review["authority_boundary"]["research_only"] is True
    assert review["authority_boundary"]["live_trading_authorized"] is False
    assert review["authority_boundary"]["broker_execution_authorized"] is False
    assert review["authority_boundary"]["capital_authorized"] is False


def test_report_contract_projects_review_evidence_and_authority() -> None:
    report = build_research_adversary_report(valid_review(), created_at="2026-06-05T00:00:00Z")

    assert report["schema_id"] == "atlas_v2_research_adversary_report_v1"
    assert report["evidence_status"] == GENERATED_ONLY_STATUS
    assert report["supporting_evidence"] == ["Generated observation clusters show continuation after compression."]
    assert report["weakening_evidence"] == ["The same pattern may disappear after regime controls."]
    assert report["falsification"][0]["test_id"] == "falsify-with-regime-control"
    assert report["taxonomy_integration"]["evidence_status"] == GENERATED_ONLY_STATUS
    assert report["taxonomy_integration"]["suspected_taxonomy_categories"] == ["REGIME_DEPENDENCY", "WARNING_RECURRENCE"]
    assert report["taxonomy_integration"]["taxonomy_confidence"] == 0.68
    assert report["taxonomy_integration"]["related_failure_patterns"][0]["category"] == "REGIME_DEPENDENCY"
    assert report["authority_boundary"]["candidate_promotion_authorized"] is False
    assert report["authority_boundary"]["replay_override_authorized"] is False
    assert report["authority_boundary"]["qualification_override_authorized"] is False


def test_taxonomy_fields_are_optional_and_do_not_expand_authority() -> None:
    review = valid_review()
    review["suspected_taxonomy_categories"] = []
    review["taxonomy_confidence"] = None
    review["related_failure_patterns"] = []
    review["taxonomy_reasoning"] = []

    assert validate_research_adversary_review(review) is True
    assert review["evidence_status"] == GENERATED_ONLY_STATUS
    assert review["authority_boundary"]["research_only"] is True
    assert review["authority_boundary"]["live_trading_authorized"] is False
    assert review["authority_boundary"]["broker_execution_authorized"] is False
    assert review["authority_boundary"]["capital_authorized"] is False
    assert review["authority_boundary"]["candidate_promotion_authorized"] is False
    assert review["authority_boundary"]["trade_recommendation_authorized"] is False
    assert review["authority_boundary"]["replay_override_authorized"] is False
    assert review["authority_boundary"]["qualification_override_authorized"] is False
