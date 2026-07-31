from __future__ import annotations

import pytest

from constellation_2.common.atlas_v2_research_os.research_adversary import (
    DEFAULT_FORBIDDEN_ACTIONS_ACKNOWLEDGED,
    RESEARCH_ADVERSARY_AUTHORITY_BOUNDARY,
    AssumptionExtraction,
    CompetingExplanationSet,
    ConstraintAnalysis,
    ExperimentProposal,
    FalsificationProposal,
    ResearchAdversaryReview,
    ResearchAdversaryValidationError,
    validate_no_forbidden_authority_terms,
    validate_research_adversary_review,
)


def test_research_adversary_review_contract_serializes_and_validates() -> None:
    review = _review()

    payload = review.to_dict()

    assert payload["status"] == "HUMAN_REVIEW_REQUIRED"
    assert payload["source_claim_id"] == "claim-1"
    assert payload["source_hypothesis_id"] == "hyp-1"
    assert payload["authority_boundary"]["artifact_contract_only"] is True
    assert payload["authority_boundary"]["candidate_promotion_authorized"] is False
    assert payload["forbidden_actions_acknowledged"]["governance_override"] is True
    assert validate_research_adversary_review(payload) is True


def test_research_adversary_rejects_invalid_status() -> None:
    payload = _review().to_dict()
    payload["status"] = "READY_FOR_CANDIDATE"

    with pytest.raises(ResearchAdversaryValidationError, match="invalid research adversary status"):
        validate_research_adversary_review(payload)


def test_research_adversary_rejects_forbidden_authority_language() -> None:
    payload = _review().to_dict()
    payload["recommendation"] = "Use this as a trade recommendation."

    with pytest.raises(ResearchAdversaryValidationError, match="forbidden authority language"):
        validate_research_adversary_review(payload)


def test_research_adversary_rejects_nested_forbidden_authority_language() -> None:
    with pytest.raises(ResearchAdversaryValidationError, match="position sizing"):
        validate_no_forbidden_authority_terms({"nested": ["Add position sizing details."]})


def test_research_adversary_rejects_authority_boundary_expansion() -> None:
    payload = _review().to_dict()
    payload["authority_boundary"]["replay_override_authorized"] = True

    with pytest.raises(ResearchAdversaryValidationError, match="replay_override_authorized"):
        validate_research_adversary_review(payload)


def test_component_contracts_reject_forbidden_text() -> None:
    assumption = AssumptionExtraction(
        assumption_id="assumption-1",
        statement="This depends on hidden liquidity behavior.",
        source_field="anomaly_narrative",
        confidence=0.7,
    )
    assert assumption.to_dict()["testable"] is True

    proposal = ExperimentProposal(
        experiment_id="experiment-1",
        question="Can the alternative explanation be separated from volatility clustering?",
        design_summary="Compare matched regimes with and without the anomaly.",
        required_inputs=["historical observations"],
        success_criteria=["Clear separation in observation frequencies"],
        failure_criteria=["No separation after regime matching"],
        expected_artifacts=["experiment design brief"],
    )
    assert proposal.to_dict()["human_review_required"] is True

    bad = FalsificationProposal(
        falsification_id="falsify-1",
        target_assumption_id="assumption-1",
        test_description="Create a replay override.",
        expected_disconfirming_observation="Mismatch persists.",
        minimum_evidence_required="matched sample",
        priority="HIGH",
    )
    with pytest.raises(ResearchAdversaryValidationError, match="replay override"):
        bad.to_dict()


def _review() -> ResearchAdversaryReview:
    assumption = AssumptionExtraction(
        assumption_id="assumption-1",
        statement="The anomaly depends on post-event liquidity imbalance.",
        source_field="anomaly_narrative",
        confidence=0.72,
        dependency="observation reconstruction",
    ).to_dict()
    constraint = ConstraintAnalysis(
        constraint_id="constraint-1",
        constraint_type="DATA",
        description="Intraday source coverage is incomplete.",
        impact="Limits mechanism separation.",
        severity="MEDIUM",
        mitigation="Use matched observation windows.",
    ).to_dict()
    explanations = CompetingExplanationSet(
        explanation_set_id="explanations-1",
        primary_mechanism="LIQUIDITY_RESPONSE",
        alternative_explanations=["Volatility clustering", "Calendar effect", "Data artifact"],
        null_explanation="The anomaly is random conditional noise.",
        discriminating_evidence_needed=["matched regime samples", "source reconstruction"],
    ).to_dict()
    falsification = FalsificationProposal(
        falsification_id="falsify-1",
        target_assumption_id="assumption-1",
        test_description="Compare anomaly frequency against matched non-event windows.",
        expected_disconfirming_observation="Matched windows show the same frequency.",
        minimum_evidence_required="At least 30 matched observations.",
        priority="HIGH",
    ).to_dict()
    experiment = ExperimentProposal(
        experiment_id="experiment-1",
        question="Does the anomaly remain after matching regime and timeframe?",
        design_summary="Build an observation-only matched sample design.",
        required_inputs=["source observations", "regime labels"],
        success_criteria=["Anomaly remains distinguishable after matching"],
        failure_criteria=["Anomaly disappears after matching"],
        expected_artifacts=["experiment proposal", "falsification checklist"],
    ).to_dict()
    return ResearchAdversaryReview(
        review_id="review-1",
        source_observation_id="obs-1",
        source_claim_id="claim-1",
        source_hypothesis_id="hyp-1",
        anomaly_narrative="Unexpected reversal appears after repeated liquidity stress observations.",
        mechanism_proposals=["LIQUIDITY_RESPONSE", "VOLATILITY_CLUSTERING"],
        competing_explanations=[explanations],
        null_explanation="The observation is noise after matching context.",
        assumptions=[assumption],
        constraints=[constraint],
        falsification_tests=[falsification],
        experiment_proposals=[experiment],
        evidence_supporting=["Repeated observations in the same context."],
        evidence_weakening=["Limited direct source coverage."],
        authority_boundary=dict(RESEARCH_ADVERSARY_AUTHORITY_BOUNDARY),
        forbidden_actions_acknowledged=dict(DEFAULT_FORBIDDEN_ACTIONS_ACKNOWLEDGED),
        recommendation="Send to human review for observation-only experiment design.",
        status="HUMAN_REVIEW_REQUIRED",
    )
