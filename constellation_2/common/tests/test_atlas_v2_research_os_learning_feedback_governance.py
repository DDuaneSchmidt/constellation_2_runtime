from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.learning_feedback_governance import (
    LearningFeedbackGovernanceError,
    validate_learning_feedback_candidate_isolation,
    validate_learning_feedback_memory_influence,
    validate_learning_feedback_no_authority_escalation,
    validate_learning_feedback_no_forbidden_artifacts,
)


def test_forbidden_artifacts_rejected() -> None:
    with pytest.raises(LearningFeedbackGovernanceError):
        validate_learning_feedback_no_forbidden_artifacts({"artifact_types": ["TradeRecommendation"]})


def test_authority_escalation_rejected() -> None:
    with pytest.raises(LearningFeedbackGovernanceError):
        validate_learning_feedback_no_authority_escalation({"metadata": {"capital_authorized": True}})


def test_candidate_promotion_isolation_enforced() -> None:
    with pytest.raises(LearningFeedbackGovernanceError):
        validate_learning_feedback_candidate_isolation({"metadata": {"candidate_factory_modified": True}})


def test_live_capital_sleeve_portfolio_recommendations_blocked() -> None:
    for recommendation in ["Trade this.", "Deploy sleeve.", "Allocate capital.", "Promote candidate.", "Increase position size."]:
        with pytest.raises(LearningFeedbackGovernanceError):
            validate_learning_feedback_no_authority_escalation({"recommendation": recommendation})


def test_generated_mock_only_evidence_does_not_become_validated() -> None:
    assert validate_learning_feedback_memory_influence({"memory_id": "m", "lifecycle_state": "NEW", "evidence_level": "GENERATED_ONLY", "labels": ["generated_only"]}, target="RESEARCH_PRIORITY")
    with pytest.raises(LearningFeedbackGovernanceError):
        validate_learning_feedback_memory_influence({"memory_id": "m", "lifecycle_state": "NEW", "evidence_level": "MOCK_ONLY", "labels": []}, target="RESEARCH_PRIORITY")
