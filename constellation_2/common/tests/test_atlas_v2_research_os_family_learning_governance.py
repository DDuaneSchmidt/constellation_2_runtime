from __future__ import annotations

import pytest

from constellation_2.common.atlas_v2_research_os.family_learning_governance import (
    FamilyLearningGovernanceError,
    family_learning_authority_boundary,
    validate_family_learning_allowed,
)


def test_family_learning_governance_allows_research_memory_updates_only() -> None:
    payload = {
        "allowed_actions": ["family confidence update", "research memory update proposal", "observation recommendation", "retirement recommendation"],
        "artifact_types": ["FamilyLearningReport"],
        "authority_boundary": family_learning_authority_boundary(),
    }

    assert validate_family_learning_allowed(payload) is True


def test_family_learning_governance_blocks_trade_recommendations() -> None:
    payload = {
        "allowed_actions": ["trade recommendation"],
        "artifact_types": ["FamilyLearningReport"],
        "authority_boundary": family_learning_authority_boundary(),
    }

    with pytest.raises(FamilyLearningGovernanceError):
        validate_family_learning_allowed(payload)


def test_family_learning_governance_blocks_authority_escalation_and_forbidden_artifacts() -> None:
    boundary = family_learning_authority_boundary() | {"capital_authorized": True}
    with pytest.raises(FamilyLearningGovernanceError):
        validate_family_learning_allowed(
            {
                "allowed_actions": ["family confidence update"],
                "artifact_types": ["FamilyLearningReport"],
                "authority_boundary": boundary,
            }
        )

    with pytest.raises(FamilyLearningGovernanceError):
        validate_family_learning_allowed(
            {
                "allowed_actions": ["family confidence update"],
                "artifact_types": ["TradeRecommendation"],
                "authority_boundary": family_learning_authority_boundary(),
            }
        )
