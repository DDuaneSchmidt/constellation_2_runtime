from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.learning_validation_governance import (
    LearningValidationGovernanceError,
    validate_learning_validation_allowed,
    validate_learning_validation_certification,
)


def test_learning_validation_blocks_authority_escalation() -> None:
    with pytest.raises(LearningValidationGovernanceError):
        validate_learning_validation_allowed({"metadata": {"candidate_promotion_authorized": True}})
    with pytest.raises(LearningValidationGovernanceError):
        validate_learning_validation_allowed({"metadata": {"capital_authorized": True}})


def test_learning_validation_blocks_forbidden_artifacts_and_recommendations() -> None:
    with pytest.raises(LearningValidationGovernanceError):
        validate_learning_validation_allowed({"artifact_types": ["PortfolioRecommendation"]})
    with pytest.raises(LearningValidationGovernanceError):
        validate_learning_validation_allowed({"recommendation": "Trade this."})


def test_learning_validation_certification_values() -> None:
    assert validate_learning_validation_certification({"certification_result": "IMPROVING"})
    with pytest.raises(LearningValidationGovernanceError):
        validate_learning_validation_certification({"certification_result": "PROMOTE_CANDIDATE"})
