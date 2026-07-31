from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.edge_qualification_models import EDGE_AUTHORITY_LEVEL
from constellation_2.common.atlas_v2_research_os.paper_trade_candidate_governance import PaperTradeCandidateGovernanceError, validate_candidate_governance


def base_candidate() -> dict:
    return {
        "candidate_id": "ptc-1",
        "source_artifact_ids": ["artifact-1"],
        "paper_trade_eligible": True,
        "human_review_required": True,
        "authority_level": EDGE_AUTHORITY_LEVEL,
        "metadata": {"research_only": True},
    }


def test_allows_human_reviewed_paper_testing_consideration() -> None:
    assert validate_candidate_governance(base_candidate()) is True


def test_rejects_forbidden_artifact_type() -> None:
    candidate = base_candidate()
    candidate["artifact_type"] = "LiveTrade"
    with pytest.raises(PaperTradeCandidateGovernanceError):
        validate_candidate_governance(candidate)


def test_rejects_capital_authorization() -> None:
    candidate = base_candidate()
    candidate["capital_authorized"] = True
    with pytest.raises(PaperTradeCandidateGovernanceError):
        validate_candidate_governance(candidate)


def test_rejects_live_trading_language() -> None:
    candidate = base_candidate()
    candidate["metadata"] = {"note": "authorize live trading"}
    with pytest.raises(PaperTradeCandidateGovernanceError):
        validate_candidate_governance(candidate)


def test_rejects_paper_eligibility_without_human_review() -> None:
    candidate = base_candidate()
    candidate["human_review_required"] = False
    with pytest.raises(PaperTradeCandidateGovernanceError):
        validate_candidate_governance(candidate)
