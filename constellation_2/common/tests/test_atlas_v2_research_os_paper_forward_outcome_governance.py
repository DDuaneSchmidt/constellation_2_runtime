from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.paper_forward_outcome_governance import (
    PaperForwardOutcomeGovernanceError,
    validate_paper_forward_outcome_allowed,
)


def test_cannot_create_trading_or_capital_authority() -> None:
    for payload in [
        {"trading_authorized": True},
        {"capital_authorized": True},
        {"broker_execution_authorized": True},
        {"candidate_promotion_authorized": True},
        {"metadata": {"position_sizing_authorized": True}},
    ]:
        try:
            validate_paper_forward_outcome_allowed(payload)
        except PaperForwardOutcomeGovernanceError:
            pass
        else:
            raise AssertionError(f"payload should have failed governance: {payload}")


def test_evidence_boundary_must_remain_paper_forward_observation() -> None:
    try:
        validate_paper_forward_outcome_allowed({"evidence_level": "LIVE_VALIDATED"})
    except PaperForwardOutcomeGovernanceError as exc:
        assert "PAPER_FORWARD_OBSERVATION" in str(exc)
    else:
        raise AssertionError("live evidence level should fail")
