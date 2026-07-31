from __future__ import annotations

from pathlib import Path
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.paper_trade_outcome_governance import (
    PaperTradeOutcomeGovernanceError,
    validate_paper_trade_outcome_allowed,
)


def test_blocks_live_capital_production_position_and_portfolio_authority() -> None:
    for flag in [
        "live_trading_allowed",
        "broker_execution_allowed",
        "capital_authorized",
        "candidate_promotion_authorized",
        "position_sizing_authorized",
        "portfolio_construction_authorized",
    ]:
        with pytest.raises(PaperTradeOutcomeGovernanceError):
            validate_paper_trade_outcome_allowed({"metadata": {flag: True}})


def test_allows_research_only_targets() -> None:
    for target in ["RESEARCH_MEMORY", "CANDIDATE_QUALITY_MEASUREMENT", "RESEARCH_EFFECTIVENESS", "RESEARCH_PRIORITIZATION", "LEARNING_VALIDATION"]:
        assert validate_paper_trade_outcome_allowed({"metadata": {"influence_target": target}})


def test_blocks_trading_recommendation() -> None:
    with pytest.raises(PaperTradeOutcomeGovernanceError):
        validate_paper_trade_outcome_allowed({"recommendation": "Trade this."})
