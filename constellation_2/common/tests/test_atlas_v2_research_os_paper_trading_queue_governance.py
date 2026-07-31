from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from constellation_2.common.atlas_v2_research_os.paper_trading_queue_governance import PaperTradingQueueGovernanceError, validate_paper_trading_queue_allowed


def test_live_trading_authority_rejected() -> None:
    try:
        validate_paper_trading_queue_allowed({"metadata": {"live_trading_authorized": True}})
    except PaperTradingQueueGovernanceError as exc:
        assert "live_trading_authorized" in str(exc)
    else:
        raise AssertionError("live authority should fail")


def test_broker_and_capital_authority_rejected() -> None:
    for flag in ["broker_execution_authorized", "capital_authorized", "position_sizing_authorized", "production_promotion_authorized"]:
        try:
            validate_paper_trading_queue_allowed({flag: True})
        except PaperTradingQueueGovernanceError as exc:
            assert flag in str(exc)
        else:
            raise AssertionError(f"{flag} should fail")


def test_forbidden_artifact_and_recommendation_rejected() -> None:
    for payload in [
        {"artifact_types": ["BrokerOrder"]},
        {"recommendation": "Trade this."},
    ]:
        try:
            validate_paper_trading_queue_allowed(payload)
        except PaperTradingQueueGovernanceError:
            pass
        else:
            raise AssertionError("forbidden payload should fail")
