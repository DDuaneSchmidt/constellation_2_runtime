from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.historical_replay_governance import HistoricalReplayGovernanceError, validate_historical_replay_allowed


def test_cannot_create_trading_authority() -> None:
    with pytest.raises(HistoricalReplayGovernanceError):
        validate_historical_replay_allowed({"evidence_level": "HISTORICAL_REPLAY", "trading_authorized": True})


def test_cannot_create_capital_authority() -> None:
    with pytest.raises(HistoricalReplayGovernanceError):
        validate_historical_replay_allowed({"evidence_level": "HISTORICAL_REPLAY", "metadata": {"capital_authorized": True}})


def test_cannot_promote_candidate() -> None:
    with pytest.raises(HistoricalReplayGovernanceError):
        validate_historical_replay_allowed({"evidence_level": "HISTORICAL_REPLAY", "candidate_promotion_authorized": True})


def test_cannot_escape_evidence_boundary() -> None:
    with pytest.raises(HistoricalReplayGovernanceError):
        validate_historical_replay_allowed({"evidence_level": "LIVE_VALIDATED"})
