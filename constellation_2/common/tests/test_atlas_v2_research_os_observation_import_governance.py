from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.observation_import_governance import ObservationImportGovernanceError, validate_observation_import_allowed, validate_observation_no_forbidden_artifacts


def test_observation_import_cannot_create_authority() -> None:
    for payload in [
        {"trading_authorized": True},
        {"capital_authorized": True},
        {"broker_execution_authorized": True},
        {"position_sizing_authorized": True},
        {"candidate_promotion_authorized": True},
        {"metadata": {"automatic_paper_trade_placement_authorized": True}},
    ]:
        try:
            validate_observation_import_allowed(payload)
        except ObservationImportGovernanceError:
            pass
        else:
            raise AssertionError(f"payload should have failed governance: {payload}")


def test_observation_import_blocks_forbidden_artifacts() -> None:
    try:
        validate_observation_no_forbidden_artifacts({"artifact_type": "LiveTrade"})
    except ObservationImportGovernanceError:
        pass
    else:
        raise AssertionError("LiveTrade artifact should fail")


def test_observation_import_blocks_trade_recommendation_text() -> None:
    try:
        validate_observation_import_allowed({"observation_text": "trade recommendation to buy SPY"})
    except ObservationImportGovernanceError:
        pass
    else:
        raise AssertionError("trade recommendation text should fail")
