from __future__ import annotations

from pathlib import Path
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.research_effectiveness import evaluate_research_effectiveness
from constellation_2.common.atlas_v2_research_os.research_effectiveness_governance import (
    ResearchEffectivenessGovernanceError,
    certify_research_effectiveness,
    validate_research_effectiveness_allowed,
)
from constellation_2.common.atlas_v2_research_os.research_effectiveness_models import RESEARCH_EFFECTIVENESS_CERTIFICATION

NOW = "2026-06-04T00:00:00Z"


def test_blocks_trading_capital_and_candidate_promotion_authority() -> None:
    for flag in ["trade_advice_allowed", "capital_authorized", "candidate_promotion_authorized"]:
        with pytest.raises(ResearchEffectivenessGovernanceError):
            validate_research_effectiveness_allowed({"metadata": {flag: True}})


def test_blocks_non_research_priority_targets() -> None:
    with pytest.raises(ResearchEffectivenessGovernanceError):
        validate_research_effectiveness_allowed({"metadata": {"influence_target": "TRADING"}})


def test_certification_results_are_bounded() -> None:
    empty = certify_research_effectiveness([])
    assert empty["certification_type"] == RESEARCH_EFFECTIVENESS_CERTIFICATION
    assert empty["result"] == "INSUFFICIENT_DATA"
    report = evaluate_research_effectiveness([
        {
            "activity_id": "ra-1",
            "created_at": NOW,
            "mechanism": "OPENING_RANGE",
            "worker": "worker",
            "backlog_type": "FAILURE_ANALYSIS",
            "experiment_type": "CHEAP_EXPERIMENT",
            "failure_category": "REGIME_MISMATCH",
            "regime": "HIGH_VOL",
            "evidence_maturity": "HISTORICAL_REPLAY",
        }
    ])
    assert report["certification"]["result"] == "MEASURABLE"
    assert report["certification"]["trading_authorized"] is False
    assert report["certification"]["capital_authorized"] is False
    assert report["certification"]["candidate_promotion_authorized"] is False
