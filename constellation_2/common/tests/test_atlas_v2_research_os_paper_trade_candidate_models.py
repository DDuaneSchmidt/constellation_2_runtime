from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.edge_qualification_models import EDGE_AUTHORITY_LEVEL
from constellation_2.common.atlas_v2_research_os.paper_trade_candidate_models import PaperTradeCandidate, PaperTradeCandidateCertification

NOW = "2026-06-05T00:00:00Z"


def test_paper_trade_candidate_contains_required_fields() -> None:
    row = PaperTradeCandidate(
        candidate_id="ptc-1",
        created_at=NOW,
        source_artifact_ids=["artifact-1"],
        source_hypothesis_ids=["hyp-1"],
        source_experiment_ids=["exp-1"],
        source_memory_ids=["mem-1"],
        mechanism_tags=["seasonality"],
        regime_context={"regime": "risk_on"},
        edge_score=0.82,
        confidence=0.88,
        evidence_level="PAPER_FORWARD_OBSERVATION",
        lifecycle_state="QUALIFIED_FOR_HUMAN_REVIEW",
        qualification_reasons=["edge_score >= 0.70"],
        disqualification_reasons=[],
        paper_trade_eligible=True,
        human_review_required=True,
    ).to_dict()
    for field in [
        "candidate_id",
        "created_at",
        "source_artifact_ids",
        "source_hypothesis_ids",
        "source_experiment_ids",
        "source_memory_ids",
        "mechanism_tags",
        "regime_context",
        "edge_score",
        "confidence",
        "evidence_level",
        "lifecycle_state",
        "qualification_reasons",
        "disqualification_reasons",
        "paper_trade_eligible",
        "human_review_required",
        "authority_level",
        "metadata",
    ]:
        assert field in row
    assert row["authority_level"] == EDGE_AUTHORITY_LEVEL


def test_candidate_rejects_invalid_authority_level() -> None:
    candidate = PaperTradeCandidate(
        "ptc-2",
        NOW,
        ["artifact-1"],
        [],
        [],
        [],
        [],
        {},
        0.82,
        0.88,
        "PAPER_FORWARD_OBSERVATION",
        "QUALIFIED_FOR_HUMAN_REVIEW",
        [],
        [],
        True,
        True,
        authority_level="CAPITAL_APPROVAL",
    )
    with pytest.raises(ValueError):
        candidate.to_dict()


def test_certification_defaults_do_not_authorize_live_or_capital_use() -> None:
    cert = PaperTradeCandidateCertification(
        certification_id="cert-1",
        candidate_id="ptc-1",
        created_at=NOW,
        status="CERTIFIED_FOR_HUMAN_REVIEW",
        reasons=["eligible only for human-reviewed paper testing consideration"],
        governance_pass=True,
        lineage_complete=True,
        paper_trade_eligible=True,
        human_review_required=True,
    ).to_dict()
    assert cert["live_trading_authorized"] is False
    assert cert["capital_authorized"] is False
    assert cert["broker_execution_authorized"] is False
