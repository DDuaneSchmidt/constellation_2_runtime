from __future__ import annotations

import sys
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    build_edge_taxonomy_v1,
    build_research_evidence_packet_v1,
    build_research_lab_index_v1,
    build_research_to_lite_promotion_v1,
    validate_research_lab_artifact_v1,
)


NOW = "2026-05-14T21:30:00Z"


def _evidence(status: str = "VALIDATED_RESEARCH") -> dict[str, object]:
    return build_research_evidence_packet_v1(
        research_id="research-spy-trend",
        hypothesis_id="hyp-spy-trend",
        title="SPY trend continuation",
        hypothesis_description="SPY continuation after risk-on regime confirmation.",
        research_type="EDGE",
        created_at_utc=NOW,
        source_data_summary="Offline replay over daily bars.",
        replay_window="2024-01-01/2026-05-01",
        instruments_tested=["SPY", "QQQ"],
        regimes_tested=["RISK_ON", "NEUTRAL"],
        edge_family="TREND_CONTINUATION",
        expected_holding_period="1-5D",
        methodology_summary="Walk-forward replay with fixed costs.",
        metrics_summary={"sample_count": 120, "win_rate": "54pct"},
        expectancy_summary="Positive after costs in risk-on windows.",
        drawdown_summary="Max drawdown bounded in replay.",
        MAE_MFE_summary="MAE/MFE reviewed by quartile.",
        failure_modes=["CHOPPY_REGIME"],
        known_limitations=["No live fill modeling."],
        reproducibility_notes="Replay config pinned in artifact lineage.",
        artifact_lineage=[{"artifact_type": "replay_config", "path": "/tmp/replay.json"}],
        research_status=status,
    )


def _promotion(**overrides: object) -> dict[str, object]:
    base = {
        "promotion_id": "promo-spy-trend",
        "research_id": "research-spy-trend",
        "hypothesis_id": "hyp-spy-trend",
        "proposed_lite_component_type": "EDGE_CLUSTER_RULE",
        "promotion_status": "APPROVED_FOR_LITE_IMPLEMENTATION",
        "evidence_packet_refs": [{"artifact_type": "research_evidence_packet_v1", "path": "/tmp/evidence.json"}],
        "validation_summary": "Evidence reviewed.",
        "regime_evidence": "Works in risk-on regimes.",
        "expectancy_evidence": "Positive expectancy.",
        "failure_mode_review": "Fails in chop.",
        "governance_compatibility_review": "Compatible with manual EOD report.",
        "risk_contract_review": "Stop based risk sizing required.",
        "stop_logic_review": "Stop logic is explicit.",
        "manual_execution_compatibility": "Single equity order plus protective stop.",
        "operator_clarity_review": "Operator can execute manually.",
        "implementation_notes": "Implement as source change after approval.",
        "required_tests": ["edge cluster grouping", "manual queue"],
        "approval_reason_codes": ["EVIDENCE_VALIDATED"],
        "rejection_reason_codes": [],
        "approved_by_human": True,
        "created_at_utc": NOW,
        "source_research_status": "VALIDATED_RESEARCH",
    }
    base.update(overrides)
    return build_research_to_lite_promotion_v1(**base)


def test_research_evidence_packet_does_not_authorize_runtime_use() -> None:
    packet = _evidence()

    validate_research_lab_artifact_v1(packet)
    assert packet["research_lab_only"] is True
    assert packet["execution_authority_granted"] is False
    assert packet["runtime_authorized"] is False
    assert packet["broker_submit_required"] is False


def test_promotion_requires_evidence_refs() -> None:
    with pytest.raises(ValueError, match="PROMOTION_REQUIRES_EVIDENCE_PACKET_REFS"):
        _promotion(evidence_packet_refs=[])


def test_approved_for_lite_implementation_requires_human_approval() -> None:
    with pytest.raises(ValueError, match="APPROVED_FOR_LITE_IMPLEMENTATION_REQUIRES_APPROVED_BY_HUMAN"):
        _promotion(approved_by_human=False)


def test_rejected_research_cannot_be_promoted() -> None:
    with pytest.raises(ValueError, match="PROMOTION_REQUIRES_VALIDATED_RESEARCH"):
        _promotion(source_research_status="REJECTED")


def test_draft_research_cannot_be_promoted() -> None:
    with pytest.raises(ValueError, match="PROMOTION_REQUIRES_VALIDATED_RESEARCH"):
        _promotion(source_research_status="DRAFT")


def test_research_lab_artifacts_do_not_require_ib_or_lite_runtime_state() -> None:
    packet = _evidence()
    promotion = _promotion()
    index = build_research_lab_index_v1(
        generated_at_utc=NOW,
        research_items=[
            {
                "research_id": packet["research_id"],
                "title": packet["title"],
                "research_type": packet["research_type"],
                "research_status": packet["research_status"],
                "latest_evidence_packet": "/tmp/evidence.json",
                "latest_promotion_status": promotion["promotion_status"],
                "related_edge_family": packet["edge_family"],
                "related_sleeves": ["C2_TREND_EQ_PRIMARY"],
                "archived": False,
                "operator_notes": "Offline only.",
            }
        ],
    )

    validate_research_lab_artifact_v1(promotion)
    validate_research_lab_artifact_v1(index)
    assert promotion["broker_submit_required"] is False
    assert promotion["runtime_mutation_allowed"] is False
    assert index["runtime_mutation_allowed"] is False


def test_edge_taxonomy_detects_duplicate_and_deprecated_terms() -> None:
    taxonomy = build_edge_taxonomy_v1(
        taxonomy_id="taxonomy-v1",
        generated_at_utc=NOW,
        market_theses=["Risk On Beta"],
        edge_families=["Risk On Beta", "Trend Continuation"],
        edge_clusters=["SPY Trend"],
        trade_expressions=["Long SPY"],
        deprecated_terms=["Trend Continuation"],
        naming_rules=["market thesis -> edge family -> edge cluster -> trade expression"],
    )

    validate_research_lab_artifact_v1(taxonomy)
    assert any(item.startswith("DUPLICATE_TERM:RISK_ON_BETA") for item in taxonomy["duplicate_term_warnings"])
    assert "DEPRECATED_TERM_USED:TREND_CONTINUATION" in taxonomy["duplicate_term_warnings"]


def test_promotion_artifact_is_governance_evidence_only() -> None:
    promotion = _promotion()

    assert promotion["eligible_for_lite_implementation"] is True
    assert promotion["advisory_governance_evidence_only"] is True
    assert promotion["automatic_lite_promotion_allowed"] is False
    assert promotion["runtime_mutation_allowed"] is False


def test_no_execution_lifecycle_dependency_is_introduced() -> None:
    packet = _evidence()
    promotion = _promotion()
    combined = f"{packet} {promotion}"

    assert "submit_allowed" not in combined
    assert "fill_lifecycle" not in combined
    assert packet["transmit_automation_required"] is False
    assert promotion["transmit_automation_required"] is False
