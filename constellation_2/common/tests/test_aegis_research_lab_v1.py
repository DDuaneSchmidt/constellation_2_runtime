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
    apply_experiment_result_v1,
    build_edge_taxonomy_v1,
    build_hypothesis_progress_report_v1,
    build_hypothesis_registry_v1,
    build_manual_execution_receipt_v1,
    build_outcome_ledger_v1,
    build_promoted_sleeve_library_v1,
    build_research_evidence_packet_v1,
    build_research_experiment_result_v1,
    build_research_lab_index_v1,
    build_research_lab_awareness_report_v1,
    build_research_task_queue_v1,
    build_research_to_lite_promotion_v1,
    register_hypothesis_v1,
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


def test_manual_hypothesis_registration_enqueues_exploratory_test_and_test_plan() -> None:
    registry = build_hypothesis_registry_v1(generated_at_utc=NOW, hypotheses=[])
    result = register_hypothesis_v1(
        registry=registry,
        title="Narrow breadth weakens continuation",
        edge_family="FRAGILITY_BREADTH",
        behavioral_thesis="Markets become fragile when index strength is supported by fewer stocks.",
        market_regime="LATE_TREND",
        trigger_conditions="SPY strength plus declining participation plus rising concentration",
        expected_outcome="higher pullback probability over next 1-5 sessions",
        failure_modes="breadth recovery; volatility compression resumes",
        created_at_utc=NOW,
        overlap_tags=["breadth", "concentration"],
    )

    validate_research_lab_artifact_v1(result["registry"])
    validate_research_lab_artifact_v1(result["task_queue"])
    validate_research_lab_artifact_v1(result["test_plan"])
    assert result["hypothesis"]["lifecycle_state"] == "proposed"
    assert result["task_queue"]["tasks"][0]["task_type"] == "exploratory_test"
    assert result["test_plan"]["required_test_stages"][0] == "definition_check"
    assert result["test_plan"]["next_required_task"] == "exploratory_test"
    assert result["registry"]["automatic_trade_generation_allowed"] is False


def test_duplicate_awareness_warns_but_does_not_block_creation() -> None:
    first = register_hypothesis_v1(
        registry=build_hypothesis_registry_v1(generated_at_utc=NOW, hypotheses=[]),
        title="Breadth fragility weakens SPY",
        edge_family="FRAGILITY_BREADTH",
        behavioral_thesis="Index strength with fewer participating stocks becomes fragile.",
        market_regime="LATE_TREND",
        trigger_conditions="SPY strength and declining participation",
        expected_outcome="failed continuation risk rises",
        failure_modes="participation recovery",
        created_at_utc=NOW,
        overlap_tags=["breadth"],
    )
    second = register_hypothesis_v1(
        registry=first["registry"],
        title="Narrow breadth weakens continuation",
        edge_family="FRAGILITY_BREADTH",
        behavioral_thesis="Index strength with narrow participation becomes fragile.",
        market_regime="LATE_TREND",
        trigger_conditions="SPY strength and declining participation",
        expected_outcome="failed continuation risk rises",
        failure_modes="breadth recovery",
        created_at_utc=NOW,
        overlap_tags=["breadth"],
    )

    assert second["duplicate_warning"] is True
    assert second["related_hypothesis_ids"] == [first["hypothesis"]["hypothesis_id"]]
    assert "EDGE_FAMILY_MATCH" in second["overlap_reason_codes"]
    assert len(second["registry"]["hypotheses"]) == 2


def test_experiment_result_updates_registry_and_enqueues_next_test_stage() -> None:
    registered = register_hypothesis_v1(
        registry=build_hypothesis_registry_v1(generated_at_utc=NOW, hypotheses=[]),
        title="Narrow breadth weakens continuation",
        edge_family="FRAGILITY_BREADTH",
        behavioral_thesis="Measurable fragility thesis.",
        market_regime="LATE_TREND",
        trigger_conditions="explicit trigger",
        expected_outcome="explicit outcome",
        failure_modes="explicit failure",
        created_at_utc=NOW,
    )
    result = build_research_experiment_result_v1(
        experiment_id="exp-1",
        hypothesis_id=registered["hypothesis"]["hypothesis_id"],
        task_id=registered["task_queue"]["tasks"][0]["task_id"],
        dataset_used="offline-bars",
        test_window="2020/2025",
        trigger_definition="explicit trigger",
        outcome_definition="explicit outcome",
        sample_count=44,
        expectancy="positive",
        win_rate="55pct",
        drawdown="bounded",
        regime_dependency="late trend",
        robustness_notes="initial pass",
        overlap_with_existing_sleeves="none",
        result_status="promising",
        recommendation="continue",
        next_action="regime_segmentation",
        completed_stage="definition_check",
        completed_at_utc=NOW,
    )
    applied = apply_experiment_result_v1(
        registry=registered["registry"],
        task_queue=registered["task_queue"],
        test_plan=registered["test_plan"],
        result=result,
        updated_at_utc=NOW,
    )

    validate_research_lab_artifact_v1(result)
    validate_research_lab_artifact_v1(applied["registry"])
    validate_research_lab_artifact_v1(applied["task_queue"])
    validate_research_lab_artifact_v1(applied["test_plan"])
    assert applied["registry"]["hypotheses"][0]["lifecycle_state"] == "promising"
    assert applied["test_plan"]["completed_stages"] == ["definition_check"]
    assert applied["test_plan"]["current_stage"] == "duplicate_overlap_check"
    assert any(task["status"] == "open" and task["source_trigger"] == "test_plan_next_stage" for task in applied["task_queue"]["tasks"])


def test_hypothesis_progress_report_blocks_until_terminal_state() -> None:
    registered = register_hypothesis_v1(
        registry=build_hypothesis_registry_v1(generated_at_utc=NOW, hypotheses=[]),
        title="A measurable idea",
        edge_family="EDGE",
        behavioral_thesis="A measurable behavioral thesis.",
        market_regime="TREND",
        trigger_conditions="explicit trigger",
        expected_outcome="explicit outcome",
        failure_modes="explicit failure",
        created_at_utc=NOW,
    )
    report = build_hypothesis_progress_report_v1(
        generated_at_utc=NOW,
        hypothesis=registered["hypothesis"],
        test_plan=registered["test_plan"],
    )

    validate_research_lab_artifact_v1(report)
    assert report["blockers"] == ["TEST_PLAN_NOT_TERMINAL"]
    assert "expectancy <= 0 after friction" in report["what_would_disprove_it"]


def test_awareness_report_tracks_queue_duplicates_and_promotion_candidates() -> None:
    registered = register_hypothesis_v1(
        registry=build_hypothesis_registry_v1(generated_at_utc=NOW, hypotheses=[]),
        title="A candidate idea",
        edge_family="EDGE",
        behavioral_thesis="A measurable behavioral thesis.",
        market_regime="TREND",
        trigger_conditions="explicit trigger",
        expected_outcome="explicit outcome",
        failure_modes="explicit failure",
        created_at_utc=NOW,
    )
    registry = registered["registry"]
    registry["hypotheses"][0]["promotion_status"] = "candidate"
    report = build_research_lab_awareness_report_v1(
        generated_at_utc=NOW,
        registry=registry,
        task_queue=registered["task_queue"],
        experiment_results=[],
    )

    validate_research_lab_artifact_v1(report)
    assert report["queued_tasks"]
    assert report["promotion_candidates"][0]["hypothesis_id"] == registered["hypothesis"]["hypothesis_id"]
    assert report["executable_trade_created"] is False


def test_promoted_sleeve_library_rejects_non_promoted_sleeves() -> None:
    sleeve = {
        "sleeve_id": "C2_TREND_EQ_PRIMARY",
        "edge_family": "TREND_CONTINUATION",
        "behavioral_thesis": "Trend persistence after confirmation.",
        "regime_fit": ["trend"],
        "instrument_universe": ["SPY"],
        "entry_logic": "EOD close confirmation",
        "stop_logic": "STOP_BASED",
        "sizing_logic": "risk based",
        "invalidation_logic": "trend failure",
        "known_failure_modes": ["chop"],
        "overlap_tags": ["equity_beta"],
        "promotion_evidence_path": "/tmp/promotion.json",
        "research_hypothesis_id": "hyp-1",
        "promotion_status": "promoted",
    }
    library = build_promoted_sleeve_library_v1(generated_at_utc=NOW, sleeves=[sleeve])

    validate_research_lab_artifact_v1(library)
    assert library["only_promoted_sleeves_allowed"] is True
    assert library["research_lab_artifacts_directly_executable"] is False

    with pytest.raises(ValueError, match="SLEEVE_LIBRARY_REQUIRES_PROMOTED_SLEEVES"):
        build_promoted_sleeve_library_v1(generated_at_utc=NOW, sleeves=[{**sleeve, "promotion_status": "validated"}])


def test_manual_execution_receipt_and_outcome_ledger_are_observational() -> None:
    receipt = build_manual_execution_receipt_v1(
        receipt_id="receipt-1",
        recommended_trade_id="trade-1",
        actual_symbol="SPY",
        actual_side="BUY",
        actual_quantity=1,
        order_type="MKT",
        fill_price="520.10",
        fill_timestamp=NOW,
        stop_order_entered=True,
        stop_price="514.90",
        operator_notes="Entered manually in IB paper.",
    )
    ledger = build_outcome_ledger_v1(
        generated_at_utc=NOW,
        outcome_rows=[
            {
                "recommended_trade_id": "trade-1",
                "manual_execution_receipt_id": receipt["receipt_id"],
                "actual_trade_outcome": "open",
                "stop_behavior": "stop_entered",
                "sleeve_attribution": "pending",
                "edge_overlap_attribution": "pending",
                "research_feedback_action": "none",
            }
        ],
    )

    validate_research_lab_artifact_v1(receipt)
    validate_research_lab_artifact_v1(ledger)
    assert receipt["manual_observation_only"] is True
    assert receipt["broker_submit_required"] is False
    assert ledger["feeds_research_lab"] is True
    assert ledger["runtime_mutation_allowed"] is False
