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
    build_experiment_result_v1,
    build_hypothesis_progress_report_v1,
    build_hypothesis_registry_v1,
    build_learning_tasks_from_outcomes_v1,
    build_manual_execution_receipt_v1,
    build_manual_trade_packet_v1,
    build_outcome_ledger_v1,
    build_promotion_review_v1,
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
        instrument_universe=["SPY", "QQQ"],
        time_horizon="1-5 sessions",
        created_at_utc=NOW,
        overlap_tags=["breadth", "concentration"],
    )

    validate_research_lab_artifact_v1(result["registry"])
    validate_research_lab_artifact_v1(result["task_queue"])
    validate_research_lab_artifact_v1(result["test_plan"])
    assert result["hypothesis"]["lifecycle_state"] == "proposed"
    assert result["hypothesis"]["instrument_universe"] == ["SPY", "QQQ"]
    assert result["hypothesis"]["time_horizon"] == "1-5 sessions"
    assert result["task_queue"]["tasks"][0]["task_type"] == "definition_check"
    assert result["test_plan"]["required_test_stages"][0] == "definition_check"
    assert result["test_plan"]["next_required_task"] == "definition_check"
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


def test_hypothesis_registration_preserves_existing_task_queue() -> None:
    first = register_hypothesis_v1(
        registry=build_hypothesis_registry_v1(generated_at_utc=NOW, hypotheses=[]),
        title="First idea",
        edge_family="EDGE_A",
        behavioral_thesis="First measurable behavioral thesis.",
        market_regime="TREND",
        trigger_conditions="first trigger",
        expected_outcome="first outcome",
        failure_modes="first failure",
        created_at_utc=NOW,
    )
    second = register_hypothesis_v1(
        registry=first["registry"],
        task_queue=first["task_queue"],
        title="Second idea",
        edge_family="EDGE_B",
        behavioral_thesis="Second measurable behavioral thesis.",
        market_regime="CHOP",
        trigger_conditions="second trigger",
        expected_outcome="second outcome",
        failure_modes="second failure",
        created_at_utc=NOW,
    )

    validate_research_lab_artifact_v1(second["task_queue"])
    assert len(second["task_queue"]["tasks"]) == 2
    assert {task["hypothesis_id"] for task in second["task_queue"]["tasks"]} == {
        first["hypothesis"]["hypothesis_id"],
        second["hypothesis"]["hypothesis_id"],
    }


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
    result = build_experiment_result_v1(
        experiment_id="exp-1",
        hypothesis_id=registered["hypothesis"]["hypothesis_id"],
        task_id=registered["task_queue"]["tasks"][0]["task_id"],
        test_stage="definition_check",
        dataset_used="offline-bars",
        instrument_universe=["SPY"],
        test_window="2020/2025",
        trigger_definition="explicit trigger",
        outcome_definition="explicit outcome",
        sample_count=44,
        expectancy="positive",
        win_rate="55pct",
        avg_return="0.3pct",
        median_return="0.2pct",
        max_drawdown="bounded",
        volatility="moderate",
        friction_adjusted_result="positive",
        regime_dependency="late trend",
        robustness_notes="initial pass",
        out_of_sample_result="not_yet_tested",
        overlap_with_existing_sleeves="none",
        result_status="promising",
        recommendation="continue",
        next_action="regime_segmentation",
        created_at=NOW,
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
    assert applied["registry"]["hypotheses"][0]["lifecycle_state"] == "duplicate_checked"
    assert applied["test_plan"]["completed_stages"] == ["definition_check"]
    assert applied["test_plan"]["current_stage"] == "duplicate_overlap_check"
    assert any(task["status"] == "open" and task["source_trigger"] == "test_plan_next_stage" for task in applied["task_queue"]["tasks"])


def test_experiment_result_cannot_skip_test_plan_stage() -> None:
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
    result = build_experiment_result_v1(
        experiment_id="exp-skip",
        hypothesis_id=registered["hypothesis"]["hypothesis_id"],
        task_id=registered["task_queue"]["tasks"][0]["task_id"],
        test_stage="exploratory_backtest",
        dataset_used="offline-bars",
        instrument_universe=["SPY"],
        test_window="2020/2025",
        trigger_definition="explicit trigger",
        outcome_definition="explicit outcome",
        sample_count=44,
        expectancy="positive",
        win_rate="55pct",
        avg_return="0.3pct",
        median_return="0.2pct",
        max_drawdown="bounded",
        volatility="moderate",
        friction_adjusted_result="positive",
        regime_dependency="late trend",
        robustness_notes="initial pass",
        out_of_sample_result="not_yet_tested",
        overlap_with_existing_sleeves="none",
        result_status="promising",
        recommendation="continue",
        next_action="regime_segmentation",
        created_at=NOW,
    )

    with pytest.raises(ValueError, match="EXPERIMENT_RESULT_STAGE_MISMATCH"):
        apply_experiment_result_v1(
            registry=registered["registry"],
            task_queue=registered["task_queue"],
            test_plan=registered["test_plan"],
            result=result,
            updated_at_utc=NOW,
        )


def test_validated_candidate_requires_full_test_plan() -> None:
    registered = register_hypothesis_v1(
        registry=build_hypothesis_registry_v1(generated_at_utc=NOW, hypotheses=[]),
        title="Premature validation",
        edge_family="EDGE",
        behavioral_thesis="Measurable thesis.",
        market_regime="TREND",
        trigger_conditions="explicit trigger",
        expected_outcome="explicit outcome",
        failure_modes="explicit failure",
        created_at_utc=NOW,
    )
    result = build_experiment_result_v1(
        experiment_id="exp-premature",
        hypothesis_id=registered["hypothesis"]["hypothesis_id"],
        task_id=registered["task_queue"]["tasks"][0]["task_id"],
        test_stage="definition_check",
        dataset_used="offline-bars",
        instrument_universe=["SPY"],
        test_window="2020/2025",
        trigger_definition="explicit trigger",
        outcome_definition="explicit outcome",
        sample_count=44,
        expectancy="positive",
        win_rate="55pct",
        avg_return="0.3pct",
        median_return="0.2pct",
        max_drawdown="bounded",
        volatility="moderate",
        friction_adjusted_result="positive",
        regime_dependency="late trend",
        robustness_notes="initial pass",
        out_of_sample_result="positive holdout",
        overlap_with_existing_sleeves="none",
        result_status="validated_candidate",
        recommendation="promote",
        next_action="promotion_review",
        created_at=NOW,
    )

    with pytest.raises(ValueError, match="VALIDATED_CANDIDATE_REQUIRES_COMPLETED_TEST_PLAN"):
        apply_experiment_result_v1(
            registry=registered["registry"],
            task_queue=registered["task_queue"],
            test_plan=registered["test_plan"],
            result=result,
            updated_at_utc=NOW,
        )


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
        "source_hypothesis_id": "hyp-1",
        "promotion_status": "promoted",
        "exit_logic": "EOD or stop exit",
        "production_status": "active",
        "created_at": NOW,
        "updated_at": NOW,
    }
    library = build_promoted_sleeve_library_v1(generated_at_utc=NOW, sleeves=[sleeve])

    validate_research_lab_artifact_v1(library)
    assert library["only_promoted_sleeves_allowed"] is True
    assert library["research_lab_artifacts_directly_executable"] is False
    assert library["sleeves"][0]["source_hypothesis_id"] == "hyp-1"

    with pytest.raises(ValueError, match="SLEEVE_LIBRARY_REQUIRES_PROMOTED_SLEEVES"):
        build_promoted_sleeve_library_v1(generated_at_utc=NOW, sleeves=[{**sleeve, "promotion_status": "validated"}])


def test_promotion_review_requires_completed_protocol_and_human_approval() -> None:
    incomplete_plan = {
        "completed_stages": ["definition_check"],
    }
    review = build_promotion_review_v1(
        promotion_id="promotion-review-1",
        hypothesis_id="hyp-1",
        generated_at_utc=NOW,
        test_plan=incomplete_plan,
        friction_adjusted_result="positive",
        out_of_sample_support="positive holdout",
        regime_notes="trend only",
        failure_mode_notes="fails in chop",
        edge_overlap_review="distinct edge",
        approved_by_human=True,
    )

    validate_research_lab_artifact_v1(review)
    assert review["eligible_for_promoted_sleeve_library"] is False

    complete_plan = {"completed_stages": [
        "definition_check",
        "duplicate_overlap_check",
        "exploratory_backtest",
        "regime_segmentation",
        "robustness_check",
        "transaction_friction_check",
        "out_of_sample_check",
        "failure_mode_review",
        "edge_overlap_review",
        "promotion_review",
    ]}
    approved = build_promotion_review_v1(
        promotion_id="promotion-review-2",
        hypothesis_id="hyp-1",
        generated_at_utc=NOW,
        test_plan=complete_plan,
        friction_adjusted_result="positive",
        out_of_sample_support="positive holdout",
        regime_notes="trend only",
        failure_mode_notes="fails in chop",
        edge_overlap_review="distinct edge",
        approved_by_human=True,
    )
    validate_research_lab_artifact_v1(approved)
    assert approved["eligible_for_promoted_sleeve_library"] is True

    negative = build_promotion_review_v1(
        promotion_id="promotion-review-3",
        hypothesis_id="hyp-1",
        generated_at_utc=NOW,
        test_plan=complete_plan,
        friction_adjusted_result="negative after friction",
        out_of_sample_support="positive holdout",
        regime_notes="trend only",
        failure_mode_notes="fails in chop",
        edge_overlap_review="distinct edge",
        approved_by_human=True,
    )
    validate_research_lab_artifact_v1(negative)
    assert negative["eligible_for_promoted_sleeve_library"] is False


def test_manual_trade_packet_blocks_incomplete_trade_but_preserves_manual_only() -> None:
    library = build_promoted_sleeve_library_v1(
        generated_at_utc=NOW,
        sleeves=[
            {
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "source_hypothesis_id": "hyp-1",
                "edge_family": "TREND_CONTINUATION",
                "behavioral_thesis": "Trend persistence after confirmation.",
                "regime_fit": ["trend"],
                "instrument_universe": ["SPY", "QQQ"],
                "entry_logic": "EOD close confirmation",
                "exit_logic": "EOD or stop exit",
                "stop_logic": "STOP_BASED",
                "sizing_logic": "risk based",
                "invalidation_logic": "trend failure",
                "known_failure_modes": ["chop"],
                "overlap_tags": ["equity_beta"],
                "promotion_evidence_path": "/tmp/promotion.json",
                "promotion_status": "promoted",
                "production_status": "active",
                "created_at": NOW,
                "updated_at": NOW,
            }
        ],
    )
    packet = build_manual_trade_packet_v1(
        packet_id="packet-1",
        run_id="run-1",
        date="2026-05-14",
        generated_at_utc=NOW,
        regime_state="TREND",
        promoted_sleeve_library=library,
        trade_candidates=[
            {
                "recommended_trade_id": "trade-1",
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "source_hypothesis_id": "hyp-1",
                "symbol": "SPY",
                "side": "BUY",
                "instrument_type": "LONG_EQUITY",
                "entry_reference_price": "520.00",
                "quantity_or_sizing_guidance": "1 share",
                "stop_price": "514.80",
                "stop_logic": "STOP_BASED",
                "risk_per_trade": "5.20",
                "edge_family": "TREND_CONTINUATION",
                "inclusion_reason": "Promoted sleeve fired.",
            },
            {
                "recommended_trade_id": "trade-2",
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "source_hypothesis_id": "hyp-1",
                "symbol": "QQQ",
                "side": "BUY",
                "instrument_type": "LONG_EQUITY",
                "entry_reference_price": "",
                "quantity_or_sizing_guidance": "1 share",
                "stop_price": "400.00",
                "stop_logic": "STOP_BASED",
                "risk_per_trade": "4.00",
            },
        ],
    )

    validate_research_lab_artifact_v1(packet)
    assert packet["manual_execution_only"] is True
    assert packet["broker_submit_required"] is False
    assert packet["trade_candidates"][0]["actionable"] is True
    assert packet["trade_candidates"][1]["actionable"] is False
    assert "MISSING_ENTRY_REFERENCE_PRICE" in packet["trade_candidates"][1]["do_not_trade_blockers"]


def test_manual_trade_packet_requires_promoted_sleeve_library_match() -> None:
    packet = build_manual_trade_packet_v1(
        packet_id="packet-1",
        run_id="run-1",
        date="2026-05-14",
        generated_at_utc=NOW,
        regime_state="TREND",
        trade_candidates=[
            {
                "recommended_trade_id": "trade-1",
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "source_hypothesis_id": "unpromoted-hyp",
                "symbol": "SPY",
                "side": "BUY",
                "instrument_type": "LONG_EQUITY",
                "entry_reference_price": "520.00",
                "quantity_or_sizing_guidance": "1 share",
                "stop_price": "514.80",
                "stop_logic": "STOP_BASED",
                "risk_per_trade": "5.20",
            }
        ],
    )

    validate_research_lab_artifact_v1(packet)
    assert packet["trade_candidates"][0]["actionable"] is False
    assert "PROMOTED_SLEEVE_LIBRARY_MISSING" in packet["trade_candidates"][0]["do_not_trade_blockers"]

    library = build_promoted_sleeve_library_v1(
        generated_at_utc=NOW,
        sleeves=[
            {
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "source_hypothesis_id": "hyp-1",
                "edge_family": "TREND_CONTINUATION",
                "behavioral_thesis": "Trend persistence after confirmation.",
                "regime_fit": ["trend"],
                "instrument_universe": ["SPY"],
                "entry_logic": "EOD close confirmation",
                "exit_logic": "EOD or stop exit",
                "stop_logic": "STOP_BASED",
                "sizing_logic": "risk based",
                "invalidation_logic": "trend failure",
                "known_failure_modes": ["chop"],
                "overlap_tags": ["equity_beta"],
                "promotion_evidence_path": "/tmp/promotion.json",
                "promotion_status": "promoted",
                "production_status": "active",
                "created_at": NOW,
                "updated_at": NOW,
            }
        ],
    )
    mismatched = build_manual_trade_packet_v1(
        packet_id="packet-2",
        run_id="run-1",
        date="2026-05-14",
        generated_at_utc=NOW,
        regime_state="TREND",
        promoted_sleeve_library=library,
        trade_candidates=packet["trade_candidates"],
    )

    assert mismatched["trade_candidates"][0]["actionable"] is False
    assert "UNPROMOTED_SLEEVE_SOURCE" in mismatched["trade_candidates"][0]["do_not_trade_blockers"]


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
        deviations_from_recommendation=["none"],
    )
    ledger = build_outcome_ledger_v1(
        generated_at_utc=NOW,
        outcome_rows=[
            {
                "trade_id": "trade-1",
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "hypothesis_id": "hyp-1",
                "recommended_entry": "520.00",
                "actual_entry": "520.10",
                "recommended_stop": "514.90",
                "actual_stop": "514.90",
                "exit_price": "",
                "return_pct": "",
                "risk_adjusted_return": "",
                "max_adverse_excursion": "",
                "max_favorable_excursion": "",
                "outcome_status": "open",
                "failure_reason": "",
                "operator_deviation": "none",
                "notes": "pending",
                "sleeve_attribution": "pending",
                "edge_overlap_attribution": "pending",
            }
        ],
    )

    validate_research_lab_artifact_v1(receipt)
    validate_research_lab_artifact_v1(ledger)
    assert receipt["manual_observation_only"] is True
    assert receipt["deviations_from_recommendation"] == ["none"]
    assert receipt["broker_submit_required"] is False
    assert ledger["feeds_research_lab"] is True
    assert ledger["runtime_mutation_allowed"] is False


def test_outcome_ledger_can_enqueue_learning_loop_tasks() -> None:
    ledger = build_outcome_ledger_v1(
        generated_at_utc=NOW,
        outcome_rows=[
            {
                "trade_id": "trade-1",
                "sleeve_id": "C2_TREND_EQ_PRIMARY",
                "hypothesis_id": "hyp-1",
                "outcome_status": "stopped_out",
                "failure_reason": "regime shift and overlap loss",
                "operator_deviation": "manual delay",
            }
        ],
    )
    queue = build_learning_tasks_from_outcomes_v1(generated_at_utc=NOW, outcome_ledger=ledger)

    validate_research_lab_artifact_v1(queue)
    task_types = {task["task_type"] for task in queue["tasks"]}
    assert {"sleeve_failure_review", "friction_test", "edge_overlap_review", "regime_test"}.issubset(task_types)
    assert all(task["output_expected"] == "experiment_result.v1" for task in queue["tasks"])
