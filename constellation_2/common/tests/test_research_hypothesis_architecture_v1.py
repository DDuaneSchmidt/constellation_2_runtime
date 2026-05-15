from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    apply_research_result_to_hypothesis_v1,
    build_manual_trade_packet_v1,
    build_research_architecture_integrity_review_v1,
    build_research_conclusion_v1,
    build_research_evidence_packet_v1,
    build_research_failure_archetype_v1,
    build_research_hypothesis_v1,
    build_research_inbox_item_v1,
    build_research_knowledge_graph_from_artifacts_v1,
    build_research_knowledge_graph_v1,
    build_research_program_v1,
    build_research_queue_task_v1,
    build_research_result_ledger_v1,
    build_research_result_v1,
    build_research_task_queue_v1,
    build_research_to_lite_promotion_v1,
    convert_inbox_item_to_research_hypothesis_v1,
    supersede_research_conclusion_v1,
    taxonomy_warnings_for_research_hypotheses_v1,
    reject_invalidated_hypothesis_for_promotion_v1,
    validate_research_lifecycle_transition_v1,
    validate_research_lab_artifact_v1,
    write_research_lab_artifact_v1,
)
from ops.tools.research_architecture_integrity_review_v1 import main as integrity_main  # noqa: E402
from ops.tools.hypothesis_registry_to_research_hypothesis_adapter_v1 import main as adapter_main  # noqa: E402
from ops.tools.ingest_research_hypotheses_v1 import main as ingest_main  # noqa: E402
from ops.tools.ingest_trade_outcome_attribution_to_research_v1 import main as feedback_main  # noqa: E402
from ops.tools.run_research_lab_task_queue_v1 import main as run_queue_main  # noqa: E402


NOW = "2026-05-14T21:30:00Z"
DAY = "2026-05-14"


def _hypothesis(**overrides: object) -> dict[str, object]:
    base = {
        "hypothesis_id": "rh-test-001",
        "created_at_utc": NOW,
        "title": "Risk compression fails after breadth divergence",
        "hypothesis_summary": "Compression regimes with narrow breadth have weaker upside follow-through.",
        "market_thesis": "Narrow participation makes index strength fragile.",
        "edge_family": "BREADTH_FRAGILITY",
        "behavioral_state": "late trend complacency",
        "expected_regime": "compression",
        "expected_direction": "down",
        "expected_holding_period": "1-5 sessions",
        "instruments": ["SPY", "QQQ"],
        "rationale": "Breadth divergence can expose crowded index strength.",
        "expected_behavior": "Failed breakout or lower forward return after signal.",
        "failure_conditions": ["participation broadens"],
        "invalidation_conditions": ["positive expectancy absent after costs"],
        "related_sleeves": [],
        "related_research_refs": [],
        "confidence_level": "LOW",
        "status": "IDEA",
        "source": "MANUAL",
        "notes": "Offline seed.",
    }
    base.update(overrides)
    return build_research_hypothesis_v1(**base)


def _task(**overrides: object) -> dict[str, object]:
    base = {
        "task_id": "task-rh-test-001-definition",
        "hypothesis_id": "rh-test-001",
        "task_type": "DEFINITION_CHECK",
        "priority": "NORMAL",
        "status": "QUEUED",
        "required_inputs": ["research_hypothesis.v1:rh-test-001"],
        "output_artifact_refs": [],
        "blocker_reason_codes": [],
        "created_at_utc": NOW,
        "updated_at_utc": NOW,
    }
    base.update(overrides)
    return build_research_queue_task_v1(**base)


def _result(**overrides: object) -> dict[str, object]:
    base = {
        "result_id": "result-rh-test-001-definition",
        "hypothesis_id": "rh-test-001",
        "task_id": "task-rh-test-001-definition",
        "result_status": "NEEDS_MORE_RESEARCH",
        "evidence_refs": [],
        "conclusion_summary": "Definition needs deterministic validation.",
        "confidence_before": "LOW",
        "confidence_after": "LOW",
        "confidence_change": "NONE",
        "metrics_summary": {},
        "failure_mode_notes": "",
        "invalidation_notes": "",
        "next_recommended_task": "DATA_AVAILABILITY_CHECK",
        "promotion_recommendation": "CONTINUE_RESEARCH",
        "created_at_utc": NOW,
    }
    base.update(overrides)
    return build_research_result_v1(**base)


def test_research_hypothesis_is_durable_artifact(tmp_path: Path) -> None:
    hypothesis = _hypothesis()

    validate_research_lab_artifact_v1(hypothesis)
    path = write_research_lab_artifact_v1(truth_root=tmp_path, day_utc=DAY, payload=hypothesis)
    persisted = json.loads(path.read_text(encoding="utf-8"))

    assert persisted["hypothesis_id"] == "rh-test-001"
    assert persisted["status"] == "IDEA"
    assert persisted["research_lab_only"] is True
    assert persisted["trade_authorization_allowed"] is False
    assert persisted["broker_submit_required"] is False


def test_tasks_are_separate_from_hypotheses() -> None:
    hypothesis = _hypothesis()
    task = _task()
    queue = build_research_task_queue_v1(generated_at_utc=NOW, tasks=[task])

    validate_research_lab_artifact_v1(hypothesis)
    validate_research_lab_artifact_v1(queue)
    assert "tasks" not in hypothesis
    assert queue["tasks"][0]["hypothesis_id"] == hypothesis["hypothesis_id"]
    assert queue["tasks"][0]["status"] == "QUEUED"


def test_task_queue_controls_execution_and_runner_writes_result_ledger(tmp_path: Path) -> None:
    write_research_lab_artifact_v1(truth_root=tmp_path, day_utc=DAY, payload=_hypothesis())
    queue = build_research_task_queue_v1(generated_at_utc=NOW, tasks=[_task()])
    write_research_lab_artifact_v1(truth_root=tmp_path, day_utc=DAY, payload=queue)

    rc = run_queue_main(["--truth_root", str(tmp_path), "--day_utc", DAY, "--max_tasks", "1"])

    assert rc == 0
    queue_path = tmp_path / "research_lab" / "research_task_queue_v1" / DAY / "index" / "research_task_queue.v1.json"
    ledger_path = tmp_path / "research_lab" / "research_result_ledger_v1" / DAY / "index" / "research_result_ledger.v1.json"
    updated_queue = json.loads(queue_path.read_text(encoding="utf-8"))
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert updated_queue["tasks"][0]["status"] == "COMPLETED"
    assert updated_queue["tasks"][1]["task_type"] == "DATA_AVAILABILITY_CHECK"
    assert ledger["results"][0]["result_status"] == "SUPPORTS_HYPOTHESIS"
    assert ledger["runtime_mutation_allowed"] is False
    assert ledger["trade_authorization_allowed"] is False
    assert list((tmp_path / "research_lab" / "research_evidence_packet_v1").rglob("research_evidence_packet.v1.json"))


def test_draft_hypotheses_do_not_affect_lite() -> None:
    hypothesis = _hypothesis(status="IDEA")
    packet = build_manual_trade_packet_v1(
        packet_id="packet-1",
        run_id="run-1",
        date=DAY,
        generated_at_utc=NOW,
        regime_state="UNKNOWN",
        trade_candidates=[],
        promoted_sleeve_library=None,
    )

    validate_research_lab_artifact_v1(hypothesis)
    assert hypothesis["runtime_mutation_allowed"] is False
    assert packet["trade_candidates"] == []
    assert packet["manual_execution_only"] is True
    assert packet["broker_submit_required"] is False


def test_completed_tasks_write_result_ledger_entries() -> None:
    result = _result(result_status="SUPPORTS_HYPOTHESIS", promotion_recommendation="CONTINUE_RESEARCH")
    ledger = build_research_result_ledger_v1(generated_at_utc=NOW, results=[result])

    validate_research_lab_artifact_v1(ledger)
    assert ledger["results"][0]["task_id"] == "task-rh-test-001-definition"
    assert ledger["results"][0]["promotion_recommendation"] == "CONTINUE_RESEARCH"


def test_failed_hypotheses_are_preserved() -> None:
    result = _result(
        result_status="INVALIDATED",
        promotion_recommendation="REJECT",
        invalidation_notes="No positive expectancy after costs.",
    )
    ledger = build_research_result_ledger_v1(generated_at_utc=NOW, results=[result])

    validate_research_lab_artifact_v1(ledger)
    assert len(ledger["results"]) == 1
    assert ledger["results"][0]["result_status"] == "INVALIDATED"
    assert ledger["results"][0]["invalidation_notes"]


def test_invalidated_hypothesis_cannot_be_promoted() -> None:
    hypothesis = _hypothesis(status="PROMOTION_CANDIDATE")
    ledger = build_research_result_ledger_v1(
        generated_at_utc=NOW,
        results=[_result(result_status="INVALIDATED", promotion_recommendation="REJECT")],
    )

    with pytest.raises(ValueError, match="INVALIDATED_HYPOTHESIS_CANNOT_BE_PROMOTED"):
        reject_invalidated_hypothesis_for_promotion_v1(hypothesis, ledger)


def test_research_artifacts_do_not_require_ib_or_broker_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IB_GATEWAY_HOST", raising=False)
    monkeypatch.delenv("IB_ACCOUNT", raising=False)
    hypothesis = _hypothesis()
    queue = build_research_task_queue_v1(generated_at_utc=NOW, tasks=[_task()])
    ledger = build_research_result_ledger_v1(generated_at_utc=NOW, results=[_result()])

    for artifact in (hypothesis, queue, ledger):
        validate_research_lab_artifact_v1(artifact)
        assert artifact["broker_submit_required"] is False


def test_research_artifacts_do_not_mutate_lite_runtime(tmp_path: Path) -> None:
    write_research_lab_artifact_v1(truth_root=tmp_path, day_utc=DAY, payload=_hypothesis())
    write_research_lab_artifact_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        payload=build_research_result_ledger_v1(generated_at_utc=NOW, results=[_result()]),
    )

    assert (tmp_path / "research_lab").exists()
    assert not (tmp_path / "aegis_lite").exists()
    assert not (tmp_path / "broker").exists()


def test_knowledge_graph_scaffold_is_non_authoritative() -> None:
    graph = build_research_knowledge_graph_v1(
        generated_at_utc=NOW,
        hypothesis_refs=["rh-test-001"],
        edge_family_refs=["BREADTH_FRAGILITY"],
        regime_refs=["compression"],
        sleeve_refs=[],
        evidence_refs=[],
        failure_mode_refs=["participation broadens"],
    )

    validate_research_lab_artifact_v1(graph)
    assert graph["non_authoritative_scaffold"] is True
    assert graph["runtime_mutation_allowed"] is False
    assert graph["automatic_promotion_allowed"] is False


def test_seed_text_ingest_creates_explicit_hypothesis_only(tmp_path: Path) -> None:
    rc = ingest_main(
        [
            "--truth_root",
            str(tmp_path),
            "--day_utc",
            DAY,
            "--created_at_utc",
            NOW,
            "--title",
            "Explicit seed",
            "--seed_text",
            "Only this provided text becomes the hypothesis summary.",
            "--source",
            "CHATGPT_SEED",
        ]
    )

    assert rc == 0
    paths = list((tmp_path / "research_lab" / "research_hypothesis_v1").rglob("research_hypothesis.v1.json"))
    assert len(paths) == 1
    payload = json.loads(paths[0].read_text(encoding="utf-8"))
    assert payload["source"] == "CHATGPT_SEED"
    assert payload["hypothesis_summary"] == "Only this provided text becomes the hypothesis summary."


def test_legacy_registry_adapter_is_one_way_and_not_authoritative(tmp_path: Path) -> None:
    registry_path = tmp_path / "legacy_registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "hypotheses": [
                    {
                        "hypothesis_id": "legacy-hyp-1",
                        "title": "Legacy thesis",
                        "behavioral_thesis": "Legacy behavioral thesis.",
                        "edge_family": "LEGACY_EDGE",
                        "market_regime": "TREND",
                        "trigger_conditions": "legacy trigger",
                        "expected_outcome": "legacy outcome",
                        "failure_modes": ["legacy failure"],
                        "instrument_universe": ["SPY"],
                        "time_horizon": "1D",
                        "related_hypotheses": [],
                        "lifecycle_state": "proposed",
                        "created_at": NOW,
                        "latest_result_status": "",
                        "notes": "",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    rc = adapter_main(["--registry_json", str(registry_path), "--truth_root", str(tmp_path), "--day_utc", DAY, "--created_at_utc", NOW])

    assert rc == 0
    adapted = list((tmp_path / "research_lab" / "research_hypothesis_v1").rglob("research_hypothesis.v1.json"))
    assert len(adapted) == 1
    payload = json.loads(adapted[0].read_text(encoding="utf-8"))
    assert payload["schema_id"] == "research_hypothesis"
    assert payload["source"] == "RESEARCH_LAB"
    assert "Adapted one-way" in payload["notes"]


def test_apply_research_result_updates_hypothesis_status_and_confidence() -> None:
    hypothesis = _hypothesis(confidence_level="LOW")
    evidence_ref = "/tmp/evidence.json"
    result = _result(
        result_status="SUPPORTS_HYPOTHESIS",
        evidence_refs=[evidence_ref],
        confidence_before="LOW",
        confidence_after="MEDIUM",
        promotion_recommendation="PROMOTION_CANDIDATE",
    )

    transition = apply_research_result_to_hypothesis_v1(hypothesis=hypothesis, result=result, evidence_packet_refs=[evidence_ref])

    assert transition["updated_hypothesis"]["status"] == "VALIDATED_RESEARCH"
    assert transition["confidence_before"] == "LOW"
    assert transition["confidence_after"] == "MEDIUM"
    assert "EVIDENCE_SUPPORTS_HYPOTHESIS" in transition["transition_reason_codes"]


def test_apply_research_result_blocks_invalid_promotion_semantics() -> None:
    with pytest.raises(ValueError, match="INVALIDATED_HYPOTHESIS_CANNOT_BE_PROMOTION_CANDIDATE"):
        apply_research_result_to_hypothesis_v1(
            hypothesis=_hypothesis(),
            result=_result(result_status="INVALIDATED", promotion_recommendation="PROMOTION_CANDIDATE"),
            evidence_packet_refs=[],
        )

    with pytest.raises(ValueError, match="REJECTED_HYPOTHESIS_CANNOT_BE_PROMOTED"):
        apply_research_result_to_hypothesis_v1(
            hypothesis=_hypothesis(status="REJECTED"),
            result=_result(result_status="SUPPORTS_HYPOTHESIS", promotion_recommendation="PROMOTION_CANDIDATE", evidence_refs=["/tmp/evidence.json"]),
            evidence_packet_refs=["/tmp/evidence.json"],
        )


def test_promotion_requires_result_ledger_and_human_approval() -> None:
    evidence_refs = [{"artifact_type": "research_evidence_packet_v1", "path": "/tmp/evidence.json"}]
    result_refs = [{"artifact_type": "research_result_ledger_v1", "path": "/tmp/result_ledger.json"}]

    with pytest.raises(ValueError, match="PROMOTION_REQUIRES_RESULT_LEDGER_REFS"):
        build_research_to_lite_promotion_v1(
            promotion_id="promo-1",
            research_id="research-1",
            hypothesis_id="rh-test-001",
            proposed_lite_component_type="SLEEVE",
            promotion_status="APPROVED_FOR_LITE_IMPLEMENTATION",
            evidence_packet_refs=evidence_refs,
            validation_summary="ok",
            regime_evidence="ok",
            expectancy_evidence="ok",
            failure_mode_review="ok",
            governance_compatibility_review="ok",
            risk_contract_review="ok",
            stop_logic_review="ok",
            manual_execution_compatibility="ok",
            operator_clarity_review="ok",
            implementation_notes="requires source implementation",
            required_tests=["unit"],
            approval_reason_codes=["HUMAN_APPROVED"],
            rejection_reason_codes=[],
            approved_by_human=True,
            created_at_utc=NOW,
            result_ledger_refs=[],
        )

    with pytest.raises(ValueError, match="APPROVED_FOR_LITE_IMPLEMENTATION_REQUIRES_APPROVED_BY_HUMAN"):
        build_research_to_lite_promotion_v1(
            promotion_id="promo-2",
            research_id="research-1",
            hypothesis_id="rh-test-001",
            proposed_lite_component_type="SLEEVE",
            promotion_status="APPROVED_FOR_LITE_IMPLEMENTATION",
            evidence_packet_refs=evidence_refs,
            result_ledger_refs=result_refs,
            validation_summary="ok",
            regime_evidence="ok",
            expectancy_evidence="ok",
            failure_mode_review="ok",
            governance_compatibility_review="ok",
            risk_contract_review="ok",
            stop_logic_review="ok",
            manual_execution_compatibility="ok",
            operator_clarity_review="ok",
            implementation_notes="requires source implementation",
            required_tests=["unit"],
            approval_reason_codes=[],
            rejection_reason_codes=[],
            approved_by_human=False,
            created_at_utc=NOW,
        )


def test_lite_feedback_ingestion_creates_research_result_and_task_without_lite_mutation(tmp_path: Path) -> None:
    attribution_path = tmp_path / "trade_outcome_attribution.v1.json"
    attribution_path.write_text(
        json.dumps(
            {
                "schema_id": "trade_outcome_attribution",
                "schema_version": "v1",
                "artifact_id": "trade_outcome_attribution_v1",
                "day_utc": DAY,
                "run_id": "run-1",
                "attributions": [
                    {
                        "candidate_id": "cand-1",
                        "run_id": "run-1",
                        "sleeve_id": "sleeve-1",
                        "edge_cluster_id": "edge-1",
                        "recommended_entry_reference": "100",
                        "actual_entry_price": "",
                        "actual_exit_price": "",
                        "model_forward_returns": ["+0.5pct"],
                        "realized_pnl": "",
                        "unrealized_pnl": "",
                        "MAE": "",
                        "MFE": "",
                        "operator_slippage": "",
                        "skipped_trade_outcome": "would_have_won",
                        "governance_adjustment_effect": "size_reduction_helped",
                        "sleeve_signal_quality": "GOOD",
                        "implementation_quality": "N/A",
                        "operator_execution_quality": "SKIPPED",
                    }
                ],
                "ib_execution_required": False,
                "source_artifact_lineage": [],
                "canonical_json_hash": "0" * 64,
            }
        ),
        encoding="utf-8",
    )

    rc = feedback_main(
        [
            "--truth_root",
            str(tmp_path),
            "--day_utc",
            DAY,
            "--trade_outcome_attribution_json",
            str(attribution_path),
            "--hypothesis_id",
            "rh-test-001",
            "--generated_at_utc",
            NOW,
        ]
    )

    assert rc == 0
    ledger = json.loads((tmp_path / "research_lab" / "research_result_ledger_v1" / DAY / "index" / "research_result_ledger.v1.json").read_text(encoding="utf-8"))
    queue = json.loads((tmp_path / "research_lab" / "research_task_queue_v1" / DAY / "index" / "research_task_queue.v1.json").read_text(encoding="utf-8"))
    assert ledger["results"][0]["result_status"] == "WEAK_SUPPORT"
    assert queue["tasks"][0]["task_type"] == "FORWARD_OBSERVATION"
    assert not (tmp_path / "reports" / "operator_execution_queue_v1").exists()


def test_taxonomy_warnings_detect_unmapped_and_duplicate_hypotheses() -> None:
    taxonomy = {
        "edge_families": ["BREADTH_FRAGILITY"],
        "edge_clusters": ["EDGE_A"],
        "deprecated_terms": ["OLD_EDGE"],
        "duplicate_term_warnings": [],
    }
    warnings = taxonomy_warnings_for_research_hypotheses_v1(
        taxonomy=taxonomy,
        hypotheses=[
            _hypothesis(hypothesis_id="h1", title="Same", edge_family="OLD_EDGE", related_research_refs=["EDGE_CLUSTER:missing"]),
            _hypothesis(hypothesis_id="h2", title="Same", edge_family="UNMAPPED_EDGE"),
        ],
    )

    assert any(item.startswith("DUPLICATE_HYPOTHESIS_TITLE") for item in warnings)
    assert any(item.startswith("DEPRECATED_EDGE_FAMILY") for item in warnings)
    assert any(item.startswith("UNMAPPED_EDGE_CLUSTER") for item in warnings)


def test_generated_knowledge_graph_is_index_only() -> None:
    evidence = build_research_evidence_packet_v1(
        research_id="evidence-1",
        hypothesis_id="rh-test-001",
        title="Evidence",
        hypothesis_description="Description",
        research_type="EDGE",
        created_at_utc=NOW,
        source_data_summary="fixture",
        replay_window="fixture",
        instruments_tested=["SPY"],
        regimes_tested=["TREND"],
        edge_family="BREADTH_FRAGILITY",
        expected_holding_period="1D",
        methodology_summary="fixture",
        metrics_summary={"expectancy": "0.1"},
        expectancy_summary="positive",
        drawdown_summary="bounded",
        MAE_MFE_summary="reviewed",
        failure_modes=[],
        known_limitations=[],
        reproducibility_notes="fixture",
        artifact_lineage=[],
        research_status="UNDER_REVIEW",
    )
    graph = build_research_knowledge_graph_from_artifacts_v1(
        generated_at_utc=NOW,
        hypotheses=[_hypothesis()],
        evidence_packets=[evidence],
        result_ledgers=[build_research_result_ledger_v1(generated_at_utc=NOW, results=[_result()])],
        promotions=[],
    )

    validate_research_lab_artifact_v1(graph)
    assert graph["non_authoritative_scaffold"] is True
    assert {"from": "rh-test-001", "to": "evidence-1", "edge_type": "HAS_EVIDENCE"} in graph["edges"]
    assert graph["runtime_mutation_allowed"] is False


def test_inbox_item_is_not_hypothesis_and_conversion_is_explicit() -> None:
    inbox = build_research_inbox_item_v1(
        inbox_id="inbox-1",
        created_at_utc=NOW,
        source="CHATGPT",
        raw_text="Raw idea about breadth fragility.",
        tags=["breadth"],
        related_symbols=["SPY"],
        related_edge_family="BREADTH_FRAGILITY",
        suggested_program="program-breadth",
    )

    validate_research_lab_artifact_v1(inbox)
    assert inbox["schema_id"] == "research_inbox_item"
    assert inbox["can_authorize_research_execution"] is False
    assert inbox["automatic_promotion_allowed"] is False

    converted = convert_inbox_item_to_research_hypothesis_v1(
        inbox_item=inbox,
        hypothesis_id="rh-from-inbox",
        created_at_utc=NOW,
        title="Breadth fragility idea",
        hypothesis_summary="Breadth fragility weakens forward return.",
        failure_conditions=["breadth recovers"],
        invalidation_conditions=["no expectancy after costs"],
    )

    assert converted["inbox_item"]["triage_status"] == "CONVERTED_TO_HYPOTHESIS"
    assert converted["hypothesis"]["schema_id"] == "research_hypothesis"
    assert converted["hypothesis"]["status"] == "IDEA"


def test_programs_organize_but_do_not_authorize() -> None:
    program = build_research_program_v1(
        program_id="program-breadth",
        title="Breadth fragility",
        thesis="Narrow participation weakens index follow-through.",
        related_hypotheses=["rh-test-001"],
        edge_families=["BREADTH_FRAGILITY"],
        regimes=["compression"],
        instruments=["SPY"],
        current_open_questions=["Does expectancy survive costs?"],
        created_at_utc=NOW,
    )

    validate_research_lab_artifact_v1(program)
    assert program["can_authorize_promotion"] is False
    assert program["runtime_mutation_allowed"] is False


def _conclusion(**overrides: object) -> dict[str, object]:
    base = {
        "conclusion_id": "conclusion-1",
        "hypothesis_id": "rh-test-001",
        "program_id": "program-breadth",
        "created_at_utc": NOW,
        "conclusion_summary": "Breadth fragility remains plausible but limited.",
        "supporting_evidence_refs": ["/tmp/evidence.json"],
        "contradictory_evidence_refs": [],
        "result_ledger_refs": ["/tmp/result_ledger.json"],
        "confidence_level": "MEDIUM",
        "confidence_trend": "UP",
        "regime_specificity": "compression",
        "edge_family": "BREADTH_FRAGILITY",
        "operational_implications": ["research only"],
        "known_failure_modes": ["breadth recovery"],
        "limitations": ["fixture sample"],
        "invalidation_conditions": ["expectancy absent"],
        "reproducibility_notes": "Fixture inputs are pinned.",
        "methodology_version": "test-method-v1",
        "data_snapshot_refs": ["/tmp/snapshot.json"],
        "code_version": "test-commit",
        "created_by": "pytest",
        "artifact_lineage": [{"artifact_type": "research_result_ledger_v1", "path": "/tmp/result_ledger.json"}],
        "reason_codes": ["SUPPORTED_BY_RESULT_LEDGER"],
    }
    base.update(overrides)
    return build_research_conclusion_v1(**base)


def test_conclusion_requires_evidence_and_result_refs() -> None:
    with pytest.raises(ValueError, match="RESEARCH_CONCLUSION_REQUIRES_EVIDENCE_REFS"):
        _conclusion(supporting_evidence_refs=[])

    with pytest.raises(ValueError, match="RESEARCH_CONCLUSION_REQUIRES_RESULT_LEDGER_REFS"):
        _conclusion(result_ledger_refs=[])

    conclusion = _conclusion()
    validate_research_lab_artifact_v1(conclusion)
    assert conclusion["immutable_artifact"] is True
    assert conclusion["trade_authorization_allowed"] is False


def test_conclusion_supersession_preserves_old_conclusion() -> None:
    prior = _conclusion(conclusion_id="conclusion-old")
    new = _conclusion(conclusion_id="conclusion-new")

    supersession = supersede_research_conclusion_v1(prior_conclusion=prior, new_conclusion=new)

    assert prior["conclusion_status"] == "ACTIVE"
    assert supersession["prior_conclusion"]["conclusion_status"] == "SUPERSEDED"
    assert supersession["prior_conclusion"]["superseded_by_conclusion_id"] == "conclusion-new"
    assert "conclusion-old" in supersession["new_conclusion"]["supersedes_conclusion_ids"]


def test_failure_archetype_does_not_create_governance_rule() -> None:
    archetype = build_research_failure_archetype_v1(
        failure_archetype_id="failure-breadth-recovery",
        title="Breadth recovery invalidates fragility",
        description="The edge tends to fail when participation broadens.",
        related_hypotheses=["rh-test-001"],
        related_edge_families=["BREADTH_FRAGILITY"],
        failure_conditions=["breadth recovers"],
        observable_warning_signs=["advance-decline improves"],
        example_evidence_refs=["/tmp/evidence.json"],
        governance_implications=["consider separate promotion review"],
        stop_risk_implications=["do not auto-tighten stops"],
        created_at_utc=NOW,
    )

    validate_research_lab_artifact_v1(archetype)
    assert archetype["advisory_research_memory_only"] is True
    assert archetype["governance_rule_created"] is False


def test_invalid_lifecycle_transitions_fail_closed() -> None:
    with pytest.raises(ValueError, match="IDEA_CANNOT_BECOME_VALIDATED_RESEARCH_WITHOUT_EVIDENCE_REFS"):
        validate_research_lifecycle_transition_v1(from_status="IDEA", to_status="VALIDATED_RESEARCH")

    with pytest.raises(ValueError, match="PROMOTION_CANDIDATE_REQUIRES_VALIDATED_RESEARCH_SOURCE"):
        validate_research_lifecycle_transition_v1(
            from_status="IDEA",
            to_status="PROMOTION_CANDIDATE",
            evidence_refs=["/tmp/evidence.json"],
            result_ledger_refs=["/tmp/result.json"],
        )

    with pytest.raises(ValueError, match="APPROVED_FOR_LITE_REQUIRES_PROMOTION_ARTIFACT"):
        validate_research_lifecycle_transition_v1(
            from_status="VALIDATED_RESEARCH",
            to_status="APPROVED_FOR_LITE",
            evidence_refs=["/tmp/evidence.json"],
            result_ledger_refs=["/tmp/result.json"],
        )


def test_deterministic_task_ordering_by_priority_created_at_and_id() -> None:
    queue = build_research_task_queue_v1(
        generated_at_utc=NOW,
        tasks=[
            _task(task_id="task-c", priority="LOW", created_at_utc=NOW),
            _task(task_id="task-b", priority="HIGH", created_at_utc=NOW),
            _task(task_id="task-a", priority="HIGH", created_at_utc=NOW),
        ],
    )

    assert [row["task_id"] for row in queue["tasks"]] == ["task-a", "task-b", "task-c"]


def test_taxonomy_warnings_include_program_ambiguity_and_conflicting_thesis() -> None:
    warnings = taxonomy_warnings_for_research_hypotheses_v1(
        taxonomy={"edge_families": ["BREADTH_FRAGILITY"], "edge_clusters": [], "deprecated_terms": [], "duplicate_term_warnings": []},
        hypotheses=[
            _hypothesis(hypothesis_id="h1", title="One", market_thesis="thesis a"),
            _hypothesis(hypothesis_id="h2", title="Two", market_thesis="thesis b"),
        ],
        programs=[
            build_research_program_v1(program_id="p1", title="P1", thesis="t", related_hypotheses=["h1"], created_at_utc=NOW),
            build_research_program_v1(program_id="p2", title="P2", thesis="t", related_hypotheses=["h1"], created_at_utc=NOW),
        ],
    )

    assert any(item.startswith("AMBIGUOUS_PROGRAM_ASSIGNMENT:h1") for item in warnings)
    assert any(item.startswith("CONFLICTING_THESIS_LABELS:BREADTH_FRAGILITY") for item in warnings)


def test_integrity_review_catches_missing_lineage_and_unsafe_promotion() -> None:
    unsafe_result = _result(
        result_status="SUPPORTS_HYPOTHESIS",
        evidence_refs=["/tmp/evidence.json"],
        artifact_lineage=[],
        reason_codes=[],
        reproducibility_notes="",
    )
    review = build_research_architecture_integrity_review_v1(
        review_id="review-1",
        generated_at_utc=NOW,
        hypotheses=[_hypothesis(status="REJECTED")],
        result_ledgers=[build_research_result_ledger_v1(generated_at_utc=NOW, results=[unsafe_result])],
        promotions=[
            build_research_to_lite_promotion_v1(
                promotion_id="promo-unsafe",
                research_id="research-1",
                hypothesis_id="rh-test-001",
                proposed_lite_component_type="SLEEVE",
                promotion_status="REJECTED",
                evidence_packet_refs=[{"artifact_type": "research_evidence_packet_v1", "path": "/tmp/evidence.json"}],
                result_ledger_refs=[{"artifact_type": "research_result_ledger_v1", "path": "/tmp/result_ledger.json"}],
                validation_summary="not approved",
                regime_evidence="n/a",
                expectancy_evidence="n/a",
                failure_mode_review="n/a",
                governance_compatibility_review="n/a",
                risk_contract_review="n/a",
                stop_logic_review="n/a",
                manual_execution_compatibility="n/a",
                operator_clarity_review="n/a",
                implementation_notes="rejected",
                required_tests=[],
                approval_reason_codes=[],
                rejection_reason_codes=["REJECTED"],
                approved_by_human=False,
                created_at_utc=NOW,
                source_research_status="VALIDATED_RESEARCH",
            )
        ],
    )

    validate_research_lab_artifact_v1(review)
    assert review["review_status"] == "FAIL"
    assert any(item.startswith("RESULT_MISSING_LINEAGE") for item in review["blockers"])
    assert any(item.startswith("PROMOTION_REFERENCES_NON_PROMOTABLE_HYPOTHESIS") for item in review["blockers"])


def test_integrity_review_tool_writes_structured_status(tmp_path: Path) -> None:
    write_research_lab_artifact_v1(truth_root=tmp_path, day_utc=DAY, payload=_hypothesis())
    rc = integrity_main(["--truth_root", str(tmp_path), "--day_utc", DAY, "--generated_at_utc", NOW])

    assert rc == 0
    review_paths = list((tmp_path / "research_lab" / "research_architecture_integrity_review_v1").rglob("research_architecture_integrity_review.v1.json"))
    assert review_paths
    payload = json.loads(review_paths[0].read_text(encoding="utf-8"))
    assert payload["broker_submit_required"] is False
