from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATHS = {
    "research_evidence_packet": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_evidence_packet.v1.schema.json",
    "research_to_lite_promotion": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_to_lite_promotion.v1.schema.json",
    "research_lab_index": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_lab_index.v1.schema.json",
    "edge_taxonomy": "governance/04_DATA/SCHEMAS/C2/REPORTS/edge_taxonomy.v1.schema.json",
    "hypothesis_registry": "governance/04_DATA/SCHEMAS/C2/REPORTS/hypothesis_registry.v1.schema.json",
    "research_task_queue": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_task_queue.v1.schema.json",
    "experiment_result": "governance/04_DATA/SCHEMAS/C2/REPORTS/experiment_result.v1.schema.json",
    "research_experiment_result": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_experiment_result.v1.schema.json",
    "hypothesis_test_plan": "governance/04_DATA/SCHEMAS/C2/REPORTS/hypothesis_test_plan.v1.schema.json",
    "hypothesis_progress_report": "governance/04_DATA/SCHEMAS/C2/REPORTS/hypothesis_progress_report.v1.schema.json",
    "research_lab_awareness_report": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_lab_awareness_report.v1.schema.json",
    "promotion_review": "governance/04_DATA/SCHEMAS/C2/REPORTS/promotion_review.v1.schema.json",
    "promoted_sleeve_library": "governance/04_DATA/SCHEMAS/C2/REPORTS/promoted_sleeve_library.v1.schema.json",
    "manual_trade_packet": "governance/04_DATA/SCHEMAS/C2/REPORTS/manual_trade_packet.v1.schema.json",
    "manual_execution_receipt": "governance/04_DATA/SCHEMAS/C2/REPORTS/manual_execution_receipt.v1.schema.json",
    "outcome_ledger": "governance/04_DATA/SCHEMAS/C2/REPORTS/outcome_ledger.v1.schema.json",
}
HYPOTHESIS_LIFECYCLE_STATES = {
    "proposed",
    "definition_ready",
    "duplicate_checked",
    "exploratory_testing",
    "regime_testing",
    "robustness_testing",
    "friction_testing",
    "out_of_sample_testing",
    "failure_mode_review",
    "edge_overlap_review",
    "validated_candidate",
    "promotion_review",
    "promoted",
    "rejected",
    "insufficient_data",
    "regime_dependent",
    "duplicate_of_existing_edge",
    "retired",
}
PROMOTION_STATES_LC = {"not_ready", "candidate", "approved_for_lite_review", "approved_for_lite_implementation", "rejected", "deferred"}
TASK_TYPES = {
    "definition_check",
    "duplicate_review",
    "exploratory_test",
    "regime_test",
    "robustness_test",
    "friction_test",
    "out_of_sample_test",
    "failure_review",
    "edge_overlap_review",
    "promotion_review",
    "retest",
    "sleeve_failure_review",
    "anomaly_review",
    "retirement_review",
}
TASK_STATUSES = {"open", "completed", "blocked", "failed"}
RESULT_STATUSES = {
    "insufficient_data",
    "weak",
    "promising",
    "regime_dependent",
    "overlapping",
    "duplicate",
    "rejected",
    "validated_candidate",
}
TEST_STAGES = [
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
]
TERMINAL_TEST_STATES = {
    "rejected",
    "insufficient_data",
    "regime_dependent",
    "validated_candidate",
    "duplicate_of_existing_edge",
    "promoted",
    "retired",
}
STAGE_TASK_TYPES = {
    "definition_check": "definition_check",
    "duplicate_overlap_check": "duplicate_review",
    "exploratory_backtest": "exploratory_test",
    "regime_segmentation": "regime_test",
    "robustness_check": "robustness_test",
    "transaction_friction_check": "friction_test",
    "out_of_sample_check": "out_of_sample_test",
    "failure_mode_review": "failure_review",
    "edge_overlap_review": "edge_overlap_review",
    "promotion_review": "promotion_review",
}
STAGE_LIFECYCLE_STATES = {
    "definition_check": "definition_ready",
    "duplicate_overlap_check": "duplicate_checked",
    "exploratory_backtest": "exploratory_testing",
    "regime_segmentation": "regime_testing",
    "robustness_check": "robustness_testing",
    "transaction_friction_check": "friction_testing",
    "out_of_sample_check": "out_of_sample_testing",
    "failure_mode_review": "failure_mode_review",
    "edge_overlap_review": "edge_overlap_review",
    "promotion_review": "promotion_review",
}
RESEARCH_TYPES = {
    "SLEEVE",
    "EDGE",
    "REGIME",
    "GOVERNANCE",
    "OVERLAP",
    "STOP_LOGIC",
    "RISK_SIZING",
    "BEHAVIORAL_STATE",
}
RESEARCH_STATUSES = {"DRAFT", "UNDER_REVIEW", "VALIDATED_RESEARCH", "REJECTED", "ARCHIVED"}
PROMOTION_COMPONENT_TYPES = {
    "SLEEVE",
    "EDGE_CLUSTER_RULE",
    "REGIME_SIGNAL",
    "GOVERNANCE_RULE",
    "STOP_LOGIC",
    "RISK_RULE",
    "REPORT_FIELD",
}
PROMOTION_STATUSES = {
    "NOT_READY",
    "CANDIDATE",
    "APPROVED_FOR_LITE_REVIEW",
    "APPROVED_FOR_LITE_IMPLEMENTATION",
    "REJECTED",
    "DEFERRED",
}


def research_artifact_path_v1(*, truth_root: Path, artifact_id: str, day_utc: str, research_id: str) -> Path:
    filename = f"{artifact_id.replace('_v1', '')}.v1.json"
    return Path(truth_root).resolve() / "research_lab" / artifact_id / day_utc / _safe_id(research_id) / filename


def validate_research_lab_artifact_v1(payload: dict[str, Any]) -> None:
    schema_id = str(payload.get("schema_id") or "")
    relpath = SCHEMA_RELPATHS.get(schema_id)
    if not relpath:
        raise ValueError(f"UNSUPPORTED_AEGIS_RESEARCH_LAB_SCHEMA:{schema_id}")
    validate_against_repo_schema_v1(payload, REPO_ROOT, relpath)


def write_research_lab_artifact_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    validate_research_lab_artifact_v1(payload)
    schema_id = str(payload.get("schema_id") or "")
    identifier = str(
        payload.get("research_id")
        or payload.get("promotion_id")
        or payload.get("taxonomy_id")
        or payload.get("hypothesis_id")
        or payload.get("experiment_id")
        or payload.get("packet_id")
        or payload.get("receipt_id")
        or ("awareness" if schema_id == "research_lab_awareness_report" else "")
        or "index"
    )
    path = research_artifact_path_v1(
        truth_root=truth_root,
        artifact_id=str(payload["artifact_id"]),
        day_utc=day_utc,
        research_id=identifier,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_hypothesis_record_v1(
    *,
    hypothesis_id: str,
    title: str,
    behavioral_thesis: str,
    edge_family: str,
    market_regime: str,
    trigger_conditions: str,
    expected_outcome: str,
    failure_modes: str | list[str],
    instrument_universe: list[str] | str | None = None,
    time_horizon: str = "",
    related_hypotheses: list[str] | None = None,
    overlap_tags: list[str] | None = None,
    source_type: str = "manual",
    source_reference: str = "",
    priority: str = "normal",
    lifecycle_state: str = "proposed",
    created_at: str = "",
    updated_at: str = "",
    last_tested_at: str = "",
    test_count: int = 0,
    latest_result_status: str = "",
    promotion_status: str = "not_ready",
    notes: str = "",
) -> dict[str, Any]:
    return {
        "hypothesis_id": hypothesis_id,
        "title": title,
        "behavioral_thesis": behavioral_thesis,
        "edge_family": edge_family,
        "market_regime": market_regime,
        "trigger_conditions": trigger_conditions,
        "expected_outcome": expected_outcome,
        "failure_modes": _strings(failure_modes),
        "instrument_universe": _strings(instrument_universe),
        "time_horizon": time_horizon,
        "related_hypotheses": _strings(related_hypotheses),
        "overlap_tags": _strings(overlap_tags),
        "source_type": source_type,
        "source_reference": source_reference,
        "priority": priority,
        "lifecycle_state": _enum_lc(lifecycle_state, HYPOTHESIS_LIFECYCLE_STATES, "lifecycle_state"),
        "created_at": created_at,
        "updated_at": updated_at or created_at,
        "last_tested_at": last_tested_at,
        "test_count": int(test_count),
        "latest_result_status": latest_result_status,
        "promotion_status": _enum_lc(promotion_status, PROMOTION_STATES_LC, "promotion_status"),
        "notes": notes,
    }


def build_hypothesis_registry_v1(*, generated_at_utc: str, hypotheses: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "schema_id": "hypothesis_registry",
        "schema_version": "v1",
        "artifact_id": "hypothesis_registry_v1",
        "generated_at_utc": generated_at_utc,
        "hypotheses": hypotheses,
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "automatic_trade_generation_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_hypothesis_test_plan_v1(
    *,
    hypothesis_id: str,
    current_stage: str = "definition_check",
    completed_stages: list[str] | None = None,
    failed_stages: list[str] | None = None,
    disproof_conditions: list[str] | None = None,
    validation_conditions: list[str] | None = None,
    minimum_sample_count: int = 30,
    minimum_regime_coverage: list[str] | None = None,
    required_robustness_checks: list[str] | None = None,
    required_overlap_checks: list[str] | None = None,
    required_out_of_sample_checks: list[str] | None = None,
    next_required_task: str = "",
    terminal_state_allowed: list[str] | None = None,
) -> dict[str, Any]:
    completed = _stage_list(completed_stages)
    current = _enum_lc(current_stage, set(TEST_STAGES), "current_stage")
    next_task = next_required_task or _task_type_for_stage(current)
    payload = {
        "schema_id": "hypothesis_test_plan",
        "schema_version": "v1",
        "artifact_id": "hypothesis_test_plan_v1",
        "hypothesis_id": hypothesis_id,
        "required_test_stages": list(TEST_STAGES),
        "current_stage": current,
        "completed_stages": completed,
        "failed_stages": _stage_list(failed_stages),
        "disproof_conditions": _strings(disproof_conditions)
        or [
            "expectancy <= 0 after friction",
            "sample_count below minimum",
            "fails out-of-sample",
            "duplicate of existing sleeve with no incremental value",
        ],
        "validation_conditions": _strings(validation_conditions)
        or [
            "positive expectancy after friction",
            "minimum sample count met",
            "out-of-sample support present",
            "failure modes and edge overlap reviewed",
        ],
        "minimum_sample_count": int(minimum_sample_count),
        "minimum_regime_coverage": _strings(minimum_regime_coverage) or ["trend", "chop", "panic", "compression", "expansion", "fragility"],
        "required_robustness_checks": _strings(required_robustness_checks) or ["threshold_variation", "window_variation", "symbol_variation"],
        "required_overlap_checks": _strings(required_overlap_checks) or ["hypothesis_overlap", "sleeve_overlap", "edge_family_overlap"],
        "required_out_of_sample_checks": _strings(required_out_of_sample_checks) or ["holdout_period"],
        "next_required_task": next_task,
        "terminal_state_allowed": [_enum_lc(item, TERMINAL_TEST_STATES, "terminal_state_allowed") for item in (_strings(terminal_state_allowed) or sorted(TERMINAL_TEST_STATES))],
        "research_lab_only": True,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def register_hypothesis_v1(
    *,
    registry: dict[str, Any],
    task_queue: dict[str, Any] | None = None,
    title: str,
    edge_family: str,
    behavioral_thesis: str,
    market_regime: str,
    trigger_conditions: str,
    expected_outcome: str,
    failure_modes: str | list[str],
    created_at_utc: str,
    instrument_universe: list[str] | str | None = None,
    time_horizon: str = "",
    overlap_tags: list[str] | None = None,
    source_type: str = "manual",
    source_reference: str = "",
    priority: str = "normal",
    notes: str = "",
) -> dict[str, Any]:
    existing = _objects(registry.get("hypotheses"))
    hypothesis_id = _hypothesis_id(title=title, edge_family=edge_family, created_at_utc=created_at_utc, ordinal=len(existing) + 1)
    warnings = duplicate_warnings_for_hypothesis_v1(
        existing,
        edge_family=edge_family,
        market_regime=market_regime,
        trigger_conditions=trigger_conditions,
        behavioral_thesis=behavioral_thesis,
        expected_outcome=expected_outcome,
        overlap_tags=overlap_tags or [],
    )
    record = build_hypothesis_record_v1(
        hypothesis_id=hypothesis_id,
        title=title,
        behavioral_thesis=behavioral_thesis,
        edge_family=edge_family,
        market_regime=market_regime,
        trigger_conditions=trigger_conditions,
        expected_outcome=expected_outcome,
        failure_modes=failure_modes,
        instrument_universe=instrument_universe,
        time_horizon=time_horizon,
        related_hypotheses=warnings["related_hypothesis_ids"],
        overlap_tags=overlap_tags,
        source_type=source_type,
        source_reference=source_reference,
        priority=priority,
        lifecycle_state="proposed",
        created_at=created_at_utc,
        updated_at=created_at_utc,
        notes=notes,
    )
    task = build_research_task_v1(
        task_id=f"task:{hypothesis_id}:definition_check",
        task_type="definition_check",
        hypothesis_id=hypothesis_id,
        source_trigger="manual_hypothesis_added",
        priority=priority,
        requested_action="Run hypothesis definition_check stage.",
        created_at=created_at_utc,
        output_expected="experiment_result.v1",
    )
    plan = build_hypothesis_test_plan_v1(hypothesis_id=hypothesis_id)
    new_registry = build_hypothesis_registry_v1(
        generated_at_utc=created_at_utc,
        hypotheses=[*existing, record],
    )
    existing_tasks = _objects((task_queue or {}).get("tasks"))
    queue = build_research_task_queue_v1(generated_at_utc=created_at_utc, tasks=[*existing_tasks, task])
    return {
        "hypothesis": record,
        "registry": new_registry,
        "task_queue": queue,
        "test_plan": plan,
        "duplicate_warning": bool(warnings["related_hypothesis_ids"]),
        **warnings,
    }


def duplicate_warnings_for_hypothesis_v1(
    hypotheses: list[dict[str, Any]],
    *,
    edge_family: str,
    market_regime: str,
    trigger_conditions: str,
    behavioral_thesis: str,
    expected_outcome: str,
    overlap_tags: list[str],
) -> dict[str, Any]:
    related: list[str] = []
    reasons: list[str] = []
    new_tags = {_norm(tag) for tag in overlap_tags}
    new_words = _keyword_set(f"{trigger_conditions} {behavioral_thesis} {expected_outcome}")
    for row in hypotheses:
        row_reasons: list[str] = []
        if _norm(str(row.get("edge_family") or "")) == _norm(edge_family):
            row_reasons.append("EDGE_FAMILY_MATCH")
        if _norm(str(row.get("market_regime") or "")) == _norm(market_regime):
            row_reasons.append("REGIME_MATCH")
        if new_tags and new_tags.intersection({_norm(tag) for tag in _strings(row.get("overlap_tags"))}):
            row_reasons.append("OVERLAP_TAG_MATCH")
        existing_words = _keyword_set(f"{row.get('trigger_conditions')} {row.get('behavioral_thesis')} {row.get('expected_outcome')}")
        if _jaccard(new_words, existing_words) >= 0.35:
            row_reasons.append("THESIS_TRIGGER_OUTCOME_SIMILAR")
        if len(row_reasons) >= 2:
            related.append(str(row.get("hypothesis_id") or ""))
            reasons.extend(row_reasons)
    return {
        "related_hypothesis_ids": sorted({item for item in related if item}),
        "overlap_reason_codes": sorted(set(reasons)),
    }


def build_research_task_v1(
    *,
    task_id: str,
    task_type: str,
    hypothesis_id: str,
    source_trigger: str,
    priority: str,
    requested_action: str,
    created_at: str,
    status: str = "open",
    owner: str = "research_lab",
    started_at: str = "",
    completed_at: str = "",
    blocking_reason: str = "",
    output_expected: str = "experiment_result.v1",
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "task_type": _enum_lc(task_type, TASK_TYPES, "task_type"),
        "hypothesis_id": hypothesis_id,
        "source_trigger": source_trigger,
        "priority": priority,
        "requested_action": requested_action,
        "status": _enum_lc(status, TASK_STATUSES, "status"),
        "created_at": created_at,
        "started_at": started_at,
        "completed_at": completed_at,
        "owner": owner,
        "blocking_reason": blocking_reason,
        "output_expected": output_expected,
    }


def build_research_task_queue_v1(*, generated_at_utc: str, tasks: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "schema_id": "research_task_queue",
        "schema_version": "v1",
        "artifact_id": "research_task_queue_v1",
        "generated_at_utc": generated_at_utc,
        "tasks": tasks,
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_experiment_result_v1(
    *,
    experiment_id: str,
    hypothesis_id: str,
    task_id: str,
    test_stage: str,
    dataset_used: str,
    instrument_universe: list[str] | str | None = None,
    test_window: str,
    trigger_definition: str,
    outcome_definition: str,
    sample_count: int,
    expectancy: str,
    win_rate: str,
    avg_return: str = "",
    median_return: str = "",
    max_drawdown: str = "",
    volatility: str = "",
    friction_adjusted_result: str = "",
    regime_dependency: str,
    robustness_notes: str,
    out_of_sample_result: str = "",
    overlap_with_existing_sleeves: str,
    result_status: str,
    recommendation: str,
    next_action: str,
    created_at: str,
) -> dict[str, Any]:
    payload = {
        "schema_id": "experiment_result",
        "schema_version": "v1",
        "artifact_id": "experiment_result_v1",
        "experiment_id": experiment_id,
        "hypothesis_id": hypothesis_id,
        "task_id": task_id,
        "test_stage": _enum_lc(test_stage, set(TEST_STAGES), "test_stage"),
        "dataset_used": dataset_used,
        "instrument_universe": _strings(instrument_universe),
        "test_window": test_window,
        "trigger_definition": trigger_definition,
        "outcome_definition": outcome_definition,
        "sample_count": int(sample_count),
        "expectancy": expectancy,
        "win_rate": win_rate,
        "avg_return": avg_return,
        "median_return": median_return,
        "max_drawdown": max_drawdown,
        "volatility": volatility,
        "friction_adjusted_result": friction_adjusted_result,
        "regime_dependency": regime_dependency,
        "robustness_notes": robustness_notes,
        "out_of_sample_result": out_of_sample_result,
        "overlap_with_existing_sleeves": overlap_with_existing_sleeves,
        "result_status": _enum_lc(result_status, RESULT_STATUSES, "result_status"),
        "recommendation": recommendation,
        "next_action": next_action,
        "created_at": created_at,
        "research_lab_only": True,
        "executable_trade_created": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_research_experiment_result_v1(
    *,
    experiment_id: str,
    hypothesis_id: str,
    task_id: str,
    dataset_used: str,
    test_window: str,
    trigger_definition: str,
    outcome_definition: str,
    sample_count: int,
    expectancy: str,
    win_rate: str,
    drawdown: str = "",
    regime_dependency: str,
    robustness_notes: str,
    overlap_with_existing_sleeves: str,
    result_status: str,
    recommendation: str,
    next_action: str,
    completed_stage: str,
    completed_at_utc: str,
    instrument_universe: list[str] | str | None = None,
    avg_return: str = "",
    median_return: str = "",
    volatility: str = "",
    friction_adjusted_result: str = "",
    out_of_sample_result: str = "",
) -> dict[str, Any]:
    return build_experiment_result_v1(
        experiment_id=experiment_id,
        hypothesis_id=hypothesis_id,
        task_id=task_id,
        test_stage=completed_stage,
        dataset_used=dataset_used,
        instrument_universe=instrument_universe,
        test_window=test_window,
        trigger_definition=trigger_definition,
        outcome_definition=outcome_definition,
        sample_count=sample_count,
        expectancy=expectancy,
        win_rate=win_rate,
        avg_return=avg_return,
        median_return=median_return,
        max_drawdown=drawdown,
        volatility=volatility,
        friction_adjusted_result=friction_adjusted_result,
        regime_dependency=regime_dependency,
        robustness_notes=robustness_notes,
        out_of_sample_result=out_of_sample_result,
        overlap_with_existing_sleeves=overlap_with_existing_sleeves,
        result_status=result_status,
        recommendation=recommendation,
        next_action=next_action,
        created_at=completed_at_utc,
    )


def apply_experiment_result_v1(
    *,
    registry: dict[str, Any],
    task_queue: dict[str, Any],
    test_plan: dict[str, Any],
    result: dict[str, Any],
    updated_at_utc: str,
) -> dict[str, Any]:
    hypothesis_id = str(result["hypothesis_id"])
    result_status = str(result["result_status"])
    completed_stage = str(result.get("test_stage") or result.get("completed_stage") or "")
    current_stage = str(test_plan.get("current_stage") or "")
    if completed_stage != current_stage:
        raise ValueError(f"EXPERIMENT_RESULT_STAGE_MISMATCH:expected={current_stage}:actual={completed_stage}")
    prior_completed = _stage_list(test_plan.get("completed_stages"))
    completed_with_result = _stage_list([*prior_completed, completed_stage])
    if result_status == "validated_candidate" and (completed_stage != "promotion_review" or set(completed_with_result) != set(TEST_STAGES)):
        raise ValueError("VALIDATED_CANDIDATE_REQUIRES_COMPLETED_TEST_PLAN")
    hypotheses = []
    for row in _objects(registry.get("hypotheses")):
        if str(row.get("hypothesis_id")) != hypothesis_id:
            hypotheses.append(row)
            continue
        updated = dict(row)
        updated["updated_at"] = updated_at_utc
        updated["last_tested_at"] = updated_at_utc
        updated["test_count"] = int(updated.get("test_count") or 0) + 1
        updated["latest_result_status"] = result_status
        updated["lifecycle_state"] = _lifecycle_for_result(result_status, next_stage_hint=_next_stage(completed_with_result))
        updated["promotion_status"] = "candidate" if result_status == "validated_candidate" else str(updated.get("promotion_status") or "not_ready")
        hypotheses.append(updated)
    completed = completed_with_result
    failed = _stage_list(test_plan.get("failed_stages"))
    if result_status in {"weak", "rejected", "insufficient_data"}:
        failed = _stage_list([*failed, completed_stage])
    terminal = _terminal_for_result(result_status)
    next_stage = "" if terminal else _next_stage(completed)
    new_plan = dict(test_plan)
    new_plan["completed_stages"] = completed
    new_plan["failed_stages"] = failed
    new_plan["current_stage"] = next_stage or completed_stage
    new_plan["next_required_task"] = "" if terminal else _task_type_for_stage(next_stage)
    new_plan["canonical_json_hash"] = None
    new_plan["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(new_plan)
    tasks: list[dict[str, Any]] = []
    for task in _objects(task_queue.get("tasks")):
        updated_task = dict(task)
        if str(task.get("task_id")) == str(result.get("task_id")):
            updated_task["status"] = "completed"
            updated_task["completed_at"] = updated_at_utc
        tasks.append(updated_task)
    if next_stage:
        next_task_type = _task_type_for_stage(next_stage)
        tasks.append(
            build_research_task_v1(
                task_id=f"task:{hypothesis_id}:{next_task_type}",
                task_type=next_task_type,
                hypothesis_id=hypothesis_id,
                source_trigger="test_plan_next_stage",
                priority="normal",
                requested_action=f"Run hypothesis test stage {next_stage}.",
                created_at=updated_at_utc,
            )
        )
    new_registry = build_hypothesis_registry_v1(generated_at_utc=updated_at_utc, hypotheses=hypotheses)
    new_queue = build_research_task_queue_v1(generated_at_utc=updated_at_utc, tasks=tasks)
    return {"registry": new_registry, "task_queue": new_queue, "test_plan": new_plan, "terminal_state": terminal}


def build_research_lab_awareness_report_v1(
    *,
    generated_at_utc: str,
    registry: dict[str, Any],
    task_queue: dict[str, Any],
    experiment_results: list[dict[str, Any]],
) -> dict[str, Any]:
    hypotheses = _objects(registry.get("hypotheses"))
    tasks = _objects(task_queue.get("tasks"))
    payload = {
        "schema_id": "research_lab_awareness_report",
        "schema_version": "v1",
        "artifact_id": "research_lab_awareness_report_v1",
        "generated_at_utc": generated_at_utc,
        "new_hypotheses": [row for row in hypotheses if row.get("lifecycle_state") == "proposed"],
        "queued_tasks": [row for row in tasks if row.get("status") == "open"],
        "completed_experiments": experiment_results,
        "blocked_tasks": [row for row in tasks if row.get("status") in {"blocked", "failed"}],
        "duplicate_warnings": [
            {"hypothesis_id": row.get("hypothesis_id"), "related_hypotheses": row.get("related_hypotheses")}
            for row in hypotheses
            if row.get("related_hypotheses")
        ],
        "promotion_candidates": [row for row in hypotheses if row.get("promotion_status") == "candidate"],
        "rejected_ideas": [row for row in hypotheses if row.get("lifecycle_state") == "rejected"],
        "promising_ideas": [row for row in hypotheses if row.get("latest_result_status") == "promising"],
        "stale_ideas_needing_retest": [row for row in hypotheses if row.get("latest_result_status") == "promising" and not row.get("last_tested_at")],
        "sleeve_failure_reviews": [row for row in tasks if row.get("task_type") == "sleeve_failure_review"],
        "research_lab_only": True,
        "executable_trade_created": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_promotion_review_v1(
    *,
    promotion_id: str,
    hypothesis_id: str,
    generated_at_utc: str,
    test_plan: dict[str, Any],
    friction_adjusted_result: str,
    out_of_sample_support: str,
    regime_notes: str,
    failure_mode_notes: str,
    edge_overlap_review: str,
    approved_by_human: bool,
    approval_reason_codes: list[str] | None = None,
    rejection_reason_codes: list[str] | None = None,
) -> dict[str, Any]:
    completed = set(_strings(test_plan.get("completed_stages")))
    plan_complete = all(stage in completed for stage in TEST_STAGES)
    eligible = (
        plan_complete
        and _evidence_positive(friction_adjusted_result)
        and str(out_of_sample_support).strip().lower() not in {"", "none", "missing", "not_computed"}
        and bool(str(regime_notes).strip())
        and bool(str(failure_mode_notes).strip())
        and bool(str(edge_overlap_review).strip())
        and bool(approved_by_human)
    )
    payload = {
        "schema_id": "promotion_review",
        "schema_version": "v1",
        "artifact_id": "promotion_review_v1",
        "promotion_id": promotion_id,
        "hypothesis_id": hypothesis_id,
        "generated_at_utc": generated_at_utc,
        "completed_test_plan": plan_complete,
        "friction_adjusted_result": friction_adjusted_result,
        "out_of_sample_support": out_of_sample_support,
        "regime_notes": regime_notes,
        "failure_mode_notes": failure_mode_notes,
        "edge_overlap_review": edge_overlap_review,
        "approved_by_human": bool(approved_by_human),
        "eligible_for_promoted_sleeve_library": eligible,
        "approval_reason_codes": _strings(approval_reason_codes),
        "rejection_reason_codes": _strings(rejection_reason_codes),
        "research_lab_only": True,
        "automatic_promotion_allowed": False,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_hypothesis_progress_report_v1(
    *,
    generated_at_utc: str,
    hypothesis: dict[str, Any],
    test_plan: dict[str, Any],
) -> dict[str, Any]:
    blockers = []
    if str(hypothesis.get("lifecycle_state")) not in {"promoted", "rejected", "retired"} and test_plan.get("next_required_task"):
        blockers.append("TEST_PLAN_NOT_TERMINAL")
    payload = {
        "schema_id": "hypothesis_progress_report",
        "schema_version": "v1",
        "artifact_id": "hypothesis_progress_report_v1",
        "generated_at_utc": generated_at_utc,
        "hypothesis_id": str(hypothesis.get("hypothesis_id") or ""),
        "title": str(hypothesis.get("title") or ""),
        "lifecycle_state": str(hypothesis.get("lifecycle_state") or ""),
        "current_test_stage": str(test_plan.get("current_stage") or ""),
        "completed_stages": _strings(test_plan.get("completed_stages")),
        "next_required_task": str(test_plan.get("next_required_task") or ""),
        "blockers": blockers,
        "why_not_done_yet": "Hypothesis test plan has not reached a terminal state." if blockers else "Terminal state reached.",
        "what_would_disprove_it": _strings(test_plan.get("disproof_conditions")),
        "what_would_validate_it": _strings(test_plan.get("validation_conditions")),
        "research_lab_only": True,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_promoted_sleeve_library_v1(*, generated_at_utc: str, sleeves: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = [_promoted_sleeve(row) for row in sleeves]
    payload = {
        "schema_id": "promoted_sleeve_library",
        "schema_version": "v1",
        "artifact_id": "promoted_sleeve_library_v1",
        "generated_at_utc": generated_at_utc,
        "sleeves": normalized,
        "only_promoted_sleeves_allowed": True,
        "research_lab_artifacts_directly_executable": False,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_manual_trade_packet_v1(
    *,
    packet_id: str,
    run_id: str,
    date: str,
    generated_at_utc: str,
    regime_state: str,
    trade_candidates: list[dict[str, Any]],
    promoted_sleeve_library: dict[str, Any] | None = None,
) -> dict[str, Any]:
    promoted_sources = _promoted_sleeve_sources(promoted_sleeve_library)
    candidates = [
        _manual_trade_candidate(
            row,
            run_id=run_id,
            date=date,
            regime_state=regime_state,
            promoted_sources=promoted_sources,
        )
        for row in trade_candidates
    ]
    payload = {
        "schema_id": "manual_trade_packet",
        "schema_version": "v1",
        "artifact_id": "manual_trade_packet_v1",
        "packet_id": packet_id,
        "run_id": run_id,
        "date": date,
        "generated_at_utc": generated_at_utc,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_status": "DEFERRED",
        "trade_candidates": candidates,
        "all_candidates_traceable_to_promoted_sleeves": all(bool(row.get("sleeve_id") and row.get("source_hypothesis_id")) for row in candidates),
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_manual_execution_receipt_v1(
    *,
    receipt_id: str,
    recommended_trade_id: str,
    actual_symbol: str,
    actual_side: str,
    actual_quantity: int,
    order_type: str,
    fill_price: str,
    fill_timestamp: str,
    stop_order_entered: bool,
    stop_price: str,
    operator_notes: str,
    deviations_from_recommendation: list[str] | None = None,
    deviations_from_aegis_recommendation: list[str] | None = None,
) -> dict[str, Any]:
    deviations = _strings(deviations_from_recommendation) or _strings(deviations_from_aegis_recommendation)
    payload = {
        "schema_id": "manual_execution_receipt",
        "schema_version": "v1",
        "artifact_id": "manual_execution_receipt_v1",
        "receipt_id": receipt_id,
        "recommended_trade_id": recommended_trade_id,
        "actual_symbol": actual_symbol.upper(),
        "actual_side": actual_side.upper(),
        "actual_quantity": int(actual_quantity),
        "order_type": order_type.upper(),
        "fill_price": fill_price,
        "fill_timestamp": fill_timestamp,
        "stop_order_entered": bool(stop_order_entered),
        "stop_price": stop_price,
        "operator_notes": operator_notes,
        "deviations_from_recommendation": deviations,
        "deviations_from_aegis_recommendation": deviations,
        "manual_observation_only": True,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_outcome_ledger_v1(*, generated_at_utc: str, outcome_rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "schema_id": "outcome_ledger",
        "schema_version": "v1",
        "artifact_id": "outcome_ledger_v1",
        "generated_at_utc": generated_at_utc,
        "outcomes": [_outcome_row(row) for row in outcome_rows],
        "feeds_research_lab": True,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_learning_tasks_from_outcomes_v1(
    *,
    generated_at_utc: str,
    outcome_ledger: dict[str, Any],
    existing_tasks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    tasks = list(existing_tasks or [])
    for row in _objects(outcome_ledger.get("outcomes")):
        trade_id = str(row.get("trade_id") or "")
        hypothesis_id = str(row.get("hypothesis_id") or "")
        if not trade_id or not hypothesis_id:
            continue
        status = str(row.get("outcome_status") or "").lower()
        failure = str(row.get("failure_reason") or "").lower()
        deviation = str(row.get("operator_deviation") or "").lower()
        if status in {"loss", "stopped_out", "underperformed"}:
            tasks.append(
                build_research_task_v1(
                    task_id=f"task:{hypothesis_id}:sleeve_failure_review:{_safe_id(trade_id)}",
                    task_type="sleeve_failure_review",
                    hypothesis_id=hypothesis_id,
                    source_trigger="outcome_ledger_sleeve_underperformance",
                    priority="high",
                    requested_action=f"Review failed or underperforming manual trade outcome {trade_id}.",
                    created_at=generated_at_utc,
                    output_expected="experiment_result.v1",
                )
            )
        if "delay" in deviation or "delay" in failure:
            tasks.append(
                build_research_task_v1(
                    task_id=f"task:{hypothesis_id}:friction_test:{_safe_id(trade_id)}",
                    task_type="friction_test",
                    hypothesis_id=hypothesis_id,
                    source_trigger="outcome_ledger_manual_delay",
                    priority="normal",
                    requested_action=f"Test manual execution friction impact for trade {trade_id}.",
                    created_at=generated_at_utc,
                    output_expected="experiment_result.v1",
                )
            )
        if "overlap" in failure or "correlation" in failure:
            tasks.append(
                build_research_task_v1(
                    task_id=f"task:{hypothesis_id}:edge_overlap_review:{_safe_id(trade_id)}",
                    task_type="edge_overlap_review",
                    hypothesis_id=hypothesis_id,
                    source_trigger="outcome_ledger_correlated_loss",
                    priority="high",
                    requested_action=f"Review edge overlap and correlated loss behavior for trade {trade_id}.",
                    created_at=generated_at_utc,
                    output_expected="experiment_result.v1",
                )
            )
        if "regime" in failure:
            tasks.append(
                build_research_task_v1(
                    task_id=f"task:{hypothesis_id}:regime_test:{_safe_id(trade_id)}",
                    task_type="regime_test",
                    hypothesis_id=hypothesis_id,
                    source_trigger="outcome_ledger_regime_dependency",
                    priority="normal",
                    requested_action=f"Retest regime dependency for trade {trade_id}.",
                    created_at=generated_at_utc,
                    output_expected="experiment_result.v1",
                )
            )
        if "missed" in failure or status == "missed_opportunity":
            tasks.append(
                build_research_task_v1(
                    task_id=f"task:{hypothesis_id}:anomaly_review:{_safe_id(trade_id)}",
                    task_type="anomaly_review",
                    hypothesis_id=hypothesis_id,
                    source_trigger="outcome_ledger_missed_opportunity",
                    priority="normal",
                    requested_action=f"Investigate missed opportunity around trade {trade_id}.",
                    created_at=generated_at_utc,
                    output_expected="experiment_result.v1",
                )
            )
    return build_research_task_queue_v1(generated_at_utc=generated_at_utc, tasks=tasks)


def build_research_evidence_packet_v1(
    *,
    research_id: str,
    hypothesis_id: str,
    title: str,
    hypothesis_description: str,
    research_type: str,
    created_at_utc: str,
    source_data_summary: str,
    replay_window: str,
    instruments_tested: list[str],
    regimes_tested: list[str],
    edge_family: str,
    expected_holding_period: str,
    methodology_summary: str,
    metrics_summary: dict[str, Any],
    expectancy_summary: str,
    drawdown_summary: str,
    MAE_MFE_summary: str,
    failure_modes: list[str],
    known_limitations: list[str],
    reproducibility_notes: str,
    artifact_lineage: list[dict[str, Any]],
    research_status: str,
) -> dict[str, Any]:
    payload = {
        "schema_id": "research_evidence_packet",
        "schema_version": "v1",
        "artifact_id": "research_evidence_packet_v1",
        "research_id": research_id,
        "hypothesis_id": hypothesis_id,
        "title": title,
        "hypothesis_description": hypothesis_description,
        "research_type": _enum(research_type, RESEARCH_TYPES, "research_type"),
        "created_at_utc": created_at_utc,
        "source_data_summary": source_data_summary,
        "replay_window": replay_window,
        "instruments_tested": _strings(instruments_tested),
        "regimes_tested": _strings(regimes_tested),
        "edge_family": edge_family,
        "expected_holding_period": expected_holding_period,
        "methodology_summary": methodology_summary,
        "metrics_summary": metrics_summary,
        "expectancy_summary": expectancy_summary,
        "drawdown_summary": drawdown_summary,
        "MAE_MFE_summary": MAE_MFE_summary,
        "failure_modes": _strings(failure_modes),
        "known_limitations": _strings(known_limitations),
        "reproducibility_notes": reproducibility_notes,
        "artifact_lineage": artifact_lineage,
        "research_status": _enum(research_status, RESEARCH_STATUSES, "research_status"),
        "research_lab_only": True,
        "execution_authority_granted": False,
        "runtime_authorized": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "automatic_lite_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_research_to_lite_promotion_v1(
    *,
    promotion_id: str,
    research_id: str,
    hypothesis_id: str,
    proposed_lite_component_type: str,
    promotion_status: str,
    evidence_packet_refs: list[dict[str, Any]],
    validation_summary: str,
    regime_evidence: str,
    expectancy_evidence: str,
    failure_mode_review: str,
    governance_compatibility_review: str,
    risk_contract_review: str,
    stop_logic_review: str,
    manual_execution_compatibility: str,
    operator_clarity_review: str,
    implementation_notes: str,
    required_tests: list[str],
    approval_reason_codes: list[str],
    rejection_reason_codes: list[str],
    approved_by_human: bool,
    created_at_utc: str,
    reviewed_at_utc: str = "",
    source_research_status: str = "VALIDATED_RESEARCH",
) -> dict[str, Any]:
    status = _enum(promotion_status, PROMOTION_STATUSES, "promotion_status")
    source_status = _enum(source_research_status, RESEARCH_STATUSES, "source_research_status")
    if not evidence_packet_refs:
        raise ValueError("PROMOTION_REQUIRES_EVIDENCE_PACKET_REFS")
    if status in {"APPROVED_FOR_LITE_REVIEW", "APPROVED_FOR_LITE_IMPLEMENTATION"} and source_status != "VALIDATED_RESEARCH":
        raise ValueError(f"PROMOTION_REQUIRES_VALIDATED_RESEARCH:source_status={source_status}")
    if status == "APPROVED_FOR_LITE_IMPLEMENTATION" and not approved_by_human:
        raise ValueError("APPROVED_FOR_LITE_IMPLEMENTATION_REQUIRES_APPROVED_BY_HUMAN")
    payload = {
        "schema_id": "research_to_lite_promotion",
        "schema_version": "v1",
        "artifact_id": "research_to_lite_promotion_v1",
        "promotion_id": promotion_id,
        "research_id": research_id,
        "hypothesis_id": hypothesis_id,
        "proposed_lite_component_type": _enum(
            proposed_lite_component_type,
            PROMOTION_COMPONENT_TYPES,
            "proposed_lite_component_type",
        ),
        "promotion_status": status,
        "evidence_packet_refs": evidence_packet_refs,
        "validation_summary": validation_summary,
        "regime_evidence": regime_evidence,
        "expectancy_evidence": expectancy_evidence,
        "failure_mode_review": failure_mode_review,
        "governance_compatibility_review": governance_compatibility_review,
        "risk_contract_review": risk_contract_review,
        "stop_logic_review": stop_logic_review,
        "manual_execution_compatibility": manual_execution_compatibility,
        "operator_clarity_review": operator_clarity_review,
        "implementation_notes": implementation_notes,
        "required_tests": _strings(required_tests),
        "approval_reason_codes": _strings(approval_reason_codes),
        "rejection_reason_codes": _strings(rejection_reason_codes),
        "approved_by_human": bool(approved_by_human),
        "created_at_utc": created_at_utc,
        "reviewed_at_utc": reviewed_at_utc,
        "source_research_status": source_status,
        "eligible_for_lite_implementation": status == "APPROVED_FOR_LITE_IMPLEMENTATION" and bool(approved_by_human),
        "advisory_governance_evidence_only": True,
        "runtime_mutation_allowed": False,
        "automatic_lite_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_research_lab_index_v1(
    *,
    generated_at_utc: str,
    research_items: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "schema_id": "research_lab_index",
        "schema_version": "v1",
        "artifact_id": "research_lab_index_v1",
        "generated_at_utc": generated_at_utc,
        "research_items": [_research_index_item(row) for row in research_items],
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_edge_taxonomy_v1(
    *,
    taxonomy_id: str,
    generated_at_utc: str,
    market_theses: list[str],
    edge_families: list[str],
    edge_clusters: list[str],
    trade_expressions: list[str],
    deprecated_terms: list[str],
    naming_rules: list[str],
) -> dict[str, Any]:
    duplicate_warnings = _duplicate_term_warnings(
        market_theses=market_theses,
        edge_families=edge_families,
        edge_clusters=edge_clusters,
        trade_expressions=trade_expressions,
        deprecated_terms=deprecated_terms,
    )
    payload = {
        "schema_id": "edge_taxonomy",
        "schema_version": "v1",
        "artifact_id": "edge_taxonomy_v1",
        "taxonomy_id": taxonomy_id,
        "generated_at_utc": generated_at_utc,
        "market_theses": _strings(market_theses),
        "edge_families": _strings(edge_families),
        "edge_clusters": _strings(edge_clusters),
        "trade_expressions": _strings(trade_expressions),
        "deprecated_terms": _strings(deprecated_terms),
        "naming_rules": _strings(naming_rules),
        "duplicate_term_warnings": duplicate_warnings,
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def _research_index_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "research_id": str(row.get("research_id") or ""),
        "title": str(row.get("title") or ""),
        "research_type": _enum(str(row.get("research_type") or "EDGE"), RESEARCH_TYPES, "research_type"),
        "research_status": _enum(str(row.get("research_status") or "DRAFT"), RESEARCH_STATUSES, "research_status"),
        "latest_evidence_packet": str(row.get("latest_evidence_packet") or ""),
        "latest_promotion_status": _enum(
            str(row.get("latest_promotion_status") or "NOT_READY"),
            PROMOTION_STATUSES,
            "latest_promotion_status",
        ),
        "related_edge_family": str(row.get("related_edge_family") or ""),
        "related_sleeves": _strings(row.get("related_sleeves")),
        "archived": bool(row.get("archived", False)),
        "operator_notes": str(row.get("operator_notes") or ""),
    }


def _duplicate_term_warnings(
    *,
    market_theses: list[str],
    edge_families: list[str],
    edge_clusters: list[str],
    trade_expressions: list[str],
    deprecated_terms: list[str],
) -> list[str]:
    sections = {
        "market_theses": _strings(market_theses),
        "edge_families": _strings(edge_families),
        "edge_clusters": _strings(edge_clusters),
        "trade_expressions": _strings(trade_expressions),
    }
    deprecated = {_norm(term) for term in deprecated_terms}
    seen: dict[str, list[str]] = {}
    for section, values in sections.items():
        for value in values:
            seen.setdefault(_norm(value), []).append(section)
    warnings: list[str] = []
    for term, locations in sorted(seen.items()):
        if len(locations) > 1:
            warnings.append(f"DUPLICATE_TERM:{term}:{','.join(sorted(locations))}")
        if term in deprecated:
            warnings.append(f"DEPRECATED_TERM_USED:{term}")
    return warnings


def _enum(value: str, allowed: set[str], field_name: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in allowed:
        raise ValueError(f"INVALID_{field_name.upper()}:{normalized}")
    return normalized


def _enum_lc(value: str, allowed: set[str], field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in allowed:
        raise ValueError(f"INVALID_{field_name.upper()}:{normalized}")
    return normalized


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    item = str(value).strip()
    return [item] if item else []


def _objects(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _norm(value: str) -> str:
    return "_".join(str(value).strip().upper().replace("-", "_").split())


def _safe_id(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip())
    return cleaned or "research_lab"


def _hypothesis_id(*, title: str, edge_family: str, created_at_utc: str, ordinal: int) -> str:
    slug = _safe_id(f"{edge_family}-{title}")[:48].strip("_-.").lower()
    day = "".join(ch for ch in str(created_at_utc)[:10] if ch.isdigit()) or "00000000"
    return f"hyp-{day}-{ordinal:04d}-{slug or 'hypothesis'}"


def _keyword_set(value: str) -> set[str]:
    stop = {"the", "a", "an", "and", "or", "when", "with", "over", "next", "into", "from", "for"}
    words = {
        "".join(ch for ch in token.lower() if ch.isalnum())
        for token in str(value or "").replace("+", " ").replace("/", " ").split()
    }
    return {word for word in words if len(word) >= 4 and word not in stop}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left.intersection(right)) / len(left.union(right))


def _stage_list(value: Any) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in _strings(value):
        stage = _enum_lc(item, set(TEST_STAGES), "test_stage")
        if stage in seen:
            continue
        seen.add(stage)
        out.append(stage)
    return out


def _task_type_for_stage(stage: str) -> str:
    stage = str(stage or "")
    return STAGE_TASK_TYPES.get(stage, "retest")


def _next_stage(completed: list[str]) -> str:
    for stage in TEST_STAGES:
        if stage not in completed:
            return stage
    return ""


def _lifecycle_for_result(result_status: str, *, next_stage_hint: str = "") -> str:
    if result_status == "validated_candidate":
        return "validated_candidate"
    if result_status in {"weak", "rejected"}:
        return "rejected"
    if result_status == "insufficient_data":
        return "insufficient_data"
    if result_status == "regime_dependent":
        return "regime_dependent"
    if result_status in {"overlapping", "duplicate"}:
        return "duplicate_of_existing_edge"
    return STAGE_LIFECYCLE_STATES.get(next_stage_hint, "exploratory_testing")


def _terminal_for_result(result_status: str) -> str:
    mapping = {
        "weak": "rejected",
        "rejected": "rejected",
        "insufficient_data": "insufficient_data",
        "regime_dependent": "regime_dependent",
        "overlapping": "duplicate_of_existing_edge",
        "duplicate": "duplicate_of_existing_edge",
    }
    return mapping.get(result_status, "")


def _promoted_sleeve(row: dict[str, Any]) -> dict[str, Any]:
    required = [
        "sleeve_id",
        "source_hypothesis_id",
        "edge_family",
        "behavioral_thesis",
        "regime_fit",
        "instrument_universe",
        "entry_logic",
        "exit_logic",
        "stop_logic",
        "sizing_logic",
        "invalidation_logic",
        "known_failure_modes",
        "overlap_tags",
        "promotion_evidence_path",
        "production_status",
        "created_at",
        "updated_at",
    ]
    missing = [field for field in required if not row.get(field)]
    if missing:
        raise ValueError(f"PROMOTED_SLEEVE_MISSING_REQUIRED_FIELDS:{','.join(missing)}")
    if str(row.get("promotion_status") or "promoted").lower() != "promoted":
        raise ValueError("SLEEVE_LIBRARY_REQUIRES_PROMOTED_SLEEVES")
    return {
        "sleeve_id": str(row["sleeve_id"]),
        "source_hypothesis_id": str(row["source_hypothesis_id"]),
        "edge_family": str(row["edge_family"]),
        "behavioral_thesis": str(row["behavioral_thesis"]),
        "regime_fit": _strings(row["regime_fit"]),
        "instrument_universe": _strings(row["instrument_universe"]),
        "entry_logic": str(row["entry_logic"]),
        "exit_logic": str(row["exit_logic"]),
        "stop_logic": str(row["stop_logic"]),
        "sizing_logic": str(row["sizing_logic"]),
        "invalidation_logic": str(row["invalidation_logic"]),
        "known_failure_modes": _strings(row["known_failure_modes"]),
        "overlap_tags": _strings(row["overlap_tags"]),
        "promotion_evidence_path": str(row["promotion_evidence_path"]),
        "research_hypothesis_id": str(row.get("research_hypothesis_id") or row["source_hypothesis_id"]),
        "promotion_status": "promoted",
        "production_status": str(row["production_status"]),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _manual_trade_candidate(
    row: dict[str, Any],
    *,
    run_id: str,
    date: str,
    regime_state: str,
    promoted_sources: dict[str, str],
) -> dict[str, Any]:
    required = [
        "sleeve_id",
        "source_hypothesis_id",
        "symbol",
        "side",
        "instrument_type",
        "entry_reference_price",
        "quantity_or_sizing_guidance",
        "stop_price",
        "stop_logic",
        "risk_per_trade",
    ]
    missing = [field for field in required if row.get(field) is None or row.get(field) == "" or row.get(field) == []]
    sleeve_id = str(row.get("sleeve_id") or "")
    source_hypothesis_id = str(row.get("source_hypothesis_id") or row.get("hypothesis_id") or "")
    source_blockers: list[str] = []
    if not promoted_sources:
        source_blockers.append("PROMOTED_SLEEVE_LIBRARY_MISSING")
    elif promoted_sources.get(sleeve_id) != source_hypothesis_id:
        source_blockers.append("UNPROMOTED_SLEEVE_SOURCE")
    actionable = not missing and not source_blockers
    checklist = _strings(row.get("manual_execution_checklist")) or [
        "Review status and blockers before acting.",
        "Enter the position manually in IB paper.",
        "Immediately enter the protective stop.",
        "Confirm the stop is accepted.",
        "Record the manual execution receipt.",
    ]
    return {
        "run_id": run_id,
        "date": date,
        "recommended_trade_id": str(row.get("recommended_trade_id") or row.get("candidate_id") or _safe_id(f"{run_id}-{row.get('sleeve_id')}-{row.get('symbol')}")),
        "sleeve_id": sleeve_id,
        "source_hypothesis_id": source_hypothesis_id,
        "symbol": str(row.get("symbol") or "").upper(),
        "side": str(row.get("side") or row.get("direction") or "").upper(),
        "instrument_type": str(row.get("instrument_type") or ""),
        "entry_reference_price": str(row.get("entry_reference_price") or ""),
        "order_type_suggestion": str(row.get("order_type_suggestion") or "MANUAL_LIMIT_OR_MARKET_BY_OPERATOR"),
        "quantity_or_sizing_guidance": str(row.get("quantity_or_sizing_guidance") or ""),
        "stop_price": str(row.get("stop_price") or ""),
        "stop_logic": str(row.get("stop_logic") or ""),
        "risk_per_trade": str(row.get("risk_per_trade") or ""),
        "edge_family": str(row.get("edge_family") or ""),
        "regime_state": str(row.get("regime_state") or regime_state),
        "confidence": str(row.get("confidence") or ""),
        "inclusion_reason": str(row.get("inclusion_reason") or ""),
        "exclusion_reason": str(row.get("exclusion_reason") or ("MISSING_REQUIRED_MANUAL_TRADE_FIELDS:" + ",".join(missing) if missing else "")),
        "governance_notes": str(row.get("governance_notes") or ""),
        "edge_overlap_result": str(row.get("edge_overlap_result") or ""),
        "manual_execution_checklist": checklist,
        "actionable": actionable,
        "do_not_trade_blockers": [f"MISSING_{field.upper()}" for field in missing] + source_blockers,
    }


def _outcome_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "trade_id": str(row.get("trade_id") or row.get("recommended_trade_id") or ""),
        "sleeve_id": str(row.get("sleeve_id") or ""),
        "hypothesis_id": str(row.get("hypothesis_id") or row.get("source_hypothesis_id") or ""),
        "recommended_entry": str(row.get("recommended_entry") or ""),
        "actual_entry": str(row.get("actual_entry") or ""),
        "recommended_stop": str(row.get("recommended_stop") or ""),
        "actual_stop": str(row.get("actual_stop") or ""),
        "exit_price": str(row.get("exit_price") or ""),
        "return_pct": str(row.get("return_pct") or ""),
        "risk_adjusted_return": str(row.get("risk_adjusted_return") or ""),
        "max_adverse_excursion": str(row.get("max_adverse_excursion") or ""),
        "max_favorable_excursion": str(row.get("max_favorable_excursion") or ""),
        "outcome_status": str(row.get("outcome_status") or row.get("actual_trade_outcome") or ""),
        "failure_reason": str(row.get("failure_reason") or ""),
        "operator_deviation": str(row.get("operator_deviation") or ""),
        "notes": str(row.get("notes") or ""),
        "sleeve_attribution": str(row.get("sleeve_attribution") or ""),
        "edge_overlap_attribution": str(row.get("edge_overlap_attribution") or ""),
    }


def _promoted_sleeve_sources(promoted_sleeve_library: dict[str, Any] | None) -> dict[str, str]:
    if not promoted_sleeve_library:
        return {}
    sources: dict[str, str] = {}
    for row in _objects(promoted_sleeve_library.get("sleeves")):
        if str(row.get("promotion_status") or "").lower() != "promoted":
            continue
        sleeve_id = str(row.get("sleeve_id") or "")
        source_hypothesis_id = str(row.get("source_hypothesis_id") or row.get("research_hypothesis_id") or "")
        if sleeve_id and source_hypothesis_id:
            sources[sleeve_id] = source_hypothesis_id
    return sources


def _evidence_positive(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    negative_terms = {"negative", "not_computed", "missing", "none", "failed", "fail", "weak", "insufficient"}
    if any(term in normalized for term in negative_terms):
        return False
    positive_terms = {"positive", "pass", "passed", "supported", "profitable", "favorable"}
    return any(term in normalized for term in positive_terms)
