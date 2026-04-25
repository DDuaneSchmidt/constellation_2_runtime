from __future__ import annotations

from pathlib import Path
from typing import Any

from .broker_adapters import NULL_LIVE_ADAPTER, PAPER_ADAPTER
from .execution_artifacts import write_bundle, write_records
from .execution_audit import emit_execution_event
from .execution_boundary import submit_validated_intent
from .execution_intents import build_execution_intents
from .execution_receipts import write_receipt
from .execution_recovery import recover_execution_state as _recover_execution_state
from .operator_actions import build_execution_operator_view as _build_execution_operator_view
from .pre_execution_gate import evaluate_pre_execution_gate
from .runtime_supervision import supervise_runtime
from .schemas import content_hash, utc_now
from ..meta_governance.api import _store


EXECUTION_SOURCE_KINDS = (
    "candidate_actions",
    "execution_eligibilities",
    "autonomous_action_plans",
    "execution_intents",
    "pre_execution_gate_results",
    "execution_submissions",
    "execution_receipts",
    "execution_state_transitions",
    "execution_recovery_records",
    "deployment_states",
    "health_check_results",
    "execution_operator_views",
)


def _created_at_for_refs(store, refs: tuple[str, ...], fallback: str | None = None) -> str:
    timestamps: list[str] = []
    for ref in refs:
        for kind in EXECUTION_SOURCE_KINDS:
            if store.exists(kind, ref):
                timestamps.append(store.read(kind, ref)["created_at"])
                break
    if timestamps:
        return max(timestamps)
    return fallback or utc_now()


def create_execution_intents(
    *,
    candidate_action_refs: tuple[str, ...],
    action_plan_ref: str,
    execution_eligibility_refs: tuple[str, ...],
    intent_inputs_by_action: dict[str, dict[str, Any]] | None = None,
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    records = build_execution_intents(
        store,
        candidate_action_refs=candidate_action_refs,
        action_plan_ref=action_plan_ref,
        execution_eligibility_refs=execution_eligibility_refs,
        intent_inputs_by_action=intent_inputs_by_action,
    )
    created_at = _created_at_for_refs(store, tuple(sorted(candidate_action_refs + execution_eligibility_refs + (action_plan_ref,))))
    refs = write_records(store, kind="execution_intents", records=records, artifact_type="ExecutionIntent", id_field="execution_intent_id", created_at=created_at)
    for ref in refs:
        emit_execution_event(store, "execution_intent_created", "system", artifact_refs=(ref,))
    return refs


def evaluate_pre_execution_gates(
    *,
    execution_intent_refs: tuple[str, ...],
    runtime_mode: str,
    adapter_name: str,
    deployment_state_ref: str | None = None,
    strict_risk_budget: bool = False,
    risk_budget_remaining: float | None = None,
    strict_tax_check: bool = False,
    tax_uncertainty_by_intent: dict[str, float] | None = None,
    tax_uncertainty_limit: float | None = None,
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    created_at = _created_at_for_refs(store, tuple(sorted(execution_intent_refs + ((deployment_state_ref,) if deployment_state_ref else ()))))
    refs: list[str] = []
    for intent_ref in execution_intent_refs:
        results = evaluate_pre_execution_gate(
            intent_ref,
            runtime_mode=runtime_mode,
            adapter_name=adapter_name,
            deployment_state_ref=deployment_state_ref,
            strict_risk_budget=strict_risk_budget,
            risk_budget_remaining=risk_budget_remaining,
            strict_tax_check=strict_tax_check,
            tax_uncertainty=(tax_uncertainty_by_intent or {}).get(intent_ref),
            tax_uncertainty_limit=tax_uncertainty_limit,
            store_root=store.root,
        )
        for result in results:
            artifact_id = f"{result.execution_intent_id}__{result.gate_id}__{content_hash({'measured_value': result.measured_value, 'required_condition': result.required_condition, 'failure_reason': result.failure_reason, 'result': result.result})[:10]}"
            store.write_immutable("pre_execution_gate_results", artifact_id, result, artifact_type="PreExecutionGateResult", created_at=created_at)
            refs.append(artifact_id)
            emit_execution_event(store, "pre_execution_gate_evaluated", "system", artifact_refs=(artifact_id,))
    return tuple(refs)


def submit_execution_intent(
    *,
    execution_intent_ref: str,
    runtime_mode: str,
    adapter_name: str | None = None,
    deployment_state_ref: str | None = None,
    strict_risk_budget: bool = False,
    risk_budget_remaining: float | None = None,
    strict_tax_check: bool = False,
    tax_uncertainty: float | None = None,
    tax_uncertainty_limit: float | None = None,
    store_root: str | Path | None = None,
) -> dict[str, Any]:
    store = _store(store_root)
    selected_adapter = adapter_name or (PAPER_ADAPTER if runtime_mode in {"development", "paper"} else NULL_LIVE_ADAPTER)
    gate_refs = evaluate_pre_execution_gates(
        execution_intent_refs=(execution_intent_ref,),
        runtime_mode=runtime_mode,
        adapter_name=selected_adapter,
        deployment_state_ref=deployment_state_ref,
        strict_risk_budget=strict_risk_budget,
        risk_budget_remaining=risk_budget_remaining,
        strict_tax_check=strict_tax_check,
        tax_uncertainty_by_intent={execution_intent_ref: tax_uncertainty} if tax_uncertainty is not None else None,
        tax_uncertainty_limit=tax_uncertainty_limit,
        store_root=store.root,
    )
    submitted_at = _created_at_for_refs(store, tuple(sorted((execution_intent_ref,) + gate_refs)))
    result = submit_validated_intent(
        store,
        execution_intent_ref=execution_intent_ref,
        runtime_mode=runtime_mode,
        adapter_name=selected_adapter,
        gate_result_refs=gate_refs,
        submitted_at=submitted_at,
    )
    emit_execution_event(
        store,
        "execution_submission_recorded",
        "system",
        artifact_refs=(result["submission_ref"],) + tuple(result["receipt_refs"]) + tuple(result["transition_refs"]),
        details={"runtime_mode": runtime_mode, "adapter_name": selected_adapter, "status": result["status"]},
    )
    return {"gate_result_refs": gate_refs, **result}


def ingest_execution_receipt(
    *,
    submission_ref: str,
    receipt_type: str,
    received_at: str,
    external_execution_ref: str | None,
    order_status: str,
    filled_quantity: float,
    remaining_quantity: float,
    average_fill_price: float | None,
    fee_amount: float | None,
    message: str,
    store_root: str | Path | None = None,
) -> tuple[str, str]:
    store = _store(store_root)
    receipt_ref, transition_ref = write_receipt(
        store,
        submission_ref=submission_ref,
        receipt_type=receipt_type,
        received_at=received_at,
        external_execution_ref=external_execution_ref,
        order_status=order_status,
        filled_quantity=filled_quantity,
        remaining_quantity=remaining_quantity,
        average_fill_price=average_fill_price,
        fee_amount=fee_amount,
        message=message,
    )
    emit_execution_event(store, "execution_receipt_recorded", "system", artifact_refs=(receipt_ref, transition_ref, submission_ref))
    return receipt_ref, transition_ref


def recover_execution_state(
    *,
    as_of: str | None = None,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    recovery_ref = _recover_execution_state(store, as_of=as_of or utc_now())
    emit_execution_event(store, "execution_recovery_recorded", "system", artifact_refs=(recovery_ref,))
    return recovery_ref


def build_deployment_runtime(
    *,
    runtime_mode: str,
    adapter_name: str | None = None,
    recovery_ref: str | None = None,
    as_of: str | None = None,
    store_root: str | Path | None = None,
) -> dict[str, Any]:
    store = _store(store_root)
    selected_adapter = adapter_name or (PAPER_ADAPTER if runtime_mode in {"development", "paper"} else NULL_LIVE_ADAPTER)
    result = supervise_runtime(runtime_mode=runtime_mode, adapter_name=selected_adapter, recovery_ref=recovery_ref, as_of=as_of, store_root=store.root)
    emit_execution_event(
        store,
        "deployment_state_recorded",
        "system",
        artifact_refs=(result["deployment_state_ref"],) + tuple(result["health_check_refs"]) + (result["operator_view_ref"],),
        details={"runtime_mode": runtime_mode, "adapter_name": selected_adapter},
    )
    return result


def build_execution_operator_view(
    *,
    deployment_state_ref: str,
    recovery_refs: tuple[str, ...] = (),
    as_of: str | None = None,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    record = _build_execution_operator_view(store, deployment_state_ref=deployment_state_ref, recovery_refs=recovery_refs, as_of=as_of or utc_now())
    store.write_immutable("execution_operator_views", record.operator_view_id, record, artifact_type="ExecutionOperatorView", created_at=record.as_of)
    emit_execution_event(store, "execution_operator_view_created", "system", artifact_refs=(record.operator_view_id,))
    return record.operator_view_id


def create_execution_bundle(
    *,
    intent_refs: tuple[str, ...],
    gate_result_refs: tuple[str, ...],
    submission_refs: tuple[str, ...],
    receipt_refs: tuple[str, ...],
    state_transition_refs: tuple[str, ...],
    recovery_refs: tuple[str, ...],
    deployment_state_ref: str,
    health_check_refs: tuple[str, ...],
    operator_view_ref: str,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    created_at = _created_at_for_refs(
        store,
        tuple(sorted(intent_refs + gate_result_refs + submission_refs + receipt_refs + state_transition_refs + recovery_refs + health_check_refs + (deployment_state_ref, operator_view_ref))),
    )
    bundle_ref = write_bundle(
        store,
        intent_refs=intent_refs,
        gate_result_refs=gate_result_refs,
        submission_refs=submission_refs,
        receipt_refs=receipt_refs,
        state_transition_refs=state_transition_refs,
        recovery_refs=recovery_refs,
        deployment_state_ref=deployment_state_ref,
        health_check_refs=health_check_refs,
        operator_view_ref=operator_view_ref,
        created_at=created_at,
    )
    emit_execution_event(store, "execution_deployment_bundle_created", "system", artifact_refs=(bundle_ref,))
    return bundle_ref


def run_execution_cycle(
    *,
    candidate_action_refs: tuple[str, ...],
    action_plan_ref: str,
    execution_eligibility_refs: tuple[str, ...],
    runtime_mode: str,
    intent_inputs_by_action: dict[str, dict[str, Any]] | None = None,
    submit: bool = False,
    store_root: str | Path | None = None,
) -> dict[str, Any]:
    intent_refs = create_execution_intents(
        candidate_action_refs=candidate_action_refs,
        action_plan_ref=action_plan_ref,
        execution_eligibility_refs=execution_eligibility_refs,
        intent_inputs_by_action=intent_inputs_by_action,
        store_root=store_root,
    )
    recovery_ref = recover_execution_state(store_root=store_root)
    deployment = build_deployment_runtime(runtime_mode=runtime_mode, recovery_ref=recovery_ref, store_root=store_root)
    gate_refs = evaluate_pre_execution_gates(
        execution_intent_refs=intent_refs,
        runtime_mode=runtime_mode,
        adapter_name=PAPER_ADAPTER if runtime_mode in {"development", "paper"} else NULL_LIVE_ADAPTER,
        deployment_state_ref=deployment["deployment_state_ref"],
        store_root=store_root,
    )
    submission_refs: tuple[str, ...] = ()
    receipt_refs: tuple[str, ...] = ()
    transition_refs: tuple[str, ...] = ()
    if submit:
        submission_results = [
            submit_execution_intent(
                execution_intent_ref=intent_ref,
                runtime_mode=runtime_mode,
                deployment_state_ref=deployment["deployment_state_ref"],
                store_root=store_root,
            )
            for intent_ref in intent_refs
        ]
        submission_refs = tuple(result["submission_ref"] for result in submission_results)
        receipt_refs = tuple(ref for result in submission_results for ref in result["receipt_refs"])
        transition_refs = tuple(ref for result in submission_results for ref in result["transition_refs"])
    bundle_ref = create_execution_bundle(
        intent_refs=intent_refs,
        gate_result_refs=gate_refs,
        submission_refs=submission_refs,
        receipt_refs=receipt_refs,
        state_transition_refs=transition_refs,
        recovery_refs=(recovery_ref,),
        deployment_state_ref=deployment["deployment_state_ref"],
        health_check_refs=deployment["health_check_refs"],
        operator_view_ref=deployment["operator_view_ref"],
        store_root=store_root,
    )
    return {
        "intent_refs": intent_refs,
        "gate_result_refs": gate_refs,
        "submission_refs": submission_refs,
        "receipt_refs": receipt_refs,
        "state_transition_refs": transition_refs,
        "recovery_ref": recovery_ref,
        "deployment_state_ref": deployment["deployment_state_ref"],
        "health_check_refs": deployment["health_check_refs"],
        "operator_view_ref": deployment["operator_view_ref"],
        "bundle_ref": bundle_ref,
    }


def find_execution_bundles(
    *,
    snapshot_id: str | None = None,
    store_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    store = _store(store_root)
    if snapshot_id is None:
        return [store.read("execution_deployment_bundles", ref) for ref in store.list_ids("execution_deployment_bundles")]
    bundles: list[dict[str, Any]] = []
    for ref in store.list_ids("execution_deployment_bundles"):
        bundle = store.read("execution_deployment_bundles", ref)
        for intent_ref in bundle["record"]["intent_refs"]:
            intent = store.read("execution_intents", intent_ref)["record"]
            if intent["active_snapshot_id"] == snapshot_id:
                bundles.append(bundle)
                break
    return bundles
