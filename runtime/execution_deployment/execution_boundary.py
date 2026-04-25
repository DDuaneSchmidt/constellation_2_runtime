from __future__ import annotations

from typing import Any

from .broker_adapters import submit_via_adapter
from .execution_receipts import write_receipt
from .execution_state_machine import transition_submission
from .schemas import content_hash
from .types import ExecutionSubmission
from ..meta_governance.store import ArtifactStore


def submit_validated_intent(
    store: ArtifactStore,
    *,
    execution_intent_ref: str,
    runtime_mode: str,
    adapter_name: str,
    gate_result_refs: tuple[str, ...],
    submitted_at: str,
) -> dict[str, Any]:
    intent = store.read("execution_intents", execution_intent_ref)["record"]
    failures = [
        store.read("pre_execution_gate_results", ref)["record"]["failure_reason"]
        for ref in gate_result_refs
        if not store.read("pre_execution_gate_results", ref)["record"]["result"]
    ]
    payload = {
        "execution_intent_ref": execution_intent_ref,
        "adapter_name": adapter_name,
        "runtime_mode": runtime_mode,
        "target_account": intent["target_account"],
        "target_symbol": intent["target_symbol"],
        "target_side": intent["target_side"],
        "target_quantity": intent["target_quantity"],
        "target_order_type": intent["target_order_type"],
        "target_limit_price": intent["target_limit_price"],
        "target_time_in_force": intent["target_time_in_force"],
    }
    status = "rejected_local" if failures else "submitted"
    payload_hash = content_hash(payload)
    artifact_hash = content_hash({"payload": payload, "status": status})
    submission = ExecutionSubmission(
        submission_id=f"execution-submission-{content_hash({'intent': execution_intent_ref, 'adapter_name': adapter_name, 'status': status})[:12]}",
        execution_intent_ref=execution_intent_ref,
        adapter_name=adapter_name,
        submitted_at=submitted_at,
        submission_payload_hash=payload_hash,
        broker_request_ref="",
        status=status,
        artifact_hash=artifact_hash,
    )
    store.write_immutable(
        "execution_submissions",
        submission.submission_id,
        submission,
        artifact_type="ExecutionSubmission",
        created_at=submitted_at,
    )
    transition_refs: list[str] = []
    receipt_refs: list[str] = []
    if failures:
        return {
            "submission_ref": submission.submission_id,
            "receipt_refs": tuple(receipt_refs),
            "transition_refs": tuple(transition_refs),
            "status": status,
        }
    adapter_result = submit_via_adapter(adapter_name=adapter_name, runtime_mode=runtime_mode, payload=payload)
    receipt_ref, transition_ref = write_receipt(
        store,
        submission_ref=submission.submission_id,
        receipt_type=adapter_result["receipt_type"],
        received_at=submitted_at,
        external_execution_ref=adapter_result["external_execution_ref"],
        order_status=adapter_result["order_status"],
        filled_quantity=adapter_result["filled_quantity"],
        remaining_quantity=adapter_result["remaining_quantity"],
        average_fill_price=adapter_result["average_fill_price"],
        fee_amount=adapter_result["fee_amount"],
        message=adapter_result["message"],
    )
    receipt_refs.append(receipt_ref)
    transition_refs.append(transition_ref)
    return {
        "submission_ref": submission.submission_id,
        "receipt_refs": tuple(receipt_refs),
        "transition_refs": tuple(transition_refs),
        "status": status,
        "broker_request_ref": adapter_result["broker_request_ref"],
    }
