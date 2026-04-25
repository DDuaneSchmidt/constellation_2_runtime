from __future__ import annotations

from .execution_state_machine import transition_submission
from .schemas import content_hash
from .types import ExecutionReceipt
from ..meta_governance.audit_log import append_runtime_event
from ..meta_governance.store import ArtifactStore


RECEIPT_TO_STATE = {
    "ack": "accepted",
    "partial_fill": "partially_filled",
    "full_fill": "fully_filled",
    "cancel_ack": "canceled",
    "reject": "rejected",
    "timeout_notice": "timed_out",
    "replace_ack": "accepted",
}


def write_receipt(
    store: ArtifactStore,
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
) -> tuple[str, str]:
    payload = {
        "submission_ref": submission_ref,
        "receipt_type": receipt_type,
        "received_at": received_at,
        "external_execution_ref": external_execution_ref,
        "order_status": order_status,
        "filled_quantity": float(filled_quantity),
        "remaining_quantity": float(remaining_quantity),
        "average_fill_price": average_fill_price,
        "fee_amount": fee_amount,
        "message": message,
    }
    artifact_hash = content_hash(payload)
    receipt = ExecutionReceipt(
        receipt_id=f"execution-receipt-{artifact_hash[:12]}",
        submission_ref=submission_ref,
        receipt_type=receipt_type,
        received_at=received_at,
        external_execution_ref=external_execution_ref,
        order_status=order_status,
        filled_quantity=float(filled_quantity),
        remaining_quantity=float(remaining_quantity),
        average_fill_price=average_fill_price,
        fee_amount=fee_amount,
        message=message,
        artifact_hash=artifact_hash,
    )
    existing_receipt = store.exists("execution_receipts", receipt.receipt_id)
    store.write_immutable(
        "execution_receipts",
        receipt.receipt_id,
        receipt,
        artifact_type="ExecutionReceipt",
        created_at=received_at,
    )
    if existing_receipt:
        for transition_ref in store.list_ids("execution_state_transitions"):
            transition = store.read("execution_state_transitions", transition_ref)["record"]
            if transition["receipt_ref"] == receipt.receipt_id:
                return receipt.receipt_id, transition_ref
    new_state = RECEIPT_TO_STATE[receipt_type]
    transition_ref = transition_submission(
        store,
        submission_ref=submission_ref,
        new_state=new_state,
        triggered_by="execution_receipt",
        receipt_ref=receipt.receipt_id,
        occurred_at=received_at,
        explanation=message,
    )
    submission = store.read("execution_submissions", submission_ref)["record"]
    intent = store.read("execution_intents", submission["execution_intent_ref"])["record"]
    if receipt_type in {"partial_fill", "full_fill"}:
        append_runtime_event(
            store,
            {
                "type": "EXECUTION_RESULT",
                "execution_key": receipt.receipt_id,
                "account_id": intent["target_account"],
                "symbol": intent["target_symbol"],
                "position_id": intent["candidate_action_ref"],
                "action": intent["target_side"],
                "status": "PARTIAL" if receipt_type == "partial_fill" else "FILLED",
                "filled_qty": float(filled_quantity),
                "price": average_fill_price,
                "fee": fee_amount,
                "timestamp": received_at,
            },
        )
    return receipt.receipt_id, transition_ref
