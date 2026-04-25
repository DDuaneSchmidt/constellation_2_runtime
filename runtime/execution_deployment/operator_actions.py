from __future__ import annotations

from .execution_state_machine import OPEN_STATES, current_state
from .schemas import content_hash
from .types import ExecutionOperatorView
from ..meta_governance.store import ArtifactStore


def build_execution_operator_view(
    store: ArtifactStore,
    *,
    deployment_state_ref: str,
    recovery_refs: tuple[str, ...],
    as_of: str,
) -> ExecutionOperatorView:
    pending = tuple(sorted(ref for ref in store.list_ids("execution_submissions") if current_state(store, ref) in OPEN_STATES))
    blocked_intents = []
    for ref in store.list_ids("pre_execution_gate_results"):
        record = store.read("pre_execution_gate_results", ref)["record"]
        if not record["result"]:
            blocked_intents.append(record["execution_intent_id"])
    active_receipts = []
    for ref in store.list_ids("execution_receipts"):
        receipt = store.read("execution_receipts", ref)["record"]
        if receipt["receipt_type"] in {"ack", "partial_fill", "reject", "timeout_notice"}:
            active_receipts.append(ref)
    open_recovery_items = []
    for ref in recovery_refs:
        open_recovery_items.extend(store.read("execution_recovery_records", ref)["record"]["unresolved_refs"])
    deployment = store.read("deployment_states", deployment_state_ref)["record"]
    risks = list(deployment["blocked_execution_reasons"])
    if deployment["unhealthy_components"]:
        risks.extend(f"UNHEALTHY_COMPONENT:{component}" for component in deployment["unhealthy_components"])
    if open_recovery_items:
        risks.append("RECOVERY_BACKLOG_OPEN")
    payload = {
        "as_of": as_of,
        "pending_submissions": tuple(sorted(pending)),
        "blocked_intents": tuple(sorted(set(blocked_intents))),
        "active_receipts": tuple(sorted(active_receipts)),
        "open_recovery_items": tuple(sorted(set(open_recovery_items))),
        "deployment_state_ref": deployment_state_ref,
        "top_execution_risks": tuple(sorted(set(risks)))[:5],
    }
    view_hash = content_hash(payload)
    return ExecutionOperatorView(
        operator_view_id=f"execution-operator-view-{view_hash[:12]}",
        as_of=as_of,
        pending_submissions=payload["pending_submissions"],
        blocked_intents=payload["blocked_intents"],
        active_receipts=payload["active_receipts"],
        open_recovery_items=payload["open_recovery_items"],
        deployment_state_ref=deployment_state_ref,
        top_execution_risks=payload["top_execution_risks"],
        view_hash=view_hash,
    )
