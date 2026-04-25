from __future__ import annotations

from typing import Any

from .schemas import content_hash
from .types import ExecutionIntent
from ..meta_governance.startup_gate import validate_runtime_authority
from ..meta_governance.store import ArtifactStore


EXECUTABLE_ACTION_FAMILIES = {
    "close_position_candidate",
    "reduce_exposure_candidate",
    "rebalance_candidate",
}


def _intent_input(action: dict[str, Any], overrides: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(action.get("estimated_scope", {}))
    if overrides:
        merged.update(overrides)
    return merged


def build_execution_intents(
    store: ArtifactStore,
    *,
    candidate_action_refs: tuple[str, ...],
    action_plan_ref: str,
    execution_eligibility_refs: tuple[str, ...],
    intent_inputs_by_action: dict[str, dict[str, Any]] | None = None,
) -> tuple[ExecutionIntent, ...]:
    action_plan = store.read("autonomous_action_plans", action_plan_ref)["record"]
    eligibilities = {
        store.read("execution_eligibilities", ref)["record"]["candidate_action_id"]: store.read("execution_eligibilities", ref)["record"]
        for ref in execution_eligibility_refs
    }
    authority = validate_runtime_authority(store, actor="execution_intent_generation", event_type="execution_intent_generation_blocked")
    snapshot = authority["snapshot"]
    intents: dict[str, ExecutionIntent] = {}
    for ref in sorted(candidate_action_refs):
        if ref not in action_plan["action_refs"]:
            continue
        action = store.read("candidate_actions", ref)["record"]
        eligibility = eligibilities.get(ref)
        if not eligibility or not eligibility["eligible_for_execution"]:
            continue
        if action["action_family"] not in EXECUTABLE_ACTION_FAMILIES:
            continue
        intent_input = _intent_input(action, (intent_inputs_by_action or {}).get(ref))
        payload = {
            "candidate_action_ref": ref,
            "action_plan_ref": action_plan_ref,
            "active_snapshot_id": snapshot["snapshot_id"],
            "graph_hash": snapshot["graph_hash"],
            "interpreter_version": snapshot["interpreter_version"],
            "target_account": intent_input.get("target_account"),
            "target_symbol": intent_input.get("target_symbol"),
            "target_side": intent_input.get("target_side"),
            "target_quantity": intent_input.get("target_quantity"),
            "target_order_type": intent_input.get("target_order_type", "MKT"),
            "target_limit_price": intent_input.get("target_limit_price"),
            "target_time_in_force": intent_input.get("target_time_in_force", "DAY"),
            "rationale": action["rationale"],
            "supporting_evidence_refs": tuple(sorted(set(tuple(action["supporting_evidence_refs"]) + tuple(action["source_refs"]) + (action_plan_ref, ref)))),
        }
        artifact_hash = content_hash(payload)
        intent = ExecutionIntent(
            execution_intent_id=f"execution-intent-{artifact_hash[:12]}",
            candidate_action_ref=ref,
            action_plan_ref=action_plan_ref,
            active_snapshot_id=snapshot["snapshot_id"],
            graph_hash=snapshot["graph_hash"],
            interpreter_version=snapshot["interpreter_version"],
            target_account=payload["target_account"],
            target_symbol=payload["target_symbol"],
            target_side=payload["target_side"],
            target_quantity=payload["target_quantity"],
            target_order_type=payload["target_order_type"],
            target_limit_price=payload["target_limit_price"],
            target_time_in_force=payload["target_time_in_force"],
            rationale=action["rationale"],
            supporting_evidence_refs=payload["supporting_evidence_refs"],
            artifact_hash=artifact_hash,
        )
        intents[intent.execution_intent_id] = intent
    return tuple(intents[key] for key in sorted(intents))
