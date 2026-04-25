from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from .dto import markers, view_envelope
from .workflow_contracts import validate_workflow_payload


_WORKFLOW_STATE_RANK = {
    "ready": 0,
    "attention_needed": 1,
    "investigate": 2,
    "action_available": 3,
    "blocked": 4,
    "fail_closed": 5,
}

_PRIORITY_RANK = {
    "low": 0,
    "medium": 1,
    "high": 2,
    "critical": 3,
}

_DATA_CONDITION_RANK = {
    "fresh": 0,
    "stale": 1,
    "degraded": 2,
    "unknown": 3,
    "fail_closed": 4,
}

_TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELLED", "REJECTED"}
_CRITICAL_ALERT_SEVERITIES = {"CRITICAL", "ERROR"}
_HIGH_ALERT_SEVERITIES = {"WARNING"}


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_dict_list(value: Any) -> List[Dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _normalized_truth_state(value: Any) -> str:
    normalized = str(value or "").strip()
    return normalized or "UNKNOWN"


def _normalized_data_condition(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _DATA_CONDITION_RANK:
        return normalized
    return "unknown"


def _normalize_ref(ref: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(ref, dict):
        return None
    path = ref.get("path") or ref.get("artifact_path") or ""
    label = ref.get("label") or ref.get("logical_name") or ref.get("artifact_type") or ref.get("title") or "artifact"
    artifact_type = ref.get("artifact_type") or "artifact"
    last_update_utc = ref.get("last_update_utc") or ref.get("generated_at_utc")
    if not str(path).strip() and not str(label).strip():
        return None
    return {
        "label": str(label or "artifact"),
        "path": str(path or ""),
        "artifact_type": str(artifact_type or "artifact"),
        "last_update_utc": last_update_utc,
    }


def _unique_evidence_refs(*collections: Iterable[Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()
    for collection in collections:
        for item in collection or []:
            normalized = _normalize_ref(item)
            if not normalized:
                continue
            key = (
                normalized.get("label"),
                normalized.get("path"),
                normalized.get("artifact_type"),
                normalized.get("last_update_utc"),
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append(normalized)
    return rows


def _workflow_next_step(
    *,
    next_step_kind: str,
    title: str,
    rationale: str,
    target_surface: str,
    evidence_refs: Iterable[Any],
    target_entity_id: Optional[str] = None,
    action_id: Optional[str] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "next_step_kind": next_step_kind,
        "title": title,
        "rationale": rationale,
        "target_surface": target_surface,
        "evidence_refs": _unique_evidence_refs(evidence_refs),
    }
    if target_entity_id:
        payload["target_entity_id"] = target_entity_id
    if action_id:
        payload["action_id"] = action_id
    return payload


def _workflow_payload(
    *,
    workflow_state: str,
    action_readiness: str,
    operator_priority: str,
    next_steps: List[Dict[str, Any]],
    evidence_refs: List[Dict[str, Any]],
    reasons: List[str],
    derived_from_surface: Dict[str, Any],
    truth_state: str,
    data_condition: str,
) -> Dict[str, Any]:
    payload = {
        "workflow_state": workflow_state,
        "action_readiness": action_readiness,
        "operator_priority": operator_priority,
        "next_steps": next_steps,
        "evidence_refs": evidence_refs,
        "reasons": reasons,
        "derived_from_surface": derived_from_surface,
        "truth_state": truth_state,
        "data_condition": data_condition,
    }
    payload["_workflow_validation"] = validate_workflow_payload(payload)
    return payload


def _surface_descriptor(view: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "view_name": str(view.get("view_name") or "UNKNOWN"),
        "contract_id": str(view.get("contract_id") or "UNKNOWN"),
        "surface_kind": str(view.get("surface_kind") or "UNKNOWN"),
    }


def _freshness_state_for_summary(views: List[Dict[str, Any]]) -> str:
    ranked = sorted(
        [
            (
                _DATA_CONDITION_RANK.get(_normalized_data_condition(view.get("data_condition")), 999),
                str(view.get("freshness_state") or "unknown"),
            )
            for view in views
            if isinstance(view, dict)
        ],
        reverse=True,
    )
    return ranked[0][1] if ranked else "unknown"


def _summary_truth_state(cards: List[Dict[str, Any]]) -> str:
    states = {_normalized_truth_state(card.get("truth_state")) for card in cards if isinstance(card, dict)}
    if len(states) == 1:
        return next(iter(states))
    if len(states) > 1:
        return "derived"
    return "UNKNOWN"


def _summary_data_condition(cards: List[Dict[str, Any]]) -> str:
    conditions = [_normalized_data_condition(card.get("data_condition")) for card in cards if isinstance(card, dict)]
    if not conditions:
        return "unknown"
    return max(conditions, key=lambda item: _DATA_CONDITION_RANK.get(item, 999))


def _summary_state(cards: List[Dict[str, Any]]) -> str:
    states = [str(card.get("workflow_state") or "investigate") for card in cards if isinstance(card, dict)]
    if not states:
        return "investigate"
    return max(states, key=lambda item: _WORKFLOW_STATE_RANK.get(item, -1))


def _summary_priority(cards: List[Dict[str, Any]]) -> str:
    priorities = [str(card.get("operator_priority") or "medium") for card in cards if isinstance(card, dict)]
    if not priorities:
        return "medium"
    return max(priorities, key=lambda item: _PRIORITY_RANK.get(item, -1))


def _active_order_rows(view: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = _as_dict_list(view.get("orders"))
    return [
        row
        for row in rows
        if str(row.get("lifecycle_status") or "").upper() not in _TERMINAL_ORDER_STATUSES
    ]


def _relevant_action(action_inventory_view: Optional[Dict[str, Any]], action_name: str) -> Optional[Dict[str, Any]]:
    inventory = _as_dict(action_inventory_view)
    actions = _as_dict_list(inventory.get("actions"))
    for action in actions:
        if str(action.get("action_name") or "") == action_name:
            return action
    return None


def _apply_action_inventory(
    card: Dict[str, Any],
    *,
    action_inventory_view: Optional[Dict[str, Any]],
    action_name: Optional[str],
    admin_title: str,
    unsupported_reason_prefix: str,
) -> Dict[str, Any]:
    if not action_name:
        return card

    action = _relevant_action(action_inventory_view, action_name)
    if action is None:
        if card.get("workflow_state") not in {"ready"}:
            card["action_readiness"] = "unknown"
            card["reasons"] = list(card.get("reasons") or []) + [f"{unsupported_reason_prefix}: UNKNOWN"]
        return card

    if action.get("supported") is True:
        card["workflow_state"] = "action_available"
        card["action_readiness"] = "allowed"
        card["next_steps"] = list(card.get("next_steps") or []) + [
            _workflow_next_step(
                next_step_kind="open_admin_action",
                title=admin_title,
                rationale=str(action.get("reason") or "Governed action is available."),
                target_surface="admin",
                evidence_refs=card.get("evidence_refs") or [],
                action_id=action_name,
            )
        ]
        return card

    card["action_readiness"] = "blocked"
    card["reasons"] = list(card.get("reasons") or []) + [f"{unsupported_reason_prefix}: {action.get('reason') or 'unsupported'}"]
    card["next_steps"] = list(card.get("next_steps") or []) + [
        _workflow_next_step(
            next_step_kind="open_admin_action",
            title=f"Review {action_name} support",
            rationale=str(action.get("reason") or "Governed action is not currently supported."),
            target_surface="admin",
            evidence_refs=card.get("evidence_refs") or [],
        )
    ]
    return card


def derive_operations_workflow(operations_view: Dict[str, Any]) -> Dict[str, Any]:
    blocking_conditions = _as_dict_list(operations_view.get("blocking_conditions"))
    truth_state = _normalized_truth_state(operations_view.get("truth_state"))
    data_condition = _normalized_data_condition(operations_view.get("data_condition"))
    operator_action_required = bool(operations_view.get("operator_action_required"))
    evidence = _unique_evidence_refs(
        operations_view.get("provenance_refs") or [],
        operations_view.get("source_refs") or [],
        [ref for item in blocking_conditions for ref in _as_dict_list(item.get("provenance_refs"))],
    )
    reasons: List[str] = []
    next_steps: List[Dict[str, Any]] = []

    if data_condition in {"stale", "unknown", "fail_closed"} or truth_state == "UNKNOWN":
        workflow_state = "fail_closed"
        action_readiness = "blocked"
        operator_priority = "critical"
        reasons.append("Operations readiness data is stale, unknown, or fail-closed.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="wait_for_fresh_data",
                title="Wait for fresh readiness data",
                rationale="Workflow remains fail-closed until the operations surface refreshes with trustworthy readiness inputs.",
                target_surface="operations",
                evidence_refs=evidence,
            )
        )
        if blocking_conditions:
            first_blocker = blocking_conditions[0]
            next_steps.append(
                _workflow_next_step(
                    next_step_kind="inspect_blocker",
                    title=f"Inspect blocker: {first_blocker.get('source_name') or 'UNKNOWN'}",
                    rationale=str(first_blocker.get("status") or "UNKNOWN"),
                    target_surface="operations",
                    evidence_refs=first_blocker.get("provenance_refs") or evidence,
                )
            )
    elif blocking_conditions:
        workflow_state = "blocked"
        action_readiness = "unknown"
        operator_priority = "high"
        reasons.extend(
            [
                f"{item.get('source_name') or 'UNKNOWN'}={item.get('status') or 'UNKNOWN'}"
                for item in blocking_conditions
            ]
        )
        first_blocker = blocking_conditions[0]
        next_steps.append(
            _workflow_next_step(
                next_step_kind="inspect_blocker",
                title=f"Inspect blocker: {first_blocker.get('source_name') or 'UNKNOWN'}",
                rationale="Operator review is required before treating the system as ready.",
                target_surface="operations",
                evidence_refs=first_blocker.get("provenance_refs") or evidence,
            )
        )
    elif operator_action_required:
        workflow_state = "attention_needed"
        action_readiness = "unknown"
        operator_priority = "medium"
        reasons.append("Operations surface requests operator attention.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="inspect_projection",
                title="Inspect readiness summary",
                rationale="Operator action was requested by the operations workspace without a proven governed action mapping.",
                target_surface="operations",
                evidence_refs=evidence,
            )
        )
    else:
        workflow_state = "ready"
        action_readiness = "unavailable"
        operator_priority = "low"
        reasons.append("Operations readiness does not present blockers.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="no_action",
                title="No operations action required",
                rationale="Current operations surface does not indicate a blocking or attention state.",
                target_surface="operations",
                evidence_refs=evidence,
            )
        )

    return _workflow_payload(
        workflow_state=workflow_state,
        action_readiness=action_readiness,
        operator_priority=operator_priority,
        next_steps=next_steps,
        evidence_refs=evidence,
        reasons=reasons,
        derived_from_surface=_surface_descriptor(operations_view),
        truth_state=truth_state,
        data_condition=data_condition,
    )


def derive_alerts_workflow(alerts_view: Dict[str, Any]) -> Dict[str, Any]:
    alerts = _as_dict_list(alerts_view.get("alerts"))
    truth_state = _normalized_truth_state(alerts_view.get("truth_state"))
    data_condition = _normalized_data_condition(alerts_view.get("data_condition"))
    evidence = _unique_evidence_refs(
        alerts_view.get("provenance_refs") or [],
        alerts_view.get("source_refs") or [],
        [ref for item in alerts for ref in _as_dict_list(item.get("provenance_refs"))],
        [ref for item in alerts for ref in _as_dict_list(item.get("affected_artifact_refs"))],
    )
    reasons: List[str] = []
    next_steps: List[Dict[str, Any]] = []

    highest_alert = None
    highest_priority = "low"
    for item in alerts:
        severity = str(item.get("severity") or "").upper()
        if severity in _CRITICAL_ALERT_SEVERITIES:
            highest_alert = item
            highest_priority = "critical"
            break
        if severity in _HIGH_ALERT_SEVERITIES:
            highest_alert = highest_alert or item
            highest_priority = "high"
        elif highest_alert is None:
            highest_alert = item
            highest_priority = "medium"

    if data_condition in {"unknown", "fail_closed"} or truth_state == "UNKNOWN":
        workflow_state = "investigate"
        action_readiness = "blocked"
        operator_priority = "high"
        reasons.append("Alert projection is not trustworthy enough for unattended interpretation.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="wait_for_fresh_data",
                title="Wait for fresh alert projection",
                rationale="The alert surface is unknown or fail-closed.",
                target_surface="alerts",
                evidence_refs=evidence,
            )
        )
    elif data_condition == "stale":
        workflow_state = "attention_needed"
        action_readiness = "unavailable"
        operator_priority = "medium"
        reasons.append("Alert data is stale and should be reviewed before use.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="wait_for_fresh_data",
                title="Wait for refreshed alert data",
                rationale="The alert surface is stale.",
                target_surface="alerts",
                evidence_refs=evidence,
            )
        )
    elif not alerts:
        workflow_state = "ready"
        action_readiness = "unavailable"
        operator_priority = "low"
        reasons.append("No active alerts are present.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="no_action",
                title="No alert follow-up required",
                rationale="The alerts surface is currently empty.",
                target_surface="alerts",
                evidence_refs=evidence,
            )
        )
    else:
        critical = highest_priority == "critical" or any(str(item.get("status") or "").upper() == "BLOCKING" for item in alerts)
        workflow_state = "blocked" if critical else "attention_needed"
        action_readiness = "unavailable"
        operator_priority = highest_priority
        reasons.extend([str(item.get("title") or item.get("entity_id") or "UNKNOWN") for item in alerts])
        target = highest_alert or alerts[0]
        next_steps.append(
            _workflow_next_step(
                next_step_kind="review_evidence",
                title=f"Review alert evidence: {target.get('title') or target.get('entity_id') or 'UNKNOWN'}",
                rationale=str(target.get("summary") or target.get("status") or "Alert requires review."),
                target_surface="alerts",
                evidence_refs=(target.get("provenance_refs") or target.get("affected_artifact_refs") or evidence),
                target_entity_id=str(target.get("entity_id") or "") or None,
            )
        )

    return _workflow_payload(
        workflow_state=workflow_state,
        action_readiness=action_readiness,
        operator_priority=operator_priority,
        next_steps=next_steps,
        evidence_refs=evidence,
        reasons=reasons,
        derived_from_surface=_surface_descriptor(alerts_view),
        truth_state=truth_state,
        data_condition=data_condition,
    )


def derive_reconciliation_workflow(reconciliation_view: Dict[str, Any]) -> Dict[str, Any]:
    mismatches = _as_dict_list(reconciliation_view.get("mismatches"))
    truth_state = _normalized_truth_state(reconciliation_view.get("truth_state"))
    data_condition = _normalized_data_condition(reconciliation_view.get("data_condition"))
    evidence = _unique_evidence_refs(
        reconciliation_view.get("provenance_refs") or [],
        reconciliation_view.get("source_refs") or [],
        reconciliation_view.get("evidence_refs") or [],
        [ref for item in mismatches for ref in _as_dict_list(item.get("provenance_refs"))],
    )
    reasons: List[str] = []
    next_steps: List[Dict[str, Any]] = []

    if data_condition in {"unknown", "fail_closed"} or truth_state == "UNKNOWN":
        workflow_state = "investigate"
        action_readiness = "blocked"
        operator_priority = "high"
        reasons.append("Reconciliation state is unknown or fail-closed.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="wait_for_fresh_data",
                title="Wait for fresh reconciliation data",
                rationale="Reconciliation should not be treated as settled while the surface is unknown or fail-closed.",
                target_surface="reconciliation",
                evidence_refs=evidence,
            )
        )
    elif mismatches:
        workflow_state = "investigate"
        action_readiness = "unknown"
        operator_priority = "high"
        reasons.extend([str(item.get("comparison_id") or "UNKNOWN") for item in mismatches])
        first_mismatch = mismatches[0]
        next_steps.append(
            _workflow_next_step(
                next_step_kind="inspect_projection",
                title=f"Inspect mismatch: {first_mismatch.get('comparison_id') or 'UNKNOWN'}",
                rationale=str(first_mismatch.get("reason") or first_mismatch.get("status") or "Mismatch requires reconciliation review."),
                target_surface="reconciliation",
                evidence_refs=first_mismatch.get("provenance_refs") or evidence,
                target_entity_id=str(first_mismatch.get("entity_id") or "") or None,
            )
        )
    else:
        workflow_state = "ready"
        action_readiness = "unavailable"
        operator_priority = "low"
        reasons.append("Reconciliation does not report active mismatches.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="no_action",
                title="No reconciliation follow-up required",
                rationale="The reconciliation surface is currently clean.",
                target_surface="reconciliation",
                evidence_refs=evidence,
            )
        )

    return _workflow_payload(
        workflow_state=workflow_state,
        action_readiness=action_readiness,
        operator_priority=operator_priority,
        next_steps=next_steps,
        evidence_refs=evidence,
        reasons=reasons,
        derived_from_surface=_surface_descriptor(reconciliation_view),
        truth_state=truth_state,
        data_condition=data_condition,
    )


def derive_positions_workflow(positions_view: Dict[str, Any]) -> Dict[str, Any]:
    positions = _as_dict_list(positions_view.get("positions"))
    truth_state = _normalized_truth_state(positions_view.get("truth_state"))
    data_condition = _normalized_data_condition(positions_view.get("data_condition"))
    evidence = _unique_evidence_refs(
        positions_view.get("provenance_refs") or [],
        positions_view.get("source_refs") or [],
        [ref for row in positions for ref in _as_dict_list(row.get("provenance_refs"))],
        [ref for row in positions for ref in _as_dict_list(row.get("evidence_refs"))],
    )
    reasons: List[str] = []
    next_steps: List[Dict[str, Any]] = []
    linked_positions = [row for row in positions if _as_dict_list(row.get("open_order_linkage"))]

    if data_condition in {"unknown", "fail_closed", "stale"} or truth_state == "UNKNOWN":
        workflow_state = "investigate"
        action_readiness = "unavailable"
        operator_priority = "high"
        reasons.append("Positions state is stale or unknown.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="inspect_projection",
                title="Inspect positions state",
                rationale="The positions surface is not fresh enough for passive trust.",
                target_surface="positions",
                evidence_refs=evidence,
            )
        )
    elif linked_positions:
        workflow_state = "attention_needed"
        action_readiness = "unavailable"
        operator_priority = "medium"
        reasons.extend([str(row.get("position_id") or row.get("symbol") or "UNKNOWN") for row in linked_positions])
        target = linked_positions[0]
        linkage = _as_dict_list(target.get("open_order_linkage"))
        next_steps.append(
            _workflow_next_step(
                next_step_kind="inspect_projection",
                title=f"Review position with open orders: {target.get('symbol') or target.get('position_id') or 'UNKNOWN'}",
                rationale=f"{len(linkage)} linked working orders remain associated with this position.",
                target_surface="positions",
                evidence_refs=target.get("provenance_refs") or evidence,
                target_entity_id=str(target.get("position_id") or "") or None,
            )
        )
    else:
        workflow_state = "ready"
        action_readiness = "unavailable"
        operator_priority = "low"
        reasons.append("Positions do not currently indicate follow-up.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="no_action",
                title="No position follow-up required",
                rationale="No open order linkage is present on the positions workspace.",
                target_surface="positions",
                evidence_refs=evidence,
            )
        )

    return _workflow_payload(
        workflow_state=workflow_state,
        action_readiness=action_readiness,
        operator_priority=operator_priority,
        next_steps=next_steps,
        evidence_refs=evidence,
        reasons=reasons,
        derived_from_surface=_surface_descriptor(positions_view),
        truth_state=truth_state,
        data_condition=data_condition,
    )


def derive_orders_workflow(orders_view: Dict[str, Any]) -> Dict[str, Any]:
    active_orders = _active_order_rows(orders_view)
    truth_state = _normalized_truth_state(orders_view.get("truth_state"))
    data_condition = _normalized_data_condition(orders_view.get("data_condition"))
    evidence = _unique_evidence_refs(
        orders_view.get("provenance_refs") or [],
        orders_view.get("source_refs") or [],
        [ref for row in active_orders for ref in _as_dict_list(row.get("provenance_refs"))],
        [ref for row in active_orders for ref in _as_dict_list(row.get("evidence_refs"))],
    )
    reasons: List[str] = []
    next_steps: List[Dict[str, Any]] = []
    unknown_active = [row for row in active_orders if str(row.get("lifecycle_status") or "").upper() == "UNKNOWN"]

    if data_condition in {"unknown", "fail_closed", "stale"} or truth_state == "UNKNOWN":
        workflow_state = "investigate"
        action_readiness = "unavailable"
        operator_priority = "high"
        reasons.append("Orders surface is stale or unknown.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="inspect_projection",
                title="Inspect order lifecycle state",
                rationale="Order lifecycle should not be trusted while stale or unknown.",
                target_surface="orders",
                evidence_refs=evidence,
            )
        )
    elif unknown_active:
        workflow_state = "investigate"
        action_readiness = "unknown"
        operator_priority = "high"
        reasons.extend([str(row.get("submission_id") or "UNKNOWN") for row in unknown_active])
        target = unknown_active[0]
        next_steps.append(
            _workflow_next_step(
                next_step_kind="inspect_projection",
                title=f"Inspect active order: {target.get('submission_id') or 'UNKNOWN'}",
                rationale="An active order has an UNKNOWN lifecycle state.",
                target_surface="orders",
                evidence_refs=target.get("provenance_refs") or evidence,
                target_entity_id=str(target.get("submission_id") or "") or None,
            )
        )
    elif active_orders:
        workflow_state = "attention_needed"
        action_readiness = "unavailable"
        operator_priority = "medium"
        reasons.extend([str(row.get("submission_id") or "UNKNOWN") for row in active_orders])
        target = active_orders[0]
        next_steps.append(
            _workflow_next_step(
                next_step_kind="inspect_projection",
                title=f"Review active order: {target.get('submission_id') or 'UNKNOWN'}",
                rationale=str(target.get("lifecycle_status") or "ACTIVE"),
                target_surface="orders",
                evidence_refs=target.get("provenance_refs") or evidence,
                target_entity_id=str(target.get("submission_id") or "") or None,
            )
        )
    else:
        workflow_state = "ready"
        action_readiness = "unavailable"
        operator_priority = "low"
        reasons.append("No active order lifecycle issues are present.")
        next_steps.append(
            _workflow_next_step(
                next_step_kind="no_action",
                title="No order follow-up required",
                rationale="There are no active orders requiring attention.",
                target_surface="orders",
                evidence_refs=evidence,
            )
        )

    return _workflow_payload(
        workflow_state=workflow_state,
        action_readiness=action_readiness,
        operator_priority=operator_priority,
        next_steps=next_steps,
        evidence_refs=evidence,
        reasons=reasons,
        derived_from_surface=_surface_descriptor(orders_view),
        truth_state=truth_state,
        data_condition=data_condition,
    )


def build_operator_workflow_summary(
    operations_view: Dict[str, Any],
    alerts_view: Dict[str, Any],
    reconciliation_view: Dict[str, Any],
    positions_view: Dict[str, Any],
    orders_view: Dict[str, Any],
    action_inventory_view: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    operations_card = derive_operations_workflow(operations_view)
    alerts_card = derive_alerts_workflow(alerts_view)
    reconciliation_card = derive_reconciliation_workflow(reconciliation_view)
    positions_card = derive_positions_workflow(positions_view)
    orders_card = derive_orders_workflow(orders_view)

    if _as_dict_list(operations_view.get("blocking_conditions")):
        replay_block = any(
            str(item.get("source_name") or "") == "replay_certification_gate_v1"
            for item in _as_dict_list(operations_view.get("blocking_conditions"))
        )
        if replay_block:
            operations_card = _apply_action_inventory(
                operations_card,
                action_inventory_view=action_inventory_view,
                action_name="run-replay-check",
                admin_title="Open replay-check action",
                unsupported_reason_prefix="Replay check action",
            )

    if _as_dict_list(reconciliation_view.get("mismatches")):
        reconciliation_card = _apply_action_inventory(
            reconciliation_card,
            action_inventory_view=action_inventory_view,
            action_name="run-reconciliation",
            admin_title="Open reconciliation action",
            unsupported_reason_prefix="Reconciliation action",
        )

    if any(str(row.get("lifecycle_status") or "").upper() == "UNKNOWN" for row in _active_order_rows(orders_view)):
        orders_card = _apply_action_inventory(
            orders_card,
            action_inventory_view=action_inventory_view,
            action_name="refresh-lifecycle",
            admin_title="Open lifecycle refresh action",
            unsupported_reason_prefix="Lifecycle refresh action",
        )

    workflow_cards = {
        "operations": operations_card,
        "alerts": alerts_card,
        "reconciliation": reconciliation_card,
        "positions": positions_card,
        "orders": orders_card,
    }
    ordered_cards = sorted(
        workflow_cards.items(),
        key=lambda item: _PRIORITY_RANK.get(str(item[1].get("operator_priority") or "low"), -1),
        reverse=True,
    )
    next_steps = []
    seen_steps = set()
    for _, card in ordered_cards:
        for step in card.get("next_steps") or []:
            key = (
                step.get("next_step_kind"),
                step.get("title"),
                step.get("target_surface"),
                step.get("target_entity_id"),
                step.get("action_id"),
            )
            if key in seen_steps:
                continue
            seen_steps.add(key)
            next_steps.append(step)

    cards_only = list(workflow_cards.values())
    evidence = _unique_evidence_refs(
        [ref for card in cards_only for ref in _as_dict_list(card.get("evidence_refs"))],
        [ref for step in next_steps for ref in _as_dict_list(step.get("evidence_refs"))],
    )
    overall_state = _summary_state(cards_only)
    overall_priority = _summary_priority(cards_only)
    overall_truth_state = _summary_truth_state(cards_only)
    overall_data_condition = _summary_data_condition(cards_only)
    reasons = [str(card.get("reasons") or [])[0] for card in cards_only if card.get("reasons")]
    source_authority = sorted(
        {
            str(authority)
            for view in [operations_view, alerts_view, reconciliation_view, positions_view, orders_view]
            for authority in (view.get("source_authority") or [])
            if str(authority or "").strip()
        }
    )

    payload = view_envelope(
        view_name="operator_workflow",
        as_of_utc=max(
            [
                str(view.get("as_of_utc") or "")
                for view in [operations_view, alerts_view, reconciliation_view, positions_view, orders_view]
                if str(view.get("as_of_utc") or "").strip()
            ],
            default=None,
        ),
        freshness_state=_freshness_state_for_summary(
            [operations_view, alerts_view, reconciliation_view, positions_view, orders_view]
        ),
        provenance_markers=markers(overall_truth_state),
        source_refs=evidence,
        surface_kind="composition",
        contract_id="operator_workflow_summary",
        contract_version="v1",
        truth_state=overall_truth_state,
        data_condition=overall_data_condition,
        source_authority=source_authority,
        provenance_refs=evidence,
        workflow_state=overall_state,
        action_readiness=(
            "allowed"
            if any(card.get("action_readiness") == "allowed" for card in cards_only)
            else "blocked"
            if any(card.get("action_readiness") == "blocked" for card in cards_only)
            else "unknown"
            if any(card.get("action_readiness") == "unknown" for card in cards_only)
            else "unavailable"
        ),
        operator_priority=overall_priority,
        next_steps=next_steps,
        evidence_refs=evidence,
        reasons=reasons,
        derived_from_surface=[card.get("derived_from_surface") for card in cards_only],
        workflow_cards=workflow_cards,
    )
    payload["_workflow_validation"] = validate_workflow_payload(payload)
    return payload
