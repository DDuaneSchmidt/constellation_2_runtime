from __future__ import annotations

from typing import Any, Dict, List


WORKFLOW_STATES = (
    "ready",
    "attention_needed",
    "blocked",
    "investigate",
    "action_available",
    "fail_closed",
)

ACTION_READINESS = (
    "allowed",
    "blocked",
    "unavailable",
    "unknown",
)

NEXT_STEP_KINDS = (
    "review_evidence",
    "inspect_blocker",
    "inspect_projection",
    "open_admin_action",
    "wait_for_fresh_data",
    "no_action",
)

OPERATOR_PRIORITY = (
    "low",
    "medium",
    "high",
    "critical",
)

REQUIRED_WORKFLOW_FIELDS = (
    "workflow_state",
    "action_readiness",
    "operator_priority",
    "next_steps",
    "evidence_refs",
)

REQUIRED_NEXT_STEP_FIELDS = (
    "next_step_kind",
    "title",
    "rationale",
    "target_surface",
    "evidence_refs",
)


def _presence_map(payload: Dict[str, Any], fields: tuple[str, ...]) -> Dict[str, bool]:
    return {field: field in payload for field in fields}


def validate_next_step(item: Any) -> Dict[str, Any]:
    if not isinstance(item, dict):
        return {
            "ok": False,
            "kind": "workflow_next_step",
            "errors": ["next step is not a dict"],
            "missing_fields": list(REQUIRED_NEXT_STEP_FIELDS),
            "field_presence": {},
            "next_step_kind_valid": False,
            "evidence_refs_valid": False,
        }

    missing_fields = [field for field in REQUIRED_NEXT_STEP_FIELDS if field not in item]
    next_step_kind = item.get("next_step_kind")
    evidence_refs = item.get("evidence_refs")
    next_step_kind_valid = next_step_kind in NEXT_STEP_KINDS
    evidence_refs_valid = isinstance(evidence_refs, list)
    errors: List[str] = []
    if not next_step_kind_valid:
        errors.append("next_step_kind is missing or invalid")
    if not evidence_refs_valid:
        errors.append("evidence_refs must be a list")

    return {
        "ok": not missing_fields and next_step_kind_valid and evidence_refs_valid,
        "kind": "workflow_next_step",
        "errors": errors,
        "missing_fields": missing_fields,
        "field_presence": _presence_map(item, REQUIRED_NEXT_STEP_FIELDS),
        "next_step_kind_valid": next_step_kind_valid,
        "evidence_refs_valid": evidence_refs_valid,
    }


def validate_workflow_payload(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "kind": "workflow_payload",
            "errors": ["payload is not a dict"],
            "missing_fields": list(REQUIRED_WORKFLOW_FIELDS),
            "field_presence": {},
            "workflow_state_valid": False,
            "action_readiness_valid": False,
            "operator_priority_valid": False,
            "next_steps_valid": False,
            "evidence_refs_valid": False,
            "next_step_results": [],
        }

    missing_fields = [field for field in REQUIRED_WORKFLOW_FIELDS if field not in payload]
    workflow_state_valid = payload.get("workflow_state") in WORKFLOW_STATES
    action_readiness_valid = payload.get("action_readiness") in ACTION_READINESS
    operator_priority_valid = payload.get("operator_priority") in OPERATOR_PRIORITY
    next_steps = payload.get("next_steps")
    evidence_refs = payload.get("evidence_refs")
    next_steps_valid = isinstance(next_steps, list)
    evidence_refs_valid = isinstance(evidence_refs, list)
    next_step_results = [validate_next_step(item) for item in next_steps] if isinstance(next_steps, list) else []

    errors: List[str] = []
    if not workflow_state_valid:
        errors.append("workflow_state is missing or invalid")
    if not action_readiness_valid:
        errors.append("action_readiness is missing or invalid")
    if not operator_priority_valid:
        errors.append("operator_priority is missing or invalid")
    if not next_steps_valid:
        errors.append("next_steps must be a list")
    if not evidence_refs_valid:
        errors.append("evidence_refs must be a list")
    if any(not result.get("ok") for result in next_step_results):
        errors.append("one or more next_steps are invalid")

    return {
        "ok": (
            not missing_fields
            and workflow_state_valid
            and action_readiness_valid
            and operator_priority_valid
            and next_steps_valid
            and evidence_refs_valid
            and all(result.get("ok") for result in next_step_results)
        ),
        "kind": "workflow_payload",
        "errors": errors,
        "missing_fields": missing_fields,
        "field_presence": _presence_map(payload, REQUIRED_WORKFLOW_FIELDS),
        "workflow_state_valid": workflow_state_valid,
        "action_readiness_valid": action_readiness_valid,
        "operator_priority_valid": operator_priority_valid,
        "next_steps_valid": next_steps_valid,
        "evidence_refs_valid": evidence_refs_valid,
        "next_step_results": next_step_results,
    }
