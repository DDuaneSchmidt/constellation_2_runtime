from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


VALID_STATUSES = {
    "CAPTURED",
    "TRIAGED",
    "DECIDED",
    "IMPLEMENTED",
    "VALIDATING",
    "VALIDATED",
    "CLOSED",
    "DEFERRED",
    "REJECTED",
}
VALID_SEVERITIES = {"P0", "P1", "P2", "P3"}
VALID_PRIORITIES = {"P0", "P1", "P2", "P3"}
VALID_CHANGE_TYPES = {
    "BUG",
    "ENHANCEMENT",
    "OPERATOR_FEEDBACK",
    "AUDIT_FINDING",
    "PRODUCT_IMPROVEMENT",
    "ARCHITECTURE_CHANGE",
    "TECHNICAL_DEBT",
    "SAFETY_IMPROVEMENT",
}
ALLOWED_TRANSITIONS = {
    "CAPTURED": {"TRIAGED", "DECIDED", "DEFERRED", "REJECTED"},
    "TRIAGED": {"DECIDED", "DEFERRED", "REJECTED"},
    "DECIDED": {"IMPLEMENTED", "DEFERRED", "REJECTED"},
    "IMPLEMENTED": {"VALIDATING", "VALIDATED", "DEFERRED"},
    "VALIDATING": {"VALIDATED", "DEFERRED"},
    "VALIDATED": {"CLOSED", "DEFERRED"},
    "DEFERRED": {"TRIAGED", "DECIDED", "REJECTED"},
}

REGISTER_PATH = Path("aegis/change_control/aegis_change_control_register_v1.json")


class ChangeControlValidationError(ValueError):
    pass


def load_register(path: Path = REGISTER_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _non_empty(value: Any) -> bool:
    return value is not None and value != "" and value != []


def _require_fields(row: dict[str, Any], fields: list[str], label: str, failures: list[str]) -> None:
    for field in fields:
        if field not in row:
            failures.append(f"{label}:{row.get('id', '<missing-id>')}:missing:{field}")
        elif not isinstance(row[field], list) and not _non_empty(row[field]):
            failures.append(f"{label}:{row.get('id', '<missing-id>')}:missing:{field}")


def _validate_id(prefix: str, value: str, failures: list[str]) -> None:
    if not re.fullmatch(rf"{prefix}-\d{{8}}-\d{{3}}[A-Z]?", value or ""):
        failures.append(f"invalid_id:{value}")


def validate_register(payload: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    if payload.get("schema_id") != "aegis_change_control_register_v1":
        failures.append("schema_id must be aegis_change_control_register_v1")

    intake = payload.get("intake_register", [])
    decisions = payload.get("decision_records", [])
    implementations = payload.get("implementation_records", [])
    validations = payload.get("validation_records", [])

    all_rows = [*intake, *decisions, *implementations, *validations]
    ids: list[str] = []
    for row in all_rows:
        if "id" in row:
            ids.append(row["id"])
    duplicate_ids = sorted({item_id for item_id in ids if ids.count(item_id) > 1})
    for item_id in duplicate_ids:
        failures.append(f"duplicate_id:{item_id}")

    decision_by_id = {row.get("id"): row for row in decisions}
    implementation_by_id = {row.get("id"): row for row in implementations}
    validation_by_id = {row.get("id"): row for row in validations}
    intake_by_id = {row.get("id"): row for row in intake}
    intake_status_by_id = {row.get("id"): row.get("status") for row in intake}

    for row in intake:
        _require_fields(
            row,
            [
                "id",
                "title",
                "description",
                "change_type",
                "status",
                "severity",
                "priority",
                "owner",
                "source",
                "created_at",
                "updated_at",
                "decision_ids",
                "implementation_ids",
                "validation_ids",
                "tags",
            ],
            "intake",
            failures,
        )
        _validate_id("ACC", row.get("id", ""), failures)
        if row.get("change_type") not in VALID_CHANGE_TYPES:
            failures.append(f"intake:{row.get('id')}:invalid_change_type:{row.get('change_type')}")
        if row.get("status") not in VALID_STATUSES:
            failures.append(f"intake:{row.get('id')}:invalid_status:{row.get('status')}")
        if row.get("severity") not in VALID_SEVERITIES:
            failures.append(f"intake:{row.get('id')}:invalid_severity:{row.get('severity')}")
        if row.get("priority") not in VALID_PRIORITIES:
            failures.append(f"intake:{row.get('id')}:invalid_priority:{row.get('priority')}")

        previous_status = row.get("previous_status")
        status = row.get("status")
        if previous_status:
            if previous_status not in VALID_STATUSES:
                failures.append(f"intake:{row.get('id')}:invalid_previous_status:{previous_status}")
            elif status not in ALLOWED_TRANSITIONS.get(previous_status, set()):
                failures.append(f"intake:{row.get('id')}:invalid_transition:{previous_status}->{status}")

        for decision_id in row.get("decision_ids", []):
            if decision_id not in decision_by_id:
                failures.append(f"intake:{row.get('id')}:missing_decision:{decision_id}")
        for implementation_id in row.get("implementation_ids", []):
            if implementation_id not in implementation_by_id:
                failures.append(f"intake:{row.get('id')}:missing_implementation:{implementation_id}")
        for validation_id in row.get("validation_ids", []):
            if validation_id not in validation_by_id:
                failures.append(f"intake:{row.get('id')}:missing_validation:{validation_id}")

        if status in {"VALIDATED", "CLOSED"} and not row.get("validation_ids"):
            failures.append(f"intake:{row.get('id')}:status_requires_validation_record:{status}")
        _validate_relationships(row, intake_by_id, intake_status_by_id, failures)
        if status == "CLOSED":
            if previous_status and previous_status != "VALIDATED":
                failures.append(f"intake:{row.get('id')}:closed_must_follow_validated")
            _validate_closure_evidence(row, validation_by_id, failures)
        if status == "REJECTED" and not row.get("decision_ids"):
            failures.append(f"intake:{row.get('id')}:rejected_requires_decision_record")
        if status == "DEFERRED" and not row.get("decision_ids"):
            failures.append(f"intake:{row.get('id')}:deferred_requires_decision_record")

    for row in decisions:
        _require_fields(row, ["id", "change_id", "decision", "rationale", "decided_by", "decided_at", "evidence_refs"], "decision", failures)
        _validate_id("ACD", row.get("id", ""), failures)
        _validate_change_link(row, intake_by_id, "decision", failures)
        if row.get("decision") not in {"APPROVE", "DEFER", "REJECT", "PRIORITIZE", "NOTE", "NEEDS_MORE_INFORMATION"}:
            failures.append(f"decision:{row.get('id')}:invalid_decision:{row.get('decision')}")
        if row.get("decision") == "DEFER" and not row.get("revisit_condition"):
            failures.append(f"decision:{row.get('id')}:defer_requires_revisit_condition")

    for row in implementations:
        _require_fields(row, ["id", "change_id", "summary", "files_changed", "commands_added", "behavior_changed", "implemented_by", "implemented_at", "evidence_refs"], "implementation", failures)
        _validate_id("ACI", row.get("id", ""), failures)
        _validate_change_link(row, intake_by_id, "implementation", failures)

    for row in validations:
        _require_fields(
            row,
            [
                "id",
                "change_id",
                "validated_at",
                "validated_by",
                "validation_summary",
                "commands_run",
                "test_results",
                "screenshot_evidence",
                "browser_evidence",
                "artifact_evidence",
                "api_evidence",
                "safety_gate_evidence",
                "remaining_gaps",
            ],
            "validation",
            failures,
        )
        _validate_id("ACV", row.get("id", ""), failures)
        _validate_change_link(row, intake_by_id, "validation", failures)

    _validate_bidirectional_links(intake, decisions, "decision_ids", failures)
    _validate_bidirectional_links(intake, implementations, "implementation_ids", failures)
    _validate_bidirectional_links(intake, validations, "validation_ids", failures)

    return {"ok": not failures, "failure_count": len(failures), "failures": failures}


def _list_field(row: dict[str, Any], field: str) -> list[str]:
    value = row.get(field)
    return [str(item) for item in value if str(item or "").strip()] if isinstance(value, list) else []


def _validate_relationships(row: dict[str, Any], intake_by_id: dict[str, dict[str, Any]], intake_status_by_id: dict[str, str], failures: list[str]) -> None:
    row_id = str(row.get("id") or "")
    for field in ["child_ids", "required_child_ids", "dependency_ids", "blocker_ids", "prerequisite_records", "downstream_records"]:
        for linked_id in _list_field(row, field):
            if linked_id == row_id:
                failures.append(f"intake:{row_id}:self_relationship:{field}:{linked_id}")
            elif linked_id not in intake_by_id:
                failures.append(f"intake:{row_id}:missing_relationship:{field}:{linked_id}")

    parent_id = str(row.get("parent_id") or "").strip()
    if parent_id:
        if parent_id == row_id:
            failures.append(f"intake:{row_id}:parent_cannot_be_self")
        elif parent_id not in intake_by_id:
            failures.append(f"intake:{row_id}:missing_parent:{parent_id}")
        else:
            parent = intake_by_id[parent_id]
            if row_id not in _list_field(parent, "child_ids") and row_id not in _list_field(parent, "required_child_ids"):
                failures.append(f"intake:{row_id}:parent_missing_child_link:{parent_id}")

    child_ids = set(_list_field(row, "child_ids"))
    required_child_ids = set(_list_field(row, "required_child_ids"))
    for child_id in child_ids | required_child_ids:
        child = intake_by_id.get(child_id)
        if child and str(child.get("parent_id") or "") != row_id:
            failures.append(f"intake:{row_id}:child_missing_parent_link:{child_id}")

    if row.get("status") in {"VALIDATED", "CLOSED"}:
        open_required = sorted(
            child_id
            for child_id in required_child_ids
            if intake_status_by_id.get(child_id) not in {"VALIDATED", "CLOSED", "REJECTED"}
        )
        if open_required:
            failures.append(f"intake:{row_id}:parent_requires_closed_children_before_{row.get('status')}:{','.join(open_required)}")


def _validate_change_link(row: dict[str, Any], intake_by_id: dict[str, dict[str, Any]], label: str, failures: list[str]) -> None:
    change_id = row.get("change_id")
    if change_id not in intake_by_id:
        failures.append(f"{label}:{row.get('id')}:missing_change:{change_id}")


def _validate_bidirectional_links(intake: list[dict[str, Any]], rows: list[dict[str, Any]], field: str, failures: list[str]) -> None:
    intake_by_id = {row.get("id"): row for row in intake}
    for row in rows:
        change = intake_by_id.get(row.get("change_id"))
        if change and row.get("id") not in change.get(field, []):
            failures.append(f"{row.get('id')}:not_listed_on_intake:{row.get('change_id')}:{field}")


def _validation_records_for(row: dict[str, Any], validation_by_id: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [validation_by_id[item_id] for item_id in row.get("validation_ids", []) if item_id in validation_by_id]


def _validate_closure_evidence(row: dict[str, Any], validation_by_id: dict[str, dict[str, Any]], failures: list[str]) -> None:
    validations = _validation_records_for(row, validation_by_id)
    change_type = row.get("change_type")
    tags = set(row.get("tags", []))

    if not validations:
        failures.append(f"intake:{row.get('id')}:closed_requires_validation_record")
        return

    def any_field(field: str) -> bool:
        return any(bool(record.get(field)) for record in validations)

    if change_type in {"BUG", "ENHANCEMENT", "OPERATOR_FEEDBACK", "PRODUCT_IMPROVEMENT"} and "ui" in tags:
        if not any_field("screenshot_evidence"):
            failures.append(f"intake:{row.get('id')}:ui_closed_requires_screenshot_evidence")
        if "displayed_truth" in tags and not any_field("browser_evidence"):
            failures.append(f"intake:{row.get('id')}:displayed_truth_closed_requires_browser_evidence")

    if "truth_data" in tags or "artifact_api_browser_consistency" in tags:
        if not any_field("artifact_evidence"):
            failures.append(f"intake:{row.get('id')}:truth_closed_requires_artifact_evidence")
        if not any_field("api_evidence"):
            failures.append(f"intake:{row.get('id')}:truth_closed_requires_api_evidence")
        if "operator_facing" in tags and not any_field("browser_evidence"):
            failures.append(f"intake:{row.get('id')}:operator_truth_closed_requires_browser_evidence")

    if change_type == "SAFETY_IMPROVEMENT" or "safety" in tags:
        if not any_field("safety_gate_evidence"):
            failures.append(f"intake:{row.get('id')}:safety_closed_requires_safety_gate_evidence")


LIFECYCLE_STATUSES = [
    "CAPTURED",
    "TRIAGED",
    "DECIDED",
    "IMPLEMENTED",
    "VALIDATING",
    "VALIDATED",
    "CLOSED",
    "DEFERRED",
    "REJECTED",
]


def required_evidence_fields(row: dict[str, Any]) -> list[str]:
    fields: list[str] = []
    tags = set(row.get("tags", []))
    change_type = row.get("change_type")
    if change_type in {"BUG", "ENHANCEMENT", "OPERATOR_FEEDBACK", "PRODUCT_IMPROVEMENT"} and "ui" in tags:
        fields.append("screenshot_evidence")
        if "displayed_truth" in tags:
            fields.append("browser_evidence")
    if "truth_data" in tags or "artifact_api_browser_consistency" in tags:
        fields.extend(["artifact_evidence", "api_evidence"])
        if "operator_facing" in tags:
            fields.append("browser_evidence")
    if change_type == "SAFETY_IMPROVEMENT" or "safety" in tags:
        fields.append("safety_gate_evidence")
    return sorted(set(fields))


def missing_evidence_fields(row: dict[str, Any], validation_by_change: dict[str, list[dict[str, Any]]]) -> list[str]:
    records = validation_by_change.get(row.get("id"), [])
    missing: list[str] = []
    for field in required_evidence_fields(row):
        if not any(bool(record.get(field)) for record in records):
            missing.append(field)
    return missing


def evidence_completeness(row: dict[str, Any], validation_by_change: dict[str, list[dict[str, Any]]]) -> str:
    status = row.get("status")
    required = required_evidence_fields(row)
    if not required:
        return "No special closure evidence required"
    missing = missing_evidence_fields(row, validation_by_change)
    if not missing:
        return "Complete"
    if status in {"IMPLEMENTED", "VALIDATING", "VALIDATED", "CLOSED"}:
        return "Missing: " + ", ".join(missing)
    return "Required before closure: " + ", ".join(missing)


def _records_by_change(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row.get("change_id", ""), []).append(row)
    return grouped


def _priority_sort_key(row: dict[str, Any]) -> tuple[int, str]:
    rank = row.get("strategic_priority_rank")
    try:
        rank_value = int(rank)
    except (TypeError, ValueError):
        rank_value = 999
    return (rank_value, str(row.get("id", "")))


def _relationship_rollup(row: dict[str, Any], intake_by_id: dict[str, dict[str, Any]], validation_by_change: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    child_ids = _list_field(row, "child_ids")
    required_child_ids = _list_field(row, "required_child_ids")
    children = [intake_by_id[item_id] for item_id in child_ids if item_id in intake_by_id]
    required = [intake_by_id[item_id] for item_id in required_child_ids if item_id in intake_by_id]
    complete_statuses = {"VALIDATED", "CLOSED", "REJECTED"}
    required_complete = [item for item in required if item.get("status") in complete_statuses]
    required_open = [item for item in required if item.get("status") not in complete_statuses]
    return {
        "child_count": len(children),
        "required_child_count": len(required),
        "required_children_complete": len(required_complete),
        "required_children_open": len(required_open),
        "required_child_ids_open": [item.get("id") for item in required_open],
        "completion_rollup": f"{len(required_complete)}/{len(required)} required children complete" if required else "No required children",
        "parent_validation_blocked": bool(required_open and row.get("status") in {"IMPLEMENTED", "VALIDATING", "VALIDATED", "CLOSED"}),
        "evidence_completeness": evidence_completeness(row, validation_by_change),
    }


def _relationship_graph(intake: list[dict[str, Any]], validation_by_change: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    intake_by_id = {row.get("id"): row for row in intake}
    parent_rows = [row for row in intake if _list_field(row, "child_ids") or _list_field(row, "required_child_ids")]
    edges = []
    for row in intake:
        row_id = str(row.get("id") or "")
        for child_id in _list_field(row, "child_ids"):
            edges.append({"from": row_id, "to": child_id, "relationship": "child"})
        for child_id in _list_field(row, "required_child_ids"):
            edges.append({"from": row_id, "to": child_id, "relationship": "required_child"})
        for dep_id in _list_field(row, "dependency_ids"):
            edges.append({"from": row_id, "to": dep_id, "relationship": "depends_on"})
        for blocker_id in _list_field(row, "blocker_ids"):
            edges.append({"from": row_id, "to": blocker_id, "relationship": "blocked_by"})
        for prereq_id in _list_field(row, "prerequisite_records"):
            edges.append({"from": row_id, "to": prereq_id, "relationship": "prerequisite"})
    return {
        "parents": [
            {
                **_compact([row], validation_by_change)[0],
                "children": _compact([intake_by_id[item_id] for item_id in _list_field(row, "child_ids") if item_id in intake_by_id], validation_by_change),
                "required_children": _compact([intake_by_id[item_id] for item_id in _list_field(row, "required_child_ids") if item_id in intake_by_id], validation_by_change),
                "rollup": _relationship_rollup(row, intake_by_id, validation_by_change),
            }
            for row in sorted(parent_rows, key=_priority_sort_key)
        ],
        "edges": edges,
        "summary": {
            "parent_count": len(parent_rows),
            "edge_count": len(edges),
            "blocked_parent_count": sum(1 for row in parent_rows if _relationship_rollup(row, intake_by_id, validation_by_change).get("required_children_open")),
        },
    }


def build_report(payload: dict[str, Any]) -> dict[str, Any]:
    intake = payload.get("intake_register", [])
    validations = payload.get("validation_records", [])
    validation_by_change = _records_by_change(validations)
    open_p0_p1 = [row for row in intake if row.get("status") not in {"CLOSED", "REJECTED"} and row.get("priority") in {"P0", "P1"}]
    awaiting_decision = [row for row in intake if row.get("status") in {"CAPTURED", "TRIAGED"}]
    awaiting_validation = [row for row in intake if row.get("status") in {"IMPLEMENTED", "VALIDATING"}]
    recently_closed = [row for row in intake if row.get("status") == "CLOSED"]
    completed_or_validated = [row for row in intake if row.get("status") in {"VALIDATED", "CLOSED"}]
    deferred_rejected = [row for row in intake if row.get("status") in {"DEFERRED", "REJECTED"}]
    deferred = [row for row in intake if row.get("status") == "DEFERRED"]
    backlog = [row for row in intake if row.get("priority") in {"P2", "P3"} and row.get("status") not in {"CLOSED", "REJECTED"}]
    for rows in [open_p0_p1, awaiting_decision, awaiting_validation, recently_closed, completed_or_validated, deferred_rejected, deferred, backlog]:
        rows.sort(key=_priority_sort_key)
    lifecycle_counts = {status: sum(1 for row in intake if row.get("status") == status) for status in LIFECYCLE_STATUSES}
    due_for_evidence = [row for row in intake if row.get("status") in {"IMPLEMENTED", "VALIDATING", "VALIDATED", "CLOSED"}]
    missing_required = [row for row in due_for_evidence if missing_evidence_fields(row, validation_by_change)]
    ui_missing_screenshots = [row for row in due_for_evidence if "screenshot_evidence" in missing_evidence_fields(row, validation_by_change)]
    truth_missing_proof = [row for row in due_for_evidence if {"artifact_evidence", "api_evidence", "browser_evidence"}.intersection(missing_evidence_fields(row, validation_by_change))]
    safety_missing_proof = [row for row in due_for_evidence if "safety_gate_evidence" in missing_evidence_fields(row, validation_by_change)]
    relationship_graph = _relationship_graph(intake, validation_by_change)
    return {
        "schema_id": "aegis_change_control_report_v1",
        "source_register_id": payload.get("register_id"),
        "summary": {
            "total_items": len(intake),
            "open_p0_p1_count": len(open_p0_p1),
            "awaiting_decision_count": len(awaiting_decision),
            "awaiting_validation_count": len(awaiting_validation),
            "recently_closed_count": len(recently_closed),
            "deferred_rejected_count": len(deferred_rejected),
            "deferred_count": len(deferred),
            "v1_1_backlog_count": len(backlog),
        },
        "lifecycle_counts": lifecycle_counts,
        "validation_dashboard": {
            "awaiting_validation": _compact(awaiting_validation, validation_by_change),
            "missing_required_evidence": _compact(missing_required, validation_by_change),
            "ui_items_missing_screenshots": _compact(ui_missing_screenshots, validation_by_change),
            "truth_data_items_missing_artifact_api_browser_proof": _compact(truth_missing_proof, validation_by_change),
            "safety_items_missing_safety_gate_proof": _compact(safety_missing_proof, validation_by_change),
        },
        "decision_dashboard": {
            "awaiting_decision": _compact(awaiting_decision, validation_by_change),
        },
        "closure_evidence_summary": _compact(completed_or_validated, validation_by_change),
        "relationship_graph": relationship_graph,
        "open_p0_p1": _compact(open_p0_p1, validation_by_change),
        "awaiting_decision": _compact(awaiting_decision, validation_by_change),
        "awaiting_validation": _compact(awaiting_validation, validation_by_change),
        "recently_closed": _compact(recently_closed, validation_by_change),
        "deferred_rejected": _compact(deferred_rejected, validation_by_change),
        "deferred": _compact(deferred, validation_by_change),
        "v1_1_backlog": _compact(backlog, validation_by_change),
    }


def _compact(rows: list[dict[str, Any]], validation_by_change: dict[str, list[dict[str, Any]]] | None = None) -> list[dict[str, Any]]:
    validation_by_change = validation_by_change or {}
    return [
        {
            "id": row.get("id"),
            "title": row.get("title"),
            "status": row.get("status"),
            "severity": row.get("severity"),
            "priority": row.get("priority"),
            "owner": row.get("owner"),
            "affected_domain": row.get("affected_domain") or row.get("owner"),
            "decision_required_reason": row.get("decision_required_reason"),
            "proposed_next_step": row.get("proposed_next_step"),
            "evidence_completeness": evidence_completeness(row, validation_by_change),
            "strategic_priority_rank": row.get("strategic_priority_rank"),
            "decision_required": row.get("decision_required"),
            "decision_question": row.get("decision_question"),
            "recommended_option": row.get("recommended_option"),
            "implementation_phase": row.get("implementation_phase"),
            "estimated_complexity": row.get("estimated_complexity"),
            "parent_id": row.get("parent_id"),
            "child_ids": row.get("child_ids", []),
            "required_child_ids": row.get("required_child_ids", []),
            "dependency_ids": row.get("dependency_ids", []),
            "blocker_ids": row.get("blocker_ids", []),
            "prerequisite_records": row.get("prerequisite_records", []),
            "relationship_role": row.get("relationship_role"),
        }
        for row in rows
    ]




CONTROLLED_DECISION_ACTIONS = {"APPROVE", "REJECT", "DEFER", "PRIORITIZE", "NOTE"}
FORBIDDEN_DECISION_ACTIONS = {"CLOSE", "VALIDATE", "IMPLEMENT", "CLOSED", "VALIDATED", "IMPLEMENTED"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _next_record_id(rows: list[dict[str, Any]], prefix: str, day: str) -> str:
    stem = f"{prefix}-{day.replace('-', '')}-"
    max_seen = 0
    for row in rows:
        value = str(row.get("id", ""))
        if value.startswith(stem):
            try:
                max_seen = max(max_seen, int(value.rsplit("-", 1)[-1]))
            except ValueError:
                continue
    return f"{stem}{max_seen + 1:03d}"


def record_controlled_decision(payload: dict[str, Any], path: Path = REGISTER_PATH) -> dict[str, Any]:
    body = payload or {}
    change_id = str(body.get("change_id") or "").strip()
    action = str(body.get("action") or "").strip().upper()
    notes = str(body.get("notes") or "").strip()
    priority = str(body.get("priority") or "").strip().upper()
    decided_by = str(body.get("decided_by") or "David").strip() or "David"

    if action in FORBIDDEN_DECISION_ACTIONS:
        raise ChangeControlValidationError(f"FORBIDDEN_DECISION_ACTION:{action}")
    if action not in CONTROLLED_DECISION_ACTIONS:
        raise ChangeControlValidationError(f"INVALID_DECISION_ACTION:{action}")
    if not change_id:
        raise ChangeControlValidationError("CHANGE_ID_REQUIRED")
    if action == "PRIORITIZE" and priority not in VALID_PRIORITIES:
        raise ChangeControlValidationError("VALID_PRIORITY_REQUIRED")
    if action == "NOTE" and not notes:
        raise ChangeControlValidationError("DECISION_NOTES_REQUIRED")

    register = load_register(path)
    intake = register.get("intake_register", [])
    decisions = register.setdefault("decision_records", [])
    row = next((item for item in intake if item.get("id") == change_id), None)
    if not row:
        raise ChangeControlValidationError(f"CHANGE_NOT_FOUND:{change_id}")

    current_status = str(row.get("status") or "").upper()
    if current_status in {"CLOSED", "VALIDATED"}:
        raise ChangeControlValidationError(f"DECISION_NOT_ALLOWED_FOR_STATUS:{current_status}")

    target_status = current_status
    decision = action
    rationale = notes or f"Controlled decision action recorded: {action}."
    revisit_condition = None
    if action == "APPROVE":
        decision = "APPROVE"
        target_status = "DECIDED"
    elif action == "REJECT":
        decision = "REJECT"
        target_status = "REJECTED"
    elif action == "DEFER":
        decision = "DEFER"
        target_status = "DEFERRED"
        revisit_condition = notes or row.get("revisit_condition") or "Operator deferred; revisit condition not specified."
    elif action == "PRIORITIZE":
        decision = "PRIORITIZE"
        rationale = notes or f"Priority changed to {priority}."
        row["priority"] = priority
    elif action == "NOTE":
        decision = "NOTE"

    if target_status != current_status:
        allowed = ALLOWED_TRANSITIONS.get(current_status, set())
        if target_status not in allowed:
            raise ChangeControlValidationError(f"INVALID_DECISION_TRANSITION:{current_status}->{target_status}")
        row["previous_status"] = current_status
        row["status"] = target_status

    now = _utc_now()
    day = now[:10]
    decision_record = {
        "id": _next_record_id(decisions, "ACD", day),
        "change_id": change_id,
        "decision": decision,
        "rationale": rationale,
        "decided_by": decided_by,
        "decided_at": now,
        "revisit_condition": revisit_condition,
        "evidence_refs": ["/aegis-change-control controlled decision workflow"],
        "decision_notes": notes,
        "controlled_action": action,
        "priority_after": row.get("priority"),
        "status_after": row.get("status"),
    }
    decisions.append(decision_record)
    row.setdefault("decision_ids", []).append(decision_record["id"])
    row["updated_at"] = now
    register["generated_at"] = now

    validation = validate_register(register)
    if not validation.get("ok"):
        raise ChangeControlValidationError(json.dumps(validation, indent=2))
    path.write_text(json.dumps(register, indent=2) + "\n", encoding="utf-8")
    return {
        "ok": True,
        "change_id": change_id,
        "decision_record": decision_record,
        "item": row,
        "validation": validation,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def run_validate(path: Path = REGISTER_PATH) -> dict[str, Any]:
    result = validate_register(load_register(path))
    if not result["ok"]:
        raise ChangeControlValidationError(json.dumps(result, indent=2))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate or report Aegis Change Control V1")
    parser.add_argument("mode", choices=["validate", "report"])
    parser.add_argument("--path", default=str(REGISTER_PATH))
    args = parser.parse_args()

    path = Path(args.path)
    payload = load_register(path)
    if args.mode == "validate":
        result = validate_register(payload)
        print(json.dumps(result, indent=2))
        return 0 if result["ok"] else 1

    validation = validate_register(payload)
    report = build_report(payload)
    report["validation"] = validation
    print(json.dumps(report, indent=2))
    return 0 if validation["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

