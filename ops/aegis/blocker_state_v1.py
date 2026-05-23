from __future__ import annotations

from typing import Any

from ops.aegis.producer_contracts_v1 import find_contract_for_schema_v1, load_producer_contract_registry_v1


BLOCKER_STATES = {
    "VALIDATED",
    "REJECTED",
    "UNAVAILABLE_EXTERNAL_SOURCE",
    "MANUAL_REQUIRED",
    "NOT_APPLICABLE",
    "FORBIDDEN",
    "STALE",
    "MISSING",
    "TAMPERED",
    "MALFORMED",
    "UNKNOWN_PRODUCER",
    "CONTRACT_VIOLATION",
}


def build_blocker_state_report_v1(
    *,
    blockers: list[dict[str, Any]],
    evidence_results: dict[str, dict[str, Any]],
    registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    registry = registry or load_producer_contract_registry_v1()
    states = [
        classify_blocker_v1(blocker=blocker, evidence_results=evidence_results, registry=registry)
        for blocker in blockers
    ]
    root = [row for row in states if str(row.get("blocker_id") or "").startswith("EVIDENCE_")]
    return {
        "schema_id": "aegis_blocker_states",
        "schema_version": "v1",
        "root_blockers": root,
        "blocker_state": states,
        "repairability": {str(row["blocker_id"]): row.get("repairability") for row in states},
        "producer_contract_ref": {str(row["blocker_id"]): row.get("producer_contract_ref") for row in states},
        "next_safe_action": {str(row["blocker_id"]): row.get("next_safe_action") for row in states},
    }


def classify_blocker_v1(*, blocker: dict[str, Any], evidence_results: dict[str, dict[str, Any]], registry: dict[str, Any]) -> dict[str, Any]:
    blocker_id = str(blocker.get("blocker_id") or "")
    if not blocker_id.startswith("EVIDENCE_"):
        return {"blocker_id": blocker_id, "schema_id": "", "state": "NOT_APPLICABLE", "reason": str(blocker.get("reason") or ""), "repairability": "NONE", "producer_contract_ref": {}, "next_safe_action": "Resolve upstream evidence blockers."}
    schema_id = blocker_id.rsplit(":", 1)[-1] if ":" in blocker_id else ""
    result = evidence_results.get(schema_id) or {}
    status = str(result.get("status") or "")
    contract_status = str(result.get("contract_status") or "")
    contract = find_contract_for_schema_v1(schema_id, registry) if schema_id else None
    state = _state_for(status=status, contract_status=contract_status, contract=contract)
    repairability = _repairability_for(state=state, contract=contract)
    return {
        "blocker_id": blocker_id,
        "schema_id": schema_id,
        "state": state,
        "reason": str(blocker.get("reason") or result.get("reason") or ""),
        "repairability": repairability,
        "producer_contract_ref": _contract_ref(contract),
        "next_safe_action": _next_safe_action(state=state, contract=contract, schema_id=schema_id),
    }


def _state_for(*, status: str, contract_status: str, contract: dict[str, Any] | None) -> str:
    if contract_status == "UNKNOWN_PRODUCER":
        return "UNKNOWN_PRODUCER"
    if contract_status and contract_status != "CONTRACT_VALID":
        return "CONTRACT_VIOLATION"
    if not contract:
        return "UNKNOWN_PRODUCER"
    repair_class = str(contract.get("repair_class") or "")
    if repair_class == "FORBIDDEN":
        return "FORBIDDEN"
    if repair_class == "MANUAL_REQUIRED" and status in {"MISSING", "REJECTED", "INVALID", "MALFORMED", "UNVERIFIABLE"}:
        return "MANUAL_REQUIRED"
    if repair_class == "EXTERNAL_DATA_BOUNDED" and status in {"MISSING", "UNVERIFIABLE"}:
        return "UNAVAILABLE_EXTERNAL_SOURCE"
    if status in {"REJECTED", "INVALID"}:
        return "REJECTED"
    if status == "STALE":
        return "STALE"
    if status == "MISSING":
        return "MISSING"
    if status == "TAMPERED":
        return "TAMPERED"
    if status in {"MALFORMED", "WRONG_DAY", "FUTURE_DATED", "UNVERIFIABLE"}:
        return "MALFORMED"
    if status == "OK":
        return "VALIDATED"
    return "NOT_APPLICABLE" if not status else "MALFORMED"


def _repairability_for(*, state: str, contract: dict[str, Any] | None) -> str:
    if state in {"VALIDATED", "NOT_APPLICABLE"}:
        return "NONE"
    if state in {"UNKNOWN_PRODUCER", "CONTRACT_VIOLATION", "TAMPERED", "MALFORMED", "FORBIDDEN"}:
        return "BLOCKED"
    if not contract:
        return "BLOCKED"
    return str(contract.get("repair_class") or "MANUAL_REQUIRED")


def _next_safe_action(*, state: str, contract: dict[str, Any] | None, schema_id: str) -> str:
    if state == "VALIDATED":
        return "No repair required."
    if state == "UNKNOWN_PRODUCER":
        return f"Declare a producer contract for {schema_id} before trusting this evidence."
    if state == "CONTRACT_VIOLATION":
        return f"Fix producer contract violation for {schema_id}; do not use generated evidence."
    if state == "MANUAL_REQUIRED":
        return f"Collect operator intent/manual evidence for {schema_id}; do not auto-repair."
    if state == "UNAVAILABLE_EXTERNAL_SOURCE":
        return f"Refresh bounded external source for {schema_id} and include raw source hash."
    if state == "FORBIDDEN":
        return f"Do not execute producer for {schema_id}."
    if contract and str(contract.get("repair_class") or "") in {"AUTO_SAFE", "AUTO_DETERMINISTIC"}:
        return str(contract.get("command") or f"Run approved producer for {schema_id}.")
    return f"Remain blocked and inspect {schema_id}."


def _contract_ref(contract: dict[str, Any] | None) -> dict[str, Any]:
    if not contract:
        return {}
    return {
        "producer_id": str(contract.get("producer_id") or ""),
        "producer_version": str(contract.get("producer_version") or ""),
        "repair_class": str(contract.get("repair_class") or ""),
        "determinism_class": str(contract.get("determinism_class") or ""),
        "authority_level": str(contract.get("authority_level") or ""),
    }
