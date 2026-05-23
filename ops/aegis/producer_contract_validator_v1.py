from __future__ import annotations

import fnmatch
from pathlib import Path
from typing import Any

from ops.aegis.producer_contracts_v1 import find_contract_for_event_v1, load_producer_contract_registry_v1


CONTRACT_PASS = "CONTRACT_VALID"
EVIDENCE_EVENT_TYPES = {"EvidenceProduced", "EvidenceValidated", "EvidenceRejected"}


def validate_evidence_event_contract_v1(
    event: dict[str, Any],
    *,
    registry: dict[str, Any] | None = None,
    execution_context: str = "",
    allow_deprecated: bool = False,
) -> dict[str, Any]:
    event_type = str(event.get("event_type") or "")
    if event_type not in EVIDENCE_EVENT_TYPES:
        return _ok(event, None)
    registry = registry or load_producer_contract_registry_v1()
    contract = find_contract_for_event_v1(event, registry)
    if not contract:
        return _fail("UNKNOWN_PRODUCER", "event.producer is not declared in producer contract registry", event, None)
    if bool(contract.get("deprecated")) and not allow_deprecated:
        return _fail("DEPRECATED_PRODUCER", "producer contract is deprecated", event, contract)
    if event_type not in set(str(item) for item in contract.get("allowed_event_types") or []):
        return _fail("EVENT_TYPE_NOT_ALLOWED", f"{event_type} is not allowed by producer contract", event, contract)
    if str(contract.get("determinism_class") or "") == "NON_DETERMINISTIC_FORBIDDEN":
        return _fail("NON_DETERMINISTIC_FORBIDDEN", "non-deterministic producers cannot affect RuntimeEvaluation", event, contract)
    if str(contract.get("repair_class") or "") == "FORBIDDEN" and execution_context == "repair_execute":
        return _fail("FORBIDDEN_PRODUCER", "forbidden producers cannot be executed by repair orchestrator", event, contract)
    if str(event.get("schema_id") or "") != str(contract.get("output_schema_id") or "") and str(contract.get("output_schema_id") or "") != "*":
        return _fail("WRONG_SCHEMA", "event schema_id does not match producer contract output_schema_id", event, contract)
    if str(event.get("schema_version") or "") != str(contract.get("output_schema_version") or ""):
        return _fail("WRONG_SCHEMA_VERSION", "event schema_version does not match producer contract", event, contract)
    if not _artifact_paths_allowed(event, contract):
        return _fail("UNDECLARED_OUTPUT", "event artifact path is not declared by producer contract", event, contract)
    input_hashes = event.get("input_hashes") if isinstance(event.get("input_hashes"), dict) else {}
    source_unavailable_rejection = event_type == "EvidenceRejected" and "UNAVAILABLE_EXTERNAL_SOURCE" in str(event.get("validation_status") or "")
    if bool(contract.get("source_hash_required")) and not _has_source_hash(input_hashes) and not source_unavailable_rejection:
        return _fail("SOURCE_HASH_MISSING", "external-source producer must include raw source hash", event, contract)
    if bool(contract.get("requires_external_source")) and not _has_source_hash(input_hashes) and not source_unavailable_rejection:
        return _fail("SOURCE_HASH_MISSING", "external-source producer must include source hash", event, contract)
    if bool(contract.get("requires_operator_intent")) and not _has_operator_intent(input_hashes):
        return _fail("OPERATOR_INTENT_MISSING", "manual producer must include operator intent metadata", event, contract)
    return _ok(event, contract)


def _artifact_paths_allowed(event: dict[str, Any], contract: dict[str, Any]) -> bool:
    paths = [str(path) for path in (event.get("artifact_paths") or []) if str(path)]
    if not paths:
        return True
    day_utc = str(event.get("day_utc") or "")
    patterns = [str(pattern).replace("{day_utc}", day_utc).replace("{day}", day_utc) for pattern in (contract.get("outputs") or [])]
    for path in paths:
        normalized = _relative_report_path(path)
        if not any(fnmatch.fnmatch(normalized, pattern) or fnmatch.fnmatch(path, pattern) for pattern in patterns):
            return False
    return True


def _relative_report_path(path: str) -> str:
    parts = Path(path).parts
    for marker in (
        "reports",
        "research_lab",
        "allocation_v1",
        "risk_definition_contract_v1",
        "cash_ledger_v1",
        "positions_v1",
        "position_lifecycle_v2",
        "accounting_v2",
        "economic_state_package_v1",
        "engine_activity_v1",
    ):
        try:
            index = parts.index(marker)
            return "/".join(parts[index:])
        except ValueError:
            continue
    return path


def _has_source_hash(input_hashes: dict[str, Any]) -> bool:
    return any("source" in str(key).lower() or "raw" in str(key).lower() for key in input_hashes)


def _has_operator_intent(input_hashes: dict[str, Any]) -> bool:
    return any("operator_intent" in str(key).lower() or "manual_intent" in str(key).lower() for key in input_hashes)


def _ok(event: dict[str, Any], contract: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "status": CONTRACT_PASS,
        "contract_valid": True,
        "producer": str(event.get("producer") or ""),
        "schema_id": str(event.get("schema_id") or ""),
        "producer_contract_ref": _contract_ref(contract),
        "reason": "",
    }


def _fail(status: str, reason: str, event: dict[str, Any], contract: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "status": status,
        "contract_valid": False,
        "producer": str(event.get("producer") or ""),
        "schema_id": str(event.get("schema_id") or ""),
        "producer_contract_ref": _contract_ref(contract),
        "reason": reason,
    }


def _contract_ref(contract: dict[str, Any] | None) -> dict[str, Any]:
    if not contract:
        return {}
    return {
        "producer_id": str(contract.get("producer_id") or ""),
        "producer_version": str(contract.get("producer_version") or ""),
        "output_schema_id": str(contract.get("output_schema_id") or ""),
        "repair_class": str(contract.get("repair_class") or ""),
        "determinism_class": str(contract.get("determinism_class") or ""),
        "authority_level": str(contract.get("authority_level") or ""),
    }
