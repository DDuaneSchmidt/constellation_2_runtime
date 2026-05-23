from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONTRACT_REGISTRY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "AEGIS_PRODUCER_CONTRACTS_V1.json"
ALLOWED_REPAIR_CLASSES = {"AUTO_SAFE", "AUTO_DETERMINISTIC", "EXTERNAL_DATA_BOUNDED", "MANUAL_REQUIRED", "FORBIDDEN"}
ALLOWED_DETERMINISM_CLASSES = {
    "PURE_DETERMINISTIC",
    "INPUT_HASH_DETERMINISTIC",
    "EXTERNAL_DATA_BOUNDED",
    "MANUAL_DECLARATION",
    "NON_DETERMINISTIC_FORBIDDEN",
}
REQUIRED_CONTRACT_FIELDS = (
    "producer_id",
    "producer_version",
    "command",
    "inputs",
    "outputs",
    "output_schema_id",
    "output_schema_version",
    "allowed_event_types",
    "repair_class",
    "determinism_class",
    "freshness_ttl_seconds",
    "requires_operator_intent",
    "requires_external_source",
    "source_hash_required",
    "authority_level",
    "allowed_days",
    "validation_command",
    "deprecated",
    "replacement_producer_id",
)


def load_producer_contract_registry_v1(path: Path | None = None) -> dict[str, Any]:
    registry_path = Path(path or DEFAULT_CONTRACT_REGISTRY_PATH).resolve()
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Producer contract registry must be a JSON object")
    validate_producer_contract_registry_v1(payload)
    payload["_registry_path"] = str(registry_path)
    return payload


def validate_producer_contract_registry_v1(registry: dict[str, Any]) -> None:
    if str(registry.get("schema_id") or "") != "aegis_producer_contracts":
        raise ValueError("Producer contract registry schema_id mismatch")
    contracts = registry.get("contracts")
    if not isinstance(contracts, list):
        raise ValueError("Producer contract registry contracts must be a list")
    seen: set[tuple[str, str]] = set()
    for contract in contracts:
        validate_producer_contract_v1(contract)
        key = (str(contract["producer_id"]), str(contract["output_schema_id"]))
        if key in seen:
            raise ValueError("Duplicate producer contract: " + ":".join(key))
        seen.add(key)


def validate_producer_contract_v1(contract: Any) -> None:
    if not isinstance(contract, dict):
        raise ValueError("Producer contract must be an object")
    missing = [field for field in REQUIRED_CONTRACT_FIELDS if field not in contract]
    if missing:
        raise ValueError("Producer contract missing fields: " + ",".join(missing))
    if str(contract.get("repair_class") or "") not in ALLOWED_REPAIR_CLASSES:
        raise ValueError("Unsupported repair_class for producer " + str(contract.get("producer_id")))
    if str(contract.get("determinism_class") or "") not in ALLOWED_DETERMINISM_CLASSES:
        raise ValueError("Unsupported determinism_class for producer " + str(contract.get("producer_id")))
    for list_field in ("inputs", "outputs", "allowed_event_types", "allowed_days"):
        if not isinstance(contract.get(list_field), list):
            raise ValueError(f"{list_field} must be a list for producer {contract.get('producer_id')}")
    if not str(contract.get("output_schema_id") or ""):
        raise ValueError("output_schema_id is required")


def contract_registry_version_v1(registry: dict[str, Any] | None = None) -> str:
    registry = registry or load_producer_contract_registry_v1()
    return str(registry.get("contract_registry_version") or registry.get("schema_version") or "v1")


def producer_contracts_v1(registry: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    registry = registry or load_producer_contract_registry_v1()
    return [dict(row) for row in (registry.get("contracts") or []) if isinstance(row, dict)]


def critical_output_schema_ids_v1(registry: dict[str, Any] | None = None) -> set[str]:
    registry = registry or load_producer_contract_registry_v1()
    return {str(item) for item in (registry.get("critical_output_schema_ids") or []) if str(item)}


def find_contract_for_event_v1(event: dict[str, Any], registry: dict[str, Any] | None = None) -> dict[str, Any] | None:
    producer = str(event.get("producer") or "")
    schema_id = str(event.get("schema_id") or "")
    matches = [
        contract
        for contract in producer_contracts_v1(registry)
        if str(contract.get("producer_id") or "") == producer
        and str(contract.get("output_schema_id") or "") in {schema_id, "*"}
    ]
    exact = [contract for contract in matches if str(contract.get("output_schema_id") or "") == schema_id]
    if exact:
        return exact[0]
    return matches[0] if matches else None


def find_contract_for_schema_v1(schema_id: str, registry: dict[str, Any] | None = None) -> dict[str, Any] | None:
    exact = [
        contract
        for contract in producer_contracts_v1(registry)
        if str(contract.get("output_schema_id") or "") == schema_id
    ]
    critical = [contract for contract in exact if str(contract.get("authority_level") or "") == "CRITICAL"]
    return (critical or exact or [None])[0]


def repairable_contracts_by_schema_v1(registry: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for contract in producer_contracts_v1(registry):
        schema_id = str(contract.get("output_schema_id") or "")
        if not schema_id or schema_id == "*":
            continue
        current = out.get(schema_id)
        if current is None or str(current.get("authority_level") or "") != "CRITICAL":
            out[schema_id] = contract
    return out
