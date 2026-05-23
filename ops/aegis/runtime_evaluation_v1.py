from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any


RUNTIME_EVALUATION_SCHEMA_VERSION = "v1"
RUNTIME_EVALUATOR_VERSION = "aegis_runtime_evaluator.v1"
RUNTIME_EVALUATION_SCHEMA_ID = "aegis_runtime_evaluation"

RUNTIME_EVALUATION_REQUIRED_FIELDS = (
    "run_id",
    "parent_run_id",
    "day_utc",
    "generated_at_utc",
    "git_sha",
    "evaluator_version",
    "policy_version",
    "dag_version",
    "schema_version",
    "runtime_truth_classification",
    "highest_readiness_layer",
    "capabilities",
    "blockers",
    "decision_trace",
    "source_evidence_refs",
    "root_blockers",
    "blocker_state",
    "repairability",
    "producer_contract_ref",
    "next_safe_action",
    "producer_contract_registry_version",
    "producer_contract_registry_hash",
    "warnings",
    "errors",
    "deterministic_output_hash",
)


def stable_json_bytes_v1(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(stable_json_bytes_v1(payload)).hexdigest()


def runtime_evaluation_hash_v1(evaluation: dict[str, Any]) -> str:
    candidate = deepcopy(evaluation)
    candidate["deterministic_output_hash"] = ""
    return stable_hash_v1(candidate)


def finalize_runtime_evaluation_v1(evaluation: dict[str, Any]) -> dict[str, Any]:
    payload = deepcopy(evaluation)
    payload.setdefault("schema_id", RUNTIME_EVALUATION_SCHEMA_ID)
    payload.setdefault("schema_version", RUNTIME_EVALUATION_SCHEMA_VERSION)
    payload.setdefault("evaluator_version", RUNTIME_EVALUATOR_VERSION)
    payload.setdefault("capabilities", {})
    payload.setdefault("blockers", [])
    payload.setdefault("decision_trace", [])
    payload.setdefault("source_evidence_refs", [])
    payload.setdefault("root_blockers", [])
    payload.setdefault("blocker_state", [])
    payload.setdefault("repairability", {})
    payload.setdefault("producer_contract_ref", {})
    payload.setdefault("next_safe_action", {})
    payload.setdefault("producer_contract_registry_version", "")
    payload.setdefault("producer_contract_registry_hash", "")
    "root_blockers",
    "blocker_state",
    "repairability",
    "producer_contract_ref",
    "next_safe_action",
    "producer_contract_registry_version",
    "producer_contract_registry_hash",
    payload.setdefault("warnings", [])
    payload.setdefault("errors", [])
    payload["deterministic_output_hash"] = ""
    payload["deterministic_output_hash"] = runtime_evaluation_hash_v1(payload)
    validate_runtime_evaluation_v1(payload)
    return payload


def validate_runtime_evaluation_v1(evaluation: dict[str, Any]) -> None:
    if not isinstance(evaluation, dict):
        raise ValueError("RuntimeEvaluation must be a JSON object")
    missing = [field for field in RUNTIME_EVALUATION_REQUIRED_FIELDS if field not in evaluation]
    if missing:
        raise ValueError("RuntimeEvaluation missing fields: " + ",".join(missing))
    if evaluation.get("deterministic_output_hash") != runtime_evaluation_hash_v1(evaluation):
        raise ValueError("RuntimeEvaluation deterministic_output_hash mismatch")
    capabilities = evaluation.get("capabilities")
    if not isinstance(capabilities, dict):
        raise ValueError("RuntimeEvaluation capabilities must be an object")
    for name, row in capabilities.items():
        if not isinstance(row, dict):
            raise ValueError(f"Capability row must be object: {name}")
        if "allowed" not in row:
            raise ValueError(f"Capability missing allowed: {name}")
        if not row.get("allowed") and not str(row.get("reason") or "").strip():
            raise ValueError(f"Blocked capability missing reason: {name}")
    if bool(capabilities.get("TRADE_ADVICE_ALLOWED", {}).get("allowed", False)):
        raise ValueError("TRADE_ADVICE_ALLOWED cannot be enabled by RuntimeEvaluation v1")
    if bool(capabilities.get("AUTONOMOUS_EXECUTION_ALLOWED", {}).get("allowed", False)):
        raise ValueError("AUTONOMOUS_EXECUTION_ALLOWED cannot be enabled by RuntimeEvaluation v1")


def blocked_runtime_evaluation_v1(
    *,
    run_id: str,
    parent_run_id: str,
    day_utc: str,
    generated_at_utc: str,
    git_sha: str,
    policy_version: str,
    dag_version: str,
    reason: str,
    errors: list[str] | None = None,
) -> dict[str, Any]:
    reason = str(reason or "BLOCKED_UNKNOWN")
    capabilities = {
        "DATA_READY": _blocked_cap("DATA_READY", reason),
        "RESEARCH_READY": _blocked_cap("RESEARCH_READY", reason),
        "EVENT_READY": _blocked_cap("EVENT_READY", reason),
        "FEEDBACK_READY": _blocked_cap("FEEDBACK_READY", reason),
        "ALERT_GATE_PROVEN": _blocked_cap("ALERT_GATE_PROVEN", reason),
        "TRADE_ADVICE_ALLOWED": _blocked_cap("TRADE_ADVICE_ALLOWED", "Trade advice disabled and runtime blocked: " + reason),
        "MANUAL_TRADE_CAPTURE_ALLOWED": _blocked_cap("MANUAL_TRADE_CAPTURE_ALLOWED", "Manual capture blocked by RuntimeEvaluation: " + reason),
        "AUTONOMOUS_EXECUTION_ALLOWED": _blocked_cap("AUTONOMOUS_EXECUTION_ALLOWED", "Autonomous execution disabled by design."),
    }
    return finalize_runtime_evaluation_v1(
        {
            "run_id": run_id,
            "parent_run_id": parent_run_id,
            "day_utc": day_utc,
            "generated_at_utc": generated_at_utc,
            "git_sha": git_sha,
            "evaluator_version": RUNTIME_EVALUATOR_VERSION,
            "policy_version": policy_version,
            "dag_version": dag_version,
            "schema_version": RUNTIME_EVALUATION_SCHEMA_VERSION,
            "runtime_truth_classification": "PARTIAL_CONTEXT",
            "highest_readiness_layer": "BLOCKED",
            "capabilities": capabilities,
            "blockers": [{"blocker_id": "BLOCKED_UNKNOWN", "reason": reason}],
            "decision_trace": [{"capability": name, "allowed": row["allowed"], "reason": row["reason"]} for name, row in sorted(capabilities.items())],
            "source_evidence_refs": [],
            "warnings": [],
            "errors": list(errors or [reason]),
            "deterministic_output_hash": "",
        }
    )


def _blocked_cap(capability: str, reason: str) -> dict[str, Any]:
    return {
        "capability": capability,
        "allowed": False,
        "direct_dependencies": [],
        "evidence_refs_used": [],
        "validation_results": [],
        "blocker_chain": [reason],
        "reason": reason,
    }
