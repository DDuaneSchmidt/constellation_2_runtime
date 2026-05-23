from __future__ import annotations

from copy import deepcopy
from typing import Any

from ops.aegis.runtime_evaluation_v1 import stable_hash_v1


EVIDENCE_EVENT_SCHEMA_VERSION = "v1"
EVIDENCE_EVENT_TYPES = {
    "EvidenceProduced",
    "EvidenceValidated",
    "EvidenceRejected",
    "EvidenceExpired",
    "ArtifactTampered",
    "CapabilityEvaluated",
    "RuntimeEvaluated",
    "RepairPlanned",
    "RepairAttempted",
    "RepairSucceeded",
    "RepairExecuted",
    "RepairCompletedWithRejectedEvidence",
    "RepairFailed",
    "ManualActionBlocked",
    "ManualActionAccepted",
}
EVIDENCE_EVENT_REQUIRED_FIELDS = (
    "event_id",
    "event_type",
    "run_id",
    "parent_run_id",
    "day_utc",
    "created_at_utc",
    "producer",
    "producer_version",
    "git_sha",
    "schema_id",
    "schema_version",
    "input_hashes",
    "output_hashes",
    "artifact_paths",
    "validation_status",
    "previous_event_hash",
    "event_hash",
)


def evidence_event_hash_v1(event: dict[str, Any]) -> str:
    candidate = deepcopy(event)
    candidate["event_hash"] = ""
    return stable_hash_v1(candidate)


def finalize_evidence_event_v1(event: dict[str, Any], *, previous_event_hash: str) -> dict[str, Any]:
    payload = deepcopy(event)
    payload["previous_event_hash"] = str(previous_event_hash or "")
    payload.setdefault("schema_version", EVIDENCE_EVENT_SCHEMA_VERSION)
    payload.setdefault("input_hashes", {})
    payload.setdefault("output_hashes", {})
    payload.setdefault("artifact_paths", [])
    payload.setdefault("validation_status", "UNKNOWN")
    payload["event_hash"] = ""
    payload["event_hash"] = evidence_event_hash_v1(payload)
    validate_evidence_event_v1(payload)
    return payload


def validate_evidence_event_v1(event: dict[str, Any]) -> None:
    if not isinstance(event, dict):
        raise ValueError("Evidence event must be a JSON object")
    missing = [field for field in EVIDENCE_EVENT_REQUIRED_FIELDS if field not in event]
    if missing:
        raise ValueError("Evidence event missing fields: " + ",".join(missing))
    if str(event.get("event_type") or "") not in EVIDENCE_EVENT_TYPES:
        raise ValueError("Unsupported evidence event type: " + str(event.get("event_type") or ""))
    if not isinstance(event.get("input_hashes"), dict):
        raise ValueError("input_hashes must be an object")
    if not isinstance(event.get("output_hashes"), dict):
        raise ValueError("output_hashes must be an object")
    if not isinstance(event.get("artifact_paths"), list):
        raise ValueError("artifact_paths must be a list")
    if event.get("event_hash") != evidence_event_hash_v1(event):
        raise ValueError("Evidence event_hash mismatch")
