from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.evidence_event_store_v1 import append_evidence_event_v1
from ops.aegis.producer_contracts_v1 import critical_output_schema_ids_v1, find_contract_for_schema_v1
from ops.aegis.runtime_evaluation_v1 import stable_hash_v1


EVENT_BRIDGE_VERSION = "aegis_producer_event_bridge.v1"
SKIP_PARTS = {
    "aegis_evidence_events_v1",
    "aegis_decision_ledger_v1",
    "aegis_audit_bundle_v1",
}
TIMESTAMP_FIELDS = (
    "generated_at_utc",
    "created_at_utc",
    "updated_at_utc",
    "timestamp_utc",
    "produced_at_utc",
    "as_of_utc",
    "generated_at",
    "created_at",
    "updated_at",
)
REJECTING_STATUSES = {
    "BLOCKED",
    "ERROR",
    "FAIL",
    "FAILED",
    "INVALID",
    "MISSING",
    "REJECTED",
    "STALE",
    "TAMPERED",
}


def emit_evidence_events_for_artifact_v1(
    *,
    artifact_path: Path,
    payload: dict[str, Any],
    producer: str,
    producer_version: str,
    run_id: str = "",
    parent_run_id: str = "",
    git_sha: str = "",
) -> list[dict[str, Any]]:
    resolved = Path(artifact_path).expanduser().resolve()
    if not _should_emit_for_path(resolved):
        return []
    truth_root = _truth_root_from_report_path(resolved)
    day_utc = _day_utc_from_payload_or_path(payload, resolved)
    if not truth_root or not day_utc:
        return []

    schema_id = _schema_id_from_payload_or_path(payload, resolved)
    artifact_hash = sha256_file_v1(resolved)
    contract = find_contract_for_schema_v1(schema_id)
    actual_producer = str(contract.get("producer_id") or producer) if contract and str(contract.get("output_schema_id") or "") == schema_id else producer
    input_hashes = _input_hashes_from_payload(payload)
    created_at_utc = _timestamp_from_payload(payload) or _now_utc_v1()
    base = {
        "run_id": run_id or f"producer:{schema_id}:{day_utc}",
        "parent_run_id": parent_run_id,
        "day_utc": day_utc,
        "created_at_utc": created_at_utc,
        "producer": actual_producer,
        "producer_version": str((contract or {}).get("producer_version") or producer_version),
        "git_sha": git_sha or os.environ.get("AEGIS_PRODUCER_GIT_SHA", "UNKNOWN"),
        "schema_id": schema_id,
        "schema_version": str(payload.get("schema_version") or "v1"),
        "input_hashes": input_hashes,
        "output_hashes": {str(resolved): artifact_hash},
        "artifact_paths": [str(resolved)],
    }
    emitted = [
        append_evidence_event_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            event={
                **base,
                "event_id": _event_id("EvidenceProduced", base, artifact_hash),
                "event_type": "EvidenceProduced",
                "validation_status": "PRODUCED",
                "previous_event_hash": "",
                "event_hash": "",
            },
        )
    ]
    validation = _validation_status_for_payload(payload=payload, path=resolved, day_utc=day_utc)
    event_type = "EvidenceValidated" if validation == "VALID" else "EvidenceRejected"
    emitted.append(
        append_evidence_event_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            event={
                **base,
                "event_id": _event_id(event_type, base, artifact_hash + ":" + validation),
                "event_type": event_type,
                "validation_status": validation,
                "previous_event_hash": "",
                "event_hash": "",
            },
        )
    )
    return emitted


def emit_evidence_events_for_status_rows_v1(
    *,
    truth_root: Path,
    day_utc: str,
    artifact_statuses: list[dict[str, Any]],
    generated_at_utc: str,
    producer: str,
    producer_version: str,
    git_sha: str = "",
    run_id: str = "",
    parent_run_id: str = "",
) -> list[dict[str, Any]]:
    emitted: list[dict[str, Any]] = []
    root = Path(truth_root).expanduser().resolve()
    critical_schema_ids = critical_output_schema_ids_v1()
    for row in sorted(artifact_statuses, key=lambda item: str(item.get("artifact_id") or "")):
        artifact_id = str(row.get("artifact_id") or "")
        if artifact_id in critical_schema_ids:
            continue
        path_text = str(row.get("path") or "")
        if not path_text:
            continue
        path = Path(path_text).expanduser().resolve()
        artifact_hash = sha256_file_v1(path) if path.exists() else ""
        artifact_id = str(row.get("artifact_id") or _schema_id_from_payload_or_path({}, path))
        contract = find_contract_for_schema_v1(artifact_id)
        actual_producer = str(contract.get("producer_id") or producer) if contract and str(contract.get("output_schema_id") or "") == artifact_id else producer
        status = str(row.get("status") or "").upper()
        validation = "VALID" if status == "OK" else "INVALID"
        event_type = "EvidenceValidated" if validation == "VALID" else "EvidenceRejected"
        base = {
            "event_id": _event_id(event_type, {"schema_id": artifact_id, "day_utc": day_utc, "path": str(path), "status": status}, artifact_hash),
            "event_type": event_type,
            "run_id": run_id or f"runtime-kernel-scan:{day_utc}",
            "parent_run_id": parent_run_id,
            "day_utc": day_utc,
            "created_at_utc": str(row.get("artifact_timestamp") or generated_at_utc),
            "producer": actual_producer,
            "producer_version": str((contract or {}).get("producer_version") or producer_version),
            "git_sha": git_sha or os.environ.get("AEGIS_PRODUCER_GIT_SHA", "UNKNOWN"),
            "schema_id": artifact_id,
            "schema_version": "v1",
            "input_hashes": {},
            "output_hashes": {str(path): artifact_hash} if artifact_hash else {},
            "artifact_paths": [str(path)],
            "validation_status": validation,
            "previous_event_hash": "",
            "event_hash": "",
        }
        emitted.append(append_evidence_event_v1(truth_root=root, day_utc=day_utc, event=base))
    return emitted


def sha256_file_v1(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _should_emit_for_path(path: Path) -> bool:
    parts = set(path.parts)
    return "reports" in parts and not bool(parts & SKIP_PARTS) and path.suffix == ".json"


def _truth_root_from_report_path(path: Path) -> Path | None:
    parts = path.parts
    try:
        index = parts.index("reports")
    except ValueError:
        return None
    if index <= 0:
        return None
    return Path(*parts[:index])


def _day_utc_from_payload_or_path(payload: dict[str, Any], path: Path) -> str:
    for key in ("day_utc", "trade_date", "date"):
        value = str(payload.get(key) or "")
        if _is_day(value):
            return value
    parts = path.parts
    for part in reversed(parts):
        if _is_day(part):
            return part
    return ""


def _schema_id_from_payload_or_path(payload: dict[str, Any], path: Path) -> str:
    schema_id = str(payload.get("artifact_id") or payload.get("schema_id") or "")
    if schema_id:
        return schema_id
    parts = path.parts
    try:
        index = parts.index("reports")
        return str(parts[index + 1])
    except (ValueError, IndexError):
        return path.stem


def _validation_status_for_payload(*, payload: dict[str, Any], path: Path, day_utc: str) -> str:
    if not isinstance(payload, dict):
        return "INVALID"
    payload_day = _day_utc_from_payload_or_path(payload, path)
    if payload_day and payload_day != day_utc:
        return "INVALID"
    if not str(payload.get("schema_id") or payload.get("artifact_id") or ""):
        return "INVALID"
    if not _timestamp_from_payload(payload):
        return "INVALID"
    status = str(payload.get("status") or payload.get("validation_status") or payload.get("result") or "").upper()
    if status in REJECTING_STATUSES:
        return "INVALID"
    return "VALID"


def _input_hashes_from_payload(payload: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key in ("source_hash", "raw_source_hash", "source_data_hash", "input_hash", "evidence_hash", "operator_intent_hash", "manual_intent_hash"):
        value = str(payload.get(key) or "")
        if value:
            out[key] = value
    for key in ("source_hashes", "input_hashes", "raw_source_hashes"):
        value = payload.get(key)
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if str(sub_value or ""):
                    out[f"{key}.{sub_key}"] = str(sub_value)
    return out



def _timestamp_from_payload(payload: dict[str, Any]) -> str:
    for field in TIMESTAMP_FIELDS:
        value = str(payload.get(field) or "")
        if value:
            return value
    return ""


def _event_id(event_type: str, base: dict[str, Any], salt: str) -> str:
    return event_type + ":" + stable_hash_v1({**base, "salt": salt})[:32]


def _is_day(value: str) -> bool:
    if len(value) != 10:
        return False
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return value[4] == "-" and value[7] == "-"


def _now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
