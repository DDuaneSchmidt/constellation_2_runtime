from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from ops.aegis.evidence_event_store_v1 import append_evidence_event_v1, read_evidence_events_v1
from ops.aegis.evidence_event_v1 import finalize_evidence_event_v1, validate_evidence_event_v1
from ops.aegis.producer_contract_validator_v1 import validate_evidence_event_contract_v1
from ops.aegis.producer_contracts_v1 import find_contract_for_schema_v1, load_producer_contract_registry_v1
from ops.aegis.runtime_evaluation_v1 import stable_hash_v1


QUARANTINE_FAMILY = "aegis_quarantined_events_v1"
EVENTS_FILENAME = "events.jsonl"


def quarantine_events_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "events" / QUARANTINE_FAMILY / day_utc / EVENTS_FILENAME


def append_event_transaction_v1(
    *,
    truth_root: Path,
    day_utc: str,
    event: dict[str, Any],
    execution_context: str = "",
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    reasons = _event_rejection_reasons(root=root, day_utc=day_utc, event=event, execution_context=execution_context)
    if reasons:
        finalized = _append_quarantined_event_v1(root=root, day_utc=str(event.get("day_utc") or day_utc), event=event, reasons=reasons)
        return {"ok": False, "status": "QUARANTINED", "canonical_event_validation_status": "QUARANTINED_INVALID", "reasons": reasons, "event": finalized}
    canonical_event = {**event, "canonical_event_validation_status": "CANONICAL_VALID"}
    finalized = append_evidence_event_v1(truth_root=root, day_utc=day_utc, event=canonical_event)
    return {"ok": True, "status": "APPENDED", "canonical_event_validation_status": "CANONICAL_VALID", "reasons": [], "event": finalized}


def emit_artifact_evidence_transaction_v1(
    *,
    truth_root: Path,
    day_utc: str,
    artifact_path: Path,
    payload: dict[str, Any],
    producer_id: str,
    producer_version: str = "v1",
    run_id: str = "",
    parent_run_id: str = "",
    created_at_utc: str = "",
    git_sha: str = "",
    input_hashes: dict[str, Any] | None = None,
    validation_status: str = "VALID",
) -> list[dict[str, Any]]:
    path = Path(artifact_path).expanduser().resolve()
    schema_id = str(payload.get("schema_id") or payload.get("artifact_id") or _schema_from_path(path))
    schema_version = str(payload.get("schema_version") or "v1")
    created = created_at_utc or _timestamp_from_payload(payload)
    output_hash = sha256_file_v1(path) if path.exists() else ""
    base = {
        "run_id": run_id or f"{producer_id}:{schema_id}:{day_utc}",
        "parent_run_id": parent_run_id,
        "day_utc": day_utc,
        "created_at_utc": created,
        "producer": producer_id,
        "producer_version": producer_version,
        "git_sha": git_sha or os.environ.get("AEGIS_PRODUCER_GIT_SHA", "UNKNOWN"),
        "schema_id": schema_id,
        "schema_version": schema_version,
        "input_hashes": _string_map(input_hashes or {}),
        "output_hashes": {str(path): output_hash} if output_hash else {},
        "artifact_paths": [str(path)],
    }
    emitted = []
    for event_type, status, salt in (
        ("EvidenceProduced", "PRODUCED", output_hash),
        ("EvidenceValidated" if validation_status == "VALID" else "EvidenceRejected", validation_status, output_hash + ":" + validation_status),
    ):
        event = {
            **base,
            "event_id": _event_id(event_type, base, salt),
            "event_type": event_type,
            "validation_status": status,
            "previous_event_hash": "",
            "event_hash": "",
        }
        emitted.append(append_event_transaction_v1(truth_root=truth_root, day_utc=day_utc, event=event))
    return emitted


def contract_input_hashes_for_paths_v1(paths: list[Path | str], *, extra: dict[str, Any] | None = None) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for path_like in sorted({str(item) for item in paths if str(item)}):
        path = Path(path_like).expanduser().resolve()
        if path.exists() and path.is_file():
            hashes[f"input:{path}"] = sha256_file_v1(path)
    for key, value in sorted((extra or {}).items()):
        text = str(value or "")
        if text:
            hashes[str(key)] = text
    return hashes


def sha256_file_v1(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_payload_hash_v1(payload: Any) -> str:
    return stable_hash_v1(payload)


def _event_rejection_reasons(*, root: Path, day_utc: str, event: dict[str, Any], execution_context: str) -> list[str]:
    reasons: list[str] = []
    if str(event.get("day_utc") or "") != day_utc:
        reasons.append("WRONG_DAY")
    try:
        validate_evidence_event_v1(finalize_evidence_event_v1(event, previous_event_hash=""))
    except Exception as exc:
        reasons.append(f"MALFORMED:{type(exc).__name__}")
    try:
        existing = read_evidence_events_v1(truth_root=root, day_utc=day_utc)
    except Exception as exc:
        reasons.append(f"BROKEN_HASH_CHAIN:{type(exc).__name__}")
        existing = []
    event_id = str(event.get("event_id") or "")
    if event_id and any(str(row.get("event_id") or "") == event_id for row in existing):
        reasons.append("DUPLICATE_EVENT_ID")
    contract = validate_evidence_event_contract_v1(event, registry=load_producer_contract_registry_v1(), execution_context=execution_context)
    if not contract.get("contract_valid"):
        reasons.append(f"CONTRACT:{contract.get('status')}")
    return sorted(set(reasons))


def _append_quarantined_event_v1(*, root: Path, day_utc: str, event: dict[str, Any], reasons: list[str]) -> dict[str, Any]:
    path = quarantine_events_path_v1(truth_root=root, day_utc=day_utc)
    existing = _read_quarantine(path)
    previous_hash = existing[-1]["event_hash"] if existing else ""
    quarantined = {
        "event_id": str(event.get("event_id") or "quarantined:" + stable_hash_v1(event)[:24]),
        "event_type": str(event.get("event_type") or "EvidenceRejected"),
        "run_id": str(event.get("run_id") or "quarantine"),
        "parent_run_id": str(event.get("parent_run_id") or ""),
        "day_utc": str(event.get("day_utc") or day_utc),
        "created_at_utc": str(event.get("created_at_utc") or "1970-01-01T00:00:00Z"),
        "producer": str(event.get("producer") or "UNKNOWN"),
        "producer_version": str(event.get("producer_version") or "UNKNOWN"),
        "git_sha": str(event.get("git_sha") or "UNKNOWN"),
        "schema_id": str(event.get("schema_id") or "unknown"),
        "schema_version": str(event.get("schema_version") or "v1"),
        "input_hashes": event.get("input_hashes") if isinstance(event.get("input_hashes"), dict) else {},
        "output_hashes": event.get("output_hashes") if isinstance(event.get("output_hashes"), dict) else {},
        "artifact_paths": event.get("artifact_paths") if isinstance(event.get("artifact_paths"), list) else [],
        "previous_event_hash": "",
        "event_hash": "",
        "validation_status": "QUARANTINED:" + ",".join(reasons),
        "canonical_event_validation_status": "QUARANTINED_INVALID",
        "quarantine_reasons": reasons,
    }
    finalized = finalize_evidence_event_v1(quarantined, previous_event_hash=previous_hash)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, json.dumps(finalized, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
        os.fsync(fd)
    finally:
        os.close(fd)
    return finalized


def _read_quarantine(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _schema_from_path(path: Path) -> str:
    try:
        index = path.parts.index("reports")
        family = path.parts[index + 1]
        return family[:-3] if family.endswith("_v1") else family
    except (ValueError, IndexError):
        return path.stem


def _timestamp_from_payload(payload: dict[str, Any]) -> str:
    for key in ("generated_at_utc", "created_at_utc", "timestamp_utc", "updated_at_utc"):
        value = str(payload.get(key) or "")
        if value:
            return value
    return ""


def _string_map(values: dict[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in sorted(values.items()) if str(value)}


def _event_id(event_type: str, base: dict[str, Any], salt: str) -> str:
    return event_type + ":" + stable_hash_v1({**base, "salt": salt})[:32]


def read_quarantined_events_v1(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    return _read_quarantine(quarantine_events_path_v1(truth_root=truth_root, day_utc=day_utc))


def canonical_vs_quarantine_report_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    canonical = read_evidence_events_v1(truth_root=root, day_utc=day_utc)
    quarantine = read_quarantined_events_v1(truth_root=root, day_utc=day_utc)
    return {
        "schema_id": "aegis_canonical_vs_quarantine_events",
        "schema_version": "v1",
        "day_utc": day_utc,
        "canonical_event_count": len(canonical),
        "quarantined_event_count": len(quarantine),
        "canonical_event_type_counts": _event_type_counts(canonical),
        "quarantined_event_type_counts": _event_type_counts(quarantine),
        "quarantined_events": [
            {
                "event_id": row.get("event_id"),
                "event_type": row.get("event_type"),
                "producer": row.get("producer"),
                "schema_id": row.get("schema_id"),
                "quarantine_reasons": row.get("quarantine_reasons", []),
            }
            for row in quarantine
        ],
    }


def _event_type_counts(events: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for event in events:
        key = str(event.get("event_type") or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))
