from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "aegis_evidence_event.v1"
VALID_ENVIRONMENTS = {"PAPER", "LIVE", "UNKNOWN"}
VALID_STATUSES = {"OK", "WARN", "DEGRADED", "BLOCKED", "FORBIDDEN", "UNKNOWN", "MISSING", "STALE"}
VALID_SEVERITIES = {"INFO", "WARN", "ERROR", "CRITICAL"}


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def payload_hash(payload: dict[str, Any]) -> str:
    return sha256_text(canonical_json(payload))


def event_id_for(*, producer: str, event_type: str, target_day: str, observed_at_utc: str, payload_hash_value: str) -> str:
    return sha256_text("|".join([producer, event_type, target_day, observed_at_utc, payload_hash_value]))


def build_event(
    *,
    event_type: str,
    producer: str,
    target_day: str,
    environment: str = "UNKNOWN",
    status: str,
    blocker: str | None,
    owner: str,
    severity: str,
    payload: dict[str, Any] | None = None,
    next_action: str,
    evidence_path: str | Path | None = None,
    observed_at_utc: str | None = None,
) -> dict[str, Any]:
    payload_obj = dict(payload or {})
    observed = observed_at_utc or utc_now_iso()
    env = environment if environment in VALID_ENVIRONMENTS else "UNKNOWN"
    ph = payload_hash(payload_obj)
    event = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id_for(
            producer=producer,
            event_type=event_type,
            target_day=target_day,
            observed_at_utc=observed,
            payload_hash_value=ph,
        ),
        "event_type": event_type,
        "producer": producer,
        "target_day": target_day,
        "environment": env,
        "observed_at_utc": observed,
        "status": status,
        "blocker": blocker,
        "owner": owner,
        "severity": severity,
        "evidence_path": str(evidence_path) if evidence_path else None,
        "payload_hash": ph,
        "payload": payload_obj,
        "next_action": next_action,
    }
    validate_event(event)
    return event


def validate_event(event: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "event_id",
        "event_type",
        "producer",
        "target_day",
        "environment",
        "observed_at_utc",
        "status",
        "blocker",
        "owner",
        "severity",
        "evidence_path",
        "payload_hash",
        "payload",
        "next_action",
    }
    missing = sorted(required - set(event))
    if missing:
        raise ValueError(f"missing event fields: {missing}")
    if event["schema_version"] != SCHEMA_VERSION:
        raise ValueError("invalid schema_version")
    for key in ("event_id", "event_type", "producer", "target_day", "observed_at_utc", "owner", "payload_hash", "next_action"):
        if not isinstance(event[key], str) or not event[key].strip():
            raise ValueError(f"{key} must be a non-empty string")
    if event["environment"] not in VALID_ENVIRONMENTS:
        raise ValueError("invalid environment")
    if event["status"] not in VALID_STATUSES:
        raise ValueError("invalid status")
    if event["severity"] not in VALID_SEVERITIES:
        raise ValueError("invalid severity")
    if event["blocker"] is not None and not isinstance(event["blocker"], str):
        raise ValueError("blocker must be string|null")
    if event["evidence_path"] is not None and not isinstance(event["evidence_path"], str):
        raise ValueError("evidence_path must be string|null")
    if not isinstance(event["payload"], dict):
        raise ValueError("payload must be object")
    expected_hash = payload_hash(event["payload"])
    if expected_hash != event["payload_hash"]:
        raise ValueError("payload_hash mismatch")
    expected_id = event_id_for(
        producer=event["producer"],
        event_type=event["event_type"],
        target_day=event["target_day"],
        observed_at_utc=event["observed_at_utc"],
        payload_hash_value=event["payload_hash"],
    )
    if expected_id != event["event_id"]:
        raise ValueError("event_id mismatch")
