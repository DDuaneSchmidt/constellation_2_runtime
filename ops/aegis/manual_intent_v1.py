from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.evidence_event_store_v1 import append_evidence_event_v1, read_evidence_events_v1
from ops.aegis.runtime_evaluation_v1 import stable_hash_v1, stable_json_bytes_v1


ALLOWED_MANUAL_INTENTS = {
    "NONE_DECLARED",
    "REVIEW_COMPLETED",
    "REPORT_ACKNOWLEDGED",
    "MANUAL_ACTION_REQUIRED",
    "MANUAL_ACTION_COMPLETED",
}


def manual_intent_path_v1(*, truth_root: Path, day_utc: str, intent_id: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in intent_id) or "manual_intent"
    return Path(truth_root).expanduser().resolve() / "reports" / "manual_intent_v1" / day_utc / safe / "manual_intent.v1.json"


def build_manual_intent_v1(
    *,
    operator_id: str,
    operator_intent: str,
    intent_timestamp_utc: str,
    day_utc: str,
    reason: str,
    runtime_evaluation_hash: str,
) -> dict[str, Any]:
    payload = {
        "schema_id": "manual_intent",
        "schema_version": "v1",
        "artifact_id": "manual_intent_v1",
        "intent_id": "manual_intent:" + stable_hash_v1(
            {
                "operator_id": operator_id,
                "operator_intent": operator_intent,
                "intent_timestamp_utc": intent_timestamp_utc,
                "day_utc": day_utc,
                "runtime_evaluation_hash": runtime_evaluation_hash,
            }
        )[:24],
        "operator_id": str(operator_id or "").strip(),
        "operator_intent": str(operator_intent or "").strip().upper(),
        "intent_timestamp_utc": str(intent_timestamp_utc or "").strip(),
        "day_utc": str(day_utc or "").strip(),
        "reason": str(reason or "").strip(),
        "runtime_evaluation_hash": str(runtime_evaluation_hash or "").strip(),
        "acknowledgment_hash": "",
        "manual_evidence_placeholder": False,
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    payload["acknowledgment_hash"] = stable_hash_v1({**payload, "acknowledgment_hash": ""})
    return payload


def validate_manual_intent_v1(payload: dict[str, Any], *, day_utc: str, runtime_evaluation_hash: str, generated_at_utc: str | None = None) -> list[str]:
    reasons: list[str] = []
    if not isinstance(payload, dict):
        return ["MANUAL_INTENT_NOT_OBJECT"]
    if payload.get("schema_id") != "manual_intent":
        reasons.append("MANUAL_INTENT_SCHEMA_MISMATCH")
    if str(payload.get("operator_id") or "").strip() == "":
        reasons.append("OPERATOR_ID_MISSING")
    if str(payload.get("operator_intent") or "").strip().upper() not in ALLOWED_MANUAL_INTENTS:
        reasons.append("OPERATOR_INTENT_NOT_ALLOWED")
    if str(payload.get("day_utc") or "") != day_utc:
        reasons.append("MANUAL_INTENT_WRONG_DAY")
    if str(payload.get("runtime_evaluation_hash") or "") != runtime_evaluation_hash:
        reasons.append("RUNTIME_EVALUATION_HASH_MISMATCH")
    if bool(payload.get("manual_evidence_placeholder")):
        reasons.append("PLACEHOLDER_MANUAL_EVIDENCE_FORBIDDEN")
    if not str(payload.get("acknowledgment_hash") or ""):
        reasons.append("ACKNOWLEDGMENT_HASH_MISSING")
    timestamp = _parse_utc(str(payload.get("intent_timestamp_utc") or ""))
    if timestamp is None:
        reasons.append("INTENT_TIMESTAMP_MALFORMED")
    elif generated_at_utc:
        generated = _parse_utc(generated_at_utc)
        if generated and timestamp > generated:
            reasons.append("INTENT_TIMESTAMP_FUTURE")
        if generated and (generated - timestamp).total_seconds() > 7 * 86400:
            reasons.append("INTENT_TIMESTAMP_STALE")
    return sorted(set(reasons))


def write_manual_intent_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    path = manual_intent_path_v1(truth_root=truth_root, day_utc=str(payload["day_utc"]), intent_id=str(payload["intent_id"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(stable_json_bytes_v1(payload) + b"\n")
    return path


def emit_manual_action_event_v1(
    *,
    truth_root: Path,
    payload: dict[str, Any],
    runtime_evaluation_hash: str,
    generated_at_utc: str,
    git_sha: str = "UNKNOWN",
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day_utc = str(payload.get("day_utc") or "")
    reasons = validate_manual_intent_v1(payload, day_utc=day_utc, runtime_evaluation_hash=runtime_evaluation_hash, generated_at_utc=generated_at_utc)
    event_type = "ManualActionBlocked" if reasons else "ManualActionAccepted"
    path = manual_intent_path_v1(truth_root=root, day_utc=day_utc, intent_id=str(payload.get("intent_id") or "manual_intent"))
    artifact_hash = _sha256(path) if path.exists() else stable_hash_v1(payload)
    return append_evidence_event_v1(
        truth_root=root,
        day_utc=day_utc,
        event={
            "event_id": event_type + ":" + stable_hash_v1({"payload": payload, "reasons": reasons})[:32],
            "event_type": event_type,
            "run_id": str(payload.get("intent_id") or "manual-intent"),
            "parent_run_id": "",
            "day_utc": day_utc,
            "created_at_utc": generated_at_utc,
            "producer": "ops.aegis.manual_intent_v1",
            "producer_version": "v1",
            "git_sha": git_sha,
            "schema_id": "manual_intent",
            "schema_version": "v1",
            "input_hashes": {"runtime_evaluation_hash": runtime_evaluation_hash, "operator_intent_hash": str(payload.get("acknowledgment_hash") or "")},
            "output_hashes": {str(path): artifact_hash},
            "artifact_paths": [str(path)],
            "validation_status": "BLOCKED:" + ",".join(reasons) if reasons else "ACCEPTED",
            "manual_intent_validation_reasons": reasons,
            "previous_event_hash": "",
            "event_hash": "",
        },
    )


def manual_intent_report_v1(*, truth_root: Path, day_utc: str, runtime_evaluation_hash: str, generated_at_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    base = root / "reports" / "manual_intent_v1" / day_utc
    intents = []
    if base.exists():
        for path in sorted(base.rglob("manual_intent.v1.json")):
            try:
                import json
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                intents.append({"path": str(path), "status": "MALFORMED", "reasons": [type(exc).__name__]})
                continue
            reasons = validate_manual_intent_v1(payload, day_utc=day_utc, runtime_evaluation_hash=runtime_evaluation_hash, generated_at_utc=generated_at_utc)
            intents.append({"path": str(path), "intent_id": payload.get("intent_id"), "operator_intent": payload.get("operator_intent"), "status": "BLOCKED" if reasons else "ACCEPTED", "reasons": reasons, "acknowledgment_hash": payload.get("acknowledgment_hash")})
    events = [row for row in read_evidence_events_v1(truth_root=root, day_utc=day_utc) if str(row.get("event_type") or "") in {"ManualActionAccepted", "ManualActionBlocked"}]
    return {"schema_id": "aegis_manual_intent_report", "schema_version": "v1", "day_utc": day_utc, "runtime_evaluation_hash": runtime_evaluation_hash, "manual_intents": intents, "manual_action_events": events}


def _parse_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
