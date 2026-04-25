from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import (
    canonical_hash_for_c2_artifact_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    validate_against_repo_schema_v1,
)


ATTEMPT_FILENAME = "broker_submit_attempt_v1.json"
ACK_FILENAME = "broker_acknowledgement_v1.json"
OUTCOME_FILENAME = "broker_order_outcome_v1.json"

ATTEMPT_SCHEMA_RELPATH = "constellation_2/schemas/broker_submit_attempt.v1.schema.json"
ACK_SCHEMA_RELPATH = "constellation_2/schemas/broker_acknowledgement.v1.schema.json"
OUTCOME_SCHEMA_RELPATH = "constellation_2/schemas/broker_order_outcome.v1.schema.json"

OUTCOME_STATE_ORDER: Dict[str, int] = {
    "NOT_ATTEMPTED": 0,
    "SUBMIT_ATTEMPTED": 1,
    "BROKER_ID_ASSIGNED": 2,
    "BROKER_ACCEPTED": 3,
    "UNKNOWN_PENDING": 3,
    "BROKER_REJECTED": 4,
    "BROKER_CANCELLED": 4,
    "PARTIALLY_FILLED": 5,
    "FILLED": 6,
}


class BrokerReconciliationArtifactError(Exception):
    pass


def _normalize_utc_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise BrokerReconciliationArtifactError("UTC_TIMESTAMP_MISSING")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception as exc:  # noqa: BLE001
        raise BrokerReconciliationArtifactError(f"UTC_TIMESTAMP_INVALID:{text}") from exc
    return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_reason_codes(reason_codes: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in reason_codes:
        code = str(item or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _normalize_evidence_paths(paths: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in paths:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(prefix=f".{path.name}.tmp.", dir=str(path.parent), delete=False) as tmp:
        tmp.write(payload)
        tmp.flush()
        import os

        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _state_rank(state: str) -> int:
    return OUTCOME_STATE_ORDER.get(str(state or "").strip().upper(), -1)


def _write_artifact(
    *,
    repo_root: Path,
    path: Path,
    payload: Dict[str, Any],
    schema_relpath: str,
    allow_monotonic_upgrade: bool = False,
) -> str:
    validate_against_repo_schema_v1(payload, repo_root, schema_relpath)
    canonical_payload = {**payload, "canonical_json_hash": None}
    canonical_payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(canonical_payload)
    validate_against_repo_schema_v1(canonical_payload, repo_root, schema_relpath)
    new_bytes = canonical_json_bytes_v1(canonical_payload) + b"\n"

    if path.exists() and path.is_file():
        existing_obj = json.loads(path.read_text(encoding="utf-8"))
        existing_bytes = canonical_json_bytes_v1(existing_obj) + b"\n"
        if existing_bytes == new_bytes:
            return "SKIP_IDENTICAL"
        if not allow_monotonic_upgrade:
            raise BrokerReconciliationArtifactError(f"REFUSE_OVERWRITE_EXISTING:{path}")

        if not isinstance(existing_obj, dict):
            raise BrokerReconciliationArtifactError(f"EXISTING_ARTIFACT_MALFORMED:{path}")
        if str(existing_obj.get("submission_id") or "").strip() != str(canonical_payload.get("submission_id") or "").strip():
            raise BrokerReconciliationArtifactError(f"SUBMISSION_ID_MISMATCH:{path}")
        if str(existing_obj.get("day_utc") or "").strip() != str(canonical_payload.get("day_utc") or "").strip():
            raise BrokerReconciliationArtifactError(f"DAY_UTC_MISMATCH:{path}")
        if str(existing_obj.get("environment") or "").strip().upper() != str(canonical_payload.get("environment") or "").strip().upper():
            raise BrokerReconciliationArtifactError(f"ENVIRONMENT_MISMATCH:{path}")

        old_rank = _state_rank(str(existing_obj.get("outcome_state") or ""))
        new_rank = _state_rank(str(canonical_payload.get("outcome_state") or ""))
        if new_rank < old_rank:
            raise BrokerReconciliationArtifactError(
                f"OUTCOME_STATE_REGRESSION:{path}:old={existing_obj.get('outcome_state')}:new={canonical_payload.get('outcome_state')}"
            )
        _atomic_write_bytes(path, new_bytes)
        return "UPDATED"

    _atomic_write_bytes(path, new_bytes)
    return "WROTE"


def build_broker_submit_attempt_v1(
    *,
    day_utc: str,
    environment: str,
    submission_id: str,
    attempted_at_utc: str,
    ib_account: str | None,
    dry_run: bool,
    reason_codes: Iterable[str] = (),
    evidence_artifacts: Iterable[str] = (),
) -> Dict[str, Any]:
    return {
        "schema_id": "broker_submit_attempt",
        "schema_version": "v1",
        "day_utc": str(day_utc or "").strip(),
        "environment": str(environment or "").strip().upper(),
        "submission_id": str(submission_id or "").strip(),
        "attempted_at_utc": _normalize_utc_text(attempted_at_utc),
        "attempt_state": "SUBMIT_ATTEMPTED",
        "ib_account": str(ib_account or "").strip() or None,
        "dry_run": bool(dry_run),
        "reason_codes": _normalize_reason_codes(reason_codes),
        "evidence_artifacts": _normalize_evidence_paths(evidence_artifacts),
        "canonical_json_hash": None,
    }


def build_broker_acknowledgement_v1(
    *,
    day_utc: str,
    environment: str,
    submission_id: str,
    acknowledged_at_utc: str,
    order_id: int | None,
    perm_id: int | None,
    status: str,
    ib_account: str | None,
    reason_codes: Iterable[str] = (),
    evidence_artifacts: Iterable[str] = (),
) -> Dict[str, Any]:
    perm_value = int(perm_id) if isinstance(perm_id, int) else None
    order_value = int(order_id) if isinstance(order_id, int) else None
    identity_strength = "STRONG"
    if order_value is None or perm_value is None or perm_value <= 0:
        identity_strength = "WEAK"
    return {
        "schema_id": "broker_acknowledgement",
        "schema_version": "v1",
        "day_utc": str(day_utc or "").strip(),
        "environment": str(environment or "").strip().upper(),
        "submission_id": str(submission_id or "").strip(),
        "acknowledged_at_utc": _normalize_utc_text(acknowledged_at_utc),
        "ack_state": "BROKER_ID_ASSIGNED",
        "ib_account": str(ib_account or "").strip() or None,
        "status": str(status or "").strip().upper() or "UNKNOWN",
        "broker_ids": {"order_id": order_value, "perm_id": perm_value},
        "identity_strength": identity_strength,
        "reason_codes": _normalize_reason_codes(reason_codes),
        "evidence_artifacts": _normalize_evidence_paths(evidence_artifacts),
        "canonical_json_hash": None,
    }


def build_broker_order_outcome_v1(
    *,
    day_utc: str,
    environment: str,
    submission_id: str,
    evaluated_at_utc: str,
    outcome_state: str,
    status: str | None,
    order_id: int | None,
    perm_id: int | None,
    ib_account: str | None,
    reason_codes: Iterable[str] = (),
    evidence_artifacts: Iterable[str] = (),
    error: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    normalized_state = str(outcome_state or "").strip().upper()
    if normalized_state not in OUTCOME_STATE_ORDER:
        raise BrokerReconciliationArtifactError(f"OUTCOME_STATE_INVALID:{normalized_state}")
    normalized_error = None
    if isinstance(error, dict) and str(error.get("code") or "").strip():
        normalized_error = {
            "code": str(error.get("code")).strip(),
            "message": str(error.get("message") or "").strip() or "Broker outcome error",
        }
    return {
        "schema_id": "broker_order_outcome",
        "schema_version": "v1",
        "day_utc": str(day_utc or "").strip(),
        "environment": str(environment or "").strip().upper(),
        "submission_id": str(submission_id or "").strip(),
        "evaluated_at_utc": _normalize_utc_text(evaluated_at_utc),
        "outcome_state": normalized_state,
        "status": str(status or "").strip().upper() or None,
        "ib_account": str(ib_account or "").strip() or None,
        "broker_ids": {
            "order_id": int(order_id) if isinstance(order_id, int) else None,
            "perm_id": int(perm_id) if isinstance(perm_id, int) else None,
        },
        "reason_codes": _normalize_reason_codes(reason_codes),
        "error": normalized_error,
        "evidence_artifacts": _normalize_evidence_paths(evidence_artifacts),
        "canonical_json_hash": None,
    }


def write_broker_submit_attempt_v1(*, repo_root: Path, submission_dir: Path, payload: Dict[str, Any]) -> str:
    return _write_artifact(
        repo_root=repo_root,
        path=(submission_dir / ATTEMPT_FILENAME).resolve(),
        payload=payload,
        schema_relpath=ATTEMPT_SCHEMA_RELPATH,
        allow_monotonic_upgrade=False,
    )


def write_broker_acknowledgement_v1(*, repo_root: Path, submission_dir: Path, payload: Dict[str, Any]) -> str:
    return _write_artifact(
        repo_root=repo_root,
        path=(submission_dir / ACK_FILENAME).resolve(),
        payload=payload,
        schema_relpath=ACK_SCHEMA_RELPATH,
        allow_monotonic_upgrade=False,
    )


def write_broker_order_outcome_v1(*, repo_root: Path, submission_dir: Path, payload: Dict[str, Any]) -> str:
    return _write_artifact(
        repo_root=repo_root,
        path=(submission_dir / OUTCOME_FILENAME).resolve(),
        payload=payload,
        schema_relpath=OUTCOME_SCHEMA_RELPATH,
        allow_monotonic_upgrade=True,
    )
