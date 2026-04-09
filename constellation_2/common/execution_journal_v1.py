from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import resolve_execution_journal_path
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_journal.v1.schema.json"
VALID_EVENT_TYPES_V1 = (
    "DEPLOYMENT_ACTIVATED",
    "DEPLOYMENT_BLOCKED",
    "STARTUP_MATERIALIZATION_COMPLETED",
    "STARTUP_PROOF_VALIDATION_COMPLETED",
    "LEDGER_AUTHORITY_RECORDED",
    "STATE_MACHINE_DECISION_RECORDED",
    "SUBMISSION_AUTHORIZATION_RECORDED",
    "STAGE_DURATION_RECORDED",
    "SYSTEM_CONTRADICTION_DETECTED",
)
COMMON_PAYLOAD_KEYS_V1 = (
    "source_artifact_path",
    "source_artifact_sha256",
    "source_generated_at_utc",
)
PAYLOAD_REQUIREMENTS_V1: dict[str, tuple[str, ...]] = {
    "DEPLOYMENT_ACTIVATED": (
        *COMMON_PAYLOAD_KEYS_V1,
        "final_deployment_decision",
        "blocking_codes",
        "first_true_blocker_code",
    ),
    "DEPLOYMENT_BLOCKED": (
        *COMMON_PAYLOAD_KEYS_V1,
        "final_deployment_decision",
        "blocking_codes",
        "first_true_blocker_code",
    ),
    "STARTUP_MATERIALIZATION_COMPLETED": (
        *COMMON_PAYLOAD_KEYS_V1,
        "startup_status",
        "freshness_verdict",
        "linkage_verdict",
        "blocking_codes",
    ),
    "STARTUP_PROOF_VALIDATION_COMPLETED": (
        *COMMON_PAYLOAD_KEYS_V1,
        "startup_proof_status",
        "ledger_authority_status",
        "blocking_codes",
    ),
    "LEDGER_AUTHORITY_RECORDED": (
        *COMMON_PAYLOAD_KEYS_V1,
        "ledger_id",
        "authority_status",
        "system_ready",
        "submission_authorized",
        "blocking_codes",
    ),
    "STATE_MACHINE_DECISION_RECORDED": (
        *COMMON_PAYLOAD_KEYS_V1,
        "state_machine_id",
        "final_start_decision",
        "first_true_blocker_code",
        "first_true_blocker_artifact_path",
    ),
    "SUBMISSION_AUTHORIZATION_RECORDED": (
        *COMMON_PAYLOAD_KEYS_V1,
        "ledger_id",
        "submission_authorized",
        "authority_status",
    ),
    "STAGE_DURATION_RECORDED": (
        *COMMON_PAYLOAD_KEYS_V1,
        "stage_name",
        "started_at_utc",
        "ended_at_utc",
        "duration_ms",
    ),
    "SYSTEM_CONTRADICTION_DETECTED": (
        *COMMON_PAYLOAD_KEYS_V1,
        "contradiction_code",
        "contradiction_details",
        "source_artifact_paths",
    ),
}
BOOLEAN_PAYLOAD_KEYS_V1 = {"system_ready", "submission_authorized"}
INTEGER_PAYLOAD_KEYS_V1 = {"duration_ms"}
LIST_OF_STRINGS_PAYLOAD_KEYS_V1 = {
    "blocking_codes",
    "source_artifact_paths",
}


def _stable_hash(data: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(dict(data))).hexdigest()


def _require_nonempty_string(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"EXECUTION_JOURNAL_REQUIRED_FIELD_MISSING:{field_name}")
    return text


def _require_sha(value: Any, field_name: str) -> str:
    text = _require_nonempty_string(value, field_name).lower()
    if len(text) != 40 or any(ch not in "0123456789abcdef" for ch in text):
        raise ValueError(f"EXECUTION_JOURNAL_INVALID_SHA:{field_name}")
    return text


def _normalize_codes(values: Iterable[Any]) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _journal_identity_tuple(
    *,
    day_utc: str,
    day_attempt_id: str,
    pipeline_run_id: str,
    release_id: str,
    git_sha: str,
) -> dict[str, str]:
    return {
        "day_utc": parse_day_utc_v1(day_utc),
        "day_attempt_id": _require_nonempty_string(day_attempt_id, "day_attempt_id"),
        "pipeline_run_id": _require_nonempty_string(pipeline_run_id, "pipeline_run_id"),
        "release_id": _require_nonempty_string(release_id, "release_id"),
        "git_sha": _require_sha(git_sha, "git_sha"),
    }


def execution_journal_id_v1(*, day_utc: str, day_attempt_id: str, pipeline_run_id: str, release_id: str, git_sha: str) -> str:
    identity = _journal_identity_tuple(
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
    )
    return f"execution_journal:{identity['day_utc']}:{_stable_hash(identity)[:16]}"


def _validate_payload_shape(event_type: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    if event_type not in VALID_EVENT_TYPES_V1:
        raise ValueError(f"EXECUTION_JOURNAL_INVALID_EVENT_TYPE:{event_type}")
    if not isinstance(payload, Mapping):
        raise ValueError(f"EXECUTION_JOURNAL_INVALID_PAYLOAD_TYPE:{event_type}")
    normalized = dict(payload)
    for required_key in PAYLOAD_REQUIREMENTS_V1[event_type]:
        if required_key not in normalized:
            raise ValueError(f"EXECUTION_JOURNAL_PAYLOAD_FIELD_MISSING:{event_type}:{required_key}")
    for key in COMMON_PAYLOAD_KEYS_V1:
        _require_nonempty_string(normalized.get(key), f"{event_type}:{key}")
    for key in BOOLEAN_PAYLOAD_KEYS_V1:
        if key in normalized and not isinstance(normalized.get(key), bool):
            raise ValueError(f"EXECUTION_JOURNAL_PAYLOAD_BOOLEAN_INVALID:{event_type}:{key}")
    for key in INTEGER_PAYLOAD_KEYS_V1:
        if key in normalized:
            value = normalized.get(key)
            if not isinstance(value, int) or value < 0:
                raise ValueError(f"EXECUTION_JOURNAL_PAYLOAD_INTEGER_INVALID:{event_type}:{key}")
    for key in LIST_OF_STRINGS_PAYLOAD_KEYS_V1:
        if key in normalized:
            value = normalized.get(key)
            if not isinstance(value, list):
                raise ValueError(f"EXECUTION_JOURNAL_PAYLOAD_LIST_INVALID:{event_type}:{key}")
            normalized[key] = _normalize_codes(value)
    return normalized


def canonical_event_key_v1(
    *,
    day_utc: str,
    day_attempt_id: str,
    pipeline_run_id: str,
    release_id: str,
    git_sha: str,
    event_type: str,
    event_source: str,
    status: str,
    payload: Mapping[str, Any],
) -> str:
    identity = _journal_identity_tuple(
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
    )
    event_source_value = _require_nonempty_string(event_source, "event_source")
    status_value = _require_nonempty_string(status, "status")
    payload_value = _validate_payload_shape(event_type, payload)
    return _stable_hash(
        {
            **identity,
            "event_type": event_type,
            "event_source": event_source_value,
            "status": status_value,
            "payload": payload_value,
        }
    )[:24]


def build_event_record_v1(
    *,
    day_utc: str,
    day_attempt_id: str,
    pipeline_run_id: str,
    release_id: str,
    git_sha: str,
    event_seq: int,
    event_type: str,
    event_source: str,
    generated_at_utc: str,
    status: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    identity = _journal_identity_tuple(
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
    )
    if not isinstance(event_seq, int) or event_seq <= 0:
        raise ValueError("EXECUTION_JOURNAL_EVENT_SEQ_INVALID")
    generated_at_value = _require_nonempty_string(generated_at_utc, "generated_at_utc")
    status_value = _require_nonempty_string(status, "status")
    event_source_value = _require_nonempty_string(event_source, "event_source")
    payload_value = _validate_payload_shape(event_type, payload)
    event_key = canonical_event_key_v1(
        **identity,
        event_type=event_type,
        event_source=event_source_value,
        status=status_value,
        payload=payload_value,
    )
    return {
        "schema_version": "v1",
        **identity,
        "event_seq": event_seq,
        "event_key": event_key,
        "event_type": event_type,
        "event_source": event_source_value,
        "generated_at_utc": generated_at_value,
        "status": status_value,
        "payload": payload_value,
    }


def validate_execution_journal_payload_v1(payload: Mapping[str, Any]) -> dict[str, Any]:
    obj = dict(payload)
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_V1)
    identity = _journal_identity_tuple(
        day_utc=obj["day_utc"],
        day_attempt_id=obj["day_attempt_id"],
        pipeline_run_id=obj["pipeline_run_id"],
        release_id=obj["release_id"],
        git_sha=obj["git_sha"],
    )
    events = list(obj.get("events") or [])
    seen_seq: set[int] = set()
    seen_keys: set[str] = set()
    expected_seq = 1
    for raw_event in events:
        if not isinstance(raw_event, Mapping):
            raise ValueError("EXECUTION_JOURNAL_EVENT_NOT_OBJECT")
        event = build_event_record_v1(
            **identity,
            event_seq=int(raw_event.get("event_seq")),
            event_type=str(raw_event.get("event_type") or "").strip(),
            event_source=str(raw_event.get("event_source") or "").strip(),
            generated_at_utc=str(raw_event.get("generated_at_utc") or "").strip(),
            status=str(raw_event.get("status") or "").strip(),
            payload=dict(raw_event.get("payload") or {}),
        )
        if raw_event.get("event_key") != event["event_key"]:
            raise ValueError(f"EXECUTION_JOURNAL_EVENT_KEY_INVALID:seq={event['event_seq']}")
        if event["event_seq"] != expected_seq:
            raise ValueError(f"EXECUTION_JOURNAL_EVENT_SEQ_GAP_OR_DUPLICATE:{event['event_seq']}")
        if event["event_seq"] in seen_seq:
            raise ValueError(f"EXECUTION_JOURNAL_DUPLICATE_EVENT_SEQ:{event['event_seq']}")
        if event["event_key"] in seen_keys:
            raise ValueError(f"EXECUTION_JOURNAL_DUPLICATE_EVENT_KEY:{event['event_key']}")
        for identity_key, identity_value in identity.items():
            if str(raw_event.get(identity_key) or "").strip() != identity_value:
                raise ValueError(f"EXECUTION_JOURNAL_CROSS_IDENTITY_CONTAMINATION:{identity_key}")
        seen_seq.add(event["event_seq"])
        seen_keys.add(event["event_key"])
        expected_seq += 1
    if int(obj.get("last_event_seq") or 0) != len(events):
        raise ValueError("EXECUTION_JOURNAL_LAST_EVENT_SEQ_MISMATCH")
    journal_id = execution_journal_id_v1(**identity)
    if str(obj.get("journal_id") or "").strip() != journal_id:
        raise ValueError("EXECUTION_JOURNAL_ID_MISMATCH")
    return obj


def build_execution_journal_payload_v1(
    *,
    day_utc: str,
    day_attempt_id: str,
    pipeline_run_id: str,
    release_id: str,
    git_sha: str,
    generated_at_utc: str,
    events: list[dict[str, Any]],
    producer_module: str,
) -> dict[str, Any]:
    identity = _journal_identity_tuple(
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
    )
    normalized_events = [dict(event) for event in events]
    payload = {
        "schema_id": "execution_journal",
        "schema_version": "v1",
        "authority_scope": "CANONICAL_EXECUTION_JOURNAL",
        **identity,
        "journal_id": execution_journal_id_v1(**identity),
        "generated_at_utc": _require_nonempty_string(generated_at_utc, "generated_at_utc"),
        "last_event_seq": len(normalized_events),
        "events": normalized_events,
        "producer": producer_block_v1(module=producer_module, git_sha=identity["git_sha"]),
    }
    return validate_execution_journal_payload_v1(payload)


def read_execution_journal_v1(*, truth_root: str | Path, day_utc: str) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    path = resolve_execution_journal_path(truth_root=root, day_utc=day_utc)
    payload = read_json_object_v1(path)
    validated = validate_execution_journal_payload_v1(payload)
    raw = canonical_json_bytes_v1(validated) + b"\n"
    return SurfaceRefV1(path=path, payload=validated, sha256=hashlib.sha256(raw).hexdigest())


def append_execution_event_v1(
    *,
    truth_root: str | Path,
    day_utc: str,
    day_attempt_id: str,
    pipeline_run_id: str,
    release_id: str,
    git_sha: str,
    event_type: str,
    event_source: str,
    status: str,
    payload: Mapping[str, Any],
    producer_module: str,
    generated_at_utc: str | None = None,
) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    journal_path = resolve_execution_journal_path(truth_root=root, day_utc=day_utc)
    identity = _journal_identity_tuple(
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
    )
    existing_events: list[dict[str, Any]] = []
    if journal_path.exists() and journal_path.is_file():
        existing_ref = read_execution_journal_v1(truth_root=root, day_utc=day_utc)
        existing_payload = dict(existing_ref.payload)
        for identity_key, identity_value in identity.items():
            if str(existing_payload.get(identity_key) or "").strip() != identity_value:
                raise ValueError(f"EXECUTION_JOURNAL_CROSS_IDENTITY_CONTAMINATION:{identity_key}")
        existing_events = [dict(item) for item in existing_payload.get("events") or []]
    generated = generated_at_utc or now_utc_iso_v1()
    candidate = build_event_record_v1(
        **identity,
        event_seq=len(existing_events) + 1,
        event_type=event_type,
        event_source=event_source,
        generated_at_utc=generated,
        status=status,
        payload=payload,
    )
    if any(str(row.get("event_key") or "") == candidate["event_key"] for row in existing_events):
        return read_execution_journal_v1(truth_root=root, day_utc=day_utc)
    updated_events = existing_events + [candidate]
    journal_payload = build_execution_journal_payload_v1(
        **identity,
        generated_at_utc=generated,
        events=updated_events,
        producer_module=producer_module,
    )
    return atomic_write_validated_json_v1(
        path=journal_path,
        payload=journal_payload,
        schema_relpath=SCHEMA_RELPATH_V1,
    )
