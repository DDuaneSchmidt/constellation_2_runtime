from __future__ import annotations

import hashlib
from functools import lru_cache
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
    sha256_file_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import resolve_execution_journal_path
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_journal.v1.schema.json"
EVENT_TYPE_REGISTRY_RELPATH_V1 = "governance/02_REGISTRIES/C2_EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_V1.json"
IDENTITY_FIELDS_V1 = (
    "day_utc",
    "day_attempt_id",
    "pipeline_run_id",
    "release_id",
    "git_sha",
)
COMMON_PAYLOAD_KEYS_V1 = (
    "source_artifact_path",
    "source_artifact_sha256",
    "source_generated_at_utc",
)
BOOLEAN_PAYLOAD_KEYS_V1 = {"system_ready", "submission_authorized"}
INTEGER_PAYLOAD_KEYS_V1 = {"duration_ms"}
LIST_OF_STRINGS_PAYLOAD_KEYS_V1 = {"blocking_codes", "source_artifact_paths"}


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


def _require_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"EXECUTION_JOURNAL_PAYLOAD_BOOLEAN_INVALID:{field_name}")
    return value


def _require_nonnegative_int(value: Any, field_name: str) -> int:
    if not isinstance(value, int) or value < 0:
        raise ValueError(f"EXECUTION_JOURNAL_PAYLOAD_INTEGER_INVALID:{field_name}")
    return value


def execution_identity_tuple_v1(
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


def identity_tuple_from_mapping_v1(
    payload: Mapping[str, Any],
    *,
    context: str,
) -> dict[str, str]:
    if not isinstance(payload, Mapping):
        raise ValueError(f"EXECUTION_JOURNAL_IDENTITY_PAYLOAD_INVALID:{context}")
    return execution_identity_tuple_v1(
        day_utc=str(payload.get("day_utc") or "").strip(),
        day_attempt_id=str(payload.get("day_attempt_id") or "").strip(),
        pipeline_run_id=str(payload.get("pipeline_run_id") or "").strip(),
        release_id=str(payload.get("release_id") or "").strip(),
        git_sha=str(payload.get("git_sha") or "").strip(),
    )


def require_matching_identity_tuple_v1(
    *,
    expected_identity: Mapping[str, Any],
    candidate_identity: Mapping[str, Any],
    context: str,
) -> dict[str, str]:
    expected = execution_identity_tuple_v1(
        day_utc=str(expected_identity.get("day_utc") or "").strip(),
        day_attempt_id=str(expected_identity.get("day_attempt_id") or "").strip(),
        pipeline_run_id=str(expected_identity.get("pipeline_run_id") or "").strip(),
        release_id=str(expected_identity.get("release_id") or "").strip(),
        git_sha=str(expected_identity.get("git_sha") or "").strip(),
    )
    candidate = execution_identity_tuple_v1(
        day_utc=str(candidate_identity.get("day_utc") or "").strip(),
        day_attempt_id=str(candidate_identity.get("day_attempt_id") or "").strip(),
        pipeline_run_id=str(candidate_identity.get("pipeline_run_id") or "").strip(),
        release_id=str(candidate_identity.get("release_id") or "").strip(),
        git_sha=str(candidate_identity.get("git_sha") or "").strip(),
    )
    for field_name in IDENTITY_FIELDS_V1:
        if candidate[field_name] != expected[field_name]:
            raise ValueError(f"EXECUTION_JOURNAL_CROSS_IDENTITY_CONTAMINATION:{context}:{field_name}")
    return expected


@lru_cache(maxsize=1)
def event_type_registry_v1() -> dict[str, Any]:
    path = (REPO_ROOT / EVENT_TYPE_REGISTRY_RELPATH_V1).resolve()
    payload = read_json_object_v1(path)
    if str(payload.get("schema_id") or "").strip() != "C2_EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_V1":
        raise ValueError("EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_SCHEMA_ID_INVALID")
    if str(payload.get("schema_version") or "").strip() != "v1":
        raise ValueError("EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_SCHEMA_VERSION_INVALID")
    event_types = payload.get("event_types")
    if not isinstance(event_types, Mapping) or not event_types:
        raise ValueError("EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_EMPTY")
    normalized: dict[str, Any] = {}
    for event_type, raw_entry in event_types.items():
        name = _require_nonempty_string(event_type, "event_type")
        if not isinstance(raw_entry, Mapping):
            raise ValueError(f"EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_ENTRY_INVALID:{name}")
        payload_family = _require_nonempty_string(raw_entry.get("payload_family"), f"{name}:payload_family")
        semantics = _require_nonempty_string(raw_entry.get("semantics"), f"{name}:semantics")
        allowed_sources_raw = raw_entry.get("allowed_event_sources")
        required_fields_raw = raw_entry.get("required_payload_fields")
        event_key_fields_raw = raw_entry.get("event_key_payload_fields")
        duplicate_policy = _require_nonempty_string(raw_entry.get("duplicate_policy"), f"{name}:duplicate_policy")
        if not isinstance(allowed_sources_raw, list) or not allowed_sources_raw:
            raise ValueError(f"EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_SOURCES_INVALID:{name}")
        if not isinstance(required_fields_raw, list) or not required_fields_raw:
            raise ValueError(f"EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_REQUIRED_FIELDS_INVALID:{name}")
        if not isinstance(event_key_fields_raw, list) or not event_key_fields_raw:
            raise ValueError(f"EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_EVENT_KEY_FIELDS_INVALID:{name}")
        allowed_sources = tuple(
            _require_nonempty_string(value, f"{name}:allowed_event_sources") for value in allowed_sources_raw
        )
        required_fields = tuple(
            _require_nonempty_string(value, f"{name}:required_payload_fields") for value in required_fields_raw
        )
        event_key_fields = tuple(
            _require_nonempty_string(value, f"{name}:event_key_payload_fields") for value in event_key_fields_raw
        )
        multiple_per_day_attempt = raw_entry.get("multiple_per_day_attempt")
        if not isinstance(multiple_per_day_attempt, bool):
            raise ValueError(
                f"EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_MULTIPLE_PER_DAY_ATTEMPT_INVALID:{name}"
            )
        normalized[name] = {
            "payload_family": payload_family,
            "semantics": semantics,
            "allowed_event_sources": allowed_sources,
            "required_payload_fields": required_fields,
            "event_key_payload_fields": event_key_fields,
            "duplicate_policy": duplicate_policy,
            "multiple_per_day_attempt": multiple_per_day_attempt,
        }
    return {
        "path": str(path),
        "schema_id": "C2_EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_V1",
        "schema_version": "v1",
        "event_types": normalized,
    }


def event_registry_entry_v1(event_type: str) -> dict[str, Any]:
    entry = dict(event_type_registry_v1()["event_types"]).get(str(event_type or "").strip())
    if not isinstance(entry, dict):
        raise ValueError(f"EXECUTION_JOURNAL_INVALID_EVENT_TYPE:{event_type}")
    return entry


def execution_journal_id_v1(
    *,
    day_utc: str,
    day_attempt_id: str,
    pipeline_run_id: str,
    release_id: str,
    git_sha: str,
) -> str:
    identity = execution_identity_tuple_v1(
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
    )
    return f"execution_journal:{identity['day_utc']}:{_stable_hash(identity)[:16]}"


def artifact_generated_at_utc_v1(payload: Mapping[str, Any]) -> str:
    return (
        str(payload.get("evaluated_at_utc") or "").strip()
        or str(payload.get("produced_at_utc") or "").strip()
        or str(payload.get("produced_utc") or "").strip()
        or str(payload.get("generated_at_utc") or "").strip()
    )


def source_artifact_payload_v1(*, path: Path | str, payload: Mapping[str, Any]) -> dict[str, str]:
    target = Path(str(path)).resolve()
    if not target.exists() or not target.is_file():
        raise ValueError(f"EXECUTION_JOURNAL_SOURCE_ARTIFACT_MISSING:{target}")
    return {
        "source_artifact_path": str(target),
        "source_artifact_sha256": sha256_file_v1(target),
        "source_generated_at_utc": _require_nonempty_string(
            artifact_generated_at_utc_v1(payload),
            f"source_generated_at_utc:{target}",
        ),
    }


def _validate_payload_family(event_type: str, payload_family: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    if payload_family == "deployment_state_machine_outcome":
        normalized["blocking_codes"] = _normalize_codes(normalized.get("blocking_codes") or [])
    elif payload_family == "startup_materialization_outcome":
        normalized["blocking_codes"] = _normalize_codes(normalized.get("blocking_codes") or [])
    elif payload_family == "startup_proof_validation_outcome":
        normalized["blocking_codes"] = _normalize_codes(normalized.get("blocking_codes") or [])
    elif payload_family == "ledger_authority_outcome":
        normalized["system_ready"] = _require_bool(normalized.get("system_ready"), f"{event_type}:system_ready")
        normalized["submission_authorized"] = _require_bool(
            normalized.get("submission_authorized"),
            f"{event_type}:submission_authorized",
        )
        normalized["blocking_codes"] = _normalize_codes(normalized.get("blocking_codes") or [])
    elif payload_family == "submission_authorization_outcome":
        normalized["submission_authorized"] = _require_bool(
            normalized.get("submission_authorized"),
            f"{event_type}:submission_authorized",
        )
    elif payload_family == "stage_duration_measurement":
        normalized["duration_ms"] = _require_nonnegative_int(
            normalized.get("duration_ms"),
            f"{event_type}:duration_ms",
        )
    elif payload_family == "system_contradiction":
        value = normalized.get("source_artifact_paths")
        if not isinstance(value, list):
            raise ValueError(f"EXECUTION_JOURNAL_PAYLOAD_LIST_INVALID:{event_type}:source_artifact_paths")
        normalized["source_artifact_paths"] = _normalize_codes(value)
    elif payload_family == "state_machine_decision_outcome":
        pass
    else:
        raise ValueError(f"EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_PAYLOAD_FAMILY_UNKNOWN:{payload_family}")
    for key in LIST_OF_STRINGS_PAYLOAD_KEYS_V1:
        if key in normalized and isinstance(normalized.get(key), list):
            normalized[key] = _normalize_codes(normalized.get(key) or [])
    for key in COMMON_PAYLOAD_KEYS_V1:
        _require_nonempty_string(normalized.get(key), f"{event_type}:{key}")
    return normalized


def _validate_payload_shape(event_type: str, event_source: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    entry = event_registry_entry_v1(event_type)
    event_source_value = _require_nonempty_string(event_source, "event_source")
    if event_source_value not in tuple(entry["allowed_event_sources"]):
        raise ValueError(f"EXECUTION_JOURNAL_INVALID_EVENT_SOURCE:{event_type}:{event_source_value}")
    if not isinstance(payload, Mapping):
        raise ValueError(f"EXECUTION_JOURNAL_INVALID_PAYLOAD_TYPE:{event_type}")
    normalized = dict(payload)
    for required_key in tuple(entry["required_payload_fields"]):
        if required_key not in normalized:
            raise ValueError(f"EXECUTION_JOURNAL_PAYLOAD_FIELD_MISSING:{event_type}:{required_key}")
    return _validate_payload_family(event_type, str(entry["payload_family"]), normalized)


def _event_key_payload_subset(event_type: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    entry = event_registry_entry_v1(event_type)
    subset: dict[str, Any] = {}
    for field_name in tuple(entry["event_key_payload_fields"]):
        if field_name not in payload:
            raise ValueError(f"EXECUTION_JOURNAL_EVENT_KEY_PAYLOAD_FIELD_MISSING:{event_type}:{field_name}")
        value = payload[field_name]
        if isinstance(value, list):
            subset[field_name] = _normalize_codes(value)
        else:
            subset[field_name] = value
    return subset


def _legacy_canonical_event_key_v1(
    *,
    identity: Mapping[str, str],
    event_type: str,
    event_source: str,
    status: str,
    payload: Mapping[str, Any],
) -> str:
    return _stable_hash(
        {
            **identity,
            "event_type": event_type,
            "event_source": event_source,
            "status": status,
            "payload": dict(payload),
        }
    )[:24]


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
    identity = execution_identity_tuple_v1(
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
    )
    event_source_value = _require_nonempty_string(event_source, "event_source")
    status_value = _require_nonempty_string(status, "status")
    payload_value = _validate_payload_shape(event_type, event_source_value, payload)
    return _stable_hash(
        {
            **identity,
            "event_type": event_type,
            "event_source": event_source_value,
            "status": status_value,
            "payload": _event_key_payload_subset(event_type, payload_value),
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
    identity = execution_identity_tuple_v1(
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
    payload_value = _validate_payload_shape(event_type, event_source_value, payload)
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
    identity = execution_identity_tuple_v1(
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
        legacy_event_key = _legacy_canonical_event_key_v1(
            identity=identity,
            event_type=event["event_type"],
            event_source=event["event_source"],
            status=event["status"],
            payload=event["payload"],
        )
        if raw_event.get("event_key") not in {event["event_key"], legacy_event_key}:
            raise ValueError(f"EXECUTION_JOURNAL_EVENT_KEY_INVALID:seq={event['event_seq']}")
        if event["event_seq"] != expected_seq:
            raise ValueError(f"EXECUTION_JOURNAL_EVENT_SEQ_GAP_OR_DUPLICATE:{event['event_seq']}")
        if event["event_seq"] in seen_seq:
            raise ValueError(f"EXECUTION_JOURNAL_DUPLICATE_EVENT_SEQ:{event['event_seq']}")
        if event["event_key"] in seen_keys:
            raise ValueError(f"EXECUTION_JOURNAL_DUPLICATE_EVENT_KEY:{event['event_key']}")
        require_matching_identity_tuple_v1(
            expected_identity=identity,
            candidate_identity=raw_event,
            context=f"event_seq={event['event_seq']}",
        )
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
    identity = execution_identity_tuple_v1(
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


def read_execution_journal_identity_anchor_v1(
    *,
    truth_root: str | Path,
    day_utc: str,
) -> dict[str, str] | None:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    path = resolve_execution_journal_path(truth_root=root, day_utc=day_utc)
    if not path.exists() or not path.is_file():
        return None
    ref = read_execution_journal_v1(truth_root=root, day_utc=day_utc)
    return identity_tuple_from_mapping_v1(ref.payload, context="existing_journal")


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
    identity = execution_identity_tuple_v1(
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
        require_matching_identity_tuple_v1(
            expected_identity=identity,
            candidate_identity=existing_payload,
            context="existing_journal",
        )
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
    existing_event_keys = {
        build_event_record_v1(
            **identity,
            event_seq=int(row.get("event_seq")),
            event_type=str(row.get("event_type") or "").strip(),
            event_source=str(row.get("event_source") or "").strip(),
            generated_at_utc=str(row.get("generated_at_utc") or "").strip(),
            status=str(row.get("status") or "").strip(),
            payload=dict(row.get("payload") or {}),
        )["event_key"]
        for row in existing_events
        if isinstance(row, Mapping)
    }
    if candidate["event_key"] in existing_event_keys:
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


def append_deployment_outcome_event_v1(
    *,
    truth_root: str | Path,
    day_utc: str,
    day_attempt_id: str,
    pipeline_run_id: str,
    release_id: str,
    git_sha: str,
    source_path: Path | str,
    source_payload: Mapping[str, Any],
    producer_module: str,
) -> SurfaceRefV1:
    final_deployment_decision = str(source_payload.get("final_deployment_decision") or "").strip().upper()
    event_type = "DEPLOYMENT_ACTIVATED" if final_deployment_decision == "DEPLOY_ACTIVE" else "DEPLOYMENT_BLOCKED"
    return append_execution_event_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
        event_type=event_type,
        event_source="deployment_state_machine_v1",
        status=final_deployment_decision,
        payload={
            **source_artifact_payload_v1(path=source_path, payload=source_payload),
            "final_deployment_decision": final_deployment_decision,
            "blocking_codes": list(source_payload.get("blocking_codes") or []),
            "first_true_blocker_code": str(source_payload.get("first_true_blocker_code") or "").strip(),
        },
        producer_module=producer_module,
        generated_at_utc=str(source_payload.get("evaluated_at_utc") or "").strip(),
    )


def append_startup_materialization_event_v1(
    *,
    truth_root: str | Path,
    identity: Mapping[str, Any],
    source_path: Path | str,
    source_payload: Mapping[str, Any],
    producer_module: str,
) -> SurfaceRefV1:
    return append_execution_event_v1(
        truth_root=truth_root,
        event_type="STARTUP_MATERIALIZATION_COMPLETED",
        event_source="startup_materialization_v1",
        status=str(source_payload.get("status") or "").strip().upper(),
        payload={
            **source_artifact_payload_v1(path=source_path, payload=source_payload),
            "startup_status": str(source_payload.get("status") or "").strip().upper(),
            "freshness_verdict": str(source_payload.get("freshness_verdict") or "").strip().upper(),
            "linkage_verdict": str(source_payload.get("linkage_verdict") or "").strip().upper(),
            "blocking_codes": list(source_payload.get("blocking_codes") or []),
        },
        producer_module=producer_module,
        generated_at_utc=artifact_generated_at_utc_v1(source_payload),
        **execution_identity_tuple_v1(
            day_utc=str(identity.get("day_utc") or "").strip(),
            day_attempt_id=str(identity.get("day_attempt_id") or "").strip(),
            pipeline_run_id=str(identity.get("pipeline_run_id") or "").strip(),
            release_id=str(identity.get("release_id") or "").strip(),
            git_sha=str(identity.get("git_sha") or "").strip(),
        ),
    )


def append_startup_proof_validation_event_v1(
    *,
    truth_root: str | Path,
    identity: Mapping[str, Any],
    source_path: Path | str,
    source_payload: Mapping[str, Any],
    producer_module: str,
) -> SurfaceRefV1:
    return append_execution_event_v1(
        truth_root=truth_root,
        event_type="STARTUP_PROOF_VALIDATION_COMPLETED",
        event_source="startup_proof_validation_v1",
        status=str(source_payload.get("status") or "").strip().upper(),
        payload={
            **source_artifact_payload_v1(path=source_path, payload=source_payload),
            "startup_proof_status": str(source_payload.get("status") or "").strip().upper(),
            "ledger_authority_status": str(source_payload.get("ledger_authority_status") or "").strip().upper(),
            "blocking_codes": list(source_payload.get("blocking_codes") or []),
        },
        producer_module=producer_module,
        generated_at_utc=artifact_generated_at_utc_v1(source_payload),
        **execution_identity_tuple_v1(
            day_utc=str(identity.get("day_utc") or "").strip(),
            day_attempt_id=str(identity.get("day_attempt_id") or "").strip(),
            pipeline_run_id=str(identity.get("pipeline_run_id") or "").strip(),
            release_id=str(identity.get("release_id") or "").strip(),
            git_sha=str(identity.get("git_sha") or "").strip(),
        ),
    )


def append_ledger_authority_event_v1(
    *,
    truth_root: str | Path,
    identity: Mapping[str, Any],
    source_path: Path | str,
    source_payload: Mapping[str, Any],
    producer_module: str,
) -> SurfaceRefV1:
    control_state = dict(source_payload.get("control_state") or {})
    return append_execution_event_v1(
        truth_root=truth_root,
        event_type="LEDGER_AUTHORITY_RECORDED",
        event_source="paper_session_ledger_v1",
        status=str(control_state.get("authority_status") or source_payload.get("authority_status") or "").strip().upper(),
        payload={
            **source_artifact_payload_v1(path=source_path, payload=source_payload),
            "ledger_id": str(source_payload.get("ledger_id") or "").strip(),
            "authority_status": str(
                control_state.get("authority_status") or source_payload.get("authority_status") or ""
            ).strip().upper(),
            "system_ready": bool(control_state.get("system_ready") is True),
            "submission_authorized": bool(control_state.get("submission_authorized") is True),
            "blocking_codes": list(control_state.get("blocking_codes") or []),
        },
        producer_module=producer_module,
        generated_at_utc=artifact_generated_at_utc_v1(source_payload),
        **execution_identity_tuple_v1(
            day_utc=str(identity.get("day_utc") or "").strip(),
            day_attempt_id=str(identity.get("day_attempt_id") or "").strip(),
            pipeline_run_id=str(identity.get("pipeline_run_id") or "").strip(),
            release_id=str(identity.get("release_id") or "").strip(),
            git_sha=str(identity.get("git_sha") or "").strip(),
        ),
    )


def append_submission_authorization_event_v1(
    *,
    truth_root: str | Path,
    identity: Mapping[str, Any],
    source_path: Path | str,
    source_payload: Mapping[str, Any],
    producer_module: str,
) -> SurfaceRefV1:
    control_state = dict(source_payload.get("control_state") or {})
    return append_execution_event_v1(
        truth_root=truth_root,
        event_type="SUBMISSION_AUTHORIZATION_RECORDED",
        event_source="paper_session_ledger_v1",
        status="AUTHORIZED" if bool(control_state.get("submission_authorized") is True) else "NOT_AUTHORIZED",
        payload={
            **source_artifact_payload_v1(path=source_path, payload=source_payload),
            "ledger_id": str(source_payload.get("ledger_id") or "").strip(),
            "submission_authorized": bool(control_state.get("submission_authorized") is True),
            "authority_status": str(
                control_state.get("authority_status") or source_payload.get("authority_status") or ""
            ).strip().upper(),
        },
        producer_module=producer_module,
        generated_at_utc=artifact_generated_at_utc_v1(source_payload),
        **execution_identity_tuple_v1(
            day_utc=str(identity.get("day_utc") or "").strip(),
            day_attempt_id=str(identity.get("day_attempt_id") or "").strip(),
            pipeline_run_id=str(identity.get("pipeline_run_id") or "").strip(),
            release_id=str(identity.get("release_id") or "").strip(),
            git_sha=str(identity.get("git_sha") or "").strip(),
        ),
    )


def append_state_machine_decision_event_v1(
    *,
    truth_root: str | Path,
    identity: Mapping[str, Any],
    source_path: Path | str,
    source_payload: Mapping[str, Any],
    producer_module: str,
) -> SurfaceRefV1:
    first_true_blocker = dict(source_payload.get("first_true_blocker") or {})
    return append_execution_event_v1(
        truth_root=truth_root,
        event_type="STATE_MACHINE_DECISION_RECORDED",
        event_source="trading_day_state_machine_v1",
        status=str(source_payload.get("final_start_decision") or "").strip().upper(),
        payload={
            **source_artifact_payload_v1(path=source_path, payload=source_payload),
            "state_machine_id": str(source_payload.get("state_machine_id") or "").strip(),
            "final_start_decision": str(source_payload.get("final_start_decision") or "").strip().upper(),
            "first_true_blocker_code": str(
                first_true_blocker.get("first_true_blocker_code") or ""
            ).strip(),
            "first_true_blocker_artifact_path": str(
                first_true_blocker.get("first_true_blocker_artifact_path") or ""
            ).strip(),
        },
        producer_module=producer_module,
        generated_at_utc=artifact_generated_at_utc_v1(source_payload),
        **execution_identity_tuple_v1(
            day_utc=str(identity.get("day_utc") or "").strip(),
            day_attempt_id=str(identity.get("day_attempt_id") or "").strip(),
            pipeline_run_id=str(identity.get("pipeline_run_id") or "").strip(),
            release_id=str(identity.get("release_id") or "").strip(),
            git_sha=str(identity.get("git_sha") or "").strip(),
        ),
    )


def append_stage_duration_event_v1(
    *,
    truth_root: str | Path,
    identity: Mapping[str, Any],
    source_path: Path | str,
    source_payload: Mapping[str, Any],
    stage_name: str,
    started_at_utc: str,
    ended_at_utc: str,
    duration_ms: int,
    producer_module: str,
) -> SurfaceRefV1:
    return append_execution_event_v1(
        truth_root=truth_root,
        event_type="STAGE_DURATION_RECORDED",
        event_source="paper_session_startup_flow_v1",
        status="RECORDED",
        payload={
            **source_artifact_payload_v1(path=source_path, payload=source_payload),
            "stage_name": _require_nonempty_string(stage_name, "stage_name"),
            "started_at_utc": _require_nonempty_string(started_at_utc, "started_at_utc"),
            "ended_at_utc": _require_nonempty_string(ended_at_utc, "ended_at_utc"),
            "duration_ms": _require_nonnegative_int(duration_ms, "duration_ms"),
        },
        producer_module=producer_module,
        generated_at_utc=artifact_generated_at_utc_v1(source_payload),
        **execution_identity_tuple_v1(
            day_utc=str(identity.get("day_utc") or "").strip(),
            day_attempt_id=str(identity.get("day_attempt_id") or "").strip(),
            pipeline_run_id=str(identity.get("pipeline_run_id") or "").strip(),
            release_id=str(identity.get("release_id") or "").strip(),
            git_sha=str(identity.get("git_sha") or "").strip(),
        ),
    )


def append_system_contradiction_event_v1(
    *,
    truth_root: str | Path,
    identity: Mapping[str, Any],
    source_path: Path | str,
    source_payload: Mapping[str, Any],
    contradiction_code: str,
    contradiction_details: str,
    source_artifact_paths: list[str],
    producer_module: str,
) -> SurfaceRefV1:
    return append_execution_event_v1(
        truth_root=truth_root,
        event_type="SYSTEM_CONTRADICTION_DETECTED",
        event_source="execution_journal_v1",
        status="CONTRADICTION",
        payload={
            **source_artifact_payload_v1(path=source_path, payload=source_payload),
            "contradiction_code": _require_nonempty_string(contradiction_code, "contradiction_code"),
            "contradiction_details": _require_nonempty_string(
                contradiction_details,
                "contradiction_details",
            ),
            "source_artifact_paths": list(source_artifact_paths),
        },
        producer_module=producer_module,
        generated_at_utc=artifact_generated_at_utc_v1(source_payload),
        **execution_identity_tuple_v1(
            day_utc=str(identity.get("day_utc") or "").strip(),
            day_attempt_id=str(identity.get("day_attempt_id") or "").strip(),
            pipeline_run_id=str(identity.get("pipeline_run_id") or "").strip(),
            release_id=str(identity.get("release_id") or "").strip(),
            git_sha=str(identity.get("git_sha") or "").strip(),
        ),
    )
