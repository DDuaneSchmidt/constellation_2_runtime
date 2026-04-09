from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    now_utc_iso_v1,
    producer_block_v1,
    read_validated_surface_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_recurrence_registry_path,
)
from constellation_2.common.recurrence_fingerprint_v1 import (
    build_recurrence_fingerprint_record_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/recurrence_registry.v1.schema.json"
RECURRENCE_REGISTRY_STATUSES_V1 = ("OPEN", "CONTAINED", "ELIMINATED")


def _require_nonempty_string(value: Any, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(code)
    return text


def _require_status(value: Any, code: str) -> str:
    status = _require_nonempty_string(value, code).upper()
    if status not in RECURRENCE_REGISTRY_STATUSES_V1:
        raise ValueError(f"RECURRENCE_REGISTRY_STATUS_INVALID:{status}")
    return status


def build_empty_recurrence_registry_payload_v1(
    *,
    generated_at_utc: str | None = None,
    producer_module: str,
) -> dict[str, Any]:
    return {
        "schema_id": "recurrence_registry",
        "schema_version": "v1",
        "authority_scope": "CANONICAL_RECURRENCE_REGISTRY",
        "registry_id": "recurrence_registry_v1",
        "generated_at_utc": _require_nonempty_string(
            generated_at_utc or now_utc_iso_v1(),
            "RECURRENCE_REGISTRY_GENERATED_AT_MISSING",
        ),
        "entries_by_fingerprint": {},
        "producer": producer_block_v1(module=producer_module),
    }


def validate_recurrence_registry_payload_v1(payload: Mapping[str, Any]) -> dict[str, Any]:
    obj = dict(payload)
    if str(obj.get("schema_id") or "").strip() != "recurrence_registry":
        raise ValueError("RECURRENCE_REGISTRY_SCHEMA_ID_INVALID")
    if str(obj.get("schema_version") or "").strip() != "v1":
        raise ValueError("RECURRENCE_REGISTRY_SCHEMA_VERSION_INVALID")
    entries = obj.get("entries_by_fingerprint")
    if not isinstance(entries, Mapping):
        raise ValueError("RECURRENCE_REGISTRY_ENTRIES_INVALID")
    normalized_entries: dict[str, Any] = {}
    for recurrence_fingerprint, raw_entry in sorted(entries.items()):
        key = _require_nonempty_string(
            recurrence_fingerprint,
            "RECURRENCE_REGISTRY_ENTRY_FINGERPRINT_KEY_MISSING",
        )
        if not isinstance(raw_entry, Mapping):
            raise ValueError(f"RECURRENCE_REGISTRY_ENTRY_INVALID:{key}")
        entry = dict(raw_entry)
        fingerprint_record = build_recurrence_fingerprint_record_v1(
            blocker_family=str(entry.get("blocker_family") or "").strip(),
            blocker_code=str(entry.get("blocker_code") or "").strip(),
            authority_source=str(entry.get("authority_source") or "").strip(),
            stage_id=str(entry.get("stage_id") or "").strip(),
        )
        if fingerprint_record["recurrence_fingerprint"] != key:
            raise ValueError(f"RECURRENCE_REGISTRY_ENTRY_FINGERPRINT_MISMATCH:{key}")
        occurrence_count = entry.get("occurrence_count")
        if not isinstance(occurrence_count, int) or occurrence_count <= 0:
            raise ValueError(f"RECURRENCE_REGISTRY_OCCURRENCE_COUNT_INVALID:{key}")
        normalized_entries[key] = {
            **fingerprint_record,
            "first_seen_at_utc": _require_nonempty_string(
                entry.get("first_seen_at_utc"),
                f"RECURRENCE_REGISTRY_FIRST_SEEN_MISSING:{key}",
            ),
            "last_seen_at_utc": _require_nonempty_string(
                entry.get("last_seen_at_utc"),
                f"RECURRENCE_REGISTRY_LAST_SEEN_MISSING:{key}",
            ),
            "occurrence_count": occurrence_count,
            "current_status": _require_status(
                entry.get("current_status"),
                f"RECURRENCE_REGISTRY_CURRENT_STATUS_MISSING:{key}",
            ),
            "last_day_utc": _require_nonempty_string(
                entry.get("last_day_utc"),
                f"RECURRENCE_REGISTRY_LAST_DAY_MISSING:{key}",
            ),
            "last_day_attempt_id": _require_nonempty_string(
                entry.get("last_day_attempt_id"),
                f"RECURRENCE_REGISTRY_LAST_DAY_ATTEMPT_ID_MISSING:{key}",
            ),
            "last_release_id": _require_nonempty_string(
                entry.get("last_release_id"),
                f"RECURRENCE_REGISTRY_LAST_RELEASE_ID_MISSING:{key}",
            ),
            "last_git_sha": _require_nonempty_string(
                entry.get("last_git_sha"),
                f"RECURRENCE_REGISTRY_LAST_GIT_SHA_MISSING:{key}",
            ).lower(),
            "last_terminal_state": _require_nonempty_string(
                entry.get("last_terminal_state"),
                f"RECURRENCE_REGISTRY_LAST_TERMINAL_STATE_MISSING:{key}",
            ),
            "last_first_true_blocker_code": str(entry.get("last_first_true_blocker_code") or "").strip(),
        }
    validated = {
        **obj,
        "entries_by_fingerprint": normalized_entries,
    }
    validate_against_repo_schema_v1(validated, REPO_ROOT, SCHEMA_RELPATH_V1)
    return validated


def resolve_registry_status_after_v1(
    *,
    proof_status: str,
    blocker_family: str,
) -> str:
    normalized_proof_status = _require_nonempty_string(
        proof_status,
        "RECURRENCE_REGISTRY_PROOF_STATUS_MISSING",
    ).upper()
    family = _require_nonempty_string(
        blocker_family,
        "RECURRENCE_REGISTRY_BLOCKER_FAMILY_MISSING",
    ).upper()
    if normalized_proof_status == "RECURRENCE_SAFE":
        return "ELIMINATED"
    if normalized_proof_status == "MITIGATED_NOT_RECURRENCE_SAFE":
        return "CONTAINED"
    if normalized_proof_status == "INVALID_PROOF" and family == "SUCCESS_VERIFICATION":
        return "CONTAINED"
    return "OPEN"


def load_recurrence_registry_v1(*, truth_root: str | Path) -> dict[str, Any]:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    path = resolve_recurrence_registry_path(truth_root=root)
    if not path.exists() or not path.is_file():
        return build_empty_recurrence_registry_payload_v1(
            generated_at_utc=now_utc_iso_v1(),
            producer_module="constellation_2/common/recurrence_registry_v1.py",
        )
    ref = read_validated_surface_v1(path=path, schema_relpath=SCHEMA_RELPATH_V1)
    return validate_recurrence_registry_payload_v1(ref.payload)


def write_recurrence_registry_v1(*, truth_root: str | Path, payload: Mapping[str, Any]) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    path = resolve_recurrence_registry_path(truth_root=root)
    validated = validate_recurrence_registry_payload_v1(payload)
    return atomic_write_validated_json_v1(
        path=path,
        payload=validated,
        schema_relpath=SCHEMA_RELPATH_V1,
    )


def update_recurrence_registry_v1(
    *,
    truth_root: str | Path,
    fingerprint_record: Mapping[str, Any],
    proof_status: str,
    observed_at_utc: str,
    day_utc: str,
    day_attempt_id: str,
    release_id: str,
    git_sha: str,
    terminal_state: str,
    first_true_blocker_code: str,
    producer_module: str,
) -> tuple[str, str, SurfaceRefV1]:
    registry_payload = load_recurrence_registry_v1(truth_root=truth_root)
    validated_registry = validate_recurrence_registry_payload_v1(registry_payload)
    entries = dict(validated_registry.get("entries_by_fingerprint") or {})

    fingerprint = build_recurrence_fingerprint_record_v1(
        blocker_family=str(fingerprint_record.get("blocker_family") or "").strip(),
        blocker_code=str(fingerprint_record.get("blocker_code") or "").strip(),
        authority_source=str(fingerprint_record.get("authority_source") or "").strip(),
        stage_id=str(fingerprint_record.get("stage_id") or "").strip(),
    )
    recurrence_fingerprint = fingerprint["recurrence_fingerprint"]
    before_status = "UNSEEN"
    if recurrence_fingerprint in entries:
        before_status = _require_status(
            dict(entries[recurrence_fingerprint]).get("current_status"),
            f"RECURRENCE_REGISTRY_CURRENT_STATUS_MISSING:{recurrence_fingerprint}",
        )
    after_status = resolve_registry_status_after_v1(
        proof_status=proof_status,
        blocker_family=fingerprint["blocker_family"],
    )

    prior_entry = dict(entries.get(recurrence_fingerprint) or {})
    first_seen_at_utc = str(prior_entry.get("first_seen_at_utc") or "").strip() or observed_at_utc
    occurrence_count = int(prior_entry.get("occurrence_count") or 0) + 1
    entries[recurrence_fingerprint] = {
        **fingerprint,
        "first_seen_at_utc": first_seen_at_utc,
        "last_seen_at_utc": observed_at_utc,
        "occurrence_count": occurrence_count,
        "current_status": after_status,
        "last_day_utc": _require_nonempty_string(day_utc, "RECURRENCE_REGISTRY_DAY_UTC_MISSING"),
        "last_day_attempt_id": _require_nonempty_string(
            day_attempt_id,
            "RECURRENCE_REGISTRY_DAY_ATTEMPT_ID_MISSING",
        ),
        "last_release_id": _require_nonempty_string(
            release_id,
            "RECURRENCE_REGISTRY_RELEASE_ID_MISSING",
        ),
        "last_git_sha": _require_nonempty_string(
            git_sha,
            "RECURRENCE_REGISTRY_GIT_SHA_MISSING",
        ).lower(),
        "last_terminal_state": _require_nonempty_string(
            terminal_state,
            "RECURRENCE_REGISTRY_TERMINAL_STATE_MISSING",
        ),
        "last_first_true_blocker_code": str(first_true_blocker_code or "").strip(),
    }

    updated_payload = {
        **validated_registry,
        "generated_at_utc": observed_at_utc,
        "entries_by_fingerprint": dict(sorted(entries.items())),
        "producer": producer_block_v1(module=producer_module),
    }
    ref = write_recurrence_registry_v1(truth_root=truth_root, payload=updated_payload)
    return before_status, after_status, ref
