from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.execution_journal_v1 import (
    IDENTITY_FIELDS_V1,
    identity_tuple_from_mapping_v1,
    require_matching_identity_tuple_v1,
    validate_execution_journal_payload_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    producer_block_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import resolve_performance_projection_path
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/performance_projection.v1.schema.json"


def _stable_id(seed: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(dict(seed))).hexdigest()[:16]


def _parse_iso_or_none(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _require_nonempty_string(value: Any, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(code)
    return text


def build_performance_projection_v1(
    *,
    journal_payload: Mapping[str, Any],
    journal_ref: str,
    journal_generated_at_utc: str,
    generated_at_utc: str,
    producer_module: str,
) -> dict[str, Any]:
    validated_journal = validate_execution_journal_payload_v1(journal_payload)
    expected_identity = identity_tuple_from_mapping_v1(validated_journal, context="execution_journal_v1")
    _require_nonempty_string(journal_ref, "PERFORMANCE_PROJECTION_JOURNAL_REF_MISSING")
    _require_nonempty_string(
        journal_generated_at_utc,
        "PERFORMANCE_PROJECTION_JOURNAL_GENERATED_AT_MISSING",
    )
    stage_rows: list[dict[str, Any]] = []
    stage_times: list[tuple[datetime, datetime]] = []
    for event in list(validated_journal.get("events") or []):
        if not isinstance(event, Mapping):
            continue
        if str(event.get("event_type") or "").strip() != "STAGE_DURATION_RECORDED":
            continue
        require_matching_identity_tuple_v1(
            expected_identity=expected_identity,
            candidate_identity=event,
            context=f"performance_event_seq={event.get('event_seq')}",
        )
        payload = dict(event.get("payload") or {})
        started_at = _require_nonempty_string(
            payload.get("started_at_utc"),
            "PERFORMANCE_PROJECTION_STAGE_STARTED_AT_MISSING",
        )
        ended_at = _require_nonempty_string(
            payload.get("ended_at_utc"),
            "PERFORMANCE_PROJECTION_STAGE_ENDED_AT_MISSING",
        )
        duration_value = payload.get("duration_ms")
        if not isinstance(duration_value, int) or duration_value < 0:
            raise ValueError("PERFORMANCE_PROJECTION_STAGE_DURATION_INVALID")
        duration_ms = duration_value
        stage_name = _require_nonempty_string(
            payload.get("stage_name"),
            "PERFORMANCE_PROJECTION_STAGE_NAME_MISSING",
        )
        status = _require_nonempty_string(
            event.get("status"),
            "PERFORMANCE_PROJECTION_STAGE_STATUS_MISSING",
        )
        row = {
            "stage_name": stage_name,
            "status": status,
            "started_at_utc": started_at,
            "ended_at_utc": ended_at,
            "duration_ms": duration_ms,
            "source_event_seq": int(event.get("event_seq") or 0),
        }
        stage_rows.append(row)
        start_dt = _parse_iso_or_none(started_at)
        end_dt = _parse_iso_or_none(ended_at)
        if start_dt is not None and end_dt is not None and end_dt >= start_dt:
            stage_times.append((start_dt, end_dt))

    overall_wall_time_ms: int | None = None
    if stage_times:
        overall_wall_time_ms = int((max(item[1] for item in stage_times) - min(item[0] for item in stage_times)).total_seconds() * 1000)

    trend_summary = (
        {
            "status": "INSUFFICIENT_STAGE_DATA",
            "summary": "No stage duration events are present in the execution journal.",
            "history_points": 0,
        }
        if not stage_rows
        else {
            "status": "CURRENT_DAY_ONLY",
            "summary": "Stage timing is available for the current day only. No historical comparison is applied.",
            "history_points": len(stage_rows),
        }
    )

    payload = {
        "schema_id": "performance_projection",
        "schema_version": "v1",
        "authority_scope": "DERIVED_ONLY_PERFORMANCE_PROJECTION",
        "day_utc": str(expected_identity["day_utc"]),
        "projection_id": "",
        "generated_at_utc": str(generated_at_utc).strip(),
        "journal_ref": str(journal_ref).strip(),
        "journal_id": str(validated_journal.get("journal_id") or "").strip(),
        "overall_wall_time_ms": overall_wall_time_ms,
        "per_stage_durations": stage_rows,
        "trend_summary": trend_summary,
        "source_artifacts": [
            {
                "logical_name": "execution_journal_v1",
                "path": str(journal_ref).strip(),
                "generated_at_utc": str(journal_generated_at_utc).strip(),
                "status": "PRESENT",
            }
        ],
        "informational_only_notice": (
            "Informational timing projection only. "
            "This surface does not own readiness, blocker precedence, or control-plane authority."
        ),
        "producer": producer_block_v1(
            module=producer_module,
            git_sha=str(expected_identity["git_sha"]),
        ),
    }
    payload["projection_id"] = f"performance_projection:{payload['day_utc']}:{_stable_id(payload)}"
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    return payload


def write_performance_projection_v1(*, truth_root: str | Path, payload: Mapping[str, Any]) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    return atomic_write_validated_json_v1(
        path=resolve_performance_projection_path(truth_root=root, day_utc=str(payload["day_utc"])),
        payload=dict(payload),
        schema_relpath=SCHEMA_RELPATH_V1,
    )
