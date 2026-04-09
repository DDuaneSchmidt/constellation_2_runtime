from __future__ import annotations

import hashlib
import json
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
    now_utc_iso_v1,
    producer_block_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_current_system_projection_path,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/current_system_projection.v1.schema.json"
REQUIRED_EVENT_TYPES_V1 = (
    "STARTUP_MATERIALIZATION_COMPLETED",
    "STARTUP_PROOF_VALIDATION_COMPLETED",
    "LEDGER_AUTHORITY_RECORDED",
    "STATE_MACHINE_DECISION_RECORDED",
)
REQUIRED_SOURCE_ARTIFACTS_V1 = (
    "execution_journal_v1",
    "deployment_state_machine_v1",
    "startup_materialization_v1",
    "startup_proof_validation_v1",
    "paper_session_ledger_v1",
    "trading_day_state_machine_v1",
)
SOURCE_EVENT_BINDINGS_V1 = {
    "deployment_state_machine_v1": ("DEPLOYMENT_ACTIVATED", "DEPLOYMENT_BLOCKED"),
    "startup_materialization_v1": ("STARTUP_MATERIALIZATION_COMPLETED",),
    "startup_proof_validation_v1": ("STARTUP_PROOF_VALIDATION_COMPLETED",),
    "paper_session_ledger_v1": ("LEDGER_AUTHORITY_RECORDED",),
    "trading_day_state_machine_v1": ("STATE_MACHINE_DECISION_RECORDED",),
}


def _require_nonempty_string(value: Any, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(code)
    return text


def _require_mapping(value: Any, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(code)
    return value


def _source_artifacts_by_name(source_artifacts: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in source_artifacts:
        if not isinstance(row, Mapping):
            raise ValueError("CURRENT_SYSTEM_PROJECTION_SOURCE_ARTIFACT_ROW_INVALID")
        logical_name = _require_nonempty_string(
            row.get("logical_name"),
            "CURRENT_SYSTEM_PROJECTION_SOURCE_ARTIFACT_LOGICAL_NAME_MISSING",
        )
        rows[logical_name] = dict(row)
    return rows


def _require_event(events_by_type: Mapping[str, dict[str, Any]], event_type: str) -> dict[str, Any]:
    event = events_by_type.get(event_type)
    if not isinstance(event, dict):
        raise ValueError(f"CURRENT_SYSTEM_PROJECTION_REQUIRED_EVENT_MISSING:{event_type}")
    return event


def _stable_id(seed: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(dict(seed), sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:16]


def _latest_events_by_type(journal_payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for event in list(journal_payload.get("events") or []):
        if not isinstance(event, dict):
            continue
        rows[str(event.get("event_type") or "").strip()] = dict(event)
    return rows


def _required_native_identity_subset(
    *,
    logical_name: str,
    payload: Mapping[str, Any],
    expected_identity: Mapping[str, str],
) -> None:
    if str(payload.get("day_utc") or "").strip() != str(expected_identity["day_utc"]):
        raise ValueError(f"CURRENT_SYSTEM_PROJECTION_NATIVE_IDENTITY_MISMATCH:{logical_name}:day_utc")
    if logical_name == "deployment_state_machine_v1":
        if str(payload.get("deployment_attempt_id") or "").strip() != str(expected_identity["pipeline_run_id"]):
            raise ValueError(
                "CURRENT_SYSTEM_PROJECTION_NATIVE_IDENTITY_MISMATCH:deployment_state_machine_v1:pipeline_run_id"
            )
        release_build = _require_mapping(
            payload.get("release_build"),
            "CURRENT_SYSTEM_PROJECTION_NATIVE_IDENTITY_MISSING:deployment_state_machine_v1:release_build",
        )
        if str(release_build.get("release_id") or "").strip() != str(expected_identity["release_id"]):
            raise ValueError(
                "CURRENT_SYSTEM_PROJECTION_NATIVE_IDENTITY_MISMATCH:deployment_state_machine_v1:release_id"
            )
        return
    if logical_name == "trading_day_state_machine_v1":
        if str(payload.get("day_attempt_id") or "").strip() != str(expected_identity["day_attempt_id"]):
            raise ValueError(
                "CURRENT_SYSTEM_PROJECTION_NATIVE_IDENTITY_MISMATCH:trading_day_state_machine_v1:day_attempt_id"
            )


def _validate_bound_source_artifact(
    *,
    logical_name: str,
    artifact_row: Mapping[str, Any],
    expected_identity: Mapping[str, str],
    events_by_type: Mapping[str, dict[str, Any]],
) -> None:
    _require_nonempty_string(
        artifact_row.get("path"),
        f"CURRENT_SYSTEM_PROJECTION_SOURCE_ARTIFACT_PATH_MISSING:{logical_name}",
    )
    _require_nonempty_string(
        artifact_row.get("sha256"),
        f"CURRENT_SYSTEM_PROJECTION_SOURCE_ARTIFACT_SHA256_MISSING:{logical_name}",
    )
    bound_identity = identity_tuple_from_mapping_v1(
        _require_mapping(
            artifact_row.get("identity_tuple"),
            f"CURRENT_SYSTEM_PROJECTION_IDENTITY_TUPLE_MISSING:{logical_name}",
        ),
        context=f"source_artifact:{logical_name}",
    )
    require_matching_identity_tuple_v1(
        expected_identity=expected_identity,
        candidate_identity=bound_identity,
        context=f"source_artifact:{logical_name}",
    )
    if logical_name == "execution_journal_v1":
        return
    allowed_event_types = SOURCE_EVENT_BINDINGS_V1.get(logical_name)
    if not isinstance(allowed_event_types, tuple):
        raise ValueError(f"CURRENT_SYSTEM_PROJECTION_EVENT_BINDING_CONFIG_MISSING:{logical_name}")
    binding_event_type = _require_nonempty_string(
        artifact_row.get("identity_binding_event_type"),
        f"CURRENT_SYSTEM_PROJECTION_EVENT_BINDING_TYPE_MISSING:{logical_name}",
    )
    if binding_event_type not in allowed_event_types:
        raise ValueError(f"CURRENT_SYSTEM_PROJECTION_EVENT_BINDING_TYPE_INVALID:{logical_name}")
    binding_event = _require_event(events_by_type, binding_event_type)
    if str(binding_event.get("event_key") or "").strip() != str(artifact_row.get("identity_binding_event_key") or "").strip():
        raise ValueError(f"CURRENT_SYSTEM_PROJECTION_EVENT_BINDING_KEY_MISMATCH:{logical_name}")
    require_matching_identity_tuple_v1(
        expected_identity=expected_identity,
        candidate_identity=binding_event,
        context=f"source_binding_event:{logical_name}",
    )
    binding_payload = dict(binding_event.get("payload") or {})
    if str(binding_payload.get("source_artifact_path") or "").strip() != str(artifact_row.get("path") or "").strip():
        raise ValueError(f"CURRENT_SYSTEM_PROJECTION_EVENT_BINDING_PATH_MISMATCH:{logical_name}")


def _derived_contradictions(
    *,
    deployment_status: str,
    startup_materialization_status: str,
    startup_proof_status: str,
    ledger_authority_status: str,
    submission_authorized: bool,
    final_start_decision: str,
) -> list[dict[str, Any]]:
    contradictions: list[dict[str, Any]] = []
    if final_start_decision == "READY_NOW" and deployment_status != "DEPLOY_ACTIVE":
        contradictions.append(
            {
                "contradiction_code": "CURRENT_SYSTEM_PROJECTION_DEPLOYMENT_NOT_ACTIVE",
                "summary": "Trading day is READY_NOW while deployment is not DEPLOY_ACTIVE.",
                "source_paths": [],
            }
        )
    if final_start_decision == "READY_NOW" and startup_materialization_status != "SUCCESS":
        contradictions.append(
            {
                "contradiction_code": "CURRENT_SYSTEM_PROJECTION_STARTUP_MATERIALIZATION_NOT_SUCCESS",
                "summary": "Trading day is READY_NOW while startup materialization is not SUCCESS.",
                "source_paths": [],
            }
        )
    if final_start_decision == "READY_NOW" and startup_proof_status != "STARTUP_READY":
        contradictions.append(
            {
                "contradiction_code": "CURRENT_SYSTEM_PROJECTION_STARTUP_PROOF_NOT_READY",
                "summary": "Trading day is READY_NOW while startup proof is not STARTUP_READY.",
                "source_paths": [],
            }
        )
    if final_start_decision == "READY_NOW" and ledger_authority_status != "GRANTED":
        contradictions.append(
            {
                "contradiction_code": "CURRENT_SYSTEM_PROJECTION_LEDGER_NOT_GRANTED",
                "summary": "Trading day is READY_NOW while ledger authority is not GRANTED.",
                "source_paths": [],
            }
        )
    if startup_proof_status == "STARTUP_READY" and ledger_authority_status != "GRANTED":
        contradictions.append(
            {
                "contradiction_code": "CURRENT_SYSTEM_PROJECTION_PROOF_LEDGER_MISMATCH",
                "summary": "Startup proof is STARTUP_READY while ledger authority is not GRANTED.",
                "source_paths": [],
            }
        )
    if final_start_decision == "READY_NOW" and not submission_authorized:
        contradictions.append(
            {
                "contradiction_code": "CURRENT_SYSTEM_PROJECTION_SUBMISSION_NOT_AUTHORIZED",
                "summary": "Trading day is READY_NOW while submission is not authorized.",
                "source_paths": [],
            }
        )
    return contradictions


def _operator_action_summary(*, blocker_code: str, contradiction_status: str, deployment_status: str, final_start_decision: str) -> str:
    if contradiction_status == "PRESENT":
        return "Investigate contradiction and reconcile authoritative artifacts before operator action."
    if blocker_code:
        return f"Resolve first true blocker: {blocker_code}."
    if deployment_status != "DEPLOY_ACTIVE":
        return "Deploy the current authoritative release before treating the day as active."
    if final_start_decision == "READY_NOW":
        return "No operator action required. Current authoritative deployment and day-start state are healthy."
    return "Await authoritative control-plane completion."


def build_current_system_projection_v1(
    *,
    day_utc: str,
    journal_payload: Mapping[str, Any],
    source_artifacts: list[dict[str, Any]],
    deployment_payload: Mapping[str, Any],
    startup_materialization_payload: Mapping[str, Any],
    startup_proof_payload: Mapping[str, Any],
    ledger_payload: Mapping[str, Any],
    trading_day_payload: Mapping[str, Any],
    generated_at_utc: str,
    producer_module: str,
) -> dict[str, Any]:
    validated_journal = validate_execution_journal_payload_v1(journal_payload)
    expected_identity = identity_tuple_from_mapping_v1(validated_journal, context="execution_journal_v1")
    source_artifact_rows = _source_artifacts_by_name(source_artifacts)
    for logical_name in REQUIRED_SOURCE_ARTIFACTS_V1:
        if logical_name not in source_artifact_rows:
            raise ValueError(f"CURRENT_SYSTEM_PROJECTION_REQUIRED_SOURCE_ARTIFACT_MISSING:{logical_name}")
    _required_native_identity_subset(
        logical_name="deployment_state_machine_v1",
        payload=deployment_payload,
        expected_identity=expected_identity,
    )
    _required_native_identity_subset(
        logical_name="startup_materialization_v1",
        payload=startup_materialization_payload,
        expected_identity=expected_identity,
    )
    _required_native_identity_subset(
        logical_name="startup_proof_validation_v1",
        payload=startup_proof_payload,
        expected_identity=expected_identity,
    )
    _required_native_identity_subset(
        logical_name="paper_session_ledger_v1",
        payload=ledger_payload,
        expected_identity=expected_identity,
    )
    _required_native_identity_subset(
        logical_name="trading_day_state_machine_v1",
        payload=trading_day_payload,
        expected_identity=expected_identity,
    )

    events_by_type = _latest_events_by_type(validated_journal)
    for required in REQUIRED_EVENT_TYPES_V1:
        _require_event(events_by_type, required)
    for logical_name, row in source_artifact_rows.items():
        _validate_bound_source_artifact(
            logical_name=logical_name,
            artifact_row=row,
            expected_identity=expected_identity,
            events_by_type=events_by_type,
        )

    deployment_event = events_by_type.get("DEPLOYMENT_ACTIVATED") or events_by_type.get("DEPLOYMENT_BLOCKED")
    if not isinstance(deployment_event, dict):
        raise ValueError("CURRENT_SYSTEM_PROJECTION_REQUIRED_EVENT_MISSING:DEPLOYMENT_ACTIVATED_OR_BLOCKED")

    startup_event = _require_event(events_by_type, "STARTUP_MATERIALIZATION_COMPLETED")
    startup_proof_event = _require_event(events_by_type, "STARTUP_PROOF_VALIDATION_COMPLETED")
    ledger_event = _require_event(events_by_type, "LEDGER_AUTHORITY_RECORDED")
    submission_event = events_by_type.get("SUBMISSION_AUTHORIZATION_RECORDED") or {}
    state_machine_event = _require_event(events_by_type, "STATE_MACHINE_DECISION_RECORDED")

    deployment_status = str(deployment_event.get("status") or "").strip()
    startup_materialization_status = str(startup_event.get("status") or "").strip()
    startup_proof_status = str(startup_proof_event.get("status") or "").strip()
    ledger_authority_status = str(ledger_event.get("status") or "").strip()
    submission_authorized = bool(dict(submission_event.get("payload") or {}).get("submission_authorized") is True)
    final_start_decision = str(state_machine_event.get("status") or "").strip()

    contradiction_events = [
        {
            "contradiction_code": str(dict(event.get("payload") or {}).get("contradiction_code") or "").strip(),
            "summary": str(dict(event.get("payload") or {}).get("contradiction_details") or "").strip(),
            "source_paths": list(dict(event.get("payload") or {}).get("source_artifact_paths") or []),
        }
        for event in list(journal_payload.get("events") or [])
        if isinstance(event, dict)
        if str(event.get("event_type") or "").strip() == "SYSTEM_CONTRADICTION_DETECTED"
    ]
    contradiction_events.extend(
        _derived_contradictions(
            deployment_status=deployment_status,
            startup_materialization_status=startup_materialization_status,
            startup_proof_status=startup_proof_status,
            ledger_authority_status=ledger_authority_status,
            submission_authorized=submission_authorized,
            final_start_decision=final_start_decision,
        )
    )

    contradiction_status = "PRESENT" if contradiction_events else "NONE"
    authoritative_blocker = str(dict(state_machine_event.get("payload") or {}).get("first_true_blocker_code") or "").strip()
    deployment_blocker = str(dict(deployment_event.get("payload") or {}).get("first_true_blocker_code") or "").strip()
    contradiction_blocker = str(contradiction_events[0]["contradiction_code"]).strip() if contradiction_events else ""
    first_true_blocker_code = authoritative_blocker or deployment_blocker or contradiction_blocker
    first_true_blocker_source = (
        str(dict(state_machine_event.get("payload") or {}).get("source_artifact_path") or "").strip()
        or str(dict(deployment_event.get("payload") or {}).get("source_artifact_path") or "").strip()
        or "execution_journal_v1"
    )
    missing_required_event_types = sorted(
        required for required in REQUIRED_EVENT_TYPES_V1 if required not in events_by_type
    )
    operator_action_required = bool(first_true_blocker_code) or contradiction_status == "PRESENT"
    payload = {
        "schema_id": "current_system_projection",
        "schema_version": "v1",
        "authority_scope": "DERIVED_ONLY_CURRENT_SYSTEM_PROJECTION",
        "day_utc": str(day_utc).strip(),
        "projection_id": "",
        "generated_at_utc": str(generated_at_utc).strip(),
        "journal_ref": str(source_artifacts[0]["path"]) if source_artifacts else "",
        "journal_id": str(validated_journal.get("journal_id") or "").strip(),
        "journal_event_count": len(list(validated_journal.get("events") or [])),
        "day_attempt_id": str(expected_identity["day_attempt_id"]),
        "pipeline_run_id": str(expected_identity["pipeline_run_id"]),
        "release_id": str(expected_identity["release_id"]),
        "git_sha": str(expected_identity["git_sha"]),
        "current_deployment_status": deployment_status,
        "current_startup_status": startup_materialization_status,
        "current_startup_proof_status": startup_proof_status,
        "current_ledger_status": ledger_authority_status,
        "current_submission_status": "AUTHORIZED" if submission_authorized else "NOT_AUTHORIZED",
        "authoritative_day_start_decision": final_start_decision,
        "first_true_blocker_code": first_true_blocker_code,
        "first_true_blocker_source": first_true_blocker_source,
        "operator_action_required": operator_action_required,
        "operator_action_summary": _operator_action_summary(
            blocker_code=first_true_blocker_code,
            contradiction_status=contradiction_status,
            deployment_status=deployment_status,
            final_start_decision=final_start_decision,
        ),
        "contradiction_status": contradiction_status,
        "contradictions": contradiction_events,
        "operational_evidence": {
            "journal_status": "PRESENT",
            "required_event_types_present": len(missing_required_event_types) == 0,
            "missing_required_event_types": missing_required_event_types,
            "source_artifact_count": len(source_artifacts),
            "source_artifacts_complete": all(str(item.get("path") or "").strip() for item in source_artifacts),
        },
        "source_artifacts": [dict(source_artifact_rows[name]) for name in REQUIRED_SOURCE_ARTIFACTS_V1],
        "source_generated_at_utc": {
            "deployment_state_machine_v1": str(
                deployment_payload.get("evaluated_at_utc") or ""
            ).strip(),
            "startup_materialization_v1": str(
                startup_materialization_payload.get("produced_at_utc") or ""
            ).strip(),
            "startup_proof_validation_v1": str(
                startup_proof_payload.get("generated_at_utc") or ""
            ).strip(),
            "paper_session_ledger_v1": str(
                ledger_payload.get("evaluated_at_utc") or ""
            ).strip(),
            "trading_day_state_machine_v1": str(
                trading_day_payload.get("evaluated_at_utc") or ""
            ).strip(),
        },
        "non_authority_notice": (
            "Derived from execution_journal_v1 plus authoritative final snapshots only. "
            "Do not treat this projection as an independent decision authority."
        ),
        "producer": producer_block_v1(module=producer_module, git_sha=str(expected_identity["git_sha"])),
    }
    payload["projection_id"] = f"current_system_projection:{payload['day_utc']}:{_stable_id(payload)}"
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    return payload


def write_current_system_projection_v1(
    *,
    truth_root: str | Path,
    payload: Mapping[str, Any],
) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    return atomic_write_validated_json_v1(
        path=resolve_current_system_projection_path(truth_root=root, day_utc=str(payload["day_utc"])),
        payload=dict(payload),
        schema_relpath=SCHEMA_RELPATH_V1,
    )
