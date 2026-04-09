#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.current_system_projection_v1 import (
    build_current_system_projection_v1,
    write_current_system_projection_v1,
)
from constellation_2.common.execution_journal_v1 import (
    IDENTITY_FIELDS_V1,
    identity_tuple_from_mapping_v1,
    read_execution_journal_v1,
    require_matching_identity_tuple_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    now_utc_iso_v1,
    read_json_object_v1,
    read_paper_session_ledger_ref_v1,
    read_startup_materialization_ref_v1,
    read_startup_proof_validation_ref_v1,
    read_trading_day_state_machine_ref_v1,
    resolve_fact_plane_truth_root_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_current_system_projection_path,
    resolve_deployment_state_machine_path,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


DEPLOYMENT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/deployment_state_machine.v1.schema.json"
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


def _latest_events_by_type(journal_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for event in list(journal_payload.get("events") or []):
        if not isinstance(event, dict):
            continue
        rows[str(event.get("event_type") or "").strip()] = dict(event)
    return rows


def _source_artifact_row(*, logical_name: str, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "logical_name": logical_name,
        "path": str(path),
        "generated_at_utc": str(
            payload.get("evaluated_at_utc") or payload.get("produced_at_utc") or payload.get("generated_at_utc") or ""
        ).strip(),
        "sha256": sha256_file_v1(path),
        "status": "PRESENT",
        "producer_git_sha": str(dict(payload.get("producer") or {}).get("git_sha") or "").strip(),
    }


def _bound_source_artifact_row(
    *,
    logical_name: str,
    path: Path,
    payload: dict[str, Any],
    expected_identity: dict[str, str],
    events_by_type: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    base = _source_artifact_row(logical_name=logical_name, path=path, payload=payload)
    if logical_name == "execution_journal_v1":
        return {
            **base,
            "identity_tuple": dict(expected_identity),
            "identity_binding_source": "journal_self_identity",
            "identity_binding_event_type": "",
            "identity_binding_event_key": "",
        }
    binding_event: dict[str, Any] | None = None
    for event_type in SOURCE_EVENT_BINDINGS_V1[logical_name]:
        candidate = events_by_type.get(event_type)
        if not isinstance(candidate, dict):
            continue
        event_payload = dict(candidate.get("payload") or {})
        if str(event_payload.get("source_artifact_path") or "").strip() != str(path):
            continue
        if str(event_payload.get("source_artifact_sha256") or "").strip() != str(base["sha256"]):
            continue
        binding_event = candidate
        break
    if not isinstance(binding_event, dict):
        raise ValueError(f"CURRENT_SYSTEM_PROJECTION_EVENT_BINDING_NOT_FOUND:{logical_name}")
    bound_identity = identity_tuple_from_mapping_v1(binding_event, context=f"binding_event:{logical_name}")
    require_matching_identity_tuple_v1(
        expected_identity=expected_identity,
        candidate_identity=bound_identity,
        context=f"binding_event:{logical_name}",
    )
    if str(payload.get("day_utc") or "").strip() != str(expected_identity["day_utc"]):
        raise ValueError(f"CURRENT_SYSTEM_PROJECTION_NATIVE_IDENTITY_MISMATCH:{logical_name}:day_utc")
    if logical_name == "deployment_state_machine_v1":
        if str(payload.get("deployment_attempt_id") or "").strip() != str(expected_identity["pipeline_run_id"]):
            raise ValueError(
                "CURRENT_SYSTEM_PROJECTION_NATIVE_IDENTITY_MISMATCH:deployment_state_machine_v1:pipeline_run_id"
            )
        release_build = dict(payload.get("release_build") or {})
        if str(release_build.get("release_id") or "").strip() != str(expected_identity["release_id"]):
            raise ValueError(
                "CURRENT_SYSTEM_PROJECTION_NATIVE_IDENTITY_MISMATCH:deployment_state_machine_v1:release_id"
            )
    if logical_name == "trading_day_state_machine_v1":
        if str(payload.get("day_attempt_id") or "").strip() != str(expected_identity["day_attempt_id"]):
            raise ValueError(
                "CURRENT_SYSTEM_PROJECTION_NATIVE_IDENTITY_MISMATCH:trading_day_state_machine_v1:day_attempt_id"
            )
    return {
        **base,
        "identity_tuple": {field: str(bound_identity[field]) for field in IDENTITY_FIELDS_V1},
        "identity_binding_source": "execution_journal_v1",
        "identity_binding_event_type": str(binding_event.get("event_type") or "").strip(),
        "identity_binding_event_key": _require_nonempty_string(
            binding_event.get("event_key"),
            f"CURRENT_SYSTEM_PROJECTION_EVENT_BINDING_KEY_MISSING:{logical_name}",
        ),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_current_system_projection_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument(
        "--truth_root",
        default=str((REPO_ROOT / "constellation_2/runtime/truth").resolve()),
    )
    args = ap.parse_args(argv)

    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    day_utc = str(args.day_utc).strip()

    journal_ref = read_execution_journal_v1(truth_root=truth_root, day_utc=day_utc)
    expected_identity = identity_tuple_from_mapping_v1(dict(journal_ref.payload), context="execution_journal_v1")
    events_by_type = _latest_events_by_type(dict(journal_ref.payload))

    deployment_path = resolve_deployment_state_machine_path(truth_root=truth_root, day_utc=day_utc)
    deployment_payload = read_json_object_v1(deployment_path)
    validate_against_repo_schema_v1(deployment_payload, REPO_ROOT, DEPLOYMENT_SCHEMA_RELPATH)

    startup_ref = read_startup_materialization_ref_v1(truth_root=truth_root, day_utc=day_utc)
    startup_proof_ref = read_startup_proof_validation_ref_v1(truth_root=truth_root, day_utc=day_utc)
    ledger_ref = read_paper_session_ledger_ref_v1(truth_root=truth_root, day_utc=day_utc)
    trading_day_ref = read_trading_day_state_machine_ref_v1(truth_root=truth_root, day_utc=day_utc)

    source_artifacts = [
        _bound_source_artifact_row(
            logical_name="execution_journal_v1",
            path=journal_ref.path,
            payload=dict(journal_ref.payload),
            expected_identity=expected_identity,
            events_by_type=events_by_type,
        ),
        _bound_source_artifact_row(
            logical_name="deployment_state_machine_v1",
            path=deployment_path,
            payload=deployment_payload,
            expected_identity=expected_identity,
            events_by_type=events_by_type,
        ),
        _bound_source_artifact_row(
            logical_name="startup_materialization_v1",
            path=startup_ref.path,
            payload=dict(startup_ref.payload),
            expected_identity=expected_identity,
            events_by_type=events_by_type,
        ),
        _bound_source_artifact_row(
            logical_name="startup_proof_validation_v1",
            path=startup_proof_ref.path,
            payload=dict(startup_proof_ref.payload),
            expected_identity=expected_identity,
            events_by_type=events_by_type,
        ),
        _bound_source_artifact_row(
            logical_name="paper_session_ledger_v1",
            path=ledger_ref.path,
            payload=dict(ledger_ref.payload),
            expected_identity=expected_identity,
            events_by_type=events_by_type,
        ),
        _bound_source_artifact_row(
            logical_name="trading_day_state_machine_v1",
            path=trading_day_ref.path,
            payload=dict(trading_day_ref.payload),
            expected_identity=expected_identity,
            events_by_type=events_by_type,
        ),
    ]

    payload = build_current_system_projection_v1(
        day_utc=day_utc,
        journal_payload=dict(journal_ref.payload),
        source_artifacts=source_artifacts,
        deployment_payload=deployment_payload,
        startup_materialization_payload=dict(startup_ref.payload),
        startup_proof_payload=dict(startup_proof_ref.payload),
        ledger_payload=dict(ledger_ref.payload),
        trading_day_payload=dict(trading_day_ref.payload),
        generated_at_utc=now_utc_iso_v1(),
        producer_module="ops/tools/run_current_system_projection_v1.py",
    )
    ref = write_current_system_projection_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "report_path": str(ref.path),
                "projection_id": str(payload.get("projection_id") or ""),
                "first_true_blocker_code": str(payload.get("first_true_blocker_code") or ""),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
