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

from constellation_2.common.execution_journal_v1 import append_execution_event_v1, read_execution_journal_v1
from constellation_2.common.paper_session_fact_plane_v1 import (
    read_json_object_v1,
    read_paper_session_ledger_ref_v1,
    read_startup_materialization_ref_v1,
    read_startup_proof_validation_ref_v1,
    read_trading_day_state_machine_ref_v1,
    resolve_fact_plane_truth_root_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_deployment_state_machine_path,
    resolve_paper_session_ledger_path,
    resolve_startup_materialization_path,
    resolve_startup_proof_validation_path,
    resolve_trading_day_state_machine_path,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


DEPLOYMENT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/deployment_state_machine.v1.schema.json"


def _artifact_generated_at(payload: dict[str, Any]) -> str:
    return (
        str(payload.get("evaluated_at_utc") or "").strip()
        or str(payload.get("produced_at_utc") or "").strip()
        or str(payload.get("generated_at_utc") or "").strip()
    )


def _source_payload(*, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_artifact_path": str(path),
        "source_artifact_sha256": sha256_file_v1(path),
        "source_generated_at_utc": _artifact_generated_at(payload),
    }


def _load_deployment_payload(*, truth_root: Path, day_utc: str) -> tuple[Path, dict[str, Any]]:
    path = resolve_deployment_state_machine_path(truth_root=truth_root, day_utc=day_utc)
    payload = read_json_object_v1(path)
    validate_against_repo_schema_v1(payload, REPO_ROOT, DEPLOYMENT_SCHEMA_RELPATH)
    return path, payload


def _resolve_release_git_sha(deployment_payload: dict[str, Any]) -> str:
    active_release = deployment_payload.get("active_release")
    if not isinstance(active_release, dict):
        raise ValueError("EXECUTION_JOURNAL_ACTIVE_RELEASE_BLOCK_MISSING")
    target = str(active_release.get("active_symlink_target") or "").strip()
    if not target:
        raise ValueError("EXECUTION_JOURNAL_ACTIVE_RELEASE_TARGET_MISSING")
    manifest_path = (Path(target).resolve() / "release_manifest.v1.json").resolve()
    manifest = read_json_object_v1(manifest_path)
    git_sha = str(manifest.get("git_sha") or "").strip().lower()
    if len(git_sha) != 40:
        raise ValueError("EXECUTION_JOURNAL_RELEASE_MANIFEST_GIT_SHA_MISSING")
    return git_sha


def _iso_duration_ms(started_at_utc: str, ended_at_utc: str) -> int | None:
    from datetime import UTC, datetime

    started = str(started_at_utc or "").strip()
    ended = str(ended_at_utc or "").strip()
    if not started or not ended:
        return None
    try:
        started_dt = datetime.fromisoformat(started.replace("Z", "+00:00")).astimezone(UTC)
        ended_dt = datetime.fromisoformat(ended.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None
    if ended_dt < started_dt:
        return None
    return int((ended_dt - started_dt).total_seconds() * 1000)


def _contradictions(
    *,
    deployment_payload: dict[str, Any],
    startup_payload: dict[str, Any],
    startup_proof_payload: dict[str, Any],
    ledger_payload: dict[str, Any],
    trading_day_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    contradictions: list[dict[str, Any]] = []
    final_start_decision = str(trading_day_payload.get("final_start_decision") or "").strip().upper()
    deployment_status = str(deployment_payload.get("final_deployment_decision") or "").strip().upper()
    startup_status = str(startup_payload.get("status") or "").strip().upper()
    startup_proof_status = str(startup_proof_payload.get("status") or "").strip().upper()
    ledger_authority = str(
        dict(ledger_payload.get("control_state") or {}).get("authority_status") or ""
    ).strip().upper()
    if final_start_decision == "READY_NOW" and deployment_status != "DEPLOY_ACTIVE":
        contradictions.append(
            {
                "contradiction_code": "SYSTEM_CONTRADICTION_DEPLOYMENT_NOT_ACTIVE",
                "contradiction_details": "Trading day is READY_NOW while deployment is not DEPLOY_ACTIVE.",
                "source_artifact_paths": [
                    resolve_trading_day_state_machine_path(
                        truth_root=resolve_fact_plane_truth_root_v1(None),
                        day_utc=str(trading_day_payload.get("day_utc") or ""),
                    ).as_posix()
                ],
            }
        )
    if final_start_decision == "READY_NOW" and startup_status != "SUCCESS":
        contradictions.append(
            {
                "contradiction_code": "SYSTEM_CONTRADICTION_STARTUP_NOT_SUCCESS",
                "contradiction_details": "Trading day is READY_NOW while startup materialization is not SUCCESS.",
                "source_artifact_paths": [],
            }
        )
    if final_start_decision == "READY_NOW" and startup_proof_status != "STARTUP_READY":
        contradictions.append(
            {
                "contradiction_code": "SYSTEM_CONTRADICTION_STARTUP_PROOF_NOT_READY",
                "contradiction_details": "Trading day is READY_NOW while startup proof is not STARTUP_READY.",
                "source_artifact_paths": [],
            }
        )
    if final_start_decision == "READY_NOW" and ledger_authority != "GRANTED":
        contradictions.append(
            {
                "contradiction_code": "SYSTEM_CONTRADICTION_LEDGER_NOT_GRANTED",
                "contradiction_details": "Trading day is READY_NOW while ledger authority is not GRANTED.",
                "source_artifact_paths": [],
            }
        )
    return contradictions


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_execution_journal_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument(
        "--truth_root",
        default=str((REPO_ROOT / "constellation_2/runtime/truth").resolve()),
    )
    args = ap.parse_args(argv)

    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    day_utc = str(args.day_utc).strip()

    deployment_path, deployment_payload = _load_deployment_payload(truth_root=truth_root, day_utc=day_utc)
    startup_ref = read_startup_materialization_ref_v1(truth_root=truth_root, day_utc=day_utc)
    startup_proof_ref = read_startup_proof_validation_ref_v1(truth_root=truth_root, day_utc=day_utc)
    ledger_ref = read_paper_session_ledger_ref_v1(truth_root=truth_root, day_utc=day_utc)
    trading_day_ref = read_trading_day_state_machine_ref_v1(truth_root=truth_root, day_utc=day_utc)

    startup_payload = dict(startup_ref.payload)
    startup_proof_payload = dict(startup_proof_ref.payload)
    ledger_payload = dict(ledger_ref.payload)
    trading_day_payload = dict(trading_day_ref.payload)

    day_attempt_id = str(trading_day_payload.get("day_attempt_id") or "").strip()
    pipeline_run_id = str(deployment_payload.get("deployment_attempt_id") or "").strip()
    release_id = str(dict(deployment_payload.get("release_build") or {}).get("release_id") or "").strip()
    git_sha = _resolve_release_git_sha(deployment_payload)

    deployment_status = str(deployment_payload.get("final_deployment_decision") or "").strip().upper()
    deployment_event_type = "DEPLOYMENT_ACTIVATED" if deployment_status == "DEPLOY_ACTIVE" else "DEPLOYMENT_BLOCKED"
    append_execution_event_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
        event_type=deployment_event_type,
        event_source="deployment_state_machine_v1",
        status=deployment_status,
        payload={
            **_source_payload(path=deployment_path, payload=deployment_payload),
            "final_deployment_decision": deployment_status,
            "blocking_codes": list(deployment_payload.get("blocking_codes") or []),
            "first_true_blocker_code": str(deployment_payload.get("first_true_blocker_code") or "").strip(),
        },
        producer_module="ops/tools/run_execution_journal_v1.py",
        generated_at_utc=str(deployment_payload.get("evaluated_at_utc") or "").strip(),
    )

    append_execution_event_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
        event_type="STARTUP_MATERIALIZATION_COMPLETED",
        event_source="startup_materialization_v1",
        status=str(startup_payload.get("status") or "").strip().upper(),
        payload={
            **_source_payload(path=startup_ref.path, payload=startup_payload),
            "startup_status": str(startup_payload.get("status") or "").strip().upper(),
            "freshness_verdict": str(startup_payload.get("freshness_verdict") or "").strip().upper(),
            "linkage_verdict": str(startup_payload.get("linkage_verdict") or "").strip().upper(),
            "blocking_codes": list(startup_payload.get("blocking_codes") or []),
        },
        producer_module="ops/tools/run_execution_journal_v1.py",
        generated_at_utc=_artifact_generated_at(startup_payload),
    )

    append_execution_event_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
        event_type="STARTUP_PROOF_VALIDATION_COMPLETED",
        event_source="startup_proof_validation_v1",
        status=str(startup_proof_payload.get("status") or "").strip().upper(),
        payload={
            **_source_payload(path=startup_proof_ref.path, payload=startup_proof_payload),
            "startup_proof_status": str(startup_proof_payload.get("status") or "").strip().upper(),
            "ledger_authority_status": str(startup_proof_payload.get("ledger_authority_status") or "").strip().upper(),
            "blocking_codes": list(startup_proof_payload.get("blocking_codes") or []),
        },
        producer_module="ops/tools/run_execution_journal_v1.py",
        generated_at_utc=_artifact_generated_at(startup_proof_payload) or _artifact_generated_at(ledger_payload),
    )

    ledger_control = dict(ledger_payload.get("control_state") or {})
    append_execution_event_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
        event_type="LEDGER_AUTHORITY_RECORDED",
        event_source="paper_session_ledger_v1",
        status=str(ledger_control.get("authority_status") or "").strip().upper(),
        payload={
            **_source_payload(path=ledger_ref.path, payload=ledger_payload),
            "ledger_id": str(ledger_payload.get("ledger_id") or "").strip(),
            "authority_status": str(ledger_control.get("authority_status") or "").strip().upper(),
            "system_ready": bool(ledger_control.get("system_ready") is True),
            "submission_authorized": bool(ledger_control.get("submission_authorized") is True),
            "blocking_codes": list(ledger_control.get("blocking_codes") or []),
        },
        producer_module="ops/tools/run_execution_journal_v1.py",
        generated_at_utc=_artifact_generated_at(ledger_payload),
    )

    append_execution_event_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
        event_type="SUBMISSION_AUTHORIZATION_RECORDED",
        event_source="paper_session_ledger_v1",
        status="AUTHORIZED" if bool(ledger_control.get("submission_authorized") is True) else "NOT_AUTHORIZED",
        payload={
            **_source_payload(path=ledger_ref.path, payload=ledger_payload),
            "ledger_id": str(ledger_payload.get("ledger_id") or "").strip(),
            "submission_authorized": bool(ledger_control.get("submission_authorized") is True),
            "authority_status": str(ledger_control.get("authority_status") or "").strip().upper(),
        },
        producer_module="ops/tools/run_execution_journal_v1.py",
        generated_at_utc=_artifact_generated_at(ledger_payload),
    )

    first_true_blocker = dict(trading_day_payload.get("first_true_blocker") or {})
    append_execution_event_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        day_attempt_id=day_attempt_id,
        pipeline_run_id=pipeline_run_id,
        release_id=release_id,
        git_sha=git_sha,
        event_type="STATE_MACHINE_DECISION_RECORDED",
        event_source="trading_day_state_machine_v1",
        status=str(trading_day_payload.get("final_start_decision") or "").strip().upper(),
        payload={
            **_source_payload(path=trading_day_ref.path, payload=trading_day_payload),
            "state_machine_id": str(trading_day_payload.get("state_machine_id") or "").strip(),
            "final_start_decision": str(trading_day_payload.get("final_start_decision") or "").strip().upper(),
            "first_true_blocker_code": str(first_true_blocker.get("first_true_blocker_code") or "").strip(),
            "first_true_blocker_artifact_path": str(
                first_true_blocker.get("first_true_blocker_artifact_path") or ""
            ).strip(),
        },
        producer_module="ops/tools/run_execution_journal_v1.py",
        generated_at_utc=_artifact_generated_at(trading_day_payload),
    )

    stage_duration_ms = _iso_duration_ms(
        _artifact_generated_at(startup_payload),
        _artifact_generated_at(ledger_payload),
    )
    if stage_duration_ms is not None:
        append_execution_event_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            day_attempt_id=day_attempt_id,
            pipeline_run_id=pipeline_run_id,
            release_id=release_id,
            git_sha=git_sha,
            event_type="STAGE_DURATION_RECORDED",
            event_source="paper_session_startup_flow_v1",
            status="RECORDED",
            payload={
                **_source_payload(path=ledger_ref.path, payload=ledger_payload),
                "stage_name": "STARTUP_MATERIALIZATION_TO_LEDGER_ELAPSED",
                "started_at_utc": _artifact_generated_at(startup_payload),
                "ended_at_utc": _artifact_generated_at(ledger_payload),
                "duration_ms": stage_duration_ms,
            },
            producer_module="ops/tools/run_execution_journal_v1.py",
            generated_at_utc=_artifact_generated_at(ledger_payload),
        )

    for contradiction in _contradictions(
        deployment_payload=deployment_payload,
        startup_payload=startup_payload,
        startup_proof_payload=startup_proof_payload,
        ledger_payload=ledger_payload,
        trading_day_payload=trading_day_payload,
    ):
        append_execution_event_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            day_attempt_id=day_attempt_id,
            pipeline_run_id=pipeline_run_id,
            release_id=release_id,
            git_sha=git_sha,
            event_type="SYSTEM_CONTRADICTION_DETECTED",
            event_source="execution_journal_v1",
            status="CONTRADICTION",
            payload={
                "source_artifact_path": str(trading_day_ref.path),
                "source_artifact_sha256": trading_day_ref.sha256,
                "source_generated_at_utc": _artifact_generated_at(trading_day_payload),
                **contradiction,
            },
            producer_module="ops/tools/run_execution_journal_v1.py",
            generated_at_utc=_artifact_generated_at(trading_day_payload),
        )

    journal_ref = read_execution_journal_v1(truth_root=truth_root, day_utc=day_utc)
    print(
        json.dumps(
            {
                "report_path": str(journal_ref.path),
                "journal_id": str(journal_ref.payload.get("journal_id") or ""),
                "event_count": int(journal_ref.payload.get("last_event_seq") or 0),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
