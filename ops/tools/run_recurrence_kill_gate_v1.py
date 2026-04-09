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

from constellation_2.common.execution_journal_v1 import (
    execution_identity_tuple_v1,
    identity_tuple_from_mapping_v1,
    read_execution_journal_v1,
    require_matching_identity_tuple_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    now_utc_iso_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_deployment_state_machine_path,
    resolve_trading_day_state_machine_path,
)
from constellation_2.common.recurrence_fingerprint_v1 import (
    build_recurrence_fingerprint_record_v1,
)
from constellation_2.common.recurrence_kill_gate_v1 import (
    ACTIVE_POINTER,
    ACTIVE_RUNTIME_CONTRACT_PATH,
    DEPLOYMENT_SCHEMA_RELPATH_V1,
    EXPECTED_LIVE_ENTRYPOINT,
    LIVE_SERVICE_NAME,
    TRADING_DAY_SCHEMA_RELPATH_V1,
    build_authoritative_source_row_v1,
    build_recurrence_kill_gate_payload_v1,
    classify_blocker_family_v1,
    determine_terminal_state_v1,
    live_day_utc_v1,
    read_installed_service_text_v1,
    verify_live_entrypoint_v1,
    verify_rerun_idempotency_v1,
    write_recurrence_kill_gate_v1,
)
from constellation_2.common.recurrence_registry_v1 import (
    update_recurrence_registry_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


def _load_active_release_manifest(active_symlink_target: Path) -> dict[str, Any]:
    manifest_path = (active_symlink_target / "release_manifest.v1.json").resolve()
    return read_json_object_v1(manifest_path)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_recurrence_kill_gate_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument(
        "--truth_root",
        default=str((REPO_ROOT / "constellation_2/runtime/truth").resolve()),
    )
    args = ap.parse_args(argv)

    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    day_utc = str(args.day_utc).strip()
    generated_at_utc = now_utc_iso_v1()

    deployment_path = resolve_deployment_state_machine_path(truth_root=truth_root, day_utc=day_utc)
    deployment_payload = read_json_object_v1(deployment_path)
    validate_against_repo_schema_v1(deployment_payload, REPO_ROOT, DEPLOYMENT_SCHEMA_RELPATH_V1)

    trading_day_path = resolve_trading_day_state_machine_path(truth_root=truth_root, day_utc=day_utc)
    trading_day_payload = read_json_object_v1(trading_day_path)
    validate_against_repo_schema_v1(trading_day_payload, REPO_ROOT, TRADING_DAY_SCHEMA_RELPATH_V1)

    deployment_release = dict(deployment_payload.get("release_build") or {})
    active_symlink_target = ACTIVE_POINTER.resolve()
    active_runtime_contract = read_json_object_v1(ACTIVE_RUNTIME_CONTRACT_PATH)
    active_release_manifest = _load_active_release_manifest(active_symlink_target)

    identity = execution_identity_tuple_v1(
        day_utc=day_utc,
        day_attempt_id=str(trading_day_payload.get("day_attempt_id") or "").strip(),
        pipeline_run_id=str(deployment_payload.get("deployment_attempt_id") or "").strip(),
        release_id=str(deployment_release.get("release_id") or "").strip(),
        git_sha=str(active_release_manifest.get("git_sha") or "").strip().lower(),
    )
    proof_failures: list[str] = []

    if str(deployment_payload.get("day_utc") or "").strip() != day_utc:
        proof_failures.append("IDENTITY_MISMATCH:deployment_state_machine_v1:day_utc")
    if str(trading_day_payload.get("day_utc") or "").strip() != day_utc:
        proof_failures.append("IDENTITY_MISMATCH:trading_day_state_machine_v1:day_utc")
    if str(active_release_manifest.get("release_id") or "").strip() != str(identity["release_id"]):
        proof_failures.append("IDENTITY_MISMATCH:active_release_manifest:release_id")
    if str(active_runtime_contract.get("release_id") or "").strip() != str(identity["release_id"]):
        proof_failures.append("IDENTITY_MISMATCH:active_runtime_contract:release_id")
    if str(active_runtime_contract.get("git_sha") or "").strip().lower() != str(identity["git_sha"]):
        proof_failures.append("IDENTITY_MISMATCH:active_runtime_contract:git_sha")

    journal_ref = None
    try:
        journal_ref = read_execution_journal_v1(truth_root=truth_root, day_utc=day_utc)
        journal_identity = identity_tuple_from_mapping_v1(dict(journal_ref.payload), context="execution_journal_v1")
        require_matching_identity_tuple_v1(
            expected_identity=identity,
            candidate_identity=journal_identity,
            context="execution_journal_v1",
        )
    except Exception:
        proof_failures.append("IDENTITY_MISMATCH:execution_journal_v1")

    service_text = read_installed_service_text_v1(service_name=LIVE_SERVICE_NAME)
    active_entrypoint_path = (active_symlink_target / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh").resolve()
    live_entrypoint_verification = verify_live_entrypoint_v1(
        service_text=service_text,
        active_symlink_target=str(active_symlink_target),
        active_runtime_contract=active_runtime_contract,
        active_release_manifest=active_release_manifest,
        active_entrypoint_path=active_entrypoint_path,
    )

    terminal = determine_terminal_state_v1(
        deployment_payload=deployment_payload,
        trading_day_payload=trading_day_payload,
    )
    proof_failures.extend(list(live_entrypoint_verification.get("proof_failures") or []))
    live_day_value = live_day_utc_v1()
    if day_utc != live_day_value:
        proof_failures.append("TARGET_DAY_NOT_LIVE_CURRENT_DAY")

    rerun_idempotency = {
        "rerun_idempotent": False,
        "reason_code": "RERUN_IDEMPOTENCY_NOT_EXECUTED",
        "before_signature": {},
        "after_signature": {},
        "runner_exit_code": -1,
        "runner_stdout": "",
        "runner_stderr": "",
    }
    if not proof_failures and journal_ref is not None:
        rerun_idempotency = verify_rerun_idempotency_v1(
            day_utc=day_utc,
            truth_root=truth_root,
            runner_path=(REPO_ROOT / "ops/tools/run_execution_journal_v1.py").resolve(),
        )

    blocker_family = classify_blocker_family_v1(
        proof_failures=proof_failures
        + ([] if bool(rerun_idempotency.get("rerun_idempotent") is True) else [str(rerun_idempotency.get("reason_code") or "").strip()]),
        first_true_blocker_code=str(terminal.get("first_true_blocker_code") or "").strip(),
        terminal_state_source=str(terminal.get("terminal_state_source") or "").strip(),
    )
    blocker_code = (
        sorted(set(proof_failures))[0]
        if proof_failures
        else (str(terminal.get("first_true_blocker_code") or "").strip() or "NO_BLOCKER")
    )
    fingerprint_record = build_recurrence_fingerprint_record_v1(
        blocker_family=blocker_family,
        blocker_code=blocker_code,
        authority_source=str(terminal.get("terminal_state_source") or "").strip(),
        stage_id=str(terminal.get("stage_id") or "").strip(),
    )

    preliminary_payload = build_recurrence_kill_gate_payload_v1(
        day_utc=day_utc,
        identity=identity,
        live_entrypoint_verification=live_entrypoint_verification,
        live_day_utc_value=live_day_value,
        deployment_payload=deployment_payload,
        trading_day_payload=trading_day_payload,
        rerun_idempotency=rerun_idempotency,
        fingerprint_record=fingerprint_record,
        recurrence_status_before="UNSEEN",
        recurrence_status_after="OPEN",
        additional_proof_failures=proof_failures,
        authoritative_sources=[
            build_authoritative_source_row_v1(
                logical_name="deployment_state_machine_v1",
                path=deployment_path,
                payload=deployment_payload,
                status=str(deployment_payload.get("final_deployment_decision") or "").strip(),
            ),
            build_authoritative_source_row_v1(
                logical_name="trading_day_state_machine_v1",
                path=trading_day_path,
                payload=trading_day_payload,
                status=str(trading_day_payload.get("final_start_decision") or "").strip(),
            ),
            build_authoritative_source_row_v1(
                logical_name="active_runtime_contract_v1",
                path=ACTIVE_RUNTIME_CONTRACT_PATH,
                payload=active_runtime_contract,
                status=str(active_runtime_contract.get("status") or "").strip(),
            ),
        ]
        + (
            [
                build_authoritative_source_row_v1(
                    logical_name="execution_journal_v1",
                    path=journal_ref.path,
                    payload=dict(journal_ref.payload),
                    status="CHRONOLOGY_PRESENT",
                )
            ]
            if journal_ref is not None
            else []
        ),
        generated_at_utc=generated_at_utc,
        producer_module="ops/tools/run_recurrence_kill_gate_v1.py",
    )

    before_status, after_status, registry_ref = update_recurrence_registry_v1(
        truth_root=truth_root,
        fingerprint_record=fingerprint_record,
        proof_status=str(preliminary_payload.get("proof_status") or "").strip(),
        observed_at_utc=generated_at_utc,
        day_utc=day_utc,
        day_attempt_id=str(identity.get("day_attempt_id") or "").strip(),
        release_id=str(identity.get("release_id") or "").strip(),
        git_sha=str(identity.get("git_sha") or "").strip(),
        terminal_state=str(preliminary_payload.get("terminal_state") or "").strip(),
        first_true_blocker_code=str(preliminary_payload.get("first_true_blocker_code") or "").strip(),
        producer_module="ops/tools/run_recurrence_kill_gate_v1.py",
    )

    payload = build_recurrence_kill_gate_payload_v1(
        day_utc=day_utc,
        identity=identity,
        live_entrypoint_verification=live_entrypoint_verification,
        live_day_utc_value=live_day_value,
        deployment_payload=deployment_payload,
        trading_day_payload=trading_day_payload,
        rerun_idempotency=rerun_idempotency,
        fingerprint_record=fingerprint_record,
        recurrence_status_before=before_status,
        recurrence_status_after=after_status,
        additional_proof_failures=proof_failures,
        authoritative_sources=[
            build_authoritative_source_row_v1(
                logical_name="deployment_state_machine_v1",
                path=deployment_path,
                payload=deployment_payload,
                status=str(deployment_payload.get("final_deployment_decision") or "").strip(),
            ),
            build_authoritative_source_row_v1(
                logical_name="trading_day_state_machine_v1",
                path=trading_day_path,
                payload=trading_day_payload,
                status=str(trading_day_payload.get("final_start_decision") or "").strip(),
            ),
            build_authoritative_source_row_v1(
                logical_name="active_runtime_contract_v1",
                path=ACTIVE_RUNTIME_CONTRACT_PATH,
                payload=active_runtime_contract,
                status=str(active_runtime_contract.get("status") or "").strip(),
            ),
            {
                "logical_name": "recurrence_registry_v1",
                "path": str(registry_ref.path),
                "generated_at_utc": generated_at_utc,
                "sha256": registry_ref.sha256,
                "status": after_status,
            },
        ]
        + (
            [
                build_authoritative_source_row_v1(
                    logical_name="execution_journal_v1",
                    path=journal_ref.path,
                    payload=dict(journal_ref.payload),
                    status="CHRONOLOGY_PRESENT",
                )
            ]
            if journal_ref is not None
            else []
        ),
        generated_at_utc=generated_at_utc,
        producer_module="ops/tools/run_recurrence_kill_gate_v1.py",
    )
    ref = write_recurrence_kill_gate_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "report_path": str(ref.path),
                "proof_status": str(payload.get("proof_status") or ""),
                "recurrence_fingerprint": str(payload.get("recurrence_fingerprint") or ""),
                "recurrence_status_after": str(payload.get("recurrence_status_after") or ""),
                "live_service_execstart": EXPECTED_LIVE_ENTRYPOINT,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
