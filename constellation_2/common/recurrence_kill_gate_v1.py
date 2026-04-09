from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from constellation_2.common.execution_journal_v1 import (
    execution_identity_tuple_v1,
    identity_tuple_from_mapping_v1,
    read_execution_journal_v1,
    require_matching_identity_tuple_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    now_utc_iso_v1,
    producer_block_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_deployment_state_machine_path,
    resolve_recurrence_kill_gate_path,
    resolve_trading_day_state_machine_path,
)
from constellation_2.common.recurrence_fingerprint_v1 import (
    build_recurrence_fingerprint_record_v1,
)
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/recurrence_kill_gate.v1.schema.json"
DEPLOYMENT_SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/deployment_state_machine.v1.schema.json"
TRADING_DAY_SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_state_machine.v1.schema.json"
ACTIVE_POINTER = Path("/home/node/constellation_active").resolve()
ACTIVE_RUNTIME_CONTRACT_PATH = Path(
    "/home/node/constellation_runtime_data/runtime_contract_v1/active_runtime_contract.v1.json"
).resolve()
LIVE_SERVICE_NAME = "c2-paper-day-orchestrator.service"
EXPECTED_LIVE_WORKING_DIRECTORY = "/home/node/constellation_active"
EXPECTED_LIVE_ENTRYPOINT = "/home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"


def _require_nonempty_string(value: Any, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(code)
    return text


def _require_mapping(value: Any, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(code)
    return value


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(canonical_json_bytes_v1(dict(payload))).hexdigest()[:16]
    return f"{prefix}:{digest}"


def live_day_utc_v1(*, now: datetime | None = None) -> str:
    current = now.astimezone(ZoneInfo("America/New_York")) if isinstance(now, datetime) else datetime.now(
        ZoneInfo("America/New_York")
    )
    return current.date().isoformat()


def read_installed_service_text_v1(*, service_name: str = LIVE_SERVICE_NAME) -> str:
    proc = subprocess.run(
        ["systemctl", "--user", "cat", service_name],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise ValueError(f"RECURRENCE_KILL_GATE_LIVE_SERVICE_READ_FAILED:{service_name}:{proc.stderr.strip()}")
    return proc.stdout


def verify_live_entrypoint_v1(
    *,
    service_text: str,
    active_symlink_target: str,
    active_runtime_contract: Mapping[str, Any],
    active_release_manifest: Mapping[str, Any],
    active_entrypoint_path: Path,
) -> dict[str, Any]:
    proof_failures: list[str] = []
    service_text_value = _require_nonempty_string(
        service_text,
        "RECURRENCE_KILL_GATE_LIVE_SERVICE_TEXT_MISSING",
    )
    working_directory_ok = f"WorkingDirectory={EXPECTED_LIVE_WORKING_DIRECTORY}" in service_text_value
    exec_start_ok = EXPECTED_LIVE_ENTRYPOINT in service_text_value
    if not working_directory_ok:
        proof_failures.append("LIVE_ENTRYPOINT_WORKING_DIRECTORY_INVALID")
    if not exec_start_ok:
        proof_failures.append("LIVE_ENTRYPOINT_EXECSTART_INVALID")

    contract_release_id = str(active_runtime_contract.get("release_id") or "").strip()
    contract_git_sha = str(active_runtime_contract.get("git_sha") or "").strip().lower()
    contract_release_root = str(active_runtime_contract.get("release_root") or "").strip()
    if str(active_runtime_contract.get("status") or "").strip().upper() != "ACTIVE":
        proof_failures.append("LIVE_ENTRYPOINT_RUNTIME_CONTRACT_NOT_ACTIVE")
    if contract_release_root != active_symlink_target:
        proof_failures.append("LIVE_ENTRYPOINT_RUNTIME_CONTRACT_RELEASE_ROOT_MISMATCH")

    manifest_release_id = str(active_release_manifest.get("release_id") or "").strip()
    manifest_git_sha = str(active_release_manifest.get("git_sha") or "").strip().lower()
    manifest_release_root = str(active_release_manifest.get("release_root") or "").strip()
    if manifest_release_root != active_symlink_target:
        proof_failures.append("LIVE_ENTRYPOINT_ACTIVE_MANIFEST_RELEASE_ROOT_MISMATCH")
    if contract_release_id != manifest_release_id:
        proof_failures.append("LIVE_ENTRYPOINT_RUNTIME_CONTRACT_RELEASE_ID_MISMATCH")
    if contract_git_sha != manifest_git_sha:
        proof_failures.append("LIVE_ENTRYPOINT_RUNTIME_CONTRACT_GIT_SHA_MISMATCH")
    if not active_entrypoint_path.exists() or not active_entrypoint_path.is_file():
        proof_failures.append("LIVE_ENTRYPOINT_FILE_MISSING")
        entrypoint_text = ""
    else:
        entrypoint_text = active_entrypoint_path.read_text(encoding="utf-8")
        if "TZ=America/New_York date +%F" not in entrypoint_text:
            proof_failures.append("LIVE_ENTRYPOINT_TARGET_DAY_RULE_MISSING")
        if "validated paper-day execution requires release manifest" not in entrypoint_text:
            proof_failures.append("LIVE_ENTRYPOINT_RELEASE_MANIFEST_REQUIREMENT_MISSING")

    return {
        "live_entrypoint_verified": not proof_failures,
        "proof_failures": sorted(set(proof_failures)),
        "service_name": LIVE_SERVICE_NAME,
        "working_directory": EXPECTED_LIVE_WORKING_DIRECTORY if working_directory_ok else "",
        "execstart_path": EXPECTED_LIVE_ENTRYPOINT if exec_start_ok else "",
        "active_symlink_target": active_symlink_target,
        "active_entrypoint_path": str(active_entrypoint_path),
        "active_runtime_contract_path": str(ACTIVE_RUNTIME_CONTRACT_PATH),
        "active_release_manifest_release_id": manifest_release_id,
        "active_release_manifest_git_sha": manifest_git_sha,
    }


def determine_terminal_state_v1(
    *,
    deployment_payload: Mapping[str, Any],
    trading_day_payload: Mapping[str, Any],
) -> dict[str, str]:
    deployment_decision = _require_nonempty_string(
        deployment_payload.get("final_deployment_decision"),
        "RECURRENCE_KILL_GATE_DEPLOYMENT_DECISION_MISSING",
    ).upper()
    if deployment_decision != "DEPLOY_ACTIVE":
        first_true_blocker_code = str(deployment_payload.get("first_true_blocker_code") or "").strip()
        return {
            "terminal_state": deployment_decision,
            "terminal_state_source": "deployment_state_machine_v1",
            "first_true_blocker_code": first_true_blocker_code or deployment_decision,
            "first_true_blocker_source": "deployment_state_machine_v1",
            "stage_id": "DEPLOYMENT",
        }
    final_start_decision = _require_nonempty_string(
        trading_day_payload.get("final_start_decision"),
        "RECURRENCE_KILL_GATE_TRADING_DAY_DECISION_MISSING",
    ).upper()
    first_true_blocker = _require_mapping(
        trading_day_payload.get("first_true_blocker") or {},
        "RECURRENCE_KILL_GATE_TRADING_DAY_BLOCKER_BLOCK_INVALID",
    )
    return {
        "terminal_state": final_start_decision,
        "terminal_state_source": "trading_day_state_machine_v1",
        "first_true_blocker_code": str(first_true_blocker.get("first_true_blocker_code") or "").strip(),
        "first_true_blocker_source": "trading_day_state_machine_v1",
        "stage_id": "TRADING_DAY_START",
    }


def classify_blocker_family_v1(
    *,
    proof_failures: list[str],
    first_true_blocker_code: str,
    terminal_state_source: str,
) -> str:
    if proof_failures:
        return "PROOF_INVALID"
    if not str(first_true_blocker_code or "").strip():
        return "SUCCESS_VERIFICATION"
    if str(terminal_state_source) == "deployment_state_machine_v1":
        return "DEPLOYMENT_BLOCK"
    return "DAY_START_BLOCK"


def journal_stability_signature_v1(journal_payload: Mapping[str, Any]) -> dict[str, Any]:
    events = list(journal_payload.get("events") or [])
    return {
        "journal_id": str(journal_payload.get("journal_id") or "").strip(),
        "event_count": int(journal_payload.get("last_event_seq") or 0),
        "last_event_seq": int(journal_payload.get("last_event_seq") or 0),
        "event_keys": [str(dict(event).get("event_key") or "").strip() for event in events if isinstance(event, Mapping)],
    }


def validate_rerun_idempotency_result_v1(
    *,
    before_journal_payload: Mapping[str, Any],
    after_journal_payload: Mapping[str, Any],
    runner_exit_code: int,
    runner_stdout: str = "",
    runner_stderr: str = "",
) -> dict[str, Any]:
    if runner_exit_code != 0:
        return {
            "rerun_idempotent": False,
            "reason_code": "RERUN_RECONCILER_FAILED",
            "before_signature": journal_stability_signature_v1(before_journal_payload),
            "after_signature": journal_stability_signature_v1(after_journal_payload),
            "runner_exit_code": runner_exit_code,
            "runner_stdout": runner_stdout,
            "runner_stderr": runner_stderr,
        }
    before_signature = journal_stability_signature_v1(before_journal_payload)
    after_signature = journal_stability_signature_v1(after_journal_payload)
    return {
        "rerun_idempotent": before_signature == after_signature,
        "reason_code": "" if before_signature == after_signature else "RERUN_JOURNAL_CHANGED",
        "before_signature": before_signature,
        "after_signature": after_signature,
        "runner_exit_code": runner_exit_code,
        "runner_stdout": runner_stdout,
        "runner_stderr": runner_stderr,
    }


def verify_rerun_idempotency_v1(
    *,
    day_utc: str,
    truth_root: str | Path,
    runner_path: Path,
) -> dict[str, Any]:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    before_ref = read_execution_journal_v1(truth_root=root, day_utc=day_utc)
    proc = subprocess.run(
        [sys.executable, str(runner_path), "--day_utc", day_utc, "--truth_root", str(root)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    after_ref = read_execution_journal_v1(truth_root=root, day_utc=day_utc)
    return validate_rerun_idempotency_result_v1(
        before_journal_payload=dict(before_ref.payload),
        after_journal_payload=dict(after_ref.payload),
        runner_exit_code=int(proc.returncode),
        runner_stdout=proc.stdout.strip(),
        runner_stderr=proc.stderr.strip(),
    )


def build_authoritative_source_row_v1(
    *,
    logical_name: str,
    path: Path,
    payload: Mapping[str, Any],
    status: str,
) -> dict[str, Any]:
    generated_at_utc = (
        str(payload.get("evaluated_at_utc") or "").strip()
        or str(payload.get("generated_at_utc") or "").strip()
        or str(payload.get("produced_at_utc") or "").strip()
    )
    return {
        "logical_name": _require_nonempty_string(
            logical_name,
            "RECURRENCE_KILL_GATE_SOURCE_LOGICAL_NAME_MISSING",
        ),
        "path": str(path.resolve()),
        "generated_at_utc": _require_nonempty_string(
            generated_at_utc,
            f"RECURRENCE_KILL_GATE_SOURCE_GENERATED_AT_MISSING:{logical_name}",
        ),
        "sha256": sha256_file_v1(path),
        "status": _require_nonempty_string(status, f"RECURRENCE_KILL_GATE_SOURCE_STATUS_MISSING:{logical_name}"),
    }


def build_recurrence_kill_gate_payload_v1(
    *,
    day_utc: str,
    identity: Mapping[str, Any],
    live_entrypoint_verification: Mapping[str, Any],
    live_day_utc_value: str,
    deployment_payload: Mapping[str, Any],
    trading_day_payload: Mapping[str, Any],
    rerun_idempotency: Mapping[str, Any],
    fingerprint_record: Mapping[str, Any],
    recurrence_status_before: str,
    recurrence_status_after: str,
    additional_proof_failures: list[str] | None = None,
    authoritative_sources: list[dict[str, Any]],
    generated_at_utc: str,
    producer_module: str,
) -> dict[str, Any]:
    normalized_identity = execution_identity_tuple_v1(
        day_utc=str(identity.get("day_utc") or "").strip(),
        day_attempt_id=str(identity.get("day_attempt_id") or "").strip(),
        pipeline_run_id=str(identity.get("pipeline_run_id") or "").strip(),
        release_id=str(identity.get("release_id") or "").strip(),
        git_sha=str(identity.get("git_sha") or "").strip(),
    )
    live_entrypoint_verified = bool(live_entrypoint_verification.get("live_entrypoint_verified") is True)
    live_entrypoint_failures = [
        str(code).strip()
        for code in list(live_entrypoint_verification.get("proof_failures") or [])
        if str(code).strip()
    ]
    target_day_verified = str(day_utc).strip() == str(live_day_utc_value).strip()
    proof_failures: list[str] = list(live_entrypoint_failures)
    proof_failures.extend(
        str(code).strip()
        for code in list(additional_proof_failures or [])
        if str(code).strip()
    )
    if not target_day_verified:
        proof_failures.append("TARGET_DAY_NOT_LIVE_CURRENT_DAY")
    terminal = determine_terminal_state_v1(
        deployment_payload=deployment_payload,
        trading_day_payload=trading_day_payload,
    )
    if not bool(rerun_idempotency.get("rerun_idempotent") is True):
        reason_code = str(rerun_idempotency.get("reason_code") or "").strip() or "RERUN_IDEMPOTENCY_NOT_PROVEN"
        proof_failures.append(reason_code)

    first_true_blocker_code = str(terminal.get("first_true_blocker_code") or "").strip()
    first_true_blocker_source = str(terminal.get("first_true_blocker_source") or "").strip()
    blocker_family = str(fingerprint_record.get("blocker_family") or "").strip().upper()
    if not blocker_family:
        blocker_family = classify_blocker_family_v1(
            proof_failures=proof_failures,
            first_true_blocker_code=first_true_blocker_code,
            terminal_state_source=str(terminal.get("terminal_state_source") or "").strip(),
        )
    if proof_failures:
        proof_status = "INVALID_PROOF"
    elif blocker_family == "SUCCESS_VERIFICATION":
        proof_status = "RECURRENCE_SAFE"
    else:
        proof_status = "MITIGATED_NOT_RECURRENCE_SAFE"
    if proof_status == "RECURRENCE_SAFE":
        reason = "Live current-day path, single terminal state, and idempotent rerun proof all passed."
    elif proof_status == "MITIGATED_NOT_RECURRENCE_SAFE":
        reason = "Operational run is contained, but recurrence elimination proof is not yet sufficient."
    else:
        reason = "Required live-path, current-day, identity, or rerun proof was invalid or incomplete."

    payload = {
        "schema_id": "recurrence_kill_gate",
        "schema_version": "v1",
        "authority_scope": "OPERATIONAL_RECURRENCE_CLOSURE_GATE",
        "day_utc": normalized_identity["day_utc"],
        "day_attempt_id": normalized_identity["day_attempt_id"],
        "pipeline_run_id": normalized_identity["pipeline_run_id"],
        "release_id": normalized_identity["release_id"],
        "git_sha": normalized_identity["git_sha"],
        "proof_status": proof_status,
        "live_entrypoint_verified": live_entrypoint_verified,
        "target_day_verified": target_day_verified,
        "live_target_day_utc": str(live_day_utc_value).strip(),
        "terminal_state": str(terminal.get("terminal_state") or "").strip(),
        "first_true_blocker_code": first_true_blocker_code,
        "first_true_blocker_source": first_true_blocker_source,
        "blocker_family": blocker_family,
        "rerun_idempotent": bool(rerun_idempotency.get("rerun_idempotent") is True),
        "recurrence_fingerprint": _require_nonempty_string(
            fingerprint_record.get("recurrence_fingerprint"),
            "RECURRENCE_KILL_GATE_FINGERPRINT_MISSING",
        ),
        "recurrence_status_before": _require_nonempty_string(
            recurrence_status_before,
            "RECURRENCE_KILL_GATE_STATUS_BEFORE_MISSING",
        ),
        "recurrence_status_after": _require_nonempty_string(
            recurrence_status_after,
            "RECURRENCE_KILL_GATE_STATUS_AFTER_MISSING",
        ),
        "reason": reason,
        "proof_failures": sorted(set(proof_failures)),
        "authoritative_sources": authoritative_sources,
        "generated_at_utc": _require_nonempty_string(
            generated_at_utc,
            "RECURRENCE_KILL_GATE_GENERATED_AT_MISSING",
        ),
        "producer": producer_block_v1(module=producer_module, git_sha=normalized_identity["git_sha"]),
    }
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    return payload


def write_recurrence_kill_gate_v1(*, truth_root: str | Path, payload: Mapping[str, Any]) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    path = resolve_recurrence_kill_gate_path(
        truth_root=root,
        day_utc=str(payload.get("day_utc") or "").strip(),
    )
    validated = dict(payload)
    validate_against_repo_schema_v1(validated, REPO_ROOT, SCHEMA_RELPATH_V1)
    return atomic_write_validated_json_v1(
        path=path,
        payload=validated,
        schema_relpath=SCHEMA_RELPATH_V1,
    )
