#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.deployment_state_machine_v1 import (
    ACTIVE_POINTER,
    AUTHORITATIVE_SERVICE_SOURCE_PATH,
    RUNTIME_COPY_ROOT,
    RUNTIME_SERVICE_SOURCE_PATH,
    classify_deployment_decision,
    evaluate_post_activation_verification,
    evaluate_service_resolution,
    git_branch_or_fail,
    git_cleanliness_status,
    git_sha_or_fail,
    inspect_release_root,
    load_active_runtime_contract_if_present,
    resolve_live_service_fragment_path,
)
from constellation_2.common.execution_journal_v1 import (
    append_deployment_outcome_event_v1,
    read_execution_journal_identity_anchor_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_idempotent_validated_json_v1,
    resolve_authoritative_repo_root_v1,
)
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_trading_day_state_machine_path,
)
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/deployment_state_machine.v1.schema.json"
REPORT_FAMILY = "deployment_state_machine_v1"


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _report_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / REPORT_FAMILY
        / day_utc
        / "deployment_state_machine.v1.json"
    ).resolve()


def _load_prior_payload(path: Path, *, day_utc: str) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if str(payload.get("day_utc") or "").strip() != day_utc:
        return None
    return payload


def _sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _stable_payload_id(payload: dict[str, Any]) -> str:
    encoded = canonical_json_bytes_v1(payload)
    import hashlib

    return hashlib.sha256(encoded).hexdigest()[:16]


def _source_snapshot_id(git_sha: str, cleanliness_status: str) -> str:
    return git_sha if cleanliness_status == "CLEAN" else ""


def _resolve_day_attempt_anchor(*, truth_root: Path, day_utc: str) -> str:
    journal_identity = read_execution_journal_identity_anchor_v1(truth_root=truth_root, day_utc=day_utc)
    if isinstance(journal_identity, dict):
        return str(journal_identity["day_attempt_id"])
    trading_day_path = resolve_trading_day_state_machine_path(truth_root=truth_root, day_utc=day_utc)
    if not trading_day_path.exists() or not trading_day_path.is_file():
        return ""
    payload = json.loads(trading_day_path.read_text(encoding="utf-8"))
    return str(payload.get("day_attempt_id") or "").strip()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_deployment_state_machine_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument(
        "--truth_root",
        default="",
    )
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    truth_root = resolve_decision_truth_root_v1(args.truth_root, repo_root=REPO_ROOT)
    evaluated_at_utc = _utc_now_iso()
    out_path = _report_path(truth_root=truth_root, day_utc=day_utc)
    authoritative_repo_root = resolve_authoritative_repo_root_v1(REPO_ROOT)
    prior_payload = _load_prior_payload(out_path, day_utc=day_utc)
    deployment_attempt_id = str((prior_payload or {}).get("deployment_attempt_id") or "").strip()
    if not deployment_attempt_id:
        deployment_attempt_id = f"deployment_state_machine_attempt:{day_utc}:{evaluated_at_utc}"

    authoritative_git_sha = git_sha_or_fail(authoritative_repo_root)
    authoritative_branch = git_branch_or_fail(authoritative_repo_root)
    authoritative_cleanliness_status, dirty_entries = git_cleanliness_status(authoritative_repo_root)

    internal_error = ""
    active_symlink_target = ""
    active_release_git_sha = ""
    active_release: dict[str, Any] = {
        "release_id": "",
        "release_root": "",
        "release_manifest_sha": "",
        "bundled_file_hash_summary": {"file_count": 0, "aggregate_sha256": ""},
        "build_status": "RELEASE_NOT_FOUND",
        "required_startup_stack_files_present": False,
        "missing_required_startup_stack_files": [],
    }
    verification: dict[str, Any] = {
        "passed": False,
        "service_unit_path": "",
        "launcher_path": "",
        "resolved_execution_root": "",
        "active_root_match": False,
        "blocking_codes": [],
    }
    live_service_source = {
        "service_unit_path": str(AUTHORITATIVE_SERVICE_SOURCE_PATH),
        "launcher_path": "",
        "resolved_execution_root": "",
        "active_root_match": False,
    }

    try:
        active_target = ACTIVE_POINTER.resolve()
        active_symlink_target = str(active_target)
        inspection = inspect_release_root(active_target)
        active_release_git_sha = str(inspection.manifest.get("git_sha") or "").strip().lower()
        active_release = {
            "release_id": inspection.release_id,
            "release_root": str(inspection.release_root),
            "release_manifest_sha": inspection.manifest_sha256,
            "bundled_file_hash_summary": inspection.bundled_file_hash_summary,
            "build_status": "RELEASE_PRESENT",
            "required_startup_stack_files_present": inspection.required_startup_stack_present,
            "missing_required_startup_stack_files": inspection.missing_required_files,
        }
        verification = evaluate_post_activation_verification(release_root=inspection.release_root)
    except Exception as exc:
        internal_error = f"{type(exc).__name__}:{exc}"

    try:
        live_service_source = evaluate_service_resolution(AUTHORITATIVE_SERVICE_SOURCE_PATH)
    except Exception:
        pass

    installed_service_resolution = {
        "service_unit_path": "",
        "launcher_path": "",
        "resolved_execution_root": "",
        "active_root_match": False,
    }
    try:
        installed_service_resolution = evaluate_service_resolution(resolve_live_service_fragment_path())
    except Exception as exc:
        if not internal_error:
            internal_error = f"{type(exc).__name__}:{exc}"

    active_runtime_contract = load_active_runtime_contract_if_present()
    active_runtime_contract_status = (
        str(active_runtime_contract.get("status") or "").strip().upper()
        if isinstance(active_runtime_contract, dict)
        else "MISSING"
    )

    runtime_copy_direct_exec_detected = False
    runtime_copy_service_path = str(RUNTIME_SERVICE_SOURCE_PATH)
    runtime_copy_launcher_path = ""
    if RUNTIME_SERVICE_SOURCE_PATH.exists() and RUNTIME_SERVICE_SOURCE_PATH.is_file():
        runtime_service_text = RUNTIME_SERVICE_SOURCE_PATH.read_text(encoding="utf-8")
        runtime_copy_direct_exec_detected = str(RUNTIME_COPY_ROOT) in runtime_service_text
        import re

        match = re.search(
            r"(/home/node/[A-Za-z0-9_./-]+/ops/run/c2_paper_day_orchestrator_systemd_entry_v1\.sh)",
            runtime_service_text,
        )
        if match:
            runtime_copy_launcher_path = match.group(1)

    decision, blocking_codes, first_true_blocker_code = classify_deployment_decision(
        authoritative_cleanliness_status=authoritative_cleanliness_status,
        release_present=active_release["build_status"] == "RELEASE_PRESENT",
        required_startup_stack_files_present=bool(active_release["required_startup_stack_files_present"]),
        active_root_match=str(active_symlink_target) == str(active_release["release_root"]),
        live_execution_root_match=bool(installed_service_resolution["active_root_match"]),
        runtime_copy_direct_exec_detected=runtime_copy_direct_exec_detected,
        internal_error=internal_error,
    )

    if decision == "DEPLOY_ACTIVE":
        human_summary = (
            "Active release, active runtime contract, and installed paper-day unit all align to "
            "/home/node/constellation_active."
        )
    elif decision == "DEPLOY_BLOCKED_BY_DEFECT":
        human_summary = f"Deployment verification failed by defect: {first_true_blocker_code}"
    else:
        human_summary = (
            "Deployment is blocked because the authoritative source is not clean for immutable "
            "release build and/or the active release/service stack does not fully match the "
            "required startup stack."
        )

    payload = {
        "schema_id": "deployment_state_machine",
        "schema_version": "v1",
        "authority_scope": "TOP_LEVEL_DEPLOYMENT_STATE_MACHINE_OWNER",
        "day_utc": day_utc,
        "deployment_attempt_id": deployment_attempt_id,
        "deployment_state_machine_id": "",
        "evaluated_at_utc": evaluated_at_utc,
        "authoritative_source": {
            "authoritative_repo_root": str(authoritative_repo_root),
            "authoritative_git_sha": authoritative_git_sha,
            "authoritative_branch": authoritative_branch,
            "authoritative_cleanliness_status": authoritative_cleanliness_status,
            "dirty_entry_count": len(dirty_entries),
            "dirty_entry_sample": dirty_entries[:10],
            "source_snapshot_id": _source_snapshot_id(
                authoritative_git_sha, authoritative_cleanliness_status
            ),
        },
        "release_build": active_release,
        "active_release": {
            "active_symlink_path": str(ACTIVE_POINTER),
            "active_symlink_target": active_symlink_target,
            "activation_status": "ACTIVE_POINTER_PRESENT" if active_symlink_target else "ACTIVE_POINTER_MISSING",
            "active_runtime_contract_path": (
                ""
                if active_runtime_contract is None
                else "/home/node/constellation_runtime_data/runtime_contract_v1/active_runtime_contract.v1.json"
            ),
            "active_runtime_contract_status": active_runtime_contract_status,
        },
        "live_execution": {
            "service_unit_path": installed_service_resolution["service_unit_path"],
            "launcher_path": installed_service_resolution["launcher_path"],
            "resolved_execution_root": installed_service_resolution["resolved_execution_root"],
            "active_root_match": bool(installed_service_resolution["active_root_match"]),
            "authoritative_service_source_path": live_service_source["service_unit_path"],
            "authoritative_service_source_root": live_service_source["resolved_execution_root"],
        },
        "drift_checks": {
            "authoritative_vs_release": {
                "status": "PASS"
                if authoritative_cleanliness_status == "CLEAN"
                and active_release["build_status"] == "RELEASE_PRESENT"
                else "FAIL",
                "details": {
                    "authoritative_cleanliness_status": authoritative_cleanliness_status,
                    "active_release_git_sha": (
                        str(active_runtime_contract.get("git_sha") or "")
                        if isinstance(active_runtime_contract, dict)
                        else ""
                    ),
                },
            },
            "release_vs_active": {
                "status": "PASS"
                if str(active_symlink_target) == str(active_release["release_root"])
                and active_release["build_status"] == "RELEASE_PRESENT"
                else "FAIL",
                "details": {
                    "active_symlink_target": active_symlink_target,
                    "release_root": active_release["release_root"],
                },
            },
            "active_vs_live_execution": {
                "status": "PASS" if installed_service_resolution["active_root_match"] else "FAIL",
                "details": installed_service_resolution,
            },
            "runtime_copy_still_executable": {
                "status": "FAIL" if runtime_copy_direct_exec_detected else "PASS",
                "details": {
                    "runtime_service_source_path": runtime_copy_service_path,
                    "runtime_launcher_path": runtime_copy_launcher_path,
                },
            },
            "required_startup_stack_files_present": {
                "status": "PASS" if active_release["required_startup_stack_files_present"] else "FAIL",
                "details": {
                    "missing_required_startup_stack_files": active_release[
                        "missing_required_startup_stack_files"
                    ]
                },
            },
        },
        "post_activation_verification": verification,
        "final_deployment_decision": decision,
        "blocking_codes": blocking_codes,
        "first_true_blocker_code": first_true_blocker_code,
        "first_true_blocker_path": (
            installed_service_resolution["service_unit_path"]
            if first_true_blocker_code == "LIVE_EXECUTION_NOT_ACTIVE_ROOT"
            else (
                RUNTIME_SERVICE_SOURCE_PATH.as_posix()
                if first_true_blocker_code == "RUNTIME_COPY_DIRECT_EXECUTION_STILL_PRESENT"
                else (
                    authoritative_repo_root.as_posix()
                    if first_true_blocker_code == "AUTHORITATIVE_WORKTREE_DIRTY_BUILD_BLOCKED"
                    else str(active_release["release_root"])
                )
            )
        ),
        "human_readable_summary": human_summary,
        "producer": {
            "repo": "constellation",
            "module": "ops.tools.run_deployment_state_machine_v1",
            "git_sha": authoritative_git_sha,
        },
    }
    payload["deployment_state_machine_id"] = (
        f"deployment_state_machine:{day_utc}:{_stable_payload_id({**payload, 'deployment_state_machine_id': ''})}"
    )
    ref = atomic_write_idempotent_validated_json_v1(
        path=out_path,
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("evaluated_at_utc",),
    )
    journal_emission = {
        "status": "DEFERRED_IDENTITY_ANCHOR_MISSING",
        "event_type": "",
        "journal_path": "",
    }
    day_attempt_id = _resolve_day_attempt_anchor(truth_root=truth_root, day_utc=day_utc)
    if day_attempt_id and str(payload.get("release_build", {}).get("release_id") or "").strip() and active_release_git_sha:
        try:
            journal_ref = append_deployment_outcome_event_v1(
                truth_root=truth_root,
                day_utc=day_utc,
                day_attempt_id=day_attempt_id,
                pipeline_run_id=str(payload.get("deployment_attempt_id") or "").strip(),
                release_id=str(dict(payload.get("release_build") or {}).get("release_id") or "").strip(),
                git_sha=active_release_git_sha,
                source_path=ref.path,
                source_payload=payload,
                producer_module="ops/tools/run_deployment_state_machine_v1.py",
            )
            event_type = "DEPLOYMENT_ACTIVATED" if decision == "DEPLOY_ACTIVE" else "DEPLOYMENT_BLOCKED"
            journal_emission = {
                "status": "EMITTED",
                "event_type": event_type,
                "journal_path": str(journal_ref.path),
            }
        except Exception as exc:
            reason = f"{type(exc).__name__}:{exc}"
            if "EXECUTION_JOURNAL_CROSS_IDENTITY_CONTAMINATION:existing_journal:" in reason:
                journal_emission = {
                    "status": "DEFERRED_EXISTING_JOURNAL_IDENTITY_MISMATCH",
                    "event_type": "",
                    "journal_path": "",
                    "reason": reason,
                }
            else:
                print(
                    json.dumps(
                        {
                            "report_path": str(out_path),
                            "final_deployment_decision": decision,
                            "blocking_codes": blocking_codes,
                            "journal_emission": {
                                "status": "FAILED",
                                "reason": reason,
                            },
                        },
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 4
    print(
        json.dumps(
            {
                "report_path": str(out_path),
                "final_deployment_decision": decision,
                "blocking_codes": blocking_codes,
                "journal_emission": journal_emission,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if decision == "DEPLOY_ACTIVE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
