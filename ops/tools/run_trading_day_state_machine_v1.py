#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    read_intents_day_completeness_ref_v1,
    read_json_object_v1,
    read_paper_day_control_plane_ref_v1,
    read_paper_session_ledger_ref_v1,
    read_startup_materialization_ref_v1,
    read_startup_proof_validation_ref_v1,
    read_submit_boundary_status_ref_v1,
    read_paper_trading_posture_ref_v1,
    read_trading_day_control_plane_ref_v1,
    read_trading_day_execution_control_plane_ref_v1,
    read_trading_day_intent_generation_ref_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.execution_journal_v1 import (
    append_state_machine_decision_event_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_deployment_state_machine_path,
    resolve_intents_day_completeness_path,
    resolve_paper_day_control_plane_path,
    resolve_paper_session_evidence_manifest_path,
    resolve_paper_session_kernel_path,
    resolve_paper_session_ledger_path,
    resolve_paper_trading_posture_path,
    resolve_startup_materialization_path,
    resolve_startup_proof_validation_path,
    resolve_submit_boundary_status_path,
    resolve_trading_day_control_plane_path,
    resolve_trading_day_execution_control_plane_path,
    resolve_trading_day_intent_generation_path,
    resolve_trading_day_state_machine_path,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


OUTPUT_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_state_machine.v1.schema.json"
)
TRADING_DAY_INTENT_GENERATION_TOOL = (REPO_ROOT / "ops/tools/run_trading_day_intent_generation_v1.py").resolve()
INTENTS_DAY_COMPLETENESS_TOOL = (REPO_ROOT / "ops/tools/run_intents_day_completeness_v1.py").resolve()
TRADING_DAY_EXECUTION_CONTROL_PLANE_TOOL = (
    REPO_ROOT / "ops/tools/run_trading_day_execution_control_plane_v1.py"
).resolve()

_SKIPPED_REGENERATION_ROWS = (
    "startup_materialization_v1",
    "paper_trading_posture_v1",
    "submit_boundary_status_v1",
    "paper_session_ledger_v1",
    "startup_proof_validation_v1",
)


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _run(cmd: list[str], *, truth_root: Path) -> dict[str, Any]:
    env = os.environ.copy()
    env["C2_TRUTH_ROOT"] = str(truth_root)
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)
    return {
        "cmd": cmd,
        "return_code": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _load_prior_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = read_json_object_v1(path)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _first_nonempty(values: list[str]) -> str:
    for value in values:
        if str(value).strip():
            return str(value).strip()
    return ""


def _artifact_status_from_payload(logical_name: str, payload: dict[str, Any]) -> str:
    if logical_name == "startup_materialization_v1":
        return str(payload.get("status") or "").strip() or "UNKNOWN"
    if logical_name == "paper_trading_posture_v1":
        return str(payload.get("posture_status") or "").strip().upper() or "UNKNOWN"
    if logical_name == "submit_boundary_status_v1":
        return str(payload.get("boundary_status") or "").strip().upper() or "UNKNOWN"
    if logical_name == "paper_session_ledger_v1":
        control_state = payload.get("control_state")
        if not isinstance(control_state, dict):
            control_state = {}
        return str(payload.get("authority_status") or control_state.get("authority_status") or "").strip().upper() or "UNKNOWN"
    if logical_name == "startup_proof_validation_v1":
        return str(payload.get("status") or "").strip().upper() or "UNKNOWN"
    return "UNKNOWN"


def _row_from_surface(
    *,
    logical_name: str,
    ref: SurfaceRefV1,
    payload: dict[str, Any],
    return_code: int | None,
) -> dict[str, Any]:
    produced_at = str(payload.get("produced_at_utc") or payload.get("produced_utc") or "").strip()
    return {
        "logical_name": logical_name,
        "path": str(ref.path),
        "status": _artifact_status_from_payload(logical_name, payload),
        "return_code": return_code,
        "digest": ref.sha256,
        "produced_at": produced_at,
    }


def _skipped_regeneration_results() -> list[dict[str, Any]]:
    return [
        {
            "logical_name": logical_name,
            "path": "",
            "status": "SKIPPED_PREREQUISITE_BLOCKED",
            "return_code": None,
            "digest": "",
            "produced_at": "",
        }
        for logical_name in _SKIPPED_REGENERATION_ROWS
    ]


def _legacy_surface_rows(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    report_root = (truth_root / "reports").resolve()
    surfaces = [
        ("trading_day_state_v1", report_root / "trading_day_state_v1" / day_utc / "trading_day_state.v1.json"),
        ("session_readiness_refresh_v1", report_root / "session_readiness_refresh_v1" / day_utc / "session_readiness_refresh.v1.json"),
        (
            "paper_session_admission_certificate_v1",
            report_root / "paper_session_admission_certificate_v1" / day_utc / "paper_session_admission_certificate.v1.json",
        ),
        ("preopen_operator_summary_v1", report_root / "preopen_operator_summary_v1" / day_utc / "preopen_operator_summary.v1.json"),
        ("preopen_preflight_v1", report_root / "preopen_preflight_v1" / day_utc / "preopen_preflight.v1.json"),
        ("day_start_blocked_v1", report_root / "day_start_blocked_v1" / day_utc / "day_start_blocked.v1.json"),
    ]
    return [
        {
            "logical_name": logical_name,
            "path": str(path),
            "exists": bool(path.exists() and path.is_file()),
        }
        for logical_name, path in surfaces
    ]


def _superseded_surface_rows(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    surfaces = [
        ("paper_day_control_plane_v1", resolve_paper_day_control_plane_path(truth_root=truth_root, day_utc=day_utc)),
        ("trading_day_control_plane_v1", resolve_trading_day_control_plane_path(truth_root=truth_root, day_utc=day_utc)),
        (
            "trading_day_execution_control_plane_v1",
            resolve_trading_day_execution_control_plane_path(truth_root=truth_root, day_utc=day_utc),
        ),
        (
            "paper_session_evidence_manifest_v1",
            resolve_paper_session_evidence_manifest_path(truth_root=truth_root, day_utc=day_utc),
        ),
        ("paper_session_kernel_v1", resolve_paper_session_kernel_path(truth_root=truth_root, day_utc=day_utc)),
    ]
    return [
        {
            "logical_name": logical_name,
            "path": str(path),
            "exists": bool(path.exists() and path.is_file()),
        }
        for logical_name, path in surfaces
    ]


def _build_summary(*, day_utc: str, decision: str, blocker: str) -> str:
    if decision == "READY_NOW":
        return (
            f"Paper trading is startable for {day_utc}. "
            "The canonical trading-day state machine is READY_NOW."
        )
    if blocker:
        return f"Paper trading is blocked for {day_utc}. First true canonical blocker: {blocker}."
    return (
        f"Paper trading is blocked for {day_utc}. "
        "The canonical trading-day state machine did not reach READY_NOW."
    )


def _day_attempt_id(day_utc: str, evaluated_at_utc: str) -> str:
    return f"trading_day_state_machine_attempt:{day_utc}:{evaluated_at_utc}"


def _state_machine_id(day_utc: str, parts: dict[str, Any]) -> str:
    digest = _sha256_bytes(canonical_json_bytes_v1(parts))
    return f"trading_day_state_machine:{day_utc}:{digest[:16]}"


def _resolve_source_emission_identity(*, truth_root: Path, day_utc: str, day_attempt_id: str) -> dict[str, str] | None:
    deployment_path = resolve_deployment_state_machine_path(truth_root=truth_root, day_utc=day_utc)
    if not deployment_path.exists() or not deployment_path.is_file():
        return None
    deployment_payload = read_json_object_v1(deployment_path)
    pipeline_run_id = str(deployment_payload.get("deployment_attempt_id") or "").strip()
    release_build = deployment_payload.get("release_build")
    if not isinstance(release_build, dict):
        return None
    release_id = str(release_build.get("release_id") or "").strip()
    release_root = str(release_build.get("release_root") or "").strip()
    if not pipeline_run_id or not release_id or not release_root:
        return None
    manifest_path = (Path(release_root).resolve() / "release_manifest.v1.json").resolve()
    if not manifest_path.exists() or not manifest_path.is_file():
        return None
    manifest_payload = read_json_object_v1(manifest_path)
    git_sha = str(manifest_payload.get("git_sha") or "").strip().lower()
    if len(git_sha) != 40:
        return None
    return {
        "day_utc": day_utc,
        "day_attempt_id": day_attempt_id,
        "pipeline_run_id": pipeline_run_id,
        "release_id": release_id,
        "git_sha": git_sha,
    }


def _valid_zero_intent_status(completeness_status: str) -> str:
    if completeness_status == "NO_INTENTS_DECLARED":
        return "DECLARED"
    return "NOT_DECLARED"


def _append_transition(
    transitions: list[dict[str, str]],
    *,
    from_state: str,
    to_state: str,
    transition_at_utc: str,
    transition_reason_code: str,
    evidence_ref: str,
) -> None:
    transitions.append(
        {
            "from_state": from_state,
            "to_state": to_state,
            "transition_at_utc": transition_at_utc,
            "transition_reason_code": transition_reason_code,
            "evidence_ref": evidence_ref,
        }
    )


def _build_state_transitions(
    *,
    evaluated_at_utc: str,
    completeness_ref: str,
    completeness_status: str,
    execution_ref: str,
    final_start_decision: str,
    ledger_authority_status: str,
    first_true_blocker_code: str,
) -> list[dict[str, str]]:
    transitions: list[dict[str, str]] = []
    blocker_code = first_true_blocker_code or "STATE_MACHINE_DECISION_UNSPECIFIED"
    _append_transition(
        transitions,
        from_state="START",
        to_state="DAY_CREATED",
        transition_at_utc=evaluated_at_utc,
        transition_reason_code="STATE_MACHINE_EVALUATION_STARTED",
        evidence_ref="",
    )
    _append_transition(
        transitions,
        from_state="DAY_CREATED",
        to_state="PREREQUISITE_EVALUATED",
        transition_at_utc=evaluated_at_utc,
        transition_reason_code="INTENTS_DAY_COMPLETENESS_EVALUATED",
        evidence_ref=completeness_ref,
    )
    if completeness_status == "COMPLETE":
        _append_transition(
            transitions,
            from_state="PREREQUISITE_EVALUATED",
            to_state="INTENTS_READY",
            transition_at_utc=evaluated_at_utc,
            transition_reason_code="INTENTS_DAY_COMPLETENESS_COMPLETE",
            evidence_ref=completeness_ref,
        )
    elif completeness_status == "NO_INTENTS_DECLARED":
        _append_transition(
            transitions,
            from_state="PREREQUISITE_EVALUATED",
            to_state="VALID_ZERO_INTENTS",
            transition_at_utc=evaluated_at_utc,
            transition_reason_code="NO_INTENTS_DECLARED",
            evidence_ref=completeness_ref,
        )
    else:
        _append_transition(
            transitions,
            from_state="PREREQUISITE_EVALUATED",
            to_state="INTENTS_BLOCKED",
            transition_at_utc=evaluated_at_utc,
            transition_reason_code=blocker_code,
            evidence_ref=completeness_ref,
        )

    if final_start_decision == "READY_NOW":
        prior_state = "INTENTS_READY"
        _append_transition(
            transitions,
            from_state=prior_state,
            to_state="SUPPORTING_REGEN_COMPLETE",
            transition_at_utc=evaluated_at_utc,
            transition_reason_code="SUPPORTING_CHAIN_READY",
            evidence_ref=execution_ref,
        )
        _append_transition(
            transitions,
            from_state="SUPPORTING_REGEN_COMPLETE",
            to_state="SESSION_AUTHORITY_GRANTED",
            transition_at_utc=evaluated_at_utc,
            transition_reason_code="PAPER_SESSION_LEDGER_GRANTED",
            evidence_ref=execution_ref,
        )
        _append_transition(
            transitions,
            from_state="SESSION_AUTHORITY_GRANTED",
            to_state="DAY_OPEN_ALLOWED",
            transition_at_utc=evaluated_at_utc,
            transition_reason_code="STATE_MACHINE_READY_NOW",
            evidence_ref=execution_ref,
        )
        _append_transition(
            transitions,
            from_state="DAY_OPEN_ALLOWED",
            to_state="DAY_NOT_OPENED",
            transition_at_utc=evaluated_at_utc,
            transition_reason_code="STATE_MACHINE_AUTHORITY_ONLY_NO_OPEN_COMMAND_EXECUTED",
            evidence_ref=execution_ref,
        )
        return transitions

    if completeness_status == "COMPLETE":
        _append_transition(
            transitions,
            from_state="INTENTS_READY",
            to_state="SUPPORTING_REGEN_BLOCKED",
            transition_at_utc=evaluated_at_utc,
            transition_reason_code=blocker_code,
            evidence_ref=execution_ref,
        )
        from_regen_state = "SUPPORTING_REGEN_BLOCKED"
    elif completeness_status == "NO_INTENTS_DECLARED":
        _append_transition(
            transitions,
            from_state="VALID_ZERO_INTENTS",
            to_state="SUPPORTING_REGEN_BLOCKED",
            transition_at_utc=evaluated_at_utc,
            transition_reason_code=blocker_code,
            evidence_ref=completeness_ref,
        )
        from_regen_state = "SUPPORTING_REGEN_BLOCKED"
    else:
        _append_transition(
            transitions,
            from_state="INTENTS_BLOCKED",
            to_state="SUPPORTING_REGEN_BLOCKED",
            transition_at_utc=evaluated_at_utc,
            transition_reason_code="PREREQUISITE_FAILED",
            evidence_ref=completeness_ref,
        )
        from_regen_state = "SUPPORTING_REGEN_BLOCKED"

    authority_reason = blocker_code
    if ledger_authority_status == "DENIED":
        authority_reason = "PAPER_SESSION_LEDGER_DENIED"
    elif final_start_decision == "BLOCKED_BY_DEFECT":
        authority_reason = "SUPPORTING_CHAIN_DEFECT"
    _append_transition(
        transitions,
        from_state=from_regen_state,
        to_state="SESSION_AUTHORITY_DENIED",
        transition_at_utc=evaluated_at_utc,
        transition_reason_code=authority_reason,
        evidence_ref=execution_ref or completeness_ref,
    )
    _append_transition(
        transitions,
        from_state="SESSION_AUTHORITY_DENIED",
        to_state="DAY_OPEN_BLOCKED",
        transition_at_utc=evaluated_at_utc,
        transition_reason_code=blocker_code,
        evidence_ref=execution_ref or completeness_ref,
    )
    _append_transition(
        transitions,
        from_state="DAY_OPEN_BLOCKED",
        to_state="DAY_NOT_OPENED",
        transition_at_utc=evaluated_at_utc,
        transition_reason_code=blocker_code,
        evidence_ref=execution_ref or completeness_ref,
    )
    return transitions


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_trading_day_state_machine_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    evaluated_at_utc = now_utc_iso_v1()
    day_attempt_id = _day_attempt_id(day, evaluated_at_utc)
    output_path = resolve_trading_day_state_machine_path(truth_root=truth_root, day_utc=day)
    prior_payload = _load_prior_payload(output_path)
    blocking_codes: set[str] = set()
    intent_generation_path = resolve_trading_day_intent_generation_path(truth_root=truth_root, day_utc=day)

    completeness_path = resolve_intents_day_completeness_path(truth_root=truth_root, day_utc=day)
    paper_day_path = resolve_paper_day_control_plane_path(truth_root=truth_root, day_utc=day)
    trading_day_path = resolve_trading_day_control_plane_path(truth_root=truth_root, day_utc=day)
    execution_path = resolve_trading_day_execution_control_plane_path(truth_root=truth_root, day_utc=day)
    startup_path = resolve_startup_materialization_path(truth_root=truth_root, day_utc=day)
    posture_path = resolve_paper_trading_posture_path(truth_root=truth_root, day_utc=day)
    boundary_path = resolve_submit_boundary_status_path(truth_root=truth_root, day_utc=day)
    ledger_path = resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day)
    startup_proof_path = resolve_startup_proof_validation_path(truth_root=truth_root, day_utc=day)

    upstream_intent_status = {
        "intents_day_completeness_ref": "",
        "completeness_status": "DEFECT",
        "prerequisite_blocking_codes": [],
        "first_missing_prerequisite": "",
        "valid_zero_intent_status": "NOT_DECLARED",
    }
    supporting_daily_control_refs = {
        "paper_day_control_plane_path": "",
        "paper_day_control_plane_id": "",
        "paper_day_startup_attempt_id": "",
        "paper_day_final_start_decision": "NOT_EVALUATED",
        "trading_day_control_plane_path": "",
        "trading_day_control_plane_id": "",
        "trading_day_day_attempt_id": "",
        "trading_day_final_start_decision": "NOT_EVALUATED",
        "trading_day_execution_control_plane_path": "",
        "trading_day_execution_control_plane_id": "",
        "trading_day_execution_day_attempt_id": "",
        "trading_day_execution_final_start_decision": "NOT_EVALUATED",
    }
    supporting_session_authority = {
        "paper_session_ledger_path": "",
        "ledger_id": "",
        "ledger_authority_status": "NOT_EVALUATED",
        "ledger_evidence_status": "NOT_EVALUATED",
        "system_ready": False,
        "submission_authorized": False,
    }
    first_true_blocker = {
        "first_true_blocker_code": "",
        "first_true_blocker_artifact_path": "",
        "blocker_classification": "UNKNOWN",
    }
    supporting_regeneration_results: list[dict[str, Any]] = []
    final_start_decision = ""

    intent_generation_path = resolve_trading_day_intent_generation_path(truth_root=truth_root, day_utc=day)
    intent_generation_result = _run(
        [sys.executable, str(TRADING_DAY_INTENT_GENERATION_TOOL), "--day_utc", day, "--truth_root", str(truth_root)],
        truth_root=truth_root,
    )
    generation_row: dict[str, Any] | None = None
    intent_generation_payload: dict[str, Any] | None = None
    try:
        intent_generation_ref = read_trading_day_intent_generation_ref_v1(truth_root=truth_root, day_utc=day)
        intent_generation_payload = dict(intent_generation_ref.payload)
        generation_row = {
            "logical_name": "trading_day_intent_generation_v1",
            "path": str(intent_generation_ref.path),
            "status": str(intent_generation_payload.get("final_status") or "").strip().upper() or "UNKNOWN",
            "return_code": intent_generation_result["return_code"],
            "digest": intent_generation_ref.sha256,
            "produced_at": str(intent_generation_payload.get("produced_at_utc") or "").strip(),
        }
        for code in (intent_generation_payload.get("blocking_codes") or []):
            if str(code).strip():
                blocking_codes.add(str(code).strip())
    except Exception:
        blocking_codes.add("TRADING_DAY_STATE_MACHINE_INTENT_GENERATION_OUTPUT_MISSING_OR_INVALID")
        first_true_blocker = {
            "first_true_blocker_code": "TRADING_DAY_STATE_MACHINE_INTENT_GENERATION_OUTPUT_MISSING_OR_INVALID",
            "first_true_blocker_artifact_path": str(intent_generation_path) if intent_generation_path.exists() else "",
            "blocker_classification": "REGENERATION_DEFECT",
        }
        supporting_regeneration_results = [
            {
                "logical_name": "trading_day_intent_generation_v1",
                "path": str(intent_generation_path) if intent_generation_path.exists() else "",
                "status": "MISSING_OR_INVALID",
                "return_code": intent_generation_result["return_code"],
                "digest": "",
                "produced_at": "",
            }
        ] + _skipped_regeneration_results()
        final_start_decision = "BLOCKED_BY_DEFECT"

    if not final_start_decision and intent_generation_payload is not None:
        generation_status = str(intent_generation_payload.get("final_status") or "").strip().upper()
        if generation_status == "BLOCKED_BY_DEFECT":
            first_true_blocker = {
                "first_true_blocker_code": str(intent_generation_payload.get("first_blocker_code") or "").strip()
                or "TRADING_DAY_STATE_MACHINE_INTENT_GENERATION_FAILED",
                "first_true_blocker_artifact_path": str(
                    intent_generation_payload.get("first_blocker_artifact_path") or ""
                ).strip()
                or str(intent_generation_path),
                "blocker_classification": "REGENERATION_DEFECT",
            }
            supporting_regeneration_results = ([generation_row] if generation_row else []) + _skipped_regeneration_results()
            final_start_decision = "BLOCKED_BY_DEFECT"
        elif generation_status == "BLOCKED_VALID":
            first_true_blocker = {
                "first_true_blocker_code": str(intent_generation_payload.get("first_blocker_code") or "").strip()
                or "TRADING_DAY_STATE_MACHINE_INTENT_GENERATION_BLOCKED",
                "first_true_blocker_artifact_path": str(
                    intent_generation_payload.get("first_blocker_artifact_path") or ""
                ).strip()
                or str(intent_generation_path),
                "blocker_classification": "UPSTREAM_PREREQUISITE",
            }
            supporting_regeneration_results = ([generation_row] if generation_row else []) + _skipped_regeneration_results()
            final_start_decision = "BLOCKED_VALID"

    completeness_result = {"return_code": 3, "stdout": "", "stderr": ""}
    if not final_start_decision:
        completeness_result = _run(
            [sys.executable, str(INTENTS_DAY_COMPLETENESS_TOOL), "--day_utc", day, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        )

    prereq_payload: dict[str, Any] | None = None
    if not final_start_decision:
        try:
            prereq_ref = read_intents_day_completeness_ref_v1(truth_root=truth_root, day_utc=day)
            prereq_payload = dict(prereq_ref.payload)
            completeness_status = str(prereq_payload.get("completeness_status") or "").strip().upper() or "UNKNOWN"
            upstream_intent_status = {
                "intents_day_completeness_ref": str(prereq_ref.path),
                "completeness_status": completeness_status,
                "prerequisite_blocking_codes": sorted(
                    {str(code).strip() for code in (prereq_payload.get("blocking_codes") or []) if str(code).strip()}
                ),
                "first_missing_prerequisite": _first_nonempty(list(prereq_payload.get("missing_inputs") or [])),
                "valid_zero_intent_status": _valid_zero_intent_status(completeness_status),
            }
        except Exception:
            blocking_codes.add("TRADING_DAY_STATE_MACHINE_INTENTS_COMPLETENESS_OUTPUT_MISSING_OR_INVALID")

    completeness_status = upstream_intent_status["completeness_status"]
    prerequisite_blocking_codes = set(upstream_intent_status["prerequisite_blocking_codes"])
    blocking_codes.update(prerequisite_blocking_codes)

    startup_proof_result = {
        "startup_proof_validation_path": "",
        "startup_proof_validation_status": "NOT_EVALUATED",
        "agreement_with_supporting_authority": False,
    }

    if final_start_decision in {"BLOCKED_BY_DEFECT", "BLOCKED_VALID"}:
        pass
    elif prereq_payload is None:
        supporting_regeneration_results = ([generation_row] if generation_row else []) + _skipped_regeneration_results()
        first_true_blocker = {
            "first_true_blocker_code": "TRADING_DAY_STATE_MACHINE_INTENTS_COMPLETENESS_OUTPUT_MISSING_OR_INVALID",
            "first_true_blocker_artifact_path": str(completeness_path) if completeness_path.exists() else "",
            "blocker_classification": "REGENERATION_DEFECT",
        }
        final_start_decision = "BLOCKED_BY_DEFECT"
    elif completeness_result["return_code"] == 0 and completeness_status == "COMPLETE":
        execution_result = _run(
            [sys.executable, str(TRADING_DAY_EXECUTION_CONTROL_PLANE_TOOL), "--day_utc", day, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        )
        execution_payload: dict[str, Any] | None = None
        try:
            execution_ref = read_trading_day_execution_control_plane_ref_v1(truth_root=truth_root, day_utc=day)
            execution_payload = dict(execution_ref.payload)
            supporting_daily_control_refs["trading_day_execution_control_plane_path"] = str(execution_ref.path)
            supporting_daily_control_refs["trading_day_execution_control_plane_id"] = str(
                execution_payload.get("execution_control_plane_id") or ""
            ).strip()
            supporting_daily_control_refs["trading_day_execution_day_attempt_id"] = str(
                execution_payload.get("day_attempt_id") or ""
            ).strip()
            supporting_daily_control_refs["trading_day_execution_final_start_decision"] = str(
                execution_payload.get("final_start_decision") or ""
            ).strip().upper() or "NOT_EVALUATED"
            blocking_codes.update(
                str(code).strip()
                for code in (execution_payload.get("blocking_codes") or [])
                if str(code).strip()
            )
            payload_first_blocker = execution_payload.get("first_true_blocker")
            if isinstance(payload_first_blocker, dict):
                first_true_blocker = {
                    "first_true_blocker_code": str(payload_first_blocker.get("first_true_blocker_code") or "").strip(),
                    "first_true_blocker_artifact_path": str(
                        payload_first_blocker.get("first_true_blocker_artifact_path") or ""
                    ).strip(),
                    "blocker_classification": str(
                        payload_first_blocker.get("blocker_classification") or "UNKNOWN"
                    ).strip()
                    or "UNKNOWN",
                }
            startup_proof_payload = execution_payload.get("startup_proof_result")
            if isinstance(startup_proof_payload, dict):
                startup_proof_result = {
                    "startup_proof_validation_path": str(
                        startup_proof_payload.get("startup_proof_validation_path") or ""
                    ).strip(),
                    "startup_proof_validation_status": str(
                        startup_proof_payload.get("startup_proof_validation_status") or ""
                    ).strip()
                    .upper()
                    or "NOT_EVALUATED",
                    "agreement_with_supporting_authority": bool(
                        startup_proof_payload.get("agreement_with_supporting_authority") is True
                    ),
                }
        except Exception:
            blocking_codes.add("TRADING_DAY_STATE_MACHINE_EXECUTION_CONTROL_OUTPUT_MISSING_OR_INVALID")
            first_true_blocker = {
                "first_true_blocker_code": "TRADING_DAY_STATE_MACHINE_EXECUTION_CONTROL_OUTPUT_MISSING_OR_INVALID",
                "first_true_blocker_artifact_path": str(execution_path) if execution_path.exists() else "",
                "blocker_classification": "REGENERATION_DEFECT",
            }
            final_start_decision = "BLOCKED_BY_DEFECT"

        if final_start_decision != "BLOCKED_BY_DEFECT":
            try:
                paper_day_ref = read_paper_day_control_plane_ref_v1(truth_root=truth_root, day_utc=day)
                paper_day_payload = dict(paper_day_ref.payload)
                supporting_daily_control_refs["paper_day_control_plane_path"] = str(paper_day_ref.path)
                supporting_daily_control_refs["paper_day_control_plane_id"] = str(
                    paper_day_payload.get("control_plane_id") or ""
                ).strip()
                supporting_daily_control_refs["paper_day_startup_attempt_id"] = str(
                    paper_day_payload.get("startup_attempt_id") or ""
                ).strip()
                supporting_daily_control_refs["paper_day_final_start_decision"] = str(
                    paper_day_payload.get("final_start_decision") or ""
                ).strip().upper() or "NOT_EVALUATED"
            except Exception:
                blocking_codes.add("TRADING_DAY_STATE_MACHINE_PAPER_DAY_OUTPUT_MISSING_OR_INVALID")
                first_true_blocker = {
                    "first_true_blocker_code": "TRADING_DAY_STATE_MACHINE_PAPER_DAY_OUTPUT_MISSING_OR_INVALID",
                    "first_true_blocker_artifact_path": str(paper_day_path) if paper_day_path.exists() else "",
                    "blocker_classification": "REGENERATION_DEFECT",
                }
                final_start_decision = "BLOCKED_BY_DEFECT"

        if final_start_decision != "BLOCKED_BY_DEFECT":
            try:
                trading_day_ref = read_trading_day_control_plane_ref_v1(truth_root=truth_root, day_utc=day)
                trading_day_payload = dict(trading_day_ref.payload)
                supporting_daily_control_refs["trading_day_control_plane_path"] = str(trading_day_ref.path)
                supporting_daily_control_refs["trading_day_control_plane_id"] = str(
                    trading_day_payload.get("control_plane_id") or ""
                ).strip()
                supporting_daily_control_refs["trading_day_day_attempt_id"] = str(
                    trading_day_payload.get("day_attempt_id") or ""
                ).strip()
                supporting_daily_control_refs["trading_day_final_start_decision"] = str(
                    trading_day_payload.get("final_start_decision") or ""
                ).strip().upper() or "NOT_EVALUATED"
            except Exception:
                blocking_codes.add("TRADING_DAY_STATE_MACHINE_TRADING_DAY_OUTPUT_MISSING_OR_INVALID")
                first_true_blocker = {
                    "first_true_blocker_code": "TRADING_DAY_STATE_MACHINE_TRADING_DAY_OUTPUT_MISSING_OR_INVALID",
                    "first_true_blocker_artifact_path": str(trading_day_path) if trading_day_path.exists() else "",
                    "blocker_classification": "REGENERATION_DEFECT",
                }
                final_start_decision = "BLOCKED_BY_DEFECT"

        if final_start_decision != "BLOCKED_BY_DEFECT":
            try:
                startup_ref = read_startup_materialization_ref_v1(truth_root=truth_root, day_utc=day)
                startup_payload = dict(startup_ref.payload)
                posture_ref = read_paper_trading_posture_ref_v1(truth_root=truth_root, day_utc=day)
                posture_payload = dict(posture_ref.payload)
                boundary_ref = read_submit_boundary_status_ref_v1(truth_root=truth_root, day_utc=day)
                boundary_payload = dict(boundary_ref.payload)
                ledger_ref = read_paper_session_ledger_ref_v1(truth_root=truth_root, day_utc=day)
                ledger_payload = dict(ledger_ref.payload)
                startup_proof_ref = read_startup_proof_validation_ref_v1(truth_root=truth_root, day_utc=day)
                startup_proof_payload = dict(startup_proof_ref.payload)
                control_state = ledger_payload.get("control_state")
                if not isinstance(control_state, dict):
                    control_state = {}
                supporting_session_authority = {
                    "paper_session_ledger_path": str(ledger_ref.path),
                    "ledger_id": str(ledger_payload.get("ledger_id") or "").strip(),
                    "ledger_authority_status": str(
                        ledger_payload.get("authority_status") or control_state.get("authority_status") or ""
                    ).strip().upper()
                    or "NOT_EVALUATED",
                    "ledger_evidence_status": str(
                        (ledger_payload.get("evidence_freeze") or {}).get("overall_evidence_status") or ""
                    ).strip().upper()
                    or "NOT_EVALUATED",
                    "system_ready": bool(ledger_payload.get("system_ready") is True),
                    "submission_authorized": bool(ledger_payload.get("submission_authorized") is True),
                }
                startup_proof_result = {
                    "startup_proof_validation_path": str(startup_proof_ref.path),
                    "startup_proof_validation_status": str(
                        startup_proof_payload.get("status") or ""
                    ).strip().upper()
                    or "NOT_EVALUATED",
                    "agreement_with_supporting_authority": (
                        (
                            supporting_session_authority["ledger_authority_status"] == "GRANTED"
                            and str(startup_proof_payload.get("status") or "").strip().upper() == "STARTUP_READY"
                        )
                        or (
                            supporting_session_authority["ledger_authority_status"] == "DENIED"
                            and str(startup_proof_payload.get("status") or "").strip().upper() == "STARTUP_BLOCKED"
                        )
                    ),
                }
                downstream_regeneration_results = [
                    _row_from_surface(
                        logical_name="startup_materialization_v1",
                        ref=startup_ref,
                        payload=startup_payload,
                        return_code=next(
                            (
                                row.get("return_code")
                                for row in (execution_payload or {}).get("canonical_regeneration_results", [])
                                if isinstance(row, dict) and row.get("logical_name") == "startup_materialization_v1"
                            ),
                            None,
                        ),
                    ),
                    _row_from_surface(
                        logical_name="paper_trading_posture_v1",
                        ref=posture_ref,
                        payload=posture_payload,
                        return_code=next(
                            (
                                row.get("return_code")
                                for row in (execution_payload or {}).get("canonical_regeneration_results", [])
                                if isinstance(row, dict) and row.get("logical_name") == "paper_trading_posture_v1"
                            ),
                            None,
                        ),
                    ),
                    _row_from_surface(
                        logical_name="submit_boundary_status_v1",
                        ref=boundary_ref,
                        payload=boundary_payload,
                        return_code=next(
                            (
                                row.get("return_code")
                                for row in (execution_payload or {}).get("canonical_regeneration_results", [])
                                if isinstance(row, dict) and row.get("logical_name") == "submit_boundary_status_v1"
                            ),
                            None,
                        ),
                    ),
                    _row_from_surface(
                        logical_name="paper_session_ledger_v1",
                        ref=ledger_ref,
                        payload=ledger_payload,
                        return_code=next(
                            (
                                row.get("return_code")
                                for row in (execution_payload or {}).get("canonical_regeneration_results", [])
                                if isinstance(row, dict) and row.get("logical_name") == "paper_session_ledger_v1"
                            ),
                            None,
                        ),
                    ),
                    _row_from_surface(
                        logical_name="startup_proof_validation_v1",
                        ref=startup_proof_ref,
                        payload=startup_proof_payload,
                        return_code=next(
                            (
                                row.get("return_code")
                                for row in (execution_payload or {}).get("canonical_regeneration_results", [])
                                if isinstance(row, dict) and row.get("logical_name") == "startup_proof_validation_v1"
                            ),
                            None,
                        ),
                    ),
                ]
                supporting_regeneration_results = ([generation_row] if generation_row else []) + downstream_regeneration_results
                for payload in (startup_payload, posture_payload, boundary_payload, startup_proof_payload):
                    for code in (payload.get("blocking_codes") or []):
                        if str(code).strip():
                            blocking_codes.add(str(code).strip())
                for code in (control_state.get("blocking_codes") or []):
                    if str(code).strip():
                        blocking_codes.add(str(code).strip())
            except Exception:
                blocking_codes.add("TRADING_DAY_STATE_MACHINE_SUPPORTING_CANONICAL_OUTPUT_MISSING_OR_INVALID")
                if not first_true_blocker["first_true_blocker_code"]:
                    first_true_blocker = {
                        "first_true_blocker_code": "TRADING_DAY_STATE_MACHINE_SUPPORTING_CANONICAL_OUTPUT_MISSING_OR_INVALID",
                        "first_true_blocker_artifact_path": str(ledger_path) if ledger_path.exists() else "",
                        "blocker_classification": "REGENERATION_DEFECT",
                    }
                final_start_decision = "BLOCKED_BY_DEFECT"

        if final_start_decision != "BLOCKED_BY_DEFECT":
            execution_decision = supporting_daily_control_refs["trading_day_execution_final_start_decision"]
            ledger_status = supporting_session_authority["ledger_authority_status"]
            startup_status = startup_proof_result["startup_proof_validation_status"]
            agreement = startup_proof_result["agreement_with_supporting_authority"] is True
            if (
                execution_decision == "READY_NOW"
                and ledger_status == "GRANTED"
                and startup_status == "STARTUP_READY"
                and agreement
            ):
                final_start_decision = "READY_NOW"
            elif (
                execution_decision == "BLOCKED_VALID"
                and first_true_blocker["blocker_classification"]
                in {"UPSTREAM_PREREQUISITE", "CANONICAL_POLICY_OR_INPUT", "SUPPORTING_AUTHORITY_DENY"}
            ) or ledger_status == "DENIED":
                final_start_decision = "BLOCKED_VALID"
            elif execution_decision == "BLOCKED_BY_DEFECT" or execution_result["return_code"] == 3:
                final_start_decision = "BLOCKED_BY_DEFECT"
            else:
                blocking_codes.add("TRADING_DAY_STATE_MACHINE_SUPPORTING_DECISION_INCONSISTENT")
                if not first_true_blocker["first_true_blocker_code"]:
                    first_true_blocker = {
                        "first_true_blocker_code": "TRADING_DAY_STATE_MACHINE_SUPPORTING_DECISION_INCONSISTENT",
                        "first_true_blocker_artifact_path": str(execution_path) if execution_path.exists() else "",
                        "blocker_classification": "REGENERATION_DEFECT",
                    }
                final_start_decision = "BLOCKED_BY_DEFECT"
    else:
        supporting_regeneration_results = ([generation_row] if generation_row else []) + _skipped_regeneration_results()
        first_true_blocker = {
            "first_true_blocker_code": _first_nonempty(sorted(prerequisite_blocking_codes)),
            "first_true_blocker_artifact_path": str(completeness_path) if completeness_path.exists() else "",
            "blocker_classification": "UPSTREAM_PREREQUISITE",
        }
        final_start_decision = "BLOCKED_VALID"

    if not first_true_blocker["first_true_blocker_code"]:
        first_true_blocker["first_true_blocker_code"] = _first_nonempty(sorted(blocking_codes))
    if not first_true_blocker["first_true_blocker_artifact_path"]:
        if supporting_session_authority["paper_session_ledger_path"]:
            first_true_blocker["first_true_blocker_artifact_path"] = supporting_session_authority["paper_session_ledger_path"]
        elif upstream_intent_status["intents_day_completeness_ref"]:
            first_true_blocker["first_true_blocker_artifact_path"] = upstream_intent_status["intents_day_completeness_ref"]
    if first_true_blocker["blocker_classification"] == "UNKNOWN":
        if final_start_decision == "BLOCKED_BY_DEFECT":
            first_true_blocker["blocker_classification"] = "REGENERATION_DEFECT"
        elif final_start_decision == "BLOCKED_VALID" and upstream_intent_status["completeness_status"] != "COMPLETE":
            first_true_blocker["blocker_classification"] = "UPSTREAM_PREREQUISITE"
        elif final_start_decision == "BLOCKED_VALID":
            first_true_blocker["blocker_classification"] = "SUPPORTING_AUTHORITY_DENY"

    supersedes_prior_state_machine = bool(prior_payload)
    prior_state_machine_id = str((prior_payload or {}).get("state_machine_id") or "").strip()
    prior_day_attempt_id = str((prior_payload or {}).get("day_attempt_id") or "").strip()
    transitions = _build_state_transitions(
        evaluated_at_utc=evaluated_at_utc,
        completeness_ref=upstream_intent_status["intents_day_completeness_ref"],
        completeness_status=upstream_intent_status["completeness_status"],
        execution_ref=supporting_daily_control_refs["trading_day_execution_control_plane_path"],
        final_start_decision=final_start_decision,
        ledger_authority_status=supporting_session_authority["ledger_authority_status"],
        first_true_blocker_code=first_true_blocker["first_true_blocker_code"],
    )

    state_machine_parts = {
        "day_utc": day,
        "day_attempt_id": day_attempt_id,
        "evaluated_at_utc": evaluated_at_utc,
        "supersedes_prior_state_machine": supersedes_prior_state_machine,
        "prior_state_machine_id": prior_state_machine_id,
        "prior_day_attempt_id": prior_day_attempt_id,
        "upstream_intent_status": upstream_intent_status,
        "supporting_daily_control_refs": supporting_daily_control_refs,
        "supporting_regeneration_results": supporting_regeneration_results,
        "supporting_session_authority": supporting_session_authority,
        "state_transitions": transitions,
        "first_true_blocker": first_true_blocker,
        "final_start_decision": final_start_decision,
        "blocking_codes": sorted(blocking_codes),
    }
    state_machine_id = _state_machine_id(day, state_machine_parts)
    payload = {
        "schema_id": "trading_day_state_machine",
        "schema_version": "v1",
        "authority_scope": "TOP_LEVEL_DAILY_STATE_MACHINE_OWNER",
        "day_utc": day,
        "day_attempt_id": day_attempt_id,
        "state_machine_id": state_machine_id,
        "evaluated_at_utc": evaluated_at_utc,
        "supersedes_prior_state_machine": supersedes_prior_state_machine,
        "prior_state_machine_id": prior_state_machine_id,
        "prior_day_attempt_id": prior_day_attempt_id,
        "producer": producer_block_v1(module="ops/tools/run_trading_day_state_machine_v1.py"),
        "upstream_intent_status": upstream_intent_status,
        "supporting_daily_control_refs": supporting_daily_control_refs,
        "supporting_regeneration_results": supporting_regeneration_results,
        "supporting_session_authority": supporting_session_authority,
        "state_transitions": transitions,
        "first_true_blocker": first_true_blocker,
        "final_start_decision": final_start_decision,
        "blocking_codes": sorted(blocking_codes),
        "human_readable_summary": _build_summary(
            day_utc=day,
            decision=final_start_decision,
            blocker=first_true_blocker["first_true_blocker_code"],
        ),
        "ignored_legacy_surfaces": _legacy_surface_rows(truth_root=truth_root, day_utc=day),
        "ignored_superseded_surfaces": _superseded_surface_rows(truth_root=truth_root, day_utc=day),
        "suppression_reason": "NON_AUTHORITATIVE_FOR_STARTUP",
        "derived_daily_summary": {
            "summary_state": final_start_decision,
            "state_machine_id": state_machine_id,
            "day_attempt_id": day_attempt_id,
            "first_true_blocker_code": first_true_blocker["first_true_blocker_code"],
            "ledger_id": supporting_session_authority["ledger_id"],
            "non_authority_notice": (
                "Operator-facing summary derived only from trading_day_state_machine_v1. "
                "No legacy or superseded startup surface may override this state machine."
            ),
        },
    }
    ref = atomic_write_validated_json_v1(
        path=output_path,
        payload=payload,
        schema_relpath=OUTPUT_SCHEMA_RELPATH_V1,
    )
    journal_emission = {
        "status": "DEFERRED_IDENTITY_ANCHOR_MISSING",
        "event_type": "",
        "journal_path": "",
    }
    source_emission_identity = _resolve_source_emission_identity(
        truth_root=truth_root,
        day_utc=day,
        day_attempt_id=day_attempt_id,
    )
    if isinstance(source_emission_identity, dict):
        try:
            journal_ref = append_state_machine_decision_event_v1(
                truth_root=truth_root,
                identity=source_emission_identity,
                source_path=ref.path,
                source_payload=payload,
                producer_module="ops/tools/run_trading_day_state_machine_v1.py",
            )
            journal_emission = {
                "status": "EMITTED",
                "event_type": "STATE_MACHINE_DECISION_RECORDED",
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
                            "path": str(ref.path),
                            "sha256": ref.sha256,
                            "state_machine_id": state_machine_id,
                            "day_attempt_id": day_attempt_id,
                            "final_start_decision": final_start_decision,
                            "first_true_blocker_code": first_true_blocker["first_true_blocker_code"],
                            "ledger_id": supporting_session_authority["ledger_id"],
                            "journal_emission": {
                                "status": "FAILED",
                                "reason": reason,
                            },
                        },
                        sort_keys=True,
                    )
                )
                return 4
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "state_machine_id": state_machine_id,
                "day_attempt_id": day_attempt_id,
                "final_start_decision": final_start_decision,
                "first_true_blocker_code": first_true_blocker["first_true_blocker_code"],
                "ledger_id": supporting_session_authority["ledger_id"],
                "journal_emission": journal_emission,
            },
            sort_keys=True,
        )
    )
    if final_start_decision == "READY_NOW":
        return 0
    if final_start_decision == "BLOCKED_VALID":
        return 2
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
