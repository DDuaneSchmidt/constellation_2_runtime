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
    read_paper_trading_posture_ref_v1,
    read_startup_materialization_ref_v1,
    read_startup_proof_validation_ref_v1,
    read_submit_boundary_status_ref_v1,
    read_trading_day_control_plane_ref_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1
from constellation_2.common.paper_session_path_alignment_v1 import (
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
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


OUTPUT_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_execution_control_plane.v1.schema.json"
)
TRADING_DAY_INTENT_GENERATION_TOOL = (REPO_ROOT / "ops/tools/run_trading_day_intent_generation_v1.py").resolve()
INTENTS_DAY_COMPLETENESS_TOOL = (REPO_ROOT / "ops/tools/run_intents_day_completeness_v1.py").resolve()
TRADING_DAY_CONTROL_PLANE_TOOL = (REPO_ROOT / "ops/tools/run_trading_day_control_plane_v1.py").resolve()

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
            "The canonical trading-day execution control plane is READY_NOW."
        )
    if blocker:
        return f"Paper trading is blocked for {day_utc}. First true canonical blocker: {blocker}."
    return (
        f"Paper trading is blocked for {day_utc}. "
        "The canonical trading-day execution control plane did not reach READY_NOW."
    )


def _day_attempt_id(day_utc: str, evaluated_at_utc: str) -> str:
    return f"trading_day_execution_start_attempt:{day_utc}:{evaluated_at_utc}"


def _execution_control_plane_id(day_utc: str, parts: dict[str, Any]) -> str:
    digest = _sha256_bytes(canonical_json_bytes_v1(parts))
    return f"trading_day_execution_control_plane:{day_utc}:{digest[:16]}"


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    ap = argparse.ArgumentParser(prog="run_trading_day_execution_control_plane_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    evaluated_at_utc = now_utc_iso_v1()
    day_attempt_id = _day_attempt_id(day, evaluated_at_utc)
    output_path = resolve_trading_day_execution_control_plane_path(truth_root=truth_root, day_utc=day)
    prior_payload = _load_prior_payload(output_path)
    blocking_codes: set[str] = set()
    intent_generation_path = resolve_trading_day_intent_generation_path(truth_root=truth_root, day_utc=day)

    completeness_path = resolve_intents_day_completeness_path(truth_root=truth_root, day_utc=day)
    paper_day_path = resolve_paper_day_control_plane_path(truth_root=truth_root, day_utc=day)
    trading_day_path = resolve_trading_day_control_plane_path(truth_root=truth_root, day_utc=day)
    startup_path = resolve_startup_materialization_path(truth_root=truth_root, day_utc=day)
    posture_path = resolve_paper_trading_posture_path(truth_root=truth_root, day_utc=day)
    boundary_path = resolve_submit_boundary_status_path(truth_root=truth_root, day_utc=day)
    ledger_path = resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day)
    startup_proof_path = resolve_startup_proof_validation_path(truth_root=truth_root, day_utc=day)

    supporting_daily_controls = {
        "paper_day_control_plane_path": "",
        "paper_day_control_plane_id": "",
        "paper_day_startup_attempt_id": "",
        "paper_day_final_start_decision": "NOT_EVALUATED",
        "trading_day_control_plane_path": "",
        "trading_day_control_plane_id": "",
        "trading_day_day_attempt_id": "",
        "trading_day_final_start_decision": "NOT_EVALUATED",
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
    startup_proof_result = {
        "startup_proof_validation_path": "",
        "startup_proof_validation_status": "NOT_EVALUATED",
        "agreement_with_supporting_authority": False,
    }
    upstream_completeness = {
        "intents_day_completeness_ref": "",
        "completeness_status": "DEFECT",
        "prerequisite_blocking_codes": [],
        "first_missing_prerequisite": "",
    }
    canonical_regeneration_results: list[dict[str, Any]] = []
    final_start_decision = ""

    intent_generation_result = _run(
        [sys.executable, str(TRADING_DAY_INTENT_GENERATION_TOOL), "--day_utc", day, "--truth_root", str(truth_root)],
        truth_root=truth_root,
    )
    intent_generation_payload = _load_prior_payload(intent_generation_path)
    if isinstance(intent_generation_payload, dict):
        for code in (intent_generation_payload.get("blocking_codes") or []):
            if str(code).strip():
                blocking_codes.add(str(code).strip())
    if intent_generation_result["return_code"] == 3:
        blocking_codes.add("TRADING_DAY_EXECUTION_CONTROL_PLANE_INTENT_GENERATION_FAILED")
        first_true_blocker = {
            "first_true_blocker_code": "TRADING_DAY_EXECUTION_CONTROL_PLANE_INTENT_GENERATION_FAILED",
            "first_true_blocker_artifact_path": str(intent_generation_path) if intent_generation_path.exists() else "",
            "blocker_classification": "REGENERATION_DEFECT",
        }
        canonical_regeneration_results = _skipped_regeneration_results()
        final_start_decision = "BLOCKED_BY_DEFECT"
    elif intent_generation_result["return_code"] == 2:
        blocking_codes.add("TRADING_DAY_EXECUTION_CONTROL_PLANE_INTENT_GENERATION_BLOCKED")
        first_true_blocker = {
            "first_true_blocker_code": _first_nonempty(
                [
                    _first_nonempty(
                        [str(code).strip() for code in (intent_generation_payload or {}).get("blocking_codes", [])]
                    ),
                    "TRADING_DAY_EXECUTION_CONTROL_PLANE_INTENT_GENERATION_BLOCKED",
                ]
            ),
            "first_true_blocker_artifact_path": str(intent_generation_path) if intent_generation_path.exists() else "",
            "blocker_classification": "CANONICAL_POLICY_OR_INPUT",
        }
        canonical_regeneration_results = _skipped_regeneration_results()
        final_start_decision = "BLOCKED_VALID"

    completeness_result = {"return_code": 3, "stdout": "", "stderr": ""}
    if not final_start_decision:
        completeness_result = _run(
            [sys.executable, str(INTENTS_DAY_COMPLETENESS_TOOL), "--day_utc", day, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        )

    prereq_payload: dict[str, Any] | None = None
    try:
        prereq_ref = read_intents_day_completeness_ref_v1(truth_root=truth_root, day_utc=day)
        prereq_payload = dict(prereq_ref.payload)
        upstream_completeness = {
            "intents_day_completeness_ref": str(prereq_ref.path),
            "completeness_status": str(prereq_payload.get("completeness_status") or "").strip().upper() or "UNKNOWN",
            "prerequisite_blocking_codes": sorted(
                {str(code).strip() for code in (prereq_payload.get("blocking_codes") or []) if str(code).strip()}
            ),
            "first_missing_prerequisite": _first_nonempty(list(prereq_payload.get("missing_inputs") or [])),
        }
    except Exception:
        blocking_codes.add("TRADING_DAY_EXECUTION_CONTROL_PLANE_INTENTS_COMPLETENESS_OUTPUT_MISSING_OR_INVALID")

    completeness_status = upstream_completeness["completeness_status"]
    prerequisite_blocking_codes = set(upstream_completeness["prerequisite_blocking_codes"])
    blocking_codes.update(prerequisite_blocking_codes)

    if final_start_decision in {"BLOCKED_BY_DEFECT", "BLOCKED_VALID"}:
        pass
    elif prereq_payload is None:
        canonical_regeneration_results = _skipped_regeneration_results()
        first_true_blocker = {
            "first_true_blocker_code": "TRADING_DAY_EXECUTION_CONTROL_PLANE_INTENTS_COMPLETENESS_OUTPUT_MISSING_OR_INVALID",
            "first_true_blocker_artifact_path": str(completeness_path) if completeness_path.exists() else "",
            "blocker_classification": "REGENERATION_DEFECT",
        }
        final_start_decision = "BLOCKED_BY_DEFECT"
    elif completeness_result["return_code"] == 0 and completeness_status == "COMPLETE":
        trading_day_result = _run(
            [sys.executable, str(TRADING_DAY_CONTROL_PLANE_TOOL), "--day_utc", day, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        )
        trading_day_payload: dict[str, Any] | None = None
        supporting_rows: dict[str, dict[str, Any]] = {}
        try:
            trading_day_ref = read_trading_day_control_plane_ref_v1(truth_root=truth_root, day_utc=day)
            trading_day_payload = dict(trading_day_ref.payload)
            supporting_daily_controls["trading_day_control_plane_path"] = str(trading_day_ref.path)
            supporting_daily_controls["trading_day_control_plane_id"] = str(
                trading_day_payload.get("control_plane_id") or ""
            ).strip()
            supporting_daily_controls["trading_day_day_attempt_id"] = str(
                trading_day_payload.get("day_attempt_id") or ""
            ).strip()
            supporting_daily_controls["trading_day_final_start_decision"] = str(
                trading_day_payload.get("final_start_decision") or ""
            ).strip().upper() or "NOT_EVALUATED"
            supporting_rows = {
                str(row.get("logical_name") or "").strip(): dict(row)
                for row in (trading_day_payload.get("canonical_regeneration_results") or [])
                if isinstance(row, dict) and str(row.get("logical_name") or "").strip()
            }
            blocking_codes.update(
                str(code).strip()
                for code in (trading_day_payload.get("blocking_codes") or [])
                if str(code).strip()
            )
            payload_first_blocker = trading_day_payload.get("first_true_blocker")
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
        except Exception:
            blocking_codes.add("TRADING_DAY_EXECUTION_CONTROL_PLANE_TRADING_DAY_OUTPUT_MISSING_OR_INVALID")
            first_true_blocker = {
                "first_true_blocker_code": "TRADING_DAY_EXECUTION_CONTROL_PLANE_TRADING_DAY_OUTPUT_MISSING_OR_INVALID",
                "first_true_blocker_artifact_path": str(trading_day_path) if trading_day_path.exists() else "",
                "blocker_classification": "REGENERATION_DEFECT",
            }
            final_start_decision = "BLOCKED_BY_DEFECT"

        if final_start_decision != "BLOCKED_BY_DEFECT":
            try:
                paper_day_ref = read_paper_day_control_plane_ref_v1(truth_root=truth_root, day_utc=day)
                paper_day_payload = dict(paper_day_ref.payload)
                supporting_daily_controls["paper_day_control_plane_path"] = str(paper_day_ref.path)
                supporting_daily_controls["paper_day_control_plane_id"] = str(
                    paper_day_payload.get("control_plane_id") or ""
                ).strip()
                supporting_daily_controls["paper_day_startup_attempt_id"] = str(
                    paper_day_payload.get("startup_attempt_id") or ""
                ).strip()
                supporting_daily_controls["paper_day_final_start_decision"] = str(
                    paper_day_payload.get("final_start_decision") or ""
                ).strip().upper() or "NOT_EVALUATED"
            except Exception:
                blocking_codes.add("TRADING_DAY_EXECUTION_CONTROL_PLANE_PAPER_DAY_OUTPUT_MISSING_OR_INVALID")
                first_true_blocker = {
                    "first_true_blocker_code": "TRADING_DAY_EXECUTION_CONTROL_PLANE_PAPER_DAY_OUTPUT_MISSING_OR_INVALID",
                    "first_true_blocker_artifact_path": str(paper_day_path) if paper_day_path.exists() else "",
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
                    "system_ready": bool(
                        control_state.get("system_ready") is True or ledger_payload.get("system_ready") is True
                    ),
                    "submission_authorized": bool(
                        control_state.get("submission_authorized") is True
                        or ledger_payload.get("submission_authorized") is True
                    ),
                }
                startup_status = str(startup_proof_payload.get("status") or "").strip().upper() or "NOT_EVALUATED"
                startup_proof_result = {
                    "startup_proof_validation_path": str(startup_proof_ref.path),
                    "startup_proof_validation_status": startup_status,
                    "agreement_with_supporting_authority": (
                        (
                            supporting_session_authority["ledger_authority_status"] == "GRANTED"
                            and startup_status == "STARTUP_READY"
                        )
                        or (
                            supporting_session_authority["ledger_authority_status"] == "DENIED"
                            and startup_status == "STARTUP_BLOCKED"
                        )
                    ),
                }
                canonical_regeneration_results = [
                    _row_from_surface(
                        logical_name="startup_materialization_v1",
                        ref=startup_ref,
                        payload=startup_payload,
                        return_code=supporting_rows.get("startup_materialization_v1", {}).get("return_code"),
                    ),
                    _row_from_surface(
                        logical_name="paper_trading_posture_v1",
                        ref=posture_ref,
                        payload=posture_payload,
                        return_code=supporting_rows.get("paper_trading_posture_v1", {}).get("return_code"),
                    ),
                    _row_from_surface(
                        logical_name="submit_boundary_status_v1",
                        ref=boundary_ref,
                        payload=boundary_payload,
                        return_code=supporting_rows.get("submit_boundary_status_v1", {}).get("return_code"),
                    ),
                    _row_from_surface(
                        logical_name="paper_session_ledger_v1",
                        ref=ledger_ref,
                        payload=ledger_payload,
                        return_code=supporting_rows.get("paper_session_ledger_v1", {}).get("return_code"),
                    ),
                    _row_from_surface(
                        logical_name="startup_proof_validation_v1",
                        ref=startup_proof_ref,
                        payload=startup_proof_payload,
                        return_code=supporting_rows.get("startup_proof_validation_v1", {}).get("return_code"),
                    ),
                ]
                for payload in (startup_payload, posture_payload, boundary_payload, startup_proof_payload):
                    for code in (payload.get("blocking_codes") or []):
                        if str(code).strip():
                            blocking_codes.add(str(code).strip())
                for code in (control_state.get("blocking_codes") or []):
                    if str(code).strip():
                        blocking_codes.add(str(code).strip())
            except Exception:
                blocking_codes.add("TRADING_DAY_EXECUTION_CONTROL_PLANE_SUPPORTING_CANONICAL_OUTPUT_MISSING_OR_INVALID")
                if not first_true_blocker["first_true_blocker_code"]:
                    first_true_blocker = {
                        "first_true_blocker_code": "TRADING_DAY_EXECUTION_CONTROL_PLANE_SUPPORTING_CANONICAL_OUTPUT_MISSING_OR_INVALID",
                        "first_true_blocker_artifact_path": str(ledger_path) if ledger_path.exists() else "",
                        "blocker_classification": "REGENERATION_DEFECT",
                    }
                final_start_decision = "BLOCKED_BY_DEFECT"

        if final_start_decision != "BLOCKED_BY_DEFECT":
            trading_day_decision = supporting_daily_controls["trading_day_final_start_decision"]
            ledger_status = supporting_session_authority["ledger_authority_status"]
            startup_status = startup_proof_result["startup_proof_validation_status"]
            agreement = startup_proof_result["agreement_with_supporting_authority"] is True
            if (
                trading_day_decision == "READY_NOW"
                and ledger_status == "GRANTED"
                and startup_status == "STARTUP_READY"
                and agreement
            ):
                final_start_decision = "READY_NOW"
            elif (
                trading_day_decision == "BLOCKED_VALID"
                and first_true_blocker["blocker_classification"]
                in {"UPSTREAM_PREREQUISITE", "CANONICAL_POLICY_OR_INPUT", "SUPPORTING_AUTHORITY_DENY"}
            ) or ledger_status == "DENIED":
                final_start_decision = "BLOCKED_VALID"
            elif trading_day_decision == "BLOCKED_BY_DEFECT" or trading_day_result["return_code"] == 3:
                final_start_decision = "BLOCKED_BY_DEFECT"
            else:
                blocking_codes.add("TRADING_DAY_EXECUTION_CONTROL_PLANE_SUPPORTING_DECISION_INCONSISTENT")
                if not first_true_blocker["first_true_blocker_code"]:
                    first_true_blocker = {
                        "first_true_blocker_code": "TRADING_DAY_EXECUTION_CONTROL_PLANE_SUPPORTING_DECISION_INCONSISTENT",
                        "first_true_blocker_artifact_path": str(trading_day_path) if trading_day_path.exists() else "",
                        "blocker_classification": "REGENERATION_DEFECT",
                    }
                final_start_decision = "BLOCKED_BY_DEFECT"
    else:
        canonical_regeneration_results = _skipped_regeneration_results()
        first_true_blocker = {
            "first_true_blocker_code": _first_nonempty(sorted(prerequisite_blocking_codes)),
            "first_true_blocker_artifact_path": str(completeness_path) if completeness_path.exists() else "",
            "blocker_classification": "UPSTREAM_PREREQUISITE",
        }
        final_start_decision = "BLOCKED_VALID"

    if final_start_decision == "READY_NOW":
        blocking_codes = set()
        first_true_blocker = {
            "first_true_blocker_code": "",
            "first_true_blocker_artifact_path": "",
            "blocker_classification": "UNKNOWN",
        }

    if not first_true_blocker["first_true_blocker_code"]:
        first_true_blocker["first_true_blocker_code"] = _first_nonempty(sorted(blocking_codes))
    if not first_true_blocker["first_true_blocker_artifact_path"]:
        if supporting_session_authority["paper_session_ledger_path"]:
            first_true_blocker["first_true_blocker_artifact_path"] = supporting_session_authority["paper_session_ledger_path"]
        elif upstream_completeness["intents_day_completeness_ref"]:
            first_true_blocker["first_true_blocker_artifact_path"] = upstream_completeness["intents_day_completeness_ref"]
    if first_true_blocker["blocker_classification"] == "UNKNOWN":
        if final_start_decision == "BLOCKED_BY_DEFECT":
            first_true_blocker["blocker_classification"] = "REGENERATION_DEFECT"
        elif final_start_decision == "BLOCKED_VALID" and upstream_completeness["completeness_status"] != "COMPLETE":
            first_true_blocker["blocker_classification"] = "UPSTREAM_PREREQUISITE"

    supersedes_prior_control_plane = bool(prior_payload)
    prior_control_plane_id = str((prior_payload or {}).get("execution_control_plane_id") or "").strip()
    prior_day_attempt_id = str((prior_payload or {}).get("day_attempt_id") or "").strip()

    control_plane_parts = {
        "day_utc": day,
        "day_attempt_id": day_attempt_id,
        "evaluated_at_utc": evaluated_at_utc,
        "supersedes_prior_control_plane": supersedes_prior_control_plane,
        "prior_control_plane_id": prior_control_plane_id,
        "prior_day_attempt_id": prior_day_attempt_id,
        "upstream_completeness": upstream_completeness,
        "supporting_daily_controls": supporting_daily_controls,
        "canonical_regeneration_results": canonical_regeneration_results,
        "supporting_session_authority": supporting_session_authority,
        "first_true_blocker": first_true_blocker,
        "startup_proof_result": startup_proof_result,
        "final_start_decision": final_start_decision,
        "blocking_codes": sorted(blocking_codes),
    }
    execution_control_plane_id = _execution_control_plane_id(day, control_plane_parts)
    payload = {
        "schema_id": "trading_day_execution_control_plane",
        "schema_version": "v1",
        "authority_scope": "SUPPORTING_TRADING_DAY_EXECUTION_CONTROL_ARTIFACT",
        "day_utc": day,
        "day_attempt_id": day_attempt_id,
        "execution_control_plane_id": execution_control_plane_id,
        "evaluated_at_utc": evaluated_at_utc,
        "supersedes_prior_control_plane": supersedes_prior_control_plane,
        "prior_control_plane_id": prior_control_plane_id,
        "prior_day_attempt_id": prior_day_attempt_id,
        "producer": producer_block_v1(module="ops/tools/run_trading_day_execution_control_plane_v1.py"),
        "upstream_completeness": upstream_completeness,
        "supporting_daily_controls": supporting_daily_controls,
        "canonical_regeneration_results": canonical_regeneration_results,
        "supporting_session_authority": supporting_session_authority,
        "first_true_blocker": first_true_blocker,
        "startup_proof_result": startup_proof_result,
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
            "execution_control_plane_id": execution_control_plane_id,
            "day_attempt_id": day_attempt_id,
            "first_true_blocker_code": first_true_blocker["first_true_blocker_code"],
            "ledger_id": supporting_session_authority["ledger_id"],
            "non_authority_notice": (
                "Supporting migration-era artifact only. "
                "Do not treat trading_day_execution_control_plane_v1 as the top-level daily startup authority when trading_day_state_machine_v1 exists."
            ),
        },
    }
    ref = atomic_write_validated_json_v1(
        path=output_path,
        payload=payload,
        schema_relpath=OUTPUT_SCHEMA_RELPATH_V1,
    )
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "execution_control_plane_id": execution_control_plane_id,
                "day_attempt_id": day_attempt_id,
                "final_start_decision": final_start_decision,
                "first_true_blocker_code": first_true_blocker["first_true_blocker_code"],
                "ledger_id": supporting_session_authority["ledger_id"],
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
