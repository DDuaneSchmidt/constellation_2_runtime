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
    atomic_write_validated_json_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    read_intents_day_completeness_ref_v1,
    read_json_object_v1,
    read_paper_session_ledger_ref_v1,
    resolve_fact_plane_truth_root_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_intents_day_completeness_path,
    resolve_paper_day_control_plane_path,
    resolve_paper_session_ledger_path,
    resolve_paper_trading_posture_path,
    resolve_startup_materialization_path,
    resolve_startup_proof_validation_path,
    resolve_submit_boundary_status_path,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


OUTPUT_SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_day_control_plane.v1.schema.json"
STARTUP_PROOF_SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/startup_proof_validation.v1.schema.json"
STARTUP_MATERIALIZATION_TOOL = (REPO_ROOT / "ops/tools/run_startup_materialization_v1.py").resolve()
PAPER_TRADING_POSTURE_TOOL = (REPO_ROOT / "ops/tools/run_paper_trading_posture_v1.py").resolve()
SUBMIT_BOUNDARY_STATUS_TOOL = (REPO_ROOT / "ops/tools/run_submit_boundary_status_v1.py").resolve()
PAPER_SESSION_LEDGER_TOOL = (REPO_ROOT / "ops/tools/run_paper_session_ledger_v1.py").resolve()
STARTUP_PROOF_VALIDATION_TOOL = (REPO_ROOT / "ops/tools/run_startup_proof_validation_v1.py").resolve()
INTENTS_DAY_COMPLETENESS_TOOL = (REPO_ROOT / "ops/tools/run_intents_day_completeness_v1.py").resolve()


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


def _parse_json_stdout(result: dict[str, Any]) -> dict[str, Any]:
    stdout = str(result.get("stdout") or "").strip()
    if not stdout:
        return {}
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _artifact_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    payload = read_json_object_v1(path)
    return payload


def _artifact_result(*, logical_name: str, path: Path, result: dict[str, Any], status_field: str) -> dict[str, Any]:
    payload = _artifact_payload(path)
    status = "MISSING_OUTPUT"
    produced_at = ""
    if payload is not None:
        status = str(payload.get(status_field) or "").strip() or "UNKNOWN"
        produced_at = str(payload.get("produced_at_utc") or payload.get("produced_utc") or "").strip()
    digest = sha256_file_v1(path) if path.exists() and path.is_file() else ""
    return {
        "logical_name": logical_name,
        "path": str(path) if path.exists() else "",
        "status": status,
        "return_code": result.get("return_code"),
        "digest": digest,
        "produced_at": produced_at,
    }


def _legacy_surface_rows(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    report_root = (truth_root / "reports").resolve()
    surfaces = [
        ("trading_day_state_v1", report_root / "trading_day_state_v1" / day_utc / "trading_day_state.v1.json"),
        ("session_readiness_refresh_v1", report_root / "session_readiness_refresh_v1" / day_utc / "session_readiness_refresh.v1.json"),
        ("paper_session_admission_certificate_v1", report_root / "paper_session_admission_certificate_v1" / day_utc / "paper_session_admission_certificate.v1.json"),
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


def _first_nonempty(values: list[str]) -> str:
    for value in values:
        if str(value).strip():
            return str(value).strip()
    return ""


def _first_blocker_from_payload(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return ""
    control_state = payload.get("control_state")
    if isinstance(control_state, dict):
        for code in control_state.get("blocking_codes") or []:
            if str(code).strip():
                return str(code).strip()
    evidence_freeze = payload.get("evidence_freeze")
    if isinstance(evidence_freeze, dict):
        for code in evidence_freeze.get("blocking_codes") or []:
            if str(code).strip():
                return str(code).strip()
    for key in ("blocking_codes", "blocking_reason_codes", "reason_codes"):
        for code in payload.get(key) or []:
            if str(code).strip():
                return str(code).strip()
    return ""


def _build_summary(*, day_utc: str, decision: str, blocker: str) -> str:
    if decision == "READY_NOW":
        return f"Paper trading is startable for {day_utc}. The supporting paper-day control plane is READY_NOW."
    if blocker:
        return f"Paper trading is blocked for {day_utc}. First true canonical blocker: {blocker}."
    return f"Paper trading is blocked for {day_utc}. The canonical day-start control plane did not reach READY_NOW."


def _startup_proof_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    payload = read_json_object_v1(path)
    validate_against_repo_schema_v1(payload, REPO_ROOT, STARTUP_PROOF_SCHEMA_RELPATH_V1)
    return payload


def _control_plane_identity(day_utc: str, evaluated_at_utc: str, parts: dict[str, Any]) -> tuple[str, str]:
    startup_attempt_id = f"paper_day_start_attempt:{day_utc}:{evaluated_at_utc}"
    digest = _sha256_bytes(canonical_json_bytes_v1(parts))
    control_plane_id = f"paper_day_control_plane:{day_utc}:{digest[:16]}"
    return startup_attempt_id, control_plane_id


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_paper_day_control_plane_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    evaluated_at_utc = now_utc_iso_v1()
    blocking_codes: set[str] = set()
    regeneration_results: list[dict[str, Any]] = []
    decision = "BLOCKED_BY_DEFECT"
    ledger_path = resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day)
    startup_proof_path = resolve_startup_proof_validation_path(truth_root=truth_root, day_utc=day)

    prereq_result = _run(
        [sys.executable, str(INTENTS_DAY_COMPLETENESS_TOOL), "--day_utc", day, "--truth_root", str(truth_root)],
        truth_root=truth_root,
    )
    prereq_path = resolve_intents_day_completeness_path(truth_root=truth_root, day_utc=day)
    prerequisite_status = "DEFECT"
    prerequisite_blocking_codes: list[str] = []
    prerequisite_first_missing = ""
    if prereq_path.exists() and prereq_path.is_file():
        prereq_ref = read_intents_day_completeness_ref_v1(truth_root=truth_root, day_utc=day)
        prereq_payload = dict(prereq_ref.payload)
        prerequisite_blocking_codes = sorted(
            {str(code).strip() for code in prereq_payload.get("blocking_codes") or [] if str(code).strip()}
        )
        prerequisite_first_missing = _first_nonempty(list(prereq_payload.get("missing_inputs") or []))
        completeness_status = str(prereq_payload.get("completeness_status") or "").strip()
        if prereq_result["return_code"] == 0 and completeness_status == "COMPLETE":
            prerequisite_status = "PASS"
        elif completeness_status in {"NO_INTENTS_DECLARED", "INCOMPLETE", "MALFORMED", "UNKNOWN"}:
            prerequisite_status = "BLOCKED"
        else:
            prerequisite_status = "DEFECT"
    else:
        prerequisite_blocking_codes = ["PAPER_DAY_CONTROL_PLANE_PREREQUISITE_OUTPUT_MISSING"]

    authority_result = {
        "ledger_path": "",
        "ledger_id": "",
        "ledger_authority_status": "NOT_EVALUATED",
        "ledger_evidence_status": "NOT_EVALUATED",
        "first_true_blocker_code": "",
        "first_true_blocker_artifact_path": "",
    }
    startup_proof_result = {
        "startup_proof_validation_path": "",
        "startup_proof_validation_status": "NOT_EVALUATED",
        "agreement_with_ledger": False,
    }

    if prerequisite_status == "PASS":
        tool_specs = [
            (
                "startup_materialization_v1",
                STARTUP_MATERIALIZATION_TOOL,
                resolve_startup_materialization_path(truth_root=truth_root, day_utc=day),
                "status",
            ),
            (
                "paper_trading_posture_v1",
                PAPER_TRADING_POSTURE_TOOL,
                resolve_paper_trading_posture_path(truth_root=truth_root, day_utc=day),
                "posture_status",
            ),
            (
                "submit_boundary_status_v1",
                SUBMIT_BOUNDARY_STATUS_TOOL,
                resolve_submit_boundary_status_path(truth_root=truth_root, day_utc=day),
                "boundary_status",
            ),
            (
                "paper_session_ledger_v1",
                PAPER_SESSION_LEDGER_TOOL,
                ledger_path,
                "authority_status",
            ),
            (
                "startup_proof_validation_v1",
                STARTUP_PROOF_VALIDATION_TOOL,
                startup_proof_path,
                "status",
            ),
        ]
        regeneration_defect = False
        for logical_name, tool_path, output_path, status_field in tool_specs:
            result = _run(
                [sys.executable, str(tool_path), "--day_utc", day, "--truth_root", str(truth_root)],
                truth_root=truth_root,
            )
            regeneration_results.append(
                _artifact_result(
                    logical_name=logical_name,
                    path=output_path,
                    result=result,
                    status_field=status_field,
                )
            )
            if not output_path.exists() or not output_path.is_file():
                regeneration_defect = True
                blocking_codes.add(f"PAPER_DAY_CONTROL_PLANE_OUTPUT_MISSING:{logical_name}")

        if ledger_path.exists() and ledger_path.is_file():
            ledger_ref = read_paper_session_ledger_ref_v1(truth_root=truth_root, day_utc=day)
            ledger_payload = dict(ledger_ref.payload)
            control_state = ledger_payload.get("control_state")
            if not isinstance(control_state, dict):
                control_state = {}
            authority_result = {
                "ledger_path": str(ledger_ref.path),
                "ledger_id": str(ledger_payload.get("ledger_id") or "").strip(),
                "ledger_authority_status": str(
                    ledger_payload.get("authority_status") or control_state.get("authority_status") or ""
                ).strip().upper() or "NOT_EVALUATED",
                "ledger_evidence_status": str(
                    (ledger_payload.get("evidence_freeze") or {}).get("overall_evidence_status") or ""
                ).strip().upper() or "NOT_EVALUATED",
                "first_true_blocker_code": _first_blocker_from_payload(ledger_payload),
                "first_true_blocker_artifact_path": str(ledger_ref.path),
            }
            for code in (ledger_payload.get("control_state") or {}).get("blocking_codes") or []:
                if str(code).strip():
                    blocking_codes.add(str(code).strip())
        else:
            regeneration_defect = True

        startup_payload = _startup_proof_payload(startup_proof_path) if startup_proof_path.exists() else None
        if startup_payload is not None:
            startup_status = str(startup_payload.get("status") or "").strip().upper() or "UNKNOWN"
            startup_proof_result = {
                "startup_proof_validation_path": str(startup_proof_path),
                "startup_proof_validation_status": startup_status,
                "agreement_with_ledger": (
                    (authority_result["ledger_authority_status"] == "GRANTED" and startup_status == "STARTUP_READY")
                    or (authority_result["ledger_authority_status"] == "DENIED" and startup_status == "STARTUP_BLOCKED")
                ),
            }
            for code in startup_payload.get("blocking_codes") or []:
                if str(code).strip():
                    blocking_codes.add(str(code).strip())
        else:
            regeneration_defect = True
            blocking_codes.add("PAPER_DAY_CONTROL_PLANE_STARTUP_PROOF_OUTPUT_MISSING")

        if regeneration_defect:
            decision = "BLOCKED_BY_DEFECT"
        elif (
            authority_result["ledger_authority_status"] == "GRANTED"
            and startup_proof_result["startup_proof_validation_status"] == "STARTUP_READY"
            and startup_proof_result["agreement_with_ledger"] is True
        ):
            decision = "READY_NOW"
        elif authority_result["ledger_authority_status"] == "DENIED":
            decision = "BLOCKED_VALID"
        else:
            decision = "BLOCKED_BY_DEFECT"
    elif prerequisite_status == "BLOCKED":
        decision = "BLOCKED_VALID"
        blocking_codes.update(prerequisite_blocking_codes)
        regeneration_results.extend(
            [
                {
                    "logical_name": logical_name,
                    "path": "",
                    "status": "SKIPPED_PREREQUISITE_BLOCKED",
                    "return_code": None,
                    "digest": "",
                    "produced_at": "",
                }
                for logical_name in (
                    "startup_materialization_v1",
                    "paper_trading_posture_v1",
                    "submit_boundary_status_v1",
                    "paper_session_ledger_v1",
                    "startup_proof_validation_v1",
                )
            ]
        )
        authority_result["first_true_blocker_code"] = _first_nonempty(prerequisite_blocking_codes)
        authority_result["first_true_blocker_artifact_path"] = str(prereq_path)
    else:
        decision = "BLOCKED_BY_DEFECT"
        blocking_codes.update(prerequisite_blocking_codes or ["PAPER_DAY_CONTROL_PLANE_PREREQUISITE_DEFECT"])
        authority_result["first_true_blocker_code"] = _first_nonempty(prerequisite_blocking_codes)
        authority_result["first_true_blocker_artifact_path"] = str(prereq_path) if prereq_path.exists() else ""

    if not authority_result["first_true_blocker_code"]:
        authority_result["first_true_blocker_code"] = _first_nonempty(sorted(blocking_codes))
    if not authority_result["first_true_blocker_artifact_path"] and prereq_path.exists():
        authority_result["first_true_blocker_artifact_path"] = str(prereq_path)

    control_plane_parts = {
        "day_utc": day,
        "evaluated_at_utc": evaluated_at_utc,
        "prerequisite_status": prerequisite_status,
        "regeneration_results": regeneration_results,
        "authority_result": authority_result,
        "startup_proof_result": startup_proof_result,
        "final_start_decision": decision,
        "blocking_codes": sorted(blocking_codes | set(prerequisite_blocking_codes)),
    }
    startup_attempt_id, control_plane_id = _control_plane_identity(day, evaluated_at_utc, control_plane_parts)
    payload = {
        "schema_id": "paper_day_control_plane",
        "schema_version": "v1",
        "authority_scope": "SUPPORTING_DAY_CONTROL_ARTIFACT",
        "day_utc": day,
        "startup_attempt_id": startup_attempt_id,
        "control_plane_id": control_plane_id,
        "evaluated_at_utc": evaluated_at_utc,
        "producer": producer_block_v1(module="ops/tools/run_paper_day_control_plane_v1.py"),
        "prerequisite_gate": {
            "prerequisite_status": prerequisite_status,
            "prerequisite_blocking_codes": sorted(prerequisite_blocking_codes),
            "prerequisite_artifact_refs": [str(prereq_path)] if prereq_path.exists() else [],
            "first_missing_prerequisite": prerequisite_first_missing,
        },
        "canonical_regeneration_results": regeneration_results,
        "authority_result": authority_result,
        "startup_proof_result": startup_proof_result,
        "final_start_decision": decision,
        "blocking_codes": sorted(blocking_codes | set(prerequisite_blocking_codes)),
        "human_readable_summary": _build_summary(
            day_utc=day,
            decision=decision,
            blocker=authority_result["first_true_blocker_code"],
        ),
        "ignored_legacy_surfaces": _legacy_surface_rows(truth_root=truth_root, day_utc=day),
        "suppression_reason": "NON_AUTHORITATIVE_FOR_STARTUP",
    }
    ref = atomic_write_validated_json_v1(
        path=resolve_paper_day_control_plane_path(truth_root=truth_root, day_utc=day),
        payload=payload,
        schema_relpath=OUTPUT_SCHEMA_RELPATH_V1,
    )
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "control_plane_id": control_plane_id,
                "final_start_decision": decision,
                "ledger_id": authority_result["ledger_id"],
                "first_true_blocker_code": authority_result["first_true_blocker_code"],
            },
            sort_keys=True,
        )
    )
    if decision == "READY_NOW":
        return 0
    if decision == "BLOCKED_VALID":
        return 2
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
