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
    read_json_object_v1,
    read_intents_day_completeness_ref_v1,
    read_paper_day_control_plane_ref_v1,
    read_paper_session_ledger_ref_v1,
    read_startup_proof_validation_ref_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_intents_day_completeness_path,
    resolve_paper_day_control_plane_path,
    resolve_paper_session_ledger_path,
    resolve_startup_proof_validation_path,
    resolve_trading_day_control_plane_path,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


OUTPUT_SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_control_plane.v1.schema.json"
INTENTS_DAY_COMPLETENESS_TOOL = (REPO_ROOT / "ops/tools/run_intents_day_completeness_v1.py").resolve()
PAPER_DAY_CONTROL_PLANE_TOOL = (REPO_ROOT / "ops/tools/run_paper_day_control_plane_v1.py").resolve()

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


def _parse_json_stdout(result: dict[str, Any]) -> dict[str, Any]:
    stdout = str(result.get("stdout") or "").strip()
    if not stdout:
        return {}
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _legacy_surface_rows(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    report_root = (truth_root / "reports").resolve()
    surfaces = [
        ("paper_day_control_plane_v1", report_root / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"),
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


def _load_prior_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = read_json_object_v1(path)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _build_summary(*, day_utc: str, decision: str, blocker: str) -> str:
    if decision == "READY_NOW":
        return f"Paper trading is startable for {day_utc}. The supporting trading-day control plane is READY_NOW."
    if blocker:
        return f"Paper trading is blocked for {day_utc}. First true canonical blocker: {blocker}."
    return f"Paper trading is blocked for {day_utc}. The canonical trading-day control plane did not reach READY_NOW."


def _day_attempt_id(day_utc: str, evaluated_at_utc: str) -> str:
    return f"trading_day_start_attempt:{day_utc}:{evaluated_at_utc}"


def _control_plane_id(day_utc: str, parts: dict[str, Any]) -> str:
    digest = _sha256_bytes(canonical_json_bytes_v1(parts))
    return f"trading_day_control_plane:{day_utc}:{digest[:16]}"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_trading_day_control_plane_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    evaluated_at_utc = now_utc_iso_v1()
    day_attempt_id = _day_attempt_id(day, evaluated_at_utc)
    output_path = resolve_trading_day_control_plane_path(truth_root=truth_root, day_utc=day)
    prior_payload = _load_prior_payload(output_path)
    blocking_codes: set[str] = set()

    completeness_ref_path = resolve_intents_day_completeness_path(truth_root=truth_root, day_utc=day)
    paper_day_path = resolve_paper_day_control_plane_path(truth_root=truth_root, day_utc=day)
    ledger_path = resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day)
    startup_proof_path = resolve_startup_proof_validation_path(truth_root=truth_root, day_utc=day)

    completeness_result = _run(
        [sys.executable, str(INTENTS_DAY_COMPLETENESS_TOOL), "--day_utc", day, "--truth_root", str(truth_root)],
        truth_root=truth_root,
    )

    upstream_completeness = {
        "intents_day_completeness_ref": "",
        "completeness_status": "DEFECT",
        "prerequisite_blocking_codes": [],
        "first_missing_prerequisite": "",
    }
    supporting_daily_control = {
        "paper_day_control_plane_path": "",
        "paper_day_control_plane_id": "",
        "paper_day_startup_attempt_id": "",
        "paper_day_final_start_decision": "NOT_EVALUATED",
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
    canonical_regeneration_results: list[dict[str, Any]] = []
    final_start_decision = ""

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
        blocking_codes.add("TRADING_DAY_CONTROL_PLANE_INTENTS_COMPLETENESS_OUTPUT_MISSING_OR_INVALID")

    completeness_status = upstream_completeness["completeness_status"]
    prerequisite_blocking_codes = set(upstream_completeness["prerequisite_blocking_codes"])
    blocking_codes.update(prerequisite_blocking_codes)

    if prereq_payload is None:
        first_true_blocker = {
            "first_true_blocker_code": "TRADING_DAY_CONTROL_PLANE_INTENTS_COMPLETENESS_OUTPUT_MISSING_OR_INVALID",
            "first_true_blocker_artifact_path": str(completeness_ref_path) if completeness_ref_path.exists() else "",
            "blocker_classification": "REGENERATION_DEFECT",
        }
        canonical_regeneration_results = _skipped_regeneration_results()
        final_start_decision = "BLOCKED_BY_DEFECT"
    elif completeness_result["return_code"] == 0 and completeness_status == "COMPLETE":
        paper_day_result = _run(
            [sys.executable, str(PAPER_DAY_CONTROL_PLANE_TOOL), "--day_utc", day, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        )
        try:
            paper_day_ref = read_paper_day_control_plane_ref_v1(truth_root=truth_root, day_utc=day)
            paper_day_payload = dict(paper_day_ref.payload)
            supporting_daily_control = {
                "paper_day_control_plane_path": str(paper_day_ref.path),
                "paper_day_control_plane_id": str(paper_day_payload.get("control_plane_id") or "").strip(),
                "paper_day_startup_attempt_id": str(paper_day_payload.get("startup_attempt_id") or "").strip(),
                "paper_day_final_start_decision": str(paper_day_payload.get("final_start_decision") or "").strip().upper()
                or "NOT_EVALUATED",
            }
            canonical_regeneration_results = list(paper_day_payload.get("canonical_regeneration_results") or [])
            paper_day_authority = paper_day_payload.get("authority_result")
            if not isinstance(paper_day_authority, dict):
                paper_day_authority = {}
            first_true_blocker = {
                "first_true_blocker_code": str(paper_day_authority.get("first_true_blocker_code") or "").strip(),
                "first_true_blocker_artifact_path": str(
                    paper_day_authority.get("first_true_blocker_artifact_path") or ""
                ).strip(),
                "blocker_classification": "UNKNOWN",
            }
            blocking_codes.update(
                str(code).strip()
                for code in (paper_day_payload.get("blocking_codes") or [])
                if str(code).strip()
            )
        except Exception:
            blocking_codes.add("TRADING_DAY_CONTROL_PLANE_PAPER_DAY_OUTPUT_MISSING_OR_INVALID")
            first_true_blocker = {
                "first_true_blocker_code": "TRADING_DAY_CONTROL_PLANE_PAPER_DAY_OUTPUT_MISSING_OR_INVALID",
                "first_true_blocker_artifact_path": str(paper_day_path) if paper_day_path.exists() else "",
                "blocker_classification": "REGENERATION_DEFECT",
            }
            final_start_decision = "BLOCKED_BY_DEFECT"
            paper_day_payload = None

        if final_start_decision != "BLOCKED_BY_DEFECT":
            try:
                ledger_ref = read_paper_session_ledger_ref_v1(truth_root=truth_root, day_utc=day)
                ledger_payload = dict(ledger_ref.payload)
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
                blocking_codes.update(
                    str(code).strip()
                    for code in (control_state.get("blocking_codes") or [])
                    if str(code).strip()
                )
            except Exception:
                blocking_codes.add("TRADING_DAY_CONTROL_PLANE_LEDGER_OUTPUT_MISSING_OR_INVALID")
                if not first_true_blocker["first_true_blocker_code"]:
                    first_true_blocker = {
                        "first_true_blocker_code": "TRADING_DAY_CONTROL_PLANE_LEDGER_OUTPUT_MISSING_OR_INVALID",
                        "first_true_blocker_artifact_path": str(ledger_path) if ledger_path.exists() else "",
                        "blocker_classification": "REGENERATION_DEFECT",
                    }
                final_start_decision = "BLOCKED_BY_DEFECT"

        if final_start_decision != "BLOCKED_BY_DEFECT":
            try:
                startup_proof_ref = read_startup_proof_validation_ref_v1(truth_root=truth_root, day_utc=day)
                startup_proof_payload = dict(startup_proof_ref.payload)
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
                blocking_codes.update(
                    str(code).strip()
                    for code in (startup_proof_payload.get("blocking_codes") or [])
                    if str(code).strip()
                )
            except Exception:
                blocking_codes.add("TRADING_DAY_CONTROL_PLANE_STARTUP_PROOF_OUTPUT_MISSING_OR_INVALID")
                if not first_true_blocker["first_true_blocker_code"]:
                    first_true_blocker = {
                        "first_true_blocker_code": "TRADING_DAY_CONTROL_PLANE_STARTUP_PROOF_OUTPUT_MISSING_OR_INVALID",
                        "first_true_blocker_artifact_path": str(startup_proof_path) if startup_proof_path.exists() else "",
                        "blocker_classification": "REGENERATION_DEFECT",
                    }
                final_start_decision = "BLOCKED_BY_DEFECT"

        if final_start_decision != "BLOCKED_BY_DEFECT":
            paper_day_decision = supporting_daily_control["paper_day_final_start_decision"]
            ledger_status = supporting_session_authority["ledger_authority_status"]
            startup_status = startup_proof_result["startup_proof_validation_status"]
            agreement = startup_proof_result["agreement_with_supporting_authority"] is True
            if (
                paper_day_decision == "READY_NOW"
                and ledger_status == "GRANTED"
                and startup_status == "STARTUP_READY"
                and agreement
            ):
                final_start_decision = "READY_NOW"
            elif paper_day_decision == "BLOCKED_VALID" and ledger_status == "DENIED":
                final_start_decision = "BLOCKED_VALID"
            elif paper_day_decision == "BLOCKED_BY_DEFECT" or paper_day_result["return_code"] == 3:
                final_start_decision = "BLOCKED_BY_DEFECT"
            elif ledger_status == "DENIED":
                final_start_decision = "BLOCKED_VALID"
            else:
                blocking_codes.add("TRADING_DAY_CONTROL_PLANE_SUPPORTING_DECISION_INCONSISTENT")
                if not first_true_blocker["first_true_blocker_code"]:
                    first_true_blocker = {
                        "first_true_blocker_code": "TRADING_DAY_CONTROL_PLANE_SUPPORTING_DECISION_INCONSISTENT",
                        "first_true_blocker_artifact_path": str(paper_day_path) if paper_day_path.exists() else "",
                        "blocker_classification": "REGENERATION_DEFECT",
                    }
                final_start_decision = "BLOCKED_BY_DEFECT"

            if first_true_blocker["blocker_classification"] == "UNKNOWN":
                if final_start_decision == "BLOCKED_BY_DEFECT":
                    first_true_blocker["blocker_classification"] = "REGENERATION_DEFECT"
                elif ledger_status == "DENIED":
                    first_true_blocker["blocker_classification"] = "SUPPORTING_AUTHORITY_DENY"
                elif first_true_blocker["first_true_blocker_code"]:
                    first_true_blocker["blocker_classification"] = "CANONICAL_POLICY_OR_INPUT"

    else:
        canonical_regeneration_results = _skipped_regeneration_results()
        first_true_blocker = {
            "first_true_blocker_code": _first_nonempty(sorted(prerequisite_blocking_codes)),
            "first_true_blocker_artifact_path": str(completeness_ref_path) if completeness_ref_path.exists() else "",
            "blocker_classification": "UPSTREAM_PREREQUISITE",
        }
        final_start_decision = "BLOCKED_VALID"

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

    supersession = {
        "semantics": "LATEST_AUTHORITATIVE_SAME_DAY_PATH",
        "supersedes_prior_control_plane": bool(prior_payload),
        "prior_control_plane_id": str((prior_payload or {}).get("control_plane_id") or "").strip(),
        "prior_day_attempt_id": str((prior_payload or {}).get("day_attempt_id") or "").strip(),
    }

    control_plane_parts = {
        "day_utc": day,
        "day_attempt_id": day_attempt_id,
        "evaluated_at_utc": evaluated_at_utc,
        "upstream_completeness": upstream_completeness,
        "supporting_daily_control": supporting_daily_control,
        "canonical_regeneration_results": canonical_regeneration_results,
        "supporting_session_authority": supporting_session_authority,
        "first_true_blocker": first_true_blocker,
        "startup_proof_result": startup_proof_result,
        "final_start_decision": final_start_decision,
        "blocking_codes": sorted(blocking_codes),
        "supersession": supersession,
    }
    control_plane_id = _control_plane_id(day, control_plane_parts)
    payload = {
        "schema_id": "trading_day_control_plane",
        "schema_version": "v1",
        "authority_scope": "SUPPORTING_TRADING_DAY_CONTROL_ARTIFACT",
        "day_utc": day,
        "day_attempt_id": day_attempt_id,
        "control_plane_id": control_plane_id,
        "evaluated_at_utc": evaluated_at_utc,
        "producer": producer_block_v1(module="ops/tools/run_trading_day_control_plane_v1.py"),
        "supersession": supersession,
        "upstream_completeness": upstream_completeness,
        "supporting_daily_control": supporting_daily_control,
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
        "suppression_reason": "NON_AUTHORITATIVE_FOR_STARTUP",
        "derived_daily_summary": {
                "summary_state": final_start_decision,
                "control_plane_id": control_plane_id,
                "day_attempt_id": day_attempt_id,
                "first_true_blocker_code": first_true_blocker["first_true_blocker_code"],
                "ledger_id": supporting_session_authority["ledger_id"],
                "non_authority_notice": (
                    "Supporting trading_day_control_plane_v1 artifact only. "
                    "Do not treat this as the top-level daily startup authority when trading_day_execution_control_plane_v1 exists."
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
                "control_plane_id": control_plane_id,
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
