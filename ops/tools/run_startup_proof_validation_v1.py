#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_idempotent_validated_json_v1,
    canonical_paper_session_id_v1,
    now_utc_iso_v1,
    read_paper_session_ledger_ref_v1,
    read_startup_materialization_ref_v1,
    repo_git_sha_v1,
    resolve_authoritative_repo_root_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_paper_session_ledger_path,
    resolve_startup_proof_validation_path,
)
from ops.tools.run_repo_authority_proof_v1 import collect_repo_authority_proof_v1


SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/startup_proof_validation.v1.schema.json"


def _producer_block() -> dict[str, str]:
    return {
        "repo": REPO_ROOT.name,
        "module": "ops/tools/run_startup_proof_validation_v1.py",
        "git_sha": repo_git_sha_v1(),
    }


def _check_row(
    *,
    logical_name: str,
    status: str,
    evidence_ref: str,
    blocking_codes: list[str],
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "logical_name": logical_name,
        "status": status,
        "evidence_ref": evidence_ref,
        "blocking_codes": sorted({str(code).strip() for code in blocking_codes if str(code).strip()}),
        "detail": dict(detail or {}),
    }


def _ledger_check(*, truth_root: Path, day_utc: str, session_id: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    try:
        ledger_ref = read_paper_session_ledger_ref_v1(truth_root=truth_root, day_utc=day_utc)
    except Exception as exc:
        return (
            _check_row(
                logical_name="paper_session_ledger_v1",
                status="FAIL",
                evidence_ref=str(resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day_utc)),
                blocking_codes=["STARTUP_PROOF_LEDGER_UNUSABLE"],
                detail={"error": f"{type(exc).__name__}:{exc}"},
            ),
            None,
        )
    payload = dict(ledger_ref.payload)
    codes: list[str] = []
    if str(payload.get("day_utc") or "").strip() != day_utc:
        codes.append("STARTUP_PROOF_LEDGER_DAY_MISMATCH")
    if str(payload.get("session_id") or "").strip() != session_id:
        codes.append("STARTUP_PROOF_LEDGER_SESSION_MISMATCH")
    if str(payload.get("authority_scope") or "").strip() != "CANONICAL_SESSION_LEDGER":
        codes.append("STARTUP_PROOF_LEDGER_SCOPE_INVALID")
    if str(payload.get("control_state", {}).get("authority_status") or "").strip().upper() != "GRANTED":
        codes.append("STARTUP_PROOF_LEDGER_AUTHORITY_DENIED")
    if str(payload.get("evidence_freeze", {}).get("overall_evidence_status") or "").strip().upper() != "READY":
        codes.append("STARTUP_PROOF_LEDGER_EVIDENCE_NOT_READY")
    return (
        _check_row(
            logical_name="paper_session_ledger_v1",
            status="PASS" if not codes else "FAIL",
            evidence_ref=str(ledger_ref.path),
            blocking_codes=codes,
            detail={
                "ledger_id": str(payload.get("ledger_id") or "").strip(),
                "authority_status": str(payload.get("control_state", {}).get("authority_status") or "").strip().upper(),
            },
        ),
        payload,
    )


def _startup_materialization_check(*, truth_root: Path, day_utc: str, session_id: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    try:
        startup_ref = read_startup_materialization_ref_v1(truth_root=truth_root, day_utc=day_utc)
    except Exception as exc:
        return (
            _check_row(
                logical_name="startup_materialization_v1",
                status="FAIL",
                evidence_ref="",
                blocking_codes=["STARTUP_PROOF_STARTUP_MATERIALIZATION_UNUSABLE"],
                detail={"error": f"{type(exc).__name__}:{exc}"},
            ),
            None,
        )
    payload = dict(startup_ref.payload)
    codes: list[str] = []
    if str(payload.get("day_utc") or "").strip() != day_utc:
        codes.append("STARTUP_PROOF_STARTUP_MATERIALIZATION_DAY_MISMATCH")
    if str(payload.get("session_id") or "").strip() != session_id:
        codes.append("STARTUP_PROOF_STARTUP_MATERIALIZATION_SESSION_MISMATCH")
    if str(payload.get("status") or "").strip().upper() != "SUCCESS":
        codes.append("STARTUP_PROOF_STARTUP_MATERIALIZATION_NOT_SUCCESS")
    if str(payload.get("freshness_verdict") or "").strip().upper() != "CURRENT":
        codes.append("STARTUP_PROOF_STARTUP_MATERIALIZATION_NOT_CURRENT")
    if str(payload.get("linkage_verdict") or "").strip().upper() != "LINKED":
        codes.append("STARTUP_PROOF_STARTUP_MATERIALIZATION_NOT_LINKED")
    return (
        _check_row(
            logical_name="startup_materialization_v1",
            status="PASS" if not codes else "FAIL",
            evidence_ref=str(startup_ref.path),
            blocking_codes=codes,
            detail={"status": str(payload.get("status") or "").strip().upper()},
        ),
        payload,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_startup_proof_validation_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    session_id = canonical_paper_session_id_v1(day_utc)
    produced_at_utc = now_utc_iso_v1()

    authoritative_repo_root = resolve_authoritative_repo_root_v1(REPO_ROOT)
    try:
        repo_proof_payload, repo_failures = collect_repo_authority_proof_v1(
            authoritative_repo_root=authoritative_repo_root,
            mode="authoritative_source_only",
        )
    except Exception as exc:
        repo_proof_payload = {
            "mode": "authoritative_source_only",
            "status": "FAIL",
            "error": f"{type(exc).__name__}:{exc}",
        }
        repo_failures = [f"STARTUP_PROOF_REPO_AUTHORITY_UNUSABLE:{type(exc).__name__}"]
    repo_check = _check_row(
        logical_name="repo_authority_proof_v1",
        status="PASS" if not repo_failures else "FAIL",
        evidence_ref=f"{authoritative_repo_root}/repo_role.v1.json",
        blocking_codes=["STARTUP_PROOF_REPO_AUTHORITY_INVALID"] if repo_failures else [],
        detail={
            "mode": str(repo_proof_payload.get("mode") or "").strip(),
            "status": str(repo_proof_payload.get("status") or "").strip(),
            "failures": list(repo_failures),
        },
    )
    startup_check, startup_payload = _startup_materialization_check(
        truth_root=truth_root,
        day_utc=day_utc,
        session_id=session_id,
    )
    ledger_check, ledger_payload = _ledger_check(
        truth_root=truth_root,
        day_utc=day_utc,
        session_id=session_id,
    )

    checks = [repo_check, startup_check, ledger_check]
    blocking_codes = sorted(
        {
            str(code).strip()
            for row in checks
            for code in row.get("blocking_codes") or []
            if str(code).strip()
        }
    )
    status = "STARTUP_READY" if not blocking_codes else "STARTUP_BLOCKED"
    payload = {
        "schema_id": "startup_proof_validation",
        "schema_version": "v1",
        "authority_scope": "NON_AUTHORITY_STARTUP_VALIDATION",
        "validation_scope": "BOD_CONTROL_PLANE_ONLY",
        "day_utc": day_utc,
        "session_id": session_id,
        "status": status,
        "ledger_ref": ledger_check["evidence_ref"],
        "ledger_id": str((ledger_payload or {}).get("ledger_id") or "").strip(),
        "ledger_authority_status": str((ledger_payload or {}).get("control_state", {}).get("authority_status") or "").strip().upper(),
        "checks": checks,
        "blocking_codes": blocking_codes,
        "producer": _producer_block(),
        "produced_at_utc": produced_at_utc,
        "non_authority_notice": (
            "Bounded startup validation only. paper_session_ledger_v1 remains the only "
            "authoritative paper-session control owner."
        ),
    }
    ref = atomic_write_idempotent_validated_json_v1(
        path=resolve_startup_proof_validation_path(truth_root=truth_root, day_utc=day_utc),
        payload=payload,
        schema_relpath=SCHEMA_RELPATH_V1,
        volatile_field_names=("produced_at_utc",),
    )
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "status": status,
                "ledger_id": str(payload.get("ledger_id") or "").strip(),
            },
            sort_keys=True,
        )
    )
    return 0 if status == "STARTUP_READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
