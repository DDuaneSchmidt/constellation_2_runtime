#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
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
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from ops.tools.run_repo_authority_proof_v1 import collect_repo_authority_proof_v1


SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/startup_proof_validation.v1.schema.json"
_STARTUP_PHASEC_VETO_FAIL_CLOSED_CODE = "STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"
_OPTIONS_SNAPSHOT_MISSING_PREFIX = "OPTIONS_SNAPSHOT_ROOT_MISSING"
_PAPER_STARTUP_TOLERANCE_REASON_CODE = "PAPER_START_ALLOWED_OPTIONS_SNAPSHOT_PENDING"
_PAPER_STARTUP_TOLERANCE_POLICY = "PAPER_ONLY_STARTUP_OPTIONS_SNAPSHOT_PENDING"


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


def _safe_read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _extract_phasec_veto_paths(startup_payload: dict[str, Any]) -> list[Path]:
    paths: list[Path] = []
    evidence = startup_payload.get("path_resolution_evidence")
    if isinstance(evidence, dict):
        pointer_raw = str(evidence.get("latest_active_attempt_path") or "").strip()
        if pointer_raw:
            pointer_path = Path(pointer_raw)
            pointer_payload = _safe_read_json(pointer_path)
            if isinstance(pointer_payload, dict):
                attempt_dir_raw = str(pointer_payload.get("attempt_dir") or "").strip()
                if attempt_dir_raw:
                    attempt_dir = Path(attempt_dir_raw)
                    if attempt_dir.exists() and attempt_dir.is_dir():
                        paths.extend(sorted(attempt_dir.glob("*.veto_record.v1.json"), key=lambda p: p.name))
    phasec_result = startup_payload.get("phasec_materializer_result")
    stdout = ""
    if isinstance(phasec_result, dict):
        stdout = str(phasec_result.get("stdout") or "")
    for match in re.finditer(r"path=(\S+\.veto_record\.v1\.json)", stdout):
        paths.append(Path(match.group(1)))
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in paths:
        resolved = str(candidate.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(candidate.resolve())
    return deduped


def _paper_startup_tolerance_for_options_snapshot(
    *,
    startup_payload: dict[str, Any],
    day_utc: str,
    session_id: str,
    environment: str,
) -> dict[str, Any]:
    env = str(environment or "").strip().upper()
    if env != "PAPER":
        return {"applied": False}
    if session_id != canonical_paper_session_id_v1(day_utc):
        return {"applied": False}

    startup_status = str(startup_payload.get("status") or "").strip().upper()
    if startup_status == "SUCCESS":
        return {"applied": False}

    startup_blocking_codes = [
        str(code).strip()
        for code in (startup_payload.get("blocking_codes") or [])
        if str(code).strip()
    ]
    if _STARTUP_PHASEC_VETO_FAIL_CLOSED_CODE not in startup_blocking_codes:
        return {"applied": False}

    for code in startup_blocking_codes:
        if code == _STARTUP_PHASEC_VETO_FAIL_CLOSED_CODE:
            continue
        if code.startswith("STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:OPTIONS_SNAPSHOT_ROOT_MISSING"):
            continue
        return {"applied": False}

    veto_paths = _extract_phasec_veto_paths(startup_payload)
    if not veto_paths:
        return {"applied": False}

    for veto_path in veto_paths:
        veto_payload = _safe_read_json(veto_path)
        if not isinstance(veto_payload, dict):
            return {"applied": False}
        reason_code = str(veto_payload.get("reason_code") or "").strip()
        reason_detail = str(veto_payload.get("reason_detail") or "").strip()
        if reason_code != "C2_SUBMIT_FAIL_CLOSED_REQUIRED":
            return {"applied": False}
        if not reason_detail.startswith(_OPTIONS_SNAPSHOT_MISSING_PREFIX):
            return {"applied": False}

    return {
        "applied": True,
        "reason_codes": [_PAPER_STARTUP_TOLERANCE_REASON_CODE],
        "policy": _PAPER_STARTUP_TOLERANCE_POLICY,
        "veto_paths": [str(path) for path in veto_paths],
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


def _startup_materialization_check(
    *,
    truth_root: Path,
    day_utc: str,
    session_id: str,
    environment: str,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
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
    tolerance = _paper_startup_tolerance_for_options_snapshot(
        startup_payload=payload,
        day_utc=day_utc,
        session_id=session_id,
        environment=environment,
    )
    startup_status = str(payload.get("status") or "").strip().upper()
    if startup_status != "SUCCESS" and not bool(tolerance.get("applied") is True):
        codes.append("STARTUP_PROOF_STARTUP_MATERIALIZATION_NOT_SUCCESS")
    if str(payload.get("freshness_verdict") or "").strip().upper() != "CURRENT":
        codes.append("STARTUP_PROOF_STARTUP_MATERIALIZATION_NOT_CURRENT")
    if str(payload.get("linkage_verdict") or "").strip().upper() != "LINKED":
        codes.append("STARTUP_PROOF_STARTUP_MATERIALIZATION_NOT_LINKED")
    detail: dict[str, Any] = {"status": startup_status}
    if bool(tolerance.get("applied") is True):
        detail["startup_tolerance_applied"] = True
        detail["startup_tolerance_reason_codes"] = list(tolerance.get("reason_codes") or [])
        detail["startup_tolerance_policy"] = str(tolerance.get("policy") or "")
        detail["startup_tolerance_veto_paths"] = list(tolerance.get("veto_paths") or [])
    return (
        _check_row(
            logical_name="startup_materialization_v1",
            status="PASS" if not codes else "FAIL",
            evidence_ref=str(startup_ref.path),
            blocking_codes=codes,
            detail=detail,
        ),
        payload,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_startup_proof_validation_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--environment", default="PAPER")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    environment = str(args.environment or "").strip().upper() or "PAPER"
    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root,
        repo_root=REPO_ROOT,
        caller="ops/tools/run_startup_proof_validation_v1.py",
    )
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
        environment=environment,
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
