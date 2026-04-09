#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.paper_session_fact_plane_v1 import (
    NON_AUTHORITY_SCOPE,
    atomic_write_idempotent_validated_json_v1,
    build_fact_dependency_row_v1,
    canonical_paper_session_id_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    read_paper_trading_posture_ref_v1,
    read_startup_materialization_ref_v1,
    read_trade_submit_readiness_for_day_v1,
    repo_git_sha_v1,
    resolve_authoritative_repo_root_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_submit_boundary_status_path,
)


def _check_row(*, logical_name: str, path: Path, status: str, day_utc: str, reason_codes: List[str]) -> Dict[str, Any]:
    return build_fact_dependency_row_v1(
        logical_name=logical_name,
        absolute_path=path,
        status=status,
        reason_codes=reason_codes,
        day_utc=day_utc,
    )


def _refresh_trade_submit_readiness_artifact_v1(*, truth_root: Path, day_utc: str, ib_account: str, environment: str = "PAPER") -> int:
    import ops.tools.run_trade_submit_readiness_c2_v1 as readiness_module

    resolved_truth_root = Path(truth_root).resolve()
    resolved_truth_root.mkdir(parents=True, exist_ok=True)
    authoritative_repo_root = resolve_authoritative_repo_root_v1(readiness_module.REPO_ROOT)
    original_repo_root = readiness_module.REPO_ROOT
    original_truth_root = readiness_module.TRUTH_ROOT
    original_out_root = readiness_module.OUT_ROOT
    original_argv = list(sys.argv)
    try:
        readiness_module.REPO_ROOT = authoritative_repo_root
        readiness_module.TRUTH_ROOT = resolved_truth_root
        readiness_module.OUT_ROOT = (resolved_truth_root / "trade_submit_readiness_c2_v1").resolve()
        sys.argv = [
            "run_trade_submit_readiness_c2_v1.py",
            "--day_utc",
            day_utc,
            "--ib_account",
            ib_account,
            "--environment",
            str(environment or "").strip().upper(),
        ]
        return int(readiness_module.main())
    finally:
        sys.argv = original_argv
        readiness_module.REPO_ROOT = original_repo_root
        readiness_module.TRUTH_ROOT = original_truth_root
        readiness_module.OUT_ROOT = original_out_root


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_submit_boundary_status_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    session_id = canonical_paper_session_id_v1(day_utc)
    produced_at_utc = now_utc_iso_v1()
    paper_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)

    required_checks: List[Dict[str, Any]] = []
    failed_checks: List[Dict[str, Any]] = []
    blocking_codes: List[str] = []
    submission_authorized = True
    boundary_status = "AUTHORIZED"
    freshness_verdict = "CURRENT"
    linkage_verdict = "LINKED"

    try:
        startup_ref = read_startup_materialization_ref_v1(truth_root=truth_root, day_utc=day_utc)
        startup_status = str(startup_ref.payload.get("status") or "").strip().upper()
        startup_ok = startup_status == "SUCCESS"
        startup_codes = [str(code).strip() for code in (startup_ref.payload.get("blocking_codes") or []) if str(code).strip()]
        required_checks.append(
            _check_row(
                logical_name="startup_materialization_v1",
                path=startup_ref.path,
                status="PASS" if startup_ok else "FAIL",
                day_utc=day_utc,
                reason_codes=startup_codes,
            )
        )
        if not startup_ok:
            submission_authorized = False
            blocking_codes.extend(startup_codes or ["SUBMIT_BOUNDARY_STARTUP_MATERIALIZATION_DENIED"])
            failed_checks.append(required_checks[-1])
            boundary_status = "BLOCKED"
    except Exception as exc:
        missing_path = (truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json").resolve()
        row = _check_row(
            logical_name="startup_materialization_v1",
            path=missing_path,
            status="MISSING",
            day_utc=day_utc,
            reason_codes=[f"SUBMIT_BOUNDARY_STARTUP_MATERIALIZATION_UNAVAILABLE:{type(exc).__name__}"],
        )
        required_checks.append(row)
        failed_checks.append(row)
        submission_authorized = False
        boundary_status = "BLOCKED"
        freshness_verdict = "UNKNOWN"
        linkage_verdict = "UNLINKED"
        blocking_codes.extend(row["reason_codes"])

    try:
        posture_ref = read_paper_trading_posture_ref_v1(truth_root=truth_root, day_utc=day_utc)
        posture_ready = bool(posture_ref.payload.get("system_ready") is True)
        posture_status = str(posture_ref.payload.get("posture_status") or "").strip().upper()
        posture_codes = [str(code).strip() for code in (posture_ref.payload.get("blocking_codes") or []) if str(code).strip()]
        required_checks.append(
            _check_row(
                logical_name="paper_trading_posture_v1",
                path=posture_ref.path,
                status="PASS" if posture_ready else "FAIL",
                day_utc=day_utc,
                reason_codes=posture_codes or ([f"PAPER_TRADING_POSTURE_STATUS:{posture_status}"] if not posture_ready else []),
            )
        )
        if not posture_ready:
            submission_authorized = False
            failed_checks.append(required_checks[-1])
            if posture_status == "DISABLED":
                boundary_status = "DENIED" if boundary_status != "BLOCKED" else boundary_status
            else:
                boundary_status = "BLOCKED"
            blocking_codes.extend(required_checks[-1]["reason_codes"])
    except Exception as exc:
        missing_path = (truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json").resolve()
        row = _check_row(
            logical_name="paper_trading_posture_v1",
            path=missing_path,
            status="MISSING",
            day_utc=day_utc,
            reason_codes=[f"SUBMIT_BOUNDARY_PAPER_TRADING_POSTURE_UNAVAILABLE:{type(exc).__name__}"],
        )
        required_checks.append(row)
        failed_checks.append(row)
        submission_authorized = False
        boundary_status = "BLOCKED"
        freshness_verdict = "UNKNOWN"
        linkage_verdict = "UNLINKED"
        blocking_codes.extend(row["reason_codes"])

    try:
        if freshness_verdict == "CURRENT" and linkage_verdict == "LINKED":
            _refresh_trade_submit_readiness_artifact_v1(
                truth_root=truth_root,
                day_utc=day_utc,
                ib_account=paper_account,
                environment="PAPER",
            )
        readiness_ref = read_trade_submit_readiness_for_day_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            ib_account=paper_account,
            environment="PAPER",
        )
        readiness_payload = readiness_ref.payload
        readiness_ok = bool(readiness_payload.get("ok") is True) and str(readiness_payload.get("state") or "").strip().upper() == "OK"
        readiness_codes = [str(code).strip() for code in (readiness_payload.get("reasons") or []) if str(code).strip()]
        required_checks.append(
            _check_row(
                logical_name="trade_submit_readiness_c2_v1",
                path=readiness_ref.path,
                status="PASS" if readiness_ok else "FAIL",
                day_utc=day_utc,
                reason_codes=readiness_codes,
            )
        )
        if not readiness_ok:
            submission_authorized = False
            failed_checks.append(required_checks[-1])
            if boundary_status != "BLOCKED":
                boundary_status = "DENIED"
            blocking_codes.extend(readiness_codes or ["SUBMIT_BOUNDARY_TRADE_SUBMIT_READINESS_DENIED"])
    except Exception as exc:
        missing_path = (truth_root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / paper_account / day_utc / "status.json").resolve()
        row = _check_row(
            logical_name="trade_submit_readiness_c2_v1",
            path=missing_path,
            status="MISSING",
            day_utc=day_utc,
            reason_codes=[f"SUBMIT_BOUNDARY_TRADE_SUBMIT_READINESS_UNAVAILABLE:{type(exc).__name__}"],
        )
        required_checks.append(row)
        failed_checks.append(row)
        submission_authorized = False
        boundary_status = "BLOCKED"
        freshness_verdict = "UNKNOWN"
        linkage_verdict = "UNLINKED"
        blocking_codes.extend(row["reason_codes"])

    payload: Dict[str, Any] = {
        "schema_id": "submit_boundary_status",
        "schema_version": "v1",
        "authority_scope": NON_AUTHORITY_SCOPE,
        "day_utc": day_utc,
        "session_id": session_id,
        "submission_authorized": bool(submission_authorized),
        "boundary_status": "AUTHORIZED" if submission_authorized else boundary_status,
        "required_boundary_checks": required_checks,
        "failed_checks": failed_checks,
        "blocking_codes": sorted(set(blocking_codes)),
        "producer": producer_block_v1(module="ops/tools/run_submit_boundary_status_v1.py", git_sha=repo_git_sha_v1()),
        "produced_at_utc": produced_at_utc,
        "freshness_verdict": freshness_verdict,
        "linkage_verdict": linkage_verdict,
        "paper_account": paper_account,
    }
    ref = atomic_write_idempotent_validated_json_v1(
        path=resolve_submit_boundary_status_path(truth_root=truth_root, day_utc=day_utc),
        payload=payload,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_status.v1.schema.json",
        volatile_field_names=("produced_at_utc",),
    )
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "boundary_status": payload["boundary_status"]}, sort_keys=True))
    return 0 if payload["boundary_status"] in {"AUTHORIZED", "DENIED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
