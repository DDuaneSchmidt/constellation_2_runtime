#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
SOURCE_REPO_ROOT = _THIS_FILE.parents[2].resolve()
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
from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.runtime_control_kernel.runtime_control_runner_v1 import (
    run_runtime_control_kernel_v1,
)
from constellation_2.common.budget_enforcement_v1 import (
    check_budget_enforcement_day_v1,
    effective_enforcement_mode_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.runtime_path_authority_v1 import (
    require_authoritative_repo_runtime_v1,
)
from constellation_2.common.kill_switch_authority_v1 import (
    STATUS_PASS as KILL_SWITCH_STATUS_PASS,
    resolve_kill_switch_authority_v1,
)
from constellation_2.common.session_authority_v1 import (
    read_target_day_admission_ref_v1,
    read_target_day_build_ref_v1,
    resolve_target_day_admission_path,
    resolve_target_day_build_path,
)
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1


def _check_row(*, logical_name: str, path: Path, status: str, day_utc: str, reason_codes: List[str]) -> Dict[str, Any]:
    return build_fact_dependency_row_v1(
        logical_name=logical_name,
        absolute_path=path,
        status=status,
        reason_codes=reason_codes,
        day_utc=day_utc,
    )


def _blocker_codes_from_chain(blocker_chain: List[Dict[str, Any]]) -> List[str]:
    return sorted(
        {
            str(row.get("blocker_code") or "").strip()
            for row in blocker_chain
            if isinstance(row, dict) and str(row.get("blocker_code") or "").strip()
        }
    )


def _sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _is_64hex(value: str) -> bool:
    token = str(value or "").strip().lower()
    return len(token) == 64 and all(ch in "0123456789abcdef" for ch in token)


def _normalize_reason_codes(values: List[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        code = str(value or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _extract_intent_authorization_reason_codes(payload: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    out.extend(_normalize_reason_codes(list(payload.get("reason_codes") or [])))
    constitutional_shadow = payload.get("constitutional_shadow")
    if isinstance(constitutional_shadow, dict):
        decision = constitutional_shadow.get("decision")
        if isinstance(decision, dict):
            out.extend(_normalize_reason_codes(list(decision.get("blocker_rules") or [])))
    return _normalize_reason_codes(out)


def _extract_submit_boundary_reason_codes(payload: Dict[str, Any]) -> List[str]:
    authorization_codes = _extract_intent_authorization_reason_codes(payload)
    out: List[str] = [code for code in authorization_codes if code.startswith("AUTHZ_")]
    if "BUNDLE_B_HEADROOM_REJECTED" in authorization_codes:
        out.extend(["BUNDLE_B_HEADROOM_REJECTED", "SUBMIT_BOUNDARY_HEADROOM_REJECTED"])
    return _normalize_reason_codes(out)


def _summarize_execution_intent_authorization_v1(*, execution_truth_root: Path, day_utc: str) -> Dict[str, Any]:
    intents_dir = (execution_truth_root / "intents_v1" / "snapshots" / day_utc).resolve()
    auth_dir = (execution_truth_root / "engine_activity_v1" / "authorization_v1" / day_utc).resolve()
    intent_hashes: List[str] = []
    if intents_dir.exists() and intents_dir.is_dir():
        for intent_path in sorted(intents_dir.glob("*.exposure_intent.v1.json")):
            stem = str(intent_path.name).split(".", 1)[0]
            if _is_64hex(stem):
                intent_hashes.append(stem)
    unique_hashes = sorted(set(intent_hashes))
    if not unique_hashes:
        return {
            "status": "NO_INTENTS",
            "path": intents_dir,
            "reason_codes": ["SUBMIT_BOUNDARY_NO_INTENTS_PRESENT"],
        }

    authorized_count = 0
    reason_codes: List[str] = []
    for intent_hash in unique_hashes:
        auth_path = (auth_dir / f"{intent_hash}.authorization.v1.json").resolve()
        if not auth_path.exists() or not auth_path.is_file():
            reason_codes.append("SUBMIT_BOUNDARY_INTENT_AUTHORIZATION_MISSING")
            continue
        try:
            payload = json.loads(auth_path.read_text(encoding="utf-8"))
        except Exception:
            reason_codes.append("SUBMIT_BOUNDARY_INTENT_AUTHORIZATION_PARSE_ERROR")
            continue
        if not isinstance(payload, dict):
            reason_codes.append("SUBMIT_BOUNDARY_INTENT_AUTHORIZATION_PARSE_ERROR")
            continue
        auth = payload.get("authorization")
        auth_obj = auth if isinstance(auth, dict) else {}
        status = str(payload.get("status") or "").strip().upper()
        decision = str(auth_obj.get("decision") or "").strip().upper()
        authorization_reason_codes = _extract_submit_boundary_reason_codes(payload)
        try:
            qty = int(auth_obj.get("authorized_quantity") or 0)
        except Exception:
            qty = 0
        if status == "AUTHORIZED" and decision == "AUTHORIZED" and qty > 0:
            authorized_count += 1
            continue
        if status != "AUTHORIZED" or decision != "AUTHORIZED":
            reason_codes.append("SUBMIT_BOUNDARY_INTENT_AUTHORIZATION_DENIED")
            reason_codes.extend(authorization_reason_codes)
        if qty <= 0:
            reason_codes.append("SUBMIT_BOUNDARY_INTENT_AUTHORIZATION_ZERO_QUANTITY")
            reason_codes.extend(authorization_reason_codes)

    if authorized_count > 0:
        return {
            "status": "PASS",
            "path": auth_dir,
            "reason_codes": [],
        }
    reason_codes.append("SUBMIT_BOUNDARY_NO_AUTHORIZED_INTENTS")
    return {
        "status": "FAIL",
        "path": auth_dir,
        "reason_codes": sorted(set(reason_codes)),
    }


def _closure_state_for_boundary(boundary_status: str, blocking_codes: List[str]) -> str:
    status = str(boundary_status or "").strip().upper()
    if status == "AUTHORIZED":
        return "COMPLETE"
    if status in {"BLOCKED", "DENIED", "MALFORMED", "STALE"}:
        return "BLOCKED"
    if blocking_codes:
        return "DEGRADED"
    return "OPEN"


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


def _resolve_session_day_blocker_v1(*, truth_root: Path, day_utc: str) -> Tuple[str, Path]:
    authority_path = (truth_root / "reports" / "paper_session_authority_v1" / day_utc / "paper_session_authority.v1.json").resolve()
    if authority_path.exists() and authority_path.is_file():
        try:
            payload = json.loads(authority_path.read_text(encoding="utf-8"))
        except Exception:
            return "SESSION_AUTHORITY_MISSING", authority_path
        if isinstance(payload, dict):
            reason_codes = _normalize_reason_codes(list(payload.get("blocking_reason_codes") or []))
            for code in reason_codes:
                if code in {"NON_TRADING_DAY", "NO_ACTIVE_PAPER_SESSION"}:
                    return code, authority_path

    calendar_path = (truth_root / "market_calendar_v1" / "NYSE" / f"{day_utc[:4]}.jsonl").resolve()
    if not calendar_path.exists() or not calendar_path.is_file():
        return "SESSION_AUTHORITY_MISSING", calendar_path
    try:
        for raw in calendar_path.read_text(encoding="utf-8").splitlines():
            line = str(raw).strip()
            if not line:
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                continue
            if str(row.get("day_utc") or "").strip() != day_utc:
                continue
            if row.get("is_trading_session") is False:
                return "NON_TRADING_DAY", calendar_path
            return "", calendar_path
    except Exception:
        return "SESSION_AUTHORITY_MISSING", calendar_path
    return "SESSION_AUTHORITY_MISSING", calendar_path


def _ensure_dependency_ref_v1(
    *,
    artifact_id: str,
    path: Path,
    sha256: str,
    day_utc: str,
) -> Dict[str, str]:
    path_text = str(path).strip()
    sha_text = str(sha256 or "").strip()
    if not sha_text:
        sha_text = canonical_hash_for_c2_artifact_v1(
            {
                "artifact_id": artifact_id,
                "path": path_text,
                "day_utc": day_utc,
                "availability": "UNAVAILABLE",
            }
        )
    return {
        "artifact_id": artifact_id,
        "path": path_text,
        "sha256": sha_text,
        "artifact_class": "admission_result",
        "finality_state": "provisional",
    }


def _canonical_blocker_for_boundary_v1(blocking_codes: List[str]) -> str:
    normalized = _normalize_reason_codes(blocking_codes)
    for preferred in ("NON_TRADING_DAY", "NO_ACTIVE_PAPER_SESSION", "SESSION_AUTHORITY_MISSING"):
        if preferred in normalized:
            return preferred
    return normalized[0] if normalized else ""


def _source_surface_path_for_blocker_v1(*, blocker_code: str, rows: List[Dict[str, Any]]) -> str:
    blocker = str(blocker_code or "").strip().upper()
    if blocker:
        for row in rows:
            reason_codes = _normalize_reason_codes(list(row.get("reason_codes") or []))
            if blocker in reason_codes:
                return str(row.get("absolute_path") or "").strip()
    for row in rows:
        path = str(row.get("absolute_path") or "").strip()
        if path:
            return path
    return ""


def main(argv: List[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    ap = argparse.ArgumentParser(prog="run_submit_boundary_status_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root,
        repo_root=REPO_ROOT,
        caller="ops/tools/run_submit_boundary_status_v1.py",
    )
    session_id = canonical_paper_session_id_v1(day_utc)
    produced_at_utc = now_utc_iso_v1()
    paper_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    execution_root = resolve_sleeve_execution_root_v1(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=paper_account,
        sleeve_id="PRIMARY",
    )
    execution_truth_root = execution_root.execution_root_path

    required_checks: List[Dict[str, Any]] = []
    failed_checks: List[Dict[str, Any]] = []
    blocking_codes: List[str] = []
    # Build/admission remain visible for audit continuity, but PAPER submit-boundary
    # authorization is owned by submit-local safety checks.
    legacy_activation_coupling_advisory_only = True
    submission_authorized = True
    boundary_status = "AUTHORIZED"
    freshness_verdict = "CURRENT"
    linkage_verdict = "LINKED"
    build_path = resolve_target_day_build_path(truth_root=truth_root, target_day=day_utc)
    build_sha256 = ""
    readiness_path = (execution_truth_root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / paper_account / day_utc / "status.json").resolve()
    readiness_sha256 = ""
    session_day_blocker, session_day_source_path = _resolve_session_day_blocker_v1(truth_root=truth_root, day_utc=day_utc)
    if session_day_blocker:
        session_block_row = _check_row(
            logical_name="target_day_session_authority_v1",
            path=session_day_source_path,
            status="FAIL",
            day_utc=day_utc,
            reason_codes=[session_day_blocker],
        )
        required_checks.append(session_block_row)
        failed_checks.append(session_block_row)
        submission_authorized = False
        boundary_status = "BLOCKED"
        blocking_codes.extend([session_day_blocker])

    try:
        build_ref = read_target_day_build_ref_v1(truth_root=truth_root, target_day=day_utc)
        build_payload = dict(build_ref.payload)
        build_path = build_ref.path.resolve()
        if build_path.exists() and build_path.is_file():
            build_sha256 = _sha256_file(build_path)
        build_blocker_codes = _blocker_codes_from_chain(list(build_payload.get("blocker_chain") or []))
        build_ok = (
            str(build_payload.get("build_status") or "").strip().upper() == "COMPLETE"
            and str(build_payload.get("completeness_result") or "").strip().upper() == "COMPLETE"
            and str(build_payload.get("closure_status") or "").strip().upper() == "CLOSED"
            and str((build_payload.get("hidden_dependency_check_result") or {}).get("status") or "").strip().upper() == "PASS"
        )
        required_checks.append(
            _check_row(
                logical_name="target_day_build_v1",
                path=build_ref.path,
                status="PASS" if build_ok else ("ADVISORY_FAIL" if legacy_activation_coupling_advisory_only else "FAIL"),
                day_utc=day_utc,
                reason_codes=build_blocker_codes,
            )
        )
        if not build_ok:
            if legacy_activation_coupling_advisory_only:
                pass
            else:
                submission_authorized = False
                boundary_status = "BLOCKED"
                failed_checks.append(required_checks[-1])
                blocking_codes.extend(build_blocker_codes or ["TARGET_DAY_BUILD_BLOCKED"])
    except Exception as exc:
        missing_path = resolve_target_day_build_path(truth_root=truth_root, target_day=day_utc)
        row = _check_row(
            logical_name="target_day_build_v1",
            path=missing_path,
            status="ADVISORY_MISSING" if legacy_activation_coupling_advisory_only else "MISSING",
            day_utc=day_utc,
            reason_codes=[f"SUBMIT_BOUNDARY_TARGET_DAY_BUILD_UNAVAILABLE:{type(exc).__name__}"],
        )
        required_checks.append(row)
        if not legacy_activation_coupling_advisory_only:
            failed_checks.append(row)
            submission_authorized = False
            boundary_status = "BLOCKED"
            freshness_verdict = "UNKNOWN"
            linkage_verdict = "UNLINKED"
            blocking_codes.extend(row["reason_codes"])

    try:
        admission_ref = read_target_day_admission_ref_v1(truth_root=truth_root, target_day=day_utc)
        admission_payload = dict(admission_ref.payload)
        admission_codes = [
            str(code).strip()
            for code in (admission_payload.get("blocking_reason_codes") or [])
            if str(code).strip()
        ]
        admission_ok = (
            str(admission_payload.get("admission_status") or "").strip().upper() == "ADMIT"
            and bool(admission_payload.get("binding") is True)
        )
        required_checks.append(
            _check_row(
                logical_name="target_day_admission_v1",
                path=admission_ref.path,
                status="PASS" if admission_ok else ("ADVISORY_FAIL" if legacy_activation_coupling_advisory_only else "FAIL"),
                day_utc=day_utc,
                reason_codes=admission_codes,
            )
        )
        if not admission_ok:
            if legacy_activation_coupling_advisory_only:
                pass
            else:
                submission_authorized = False
                boundary_status = "BLOCKED"
                failed_checks.append(required_checks[-1])
                blocking_codes.extend(admission_codes or ["TARGET_DAY_ADMISSION_BLOCKED"])
    except Exception as exc:
        missing_path = resolve_target_day_admission_path(truth_root=truth_root, target_day=day_utc)
        row = _check_row(
            logical_name="target_day_admission_v1",
            path=missing_path,
            status="ADVISORY_MISSING" if legacy_activation_coupling_advisory_only else "MISSING",
            day_utc=day_utc,
            reason_codes=[f"SUBMIT_BOUNDARY_TARGET_DAY_ADMISSION_UNAVAILABLE:{type(exc).__name__}"],
        )
        required_checks.append(row)
        if not legacy_activation_coupling_advisory_only:
            failed_checks.append(row)
            submission_authorized = False
            boundary_status = "BLOCKED"
            freshness_verdict = "UNKNOWN"
            linkage_verdict = "UNLINKED"
            blocking_codes.extend(row["reason_codes"])

    readiness_refresh_rc = _refresh_trade_submit_readiness_artifact_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        ib_account=paper_account,
        environment="PAPER",
    )
    if readiness_refresh_rc != 0:
        row = _check_row(
            logical_name="trade_submit_readiness_c2_v1",
            path=readiness_path,
            status="FAIL",
            day_utc=day_utc,
            reason_codes=[f"SUBMIT_BOUNDARY_TRADE_SUBMIT_READINESS_REFRESH_FAILED:RC_{int(readiness_refresh_rc)}"],
        )
        required_checks.append(row)
        failed_checks.append(row)
        submission_authorized = False
        boundary_status = "BLOCKED"
        freshness_verdict = "UNKNOWN"
        linkage_verdict = "UNLINKED"
        blocking_codes.extend(row["reason_codes"])
    else:
        try:
            readiness_ref = read_trade_submit_readiness_for_day_v1(
                truth_root=execution_truth_root,
                day_utc=day_utc,
                ib_account=paper_account,
                environment="PAPER",
            )
            readiness_path = readiness_ref.path.resolve()
            readiness_sha256 = _sha256_file(readiness_path)
            validate_governed_artifact_payload_v1(
                repo_root=SOURCE_REPO_ROOT,
                artifact_id="trade_submit_readiness_c2_v1",
                payload=dict(readiness_ref.payload),
                consumer_id="submit_boundary_status_v1",
                required_finality_states=["provisional", "finalized", "corrected"],
            )
            readiness_reasons = [
                str(code).strip()
                for code in (readiness_ref.payload.get("reasons") or [])
                if str(code).strip()
            ]
            readiness_ok = bool(readiness_ref.payload.get("ok") is True)
            readiness_codes = [
                code
                for code in readiness_reasons
                if code.startswith("FAIL:")
            ]
            if any("NOT_PASS" in code.upper() for code in readiness_reasons):
                readiness_ok = False
                readiness_codes.append("SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS")
            readiness_codes = sorted(set(readiness_codes))
            required_checks.append(
                _check_row(
                    logical_name="trade_submit_readiness_c2_v1",
                    path=readiness_path,
                    status="PASS" if readiness_ok else "FAIL",
                    day_utc=day_utc,
                    reason_codes=readiness_codes,
                )
            )
            if not readiness_ok:
                submission_authorized = False
                boundary_status = "BLOCKED"
                failed_checks.append(required_checks[-1])
                blocking_codes.extend(readiness_codes or ["SUBMIT_BOUNDARY_READINESS_NOT_OK"])
        except Exception as exc:
            row = _check_row(
                logical_name="trade_submit_readiness_c2_v1",
                path=readiness_path,
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
        truth_sleeves_root = execution_truth_root.parent.parent.resolve()
        kill_switch = resolve_kill_switch_authority_v1(
            canonical_truth_root=truth_root,
            truth_sleeves_root=truth_sleeves_root,
            day_utc=day_utc,
        )
        kill_reason_codes = list(kill_switch.reason_codes)
        kill_switch_ok = (
            kill_switch.status == KILL_SWITCH_STATUS_PASS
            and str(kill_switch.state or "").strip().upper() == "INACTIVE"
            and bool(kill_switch.allow_entries is True)
        )
        if not kill_reason_codes and not kill_switch_ok:
            kill_reason_codes = ["SUBMIT_BOUNDARY_KILL_SWITCH_BLOCKED"]
        required_checks.append(
            _check_row(
                logical_name="global_kill_switch_state_v1",
                path=kill_switch.canonical_path,
                status="PASS" if kill_switch_ok else "FAIL",
                day_utc=day_utc,
                reason_codes=kill_reason_codes,
            )
        )
        if not kill_switch_ok:
            submission_authorized = False
            boundary_status = "BLOCKED"
            failed_checks.append(required_checks[-1])
            blocking_codes.extend(kill_reason_codes)
    except Exception as exc:
        missing_path = (truth_root / "risk_v1" / "kill_switch_v1" / day_utc / "global_kill_switch_state.v1.json").resolve()
        row = _check_row(
            logical_name="global_kill_switch_state_v1",
            path=missing_path,
            status="MISSING",
            day_utc=day_utc,
            reason_codes=[f"SUBMIT_BOUNDARY_KILL_SWITCH_UNAVAILABLE:{type(exc).__name__}"],
        )
        required_checks.append(row)
        failed_checks.append(row)
        submission_authorized = False
        boundary_status = "BLOCKED"
        freshness_verdict = "UNKNOWN"
        linkage_verdict = "UNLINKED"
        blocking_codes.extend(row["reason_codes"])

    try:
        control_run_id = canonical_hash_for_c2_artifact_v1(
            {
                "tool": "run_submit_boundary_status_v1",
                "day_utc": day_utc,
                "ib_account": paper_account,
                "produced_at_utc": produced_at_utc,
            }
        )
        control_result = run_runtime_control_kernel_v1(
            canonical_truth_root=truth_root,
            execution_truth_root=execution_truth_root,
            day_utc=day_utc,
            produced_utc=produced_at_utc,
            run_id=f"submit-boundary-status:{control_run_id}",
            environment="PAPER",
            ib_account=paper_account,
            sleeve_id="PRIMARY",
        )
        control_decision = control_result["runtime_control_decision"]
        control_record = control_result["runtime_control_record"]
        control_record_path = control_result["runtime_control_record_path"]
        control_reason_codes = list(control_decision.reason_codes)
        control_ok = control_record is not None and control_record.control_state == "ALLOW"
        control_path = Path(str(control_record_path or control_result["runtime_control_decision_path"])).resolve()
        required_checks.append(
            _check_row(
                logical_name="runtime_control_record_v1",
                path=control_path,
                status="PASS" if control_ok else "FAIL",
                day_utc=day_utc,
                reason_codes=control_reason_codes,
            )
        )
        if not control_ok:
            submission_authorized = False
            failed_checks.append(required_checks[-1])
            boundary_status = "BLOCKED"
            if control_record is None:
                freshness_verdict = "UNKNOWN"
                linkage_verdict = "UNLINKED"
            blocking_codes.extend(control_reason_codes or ["SUBMIT_BOUNDARY_RUNTIME_CONTROL_BLOCKED"])
    except Exception as exc:
        missing_path = (truth_root / "runtime_control_kernel_v1" / "records" / day_utc / "PAPER" / paper_account).resolve()
        row = _check_row(
            logical_name="runtime_control_record_v1",
            path=missing_path,
            status="MISSING",
            day_utc=day_utc,
            reason_codes=[f"SUBMIT_BOUNDARY_RUNTIME_CONTROL_UNAVAILABLE:{type(exc).__name__}"],
        )
        required_checks.append(row)
        failed_checks.append(row)
        submission_authorized = False
        boundary_status = "BLOCKED"
        freshness_verdict = "UNKNOWN"
        linkage_verdict = "UNLINKED"
        blocking_codes.extend(row["reason_codes"])

    intent_auth_summary = _summarize_execution_intent_authorization_v1(
        execution_truth_root=execution_truth_root,
        day_utc=day_utc,
    )
    intent_auth_status = str(intent_auth_summary["status"]).strip().upper()
    intent_auth_row = _check_row(
        logical_name="engine_activity_authorization_v1",
        path=Path(str(intent_auth_summary["path"])).resolve(),
        status="PASS" if intent_auth_status in {"PASS", "NO_INTENTS"} else "FAIL",
        day_utc=day_utc,
        reason_codes=list(intent_auth_summary["reason_codes"]),
    )
    required_checks.append(intent_auth_row)
    if intent_auth_status == "FAIL":
        submission_authorized = False
        boundary_status = "BLOCKED"
        failed_checks.append(intent_auth_row)
        blocking_codes.extend(intent_auth_row["reason_codes"] or ["SUBMIT_BOUNDARY_NO_AUTHORIZED_INTENTS"])

    if effective_enforcement_mode_v1() == "HARD":
        budget_guard = check_budget_enforcement_day_v1(day_utc=day_utc, truth_root=truth_root)
        guard_row = _check_row(
            logical_name="budget_enforcement_guard_v1",
            path=Path(budget_guard["path"]).resolve(),
            status="FAIL" if bool(budget_guard["blocking"]) else "PASS",
            day_utc=day_utc,
            reason_codes=list(budget_guard["reason_codes"]),
        )
        required_checks.append(guard_row)
        if bool(budget_guard["blocking"]):
            submission_authorized = False
            boundary_status = "BLOCKED"
            failed_checks.append(guard_row)
            blocking_codes.extend(guard_row["reason_codes"])

    boundary_contract = assert_constitutional_writer_allowed_v1(
        SOURCE_REPO_ROOT,
        "submit_boundary_status_v1",
        "ops/tools/run_submit_boundary_status_v1.py",
    )
    constitutional_dependency_refs = [
        _ensure_dependency_ref_v1(
            artifact_id="target_day_build_v1",
            path=build_path,
            sha256=build_sha256,
            day_utc=day_utc,
        ),
        _ensure_dependency_ref_v1(
            artifact_id="trade_submit_readiness_c2_v1",
            path=readiness_path,
            sha256=readiness_sha256,
            day_utc=day_utc,
        ),
    ]
    blocking_codes_sorted = sorted(set(blocking_codes))
    effective_boundary_status = "AUTHORIZED" if submission_authorized else boundary_status
    canonical_blocker = _canonical_blocker_for_boundary_v1(blocking_codes_sorted)
    source_surface_path = _source_surface_path_for_blocker_v1(
        blocker_code=canonical_blocker,
        rows=(failed_checks + required_checks),
    )
    constitutional_dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type="submit_boundary_status_v1",
        artifact_class=str(boundary_contract.get("artifact_class") or "").strip(),
        authority_id="submit_boundary_status_v1",
        declared_dependency_artifacts=[
            str(item).strip()
            for item in (boundary_contract.get("required_upstream_dependencies") or [])
            if str(item).strip()
        ],
        dependency_refs=constitutional_dependency_refs,
    )
    constitutional_lineage = build_governed_artifact_lineage_v1(
        artifact_type="submit_boundary_status_v1",
        artifact_version="v1",
        artifact_class=str(boundary_contract.get("artifact_class") or "").strip(),
        authority_id="submit_boundary_status_v1",
        producer_id="ops/tools/run_submit_boundary_status_v1.py",
        generated_at_utc=produced_at_utc,
        effective_at_utc=produced_at_utc,
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=constitutional_dependency_refs,
        policy_snapshot_refs=[],
        code_version=repo_git_sha_v1(),
        run_id=f"{day_utc}:{paper_account}",
    )
    payload: Dict[str, Any] = {
        "schema_id": "submit_boundary_status",
        "schema_version": "v1",
        "authority_scope": NON_AUTHORITY_SCOPE,
        "day_utc": day_utc,
        "session_id": session_id,
        "submission_authorized": bool(submission_authorized),
        "submit_allowed": bool(submission_authorized),
        "status": "READY" if submission_authorized else ("DENIED" if effective_boundary_status == "DENIED" else "NOT_READY"),
        "canonical_blocker": canonical_blocker or None,
        "reason_codes": blocking_codes_sorted,
        "source_surface_path": source_surface_path,
        "generated_at_utc": produced_at_utc,
        "boundary_status": effective_boundary_status,
        "required_boundary_checks": required_checks,
        "failed_checks": failed_checks,
        "blocking_codes": blocking_codes_sorted,
        "closure_state": _closure_state_for_boundary(
            effective_boundary_status,
            blocking_codes_sorted,
        ),
        "first_blocker_code": canonical_blocker,
        "missing_dependency_artifacts": sorted(
            {
                str(row.get("logical_name") or "").strip()
                for row in failed_checks
                if str(row.get("status") or "").strip().upper() == "MISSING"
                and str(row.get("logical_name") or "").strip()
            }
        ),
        "constitutional_dependency_declaration": constitutional_dependency_declaration,
        "constitutional_lineage": constitutional_lineage,
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
