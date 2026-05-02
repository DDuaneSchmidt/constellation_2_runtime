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
    read_validated_surface_v1,
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
from constellation_2.common.safety_state_authority_v1 import (
    evaluate_safety_state_authority_v1,
    safety_state_authority_output_path,
    write_safety_state_authority_v1,
)
from constellation_2.common.trading_day_readiness_authority_v1 import read_or_evaluate_trading_day_readiness_authority_v1
from ops.tools.run_runtime_resilience_authority_v1 import (
    build_runtime_resilience_authority_v1,
    runtime_resilience_authority_path,
)
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from constellation_2.common.session_authority_v1 import (
    read_target_day_admission_ref_v1,
    read_target_day_build_ref_v1,
    resolve_target_day_admission_path,
    resolve_target_day_build_path,
)
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.common.paper_submit_mode_status_v1 import classify_paper_submit_mode_status_v1
from constellation_2.common.stale_artifact_guard_v1 import STALE_ARTIFACT

PAPER_TRADING_DAY_AUTHORITY_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_trading_day_authority.v1.schema.json"
)
PAPER_TRADING_DAY_AUTHORITY_TOOL = (REPO_ROOT / "ops/tools/run_paper_trading_day_authority_v1.py").resolve()


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


def _is_informational_reason_code_v1(code: str) -> bool:
    normalized = str(code or "").strip().upper()
    return normalized.startswith("INFO:") or normalized.startswith("WARN:") or normalized.startswith("ADVISORY:")


def _read_json_object_if_exists_v1(path: Path) -> Tuple[Dict[str, Any] | None, str]:
    if not path.exists() or not path.is_file():
        return None, "MISSING"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return None, f"JSON_PARSE_ERROR:{type(exc).__name__}"
    if not isinstance(payload, dict):
        return None, "TOP_LEVEL_NOT_OBJECT"
    return payload, ""


def _load_trade_readiness_policy_view_v1(*, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    presubmit_path = (
        truth_root / "reports" / "trade_readiness_presubmit_v1" / day_utc / "trade_readiness_presubmit.v1.json"
    ).resolve()
    decision_path = (
        truth_root / "reports" / "trade_readiness_decision_v1" / day_utc / "trade_readiness_decision.v1.json"
    ).resolve()
    sources = (
        ("trade_readiness_presubmit_v1", presubmit_path),
        ("trade_readiness_decision_v1", decision_path),
    )
    source_paths = {name: str(path) for name, path in sources}
    failed_conditions: List[Dict[str, str]] = []

    for logical_name, path in sources:
        payload, error_code = _read_json_object_if_exists_v1(path)
        if payload is None:
            if error_code != "MISSING":
                failed_conditions.append(
                    {
                        "condition": "READINESS_POLICY_SURFACE_INVALID",
                        "logical_name": logical_name,
                        "path": str(path),
                        "detail": error_code,
                    }
                )
            continue

        observed_day = str(payload.get("day_utc") or payload.get("day") or "").strip()
        if observed_day and observed_day != day_utc:
            failed_conditions.append(
                {
                    "condition": "READINESS_POLICY_SURFACE_DAY_MISMATCH",
                    "logical_name": logical_name,
                    "path": str(path),
                    "detail": f"observed_day={observed_day}",
                }
            )
            continue

        decision = str(payload.get("decision") or "").strip().upper()
        status = str(payload.get("status") or "").strip().upper()
        submit_allowed_raw = payload.get("submit_allowed")
        submit_allowed: bool | None = submit_allowed_raw if isinstance(submit_allowed_raw, bool) else None
        if submit_allowed is None and decision in {"YES", "NO"}:
            submit_allowed = decision == "YES"

        return {
            "selected_logical_name": logical_name,
            "selected_path": str(path),
            "status": status,
            "decision": decision,
            "submit_allowed": submit_allowed,
            "source_paths": source_paths,
            "failed_conditions": failed_conditions,
        }

    return {
        "selected_logical_name": "",
        "selected_path": "",
        "status": "",
        "decision": "",
        "submit_allowed": None,
        "source_paths": source_paths,
        "failed_conditions": failed_conditions,
    }


def _evaluate_paper_session_ledger_surface_v1(*, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    ledger_path = (
        truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
    ).resolve()
    payload, _error_code = _read_json_object_if_exists_v1(ledger_path)
    if payload is None:
        return {
            "ok": True,
            "logical_name": "paper_session_ledger_v1",
            "path": str(ledger_path),
            "reason_codes": [],
            "failed_conditions": [],
        }

    submit_lifecycle = payload.get("submit_lifecycle")
    if not isinstance(submit_lifecycle, dict):
        submit_lifecycle = {}
    control_state = payload.get("control_state")
    if not isinstance(control_state, dict):
        control_state = {}

    submit_result_status = str(submit_lifecycle.get("submit_result_status") or "").strip().upper()
    post_submit_lifecycle = payload.get("post_submit_lifecycle")
    if not isinstance(post_submit_lifecycle, dict):
        post_submit_lifecycle = {}
    nested_lineage_status = str(post_submit_lifecycle.get("lineage_status") or "").strip().upper()
    legacy_lineage_status = str(payload.get("lineage_status") or "").strip().upper()
    lineage_status = nested_lineage_status or legacy_lineage_status
    lineage_status_path = (
        "post_submit_lifecycle.lineage_status"
        if nested_lineage_status
        else ("lineage_status" if legacy_lineage_status else "post_submit_lifecycle.lineage_status|lineage_status")
    )
    control_state_blockers = _normalize_reason_codes(list(control_state.get("blocking_codes") or []))

    reason_codes: List[str] = []
    failed_conditions: List[Dict[str, str]] = []
    if submit_result_status == "FAIL" and lineage_status != "BOUND":
        reason_codes.append("POST_SUBMIT_LINEAGE_GAP")
        failed_conditions.append(
            {
                "logical_name": "paper_session_ledger_v1",
                "path": str(ledger_path),
                "condition": "LEDGER_SUBMIT_RESULT_FAIL",
                "code": "POST_SUBMIT_LINEAGE_GAP",
                "detail": (
                    "paper_session_ledger submit_lifecycle.submit_result_status=FAIL without "
                    "post_submit_lifecycle.lineage_status=BOUND"
                ),
            }
        )
    if not lineage_status:
        reason_codes.append("POST_SUBMIT_LINEAGE_GAP")
        failed_conditions.append(
            {
                "logical_name": "paper_session_ledger_v1",
                "path": str(ledger_path),
                "condition": "LEDGER_LINEAGE_STATUS_MISSING",
                "code": "POST_SUBMIT_LINEAGE_GAP",
                "detail": (
                    "paper_session_ledger post-submit lineage status missing at "
                    "post_submit_lifecycle.lineage_status and legacy lineage_status"
                ),
            }
        )
    elif lineage_status == "GAP":
        reason_codes.append("POST_SUBMIT_LINEAGE_GAP")
        failed_conditions.append(
            {
                "logical_name": "paper_session_ledger_v1",
                "path": str(ledger_path),
                "condition": "LEDGER_LINEAGE_GAP",
                "code": "POST_SUBMIT_LINEAGE_GAP",
                "detail": f"paper_session_ledger {lineage_status_path}=GAP",
            }
        )
    elif lineage_status != "BOUND":
        reason_codes.append("POST_SUBMIT_LINEAGE_GAP")
        failed_conditions.append(
            {
                "logical_name": "paper_session_ledger_v1",
                "path": str(ledger_path),
                "condition": "LEDGER_LINEAGE_STATUS_UNBOUND",
                "code": "POST_SUBMIT_LINEAGE_GAP",
                "detail": f"paper_session_ledger {lineage_status_path}={lineage_status}",
            }
        )
    for blocker in control_state_blockers:
        reason_codes.append(blocker)
        failed_conditions.append(
            {
                "logical_name": "paper_session_ledger_v1",
                "path": str(ledger_path),
                "condition": "LEDGER_CONTROL_BLOCKING_CODE",
                "code": blocker,
                "detail": f"paper_session_ledger control_state blocking code: {blocker}",
            }
        )

    normalized_codes = _normalize_reason_codes(reason_codes)
    return {
        "ok": len(normalized_codes) == 0,
        "logical_name": "paper_session_ledger_v1",
        "path": str(ledger_path),
        "reason_codes": normalized_codes,
        "failed_conditions": failed_conditions,
    }


def _evaluate_trade_submit_readiness_payload_v1(payload: Dict[str, Any]) -> Dict[str, Any]:
    reasons = [str(code).strip() for code in (payload.get("reasons") or []) if str(code).strip()]
    readiness_ok_claimed = bool(payload.get("ok") is True)
    readiness_status = str(payload.get("state") or payload.get("status") or "").strip().upper()
    readiness_decision = str(payload.get("decision") or "").strip().upper()
    submit_allowed_raw = payload.get("submit_allowed")
    readiness_submit_allowed: bool | None = submit_allowed_raw if isinstance(submit_allowed_raw, bool) else None

    reason_codes: List[str] = []
    failed_conditions: List[Dict[str, str]] = []
    for reason_code in reasons:
        upper = reason_code.upper()
        if upper.startswith("FAIL:"):
            reason_codes.append(reason_code)
            failed_conditions.append(
                {
                    "condition": "READINESS_REASON_FAIL",
                    "source_reason_code": reason_code,
                    "detail": "trade_submit_readiness_c2_v1 reason code is fail-prefixed.",
                }
            )
            continue
        if "NOT_PASS" in upper and not _is_informational_reason_code_v1(reason_code):
            reason_codes.append("SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS")
            failed_conditions.append(
                {
                    "condition": "READINESS_REASON_NOT_PASS",
                    "source_reason_code": reason_code,
                    "detail": "trade_submit_readiness_c2_v1 reason code indicates not-pass.",
                }
            )

    if isinstance(readiness_submit_allowed, bool) and not readiness_submit_allowed:
        reason_codes.append("SUBMIT_BOUNDARY_READINESS_SUBMIT_NOT_ALLOWED")
        failed_conditions.append(
            {
                "condition": "READINESS_SUBMIT_ALLOWED_FALSE",
                "source_reason_code": "submit_allowed=false",
                "detail": "trade_submit_readiness_c2_v1 submit_allowed is false.",
            }
        )

    if not readiness_ok_claimed:
        reason_codes.append("SUBMIT_BOUNDARY_READINESS_NOT_OK")
        failed_conditions.append(
            {
                "condition": "READINESS_OK_FALSE",
                "source_reason_code": "ok=false",
                "detail": "trade_submit_readiness_c2_v1 ok field is false.",
            }
        )

    normalized_codes = sorted(set(reason_codes))
    return {
        "ok": readiness_ok_claimed and not normalized_codes,
        "status": readiness_status,
        "decision": readiness_decision,
        "submit_allowed": readiness_submit_allowed,
        "reason_codes": normalized_codes,
        "failed_conditions": failed_conditions,
    }


def _build_failed_conditions_v1(*, failed_checks: List[Dict[str, Any]], extra_conditions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    dedupe = set()
    merged: List[Dict[str, Any]] = []
    for condition in list(extra_conditions):
        logical_name = str(condition.get("logical_name") or "").strip()
        code = str(condition.get("code") or condition.get("source_reason_code") or condition.get("condition") or "").strip()
        path = str(condition.get("path") or "").strip()
        key = (logical_name, code, path)
        if key in dedupe:
            continue
        dedupe.add(key)
        merged.append(dict(condition))
    for row in failed_checks:
        logical_name = str(row.get("logical_name") or "").strip()
        absolute_path = str(row.get("absolute_path") or "").strip()
        status = str(row.get("status") or "").strip().upper()
        row_reason_codes = [str(code).strip() for code in (row.get("reason_codes") or []) if str(code).strip()]
        if not row_reason_codes:
            row_reason_codes = [f"{logical_name}_FAILED"]
        for code in row_reason_codes:
            key = (logical_name, code, absolute_path)
            if key in dedupe:
                continue
            dedupe.add(key)
            merged.append(
                {
                    "logical_name": logical_name,
                    "path": absolute_path,
                    "status": status,
                    "condition": "BOUNDARY_CHECK_FAILED",
                    "code": code,
                    "detail": f"{logical_name} reported {status}.",
                }
            )
    return merged


def _build_blocking_evidence_v1(*, failed_checks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    evidence: List[Dict[str, Any]] = []
    for row in failed_checks:
        evidence.append(
            {
                "logical_name": str(row.get("logical_name") or "").strip(),
                "path": str(row.get("absolute_path") or "").strip(),
                "status": str(row.get("status") or "").strip(),
                "reason_codes": [str(code).strip() for code in (row.get("reason_codes") or []) if str(code).strip()],
            }
        )
    return evidence


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
    if status == "DRY_RUN_COMPLETE":
        return "COMPLETE"
    if status in {"BLOCKED", "DENIED", "MALFORMED", "STALE"}:
        return "BLOCKED"
    if blocking_codes:
        return "DEGRADED"
    return "OPEN"


def _refresh_day_authority_decision_artifact_v1(*, truth_root: Path, day_utc: str) -> int:
    import ops.tools.run_day_authority_decision_v1 as day_authority_module

    try:
        day_authority_module.write_day_authority_decision_from_refresh_report_v1(
            day_utc=day_utc,
            truth_root=Path(truth_root).resolve(),
            producer_git_sha=repo_git_sha_v1(),
        )
        return 0
    except SystemExit as exc:
        if "session_readiness_refresh_missing" in str(exc):
            return 0
        return 2
    except Exception:
        return 2


def _refresh_trade_submit_readiness_artifact_v1(*, truth_root: Path, day_utc: str, ib_account: str, environment: str = "PAPER") -> int:
    import ops.tools.run_trade_submit_readiness_c2_v1 as readiness_module

    resolved_truth_root = Path(truth_root).resolve()
    resolved_truth_root.mkdir(parents=True, exist_ok=True)
    day_authority_refresh_rc = _refresh_day_authority_decision_artifact_v1(
        truth_root=resolved_truth_root,
        day_utc=day_utc,
    )
    if day_authority_refresh_rc != 0:
        return int(day_authority_refresh_rc)
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


def _refresh_paper_trading_day_authority_artifact_v1(*, truth_root: Path, day_utc: str) -> int:
    import ops.tools.run_paper_trading_day_authority_v1 as authority_module

    resolved_truth_root = Path(truth_root).resolve()
    authoritative_repo_root = resolve_authoritative_repo_root_v1(authority_module.REPO_ROOT)
    original_repo_root = authority_module.REPO_ROOT
    original_argv = list(sys.argv)
    try:
        authority_module.REPO_ROOT = authoritative_repo_root
        sys.argv = [
            "run_paper_trading_day_authority_v1.py",
            "--day_utc",
            day_utc,
            "--truth_root",
            str(resolved_truth_root),
        ]
        return int(authority_module.main())
    finally:
        sys.argv = original_argv
        authority_module.REPO_ROOT = original_repo_root


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
    if "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE" in normalized:
        return "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE"
    for preferred in (
        "IB_RECONNECTING",
        "IB_DISCONNECTED",
        "ACCOUNT_SUMMARY_MISSING",
        "BROKER_EVENT_LOG_MISSING",
        "BROKER_EVENT_LOG_STALE",
        "POSITION_OR_ORDER_TRUTH_UNKNOWN",
        "PENDING_ORDERS_NOT_RECONCILED",
        "OPEN_POSITIONS_NOT_RECONCILED",
        "RESTART_RECOVERY_REQUIRED",
        "RUNTIME_RESILIENCE_AUTHORITY_NOT_PASS",
    ):
        if preferred in normalized:
            return preferred
    if STALE_ARTIFACT in normalized:
        return STALE_ARTIFACT
    for preferred in (
        "NAV_INVALID",
        "DRAWDOWN_LIMIT_EXCEEDED",
        "KILL_SWITCH_ACTIVE",
        "GLOBAL_KILL_SWITCH_STATE_MISSING",
        "CAPITAL_RISK_ENVELOPE_NOT_PASS",
        "CAPITAL_RISK_ENVELOPE_MISSING",
        "TRADE_SUBMIT_READINESS_BLOCKED",
        "TRADE_SUBMIT_READINESS_MISSING",
        "SAFETY_INPUTS_DEGRADED",
        "SAFETY_STATE_AUTHORITY_NOT_PASS",
    ):
        if preferred in normalized:
            return preferred
    for preferred in ("NON_TRADING_DAY", "NO_ACTIVE_PAPER_SESSION", "SESSION_AUTHORITY_MISSING"):
        if preferred in normalized:
            return preferred
    return normalized[0] if normalized else ""


def _day_authority_has_manifest_required_inputs_v1(payload: Dict[str, Any]) -> bool:
    input_status = payload.get("input_status")
    if not isinstance(input_status, dict):
        return False
    observed_required = 0
    for row in input_status.values():
        if not isinstance(row, dict):
            continue
        if str(row.get("required_or_diagnostic") or "").strip().lower() != "required":
            continue
        if str(row.get("readiness_role") or "").strip().lower() != "authority_input":
            continue
        observed_required += 1
        if str(row.get("status") or "").strip().upper() != "PASS":
            return False
    return observed_required > 0


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


def _runtime_resilience_boundary_check_v1(*, payload: Dict[str, Any]) -> Tuple[bool, List[str], str]:
    status = str(payload.get("status") or "").strip().upper()
    blocker = str(payload.get("canonical_blocker") or "").strip().upper()
    recovery = str(payload.get("recovery_status") or "").strip().upper()
    ib_state = str(payload.get("ib_connection_state") or "").strip().upper()
    pending_reconciled = bool(payload.get("pending_orders_reconciled") is True)
    open_reconciled = bool(payload.get("open_positions_reconciled") is True)
    submit_blocked_during_recovery = bool(payload.get("submit_blocked_during_recovery") is True)
    codes = _normalize_reason_codes(list(payload.get("reason_codes") or []))
    if blocker:
        codes.append(blocker)
    if recovery in {"IN_PROGRESS", "BLOCKED", "DEGRADED"}:
        codes.append(blocker or "RESTART_RECOVERY_REQUIRED")
    if ib_state in {"DISCONNECTED", "STALE", "RECONNECTING", "UNKNOWN"}:
        codes.append(
            "IB_RECONNECTING"
            if ib_state == "RECONNECTING"
            else ("ACCOUNT_SUMMARY_MISSING" if ib_state == "STALE" else "IB_DISCONNECTED")
        )
    if not pending_reconciled:
        codes.append("PENDING_ORDERS_NOT_RECONCILED")
    if not open_reconciled:
        codes.append("OPEN_POSITIONS_NOT_RECONCILED")
    if submit_blocked_during_recovery and not codes:
        codes.append("RUNTIME_RESILIENCE_AUTHORITY_NOT_PASS")
    normalized = _normalize_reason_codes(codes)
    ok = status == "PASS" and not normalized
    return ok, normalized, blocker or (normalized[0] if normalized else "")


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
    submit_mode = classify_paper_submit_mode_status_v1(
        execution_root=execution_truth_root,
        day_utc=day_utc,
    )
    submit_mode_status = str(submit_mode.get("submit_mode_status") or "NO_SUBMIT_ATTEMPT").strip().upper()
    dry_run_complete = submit_mode_status == "DRY_RUN_COMPLETE"
    day_readiness_path, day_readiness = read_or_evaluate_trading_day_readiness_authority_v1(
        target_day=day_utc,
        truth_root=truth_root,
        execution_root=execution_truth_root,
        environment="PAPER",
    )
    day_readiness_mode = str(day_readiness.get("readiness_mode") or "").strip().upper()
    day_readiness_submit_allowed = bool(day_readiness.get("submit_allowed_by_mode") is True)

    required_checks: List[Dict[str, Any]] = []
    failed_checks: List[Dict[str, Any]] = []
    blocking_codes: List[str] = []
    # Build/admission remain visible for audit continuity, but PAPER submit-boundary
    # authorization is owned by submit-local safety checks.
    legacy_activation_coupling_advisory_only = True
    submission_authorized = True
    boundary_status = "AUTHORIZED"
    day_authority_ok = False
    freshness_verdict = "CURRENT"
    linkage_verdict = "LINKED"
    build_path = resolve_target_day_build_path(truth_root=truth_root, target_day=day_utc)
    admission_path = resolve_target_day_admission_path(truth_root=truth_root, target_day=day_utc)
    build_sha256 = ""
    readiness_path = (execution_truth_root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / paper_account / day_utc / "status.json").resolve()
    readiness_sha256 = ""
    safety_state_path = safety_state_authority_output_path(truth_root=truth_root, day_utc=day_utc).resolve()
    safety_state_sha256 = ""
    safety_state_ok = False
    readiness_status = ""
    readiness_decision = ""
    readiness_submit_allowed: bool | None = None
    day_authority_path = (
        truth_root
        / "reports"
        / "paper_trading_day_authority_v1"
        / day_utc
        / "paper_trading_day_authority.v1.json"
    ).resolve()
    source_paths: Dict[str, str] = {
        "target_day_build_v1": str(build_path),
        "target_day_admission_v1": str(admission_path),
        "trade_submit_readiness_c2_v1": str(readiness_path),
        "safety_state_authority_v1": str(safety_state_path),
        "trading_day_readiness_authority_v1": str(day_readiness_path),
        "paper_trading_day_authority_v1": str(day_authority_path),
    }
    runtime_resilience_path = runtime_resilience_authority_path(truth_root=truth_root, day_utc=day_utc).resolve()
    runtime_resilience_status = "UNKNOWN"
    runtime_resilience_blocker = ""
    submit_blocked_during_recovery = False
    extra_failed_conditions: List[Dict[str, Any]] = []
    day_readiness_codes = [] if day_readiness_submit_allowed else [str(day_readiness.get("canonical_blocker") or "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE")]
    day_readiness_row = _check_row(
        logical_name="trading_day_readiness_authority_v1",
        path=day_readiness_path,
        status="PASS" if day_readiness_submit_allowed else "FAIL",
        day_utc=day_utc,
        reason_codes=day_readiness_codes,
    )
    required_checks.append(day_readiness_row)
    if not day_readiness_submit_allowed:
        submission_authorized = False
        boundary_status = "BLOCKED"
        failed_checks.append(day_readiness_row)
        blocking_codes.extend(day_readiness_codes)
    try:
        runtime_resilience_payload = build_runtime_resilience_authority_v1(
            day_utc=day_utc,
            truth_root=truth_root,
            execution_root=execution_truth_root,
            runtime_root=truth_root.parent,
            environment="PAPER",
            broker_account=paper_account,
        )
        runtime_resilience_path = Path(str(runtime_resilience_payload.get("artifact_path") or runtime_resilience_path)).resolve()
        source_paths["runtime_resilience_authority_v1"] = str(runtime_resilience_path)
        runtime_resilience_status = str(runtime_resilience_payload.get("status") or "").strip().upper()
        runtime_resilience_blocker = str(runtime_resilience_payload.get("canonical_blocker") or "").strip().upper()
        submit_blocked_during_recovery = bool(runtime_resilience_payload.get("submit_blocked_during_recovery") is True)
        runtime_ok, runtime_codes, runtime_blocker = _runtime_resilience_boundary_check_v1(payload=runtime_resilience_payload)
        runtime_row = _check_row(
            logical_name="runtime_resilience_authority_v1",
            path=runtime_resilience_path,
            status="PASS" if runtime_ok else "FAIL",
            day_utc=day_utc,
            reason_codes=runtime_codes,
        )
        required_checks.append(runtime_row)
        if not runtime_ok:
            submission_authorized = False
            boundary_status = "BLOCKED"
            failed_checks.append(runtime_row)
            blocking_codes.extend(runtime_codes or [runtime_blocker or "RUNTIME_RESILIENCE_AUTHORITY_NOT_PASS"])
            extra_failed_conditions.append(
                {
                    "logical_name": "runtime_resilience_authority_v1",
                    "path": str(runtime_resilience_path),
                    "condition": "RUNTIME_RESILIENCE_AUTHORITY_NOT_PASS",
                    "code": runtime_blocker or "RUNTIME_RESILIENCE_AUTHORITY_NOT_PASS",
                    "detail": str(runtime_resilience_payload.get("operator_next_action") or ""),
                }
            )
    except Exception as exc:
        source_paths["runtime_resilience_authority_v1"] = str(runtime_resilience_path)
        runtime_row = _check_row(
            logical_name="runtime_resilience_authority_v1",
            path=runtime_resilience_path,
            status="MISSING",
            day_utc=day_utc,
            reason_codes=[f"SUBMIT_BOUNDARY_RUNTIME_RESILIENCE_UNAVAILABLE:{type(exc).__name__}"],
        )
        required_checks.append(runtime_row)
        failed_checks.append(runtime_row)
        submission_authorized = False
        boundary_status = "BLOCKED"
        freshness_verdict = "UNKNOWN"
        linkage_verdict = "UNLINKED"
        blocking_codes.extend(runtime_row["reason_codes"])
    readiness_policy_view = _load_trade_readiness_policy_view_v1(truth_root=truth_root, day_utc=day_utc)
    source_paths.update(dict(readiness_policy_view.get("source_paths") or {}))
    extra_failed_conditions.extend(list(readiness_policy_view.get("failed_conditions") or []))
    ledger_surface_eval = _evaluate_paper_session_ledger_surface_v1(truth_root=truth_root, day_utc=day_utc)
    source_paths["paper_session_ledger_v1"] = str(ledger_surface_eval.get("path") or "")
    extra_failed_conditions.extend(list(ledger_surface_eval.get("failed_conditions") or []))

    day_authority_refresh_rc = _refresh_paper_trading_day_authority_artifact_v1(
        truth_root=truth_root,
        day_utc=day_utc,
    )
    try:
        day_authority_ref = read_validated_surface_v1(
            path=day_authority_path,
            schema_relpath=PAPER_TRADING_DAY_AUTHORITY_SCHEMA_RELPATH_V1,
        )
        day_authority_payload = dict(day_authority_ref.payload)
        day_authority_state = str(day_authority_payload.get("state") or "").strip().upper()
        day_authority_submit_allowed = bool(
            day_authority_payload.get("can_submit_paper_orders") is True
        )
        day_authority_codes = [
            str(code).strip()
            for code in (day_authority_payload.get("reason_codes") or [])
            if str(code).strip()
        ]
        day_authority_blocker = str(day_authority_payload.get("canonical_blocker") or "").strip()
        day_authority_open_claim = day_authority_state == "OPEN_READY" and day_authority_submit_allowed
        day_authority_ok = (
            day_authority_open_claim
            and _day_authority_has_manifest_required_inputs_v1(day_authority_payload)
        )
        row = _check_row(
            logical_name="paper_trading_day_authority_v1",
            path=day_authority_ref.path,
            status="PASS" if day_authority_ok else ("ADVISORY_UNVERIFIED" if day_authority_open_claim else "FAIL"),
            day_utc=day_utc,
            reason_codes=day_authority_codes
            or ([day_authority_blocker] if day_authority_blocker else []),
        )
        required_checks.append(row)
        if not day_authority_open_claim and not dry_run_complete:
            submission_authorized = False
            boundary_status = "BLOCKED"
            failed_checks.append(row)
            blocking_codes.extend(
                row["reason_codes"]
                or ([day_authority_blocker] if day_authority_blocker else [])
                or ["SUBMIT_BOUNDARY_DAY_AUTHORITY_NOT_OPEN_READY"]
            )
            extra_failed_conditions.append(
                {
                    "logical_name": "paper_trading_day_authority_v1",
                    "path": str(day_authority_ref.path),
                    "condition": "DAY_AUTHORITY_NOT_OPEN_READY",
                    "code": row["reason_codes"][0]
                    if row["reason_codes"]
                    else "SUBMIT_BOUNDARY_DAY_AUTHORITY_NOT_OPEN_READY",
                    "detail": (
                        f"state={day_authority_state or 'UNKNOWN'} "
                        f"can_submit_paper_orders={day_authority_submit_allowed}"
                    ),
                }
            )
        if day_authority_refresh_rc not in (0, 2):
            extra_failed_conditions.append(
                {
                    "logical_name": "paper_trading_day_authority_v1",
                    "path": str(day_authority_ref.path),
                    "condition": "DAY_AUTHORITY_REFRESH_ERROR",
                    "code": f"SUBMIT_BOUNDARY_DAY_AUTHORITY_REFRESH_FAILED:RC_{int(day_authority_refresh_rc)}",
                    "detail": "day authority producer returned unexpected non-readiness exit code",
                }
            )
    except Exception as exc:
        row = _check_row(
            logical_name="paper_trading_day_authority_v1",
            path=day_authority_path,
            status="MISSING",
            day_utc=day_utc,
            reason_codes=[f"SUBMIT_BOUNDARY_DAY_AUTHORITY_UNAVAILABLE:{type(exc).__name__}"],
        )
        required_checks.append(row)
        failed_checks.append(row)
        submission_authorized = False
        boundary_status = "BLOCKED"
        freshness_verdict = "UNKNOWN"
        linkage_verdict = "UNLINKED"
        blocking_codes.extend(row["reason_codes"])

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
        source_paths["target_day_build_v1"] = str(build_path)
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
        admission_path = admission_ref.path.resolve()
        source_paths["target_day_admission_v1"] = str(admission_path)
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
            source_paths["trade_submit_readiness_c2_v1"] = str(readiness_path)
            readiness_sha256 = _sha256_file(readiness_path)
            validate_governed_artifact_payload_v1(
                repo_root=SOURCE_REPO_ROOT,
                artifact_id="trade_submit_readiness_c2_v1",
                payload=dict(readiness_ref.payload),
                consumer_id="submit_boundary_status_v1",
                required_finality_states=["provisional", "finalized", "corrected"],
            )
            readiness_eval = _evaluate_trade_submit_readiness_payload_v1(dict(readiness_ref.payload))
            readiness_ok = bool(readiness_eval.get("ok") is True)
            readiness_codes = [str(code).strip() for code in (readiness_eval.get("reason_codes") or []) if str(code).strip()]
            readiness_status = str(readiness_eval.get("status") or "").strip().upper()
            readiness_decision = str(readiness_eval.get("decision") or "").strip().upper()
            submit_allowed_value = readiness_eval.get("submit_allowed")
            if isinstance(submit_allowed_value, bool):
                readiness_submit_allowed = submit_allowed_value
            for condition in list(readiness_eval.get("failed_conditions") or []):
                merged_condition = dict(condition)
                merged_condition.setdefault("logical_name", "trade_submit_readiness_c2_v1")
                merged_condition.setdefault("path", str(readiness_path))
                extra_failed_conditions.append(merged_condition)
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
        safety_payload = evaluate_safety_state_authority_v1(
            day_utc=day_utc,
            truth_root=truth_root,
            execution_root=execution_truth_root,
            account=paper_account,
            environment="PAPER",
            produced_utc=produced_at_utc,
        )
        safety_state_path = write_safety_state_authority_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            payload=safety_payload,
        ).resolve()
        safety_state_sha256 = _sha256_file(safety_state_path)
        source_paths["safety_state_authority_v1"] = str(safety_state_path)
        safety_state_ok = str(safety_payload.get("status") or "").strip().upper() == "PASS"
        safety_blocker = str(safety_payload.get("canonical_blocker") or "").strip().upper()
        safety_reason_codes = [code for code in [safety_blocker or "SAFETY_STATE_AUTHORITY_NOT_PASS"] if code]
        row = _check_row(
            logical_name="safety_state_authority_v1",
            path=safety_state_path,
            status="PASS" if safety_state_ok else "FAIL",
            day_utc=day_utc,
            reason_codes=[] if safety_state_ok else safety_reason_codes,
        )
        required_checks.append(row)
        if not safety_state_ok:
            submission_authorized = False
            boundary_status = "BLOCKED"
            failed_checks.append(row)
            blocking_codes.extend(safety_reason_codes)
            extra_failed_conditions.append(
                {
                    "logical_name": "safety_state_authority_v1",
                    "path": str(safety_state_path),
                    "condition": "SAFETY_STATE_AUTHORITY_NOT_PASS",
                    "code": safety_blocker or "SAFETY_STATE_AUTHORITY_NOT_PASS",
                    "detail": str(safety_payload.get("root_cause") or ""),
                }
            )
    except Exception as exc:
        row = _check_row(
            logical_name="safety_state_authority_v1",
            path=safety_state_path,
            status="MISSING",
            day_utc=day_utc,
            reason_codes=[f"SUBMIT_BOUNDARY_SAFETY_STATE_AUTHORITY_UNAVAILABLE:{type(exc).__name__}"],
        )
        required_checks.append(row)
        failed_checks.append(row)
        submission_authorized = False
        boundary_status = "BLOCKED"
        freshness_verdict = "UNKNOWN"
        linkage_verdict = "UNLINKED"
        blocking_codes.extend(row["reason_codes"])

    policy_logical_name = str(readiness_policy_view.get("selected_logical_name") or "").strip()
    policy_path = Path(str(readiness_policy_view.get("selected_path") or "").strip() or str(admission_path)).resolve()
    if policy_logical_name:
        source_paths["trade_readiness_policy_surface"] = str(policy_path)
        policy_status = str(readiness_policy_view.get("status") or "").strip().upper()
        policy_decision = str(readiness_policy_view.get("decision") or "").strip().upper()
        policy_submit_allowed = readiness_policy_view.get("submit_allowed")
        if not readiness_status and policy_status:
            readiness_status = policy_status
        if not readiness_decision and policy_decision:
            readiness_decision = policy_decision
        if readiness_submit_allowed is None and isinstance(policy_submit_allowed, bool):
            readiness_submit_allowed = policy_submit_allowed

        policy_not_pass = False
        policy_reason_codes: List[str] = []
        if isinstance(policy_submit_allowed, bool) and not policy_submit_allowed:
            policy_not_pass = True
            policy_reason_codes.append("SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS")
            extra_failed_conditions.append(
                {
                    "logical_name": policy_logical_name,
                    "path": str(policy_path),
                    "condition": "READINESS_POLICY_SUBMIT_ALLOWED_FALSE",
                    "code": "SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS",
                    "detail": "readiness policy surface submit_allowed=false",
                }
            )
        if policy_decision == "NO":
            policy_not_pass = True
            policy_reason_codes.append("SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS")
            extra_failed_conditions.append(
                {
                    "logical_name": policy_logical_name,
                    "path": str(policy_path),
                    "condition": "READINESS_POLICY_DECISION_NO",
                    "code": "SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS",
                    "detail": "readiness policy surface decision=NO",
                }
            )
        policy_reason_codes = sorted(set(policy_reason_codes))
        required_checks.append(
            _check_row(
                logical_name=policy_logical_name,
                path=policy_path,
                status="FAIL" if policy_not_pass else "PASS",
                day_utc=day_utc,
                reason_codes=policy_reason_codes,
            )
        )
        if policy_not_pass:
            submission_authorized = False
            boundary_status = "BLOCKED"
            failed_checks.append(required_checks[-1])
            blocking_codes.extend(policy_reason_codes)

    ledger_reason_codes = [str(code).strip() for code in (ledger_surface_eval.get("reason_codes") or []) if str(code).strip()]
    required_checks.append(
        _check_row(
            logical_name="paper_session_ledger_v1",
            path=Path(str(ledger_surface_eval.get("path") or "")).resolve(),
            status="PASS" if bool(ledger_surface_eval.get("ok") is True) else "FAIL",
            day_utc=day_utc,
            reason_codes=ledger_reason_codes,
        )
    )
    if not bool(ledger_surface_eval.get("ok") is True) and not dry_run_complete:
        submission_authorized = False
        boundary_status = "BLOCKED"
        failed_checks.append(required_checks[-1])
        blocking_codes.extend(ledger_reason_codes)

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
        _ensure_dependency_ref_v1(
            artifact_id="safety_state_authority_v1",
            path=safety_state_path,
            sha256=safety_state_sha256,
            day_utc=day_utc,
        ),
        _ensure_dependency_ref_v1(
            artifact_id="trading_day_readiness_authority_v1",
            path=day_readiness_path,
            sha256=_sha256_file(day_readiness_path) if day_readiness_path.exists() else "",
            day_utc=day_utc,
        ),
        _ensure_dependency_ref_v1(
            artifact_id="runtime_resilience_authority_v1",
            path=runtime_resilience_path,
            sha256=_sha256_file(runtime_resilience_path) if runtime_resilience_path.exists() else "",
            day_utc=day_utc,
        ),
    ]
    runtime_resilience_ok = runtime_resilience_status == "PASS" and not runtime_resilience_blocker and not submit_blocked_during_recovery
    if day_authority_ok and safety_state_ok and day_readiness_submit_allowed and runtime_resilience_ok:
        # Submit boundary is a projection of paper_trading_day_authority_v1. Legacy
        # submit-local checks remain visible in required_boundary_checks, but they
        # no longer carry veto power unless promoted to required authority inputs
        # in the manifest and reflected by day authority.
        submission_authorized = True
        boundary_status = "AUTHORIZED"
        failed_checks = []
        blocking_codes = []
        freshness_verdict = "CURRENT"
        linkage_verdict = "LINKED"
        extra_failed_conditions = [
            condition
            for condition in extra_failed_conditions
            if "KILL_SWITCH" not in str(condition.get("code") or condition.get("condition") or "").upper()
            and "CANONICAL_KILL_SWITCH_ACTIVE"
            not in str(condition.get("code") or condition.get("condition") or "").upper()
        ]

    if dry_run_complete:
        dry_run_projection_codes = {
            "POST_SUBMIT_LINEAGE_GAP",
            "SUBMIT_BOUNDARY_DAY_AUTHORITY_NOT_OPEN_READY",
        }
        non_projection_blockers = [
            code for code in _normalize_reason_codes(blocking_codes) if code not in dry_run_projection_codes
        ]
        if not non_projection_blockers:
            submission_authorized = False
            boundary_status = "DRY_RUN_COMPLETE"
            failed_checks = [
                row
                for row in failed_checks
                if str(row.get("logical_name") or "").strip()
                not in {"paper_trading_day_authority_v1", "paper_session_ledger_v1"}
            ]
            blocking_codes = []
            extra_failed_conditions = [
                condition
                for condition in extra_failed_conditions
                if str(condition.get("code") or "").strip().upper() not in dry_run_projection_codes
            ]

    blocking_codes_sorted = sorted(set(blocking_codes))
    effective_boundary_status = "AUTHORIZED" if submission_authorized else boundary_status
    effective_readiness_status = readiness_status or "UNKNOWN"
    effective_readiness_decision = readiness_decision
    effective_readiness_submit_allowed = readiness_submit_allowed
    if not submission_authorized:
        effective_readiness_submit_allowed = False
        if not effective_readiness_decision or effective_readiness_decision == "YES":
            effective_readiness_decision = "NO"
        if effective_readiness_status in {"UNKNOWN", "OK", "PASS", "READY"}:
            effective_readiness_status = "NOT_READY"
    canonical_blocker = _canonical_blocker_for_boundary_v1(blocking_codes_sorted)
    failed_conditions = _build_failed_conditions_v1(
        failed_checks=failed_checks,
        extra_conditions=extra_failed_conditions,
    )
    blocking_evidence = _build_blocking_evidence_v1(failed_checks=failed_checks)
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
        "status": (
            "READY"
            if submission_authorized
            else (
                "DRY_RUN_COMPLETE"
                if effective_boundary_status == "DRY_RUN_COMPLETE"
                else ("DENIED" if effective_boundary_status == "DENIED" else "NOT_READY")
            )
        ),
        "submit_mode_status": submit_mode_status,
        "dry_run_policy": str(submit_mode.get("dry_run_policy") or "UNKNOWN"),
        "broker_transmit_enabled": submit_mode.get("broker_transmit_enabled"),
        "broker_order_transmitted": bool(submit_mode.get("broker_order_transmitted") is True),
        "missing_broker_ids_blocker": bool(submit_mode.get("missing_broker_ids_blocker") is True),
        "missing_broker_ids_diagnostic": bool(submit_mode.get("missing_broker_ids_diagnostic") is True),
        "canonical_blocker": canonical_blocker or None,
        "reason_codes": blocking_codes_sorted,
        "source_surface_path": source_surface_path,
        "source_paths": source_paths,
        "readiness_authority_path": str(day_readiness_path),
        "readiness_mode": day_readiness_mode,
        "evidence_policy_used": day_readiness.get("evidence_policy") if isinstance(day_readiness.get("evidence_policy"), dict) else {},
        "carry_forward_source_used": "",
        "mode_specific_blocker": bool(canonical_blocker == "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE"),
        "runtime_resilience_authority_path": str(runtime_resilience_path),
        "runtime_resilience_status": runtime_resilience_status,
        "runtime_resilience_blocker": runtime_resilience_blocker,
        "submit_blocked_during_recovery": submit_blocked_during_recovery,
        "readiness_status": effective_readiness_status,
        "readiness_decision": effective_readiness_decision,
        "readiness_submit_allowed": effective_readiness_submit_allowed,
        "failed_conditions": failed_conditions,
        "blocking_evidence": blocking_evidence,
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
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_submit_boundary_status_v1.py",
        producer_command=f"python3 ops/tools/run_submit_boundary_status_v1.py --day_utc {day_utc}",
        input_artifacts=[
            path
            for path in list(source_paths.values()) + [str(day_readiness_path), str(runtime_resilience_path)]
            if str(path or "").strip() and Path(str(path)).expanduser().exists()
        ],
        output_artifacts=[resolve_submit_boundary_status_path(truth_root=truth_root, day_utc=day_utc)],
        schema_versions={"submit_boundary_status": "v1"},
    )
    ref = atomic_write_idempotent_validated_json_v1(
        path=resolve_submit_boundary_status_path(truth_root=truth_root, day_utc=day_utc),
        payload=payload,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_status.v1.schema.json",
        volatile_field_names=("produced_at_utc",),
    )
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "boundary_status": payload["boundary_status"]}, sort_keys=True))
    return 0 if payload["boundary_status"] in {"AUTHORIZED", "DENIED", "DRY_RUN_COMPLETE"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
