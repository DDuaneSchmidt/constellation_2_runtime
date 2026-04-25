#!/usr/bin/env python3
"""
submit_boundary_paper_v4.py

Paper submission boundary v4:
- Equity identity set supports equity_order_plan.v2.json (lineage-bearing) and v1 fallback.
- Uses IBPaperAdapterV2 for equity plan v1/v2.
- Applies RiskBudget gate with correct API signature.
- Writes broker_submission_record.v2.json + (if ids exist) execution_event_record.v1.json using evidence_writer_v1.

Deterministic, fail-closed.

AUDIT-GRADE ENFORCEMENT (v4):
- NO broker call unless ALL are proven true for the submission day:
  - canonical_authority_head exists and matches the submission day (PASS or BOOTSTRAP_PASS head)
  - authorization_gate_verdict_v1.status in (PASS, BOOTSTRAP_PASS)
  - global_kill_switch_state.state == INACTIVE AND allow_entries == true
  - authorization artifact exists for intent_hash and is AUTHORIZED (status+decision) with authorized_quantity > 0
  - ib_account is allowed by governed registry C2_IB_ACCOUNT_REGISTRY_V1:
      * account exists
      * enabled_for_submission == true
      * environment == PAPER and account_id starts with DU
      * lineage.engine_id is present in allowed_engine_ids
  - plan symbol is allowed by current governed symbol-universe policy:
      * prefer current-day ranked symbol universe evidence when present
      * otherwise prefer sealed Phase C symbol evidence linked to the candidate
      * fallback to ENGINE_MODEL_REGISTRY_V1.allowed_symbols only when current universe evidence is unavailable
  - C2-native trade submit readiness exists and is OK under the canonical sleeve execution root:
      * truth/sleeves/<sleeve_id>/<mode>/trade_submit_readiness_c2_v1/<mode>/<ib_account>/status.json exists
      * schema_id == trade_submit_readiness_c2, schema_version == v1
      * ok == true AND state == OK
      * environment == PAPER
      * ib_account matches submission ib_account
      * provenance.truth_root == this repo's canonical sleeve execution root
  - execution identity binding resolves exactly one governed submit identity:
      * sleeve_id
      * environment
      * account_id
      * client_id_orders
      * submission ib_account matches the governed account_id
      * submission ib_client_id matches the governed client_id_orders
  - phasec_out_dir is under repo_root (reject /tmp and other external dirs)

DRY RUN:
- If dry_run=True, all gates are enforced and all artifacts are written,
  but the broker adapter is NOT connected and no order is submitted.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.adapters.broker_adapter_v1 import BrokerConnectionSpec
from constellation_2.phaseD.adapters.ib_paper_adapter_v2 import IBAdapterError, IBPaperAdapterV2
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.evidence_writer_v1 import (
    EvidenceWriteError,
    write_phased_submission_only_v1,
    write_phased_success_outputs_v1,
    write_phased_veto_only_v1,
)
from constellation_2.phaseD.lib.broker_reconciliation_artifacts_v1 import (
    build_broker_acknowledgement_v1,
    build_broker_order_outcome_v1,
    build_broker_submit_attempt_v1,
    write_broker_acknowledgement_v1,
    write_broker_order_outcome_v1,
    write_broker_submit_attempt_v1,
)
from constellation_2.phaseD.lib.ib_payload_bag_order_v1 import build_binding_digest_for_order_plan_v1
from constellation_2.phaseD.lib.ib_payload_stock_order_v1 import build_binding_digest_for_equity_order_plan_v1
from constellation_2.phaseD.lib.ib_payload_stock_order_v2 import build_binding_digest_for_equity_order_plan_v2
from constellation_2.phaseD.lib.idempotency_guard_v1 import (
    IdempotencyError,
    assert_idempotent_or_raise_v1,
)
from constellation_2.phaseD.lib.lineage_assert_v1 import (
    LineageViolation,
    assert_no_synth_status_in_paper,
    assert_required_lineage_fields,
)
from constellation_2.phaseD.lib.risk_budget_gate_v1 import enforce_risk_budget_against_whatif_v1
from constellation_2.common.execution_identity_authority_v1 import resolve_submission_identity_v1
from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    truth_root_from_execution_kernel_artifact_path_v1,
)
from constellation_2.common.execution_kernel.execution_state_record_v1 import (
    write_execution_attempt_state_record_v1,
)
from constellation_2.common.execution_kernel.execution_submission_record_v1 import ExecutionSubmissionRecordV1
from constellation_2.common.constitutional_runtime_v1 import (
    assert_constitutional_completeness_v1,
)
from constellation_2.common.runtime_control_kernel.runtime_control_runner_v1 import (
    run_runtime_control_kernel_v1,
)
from constellation_2.common.execution_identity_binding_v1 import (
    RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH,
    RC_EXECUTION_IDENTITY_ACCOUNT_MISSING,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING,
    RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING,
    RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION,
    RC_EXECUTION_IDENTITY_SLEEVE_MISSING,
    RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED,
    enforce_submit_execution_identity_v1,
)
from constellation_2.common.paper_execution_authority_v1 import (
    RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN,
    RC_EXECUTION_ROOT_MODE_MISSING,
    RC_EXECUTION_ROOT_PATH_MISMATCH,
    RC_EXECUTION_ROOT_PATH_UNRESOLVED,
    RC_EXECUTION_ROOT_SLEEVE_ID_MISSING,
    require_governed_execution_family_path,
    resolve_governed_paper_execution_roots,
)
from constellation_2.common.kill_switch_authority_v1 import (
    STATUS_PASS as KILL_SWITCH_STATUS_PASS,
    resolve_kill_switch_authority_v1,
)
from constellation_2.common.runtime_base_v1 import advisor_runtime_root
from constellation_2.common.advisory.household_portfolio_compiler_v1 import (
    evaluate_portfolio_authorization_v1,
)
from constellation_2.common.advisory.household_portfolio_storage_v1 import (
    load_portfolio_authorization_for_execution_intent_v1,
)
from constellation_2.common.runtime_contract_v1 import (
    resolve_canonical_truth_root,
    resolve_truth_sleeves_root,
)
from constellation_2.common.trade_readiness_reducer_v1 import (
    build_trade_readiness_decision_payload_v1,
    write_trade_readiness_presubmit_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_trade_readiness_presubmit_path,
)
from constellation_2.common.c2_risk_policy_loader_v1 import (
    RiskPolicyLoaderError,
    get_allow_entry_only_paper_test_or_fail,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


class SubmitBoundaryV4Error(Exception):
    pass


RC_ENV_NOT_PAPER = "C2_BROKER_ENV_NOT_PAPER"
RC_FAIL_CLOSED = "C2_SUBMIT_FAIL_CLOSED_REQUIRED"
RC_LINEAGE_VIOLATION = "C2_LINEAGE_VIOLATION"

RC_PHASEC_OUT_DIR_UNSAFE = "C2_SUBMIT_PHASEC_OUT_DIR_UNSAFE"
RC_GATE_STACK_NOT_PASS = "C2_SUBMIT_GATE_STACK_NOT_PASS"
RC_AUTHORIZATION_VERDICT_NOT_PASS = "C2_SUBMIT_AUTHORIZATION_VERDICT_NOT_PASS"
RC_KILL_SWITCH_ACTIVE = "C2_SUBMIT_KILL_SWITCH_ACTIVE"

RC_AUTHZ_MISSING = "C2_SUBMIT_AUTHZ_MISSING"
RC_AUTHZ_NOT_AUTHORIZED = "C2_SUBMIT_AUTHZ_NOT_AUTHORIZED"

RC_IB_ACCOUNT_REGISTRY_INVALID = "C2_SUBMIT_IB_ACCOUNT_REGISTRY_INVALID"
RC_IB_ACCOUNT_NOT_ALLOWED = "C2_SUBMIT_IB_ACCOUNT_NOT_ALLOWED"
RC_ENGINE_SYMBOL_POLICY_INVALID = "C2_SUBMIT_ENGINE_SYMBOL_POLICY_INVALID"
RC_ENGINE_SYMBOL_NOT_ALLOWED = "C2_SUBMIT_ENGINE_SYMBOL_NOT_ALLOWED"

RC_READINESS_C2_NOT_OK = "C2_SUBMIT_READINESS_C2_NOT_OK"
RC_READINESS_C2_NONAUTHORITATIVE = "C2_SUBMIT_READINESS_C2_NONAUTHORITATIVE"

RC_AUTHORITY_HEAD_MISSING = "C2_SUBMIT_AUTHORITY_HEAD_MISSING"
RC_AUTHORITY_HEAD_INVALID = "C2_SUBMIT_AUTHORITY_HEAD_INVALID"
RC_INTENT_PROTECTIVE_STOP_MISSING = "INTENT_PROTECTIVE_STOP_MISSING"
RC_PROTECTIVE_STOP_REQUIRED_BUT_MISSING = "PROTECTIVE_STOP_REQUIRED_BUT_MISSING"
RC_BRACKET_SUBMISSION_FAILED = "BRACKET_SUBMISSION_FAILED"
RC_ADVISORY_EXECUTION_PACKAGE_REQUIRED = "C2_SUBMIT_ADVISORY_EXECUTION_PACKAGE_REQUIRED"
RC_ADVISORY_EXECUTION_PROVENANCE_MISSING = "C2_SUBMIT_ADVISORY_EXECUTION_PROVENANCE_MISSING"
RC_ADVISORY_EXECUTION_PROVENANCE_MISMATCH = "C2_SUBMIT_ADVISORY_EXECUTION_PROVENANCE_MISMATCH"
RC_EXECUTION_SUBMISSION_RECORD_REQUIRED = "C2_SUBMIT_EXECUTION_SUBMISSION_RECORD_REQUIRED"
RC_EXECUTION_SUBMISSION_RECORD_MISMATCH = "C2_SUBMIT_EXECUTION_SUBMISSION_RECORD_MISMATCH"
RC_RAW_CANDIDATE_SUBMIT_DISABLED = "C2_SUBMIT_RAW_CANDIDATE_DISABLED"
BROKER_TRANSMIT_ENABLEMENT_MSG = (
    "explicit micro-live path requires C2_ENABLE_BROKER_TRANSMIT=YES with dry_run=False"
)
SUBMISSION_PREWRITE_FILENAMES = (
    "broker_submit_attempt_v1.json",
    "broker_acknowledgement_v1.json",
    "broker_order_outcome_v1.json",
)


def _parse_utc_z(ts: str) -> None:
    if not isinstance(ts, str) or not ts.endswith("Z"):
        raise SubmitBoundaryV4Error(f"EVAL_TIME_UTC_INVALID_Z: {ts!r}")
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    if dt.tzinfo is None:
        raise SubmitBoundaryV4Error("EVAL_TIME_UTC_TZINFO_MISSING")


def _day_from_eval_time_utc(eval_time_utc: str) -> str:
    _parse_utc_z(eval_time_utc)
    dt = datetime.fromisoformat(eval_time_utc.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.date().isoformat()


def _safe_eval_stamp(eval_time_utc: str) -> str:
    _parse_utc_z(eval_time_utc)
    dt = datetime.fromisoformat(eval_time_utc.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.strftime("%Y%m%dT%H%M%SZ")


def _archive_existing_submission_dir_for_retry(
    *,
    day_dir: Path,
    submission_id: str,
    eval_time_utc: str,
    classification: str,
) -> Path:
    submission_dir = (day_dir / submission_id).resolve()
    if not submission_dir.exists():
        raise SubmitBoundaryV4Error(
            f"IDEMPOTENCY_RETRY_ARCHIVE_SOURCE_MISSING:path={submission_dir}"
        )
    if not submission_dir.is_dir():
        raise SubmitBoundaryV4Error(
            f"IDEMPOTENCY_RETRY_ARCHIVE_SOURCE_NOT_DIRECTORY:path={submission_dir}"
        )
    archive_root = (day_dir / "_idempotency_retry_archive_v1" / submission_id).resolve()
    archive_root.mkdir(parents=True, exist_ok=True)
    base_name = f"{_safe_eval_stamp(eval_time_utc)}__classification_{str(classification or '').strip().upper()}"
    archive_path = (archive_root / base_name).resolve()
    seq = 1
    while archive_path.exists():
        archive_path = (archive_root / f"{base_name}__r{seq:02d}").resolve()
        seq += 1
    try:
        submission_dir.rename(archive_path)
    except Exception as exc:  # noqa: BLE001
        raise SubmitBoundaryV4Error(
            f"IDEMPOTENCY_RETRY_ARCHIVE_MOVE_FAILED:source={submission_dir}:target={archive_path}:error={type(exc).__name__}"
        ) from exc
    return archive_path


def _resolve_submit_day_utc(
    *,
    eval_day_utc: str,
    package_obj: Optional[Dict[str, Any]],
    submission_record: Optional[ExecutionSubmissionRecordV1],
) -> str:
    day = str(eval_day_utc or "").strip()
    if not day:
        raise SubmitBoundaryV4Error("SUBMIT_DAY_UTC_MISSING")
    if package_obj is None:
        return day

    package_day = str(package_obj.get("day_utc") or "").strip()
    if not package_day:
        raise SubmitBoundaryV4Error("EXECUTION_PACKAGE_DAY_UTC_MISSING")

    if submission_record is not None:
        record_day = str(submission_record.day_utc or "").strip()
        if not record_day:
            raise SubmitBoundaryV4Error(f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:day_utc_missing")
        if record_day != package_day:
            raise SubmitBoundaryV4Error(
                f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:day_utc record={record_day} package={package_day}"
            )

    return package_day


def _read_json_file(path: Path) -> Any:
    import json

    if not path.exists():
        raise SubmitBoundaryV4Error(f"INPUT_FILE_MISSING: {str(path)}")
    if not path.is_file():
        raise SubmitBoundaryV4Error(f"INPUT_PATH_NOT_FILE: {str(path)}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _dependency_refs_by_id(package_obj: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    refs = package_obj.get("dependency_refs")
    if not isinstance(refs, list):
        raise SubmitBoundaryV4Error("EXECUTION_PACKAGE_DEPENDENCY_REFS_INVALID")
    out: Dict[str, Dict[str, Any]] = {}
    for row in refs:
        if not isinstance(row, dict):
            continue
        dep_id = str(row.get("dependency_id") or "").strip()
        if dep_id:
            out[dep_id] = row
    return out


def _read_execution_package(repo_root: Path, execution_package_path: Path) -> Tuple[Dict[str, Any], Path]:
    package_path = execution_package_path.resolve()
    package_obj = _read_json_file(package_path)
    if not isinstance(package_obj, dict):
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_INVALID:path={package_path}")
    if str(package_obj.get("schema_id") or "").strip() != "execution_package":
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_SCHEMA_ID_INVALID:path={package_path}")
    if str(package_obj.get("schema_version") or "").strip() != "v1":
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_SCHEMA_VERSION_INVALID:path={package_path}")
    if package_obj.get("sealed") is not True:
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_NOT_SEALED:path={package_path}")
    expected_package_sha = str(package_obj.get("canonical_json_hash") or "").strip()
    actual_package_sha = canonical_hash_for_c2_artifact_v1({**package_obj, "canonical_json_hash": None})
    if not expected_package_sha or expected_package_sha != actual_package_sha:
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_CANONICAL_JSON_HASH_MISMATCH:path={package_path}")

    build_ref = package_obj.get("build_ref")
    if not isinstance(build_ref, dict):
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_BUILD_REF_MISSING:path={package_path}")
    build_path = Path(str(build_ref.get("path") or "").strip()).resolve()
    if not build_path.exists() or not build_path.is_file():
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_BUILD_ARTIFACT_MISSING:path={build_path}")
    expected_build_sha = str(build_ref.get("sha256") or "").strip()
    actual_build_sha = _sha256_file(build_path)
    if expected_build_sha and expected_build_sha != actual_build_sha:
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_BUILD_SHA256_MISMATCH:path={build_path}")
    build_obj = _read_json_file(build_path)
    if str(build_obj.get("schema_id") or "").strip() != "execution_build":
        raise SubmitBoundaryV4Error(f"EXECUTION_BUILD_SCHEMA_ID_INVALID:path={build_path}")
    if str(build_obj.get("closure_status") or "").strip().upper() != "COMPLETE":
        raise SubmitBoundaryV4Error(f"EXECUTION_BUILD_NOT_COMPLETE:path={build_path}")
    selected_order_plan_ref = package_obj.get("selected_order_plan_ref")
    if not isinstance(selected_order_plan_ref, dict):
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_SELECTED_ORDER_PLAN_REF_MISSING:path={package_path}")
    selected_order_plan_path = Path(str(selected_order_plan_ref.get("path") or "").strip()).resolve()
    if not selected_order_plan_path.exists() or not selected_order_plan_path.is_file():
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_SELECTED_ORDER_PLAN_PATH_MISSING:path={selected_order_plan_path}")
    expected_order_plan_sha = str(selected_order_plan_ref.get("sha256") or "").strip()
    actual_order_plan_sha = _sha256_file(selected_order_plan_path)
    if not expected_order_plan_sha or expected_order_plan_sha != actual_order_plan_sha:
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_SELECTED_ORDER_PLAN_SHA256_MISMATCH:path={selected_order_plan_path}")

    validate_against_repo_schema_v1(package_obj, repo_root, "governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_package.v1.schema.json")
    return package_obj, build_path


def _require_package_dependency_ref(package_refs: Dict[str, Dict[str, Any]], dependency_id: str) -> Path:
    row = package_refs.get(dependency_id)
    if row is None:
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_DEPENDENCY_MISSING:dependency_id={dependency_id}")
    path = Path(str(row.get("path") or "").strip()).resolve()
    if not path.exists() or not path.is_file():
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_DEPENDENCY_PATH_MISSING:dependency_id={dependency_id}:path={path}")
    expected_sha = str(row.get("sha256") or "").strip()
    if expected_sha and expected_sha != _sha256_file(path):
        raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_DEPENDENCY_SHA256_MISMATCH:dependency_id={dependency_id}:path={path}")
    return path


def _execution_identity_source_refs_by_type(execution_identity_obj: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not isinstance(execution_identity_obj, dict):
        return {}
    refs = execution_identity_obj.get("source_refs")
    if not isinstance(refs, list):
        return {}
    out: Dict[str, str] = {}
    for row in refs:
        if not isinstance(row, dict):
            continue
        ref_type = str(row.get("type") or "").strip()
        ref_value = str(row.get("path") or "").strip()
        if ref_type and ref_value:
            out[ref_type] = ref_value
    return out


def _enforce_advisory_execution_choke_point(
    *,
    package_obj: Optional[Dict[str, Any]],
    execution_identity_obj: Optional[Dict[str, Any]],
    plan_obj: Dict[str, Any],
) -> None:
    source_refs = _execution_identity_source_refs_by_type(execution_identity_obj)
    advisory_fields = (
        "execution_intent_id",
        "execution_intent_canonical_hash",
        "promotion_record_id",
        "promotion_idempotency_key",
    )
    present = {field: source_refs.get(field, "") for field in advisory_fields if source_refs.get(field, "")}
    if present and len(present) != len(advisory_fields):
        raise SubmitBoundaryV4Error(
            f"{RC_ADVISORY_EXECUTION_PROVENANCE_MISMATCH}:source_refs_incomplete={sorted(present.keys())}"
        )

    advisory_origin = bool(present)
    if advisory_origin and package_obj is None:
        raise SubmitBoundaryV4Error(RC_ADVISORY_EXECUTION_PACKAGE_REQUIRED)

    package_provenance = package_obj.get("advisory_submission") if isinstance(package_obj, dict) and isinstance(package_obj.get("advisory_submission"), dict) else None
    if not advisory_origin and package_provenance is None:
        return
    if package_provenance is None:
        raise SubmitBoundaryV4Error(RC_ADVISORY_EXECUTION_PROVENANCE_MISSING)
    if not advisory_origin:
        raise SubmitBoundaryV4Error(f"{RC_ADVISORY_EXECUTION_PROVENANCE_MISMATCH}:candidate_source_refs_missing")

    origin = str(package_provenance.get("origin") or "").strip()
    if origin != "advisory_kernel_v1":
        raise SubmitBoundaryV4Error(f"{RC_ADVISORY_EXECUTION_PROVENANCE_MISMATCH}:origin={origin or 'MISSING'}")

    for field in advisory_fields:
        package_value = str(package_provenance.get(field) or "").strip()
        if not package_value:
            raise SubmitBoundaryV4Error(f"{RC_ADVISORY_EXECUTION_PROVENANCE_MISSING}:{field}")
        if package_value != present[field]:
            raise SubmitBoundaryV4Error(
                f"{RC_ADVISORY_EXECUTION_PROVENANCE_MISMATCH}:{field}:package={package_value}:candidate={present[field]}"
            )

    execution_intent_id = present["execution_intent_id"]
    promotion_idempotency_key = present["promotion_idempotency_key"]
    package_intent_id = str(package_obj.get("intent_id") or "").strip() if isinstance(package_obj, dict) else ""
    execution_identity_intent_id = str(execution_identity_obj.get("intent_id") or "").strip() if isinstance(execution_identity_obj, dict) else ""
    plan_source_intent_id = str(plan_obj.get("source_intent_id") or "").strip()
    plan_intent_hash = str(plan_obj.get("intent_hash") or "").strip()

    if package_intent_id != execution_intent_id:
        raise SubmitBoundaryV4Error(
            f"{RC_ADVISORY_EXECUTION_PROVENANCE_MISMATCH}:package_intent_id={package_intent_id}:execution_intent_id={execution_intent_id}"
        )
    if execution_identity_intent_id != execution_intent_id:
        raise SubmitBoundaryV4Error(
            f"{RC_ADVISORY_EXECUTION_PROVENANCE_MISMATCH}:execution_identity_intent_id={execution_identity_intent_id}:execution_intent_id={execution_intent_id}"
        )
    if plan_source_intent_id != execution_intent_id:
        raise SubmitBoundaryV4Error(
            f"{RC_ADVISORY_EXECUTION_PROVENANCE_MISMATCH}:plan_source_intent_id={plan_source_intent_id}:execution_intent_id={execution_intent_id}"
        )
    if plan_intent_hash != promotion_idempotency_key:
        raise SubmitBoundaryV4Error(
            f"{RC_ADVISORY_EXECUTION_PROVENANCE_MISMATCH}:plan_intent_hash={plan_intent_hash}:promotion_idempotency_key={promotion_idempotency_key}"
        )


def _enforce_household_portfolio_authorization_v1(
    *,
    package_obj: Optional[Dict[str, Any]],
    execution_identity_obj: Optional[Dict[str, Any]],
    eval_time_utc: str,
    pointers: List[str],
) -> None:
    source_refs = _execution_identity_source_refs_by_type(execution_identity_obj)
    package_provenance = (
        package_obj.get("advisory_submission")
        if isinstance(package_obj, dict) and isinstance(package_obj.get("advisory_submission"), dict)
        else None
    )
    execution_intent_id = str(source_refs.get("execution_intent_id") or "").strip()
    if not execution_intent_id and package_provenance is not None:
        execution_intent_id = str(package_provenance.get("execution_intent_id") or "").strip()
    if not execution_intent_id:
        return
    try:
        execution_intent, execution_intent_path, authorization_obj, authorization_path = load_portfolio_authorization_for_execution_intent_v1(
            advisor_runtime_root(),
            execution_intent_id,
        )
    except FileNotFoundError as exc:
        raise SubmitBoundaryV4Error("AUTH_ACTION_NOT_EXPLICITLY_ALLOWED:PORTFOLIO_AUTHORIZATION_MISSING") from exc
    except ValueError as exc:
        raise SubmitBoundaryV4Error(f"AUTH_ACTION_NOT_EXPLICITLY_ALLOWED:{exc}") from exc

    pointers.append(str(execution_intent_path.resolve()))
    pointers.append(str(authorization_path.resolve()))
    result = evaluate_portfolio_authorization_v1(
        portfolio_authorization=authorization_obj,
        execution_intent=execution_intent,
        eval_time_utc=eval_time_utc,
    )
    if result["allowed"]:
        return
    reason_codes = list(result.get("reason_codes") or ["AUTH_ACTION_NOT_EXPLICITLY_ALLOWED"])
    raise SubmitBoundaryV4Error(f"{reason_codes[0]}:HOUSEHOLD_PORTFOLIO_AUTHORIZATION_BLOCKED")


def _load_and_enforce_submission_record(
    *,
    submission_record_path: Optional[Path],
    execution_package_path: Optional[Path],
    package_obj: Optional[Dict[str, Any]],
    phasec_out_dir: Optional[Path],
) -> Optional[ExecutionSubmissionRecordV1]:
    if execution_package_path is None or package_obj is None:
        return None
    if submission_record_path is None:
        raise SubmitBoundaryV4Error(RC_EXECUTION_SUBMISSION_RECORD_REQUIRED)
    record = ExecutionSubmissionRecordV1.load_file(submission_record_path.resolve())
    if record.status != "READY_TO_SUBMIT":
        raise SubmitBoundaryV4Error(f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:status={record.status}")

    expected_package_path = Path(str(record.execution_package_ref.get("path") or "")).resolve()
    if expected_package_path != execution_package_path.resolve():
        raise SubmitBoundaryV4Error(f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:package_path")
    expected_package_sha = str(record.execution_package_ref.get("sha256") or "").strip()
    actual_package_sha = str(package_obj.get("canonical_json_hash") or "").strip()
    if expected_package_sha != actual_package_sha:
        raise SubmitBoundaryV4Error(f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:package_sha256")

    package_submission_id = str(package_obj.get("submission_id") or "").strip()
    if record.submission_id != package_submission_id:
        raise SubmitBoundaryV4Error(f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:submission_id")

    candidate_ref = package_obj.get("candidate_ref") if isinstance(package_obj.get("candidate_ref"), dict) else {}
    package_candidate_path = Path(str(candidate_ref.get("phasec_out_dir") or "").strip()).resolve()
    expected_candidate_path = Path(str(record.candidate_ref.get("path") or "")).resolve()
    if expected_candidate_path != package_candidate_path:
        raise SubmitBoundaryV4Error(f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:candidate_path")
    selected_order_plan_ref = package_obj.get("selected_order_plan_ref") if isinstance(package_obj.get("selected_order_plan_ref"), dict) else {}
    package_order_plan_path = Path(str(selected_order_plan_ref.get("path") or "").strip()).resolve()
    expected_order_plan_path = Path(str(record.downstream_payload_ref.get("path") or "")).resolve()
    if expected_order_plan_path != package_order_plan_path:
        raise SubmitBoundaryV4Error(f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:downstream_payload_path")
    package_order_plan_sha = str(selected_order_plan_ref.get("sha256") or "").strip()
    expected_order_plan_sha = str(record.downstream_payload_ref.get("sha256") or "").strip()
    if expected_order_plan_sha != package_order_plan_sha:
        raise SubmitBoundaryV4Error(f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:downstream_payload_sha256")
    if phasec_out_dir is not None and phasec_out_dir.resolve() != package_candidate_path:
        raise SubmitBoundaryV4Error(f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:phasec_out_dir")

    advisory_submission = package_obj.get("advisory_submission") if isinstance(package_obj.get("advisory_submission"), dict) else {}
    execution_intent_id = str(advisory_submission.get("execution_intent_id") or "").strip()
    if execution_intent_id and execution_intent_id != record.execution_intent_id:
        raise SubmitBoundaryV4Error(f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:execution_intent_id")
    promotion_record_id = str(advisory_submission.get("promotion_record_id") or "").strip()
    if promotion_record_id and promotion_record_id != record.promotion_record_id:
        raise SubmitBoundaryV4Error(f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:promotion_record_id")
    return record


def _read_global_context_package_from_path(path: Path, *, day: str, sleeve_id: str, environment: str, account_id: str) -> Path:
    obj = _read_json_file(path)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"GLOBAL_CONTEXT_PACKAGE_INVALID:path={path}")
    if str(obj.get("schema_id") or "").strip() != "global_context_package":
        raise SubmitBoundaryV4Error(f"GLOBAL_CONTEXT_PACKAGE_SCHEMA_ID_INVALID:path={path}")
    if str(obj.get("schema_version") or "").strip() != "v1":
        raise SubmitBoundaryV4Error(f"GLOBAL_CONTEXT_PACKAGE_SCHEMA_VERSION_INVALID:path={path}")
    if obj.get("sealed") is not True:
        raise SubmitBoundaryV4Error(f"GLOBAL_CONTEXT_PACKAGE_NOT_SEALED:path={path}")
    pkg_day = str(obj.get("day_utc") or "").strip()
    pkg_sleeve = str(obj.get("sleeve_id") or "").strip().upper()
    pkg_env = str(obj.get("mode") or "").strip().upper()
    pkg_account = str(obj.get("account_id") or "").strip().upper()
    if pkg_day != day or pkg_sleeve != sleeve_id or pkg_env != environment or pkg_account != account_id.upper():
        raise SubmitBoundaryV4Error(
            f"GLOBAL_CONTEXT_PACKAGE_CONTEXT_MISMATCH:path={path}:day={pkg_day}:sleeve={pkg_sleeve}:mode={pkg_env}:account={pkg_account}"
        )
    return path


def _validate_authority_head_path(path: Path, day: str) -> Path:
    obj = _read_json_file(path)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHORITY_HEAD_INVALID}: not_object path={path}")
    schema_id = str(obj.get("schema_id") or "").strip()
    schema_ver = str(obj.get("schema_version") or "").strip()
    if schema_id != "c2_run_pointer_canonical_authority_head" or schema_ver != "v1":
        raise SubmitBoundaryV4Error(f"{RC_AUTHORITY_HEAD_INVALID}: schema_mismatch path={path}")
    if obj.get("ok") is False:
        raise SubmitBoundaryV4Error(f"{RC_AUTHORITY_HEAD_MISSING}: {obj.get('error')}")
    head_day = str(obj.get("day_utc") or "").strip()
    head_status = str(obj.get("status") or "").strip().upper()
    head_authoritative = bool(obj.get("authoritative"))
    if head_day != day or head_status not in ("PASS", "BOOTSTRAP_PASS") or not head_authoritative:
        raise SubmitBoundaryV4Error(
            f"{RC_AUTHORITY_HEAD_INVALID}: head_mismatch day={head_day} status={head_status} authoritative={head_authoritative} expected_day={day}"
        )
    return path


def _read_authorization_gate_verdict_status_from_path(path: Path, day: str) -> Tuple[str, Path]:
    obj = _read_json_file(path)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHORIZATION_VERDICT_NOT_PASS}: invalid authorization_gate_verdict type: path={path}")
    if str(obj.get("schema_id") or "").strip() != "authorization_gate_verdict_v1":
        raise SubmitBoundaryV4Error(f"{RC_AUTHORIZATION_VERDICT_NOT_PASS}: schema_mismatch path={path}")
    day_utc = str(obj.get("day_utc") or "").strip()
    if day_utc and day_utc != day:
        raise SubmitBoundaryV4Error(f"{RC_AUTHORIZATION_VERDICT_NOT_PASS}: day_mismatch path={path}")
    status = str(obj.get("status") or "").strip().upper()
    return status, path


def _read_authorization_from_path(path: Path) -> Tuple[str, str, int, str, Path]:
    obj = _read_json_file(path)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: invalid authorization type: path={path}")
    schema_id = str(obj.get("schema_id") or "").strip()
    if schema_id != "C2_AUTHORIZATION_V1":
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: schema_id_mismatch got={schema_id!r} path={path}")
    status = str(obj.get("status") or "").strip().upper()
    auth = obj.get("authorization")
    if not isinstance(auth, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: missing authorization object: path={path}")
    decision = str(auth.get("decision") or "").strip().upper()
    try:
        qty = int(auth.get("authorized_quantity") or 0)
    except Exception:
        qty = 0
    return status, decision, qty, _sha256_file(path), path


def _read_capital_authority_from_path(path: Path, intent_hash: str) -> Tuple[str, int, str, Path]:
    obj = _read_json_file(path)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: invalid capital authority type: path={path}")
    schema_id = str(obj.get("schema_id") or "").strip()
    if schema_id != "C2_CAPITAL_AUTHORITY_ALLOCATION_V1":
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: capital_authority_schema_id_mismatch got={schema_id!r} path={path}")
    decision_chain = obj.get("decision_chain")
    if not isinstance(decision_chain, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: capital_authority_decision_chain_missing path={path}")
    rows = decision_chain.get("authorized_trade_intents")
    if not isinstance(rows, list):
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: capital_authority_authorized_trade_intents_missing path={path}")
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("intent_hash") or "").strip() != intent_hash:
            continue
        outcome = str(row.get("authorization_outcome") or "").strip().upper()
        try:
            qty = int(row.get("authorized_quantity") or 0)
        except Exception:
            qty = 0
        return outcome, qty, _sha256_file(path), path
    raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: capital_authority_intent_hash_missing intent_hash={intent_hash} path={path}")


def _require_paper(env: str) -> None:
    if env != "PAPER":
        raise SubmitBoundaryV4Error(RC_ENV_NOT_PAPER)


def _require_phasec_out_dir_under_truth_root(truth_root: Path, p: Path) -> None:
    allowed_root = (truth_root.resolve() / "phaseC_preflight_v1").resolve()
    pp = p.resolve()
    try:
        pp.relative_to(allowed_root)
    except Exception:
        raise SubmitBoundaryV4Error(
            f"{RC_PHASEC_OUT_DIR_UNSAFE}: path_not_under_phasec_truth_root: path={pp} phasec_root={allowed_root}"
        )


def _resolve_submit_scope_from_phasec_out_dir(phasec_out_dir: Path) -> tuple[str, str]:
    truth_sleeves_root = resolve_truth_sleeves_root().resolve()
    phasec_path = phasec_out_dir.resolve()
    try:
        relative = phasec_path.relative_to(truth_sleeves_root)
    except ValueError as exc:
        raise SubmitBoundaryV4Error(
            f"{RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN}:phasec_out_dir={phasec_path}:truth_sleeves_root={truth_sleeves_root}"
        ) from exc
    parts = relative.parts
    sleeve_id = str(parts[0]).strip().upper() if len(parts) >= 1 else ""
    mode = str(parts[1]).strip().upper() if len(parts) >= 2 else ""
    if not sleeve_id:
        raise SubmitBoundaryV4Error(
            f"{RC_EXECUTION_IDENTITY_SLEEVE_MISSING}:phasec_out_dir={phasec_path}:truth_sleeves_root={truth_sleeves_root}"
        )
    if not mode:
        raise SubmitBoundaryV4Error(
            f"{RC_EXECUTION_ROOT_MODE_MISSING}:phasec_out_dir={phasec_path}:truth_sleeves_root={truth_sleeves_root}"
        )
    return sleeve_id, mode


def _resolve_execution_roots_for_phasec_out_dir(
    *,
    repo_root: Path,
    phasec_out_dir: Path,
    ib_account: str,
    sleeve_id: str,
) -> object:
    rr = repo_root.resolve()
    roots = resolve_governed_paper_execution_roots(
        repo_root=rr,
        environment="PAPER",
        ib_account=str(ib_account).strip(),
        sleeve_id=str(sleeve_id).strip().upper(),
    )
    require_governed_execution_family_path(
        repo_root=rr,
        environment="PAPER",
        ib_account=str(ib_account).strip(),
        sleeve_id=roots.sleeve_id,
        family="phaseC_preflight_v1",
        actual_path=phasec_out_dir,
    )
    return roots


def _broker_transmit_enabled() -> bool:
    return str(os.environ.get("C2_ENABLE_BROKER_TRANSMIT") or "").strip().upper() == "YES"


def _require_explicit_live_enablement(*, dry_run: bool) -> None:
    if dry_run:
        return
    if not _broker_transmit_enabled():
        raise SubmitBoundaryV4Error(f"BROKER_TRANSMIT_DISABLED: {BROKER_TRANSMIT_ENABLEMENT_MSG}")


def _extract_reason_code_and_detail(error: Exception, *, default_code: str) -> Tuple[str, str]:
    message = str(error).strip() or repr(error)
    if ":" in message:
        possible_code, remainder = message.split(":", 1)
        possible_code = possible_code.strip().upper()
        if possible_code in {
            RC_INTENT_PROTECTIVE_STOP_MISSING,
            RC_PROTECTIVE_STOP_REQUIRED_BUT_MISSING,
            RC_BRACKET_SUBMISSION_FAILED,
        }:
            return possible_code, remainder.strip() or message
    return default_code, message


def _allow_entry_only_paper_test_or_fail(engine_id: str) -> bool:
    try:
        return bool(get_allow_entry_only_paper_test_or_fail(engine_id))
    except RiskPolicyLoaderError as e:
        raise SubmitBoundaryV4Error(f"{RC_PROTECTIVE_STOP_REQUIRED_BUT_MISSING}: governed_stop_policy_unavailable: {e}") from e


def _require_equity_protective_stop_or_fail(*, plan_obj: Dict[str, Any], engine_id: str) -> None:
    if str(plan_obj.get("schema_id") or "").strip() != "equity_order_plan":
        return
    allow_entry_only = _allow_entry_only_paper_test_or_fail(engine_id)
    stop = plan_obj.get("protective_stop")
    if not isinstance(stop, dict):
        if allow_entry_only:
            return
        raise SubmitBoundaryV4Error(
            f"{RC_PROTECTIVE_STOP_REQUIRED_BUT_MISSING}: protective_stop missing; "
            "policy allow_entry_only_paper_test=false"
        )
    order_type = str(stop.get("order_type") or "").strip().upper()
    stop_price = str(stop.get("stop_price") or "").strip()
    stop_loss_bps = stop.get("stop_loss_bps")
    basis = str(stop.get("basis") or "").strip().upper()
    if order_type != "STOP":
        raise SubmitBoundaryV4Error(
            f"{RC_PROTECTIVE_STOP_REQUIRED_BUT_MISSING}: protective_stop.order_type must be STOP"
        )
    if not stop_price:
        raise SubmitBoundaryV4Error(
            f"{RC_PROTECTIVE_STOP_REQUIRED_BUT_MISSING}: protective_stop.stop_price missing"
        )
    if not isinstance(stop_loss_bps, int) or stop_loss_bps <= 0:
        raise SubmitBoundaryV4Error(
            f"{RC_PROTECTIVE_STOP_REQUIRED_BUT_MISSING}: protective_stop.stop_loss_bps missing or non-positive"
        )
    if basis != "ENTRY_REFERENCE_PRICE":
        raise SubmitBoundaryV4Error(
            f"{RC_PROTECTIVE_STOP_REQUIRED_BUT_MISSING}: protective_stop.basis invalid"
        )


def _read_authority_head(truth_root: Path, day: str) -> Path:
    """
    Trading submission requires canonical_authority_head for the submission day.
    This enforces: trading reads authority head only.
    """
    p = (truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()
    obj = _read_json_file(p)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHORITY_HEAD_INVALID}: not_object path={p}")

    schema_id = str(obj.get("schema_id") or "").strip()
    schema_ver = str(obj.get("schema_version") or "").strip()

    # "missing marker" written by materializer has schema_id=c2_run_pointer_canonical_authority_head and ok=false
    if schema_id != "c2_run_pointer_canonical_authority_head" or schema_ver != "v1":
        raise SubmitBoundaryV4Error(f"{RC_AUTHORITY_HEAD_INVALID}: schema_mismatch path={p}")

    ok = obj.get("ok")
    if ok is False:
        raise SubmitBoundaryV4Error(f"{RC_AUTHORITY_HEAD_MISSING}: {obj.get('error')}")

    head_day = str(obj.get("day_utc") or "").strip()
    head_status = str(obj.get("status") or "").strip().upper()
    head_authoritative = bool(obj.get("authoritative"))

    if head_day != day or head_status not in ("PASS", "BOOTSTRAP_PASS") or (not head_authoritative):
        raise SubmitBoundaryV4Error(
            f"{RC_AUTHORITY_HEAD_INVALID}: head_mismatch day={head_day} status={head_status} authoritative={head_authoritative} expected_day={day}"
        )

    return p


def _read_authorization_gate_verdict_status(truth_root: Path, day: str) -> Tuple[str, Path]:
    p = (truth_root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json").resolve()
    obj = _read_json_file(p)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHORIZATION_VERDICT_NOT_PASS}: invalid authorization_gate_verdict type: path={p}")
    if str(obj.get("schema_id") or "").strip() != "authorization_gate_verdict_v1":
        raise SubmitBoundaryV4Error(f"{RC_AUTHORIZATION_VERDICT_NOT_PASS}: schema_mismatch path={p}")
    status = str(obj.get("status") or "").strip().upper()
    return status, p


def _read_kill_switch_state(truth_root: Path, day: str) -> Tuple[str, bool, Path]:
    result = resolve_kill_switch_authority_v1(canonical_truth_root=truth_root, day_utc=day)
    if result.status != KILL_SWITCH_STATUS_PASS:
        raise SubmitBoundaryV4Error(f"{RC_KILL_SWITCH_ACTIVE}: {result.reason_code}: {result.reason_detail}")
    return result.state, result.allow_entries, result.canonical_path


def _read_authorization(truth_root: Path, day: str, intent_hash: str) -> Tuple[str, str, int, str, Path]:
    """
    Authorization path is deterministic and intent_hash-addressed:
      truth/engine_activity_v1/authorization_v1/<DAY>/<INTENT_HASH>.authorization.v1.json

    Must be AUTHORIZED with authorized_quantity > 0 to submit.
    Returns (status, decision, qty, sha256, path)
    """
    p = (truth_root / "engine_activity_v1" / "authorization_v1" / day / f"{intent_hash}.authorization.v1.json").resolve()
    obj = _read_json_file(p)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: invalid authorization type: path={p}")

    schema_id = str(obj.get("schema_id") or "").strip()
    if schema_id != "C2_AUTHORIZATION_V1":
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: schema_id_mismatch got={schema_id!r} path={p}")

    status = str(obj.get("status") or "").strip().upper()
    auth = obj.get("authorization")
    if not isinstance(auth, dict):
        raise SubmitBoundaryV4Error(f"{RC_AUTHZ_MISSING}: missing authorization object: path={p}")

    decision = str(auth.get("decision") or "").strip().upper()
    try:
        qty = int(auth.get("authorized_quantity") or 0)
    except Exception:
        qty = 0

    return status, decision, qty, _sha256_file(p), p


def _read_ib_account_registry(repo_root: Path) -> Dict[str, Any]:
    reg_path = (repo_root / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()
    obj = _read_json_file(reg_path)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_REGISTRY_INVALID}: registry_not_object path={reg_path}")
    if str(obj.get("schema_id") or "") != "c2_ib_account_registry":
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_REGISTRY_INVALID}: schema_id_mismatch path={reg_path}")
    if str(obj.get("schema_version") or "") != "v1":
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_REGISTRY_INVALID}: schema_version_mismatch path={reg_path}")
    return obj


def _read_engine_model_registry(repo_root: Path) -> Dict[str, Any]:
    reg_path = (repo_root / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()
    obj = _read_json_file(reg_path)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: registry_not_object path={reg_path}")
    if str(obj.get("schema_id") or "") != "engine_model_registry":
        raise SubmitBoundaryV4Error(f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: schema_id_mismatch path={reg_path}")
    if str(obj.get("schema_version") or "") != "v1":
        raise SubmitBoundaryV4Error(f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: schema_version_mismatch path={reg_path}")
    return obj


def _ranked_symbol_universe_path(execution_truth_root: Path, day_utc: str) -> Path:
    return (
        execution_truth_root
        / "reports"
        / "ranked_symbol_universe_v1"
        / str(day_utc).strip()
        / "ranked_symbol_universe.v1.json"
    ).resolve()


def _normalize_symbol_values(raw_values: List[Any]) -> List[str]:
    out: List[str] = []
    for raw in raw_values:
        sym = str(raw or "").strip().upper()
        if sym:
            out.append(sym)
    return sorted(set(out))


def _extract_symbol_from_intent_like(obj: Dict[str, Any]) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    direct_symbol = str(obj.get("symbol") or "").strip().upper()
    if direct_symbol:
        return direct_symbol
    underlying = obj.get("underlying")
    if isinstance(underlying, dict):
        nested_symbol = str(underlying.get("symbol") or "").strip().upper()
        if nested_symbol:
            return nested_symbol
    return None


def _resolve_current_symbol_universe(
    *,
    execution_truth_root: Optional[Path],
    day_utc: str,
    phasec_out_dir: Optional[Path],
    pointers: List[str],
) -> Optional[List[str]]:
    if execution_truth_root is not None and str(day_utc).strip():
        ranked_path = _ranked_symbol_universe_path(execution_truth_root.resolve(), str(day_utc).strip())
        if ranked_path.exists() and ranked_path.is_file():
            ranked_obj = _read_json_file(ranked_path)
            if not isinstance(ranked_obj, dict):
                raise SubmitBoundaryV4Error(f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: ranked_symbol_universe_not_object path={ranked_path}")
            schema_id = str(ranked_obj.get("schema_id") or "").strip()
            schema_version = str(ranked_obj.get("schema_version") or "").strip()
            if schema_id != "ranked_symbol_universe" or schema_version != "v1":
                raise SubmitBoundaryV4Error(
                    f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: ranked_symbol_universe_schema_invalid path={ranked_path}"
                )
            ranked_status = str(ranked_obj.get("status") or "").strip().upper()
            if ranked_status not in {"PASS", "BOOTSTRAP_PASS"}:
                raise SubmitBoundaryV4Error(
                    f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: ranked_symbol_universe_not_pass status={ranked_status} path={ranked_path}"
                )
            symbols_obj = ranked_obj.get("symbols")
            if not isinstance(symbols_obj, list):
                raise SubmitBoundaryV4Error(
                    f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: ranked_symbol_universe_symbols_not_list path={ranked_path}"
                )
            pointers.append(str(ranked_path))
            return _normalize_symbol_values(symbols_obj)

    if phasec_out_dir is not None:
        out: List[str] = []
        options_intent_path = (phasec_out_dir / "options_intent.v2.json").resolve()
        if options_intent_path.exists() and options_intent_path.is_file():
            options_intent_obj = _read_json_file(options_intent_path)
            if not isinstance(options_intent_obj, dict):
                raise SubmitBoundaryV4Error(f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: options_intent_not_object path={options_intent_path}")
            symbol = _extract_symbol_from_intent_like(options_intent_obj)
            if symbol:
                out.append(symbol)
            pointers.append(str(options_intent_path))

        adapter_path = (phasec_out_dir / "exposure_to_options_adapter_record.v1.json").resolve()
        if adapter_path.exists() and adapter_path.is_file():
            adapter_obj = _read_json_file(adapter_path)
            if not isinstance(adapter_obj, dict):
                raise SubmitBoundaryV4Error(
                    f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: options_adapter_record_not_object path={adapter_path}"
                )
            intent_like = adapter_obj.get("input_exposure_intent")
            if isinstance(intent_like, dict):
                symbol = _extract_symbol_from_intent_like(intent_like)
                if symbol:
                    out.append(symbol)
            pointers.append(str(adapter_path))

        normalized = _normalize_symbol_values(out)
        if normalized:
            return normalized

    return None


def _extract_symbol_from_plan(plan_obj: Dict[str, Any]) -> Optional[str]:
    if not isinstance(plan_obj, dict):
        return None
    for k in ["symbol", "ticker", "underlying_symbol"]:
        v = plan_obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    u = plan_obj.get("underlying")
    if isinstance(u, str) and u.strip():
        return u.strip().upper()
    if isinstance(u, dict):
        for k in ["symbol", "ticker"]:
            v = u.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip().upper()
    inst = plan_obj.get("instrument")
    if isinstance(inst, dict):
        for k in ["symbol", "ticker"]:
            v = inst.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip().upper()
    return None


def _enforce_ib_account_registry(
    *,
    repo_root: Path,
    ib_account: str,
    engine_id: str,
    pointers: List[str],
) -> None:
    reg = _read_ib_account_registry(repo_root)
    reg_path = (repo_root / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()
    pointers.append(str(reg_path))

    accounts = reg.get("accounts")
    if not isinstance(accounts, list):
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_REGISTRY_INVALID}: accounts_not_list path={reg_path}")

    entry: Optional[Dict[str, Any]] = None
    for a in accounts:
        if isinstance(a, dict) and str(a.get("account_id") or "").strip() == ib_account:
            entry = a
            break

    if entry is None:
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: account_not_in_registry account={ib_account}")

    env = str(entry.get("environment") or "").strip().upper()
    enabled = bool(entry.get("enabled_for_submission") is True)
    allowed = entry.get("allowed_engine_ids")
    allowed_list = allowed if isinstance(allowed, list) else []

    if env != "PAPER":
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: environment_not_paper env={env} account={ib_account}")
    if not str(ib_account).startswith("DU"):
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: paper_account_id_must_start_with_DU account={ib_account}")
    if not enabled:
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: enabled_for_submission=false account={ib_account}")

    if engine_id not in [str(x).strip() for x in allowed_list if isinstance(x, str)]:
        raise SubmitBoundaryV4Error(f"{RC_IB_ACCOUNT_NOT_ALLOWED}: engine_not_allowed engine_id={engine_id} account={ib_account}")

def _enforce_engine_symbol_policy(
    *,
    repo_root: Path,
    engine_id: str,
    plan_symbol: Optional[str],
    pointers: List[str],
    execution_truth_root: Optional[Path] = None,
    day_utc: str = "",
    phasec_out_dir: Optional[Path] = None,
) -> None:
    if not plan_symbol:
        raise SubmitBoundaryV4Error(
            f"{RC_ENGINE_SYMBOL_NOT_ALLOWED}: plan_symbol_missing engine_id={engine_id}"
        )

    current_universe = _resolve_current_symbol_universe(
        execution_truth_root=execution_truth_root,
        day_utc=str(day_utc).strip(),
        phasec_out_dir=phasec_out_dir.resolve() if phasec_out_dir is not None else None,
        pointers=pointers,
    )
    if current_universe is not None:
        if not current_universe:
            raise SubmitBoundaryV4Error(
                f"{RC_ENGINE_SYMBOL_NOT_ALLOWED}: current_symbol_universe_empty engine_id={engine_id}"
            )
        if plan_symbol.upper() not in current_universe:
            raise SubmitBoundaryV4Error(
                f"{RC_ENGINE_SYMBOL_NOT_ALLOWED}: symbol_not_allowed symbol={plan_symbol} "
                f"engine_id={engine_id} current_symbol_universe={current_universe}"
            )
        return

    # Legacy fallback for days where current symbol-universe evidence is unavailable.
    reg = _read_engine_model_registry(repo_root)
    reg_path = (repo_root / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()
    pointers.append(str(reg_path))

    engines = reg.get("engines")
    if not isinstance(engines, list):
        raise SubmitBoundaryV4Error(f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: engines_not_list path={reg_path}")

    entry: Optional[Dict[str, Any]] = None
    for row in engines:
        if isinstance(row, dict) and str(row.get("engine_id") or "").strip() == engine_id:
            entry = row
            break

    if entry is None:
        raise SubmitBoundaryV4Error(f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: engine_not_in_registry engine_id={engine_id}")

    if "allowed_symbols" not in entry:
        raise SubmitBoundaryV4Error(
            f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: allowed_symbols_missing engine_id={engine_id}"
        )

    allowed_symbols = entry.get("allowed_symbols")
    if allowed_symbols is None:
        return
    if not isinstance(allowed_symbols, list):
        raise SubmitBoundaryV4Error(
            f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: allowed_symbols_not_list_or_null engine_id={engine_id}"
        )

    allowed_norm = [str(x).strip().upper() for x in allowed_symbols if isinstance(x, str) and str(x).strip()]
    if not allowed_norm:
        raise SubmitBoundaryV4Error(
            f"{RC_ENGINE_SYMBOL_POLICY_INVALID}: allowed_symbols_empty_list engine_id={engine_id}"
        )

    if plan_symbol.upper() not in allowed_norm:
        raise SubmitBoundaryV4Error(
            f"{RC_ENGINE_SYMBOL_NOT_ALLOWED}: symbol_not_allowed symbol={plan_symbol} engine_id={engine_id} allowed_symbols={allowed_norm}"
        )


def _read_trade_submit_readiness_c2(truth_root: Path, ib_account: str) -> Tuple[bool, str, str, str, Path]:
    p = (
        truth_root
        / "trade_submit_readiness_c2_v1"
        / "PAPER"
        / str(ib_account).strip()
        / "status.json"
    ).resolve()
    obj = _read_json_file(p)
    if not isinstance(obj, dict):
        raise SubmitBoundaryV4Error(f"{RC_READINESS_C2_NOT_OK}: invalid_readiness_type path={p}")

    schema_id = str(obj.get("schema_id") or "").strip()
    schema_ver = str(obj.get("schema_version") or "").strip()
    if schema_id != "trade_submit_readiness_c2" or schema_ver != "v1":
        raise SubmitBoundaryV4Error(
            f"{RC_READINESS_C2_NOT_OK}: schema_mismatch schema_id={schema_id!r} schema_version={schema_ver!r} path={p}"
        )

    ok = bool(obj.get("ok") is True)
    state = str(obj.get("state") or "").strip().upper()
    env = str(obj.get("environment") or "").strip().upper()
    acct = str(obj.get("ib_account") or "").strip()

    prov = obj.get("provenance")
    prov_truth = ""
    if isinstance(prov, dict):
        prov_truth = str(prov.get("truth_root") or "").strip()

    if prov_truth != str(truth_root):
        raise SubmitBoundaryV4Error(
            f"{RC_READINESS_C2_NONAUTHORITATIVE}: provenance_truth_root_mismatch got={prov_truth!r} expected={str(truth_root)!r} path={p}"
        )

    if env != "PAPER":
        raise SubmitBoundaryV4Error(f"{RC_READINESS_C2_NOT_OK}: environment_not_paper env={env} path={p}")

    if acct != str(ib_account).strip():
        raise SubmitBoundaryV4Error(f"{RC_READINESS_C2_NOT_OK}: ib_account_mismatch readiness={acct!r} submission={ib_account!r} path={p}")

    if not ok or state != "OK":
        raise SubmitBoundaryV4Error(f"{RC_READINESS_C2_NOT_OK}: ok={ok} state={state} path={p}")

    return ok, state, env, acct, p


def _refresh_trade_submit_readiness_artifact_v1(
    *,
    repo_root: Path,
    canonical_truth_root: Path,
    execution_truth_root: Path | None,
    day_utc: str,
    ib_account: str,
    environment: str = "PAPER",
) -> int:
    import ops.tools.run_trade_submit_readiness_c2_v1 as readiness_module

    resolved_repo_root = Path(repo_root).resolve()
    resolved_truth_root = Path(canonical_truth_root).resolve()
    resolved_truth_root.mkdir(parents=True, exist_ok=True)
    original_repo_root = readiness_module.REPO_ROOT
    original_truth_root = readiness_module.TRUTH_ROOT
    original_out_root = readiness_module.OUT_ROOT
    original_resolve_execution_root = readiness_module.resolve_sleeve_execution_root_v1
    original_argv = list(sys.argv)
    try:
        readiness_module.REPO_ROOT = resolved_repo_root
        readiness_module.TRUTH_ROOT = resolved_truth_root
        if execution_truth_root is not None:
            resolved_execution_truth_root = Path(execution_truth_root).resolve()
            resolved_execution_truth_root.mkdir(parents=True, exist_ok=True)
            readiness_module.resolve_sleeve_execution_root_v1 = lambda **kwargs: SimpleNamespace(
                execution_root_path=resolved_execution_truth_root
            )
        sys.argv = [
            "run_trade_submit_readiness_c2_v1.py",
            "--day_utc",
            str(day_utc).strip(),
            "--ib_account",
            str(ib_account).strip(),
            "--environment",
            str(environment or "").strip().upper(),
        ]
        return int(readiness_module.main())
    finally:
        sys.argv = original_argv
        readiness_module.REPO_ROOT = original_repo_root
        readiness_module.TRUTH_ROOT = original_truth_root
        readiness_module.OUT_ROOT = original_out_root
        readiness_module.resolve_sleeve_execution_root_v1 = original_resolve_execution_root


def _materialize_and_require_trade_readiness_decision_v1(
    *,
    repo_root: Path,
    canonical_truth_root: Path,
    day_utc: str,
    environment: str,
    ib_account: str,
    intent_hash: str,
    dry_run: bool,
    execution_truth_root: Path | None = None,
    eval_time_utc: str = "",
    refresh_trade_submit_readiness: bool = True,
) -> Tuple[Path, Dict[str, Any], str]:
    if refresh_trade_submit_readiness:
        refresh_rc = _refresh_trade_submit_readiness_artifact_v1(
            repo_root=repo_root.resolve(),
            canonical_truth_root=canonical_truth_root.resolve(),
            execution_truth_root=execution_truth_root.resolve() if execution_truth_root is not None else None,
            day_utc=day_utc,
            ib_account=ib_account,
            environment=environment,
        )
        if refresh_rc != 0:
            raise SubmitBoundaryV4Error(
                f"{RC_READINESS_C2_NOT_OK}: trade_submit_readiness_refresh_failed rc={int(refresh_rc)}"
            )
    payload = build_trade_readiness_decision_payload_v1(
        repo_root=repo_root.resolve(),
        truth_root=canonical_truth_root.resolve(),
        day_utc=day_utc,
        environment=environment,
        ib_account=ib_account,
        intent_hash=intent_hash,
        execution_build_ready_hint=True,
        broker_submission_expected=False,
        dry_run=dry_run,
        broker_transmit_enabled=str(os.environ.get("C2_ENABLE_BROKER_TRANSMIT") or "").strip().upper() == "YES",
        readiness_view="PRE_SUBMIT",
        execution_truth_root_override=execution_truth_root,
        evaluation_time_utc=eval_time_utc,
    )
    decision_path = write_trade_readiness_presubmit_v1(
        truth_root=canonical_truth_root.resolve(),
        payload=payload,
    ).resolve()
    decision_payload = _read_json_file(decision_path)
    if not isinstance(decision_payload, dict):
        raise SubmitBoundaryV4Error(f"{RC_READINESS_C2_NOT_OK}: trade_readiness_decision_not_object path={decision_path}")
    validate_against_repo_schema_v1(
        decision_payload,
        repo_root.resolve(),
        "governance/04_DATA/SCHEMAS/C2/REPORTS/trade_readiness_decision.v1.schema.json",
    )
    if str(decision_payload.get("environment") or "").strip().upper() != "PAPER":
        raise SubmitBoundaryV4Error(
            f"{RC_READINESS_C2_NOT_OK}: trade_readiness_environment_not_paper path={decision_path}"
        )
    if str(decision_payload.get("day_utc") or "").strip() != str(day_utc).strip():
        raise SubmitBoundaryV4Error(
            f"{RC_READINESS_C2_NOT_OK}: trade_readiness_day_mismatch expected={day_utc} path={decision_path}"
        )
    resolved_intent_hash = str(decision_payload.get("intent_hash") or "").strip()
    if str(intent_hash or "").strip() and resolved_intent_hash != str(intent_hash).strip():
        raise SubmitBoundaryV4Error(
            f"{RC_READINESS_C2_NOT_OK}: trade_readiness_intent_mismatch expected={intent_hash} observed={resolved_intent_hash}"
        )
    decision = str(decision_payload.get("decision") or "").strip().upper()
    submit_allowed = bool(decision_payload.get("submit_allowed") is True)
    canonical_blocker = decision_payload.get("canonical_blocker")
    blocker_text = "" if canonical_blocker is None else str(canonical_blocker).strip()
    if decision != "YES" or not submit_allowed or blocker_text:
        raise SubmitBoundaryV4Error(
            f"{RC_READINESS_C2_NOT_OK}: trade_readiness_decision={decision} submit_allowed={submit_allowed} canonical_blocker={blocker_text or '<none>'}"
        )
    return decision_path, decision_payload, _sha256_file(decision_path)


def _load_identity_set(phasec_out_dir: Path) -> Tuple[str, Dict[str, Any], Dict[str, Any], Dict[str, Any], Optional[Dict[str, Any]], List[str]]:
    p_exec = (phasec_out_dir / "execution_identity_record.v1.json").resolve()
    execution_identity_obj = _read_json_file(p_exec) if p_exec.exists() and p_exec.is_file() else None
    exec_pointer = [str(p_exec)] if execution_identity_obj is not None else []

    p_op = (phasec_out_dir / "order_plan.v1.json").resolve()
    p_ep2 = (phasec_out_dir / "equity_order_plan.v2.json").resolve()
    p_ep1 = (phasec_out_dir / "equity_order_plan.v1.json").resolve()

    if p_op.exists() and p_op.is_file():
        p_map = (phasec_out_dir / "mapping_ledger_record.v1.json").resolve()
        p_bind = (phasec_out_dir / "binding_record.v1.json").resolve()
        plan = _read_json_file(p_op)
        mapping = _read_json_file(p_map)
        binding = _read_json_file(p_bind)
        return ("OPTIONS", plan, mapping, binding, execution_identity_obj, [str(p_op), str(p_map), str(p_bind)] + exec_pointer)

    if p_ep2.exists() and p_ep2.is_file():
        p_map = (phasec_out_dir / "mapping_ledger_record.v2.json").resolve()
        p_bind = (phasec_out_dir / "binding_record.v2.json").resolve()
        plan = _read_json_file(p_ep2)
        mapping = _read_json_file(p_map)
        binding = _read_json_file(p_bind)
        return ("EQUITY", plan, mapping, binding, execution_identity_obj, [str(p_ep2), str(p_map), str(p_bind)] + exec_pointer)

    if p_ep1.exists() and p_ep1.is_file():
        p_map = (phasec_out_dir / "mapping_ledger_record.v2.json").resolve()
        p_bind = (phasec_out_dir / "binding_record.v2.json").resolve()
        plan = _read_json_file(p_ep1)
        mapping = _read_json_file(p_map)
        binding = _read_json_file(p_bind)
        return ("EQUITY", plan, mapping, binding, execution_identity_obj, [str(p_ep1), str(p_map), str(p_bind)] + exec_pointer)

    raise SubmitBoundaryV4Error("PHASEC_OUT_DIR_MISSING_IDENTITY_SET")


def _missing_lineage_fields(payload: Dict[str, Any]) -> bool:
    for k in ("engine_id", "source_intent_id", "intent_sha256"):
        v = payload.get(k)
        if not isinstance(v, str) or not v.strip():
            return True
    return False


def _inject_options_lineage_from_identity_set(*, repo_root: Path, phasec_out_dir: Path, plan_obj: Dict[str, Any], pointers: List[str]) -> Dict[str, Any]:
    # Options identity sets may carry lineage on options_intent/adaptation evidence rather than order_plan.
    if not _missing_lineage_fields(plan_obj):
        return plan_obj

    out = dict(plan_obj)
    p_opt = (phasec_out_dir / "options_intent.v2.json").resolve()
    if p_opt.exists() and p_opt.is_file():
        opt = _read_json_file(p_opt)
        validate_against_repo_schema_v1(opt, repo_root, "constellation_2/schemas/options_intent.v2.schema.json")
        pointers.append(str(p_opt))

        eng = opt.get("engine")
        if isinstance(eng, dict):
            eng_id = str(eng.get("engine_id") or "").strip()
            if eng_id and (not isinstance(out.get("engine_id"), str) or not str(out.get("engine_id")).strip()):
                out["engine_id"] = eng_id

        src_intent = str(opt.get("intent_id") or "").strip()
        if src_intent and (not isinstance(out.get("source_intent_id"), str) or not str(out.get("source_intent_id")).strip()):
            out["source_intent_id"] = src_intent

    p_adapter = (phasec_out_dir / "exposure_to_options_adapter_record.v1.json").resolve()
    if p_adapter.exists() and p_adapter.is_file():
        rec = _read_json_file(p_adapter)
        pointers.append(str(p_adapter))
        ie = rec.get("input_exposure_intent")
        if isinstance(ie, dict):
            src_sha = str(ie.get("sha256") or "").strip()
            if src_sha and (not isinstance(out.get("intent_sha256"), str) or not str(out.get("intent_sha256")).strip()):
                out["intent_sha256"] = src_sha

    if (not isinstance(out.get("intent_sha256"), str) or not str(out.get("intent_sha256")).strip()):
        fallback = str(out.get("intent_hash") or "").strip()
        if fallback:
            out["intent_sha256"] = fallback

    return out


def _write_auth_binding_record(
    *,
    repo_root: Path,
    out_dir: Path,
    eval_time_utc: str,
    submission_id: str,
    intent_hash: str,
    az_path: Path,
    az_sha: str,
) -> None:
    import json
    rec: Dict[str, Any] = {
        "schema_id": "authorization_binding_record",
        "schema_version": "v1",
        "observed_at_utc": eval_time_utc,
        "submission_id": submission_id,
        "intent_hash": intent_hash,
        "authorization_path": str(az_path),
        "authorization_sha256": str(az_sha),
        "notes": [],
        "canonical_json_hash": None,
    }
    rec["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(rec)
    validate_against_repo_schema_v1(rec, repo_root, "constellation_2/schemas/authorization_binding_record.v1.schema.json")
    p = (out_dir / "authorization_binding_record.v1.json").resolve()
    p.write_text(json.dumps(rec, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _mk_veto(
    *,
    eval_time_utc: str,
    reason_code: str,
    reason_detail: str,
    pointers: List[str],
    intent_hash: Optional[str],
    plan_hash: Optional[str],
    upstream_hash: Optional[str],
    repo_root: Path,
) -> Dict[str, Any]:
    veto = {
        "schema_id": "veto_record",
        "schema_version": "v1",
        "observed_at_utc": eval_time_utc,
        "boundary": "SUBMIT",
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "inputs": {"intent_hash": intent_hash, "plan_hash": plan_hash, "chain_snapshot_hash": None, "freshness_cert_hash": None},
        "pointers": list(pointers) if pointers else ["<none>"],
        "canonical_json_hash": None,
        "upstream_hash": upstream_hash,
    }
    veto["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(veto)
    validate_against_repo_schema_v1(veto, repo_root, "constellation_2/schemas/veto_record.v1.schema.json")
    return veto


def run_submit_boundary_paper_v4(
    *,
    repo_root: Path,
    eval_time_utc: str,
    phasec_out_dir: Optional[Path],
    execution_package_path: Optional[Path],
    allow_legacy_raw_candidate: bool,
    risk_budget_path: Path,
    ib_host: str,
    ib_port: int,
    ib_client_id: int,
    ib_account: str,
    dry_run: bool,
    submission_record_path: Optional[Path] = None,
    submissions_root_override: Optional[Path] = None,
    refresh_trade_submit_readiness: bool = True,
) -> int:
    import json

    _parse_utc_z(eval_time_utc)
    eval_day = _day_from_eval_time_utc(eval_time_utc)
    day = eval_day

    package_obj: Optional[Dict[str, Any]] = None
    package_build_path: Optional[Path] = None
    if execution_package_path is not None:
        package_obj, package_build_path = _read_execution_package(repo_root.resolve(), execution_package_path)
        candidate_ref = package_obj.get("candidate_ref") if isinstance(package_obj.get("candidate_ref"), dict) else {}
        package_candidate_text = str(candidate_ref.get("phasec_out_dir") or "").strip()
        if not package_candidate_text:
            raise SubmitBoundaryV4Error(f"EXECUTION_PACKAGE_CANDIDATE_REF_MISSING:path={execution_package_path}")
        package_candidate_path = Path(package_candidate_text).resolve()
        if phasec_out_dir is not None and phasec_out_dir.resolve() != package_candidate_path:
            raise SubmitBoundaryV4Error(
                f"EXECUTION_PACKAGE_CANDIDATE_MISMATCH:package_candidate={package_candidate_path}:phasec_out_dir={phasec_out_dir.resolve()}"
            )
        phasec_out_dir = package_candidate_path
    elif phasec_out_dir is None:
        raise SubmitBoundaryV4Error("EXECUTION_PACKAGE_REQUIRED")
    else:
        raise SubmitBoundaryV4Error(RC_RAW_CANDIDATE_SUBMIT_DISABLED)

    submission_record = _load_and_enforce_submission_record(
        submission_record_path=submission_record_path,
        execution_package_path=execution_package_path,
        package_obj=package_obj,
        phasec_out_dir=phasec_out_dir,
    )
    day = _resolve_submit_day_utc(
        eval_day_utc=eval_day,
        package_obj=package_obj,
        submission_record=submission_record,
    )

    submit_sleeve_id, submit_environment = _resolve_submit_scope_from_phasec_out_dir(phasec_out_dir)
    _require_paper(submit_environment)

    repo_root = repo_root.resolve()
    try:
        execution_identity = enforce_submit_execution_identity_v1(
            repo_root=repo_root,
            environment=submit_environment,
            sleeve_id=submit_sleeve_id,
            runtime_account_id=ib_account,
            runtime_client_id_orders=ib_client_id,
        )
    except ValueError as exc:
        raise SubmitBoundaryV4Error(str(exc)) from exc
    execution_root = _resolve_execution_roots_for_phasec_out_dir(
        repo_root=repo_root,
        phasec_out_dir=phasec_out_dir,
        ib_account=execution_identity.account_id,
        sleeve_id=execution_identity.sleeve_id,
    )
    canonical_control_truth_root = resolve_canonical_truth_root().resolve()

    _require_phasec_out_dir_under_truth_root(execution_root.execution_root_path, phasec_out_dir)

    mode, plan_obj, mapping_obj, binding_obj, execution_identity_obj, pointers = _load_identity_set(phasec_out_dir)
    actual_plan_path = Path(pointers[0]).resolve()
    if submission_record is not None:
        expected_downstream_payload_path = Path(str(submission_record.downstream_payload_ref.get("path") or "")).resolve()
        if actual_plan_path != expected_downstream_payload_path:
            raise SubmitBoundaryV4Error(
                f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:actual_downstream_payload_path"
            )
        expected_downstream_payload_sha = str(submission_record.downstream_payload_ref.get("sha256") or "").strip()
        actual_downstream_payload_sha = _sha256_file(actual_plan_path)
        if expected_downstream_payload_sha != actual_downstream_payload_sha:
            raise SubmitBoundaryV4Error(
                f"{RC_EXECUTION_SUBMISSION_RECORD_MISMATCH}:actual_downstream_payload_sha256"
            )
    if mode == "OPTIONS":
        plan_obj = _inject_options_lineage_from_identity_set(
            repo_root=repo_root,
            phasec_out_dir=phasec_out_dir,
            plan_obj=plan_obj,
            pointers=pointers,
        )
    _enforce_advisory_execution_choke_point(
        package_obj=package_obj,
        execution_identity_obj=execution_identity_obj,
        plan_obj=plan_obj,
    )
    pointers = list(pointers) + [
        str(risk_budget_path.resolve()),
        str(execution_identity.sleeve_registry_path),
        str(execution_identity.account_registry_path),
    ]
    package_dependency_refs: Dict[str, Dict[str, Any]] = {}
    if package_obj is not None and execution_package_path is not None:
        package_dependency_refs = _dependency_refs_by_id(package_obj)
        pointers.append(str(execution_package_path.resolve()))
        if package_build_path is not None:
            pointers.append(str(package_build_path.resolve()))

    intent_hash = str(plan_obj.get("intent_hash") or "").strip()
    if not intent_hash:
        raise SubmitBoundaryV4Error("PHASEC_IDENTITY_SET_MISSING_INTENT_HASH")
    trade_readiness_expected_path = resolve_trade_readiness_presubmit_path(
        truth_root=canonical_control_truth_root,
        day_utc=day,
    ).resolve()

    plan_symbol = _extract_symbol_from_plan(plan_obj)

    binding_hash = canonical_hash_for_c2_artifact_v1(binding_obj)
    resolved_submission_identity = resolve_submission_identity_v1(
        plan_obj=plan_obj,
        binding_obj=binding_obj,
        binding_hash=binding_hash,
        execution_identity_obj=execution_identity_obj,
    )
    submission_id = resolved_submission_identity.submission_id
    if package_obj is not None:
        package_submission_id = str(package_obj.get("submission_id") or "").strip()
        if package_submission_id != submission_id:
            raise SubmitBoundaryV4Error(
                f"EXECUTION_PACKAGE_SUBMISSION_ID_MISMATCH:package_submission_id={package_submission_id}:resolved_submission_id={submission_id}"
            )

    if submissions_root_override is not None:
        day_dir = (submissions_root_override / day).resolve()
    else:
        day_dir = (execution_root.execution_root_path / "execution_evidence_v1" / "submissions" / day).resolve()
        require_governed_execution_family_path(
            repo_root=repo_root,
            environment="PAPER",
            ib_account=execution_identity.account_id,
            sleeve_id=execution_root.sleeve_id,
            family="execution_evidence_v1",
            actual_path=day_dir,
        )
    day_dir.mkdir(parents=True, exist_ok=True)

    try:
        idempotency_check = assert_idempotent_or_raise_v1(submissions_root=day_dir, submission_id=submission_id)
    except IdempotencyError as e:
        # FAIL-CLOSED: submission_id already exists with brokered or ambiguous evidence.
        raise SubmitBoundaryV4Error(f"IDEMPOTENCY_FAILURE: {e!r}")
    if idempotency_check.already_exists and idempotency_check.retry_allowed:
        archived_path = _archive_existing_submission_dir_for_retry(
            day_dir=day_dir,
            submission_id=submission_id,
            eval_time_utc=eval_time_utc,
            classification=idempotency_check.classification,
        )
        pointers.append(
            "IDEMPOTENCY_RETRY_ALLOWED:"
            f"classification={idempotency_check.classification}:"
            f"path={idempotency_check.existing_path}:"
            f"diagnostic={idempotency_check.diagnostic}"
        )
        pointers.append(
            "IDEMPOTENCY_RETRY_ARCHIVED:"
            f"source={idempotency_check.existing_path}:"
            f"archive_path={archived_path}"
        )

    try:
        lineage = assert_required_lineage_fields(plan_obj)
    except LineageViolation as e:
        subdir = (day_dir / submission_id).resolve()
        subdir.mkdir(parents=True, exist_ok=True)
        veto = _mk_veto(
            eval_time_utc=eval_time_utc,
            reason_code=RC_LINEAGE_VIOLATION,
            reason_detail=f"{e}",
            pointers=pointers,
            intent_hash=plan_obj.get("intent_hash"),
            plan_hash=plan_obj.get("plan_hash"),
            upstream_hash=binding_hash,
            repo_root=repo_root,
        )
        write_phased_veto_only_v1(subdir, veto_record=veto, order_plan=plan_obj, binding_record=binding_obj, mapping_ledger_record=mapping_obj)
        return 2

    az_sha = ""
    az_path = Path(".")

    try:
        readiness_decision_path, _readiness_decision_payload, _readiness_decision_sha = _materialize_and_require_trade_readiness_decision_v1(
            repo_root=repo_root,
            canonical_truth_root=canonical_control_truth_root,
            day_utc=day,
            environment=submit_environment,
            ib_account=execution_identity.account_id,
            intent_hash=intent_hash,
            dry_run=dry_run,
            execution_truth_root=execution_root.execution_root_path.resolve(),
            eval_time_utc=eval_time_utc,
            refresh_trade_submit_readiness=refresh_trade_submit_readiness,
        )
        if readiness_decision_path != trade_readiness_expected_path:
            raise SubmitBoundaryV4Error(
                f"{RC_READINESS_C2_NOT_OK}: trade_readiness_decision_noncanonical path={readiness_decision_path} expected={trade_readiness_expected_path}"
            )
        pointers.append(str(readiness_decision_path))

        _enforce_household_portfolio_authorization_v1(
            package_obj=package_obj,
            execution_identity_obj=execution_identity_obj,
            eval_time_utc=eval_time_utc,
            pointers=pointers,
        )
        _require_equity_protective_stop_or_fail(plan_obj=plan_obj, engine_id=str(lineage.engine_id))
        # Authority head / authorization artifacts come from the sealed package when package mode is used.
        if package_obj is not None:
            completeness_rows = [
                {
                    "artifact_id": "execution_build_v1",
                    "path": str(package_build_path.resolve()) if package_build_path is not None else "",
                    "sha256": _sha256_file(package_build_path.resolve()) if package_build_path is not None else "",
                    "required_finality_states": ["provisional", "finalized", "corrected"],
                }
            ]
            for dep_id in ("trade_submit_readiness_c2_v1", "economic_state_package_v1"):
                dep_ref = package_dependency_refs.get(dep_id)
                if not isinstance(dep_ref, dict):
                    raise SubmitBoundaryV4Error(
                        f"CONSTITUTIONAL_COMPLETENESS_DEPENDENCY_REF_MISSING:artifact_id={dep_id}:path={execution_package_path.resolve() if execution_package_path is not None else '<missing>'}"
                    )
                completeness_rows.append(
                    {
                        "artifact_id": dep_id,
                        "path": str(dep_ref.get("path") or "").strip(),
                        "sha256": str(dep_ref.get("sha256") or "").strip(),
                        "required_finality_states": ["provisional", "finalized", "corrected"],
                    }
                )
            try:
                assert_constitutional_completeness_v1(
                    repo_root=repo_root,
                    consumer_id="submit_boundary_paper_v4",
                    required_artifacts=completeness_rows,
                )
            except Exception as exc:
                raise SubmitBoundaryV4Error(
                    f"CONSTITUTIONAL_COMPLETENESS_FAILED:{exc}"
                ) from exc
            global_context_path = _read_global_context_package_from_path(
                _require_package_dependency_ref(package_dependency_refs, "global_context_package_v1"),
                day=day,
                sleeve_id=execution_identity.sleeve_id,
                environment=submit_environment,
                account_id=execution_identity.account_id,
            )
            pointers.append(str(global_context_path))

            capauth_outcome, capauth_qty, capauth_sha, capauth_path = _read_capital_authority_from_path(
                _require_package_dependency_ref(package_dependency_refs, "capital_authority_allocation_v1"),
                intent_hash=str(getattr(lineage, "intent_sha256", "") or intent_hash).strip(),
            )
            pointers.append(str(capauth_path))
            if capauth_outcome not in {"APPROVED", "RESIZED"} or capauth_qty <= 0:
                raise SubmitBoundaryV4Error(
                    f"{RC_AUTHZ_NOT_AUTHORIZED}: capital_authority_outcome={capauth_outcome} authorized_quantity={capauth_qty}"
                )
            az_status = "AUTHORIZED"
            az_decision = "AUTHORIZED"
            az_qty = int(capauth_qty)
            az_sha = capauth_sha
            az_path = capauth_path
            bridge_ref = package_dependency_refs.get("engine_activity_authorization_v1")
            if bridge_ref is not None:
                bridge_status, bridge_decision, bridge_qty, bridge_sha, bridge_path = _read_authorization_from_path(
                    _require_package_dependency_ref(package_dependency_refs, "engine_activity_authorization_v1")
                )
                pointers.append(str(bridge_path))
                if bridge_status != "AUTHORIZED" or bridge_decision != "AUTHORIZED" or bridge_qty != az_qty:
                    raise SubmitBoundaryV4Error(
                        f"{RC_AUTHZ_NOT_AUTHORIZED}: compatibility_bridge_mismatch "
                        f"bridge_status={bridge_status} bridge_decision={bridge_decision} "
                        f"bridge_authorized_quantity={bridge_qty} capital_authority_authorized_quantity={az_qty}"
                    )
        else:
            head_path = _read_authority_head(execution_root.execution_root_path, day)
            pointers.append(str(head_path))

            auth_gate_status, auth_gate_path = _read_authorization_gate_verdict_status(execution_root.execution_root_path, day)
            pointers.append(str(auth_gate_path))
            if auth_gate_status not in ("PASS", "BOOTSTRAP_PASS"):
                raise SubmitBoundaryV4Error(f"{RC_AUTHORIZATION_VERDICT_NOT_PASS}: status={auth_gate_status}")

            az_lookup_hash = str(getattr(lineage, "intent_sha256", "") or intent_hash).strip()
            az_status, az_decision, az_qty, az_sha, az_path = _read_authorization(execution_root.execution_root_path, day, az_lookup_hash)
            pointers.append(str(az_path))
            if az_status != "AUTHORIZED" or az_decision != "AUTHORIZED" or az_qty <= 0:
                raise SubmitBoundaryV4Error(
                    f"{RC_AUTHZ_NOT_AUTHORIZED}: status={az_status} decision={az_decision} authorized_quantity={az_qty}"
                )

        _enforce_ib_account_registry(
            repo_root=repo_root,
            ib_account=execution_identity.account_id,
            engine_id=str(lineage.engine_id),
            pointers=pointers,
        )
        _enforce_engine_symbol_policy(
            repo_root=repo_root,
            engine_id=str(lineage.engine_id),
            plan_symbol=plan_symbol,
            pointers=pointers,
            execution_truth_root=execution_root.execution_root_path.resolve(),
            day_utc=day,
            phasec_out_dir=phasec_out_dir.resolve() if phasec_out_dir is not None else None,
        )

        runtime_control_run_id = canonical_hash_for_c2_artifact_v1(
            {
                "gate": "submit_boundary_paper_v4",
                "submission_id": submission_id,
                "day_utc": day,
                "ib_account": execution_identity.account_id,
                "eval_time_utc": eval_time_utc,
            }
        )
        runtime_control_result = run_runtime_control_kernel_v1(
            canonical_truth_root=canonical_control_truth_root,
            execution_truth_root=execution_root.execution_root_path,
            day_utc=day,
            produced_utc=eval_time_utc,
            run_id=f"submit-boundary:{runtime_control_run_id}",
            environment="PAPER",
            ib_account=execution_identity.account_id,
            sleeve_id=execution_identity.sleeve_id,
        )
        runtime_control_decision = runtime_control_result["runtime_control_decision"]
        runtime_control_record = runtime_control_result["runtime_control_record"]
        runtime_control_decision_path = runtime_control_result["runtime_control_decision_path"]
        runtime_control_record_path = runtime_control_result["runtime_control_record_path"]
        runtime_control_envelope_path = runtime_control_result["runtime_control_run_envelope_path"]
        pointers.append(str(runtime_control_decision_path))
        pointers.append(str(runtime_control_envelope_path))
        if runtime_control_record_path:
            pointers.append(str(runtime_control_record_path))
        if runtime_control_record is None or runtime_control_record.control_state != "ALLOW":
            raise SubmitBoundaryV4Error(
                f"{RC_READINESS_C2_NOT_OK}: runtime_control_outcome={runtime_control_decision.outcome} reason_codes={list(runtime_control_decision.reason_codes)}"
            )

    except Exception as gate_failure:
        subdir = (day_dir / submission_id).resolve()
        subdir.mkdir(parents=True, exist_ok=True)
        reason_code, reason_detail = _extract_reason_code_and_detail(gate_failure, default_code=RC_FAIL_CLOSED)
        if reason_code == RC_FAIL_CLOSED:
            reason_detail = f"AUTHORITY_GATE_FAILURE: {reason_detail}"
        veto = _mk_veto(
            eval_time_utc=eval_time_utc,
            reason_code=reason_code,
            reason_detail=reason_detail,
            pointers=pointers,
            intent_hash=plan_obj.get("intent_hash"),
            plan_hash=plan_obj.get("plan_hash"),
            upstream_hash=binding_hash,
            repo_root=repo_root,
        )
        try:
            write_phased_veto_only_v1(
                subdir,
                veto_record=veto,
                order_plan=plan_obj,
                binding_record=binding_obj,
                mapping_ledger_record=mapping_obj,
                allowed_existing_filenames=SUBMISSION_PREWRITE_FILENAMES,
            )
        except EvidenceWriteError as e:
            # Deterministic re-entry: if the submission dir already has evidence, keep fail-closed outcome (rc=2).
            if "OUT_DIR_NOT_EMPTY" not in str(e):
                raise
        # Write binding record if we have the authorization hash (best-effort)
        if az_sha and str(az_path) != ".":
            _write_auth_binding_record(
                repo_root=repo_root,
                out_dir=subdir,
                eval_time_utc=eval_time_utc,
                submission_id=submission_id,
                intent_hash=intent_hash,
                az_path=az_path,
                az_sha=az_sha,
            )
        return 2

    submission_dir = (day_dir / submission_id).resolve()
    submission_dir.mkdir(parents=True, exist_ok=False)

    if mode == "OPTIONS":
        _payload_obj, _dig = build_binding_digest_for_order_plan_v1(plan_obj)
    else:
        if plan_obj.get("schema_id") == "equity_order_plan" and plan_obj.get("schema_version") == "v2":
            _payload_obj, _dig = build_binding_digest_for_equity_order_plan_v2(plan_obj)
        else:
            _payload_obj, _dig = build_binding_digest_for_equity_order_plan_v1(plan_obj)

    submit_attempt = build_broker_submit_attempt_v1(
        day_utc=day,
        environment=submit_environment,
        submission_id=submission_id,
        attempted_at_utc=eval_time_utc,
        ib_account=execution_identity.account_id,
        dry_run=bool(dry_run),
        reason_codes=["DRY_RUN_SUBMIT_ATTEMPT"] if dry_run else ["REAL_SUBMIT_ATTEMPT"],
        evidence_artifacts=pointers,
    )
    write_broker_submit_attempt_v1(
        repo_root=repo_root,
        submission_dir=submission_dir,
        payload=submit_attempt,
    )

    # DRY RUN: no broker connection; still record submission-only artifact
    if dry_run:
        dry_run_stop = plan_obj.get("protective_stop") if isinstance(plan_obj.get("protective_stop"), dict) else None
        dry_run_protected = isinstance(dry_run_stop, dict)
        bsr: Dict[str, Any] = {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": submission_id,
            "submitted_at_utc": eval_time_utc,
            "binding_hash": binding_hash,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "status": "PENDINGSUBMIT",
            "broker_ids": {"order_id": None, "perm_id": None},
            "protection_status": "PROTECTED" if dry_run_protected else "ENTRY_ONLY",
            "order_linkage": {
                "parent_order_id": None,
                "parent_perm_id": None,
                "stop_order_id": None,
                "stop_perm_id": None,
                "take_profit_order_id": None,
                "take_profit_perm_id": None,
                "bracket_linkage": "PARENT_STOP" if dry_run_protected else "ENTRY_ONLY",
                "oca_group": None,
            },
            "error": {
                "code": "DRY_RUN_NO_BROKER_ID",
                "message": "Governed submit dry-run mode recorded submission without broker order identifiers.",
            },
            "canonical_json_hash": None,
        }
        bsr["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(bsr)
        validate_against_repo_schema_v1(bsr, repo_root, "constellation_2/schemas/broker_submission_record.v2.schema.json")

        write_phased_submission_only_v1(
            submission_dir,
            broker_submission_record=bsr,
            order_plan=plan_obj,
            binding_record=binding_obj,
            mapping_ledger_record=mapping_obj,
            allowed_existing_filenames=SUBMISSION_PREWRITE_FILENAMES,
        )
        dry_run_outcome = build_broker_order_outcome_v1(
            day_utc=day,
            environment=submit_environment,
            submission_id=submission_id,
            evaluated_at_utc=eval_time_utc,
            outcome_state="UNKNOWN_PENDING",
            status=bsr["status"],
            order_id=None,
            perm_id=None,
            ib_account=execution_identity.account_id,
            reason_codes=["DRY_RUN_NO_BROKER_ID", "BROKER_OUTCOME_NOT_RECONCILED"],
            evidence_artifacts=[str(submission_dir / "broker_submission_record.v2.json")],
            error={"code": "DRY_RUN_NO_BROKER_ID", "message": "Dry-run submission has no broker callbacks."},
        )
        write_broker_order_outcome_v1(
            repo_root=repo_root,
            submission_dir=submission_dir,
            payload=dry_run_outcome,
        )
        return 0

    _require_explicit_live_enablement(dry_run=dry_run)

    adapter = IBPaperAdapterV2(conn=BrokerConnectionSpec(host=ib_host, port=ib_port, client_id=ib_client_id), env="PAPER")

    try:
        adapter.connect()

        risk_budget = _read_json_file(risk_budget_path.resolve())

        whatif = adapter.whatif_order(order_plan=plan_obj)

        dec = enforce_risk_budget_against_whatif_v1(
            repo_root=repo_root,
            risk_budget=risk_budget,
            whatif_margin_change_usd=str(whatif.margin_change_usd),
            whatif_notional_usd=str(whatif.notional_usd),
            engine_id=str(lineage.engine_id),
        )
        if not dec.allow:
            veto = _mk_veto(
                eval_time_utc=eval_time_utc,
                reason_code=dec.reason_code or RC_FAIL_CLOSED,
                reason_detail=dec.reason_detail,
                pointers=pointers,
                intent_hash=plan_obj.get("intent_hash"),
                plan_hash=plan_obj.get("plan_hash"),
                upstream_hash=binding_hash,
                repo_root=repo_root,
            )
            write_phased_veto_only_v1(
                submission_dir,
                veto_record=veto,
                order_plan=plan_obj,
                binding_record=binding_obj,
                mapping_ledger_record=mapping_obj,
                allowed_existing_filenames=SUBMISSION_PREWRITE_FILENAMES,
            )
            return 2

        if submission_record is not None and submission_record_path is not None:
            write_execution_attempt_state_record_v1(
                submission_record=submission_record,
                produced_utc=eval_time_utc,
                truth_root=truth_root_from_execution_kernel_artifact_path_v1(submission_record_path.resolve()),
            )

        submit_res = adapter.submit_order(order_plan=plan_obj)
        assert_no_synth_status_in_paper("PAPER", submit_res.status)
        raw_submit = submit_res.raw if isinstance(submit_res.raw, dict) else {}
        raw_broker_ids = raw_submit.get("broker_ids") if isinstance(raw_submit.get("broker_ids"), dict) else {}
        parent_order_id = raw_broker_ids.get("parent_order_id")
        parent_perm_id = raw_broker_ids.get("parent_perm_id")
        stop_order_id = raw_broker_ids.get("stop_order_id")
        stop_perm_id = raw_broker_ids.get("stop_perm_id")
        take_profit_order_id = raw_broker_ids.get("take_profit_order_id")
        take_profit_perm_id = raw_broker_ids.get("take_profit_perm_id")
        if not isinstance(parent_order_id, int):
            parent_order_id = submit_res.order_id if isinstance(submit_res.order_id, int) else None
        if not isinstance(parent_perm_id, int):
            parent_perm_id = submit_res.perm_id if isinstance(submit_res.perm_id, int) else None
        bracket_linkage = "ENTRY_ONLY"
        if isinstance(stop_order_id, int) and isinstance(take_profit_order_id, int):
            bracket_linkage = "PARENT_STOP_TAKE_PROFIT"
        elif isinstance(stop_order_id, int):
            bracket_linkage = "PARENT_STOP"
        protection_status = "PROTECTED" if isinstance(stop_order_id, int) else "ENTRY_ONLY"
        payload_obj = raw_submit.get("payload") if isinstance(raw_submit.get("payload"), dict) else {}
        bracket_payload = payload_obj.get("bracket") if isinstance(payload_obj.get("bracket"), dict) else {}
        oca_group = str(bracket_payload.get("oca_group") or "").strip() or None

        bsr2: Dict[str, Any] = {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": submission_id,
            "submitted_at_utc": eval_time_utc,
            "binding_hash": binding_hash,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "status": submit_res.status,
            "broker_ids": {"order_id": parent_order_id, "perm_id": parent_perm_id},
            "protection_status": protection_status,
            "order_linkage": {
                "parent_order_id": parent_order_id,
                "parent_perm_id": parent_perm_id,
                "stop_order_id": stop_order_id if isinstance(stop_order_id, int) else None,
                "stop_perm_id": stop_perm_id if isinstance(stop_perm_id, int) else None,
                "take_profit_order_id": take_profit_order_id if isinstance(take_profit_order_id, int) else None,
                "take_profit_perm_id": take_profit_perm_id if isinstance(take_profit_perm_id, int) else None,
                "bracket_linkage": bracket_linkage,
                "oca_group": oca_group,
            },
            "error": None,
            "canonical_json_hash": None,
        }
        if not submit_res.ok:
            bsr2["error"] = {
                "code": submit_res.error_code or RC_BRACKET_SUBMISSION_FAILED,
                "message": submit_res.error_message or "Rejected",
            }
        bsr2["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(bsr2)
        validate_against_repo_schema_v1(bsr2, repo_root, "constellation_2/schemas/broker_submission_record.v2.schema.json")

        if parent_order_id is None or parent_perm_id is None:
            write_phased_submission_only_v1(
                submission_dir,
                broker_submission_record=bsr2,
                order_plan=plan_obj,
                binding_record=binding_obj,
                mapping_ledger_record=mapping_obj,
                allowed_existing_filenames=SUBMISSION_PREWRITE_FILENAMES,
            )
            missing_id_outcome = build_broker_order_outcome_v1(
                day_utc=day,
                environment=submit_environment,
                submission_id=submission_id,
                evaluated_at_utc=eval_time_utc,
                outcome_state="UNKNOWN_PENDING",
                status=submit_res.status,
                order_id=parent_order_id if isinstance(parent_order_id, int) else None,
                perm_id=parent_perm_id if isinstance(parent_perm_id, int) else None,
                ib_account=execution_identity.account_id,
                reason_codes=["BROKER_OUTCOME_NOT_RECONCILED", "BROKER_IDENTITY_INCOMPLETE"],
                evidence_artifacts=[str(submission_dir / "broker_submission_record.v2.json")],
                error=bsr2.get("error") if isinstance(bsr2.get("error"), dict) else None,
            )
            write_broker_order_outcome_v1(
                repo_root=repo_root,
                submission_dir=submission_dir,
                payload=missing_id_outcome,
            )
            return 3

        evt: Dict[str, Any] = {
            "schema_id": "execution_event_record",
            "schema_version": "v1",
            "created_at_utc": eval_time_utc,
            "event_time_utc": eval_time_utc,
            "binding_hash": binding_hash,
            "broker_submission_hash": bsr2["canonical_json_hash"],
            "broker_order_id": str(parent_order_id),
            "perm_id": str(parent_perm_id),
            "status": submit_res.status if submit_res.status in (
                "SUBMITTED",
                "ACKNOWLEDGED",
                "REJECTED",
                "CANCELLED",
                "PARTIALLY_FILLED",
                "FILLED",
                "UNKNOWN",
            ) else "UNKNOWN",
            "filled_qty": 0,
            "avg_price": "0",
            "protection_status": protection_status,
            "order_linkage": {
                "parent_order_id": None if parent_order_id is None else str(parent_order_id),
                "stop_order_id": None if not isinstance(stop_order_id, int) else str(stop_order_id),
                "take_profit_order_id": None if not isinstance(take_profit_order_id, int) else str(take_profit_order_id),
                "bracket_linkage": bracket_linkage,
                "oca_group": oca_group,
            },
            "raw_broker_status": None,
            "raw_payload_digest": None,
            "sequence_num": None,
            "canonical_json_hash": None,
            "upstream_hash": None,
        }
        evt["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(evt)
        validate_against_repo_schema_v1(evt, repo_root, "constellation_2/schemas/execution_event_record.v1.schema.json")

        write_phased_success_outputs_v1(
            submission_dir,
            broker_submission_record=bsr2,
            execution_event_record=evt,
            order_plan=plan_obj,
            binding_record=binding_obj,
            mapping_ledger_record=mapping_obj,
            allowed_existing_filenames=SUBMISSION_PREWRITE_FILENAMES,
        )
        broker_ack = build_broker_acknowledgement_v1(
            day_utc=day,
            environment=submit_environment,
            submission_id=submission_id,
            acknowledged_at_utc=eval_time_utc,
            order_id=parent_order_id if isinstance(parent_order_id, int) else None,
            perm_id=parent_perm_id if isinstance(parent_perm_id, int) else None,
            status=submit_res.status,
            ib_account=execution_identity.account_id,
            reason_codes=[
                "BROKER_ID_ASSIGNED_WEAK_PERM_ID"
                if isinstance(parent_perm_id, int) and parent_perm_id <= 0
                else "BROKER_ID_ASSIGNED"
            ],
            evidence_artifacts=[
                str(submission_dir / "broker_submission_record.v2.json"),
                str(submission_dir / "execution_event_record.v1.json"),
            ],
        )
        write_broker_acknowledgement_v1(
            repo_root=repo_root,
            submission_dir=submission_dir,
            payload=broker_ack,
        )
        status_upper = str(submit_res.status or "").strip().upper()
        outcome_state = "UNKNOWN_PENDING"
        outcome_reason_codes = ["BROKER_OUTCOME_NOT_RECONCILED"]
        if status_upper in {"FILLED"}:
            outcome_state = "FILLED"
            outcome_reason_codes = ["OUTCOME_FROM_SUBMIT_STATUS"]
        elif status_upper in {"PARTIALLY_FILLED"}:
            outcome_state = "PARTIALLY_FILLED"
            outcome_reason_codes = ["OUTCOME_FROM_SUBMIT_STATUS"]
        elif status_upper in {"REJECTED"}:
            outcome_state = "BROKER_REJECTED"
            outcome_reason_codes = ["BROKER_REJECTED_FROM_SUBMIT_STATUS"]
        elif status_upper in {"CANCELLED"}:
            outcome_state = "BROKER_CANCELLED"
            outcome_reason_codes = ["BROKER_CANCELLED_FROM_SUBMIT_STATUS"]
        elif status_upper in {"SUBMITTED", "PRESUBMITTED"} and bsr2.get("error") is None:
            outcome_state = "BROKER_ACCEPTED"
            outcome_reason_codes = ["BROKER_ACCEPTED_FROM_SUBMIT_STATUS"]
        submit_outcome = build_broker_order_outcome_v1(
            day_utc=day,
            environment=submit_environment,
            submission_id=submission_id,
            evaluated_at_utc=eval_time_utc,
            outcome_state=outcome_state,
            status=status_upper or None,
            order_id=parent_order_id if isinstance(parent_order_id, int) else None,
            perm_id=parent_perm_id if isinstance(parent_perm_id, int) else None,
            ib_account=execution_identity.account_id,
            reason_codes=outcome_reason_codes,
            evidence_artifacts=[
                str(submission_dir / "broker_submission_record.v2.json"),
                str(submission_dir / "execution_event_record.v1.json"),
            ],
            error=bsr2.get("error") if isinstance(bsr2.get("error"), dict) else None,
        )
        write_broker_order_outcome_v1(
            repo_root=repo_root,
            submission_dir=submission_dir,
            payload=submit_outcome,
        )
        return 0

    except Exception as e:
        reason_code, reason_detail = _extract_reason_code_and_detail(e, default_code=RC_FAIL_CLOSED)
        if reason_code == RC_FAIL_CLOSED:
            reason_detail = f"SUBMIT_FAILURE: {reason_detail}"
        veto = _mk_veto(
            eval_time_utc=eval_time_utc,
            reason_code=reason_code,
            reason_detail=reason_detail,
            pointers=pointers,
            intent_hash=plan_obj.get("intent_hash"),
            plan_hash=plan_obj.get("plan_hash"),
            upstream_hash=binding_hash,
            repo_root=repo_root,
        )
        write_phased_veto_only_v1(
            submission_dir,
            veto_record=veto,
            order_plan=plan_obj,
            binding_record=binding_obj,
            mapping_ledger_record=mapping_obj,
            allowed_existing_filenames=SUBMISSION_PREWRITE_FILENAMES,
        )
        return 2
    finally:
        try:
            adapter.disconnect()
        except Exception:
            pass
