#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping

from constellation_2.common.control_plane_read_gateway_v1 import (
    ControlPlaneReadRefV1,
    read_control_plane_surface_v1,
)
from constellation_2.common.paper_session_authority_v1 import (
    SCHEMA_RELPATH_V1 as PAPER_SESSION_AUTHORITY_SCHEMA_RELPATH,
    read_paper_session_authority_ref_v1,
)
from constellation_2.common.runtime_path_authority_v1 import classify_runtime_path_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

ROLE_REQUIRED_BINDING_INPUT = "REQUIRED_BINDING_INPUT"
ROLE_REQUIRED_DERIVED_GATE = "REQUIRED_DERIVED_GATE"
ROLE_REQUIRED_EXECUTION_BOUNDARY = "REQUIRED_EXECUTION_BOUNDARY"

CLOSURE_STATUS_CLOSED = "CLOSED"
CANONICAL_PATH_PREFIX = "CANONICAL_RUNTIME_TRUTH"

REASON_WRONG_AUTHORITY_PATH = "WRONG_AUTHORITY_PATH"
REASON_SCHEMA_INVALID = "SCHEMA_INVALID"
REASON_STALE_ARTIFACT = "STALE_ARTIFACT"
REASON_TARGET_DAY_DATE_MISMATCH = "TARGET_DAY_DATE_MISMATCH"
REASON_TARGET_DAY_ARTIFACT_MISSING = "TARGET_DAY_ARTIFACT_MISSING"
REASON_PROVENANCE_MISSING = "PROVENANCE_MISSING"
REASON_REQUIRED_GATE_FAIL = "REQUIRED_GATE_FAIL"
REPO_ROOT = Path(__file__).resolve().parents[2]
TRADING_DAY_READINESS_AUTHORITY_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_readiness_authority.v1.schema.json"
)
SAFETY_STATE_AUTHORITY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/safety_state_authority.v1.schema.json"


def _producer_command(script_relpath: str, *, day_flag: str, target_day: str, truth_root: Path) -> str:
    return f'PYTHONPATH="$PWD" python3 {script_relpath} {day_flag} {target_day} --truth_root {truth_root}'


def _path_family(path: Path) -> str:
    return str(classify_runtime_path_v1(path).get("path_class") or "UNKNOWN")


def _path_family_is_canonical(path_family: str) -> bool:
    return str(path_family).strip().upper().startswith(CANONICAL_PATH_PREFIX)


def _extract_payload_day(payload: Mapping[str, Any]) -> str:
    for field in ("target_day", "target_day_utc", "day_utc", "trading_day", "session_date"):
        value = str(payload.get(field) or "").strip()
        if value:
            return value
    return ""


def _extract_timestamp(payload: Mapping[str, Any]) -> str:
    for field in (
        "generated_at_utc",
        "generated_utc",
        "produced_at_utc",
        "produced_utc",
        "built_at_utc",
        "evaluated_at_utc",
        "emitted_at",
        "as_of_utc",
        "created_utc",
        "ingested_utc",
        "updated_utc",
    ):
        value = str(payload.get(field) or "").strip()
        if value:
            return value
    producer = payload.get("producer")
    if isinstance(producer, dict):
        for field in ("generated_at_utc", "generated_utc", "produced_at_utc", "produced_utc"):
            value = str(producer.get(field) or "").strip()
            if value:
                return value
    return ""


def _provenance_summary(*, payload: Mapping[str, Any], required: bool) -> Dict[str, Any]:
    fields_present: List[str] = []
    source = ""

    if isinstance(payload.get("producer"), dict):
        producer = payload["producer"]
        if str(producer.get("module") or "").strip():
            fields_present.append("producer.module")
        if str(producer.get("git_sha") or "").strip():
            fields_present.append("producer.git_sha")
        if fields_present:
            source = source or "producer"
    if isinstance(payload.get("run_metadata"), dict):
        metadata = payload["run_metadata"]
        if str(metadata.get("producer_module") or "").strip():
            fields_present.append("run_metadata.producer_module")
        if str(metadata.get("producer_git_sha") or "").strip():
            fields_present.append("run_metadata.producer_git_sha")
        if fields_present:
            source = source or "run_metadata"
    if isinstance(payload.get("provenance"), dict):
        provenance = payload["provenance"]
        for field in ("truth_root", "registry_sha256", "sleeve_registry_sha256"):
            if str(provenance.get(field) or "").strip():
                fields_present.append(f"provenance.{field}")
        if fields_present:
            source = source or "provenance"
    for field in (
        "generated_at_utc",
        "generated_utc",
        "produced_at_utc",
        "produced_utc",
        "evaluated_at_utc",
        "emitted_at",
        "as_of_utc",
    ):
        if str(payload.get(field) or "").strip():
            fields_present.append(field)
    if isinstance(payload.get("input_manifest"), list) and payload.get("input_manifest"):
        fields_present.append("input_manifest")
    if isinstance(payload.get("source_dependencies"), list) and payload.get("source_dependencies"):
        fields_present.append("source_dependencies")
    if isinstance(payload.get("source_artifacts"), list) and payload.get("source_artifacts"):
        fields_present.append("source_artifacts")

    return {
        "required": bool(required),
        "present": bool(fields_present),
        "fields_present": sorted(set(fields_present)),
        "source": source,
    }


def _freshness_status(
    *,
    payload: Mapping[str, Any],
    target_day: str,
    observed_day: str,
    freshness_rule: str,
) -> str:
    rule = str(freshness_rule or "").strip().upper()
    timestamp = _extract_timestamp(payload)
    payload_freshness = str(payload.get("freshness_verdict") or "").strip().upper()
    if rule == "TARGET_DAY_MATCH_ONLY":
        return "CURRENT" if observed_day == target_day else "STALE"
    if rule == "DECLARED_CURRENT":
        if payload_freshness == "CURRENT" and observed_day == target_day:
            return "CURRENT"
        return "STALE"
    if rule == "TARGET_DAY_COVERAGE_AND_SOURCE_TIMESTAMP":
        return "CURRENT" if observed_day == target_day and bool(timestamp) else "STALE"
    return "CURRENT" if observed_day == target_day and bool(timestamp) else "STALE"


def _normalize_scoped_dependency_type(dependency_type: str) -> str:
    dep = str(dependency_type or "").strip()
    if dep.startswith("canonical_authority_head_v1_scoped:"):
        return "primary_scoped_canonical_authority_head_v1"
    if dep == "authorization_gate_verdict_v1":
        return "primary_scoped_authorization_gate_verdict_v1"
    if dep.startswith("authorization_gate_verdict_v1_scoped:"):
        return "primary_scoped_authorization_gate_verdict_v1"
    if dep.startswith("gate_stack_verdict_v1_scoped:"):
        return "primary_scoped_gate_stack_verdict_v1"
    return dep


def _payload_dependencies(artifact_id: str, payload: Mapping[str, Any]) -> List[str]:
    deps: set[str] = set()
    if artifact_id == "submit_boundary_status_v1":
        for row in (payload.get("required_boundary_checks") or []):
            if not isinstance(row, dict):
                continue
            logical_name = str(row.get("logical_name") or "").strip()
            if logical_name in {"target_day_build_v1", "target_day_admission_v1", "runtime_control_record_v1"}:
                continue
            if logical_name:
                deps.add(logical_name)
    elif artifact_id == "paper_session_ledger_v1":
        for row in (payload.get("fact_refs") or []):
            if not isinstance(row, dict):
                continue
            if bool(row.get("required_for_authority") is not True):
                continue
            logical_name = str(row.get("logical_name") or "").strip()
            if logical_name:
                deps.add(logical_name)
    elif artifact_id == "trade_submit_readiness_c2_v1":
        for row in (payload.get("input_manifest") or []):
            if not isinstance(row, dict):
                continue
            normalized = _normalize_scoped_dependency_type(str(row.get("type") or "").strip())
            if normalized in {
                "ib_api_handshake_latest_pointer_v1",
                "ib_api_handshake_v1",
                "day_authority_decision_v1",
                "primary_scoped_canonical_authority_head_v1",
                "primary_scoped_authorization_gate_verdict_v1",
            }:
                deps.add(normalized)
        session_attestation = payload.get("session_authority_attestation")
        if isinstance(session_attestation, dict) and str(session_attestation.get("decision_artifact_path") or "").strip():
            deps.add("day_authority_decision_v1")
        run_state = payload.get("run_state_authority_attestation")
        if isinstance(run_state, dict):
            deps.add(_normalize_scoped_dependency_type(str(run_state.get("cycle_snapshot_family") or "").strip()))
            for row in (run_state.get("upstream_authority_refs") or []):
                if isinstance(row, dict):
                    deps.add(_normalize_scoped_dependency_type(str(row.get("artifact_family") or "").strip()))
    elif artifact_id == "trading_day_state_machine_v1":
        for row in (payload.get("supporting_regeneration_results") or []):
            if isinstance(row, dict) and str(row.get("logical_name") or "").strip():
                deps.add(str(row.get("logical_name") or "").strip())
        upstream = payload.get("upstream_intent_status")
        if isinstance(upstream, dict) and str(upstream.get("intents_day_completeness_ref") or "").strip():
            deps.add("intents_day_completeness_v1")
        support = payload.get("supporting_session_authority")
        if isinstance(support, dict) and str(support.get("paper_session_ledger_path") or "").strip():
            deps.add("paper_session_ledger_v1")
        startup_proof = payload.get("startup_proof_result")
        if isinstance(startup_proof, dict) and str(startup_proof.get("startup_proof_validation_path") or "").strip():
            deps.add("startup_proof_validation_v1")
        control_refs = payload.get("supporting_daily_control_refs")
        if isinstance(control_refs, dict):
            if str(control_refs.get("paper_day_control_plane_path") or "").strip():
                deps.add("paper_day_control_plane_v1")
            if str(control_refs.get("trading_day_control_plane_path") or "").strip():
                deps.add("trading_day_control_plane_v1")
            if str(control_refs.get("trading_day_execution_control_plane_path") or "").strip():
                deps.add("trading_day_execution_control_plane_v1")
    return sorted(dep for dep in deps if dep)


def _blocking_reason_code(
    *,
    explicit_reason_code: str,
    path_family: str,
    schema_status: str,
    date_binding_status: str,
    freshness_status: str,
    provenance_summary: Mapping[str, Any],
    result_status: str,
) -> str:
    if explicit_reason_code:
        return explicit_reason_code
    if not _path_family_is_canonical(path_family):
        return REASON_WRONG_AUTHORITY_PATH
    if str(schema_status).strip().upper() not in {"VALID", "VALIDATED_BY_LOADER", "PRACTICAL_VALID"}:
        return REASON_SCHEMA_INVALID
    if str(date_binding_status).strip().upper() == "MISMATCH":
        return REASON_TARGET_DAY_DATE_MISMATCH
    if str(freshness_status).strip().upper() != "CURRENT":
        return REASON_STALE_ARTIFACT
    if bool(provenance_summary.get("required", False)) and not bool(provenance_summary.get("present", False)):
        return REASON_PROVENANCE_MISSING
    if str(result_status).strip().upper() != "PASS":
        return REASON_REQUIRED_GATE_FAIL
    return ""


def _closure_status_for_row(
    *,
    path_family: str,
    schema_status: str,
    date_binding_status: str,
    freshness_status: str,
    provenance_summary: Mapping[str, Any],
    result_status: str,
) -> str:
    if not _path_family_is_canonical(path_family):
        return "OPEN"
    if str(schema_status).strip().upper() not in {"VALID", "VALIDATED_BY_LOADER", "PRACTICAL_VALID"}:
        return "OPEN"
    if str(date_binding_status).strip().upper() != "MATCH":
        return "OPEN"
    if str(freshness_status).strip().upper() != "CURRENT":
        return "OPEN"
    if bool(provenance_summary.get("required", False)) and not bool(provenance_summary.get("present", False)):
        return "OPEN"
    if str(result_status).strip().upper() != "PASS":
        return "OPEN"
    return CLOSURE_STATUS_CLOSED


def _result_row(
    *,
    artifact_id: str,
    required: bool,
    role_class: str,
    classification: str,
    authority_path: Path,
    observed_status: str,
    result_status: str,
    blocker_codes: List[str],
    schema_status: str,
    schema_ref: str,
    freshness_rule: str,
    freshness_status: str,
    target_day_expected: str,
    target_day_observed: str,
    date_binding_status: str,
    provenance_required: bool,
    provenance_summary: Dict[str, Any],
    producer: Dict[str, str] | None = None,
    source_refs: List[Dict[str, Any]] | None = None,
    observed_dependency_artifacts: List[str] | None = None,
    explicit_reason_code: str = "",
) -> Dict[str, Any]:
    path_family = _path_family(authority_path)
    blocking_reason_code = _blocking_reason_code(
        explicit_reason_code=explicit_reason_code,
        path_family=path_family,
        schema_status=schema_status,
        date_binding_status=date_binding_status,
        freshness_status=freshness_status,
        provenance_summary=provenance_summary,
        result_status=result_status,
    )
    return {
        "artifact_id": artifact_id,
        "artifact_name": artifact_id,
        "required": bool(required),
        "role_class": role_class,
        "classification": classification,
        "canonical_path": str(authority_path),
        "authority_path": str(authority_path),
        "path_family": path_family,
        "observed_status": observed_status,
        "result_status": result_status,
        "blocker_codes": sorted({str(code).strip() for code in blocker_codes if str(code).strip()}),
        "blocking_reason_code": blocking_reason_code,
        "schema_status": schema_status,
        "schema_ref": schema_ref,
        "freshness_rule": freshness_rule,
        "freshness_status": freshness_status,
        "target_day_expected": target_day_expected,
        "target_day_observed": target_day_observed,
        "date_binding_status": date_binding_status,
        "date_binding_value": target_day_observed,
        "provenance_required": bool(provenance_required),
        "provenance_summary": provenance_summary,
        "closure_status": _closure_status_for_row(
            path_family=path_family,
            schema_status=schema_status,
            date_binding_status=date_binding_status,
            freshness_status=freshness_status,
            provenance_summary=provenance_summary,
            result_status=result_status,
        ),
        "producer": producer or {"module": "", "git_sha": ""},
        "source_refs": list(source_refs or []),
        "observed_dependency_artifacts": sorted(
            {str(item).strip() for item in (observed_dependency_artifacts or []) if str(item).strip()}
        ),
    }


def _supporting_artifact_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(
        payload.get("final_start_decision")
        or payload.get("final_status")
        or payload.get("completeness_status")
        or payload.get("status")
        or ""
    ).strip().upper()
    if not status:
        return "UNKNOWN", False, []
    return status, "DEFECT" not in status, [str(code).strip() for code in (payload.get("blocking_codes") or []) if str(code).strip()]


def _fact_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(payload.get("status") or "").strip().upper()
    return status or "UNKNOWN", status == "PASS", [str(code).strip() for code in (payload.get("blocking_codes") or []) if str(code).strip()]


def _startup_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(payload.get("status") or "").strip().upper()
    return status, status == "SUCCESS", list(payload.get("blocking_codes") or [])


def _posture_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(payload.get("posture_status") or "").strip().upper()
    ready = status == "ENABLED" and bool(payload.get("system_ready") is True)
    return status, ready, list(payload.get("blocking_codes") or [])


def _overall_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(payload.get("overall_status") or "").strip().upper()
    return status, status == "PASS", list(payload.get("blocking_items") or [])


def _capability_artifact_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    required_capabilities = {
        "account_binding_valid",
        "startup_materialization_ready",
        "paper_trading_posture_ready",
        "broker_connectivity_available",
        "startup_authorization_gate_set_ready",
    }
    capability_map = {
        str(row.get("capability_id") or "").strip(): row
        for row in (payload.get("capabilities") or [])
        if isinstance(row, dict)
    }
    missing = sorted(capability for capability in required_capabilities if capability not in capability_map)
    if missing:
        return "INVALID", False, [f"CAPABILITY_STATE_MISSING:{capability}" for capability in missing]
    return str(payload.get("overall_status") or "PASS").strip().upper() or "PASS", True, []


def _submit_readiness_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    state = str(payload.get("state") or "").strip().upper()
    ok = bool(payload.get("ok") is True) and state == "OK"
    return state or "UNKNOWN", ok, list(payload.get("reasons") or [])


def _submit_boundary_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(payload.get("boundary_status") or "").strip().upper()
    ok = status == "AUTHORIZED" and bool(payload.get("submission_authorized") is True)
    return status, ok, list(payload.get("blocking_codes") or [])


def _submit_boundary_check_row(payload: Mapping[str, Any], *, logical_name: str) -> Dict[str, Any] | None:
    for row in (payload.get("required_boundary_checks") or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("logical_name") or "").strip() == logical_name:
            return row
    return None


def _engine_activity_authorization_row_from_submit_boundary(
    *,
    truth_root: Path,
    target_day: str,
    submit_boundary_ref: ControlPlaneReadRefV1 | None,
) -> Dict[str, Any]:
    artifact_id = "engine_activity_authorization_v1"
    fallback_path = (truth_root / "engine_activity_v1" / "authorization_v1" / target_day).resolve()

    if submit_boundary_ref is None:
        return _error_row(
            artifact_id=artifact_id,
            required=False,
            role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
            classification="SUBMIT_BOUNDARY_DEPENDENCY",
            target_day=target_day,
            freshness_rule="TARGET_DAY_MATCH_ONLY",
            provenance_required=False,
            schema_ref="governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/authorization.v1.schema.json",
            fallback_root=fallback_path,
            exc=ValueError(f"REQUIRED:missing_submit_boundary_status_v1:path={fallback_path}"),
            explicit_reason_code=REASON_TARGET_DAY_ARTIFACT_MISSING,
        )

    check_row = _submit_boundary_check_row(submit_boundary_ref.payload, logical_name=artifact_id)
    if check_row is None:
        return _result_row(
            artifact_id=artifact_id,
            required=False,
            role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
            classification="SUBMIT_BOUNDARY_DEPENDENCY",
            authority_path=fallback_path,
            observed_status="MISSING",
            result_status="FAIL",
            blocker_codes=["ENGINE_ACTIVITY_AUTHORIZATION_CHECK_MISSING"],
            schema_status="UNKNOWN",
            schema_ref="governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/authorization.v1.schema.json",
            freshness_rule="TARGET_DAY_MATCH_ONLY",
            freshness_status="STALE",
            target_day_expected=target_day,
            target_day_observed="",
            date_binding_status="MISSING",
            provenance_required=False,
            provenance_summary={"required": False, "present": True, "fields_present": ["submit_boundary_status_v1"], "source": "submit_boundary_status_v1"},
            source_refs=[{"artifact_path": str(submit_boundary_ref.path), "artifact_sha256": submit_boundary_ref.sha256}],
            explicit_reason_code=REASON_TARGET_DAY_ARTIFACT_MISSING,
        )

    raw_path = str(check_row.get("absolute_path") or check_row.get("path") or "").strip()
    authority_path = Path(raw_path).resolve() if raw_path else fallback_path
    observed_day = str(check_row.get("day_utc") or "").strip()
    date_binding_status = "MATCH" if observed_day == target_day else ("MISSING" if not observed_day else "MISMATCH")
    check_status = str(check_row.get("status") or "").strip().upper()
    is_pass = check_status == "PASS"
    result_status = "PASS" if is_pass and date_binding_status == "MATCH" else "FAIL"
    blocker_codes = [str(code).strip() for code in (check_row.get("reason_codes") or []) if str(code).strip()]
    if not blocker_codes and result_status != "PASS":
        blocker_codes.append("ENGINE_ACTIVITY_AUTHORIZATION_NOT_ALLOWED")
    if date_binding_status != "MATCH":
        blocker_codes.append(f"{artifact_id.upper()}_DAY_MISMATCH")
    source_refs = [{"artifact_path": str(submit_boundary_ref.path), "artifact_sha256": submit_boundary_ref.sha256}]
    row_sha = str(check_row.get("sha256") or "").strip()
    if raw_path:
        source_refs.append({"artifact_path": str(authority_path), "artifact_sha256": row_sha})
    return _result_row(
        artifact_id=artifact_id,
        required=False,
        role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
        classification="SUBMIT_BOUNDARY_DEPENDENCY",
        authority_path=authority_path,
        observed_status=check_status or "UNKNOWN",
        result_status=result_status,
        blocker_codes=blocker_codes,
        schema_status="PRACTICAL_VALID",
        schema_ref="governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/authorization.v1.schema.json",
        freshness_rule="TARGET_DAY_MATCH_ONLY",
        freshness_status="CURRENT" if date_binding_status == "MATCH" else "STALE",
        target_day_expected=target_day,
        target_day_observed=observed_day,
        date_binding_status=date_binding_status,
        provenance_required=False,
        provenance_summary={"required": False, "present": True, "fields_present": ["submit_boundary_status_v1"], "source": "submit_boundary_status_v1"},
        producer={
            "module": str((submit_boundary_ref.payload.get("producer") or {}).get("module") or "").strip(),
            "git_sha": str((submit_boundary_ref.payload.get("producer") or {}).get("git_sha") or "").strip(),
        },
        source_refs=source_refs,
        explicit_reason_code="" if result_status == "PASS" else REASON_REQUIRED_GATE_FAIL,
    )


def _ledger_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    control_state = payload.get("control_state")
    authority_status = ""
    if isinstance(control_state, dict):
        authority_status = str(control_state.get("authority_status") or "").strip().upper()
    authority_status = authority_status or str(payload.get("authority_status") or "").strip().upper()
    return authority_status or "UNKNOWN", authority_status == "GRANTED", list(payload.get("blocking_codes") or [])


def _startup_proof_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(payload.get("status") or "").strip().upper()
    return status, status == "STARTUP_READY", list(payload.get("blocking_codes") or [])


def _state_machine_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(payload.get("final_start_decision") or "").strip().upper()
    codes: List[str] = []
    if str(payload.get("first_true_blocker", {}).get("first_true_blocker_code") or "").strip():
        codes.append(str(payload["first_true_blocker"]["first_true_blocker_code"]).strip())
    codes.extend([str(code).strip() for code in (payload.get("blocking_codes") or []) if str(code).strip()])
    return status, status == "READY_NOW", codes


def _day_authority_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    state = str(payload.get("decision_state") or "").strip().upper()
    codes = [str(code).strip() for code in (payload.get("blocking_evidence") or []) if str(code).strip()]
    return state or "UNKNOWN", state == "OPEN", codes


def _authorization_verdict_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(payload.get("status") or "").strip().upper()
    reason_codes = [str(code).strip() for code in (payload.get("reason_codes") or []) if str(code).strip()]
    return status or "UNKNOWN", status in {"PASS", "BOOTSTRAP_PASS"}, reason_codes


def _authorization_convergence_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(payload.get("convergence_status") or "").strip().upper()
    codes = [str(row.get("blocker_code") or "").strip() for row in (payload.get("blocker_chain") or []) if isinstance(row, dict)]
    ready = status == "SUCCESS" and bool(payload.get("authorization_verdict_ready") is True)
    return status or "UNKNOWN", ready, codes


def _generic_convergence_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(payload.get("convergence_status") or "").strip().upper()
    codes = [str(row.get("blocker_code") or "").strip() for row in (payload.get("blocker_chain") or []) if isinstance(row, dict)]
    return status or "UNKNOWN", status == "SUCCESS", codes


def _economic_build_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    status = str(payload.get("closure_status") or "").strip().upper()
    blocker_codes: List[str] = []
    first_blocker = payload.get("first_real_blocker")
    if isinstance(first_blocker, dict):
        blocker_id = str(first_blocker.get("dependency_id") or first_blocker.get("reason_code") or "").strip()
        if blocker_id:
            blocker_codes.append(f"ECONOMIC_STATE_BUILD_BLOCKED:{blocker_id}")
    return status or "UNKNOWN", status == "COMPLETE", blocker_codes


def _economic_package_status(payload: Dict[str, Any]) -> tuple[str, bool, List[str]]:
    sealed = payload.get("sealed") is True
    return ("SEALED" if sealed else "UNSEALED"), sealed, ([] if sealed else ["ECONOMIC_STATE_PACKAGE_NOT_SEALED"])


def _prior_day_utc(day_utc: str) -> str:
    return (date.fromisoformat(str(day_utc).strip()) - timedelta(days=1)).isoformat()


def _error_path(exc: Exception, fallback_root: Path) -> Path:
    match = re.search(r"path=([^:]+)", str(exc))
    if match:
        return Path(match.group(1)).expanduser().resolve()
    return fallback_root.resolve()


def _error_row(
    *,
    artifact_id: str,
    required: bool,
    role_class: str,
    classification: str,
    target_day: str,
    freshness_rule: str,
    provenance_required: bool,
    schema_ref: str,
    fallback_root: Path,
    exc: Exception,
    explicit_reason_code: str,
) -> Dict[str, Any]:
    error_text = str(exc).upper()
    authority_path = _error_path(exc, fallback_root)
    missing = "MISSING" in error_text or "REQUIRED" in error_text
    invalid = any(token in error_text for token in ("INVALID", "UNREADABLE", "SHA_MISMATCH", "FAILED"))
    observed_status = "MISSING" if missing else ("INVALID" if invalid else "UNAVAILABLE")
    schema_status = "MISSING" if missing else ("INVALID" if invalid else "UNKNOWN")
    blocker_codes = [f"{artifact_id.upper()}_{observed_status}"]
    return _result_row(
        artifact_id=artifact_id,
        required=required,
        role_class=role_class,
        classification=classification,
        authority_path=authority_path,
        observed_status=observed_status,
        result_status="FAIL",
        blocker_codes=blocker_codes,
        schema_status=schema_status,
        schema_ref=schema_ref,
        freshness_rule=freshness_rule,
        freshness_status="STALE",
        target_day_expected=target_day,
        target_day_observed="",
        date_binding_status="MISSING" if missing else "UNKNOWN",
        provenance_required=provenance_required,
        provenance_summary={"required": provenance_required, "present": False, "fields_present": [], "source": ""},
        explicit_reason_code=explicit_reason_code,
    )


def _row_from_ref(
    *,
    artifact_id: str,
    required: bool,
    role_class: str,
    classification: str,
    target_day: str,
    freshness_rule: str,
    provenance_required: bool,
    observed_status_fn: Callable[[Dict[str, Any]], tuple[str, bool, List[str]]],
    ref: ControlPlaneReadRefV1,
    explicit_reason_code: str = "",
    observed_dependency_artifacts: List[str] | None = None,
) -> Dict[str, Any]:
    payload_day = _extract_payload_day(ref.payload)
    date_binding_status = "MATCH" if payload_day == target_day else "MISMATCH"
    observed_status, is_pass, blocker_codes = observed_status_fn(ref.payload)
    result_status = "PASS" if is_pass and date_binding_status == "MATCH" else "FAIL"
    if date_binding_status != "MATCH":
        blocker_codes = list(blocker_codes) + [f"{artifact_id.upper()}_DAY_MISMATCH"]
    provenance_summary = _provenance_summary(payload=ref.payload, required=provenance_required)
    freshness_status = _freshness_status(
        payload=ref.payload,
        target_day=target_day,
        observed_day=payload_day,
        freshness_rule=freshness_rule,
    )
    return _result_row(
        artifact_id=artifact_id,
        required=required,
        role_class=role_class,
        classification=classification,
        authority_path=ref.path,
        observed_status=observed_status,
        result_status=result_status,
        blocker_codes=blocker_codes,
        schema_status="VALID",
        schema_ref=str(ref.schema_relpath or ""),
        freshness_rule=freshness_rule,
        freshness_status=freshness_status,
        target_day_expected=target_day,
        target_day_observed=payload_day,
        date_binding_status=date_binding_status,
        provenance_required=provenance_required,
        provenance_summary=provenance_summary,
        producer={
            "module": str((ref.payload.get("producer") or {}).get("module") or (ref.payload.get("run_metadata") or {}).get("producer_module") or "").strip(),
            "git_sha": str((ref.payload.get("producer") or {}).get("git_sha") or (ref.payload.get("run_metadata") or {}).get("producer_git_sha") or "").strip(),
        },
        source_refs=[{"artifact_path": str(ref.path), "artifact_sha256": ref.sha256}],
        observed_dependency_artifacts=observed_dependency_artifacts or _payload_dependencies(artifact_id, ref.payload),
        explicit_reason_code=explicit_reason_code,
    )


def _surface_row_and_ref(
    *,
    artifact_id: str,
    domain: str,
    surface: str,
    truth_root: Path,
    target_day: str,
    required: bool,
    role_class: str,
    classification: str,
    freshness_rule: str,
    provenance_required: bool,
    observed_status_fn: Callable[[Dict[str, Any]], tuple[str, bool, List[str]]],
    environment: str = "PAPER",
    ib_account: str = "",
    explicit_reason_code: str = "",
    observed_dependency_artifacts: List[str] | None = None,
    day_utc: str | None = None,
    context_hash: str = "",
) -> tuple[Dict[str, Any], ControlPlaneReadRefV1 | None]:
    try:
        ref = read_control_plane_surface_v1(
            domain=domain,
            surface=surface,
            truth_root=truth_root,
            day_utc=day_utc or target_day,
            environment=environment,
            ib_account=ib_account,
            context_hash=context_hash,
        )
    except Exception as exc:
        return (
            _error_row(
                artifact_id=artifact_id,
                required=required,
                role_class=role_class,
                classification=classification,
                target_day=day_utc or target_day,
                freshness_rule=freshness_rule,
                provenance_required=provenance_required,
                schema_ref="",
                fallback_root=truth_root,
                exc=exc,
                explicit_reason_code=explicit_reason_code or REASON_TARGET_DAY_ARTIFACT_MISSING,
            ),
            None,
        )
    return (
        _row_from_ref(
            artifact_id=artifact_id,
            required=required,
            role_class=role_class,
            classification=classification,
            target_day=day_utc or target_day,
            freshness_rule=freshness_rule,
            provenance_required=provenance_required,
            observed_status_fn=observed_status_fn,
            ref=ref,
            explicit_reason_code=explicit_reason_code,
            observed_dependency_artifacts=observed_dependency_artifacts,
        ),
        ref,
    )


def _market_calendar_row(*, truth_root: Path, target_day: str) -> Dict[str, Any]:
    try:
        ref = read_control_plane_surface_v1(
            domain="session",
            surface="market_calendar_day",
            truth_root=truth_root,
            day_utc=target_day,
        )
    except Exception as exc:
        return _error_row(
            artifact_id="market_calendar_day",
            required=True,
            role_class=ROLE_REQUIRED_BINDING_INPUT,
            classification="UPSTREAM_MATERIALIZATION",
            target_day=target_day,
            freshness_rule="TARGET_DAY_COVERAGE_AND_SOURCE_TIMESTAMP",
            provenance_required=True,
            schema_ref="governance/04_DATA/SCHEMAS/C2/MARKET_DATA/market_calendar.v1.schema.json",
            fallback_root=truth_root,
            exc=exc,
            explicit_reason_code=REASON_TARGET_DAY_ARTIFACT_MISSING,
        )
    source_refs: List[Dict[str, Any]] = []
    manifest_path = str(ref.metadata.get("manifest_path") or "").strip()
    year_path = str(ref.metadata.get("year_path") or "").strip()
    if manifest_path:
        source_refs.append({"artifact_path": manifest_path, "artifact_sha256": ""})
    if year_path:
        source_refs.append({"artifact_path": year_path, "artifact_sha256": ref.sha256})
    return _result_row(
        artifact_id="market_calendar_day",
        required=True,
        role_class=ROLE_REQUIRED_BINDING_INPUT,
        classification="UPSTREAM_MATERIALIZATION",
        authority_path=ref.path,
        observed_status="OK",
        result_status="PASS",
        blocker_codes=[],
        schema_status="VALID",
        schema_ref=str(ref.schema_relpath or ""),
        freshness_rule="TARGET_DAY_COVERAGE_AND_SOURCE_TIMESTAMP",
        freshness_status="CURRENT",
        target_day_expected=target_day,
        target_day_observed=target_day,
        date_binding_status="MATCH",
        provenance_required=True,
        provenance_summary={
            "required": True,
            "present": True,
            "fields_present": ["control_plane_gateway.selection_rule"],
            "source": "control_plane_gateway",
        },
        producer={"module": "control_plane_read_gateway_v1", "git_sha": ""},
        source_refs=source_refs or [{"artifact_path": str(ref.path), "artifact_sha256": ref.sha256}],
    )


def _pre_open_bundle_rows(*, truth_root: Path, target_day: str, required: bool) -> List[Dict[str, Any]]:
    row, ref = _surface_row_and_ref(
        artifact_id="pre_open_bundle_v1",
        domain="lifecycle",
        surface="pre_open_bundle",
        truth_root=truth_root,
        target_day=target_day,
        required=required,
        role_class=ROLE_REQUIRED_BINDING_INPUT,
        classification="UPSTREAM_MATERIALIZATION",
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        provenance_required=True,
        observed_status_fn=_startup_status,
        explicit_reason_code=REASON_REQUIRED_GATE_FAIL,
    )
    if ref is None:
        return [row]
    payload = dict(ref.payload)
    materialization_state = str(payload.get("materialization_state") or "").strip().upper()
    bundle_day = str(payload.get("target_day") or "").strip()
    bundle_row = _result_row(
        artifact_id="pre_open_bundle_v1",
        required=required,
        role_class=ROLE_REQUIRED_BINDING_INPUT,
        classification="UPSTREAM_MATERIALIZATION",
        authority_path=ref.path,
        observed_status=materialization_state or "UNKNOWN",
        result_status="PASS" if materialization_state == "COMPLETE" else "FAIL",
        blocker_codes=[str(code).strip() for code in (payload.get("blocking_reason_codes") or []) if str(code).strip()],
        schema_status="VALID",
        schema_ref=str(ref.schema_relpath or ""),
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        freshness_status=_freshness_status(
            payload=payload,
            target_day=target_day,
            observed_day=bundle_day,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        ),
        target_day_expected=target_day,
        target_day_observed=bundle_day,
        date_binding_status="MATCH" if bundle_day == target_day else "MISMATCH",
        provenance_required=True,
        provenance_summary=_provenance_summary(payload=payload, required=True),
        producer={
            "module": str((payload.get("producer") or {}).get("module") or "").strip(),
            "git_sha": str((payload.get("producer") or {}).get("git_sha") or "").strip(),
        },
        source_refs=[{"artifact_path": str(ref.path), "artifact_sha256": ref.sha256}],
        observed_dependency_artifacts=[
            str(item.get("artifact_id") or "").strip()
            for item in (payload.get("prerequisite_checks") or [])
            if isinstance(item, dict) and str(item.get("artifact_id") or "").strip()
        ],
        explicit_reason_code="" if materialization_state == "COMPLETE" else REASON_REQUIRED_GATE_FAIL,
    )
    prerequisite_rows = [
        dict(item)
        for item in (payload.get("prerequisite_checks") or [])
        if isinstance(item, dict)
    ]
    for prerequisite in prerequisite_rows:
        prerequisite["required"] = required
    return [bundle_row, *prerequisite_rows]


def _paper_session_authority_row(*, truth_root: Path, target_day: str, required: bool) -> Dict[str, Any]:
    try:
        ref = read_paper_session_authority_ref_v1(
            truth_root=truth_root,
            day_utc=target_day,
        )
    except Exception as exc:
        return _error_row(
            artifact_id="paper_session_authority_v1",
            required=required,
            role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
            classification="SESSION_AUTHORITY_INPUT",
            target_day=target_day,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            provenance_required=True,
            schema_ref=PAPER_SESSION_AUTHORITY_SCHEMA_RELPATH,
            fallback_root=truth_root,
            exc=exc,
            explicit_reason_code=REASON_TARGET_DAY_ARTIFACT_MISSING,
        )

    payload = dict(ref.payload)
    observed_day = str(payload.get("day_utc") or "").strip()
    authority_status = str(payload.get("authority_status") or "").strip().upper()
    open_allowed = bool(payload.get("paper_open_allowed") is True)
    semantics_ok = authority_status in {"GRANTED", "DENIED"} and open_allowed == (authority_status == "GRANTED")
    observed_status = authority_status if authority_status else "UNKNOWN"
    result_status = "PASS" if semantics_ok else "FAIL"

    return _result_row(
        artifact_id="paper_session_authority_v1",
        required=required,
        role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
        classification="SESSION_AUTHORITY_INPUT",
        authority_path=ref.path,
        observed_status=observed_status,
        result_status=result_status,
        blocker_codes=(
            []
            if semantics_ok
            else [str(payload.get("authority_status") or "PAPER_SESSION_AUTHORITY_INVALID").strip().upper()]
        ),
        schema_status="VALID",
        schema_ref=PAPER_SESSION_AUTHORITY_SCHEMA_RELPATH,
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        freshness_status=_freshness_status(
            payload=payload,
            target_day=target_day,
            observed_day=observed_day,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        ),
        target_day_expected=target_day,
        target_day_observed=observed_day,
        date_binding_status="MATCH" if observed_day == target_day else "MISMATCH",
        provenance_required=True,
        provenance_summary=_provenance_summary(payload=payload, required=True),
        producer={
            "module": str((payload.get("producer") or {}).get("module") or "").strip(),
            "git_sha": str((payload.get("producer") or {}).get("git_sha") or "").strip(),
        },
        source_refs=[{"artifact_path": str(ref.path), "artifact_sha256": ref.sha256}],
    )


def _authority_report_path(*, truth_root: Path, artifact_id: str, target_day: str) -> Path:
    filename_by_artifact = {
        "runtime_resilience_authority_v1": "runtime_resilience_authority.v1.json",
        "safety_state_authority_v1": "safety_state_authority.v1.json",
        "trading_day_readiness_authority_v1": "trading_day_readiness_authority.v1.json",
    }
    filename = filename_by_artifact[artifact_id]
    return (truth_root / "reports" / artifact_id / target_day / filename).resolve()


def _authority_payload_status(
    *,
    artifact_id: str,
    payload: Mapping[str, Any],
) -> tuple[str, bool, List[str]]:
    if artifact_id == "trading_day_readiness_authority_v1":
        status = str(payload.get("readiness_mode") or payload.get("session_state") or "UNKNOWN").strip().upper()
        blocker = str(payload.get("canonical_blocker") or "").strip().upper()
        return status, blocker == "", ([blocker] if blocker else [])
    status = str(payload.get("status") or payload.get("authority_status") or "UNKNOWN").strip().upper()
    canonical = str(payload.get("canonical_blocker") or "").strip().upper()
    reason_codes = [str(code).strip() for code in (payload.get("reason_codes") or []) if str(code).strip()]
    blocker_codes = [str(code).strip() for code in (payload.get("blocking_codes") or []) if str(code).strip()]
    codes = [canonical] if canonical else []
    codes.extend(reason_codes or blocker_codes)
    return status, status == "PASS", codes


def _declared_session_authority_dependency_row(
    *,
    truth_root: Path,
    target_day: str,
    artifact_id: str,
    schema_ref: str,
    producer_command: str,
    required: bool = True,
) -> Dict[str, Any]:
    authority_path = _authority_report_path(truth_root=truth_root, artifact_id=artifact_id, target_day=target_day)
    if not authority_path.exists() or not authority_path.is_file():
        return _result_row(
            artifact_id=artifact_id,
            required=required,
            role_class=ROLE_REQUIRED_DERIVED_GATE,
            classification="SESSION_AUTHORITY_DECLARED_DEPENDENCY",
            authority_path=authority_path,
            observed_status="MISSING",
            result_status="FAIL",
            blocker_codes=[f"{artifact_id.upper()}_MISSING"],
            schema_status="MISSING",
            schema_ref=schema_ref,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            freshness_status="STALE",
            target_day_expected=target_day,
            target_day_observed="",
            date_binding_status="MISSING",
            provenance_required=True,
            provenance_summary={"required": True, "present": False, "fields_present": [], "source": ""},
            producer={"module": producer_command, "git_sha": ""},
            source_refs=[],
            explicit_reason_code=REASON_TARGET_DAY_ARTIFACT_MISSING,
        )
    try:
        payload = json.loads(authority_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("AUTHORITY_PAYLOAD_NOT_OBJECT")
        schema_status = "PRACTICAL_VALID"
        if schema_ref:
            validate_against_repo_schema_v1(payload, REPO_ROOT, schema_ref)
            schema_status = "VALID"
    except Exception:
        return _result_row(
            artifact_id=artifact_id,
            required=required,
            role_class=ROLE_REQUIRED_DERIVED_GATE,
            classification="SESSION_AUTHORITY_DECLARED_DEPENDENCY",
            authority_path=authority_path,
            observed_status="INVALID",
            result_status="FAIL",
            blocker_codes=[f"{artifact_id.upper()}_INVALID"],
            schema_status="INVALID",
            schema_ref=schema_ref,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            freshness_status="STALE",
            target_day_expected=target_day,
            target_day_observed="",
            date_binding_status="UNKNOWN",
            provenance_required=True,
            provenance_summary={"required": True, "present": False, "fields_present": [], "source": ""},
            producer={"module": producer_command, "git_sha": ""},
            source_refs=[{"artifact_path": str(authority_path), "artifact_sha256": ""}],
            explicit_reason_code=REASON_SCHEMA_INVALID,
        )
    observed_day = _extract_payload_day(payload)
    observed_status, is_pass, blocker_codes = _authority_payload_status(artifact_id=artifact_id, payload=payload)
    date_binding_status = "MATCH" if observed_day == target_day else ("MISSING" if not observed_day else "MISMATCH")
    result_status = "PASS" if is_pass and date_binding_status == "MATCH" else "FAIL"
    if date_binding_status != "MATCH":
        blocker_codes = list(blocker_codes) + [f"{artifact_id.upper()}_DAY_MISMATCH"]
    return _result_row(
        artifact_id=artifact_id,
        required=required,
        role_class=ROLE_REQUIRED_DERIVED_GATE,
        classification="SESSION_AUTHORITY_DECLARED_DEPENDENCY",
        authority_path=authority_path,
        observed_status=observed_status,
        result_status=result_status,
        blocker_codes=blocker_codes or ([] if result_status == "PASS" else [f"{artifact_id.upper()}_NOT_PASS"]),
        schema_status=schema_status,
        schema_ref=schema_ref,
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        freshness_status=_freshness_status(
            payload=payload,
            target_day=target_day,
            observed_day=observed_day,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        ),
        target_day_expected=target_day,
        target_day_observed=observed_day,
        date_binding_status=date_binding_status,
        provenance_required=True,
        provenance_summary=_provenance_summary(payload=payload, required=True),
        producer={
            "module": producer_command,
            "git_sha": str((payload.get("producer") or {}).get("git_sha") or "").strip(),
        },
        source_refs=[{"artifact_path": str(authority_path), "artifact_sha256": ""}],
        observed_dependency_artifacts=_payload_dependencies(artifact_id, payload),
        explicit_reason_code="" if result_status == "PASS" else REASON_REQUIRED_GATE_FAIL,
    )


def _target_day_session_authority_row(*, truth_root: Path, target_day: str) -> Dict[str, Any]:
    authority_path = (
        truth_root / "reports" / "paper_session_authority_v1" / target_day / "paper_session_authority.v1.json"
    ).resolve()
    producer_command = _producer_command(
        "ops/tools/run_session_authority_v1.py",
        day_flag="--target_day",
        target_day=target_day,
        truth_root=truth_root,
    )
    if not authority_path.exists() or not authority_path.is_file():
        return _result_row(
            artifact_id="target_day_session_authority_v1",
            required=False,
            role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
            classification="DOWNSTREAM_SESSION_AUTHORITY_SELF_CHECK",
            authority_path=authority_path,
            observed_status="MISSING",
            result_status="FAIL",
            blocker_codes=["TARGET_DAY_SESSION_AUTHORITY_V1_MISSING"],
            schema_status="MISSING",
            schema_ref=PAPER_SESSION_AUTHORITY_SCHEMA_RELPATH,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            freshness_status="STALE",
            target_day_expected=target_day,
            target_day_observed="",
            date_binding_status="MISSING",
            provenance_required=True,
            provenance_summary={"required": True, "present": False, "fields_present": [], "source": ""},
            producer={"module": producer_command, "git_sha": ""},
            explicit_reason_code=REASON_TARGET_DAY_ARTIFACT_MISSING,
        )
    try:
        ref = read_paper_session_authority_ref_v1(truth_root=truth_root, day_utc=target_day)
        payload = dict(ref.payload)
        observed_day = str(payload.get("day_utc") or "").strip()
        authority_status = str(payload.get("authority_status") or "UNKNOWN").strip().upper()
    except Exception:
        return _result_row(
            artifact_id="target_day_session_authority_v1",
            required=False,
            role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
            classification="DOWNSTREAM_SESSION_AUTHORITY_SELF_CHECK",
            authority_path=authority_path,
            observed_status="INVALID",
            result_status="FAIL",
            blocker_codes=["TARGET_DAY_SESSION_AUTHORITY_V1_INVALID"],
            schema_status="INVALID",
            schema_ref=PAPER_SESSION_AUTHORITY_SCHEMA_RELPATH,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            freshness_status="STALE",
            target_day_expected=target_day,
            target_day_observed="",
            date_binding_status="UNKNOWN",
            provenance_required=True,
            provenance_summary={"required": True, "present": False, "fields_present": [], "source": ""},
            producer={"module": producer_command, "git_sha": ""},
            explicit_reason_code=REASON_SCHEMA_INVALID,
        )
    return _result_row(
        artifact_id="target_day_session_authority_v1",
        required=False,
        role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
        classification="DOWNSTREAM_SESSION_AUTHORITY_SELF_CHECK",
        authority_path=authority_path,
        observed_status=authority_status,
        result_status="PASS" if authority_status == "GRANTED" and observed_day == target_day else "FAIL",
        blocker_codes=[] if authority_status == "GRANTED" else ["TARGET_DAY_SESSION_AUTHORITY_V1_NOT_GRANTED"],
        schema_status="VALID",
        schema_ref=PAPER_SESSION_AUTHORITY_SCHEMA_RELPATH,
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        freshness_status=_freshness_status(
            payload=payload,
            target_day=target_day,
            observed_day=observed_day,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        ),
        target_day_expected=target_day,
        target_day_observed=observed_day,
        date_binding_status="MATCH" if observed_day == target_day else "MISMATCH",
        provenance_required=True,
        provenance_summary=_provenance_summary(payload=payload, required=True),
        producer={"module": producer_command, "git_sha": ""},
        source_refs=[{"artifact_path": str(authority_path), "artifact_sha256": ""}],
        explicit_reason_code=REASON_REQUIRED_GATE_FAIL if authority_status != "GRANTED" else "",
    )


def read_optional_pre_open_bundle_surface_v1(*, truth_root: Path, target_day: str) -> ControlPlaneReadRefV1 | None:
    try:
        return read_control_plane_surface_v1(
            domain="lifecycle",
            surface="pre_open_bundle",
            truth_root=truth_root,
            day_utc=target_day,
        )
    except Exception:
        return None


def _previous_day_economic_rows(
    *,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
    readiness_ref: ControlPlaneReadRefV1 | None,
) -> List[Dict[str, Any]]:
    if readiness_ref is None:
        return []
    economic_state = readiness_ref.payload.get("economic_state")
    if not isinstance(economic_state, dict):
        return []
    previous_day = str(economic_state.get("source_day_utc") or "").strip() or _prior_day_utc(target_day)
    rows: List[Dict[str, Any]] = []
    build_path_present = str(economic_state.get("build_path") or "").strip()
    if build_path_present:
        build_row, _ = _surface_row_and_ref(
            artifact_id="economic_state_build_v1",
            domain="execution",
            surface="economic_state_build",
            truth_root=truth_root,
            target_day=target_day,
            day_utc=previous_day,
            required=False,
            role_class=ROLE_REQUIRED_BINDING_INPUT,
            classification="PREVIOUS_DAY_ECONOMIC_BUILD",
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            provenance_required=True,
            observed_status_fn=_economic_build_status,
        )
        rows.append(build_row)
    package_path_present = str(economic_state.get("package_path") or "").strip()
    if package_path_present:
        package_row, _ = _surface_row_and_ref(
            artifact_id="economic_state_package_v1",
            domain="execution",
            surface="economic_state_package",
            truth_root=truth_root,
            target_day=target_day,
            day_utc=previous_day,
            required=False,
            role_class=ROLE_REQUIRED_BINDING_INPUT,
            classification="PREVIOUS_DAY_ECONOMIC_PACKAGE",
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            provenance_required=True,
            observed_status_fn=_economic_package_status,
            environment=environment,
            ib_account=ib_account,
        )
        rows.append(package_row)
    return rows


def collect_target_day_build_artifact_rows_v1(
    *,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
) -> List[Dict[str, Any]]:
    paper_mode = str(environment or "").strip().upper() == "PAPER"

    rows: List[Dict[str, Any]] = []
    rows.append(_market_calendar_row(truth_root=truth_root, target_day=target_day))

    for spec in (
        {
            "artifact_id": "day_authority_decision_v1",
            "domain": "lifecycle",
            "surface": "day_authority_decision",
            "required": not paper_mode,
            "role_class": ROLE_REQUIRED_BINDING_INPUT,
            "classification": "SESSION_AUTHORITY_INPUT",
            "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            "provenance_required": True,
            "observed_status_fn": _day_authority_status,
        },
    ):
        row, _ = _surface_row_and_ref(
            artifact_id=spec["artifact_id"],
            domain=spec["domain"],
            surface=spec["surface"],
            truth_root=truth_root,
            target_day=target_day,
            required=spec["required"],
            role_class=spec["role_class"],
            classification=spec["classification"],
            freshness_rule=spec["freshness_rule"],
            provenance_required=spec["provenance_required"],
            observed_status_fn=spec["observed_status_fn"],
        )
        rows.append(row)

    rows.extend(_pre_open_bundle_rows(truth_root=truth_root, target_day=target_day, required=not paper_mode))

    auth_row, _ = _surface_row_and_ref(
        artifact_id="authorization_gate_verdict_v1",
        domain="execution",
        surface="authorization_gate_verdict",
        truth_root=truth_root,
        target_day=target_day,
        required=not paper_mode,
        role_class=ROLE_REQUIRED_DERIVED_GATE,
        classification="AUTHORIZATION_GATE",
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        provenance_required=True,
        observed_status_fn=_authorization_verdict_status,
        environment=environment,
        ib_account=ib_account,
        observed_dependency_artifacts=["primary_scoped_canonical_authority_head_v1"],
    )
    auth_row["artifact_id"] = "primary_scoped_authorization_gate_verdict_v1"
    auth_row["artifact_name"] = "primary_scoped_authorization_gate_verdict_v1"
    rows.append(auth_row)

    readiness_row, readiness_ref = _surface_row_and_ref(
        artifact_id="trade_submit_readiness_c2_v1",
        domain="execution",
        surface="trade_submit_readiness",
        truth_root=truth_root,
        target_day=target_day,
        required=not paper_mode,
        role_class=ROLE_REQUIRED_DERIVED_GATE,
        classification="SUBMIT_READINESS",
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        provenance_required=True,
        observed_status_fn=_submit_readiness_status,
        environment=environment,
        ib_account=ib_account,
    )

    rows.extend(
        [
            _surface_row_and_ref(
                artifact_id="paper_startup_intent_input_convergence_v1",
                domain="lifecycle",
                surface="paper_startup_intent_input_convergence",
                truth_root=truth_root,
                target_day=target_day,
                required=True,
                role_class=ROLE_REQUIRED_BINDING_INPUT,
                classification="UPSTREAM_CONVERGENCE",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=False,
                observed_status_fn=_generic_convergence_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="bod_execution_environment_proof_v1",
                domain="lifecycle",
                surface="bod_execution_environment_proof",
                truth_root=truth_root,
                target_day=target_day,
                required=True,
                role_class=ROLE_REQUIRED_BINDING_INPUT,
                classification="EXECUTION_SUBSTRATE_PROOF",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_fact_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="startup_materialization_input_convergence_v1",
                domain="lifecycle",
                surface="startup_materialization_input_convergence",
                truth_root=truth_root,
                target_day=target_day,
                required=True,
                role_class=ROLE_REQUIRED_BINDING_INPUT,
                classification="UPSTREAM_CONVERGENCE",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=False,
                observed_status_fn=_generic_convergence_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="phasec_risk_inputs_prep_v1",
                domain="lifecycle",
                surface="phasec_risk_inputs_prep",
                truth_root=truth_root,
                target_day=target_day,
                required=True,
                role_class=ROLE_REQUIRED_BINDING_INPUT,
                classification="UPSTREAM_MATERIALIZATION",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_fact_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="paper_startup_authorization_convergence_v1",
                domain="lifecycle",
                surface="paper_startup_authorization_convergence",
                truth_root=truth_root,
                target_day=target_day,
                required=not paper_mode,
                role_class=ROLE_REQUIRED_DERIVED_GATE,
                classification="AUTHORIZATION_CONVERGENCE",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_authorization_convergence_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="startup_materialization_v1",
                domain="execution",
                surface="startup_materialization",
                truth_root=truth_root,
                target_day=target_day,
                required=not paper_mode,
                role_class=ROLE_REQUIRED_BINDING_INPUT,
                classification="UPSTREAM_MATERIALIZATION",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_startup_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="paper_trading_posture_v1",
                domain="execution",
                surface="paper_trading_posture",
                truth_root=truth_root,
                target_day=target_day,
                required=not paper_mode,
                role_class=ROLE_REQUIRED_BINDING_INPUT,
                classification="UPSTREAM_POLICY_INPUT",
                freshness_rule="DECLARED_CURRENT",
                provenance_required=True,
                observed_status_fn=_posture_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="capability_state_v1",
                domain="lifecycle",
                surface="capability_state",
                truth_root=truth_root,
                target_day=target_day,
                required=True,
                role_class=ROLE_REQUIRED_DERIVED_GATE,
                classification="POLICY_PREREQUISITE",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_capability_artifact_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="paper_policy_verdict_v1",
                domain="policy",
                surface="paper_policy_verdict",
                truth_root=truth_root,
                target_day=target_day,
                required=not paper_mode,
                role_class=ROLE_REQUIRED_DERIVED_GATE,
                classification="POLICY_DECISION",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_overall_status,
            )[0],
            readiness_row,
        ]
    )
    rows.extend(
        _previous_day_economic_rows(
            truth_root=truth_root,
            target_day=target_day,
            environment=environment,
            ib_account=ib_account,
            readiness_ref=readiness_ref,
        )
    )
    submit_boundary_row, submit_boundary_ref = _surface_row_and_ref(
        artifact_id="submit_boundary_status_v1",
        domain="execution",
        surface="submit_boundary_status",
        truth_root=truth_root,
        target_day=target_day,
        required=False,
        role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
        classification="SUBMIT_BOUNDARY",
        freshness_rule="DECLARED_CURRENT",
        provenance_required=True,
        observed_status_fn=_submit_boundary_status,
    )
    engine_activity_row = _engine_activity_authorization_row_from_submit_boundary(
        truth_root=truth_root,
        target_day=target_day,
        submit_boundary_ref=submit_boundary_ref,
    )
    rows.extend(
        [
            _declared_session_authority_dependency_row(
                truth_root=truth_root,
                target_day=target_day,
                artifact_id="trading_day_readiness_authority_v1",
                schema_ref=TRADING_DAY_READINESS_AUTHORITY_SCHEMA,
                producer_command=_producer_command(
                    "ops/tools/run_trading_day_readiness_authority_v1.py",
                    day_flag="--target_day",
                    target_day=target_day,
                    truth_root=truth_root,
                ),
                required=True,
            ),
            _declared_session_authority_dependency_row(
                truth_root=truth_root,
                target_day=target_day,
                artifact_id="runtime_resilience_authority_v1",
                schema_ref="",
                producer_command=_producer_command(
                    "ops/tools/run_runtime_resilience_authority_v1.py",
                    day_flag="--day_utc",
                    target_day=target_day,
                    truth_root=truth_root,
                ),
                required=True,
            ),
            _declared_session_authority_dependency_row(
                truth_root=truth_root,
                target_day=target_day,
                artifact_id="safety_state_authority_v1",
                schema_ref=SAFETY_STATE_AUTHORITY_SCHEMA,
                producer_command=_producer_command(
                    "ops/tools/run_safety_state_authority_v1.py",
                    day_flag="--day_utc",
                    target_day=target_day,
                    truth_root=truth_root,
                ),
                required=True,
            ),
            _target_day_session_authority_row(truth_root=truth_root, target_day=target_day),
            _paper_session_authority_row(
                truth_root=truth_root,
                target_day=target_day,
                required=False,
            ),
            submit_boundary_row,
            engine_activity_row,
            _surface_row_and_ref(
                artifact_id="paper_session_ledger_v1",
                domain="execution",
                surface="paper_session_ledger",
                truth_root=truth_root,
                target_day=target_day,
                required=False,
                role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
                classification="SESSION_AUTHORITY_INPUT",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_ledger_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="startup_proof_validation_v1",
                domain="execution",
                surface="startup_proof_validation",
                truth_root=truth_root,
                target_day=target_day,
                required=False,
                role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
                classification="SESSION_AUTHORITY_INPUT",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_startup_proof_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="trading_day_intent_generation_v1",
                domain="execution",
                surface="trading_day_intent_generation",
                truth_root=truth_root,
                target_day=target_day,
                required=True,
                role_class=ROLE_REQUIRED_DERIVED_GATE,
                classification="SUPPORTING_DAILY_CONTROL",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_supporting_artifact_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="intents_day_completeness_v1",
                domain="lifecycle",
                surface="intents_day_completeness",
                truth_root=truth_root,
                target_day=target_day,
                required=True,
                role_class=ROLE_REQUIRED_DERIVED_GATE,
                classification="SUPPORTING_DAILY_CONTROL",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_supporting_artifact_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="paper_day_control_plane_v1",
                domain="execution",
                surface="paper_day_control_plane",
                truth_root=truth_root,
                target_day=target_day,
                required=False,
                role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
                classification="SUPPORTING_DAILY_CONTROL",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_supporting_artifact_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="paper_trading_day_authority_v1",
                domain="execution",
                surface="paper_trading_day_authority",
                truth_root=truth_root,
                target_day=target_day,
                required=False,
                role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
                classification="SUPPORTING_DAILY_CONTROL",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_supporting_artifact_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="trade_readiness_decision_v1",
                domain="execution",
                surface="trade_readiness_decision",
                truth_root=truth_root,
                target_day=target_day,
                required=False,
                role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
                classification="SUPPORTING_DAILY_CONTROL",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_supporting_artifact_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="trade_readiness_presubmit_v1",
                domain="execution",
                surface="trade_readiness_presubmit",
                truth_root=truth_root,
                target_day=target_day,
                required=False,
                role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
                classification="SUPPORTING_DAILY_CONTROL",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_supporting_artifact_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="trading_day_control_plane_v1",
                domain="execution",
                surface="trading_day_control_plane",
                truth_root=truth_root,
                target_day=target_day,
                required=False,
                role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
                classification="SUPPORTING_DAILY_CONTROL",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_supporting_artifact_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="trading_day_execution_control_plane_v1",
                domain="execution",
                surface="trading_day_execution_control_plane",
                truth_root=truth_root,
                target_day=target_day,
                required=False,
                role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
                classification="SUPPORTING_DAILY_CONTROL",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_supporting_artifact_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="trading_day_state_machine_v1",
                domain="execution",
                surface="trading_day_state_machine",
                truth_root=truth_root,
                target_day=target_day,
                required=False,
                role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
                classification="FINAL_DAY_STATE",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_state_machine_status,
            )[0],
            _surface_row_and_ref(
                artifact_id="deployment_state_machine_v1",
                domain="platform",
                surface="deployment_state_machine",
                truth_root=truth_root,
                target_day=target_day,
                required=False,
                role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
                classification="DIAGNOSTIC_CONTEXT",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                provenance_required=True,
                observed_status_fn=_supporting_artifact_status,
            )[0],
        ]
    )
    return rows
