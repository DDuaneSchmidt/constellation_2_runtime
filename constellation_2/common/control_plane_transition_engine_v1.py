from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterable, Mapping, Sequence

from constellation_2.common.constitutional_runtime_v1 import (
    ARTIFACT_CLASS_OUTCOME_RECORD,
    FINALITY_FINALIZED,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_governed_dependency_ref_v1,
    resolve_constitutional_artifact_path_v1,
)
from constellation_2.common.control_plane_stage_admission_v1 import (
    CONTROL_STAGE_CONTEXT_ADMITTED,
    CONTROL_STAGE_DAY_ADMITTED,
    CONTROL_STAGE_EXECUTION_BUILD_ADMITTED,
    CONTROL_STAGE_SESSION_ADMITTED,
    ControlPlaneArtifactRefV1,
    STARTUP_CHAIN_CERTIFICATION,
    _ref_from_control_artifact,
    _stage_path,
    build_control_stage_admitted_payload_v1,
    build_startup_chain_certification_payload_v1,
    write_control_stage_admitted_v1,
    write_control_stage_certification_v1,
    write_startup_chain_certification_v1,
)
from constellation_2.common.control_plane_stage_definitions_v1 import (
    CONTROL_STAGE_CONTEXT,
    CONTROL_STAGE_DAY,
    CONTROL_STAGE_EXECUTION_BUILD,
    CONTROL_STAGE_SESSION,
    POLICY_ADMIT_AND_CERTIFY,
    POLICY_CERTIFY_ONLY,
    POLICY_EVALUATE,
    POLICY_EXPLAIN_BLOCKED,
    POLICY_RECOMPUTE_FROZEN,
    POLICY_SUPERSEDE_FROM_NEW_INPUTS,
    STAGE_DEFINITION_BY_ID,
    STAGE_DEFINITIONS,
)
from constellation_2.common.control_plane_validation_kernel_v1 import (
    LEGACY_RUNTIME_ROOT,
    REPO_ROOT,
    VALIDATOR_VERSION,
    _build_scope,
    _check_path_boundary,
    _finalize_report,
    _new_report,
    validate_control_plane_boundary_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_validated_surface_v1,
)
from constellation_2.common.runtime_contract_v1 import (
    resolve_canonical_truth_root,
    resolve_truth_sleeves_root,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


TRANSITION_RECORD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_stage_transition_record.v1.schema.json"
STAGE_ADMISSION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_stage_admission.v1.schema.json"
TRANSITION_RECORD_ARTIFACT_ID = "control_stage_transition_record_v1"
TRANSITION_ENGINE_ID = "constellation_2.common.control_plane_transition_engine_v1"
MUTATING_POLICIES = {
    POLICY_ADMIT_AND_CERTIFY,
    POLICY_CERTIFY_ONLY,
    POLICY_RECOMPUTE_FROZEN,
    POLICY_SUPERSEDE_FROM_NEW_INPUTS,
}


class ControlPlaneTransitionEngineError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_codes(values: Iterable[str]) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _plain_ref(*, artifact_id: str, path: Path, sha256: str) -> Dict[str, str]:
    return {
        "artifact_id": str(artifact_id).strip(),
        "artifact_path": str(path.resolve()),
        "artifact_sha256": str(sha256).strip(),
    }


def _family_ref_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("artifact_id") or "").strip(),
        str(row.get("path") or "").strip(),
        str(row.get("sha256") or "").strip(),
        str(row.get("root_type") or "").strip(),
    )


def _normalized_family_ref(row: Mapping[str, Any]) -> Dict[str, str]:
    return {
        "artifact_id": str(row.get("artifact_id") or "").strip(),
        "path": str(row.get("path") or "").strip(),
        "sha256": str(row.get("sha256") or "").strip(),
        "root_type": str(row.get("root_type") or "").strip(),
    }


def _dedupe_family_refs(*rows: Sequence[Mapping[str, Any]]) -> list[Dict[str, str]]:
    seen: set[tuple[str, str, str, str]] = set()
    normalized: list[Dict[str, str]] = []
    for group in rows:
        for row in group:
            if not isinstance(row, Mapping):
                continue
            candidate = _normalized_family_ref(row)
            key = _family_ref_key(candidate)
            if not key[0] or not key[1] or not key[2]:
                continue
            if key in seen:
                continue
            seen.add(key)
            normalized.append(candidate)
    normalized.sort(key=lambda item: (item["artifact_id"], item["path"], item["sha256"], item["root_type"]))
    return normalized


def _control_ref_to_family_ref(row: Mapping[str, Any] | None, *, root_type: str = "canonical_truth_root") -> Dict[str, str] | None:
    if not isinstance(row, Mapping):
        return None
    artifact_id = str(row.get("artifact_id") or "").strip()
    path = str(row.get("artifact_path") or row.get("path") or "").strip()
    sha256 = str(row.get("artifact_sha256") or row.get("sha256") or "").strip()
    if not artifact_id or not path or not sha256:
        return None
    return {
        "artifact_id": artifact_id,
        "path": path,
        "sha256": sha256,
        "root_type": root_type,
    }


def _scope_identity(scope: Any) -> Dict[str, str]:
    return {
        "target_day": str(scope.day_utc),
        "scope_id": str(scope.scope_id),
        "operation_type": str(scope.operation_type),
        "sleeve_id": str(scope.sleeve_id),
        "environment": str(scope.environment),
        "ib_account": str(scope.ib_account),
    }


def _dependency_ref_from_plain(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "artifact_id": str(row.get("artifact_id") or "").strip(),
        "path": str(row.get("path") or "").strip(),
        "sha256": str(row.get("sha256") or "").strip(),
    }


def _lineage_dependency_ref(ref: ControlPlaneArtifactRefV1) -> Dict[str, Any]:
    return build_governed_dependency_ref_v1(
        repo_root=REPO_ROOT,
        artifact_id=ref.artifact_id,
        path=ref.path,
        sha256=ref.sha256,
        finality_state=FINALITY_FINALIZED,
    )


def _prospective_control_ref(*, artifact_id: str, path: Path, payload: Mapping[str, Any]) -> ControlPlaneArtifactRefV1:
    return ControlPlaneArtifactRefV1(
        artifact_id=artifact_id,
        path=path,
        payload=dict(payload),
        sha256=canonical_hash_for_c2_artifact_v1(dict(payload)),
    )


def _load_control_artifact_ref(
    *,
    scope: Any,
    path: Path,
    expected_artifact_id: str,
    label: str,
    schema_relpath: str,
) -> ControlPlaneArtifactRefV1:
    report = _new_report("control_plane_transition_engine_v1", scope)
    _check_path_boundary(
        report,
        scope=scope,
        label=label,
        path=path,
        expected_root_type="canonical_truth_root",
        reject_derived=False,
    )
    report = _finalize_report(report)
    if report["errors"]:
        raise ControlPlaneTransitionEngineError(report["errors"][0])
    ref = read_validated_surface_v1(path=path, schema_relpath=schema_relpath)
    artifact_id = str(ref.payload.get("stage_artifact_id") or ref.payload.get("transition_record_artifact_id") or "").strip()
    if artifact_id != expected_artifact_id:
        raise ControlPlaneTransitionEngineError(
            f"{label}_ARTIFACT_ID_MISMATCH:expected={expected_artifact_id}:actual={artifact_id or 'UNKNOWN'}"
        )
    return ControlPlaneArtifactRefV1(
        artifact_id=expected_artifact_id,
        path=ref.path,
        payload=dict(ref.payload),
        sha256=ref.sha256,
    )


def _load_transition_record_ref(*, scope: Any, path: Path) -> ControlPlaneArtifactRefV1:
    return _load_control_artifact_ref(
        scope=scope,
        path=path,
        expected_artifact_id=TRANSITION_RECORD_ARTIFACT_ID,
        label="CONTROL_STAGE_TRANSITION_RECORD",
        schema_relpath=TRANSITION_RECORD_SCHEMA,
    )


def _load_upstream_stage_ref(*, scope: Any, path: Path, expected_artifact_id: str) -> ControlPlaneArtifactRefV1:
    ref = _load_control_artifact_ref(
        scope=scope,
        path=path,
        expected_artifact_id=expected_artifact_id,
        label="UPSTREAM_STAGE_ADMISSION",
        schema_relpath=STAGE_ADMISSION_SCHEMA,
    )
    return ref


def _run_family_validator(*, stage_id: str, scope: Any, candidate_path: Path | None) -> Dict[str, Any]:
    stage_definition = STAGE_DEFINITION_BY_ID[stage_id]
    kwargs: Dict[str, Any] = {
        "repo_root": REPO_ROOT,
        "day_utc": scope.day_utc,
        "sleeve_id": scope.sleeve_id,
        "environment": scope.environment,
        "ib_account": scope.ib_account,
        "operation_type": scope.operation_type,
        "canonical_truth_root": scope.canonical_truth_root,
        "truth_sleeves_root": scope.truth_sleeves_root,
    }
    if stage_id == CONTROL_STAGE_EXECUTION_BUILD:
        if candidate_path is None:
            raise ControlPlaneTransitionEngineError("EXECUTION_BUILD_CANDIDATE_PATH_REQUIRED")
        kwargs["candidate_path"] = candidate_path
    return stage_definition.validator_fn(**kwargs)


def _comparison_signature(refs: Sequence[Mapping[str, Any]]) -> set[tuple[str, str, str, str]]:
    return {_family_ref_key(row) for row in refs if _family_ref_key(row)[0]}


def _compare_authoritative_inputs(*, current_refs: Sequence[Mapping[str, Any]], frozen_refs: Sequence[Mapping[str, Any]]) -> bool:
    return _comparison_signature(current_refs) == _comparison_signature(frozen_refs)


def _invariant_results(boundary_report: Mapping[str, Any], family_report: Mapping[str, Any], policy_errors: Sequence[str]) -> list[Dict[str, Any]]:
    combined = _normalize_codes(
        [
            *(boundary_report.get("invariants_checked") or []),
            *(family_report.get("invariants_checked") or []),
        ]
    )
    overall_pass = not boundary_report.get("errors") and not family_report.get("errors") and not list(policy_errors)
    return [{"invariant_id": invariant_id, "status": "PASS" if overall_pass else "CHECKED"} for invariant_id in combined]


def _input_resolution_mode(policy: str) -> str:
    return "FROZEN" if policy == POLICY_RECOMPUTE_FROZEN else "NEWLY_RESOLVED"


def _default_transition_status(policy: str, *, admissible: bool, certifiable: bool) -> str:
    if not admissible or not certifiable:
        return "BLOCKED"
    if policy == POLICY_ADMIT_AND_CERTIFY:
        return "ADMITTED_AND_CERTIFIED"
    if policy == POLICY_CERTIFY_ONLY:
        return "CERTIFIED_ONLY"
    if policy == POLICY_RECOMPUTE_FROZEN:
        return "RECOMPUTED_FROZEN"
    if policy == POLICY_SUPERSEDE_FROM_NEW_INPUTS:
        return "SUPERSEDED_FROM_NEW_INPUTS"
    if policy == POLICY_EXPLAIN_BLOCKED:
        return "EXPLAINED"
    return "EVALUATED"


def _build_explanation(*, stage_id: str, policy: str, blocked_codes: Sequence[str], persistence_plan: Mapping[str, Any]) -> Dict[str, Any]:
    codes = _normalize_codes(blocked_codes)
    if codes:
        summary = f"{stage_id} blocked under policy {policy}: {','.join(codes)}"
    else:
        summary = f"{stage_id} admissible under policy {policy}"
    return {
        "summary": summary,
        "blocked_reason_codes": codes,
        "persistence_plan": dict(persistence_plan),
    }


def _transition_id_payload(
    *,
    stage_id: str,
    policy: str,
    identity: Mapping[str, Any],
    input_resolution_mode: str,
    authoritative_inputs_used: Sequence[Mapping[str, Any]],
    upstream_stage_ref: Mapping[str, Any] | None,
    frozen_inputs_used: Sequence[Mapping[str, Any]],
    blocked_codes: Sequence[str],
    supersedes_ref: Mapping[str, Any] | None,
    admission_decision: Mapping[str, Any],
    certification_decision: Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "stage_id": stage_id,
        "policy": policy,
        "identity": dict(identity),
        "input_resolution_mode": input_resolution_mode,
        "authoritative_inputs_used": list(authoritative_inputs_used),
        "upstream_stage_ref": dict(upstream_stage_ref) if upstream_stage_ref is not None else None,
        "frozen_inputs_used": list(frozen_inputs_used),
        "blocked_codes": _normalize_codes(blocked_codes),
        "supersedes_ref": dict(supersedes_ref) if supersedes_ref is not None else None,
        "admission_decision": dict(admission_decision),
        "certification_decision": dict(certification_decision),
    }


def _transition_path(*, canonical_truth_root: Path, target_day: str, stage_id: str, scope_id: str, transition_id: str) -> Path:
    return resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=TRANSITION_RECORD_ARTIFACT_ID,
        day_utc=target_day,
        canonical_truth_root=canonical_truth_root,
        extra_variables={
            "stage_id": stage_id,
            "scope_id": scope_id,
            "transition_id": transition_id,
        },
    )


def _write_transition_record_v1(
    *,
    evaluation: Mapping[str, Any],
    transition_status: str,
    admission_decision: Mapping[str, Any],
    certification_decision: Mapping[str, Any],
    supersedes_ref: Mapping[str, Any] | None = None,
) -> ControlPlaneArtifactRefV1:
    assert_constitutional_writer_allowed_v1(REPO_ROOT, TRANSITION_RECORD_ARTIFACT_ID, TRANSITION_ENGINE_ID)
    identity = {
        "target_day": str(evaluation["target_day"]),
        "scope_id": str(evaluation["scope_id"]),
        "operation_type": str(evaluation["operation_type"]),
        "sleeve_id": str(evaluation["sleeve_id"]),
        "environment": str(evaluation["environment"]),
        "ib_account": str(evaluation["ib_account"]),
    }
    transition_id = canonical_hash_for_c2_artifact_v1(
        _transition_id_payload(
            stage_id=str(evaluation["stage_id"]),
            policy=str(evaluation["policy"]),
            identity=identity,
            input_resolution_mode=str(evaluation["input_resolution_mode"]),
            authoritative_inputs_used=list(evaluation["authoritative_inputs_used"]),
            upstream_stage_ref=evaluation.get("upstream_stage_ref"),
            frozen_inputs_used=list(evaluation.get("frozen_inputs_used") or []),
            blocked_codes=list(evaluation.get("blocked_reason_codes") or []),
            supersedes_ref=supersedes_ref,
            admission_decision=admission_decision,
            certification_decision=certification_decision,
        )
    )
    generated_at_utc = _utc_now()
    dependency_rows = _dedupe_family_refs(
        evaluation.get("authoritative_inputs_used") or [],
        evaluation.get("frozen_inputs_used") or [],
        [row for row in [_control_ref_to_family_ref(evaluation.get("upstream_stage_ref"))] if row],
        [row for row in [_control_ref_to_family_ref(supersedes_ref)] if row],
    )
    dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type=TRANSITION_RECORD_ARTIFACT_ID,
        artifact_class=ARTIFACT_CLASS_OUTCOME_RECORD,
        authority_id=TRANSITION_RECORD_ARTIFACT_ID,
        declared_dependency_artifacts=[row["artifact_id"] for row in dependency_rows],
        dependency_refs=[_dependency_ref_from_plain(row) for row in dependency_rows],
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type=TRANSITION_RECORD_ARTIFACT_ID,
        artifact_version="v1",
        artifact_class=ARTIFACT_CLASS_OUTCOME_RECORD,
        authority_id=TRANSITION_RECORD_ARTIFACT_ID,
        producer_id=TRANSITION_ENGINE_ID,
        generated_at_utc=generated_at_utc,
        effective_at_utc=generated_at_utc,
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=[_dependency_ref_from_plain(row) for row in dependency_rows],
        policy_snapshot_refs=[],
        code_version="UNKNOWN",
        run_id=str(evaluation["scope_id"]),
        supersedes_ref=(
            build_governed_dependency_ref_v1(
                repo_root=REPO_ROOT,
                artifact_id=str(supersedes_ref["artifact_id"]),
                path=Path(str(supersedes_ref["artifact_path"])).resolve(),
                sha256=str(supersedes_ref["artifact_sha256"]),
                finality_state=FINALITY_FINALIZED,
            )
            if supersedes_ref is not None
            else None
        ),
    )
    payload = {
        "schema_id": "control_stage_transition_record",
        "schema_version": "v1",
        "transition_record_artifact_id": TRANSITION_RECORD_ARTIFACT_ID,
        "transition_id": transition_id,
        "stage_id": str(evaluation["stage_id"]),
        "target_day": str(evaluation["target_day"]),
        "scope_id": str(evaluation["scope_id"]),
        "operation_type": str(evaluation["operation_type"]),
        "sleeve_id": str(evaluation["sleeve_id"]),
        "environment": str(evaluation["environment"]),
        "ib_account": str(evaluation["ib_account"]),
        "transition_request_type": str(evaluation["policy"]),
        "transition_status": transition_status,
        "generated_at_utc": generated_at_utc,
        "input_resolution_mode": str(evaluation["input_resolution_mode"]),
        "upstream_stage_ref": dict(evaluation.get("upstream_stage_ref")) if evaluation.get("upstream_stage_ref") else None,
        "authoritative_inputs_used": list(evaluation["authoritative_inputs_used"]),
        "frozen_inputs_used": list(evaluation.get("frozen_inputs_used") or []),
        "invariant_results": list(evaluation["invariant_results"]),
        "validator_kernel_version": VALIDATOR_VERSION,
        "boundary_validator_result": dict(evaluation["validator_results"]["boundary_validator_result"]),
        "family_validator_result": dict(evaluation["validator_results"]["family_validator_result"]),
        "evidence_refs": list(evaluation["evidence_refs"]),
        "admission_decision": dict(admission_decision),
        "certification_decision": dict(certification_decision),
        "supersedes_ref": dict(supersedes_ref) if supersedes_ref is not None else None,
        "superseded_by_ref": None,
        "failure_taxonomy": list(evaluation["blocked_reason_codes"]),
        "explanation_payload": dict(evaluation["explanation_payload"]),
        "blocking_reason_codes": list(evaluation["blocked_reason_codes"]),
        "closure_state": "COMPLETE" if transition_status != "BLOCKED" else "BLOCKED",
        "blocking_codes": list(evaluation["blocked_reason_codes"]),
        "first_blocker_code": str(evaluation["blocked_reason_codes"][0]) if evaluation["blocked_reason_codes"] else "",
        "missing_dependency_artifacts": [],
        "constitutional_dependency_declaration": dependency_declaration,
        "constitutional_lineage": lineage,
    }
    path = _transition_path(
        canonical_truth_root=Path(str(evaluation["canonical_truth_root"])).resolve(),
        target_day=str(evaluation["target_day"]),
        stage_id=str(evaluation["stage_id"]),
        scope_id=str(evaluation["scope_id"]),
        transition_id=transition_id,
    )
    ref = atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=TRANSITION_RECORD_SCHEMA,
        volatile_field_names=("generated_at_utc",),
    )
    return ControlPlaneArtifactRefV1(
        artifact_id=TRANSITION_RECORD_ARTIFACT_ID,
        path=ref.path,
        payload=ref.payload,
        sha256=ref.sha256,
    )


def _build_chain_scope_or_fallback(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str,
    candidate_path: Path,
    canonical_truth_root: Path | str | None,
    truth_sleeves_root: Path | str | None,
) -> Any:
    try:
        return _build_scope(
            repo_root=REPO_ROOT,
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            candidate_path=candidate_path,
            canonical_truth_root=canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root,
        )
    except Exception:
        canonical_root = Path(canonical_truth_root).resolve() if canonical_truth_root is not None else resolve_canonical_truth_root().resolve()
        sleeves_root = Path(truth_sleeves_root).resolve() if truth_sleeves_root is not None else resolve_truth_sleeves_root().resolve()
        sleeve_text = str(sleeve_id).strip().upper()
        env_text = str(environment).strip().upper()
        return SimpleNamespace(
            day_utc=str(day_utc).strip(),
            sleeve_id=sleeve_text,
            environment=env_text,
            ib_account=str(ib_account).strip().upper(),
            operation_type=str(operation_type).strip(),
            canonical_truth_root=canonical_root,
            truth_sleeves_root=sleeves_root,
            execution_truth_root=(sleeves_root / sleeve_text / env_text).resolve(),
            scope_id=candidate_path.name,
        )


def _augment_forbidden_root_boundary_report(
    *,
    report: Mapping[str, Any],
    canonical_truth_root: Path | str | None,
    truth_sleeves_root: Path | str | None,
) -> Dict[str, Any]:
    updated = dict(report)
    errors = list(updated.get("errors") or [])
    forbidden_canonical = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
    forbidden_execution = (REPO_ROOT / "constellation_2" / "runtime" / "truth_sleeves").resolve()
    legacy_root = LEGACY_RUNTIME_ROOT.resolve()
    if canonical_truth_root is not None:
        canonical_root = Path(canonical_truth_root).resolve()
        if canonical_root in {forbidden_canonical, legacy_root} and "CANONICAL_PATH_FORBIDDEN_ROOT" not in errors:
            errors.append("CANONICAL_PATH_FORBIDDEN_ROOT")
    if truth_sleeves_root is not None:
        sleeves_root = Path(truth_sleeves_root).resolve()
        if sleeves_root in {forbidden_execution, legacy_root} and "EXECUTION_PATH_FORBIDDEN_ROOT" not in errors:
            errors.append("EXECUTION_PATH_FORBIDDEN_ROOT")
    updated["errors"] = _normalize_codes(errors)
    updated["ok"] = not updated["errors"]
    return updated


def evaluate_control_plane_transition_v1(
    *,
    stage_id: str,
    policy: str,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    candidate_path: Path | str | None = None,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    upstream_stage_path: Path | str | None = None,
    stage_admission_path: Path | str | None = None,
    frozen_from_transition_record: Path | str | None = None,
    supersedes_transition_record: Path | str | None = None,
    boundary_report_override: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    stage_definition = STAGE_DEFINITION_BY_ID.get(str(stage_id).strip())
    if stage_definition is None:
        raise ControlPlaneTransitionEngineError(f"UNKNOWN_CONTROL_STAGE:{stage_id}")
    if str(policy).strip() not in stage_definition.allowed_policies:
        raise ControlPlaneTransitionEngineError(f"CONTROL_STAGE_POLICY_NOT_ALLOWED:{stage_id}:{policy}")
    candidate = Path(candidate_path).resolve() if candidate_path is not None and str(candidate_path).strip() else None
    scope = _build_scope(
        repo_root=REPO_ROOT,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        candidate_path=candidate,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    identity = _scope_identity(scope)
    boundary_report = dict(
        boundary_report_override
        if boundary_report_override is not None
        else validate_control_plane_boundary_v1(
            repo_root=REPO_ROOT,
            day_utc=scope.day_utc,
            sleeve_id=scope.sleeve_id,
            environment=scope.environment,
            ib_account=scope.ib_account,
            candidate_path=candidate,
            operation_type=scope.operation_type,
            canonical_truth_root=scope.canonical_truth_root,
            truth_sleeves_root=scope.truth_sleeves_root,
        )
    )
    family_report = _run_family_validator(stage_id=stage_definition.stage_id, scope=scope, candidate_path=candidate)
    authoritative_inputs_used = _dedupe_family_refs(
        family_report.get("validated_family_refs") or [],
        family_report.get("validated_current_surfaces") or [],
    )
    evidence_refs = _dedupe_family_refs(family_report.get("validated_artifacts") or [])
    upstream_stage_ref: ControlPlaneArtifactRefV1 | None = None
    policy_errors: list[str] = []
    frozen_inputs_used: list[Dict[str, str]] = []
    frozen_transition_ref: ControlPlaneArtifactRefV1 | None = None
    supersedes_transition_ref: ControlPlaneArtifactRefV1 | None = None
    existing_stage_admission_ref: ControlPlaneArtifactRefV1 | None = None

    if frozen_from_transition_record is not None and str(frozen_from_transition_record).strip():
        frozen_transition_ref = _load_transition_record_ref(scope=scope, path=Path(frozen_from_transition_record).resolve())
        frozen_inputs_used = _dedupe_family_refs(frozen_transition_ref.payload.get("authoritative_inputs_used") or [])
        if upstream_stage_path is None and isinstance(frozen_transition_ref.payload.get("upstream_stage_ref"), Mapping):
            upstream_stage_path = str(frozen_transition_ref.payload["upstream_stage_ref"].get("artifact_path") or "").strip() or None

    if supersedes_transition_record is not None and str(supersedes_transition_record).strip():
        supersedes_transition_ref = _load_transition_record_ref(scope=scope, path=Path(supersedes_transition_record).resolve())
        if upstream_stage_path is None and isinstance(supersedes_transition_ref.payload.get("upstream_stage_ref"), Mapping):
            upstream_stage_path = str(supersedes_transition_ref.payload["upstream_stage_ref"].get("artifact_path") or "").strip() or None

    if stage_definition.upstream_artifact_id:
        if upstream_stage_path is None or not str(upstream_stage_path).strip():
            policy_errors.append("UPSTREAM_STAGE_ADMISSION_REQUIRED")
        else:
            try:
                upstream_stage_ref = _load_upstream_stage_ref(
                    scope=scope,
                    path=Path(upstream_stage_path).resolve(),
                    expected_artifact_id=stage_definition.upstream_artifact_id,
                )
            except Exception as exc:
                policy_errors.append(str(exc))
            else:
                upstream_payload = upstream_stage_ref.payload
                if any(
                    str(upstream_payload.get(field) or "").strip() != str(identity[key]).strip()
                    for field, key in (
                        ("target_day", "target_day"),
                        ("operation_type", "operation_type"),
                        ("sleeve_id", "sleeve_id"),
                        ("environment", "environment"),
                        ("ib_account", "ib_account"),
                    )
                ):
                    policy_errors.append("UPSTREAM_STAGE_ADMISSION_IDENTITY_MISMATCH")

    if policy == POLICY_CERTIFY_ONLY:
        resolved_stage_path = (
            Path(stage_admission_path).resolve()
            if stage_admission_path is not None and str(stage_admission_path).strip()
            else _stage_path(
                stage_definition.artifact_id,
                target_day=scope.day_utc,
                scope_id=scope.scope_id,
                canonical_truth_root=scope.canonical_truth_root,
            )
        )
        try:
            existing_stage_admission_ref = _load_upstream_stage_ref(
                scope=scope,
                path=resolved_stage_path,
                expected_artifact_id=stage_definition.artifact_id,
            )
        except Exception as exc:
            policy_errors.append(str(exc))
        else:
            current_required = _dedupe_family_refs(family_report.get("validated_family_refs") or [])
            current_surfaces = _dedupe_family_refs(family_report.get("validated_current_surfaces") or [])
            payload = existing_stage_admission_ref.payload
            if _comparison_signature(payload.get("required_authoritative_family_refs") or []) != _comparison_signature(current_required):
                policy_errors.append("CERTIFY_ONLY_STAGE_ADMISSION_REQUIRED_FAMILIES_MISMATCH")
            if _comparison_signature(payload.get("effective_current_surface_refs") or []) != _comparison_signature(current_surfaces):
                policy_errors.append("CERTIFY_ONLY_STAGE_ADMISSION_CURRENT_SURFACES_MISMATCH")
            expected_upstream = _ref_from_control_artifact(upstream_stage_ref) if upstream_stage_ref is not None else None
            if payload.get("upstream_stage_admission_ref") != expected_upstream:
                policy_errors.append("CERTIFY_ONLY_STAGE_ADMISSION_UPSTREAM_MISMATCH")

    if policy == POLICY_RECOMPUTE_FROZEN:
        if frozen_transition_ref is None:
            policy_errors.append("RECOMPUTE_FROZEN_TRANSITION_RECORD_REQUIRED")
        else:
            if not _compare_authoritative_inputs(current_refs=authoritative_inputs_used, frozen_refs=frozen_inputs_used):
                policy_errors.append("RECOMPUTE_FROZEN_AUTHORITATIVE_INPUTS_CHANGED")
            frozen_upstream = frozen_transition_ref.payload.get("upstream_stage_ref")
            current_upstream = _ref_from_control_artifact(upstream_stage_ref) if upstream_stage_ref is not None else None
            if frozen_upstream != current_upstream:
                policy_errors.append("RECOMPUTE_FROZEN_UPSTREAM_STAGE_CHANGED")

    if policy == POLICY_SUPERSEDE_FROM_NEW_INPUTS:
        if supersedes_transition_ref is None:
            policy_errors.append("SUPERSEDE_FROM_NEW_INPUTS_TRANSITION_RECORD_REQUIRED")
        else:
            prior_authoritative_inputs = supersedes_transition_ref.payload.get("authoritative_inputs_used") or []
            prior_upstream = supersedes_transition_ref.payload.get("upstream_stage_ref")
            current_upstream = _ref_from_control_artifact(upstream_stage_ref) if upstream_stage_ref is not None else None
            inputs_changed = (
                not _compare_authoritative_inputs(current_refs=authoritative_inputs_used, frozen_refs=prior_authoritative_inputs)
                or prior_upstream != current_upstream
            )
            if not inputs_changed:
                policy_errors.append("SUPERSEDE_FROM_NEW_INPUTS_NOT_REQUIRED")

    blocked_reason_codes = _normalize_codes(
        [
            *(boundary_report.get("errors") or []),
            *(family_report.get("errors") or []),
            *policy_errors,
        ]
    )
    admissible = not blocked_reason_codes
    certifiable = admissible
    if policy == POLICY_CERTIFY_ONLY and existing_stage_admission_ref is None:
        certifiable = False
    persistence_plan = {
        "emit_transition_record": policy in MUTATING_POLICIES,
        "emit_stage_admission": policy in {POLICY_ADMIT_AND_CERTIFY, POLICY_SUPERSEDE_FROM_NEW_INPUTS} and admissible,
        "emit_stage_certification": policy in {POLICY_ADMIT_AND_CERTIFY, POLICY_CERTIFY_ONLY, POLICY_SUPERSEDE_FROM_NEW_INPUTS} and certifiable,
        "emit_current_projection": False,
    }
    evaluation = {
        "stage_id": stage_definition.stage_id,
        "policy": policy,
        "target_day": identity["target_day"],
        "scope_id": identity["scope_id"],
        "operation_type": identity["operation_type"],
        "sleeve_id": identity["sleeve_id"],
        "environment": identity["environment"],
        "ib_account": identity["ib_account"],
        "canonical_truth_root": str(scope.canonical_truth_root),
        "truth_sleeves_root": str(scope.truth_sleeves_root),
        "execution_truth_root": str(scope.execution_truth_root),
        "required_upstream_artifact_id": stage_definition.upstream_artifact_id,
        "upstream_stage_ref": _ref_from_control_artifact(upstream_stage_ref) if upstream_stage_ref is not None else None,
        "authoritative_inputs_used": authoritative_inputs_used,
        "frozen_inputs_used": frozen_inputs_used,
        "invariants_checked": _normalize_codes(
            [
                *(boundary_report.get("invariants_checked") or []),
                *(family_report.get("invariants_checked") or []),
            ]
        ),
        "invariant_failures": blocked_reason_codes,
        "invariant_results": _invariant_results(boundary_report, family_report, policy_errors),
        "validator_results": {
            "boundary_validator_result": boundary_report,
            "family_validator_result": family_report,
        },
        "evidence_refs": evidence_refs,
        "admissible_boolean": admissible,
        "certifiable_boolean": certifiable,
        "blocked_reason_codes": blocked_reason_codes,
        "supersession_determination": {
            "required": policy == POLICY_SUPERSEDE_FROM_NEW_INPUTS,
            "supersedes_ref": _ref_from_control_artifact(supersedes_transition_ref) if supersedes_transition_ref is not None else None,
        },
        "mutation_allowed_boolean": policy in MUTATING_POLICIES,
        "input_resolution_mode": _input_resolution_mode(policy),
        "persistence_plan": persistence_plan,
        "explanation_payload": _build_explanation(
            stage_id=stage_definition.stage_id,
            policy=policy,
            blocked_codes=blocked_reason_codes,
            persistence_plan=persistence_plan,
        ),
        "stage_artifact_id": stage_definition.artifact_id,
        "stage_certification_artifact_id": stage_definition.certification_artifact_id,
        "current_projection_allowed": stage_definition.current_projection_allowed,
        "existing_stage_admission_ref": _ref_from_control_artifact(existing_stage_admission_ref)
        if existing_stage_admission_ref is not None
        else None,
    }
    return evaluation


def run_control_plane_transition_v1(
    *,
    stage_id: str,
    policy: str,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    candidate_path: Path | str | None = None,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    upstream_stage_path: Path | str | None = None,
    stage_admission_path: Path | str | None = None,
    frozen_from_transition_record: Path | str | None = None,
    supersedes_transition_record: Path | str | None = None,
    boundary_report_override: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    evaluation = evaluate_control_plane_transition_v1(
        stage_id=stage_id,
        policy=policy,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        candidate_path=candidate_path,
        operation_type=operation_type,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
        upstream_stage_path=upstream_stage_path,
        stage_admission_path=stage_admission_path,
        frozen_from_transition_record=frozen_from_transition_record,
        supersedes_transition_record=supersedes_transition_record,
        boundary_report_override=boundary_report_override,
    )
    stage_definition = STAGE_DEFINITION_BY_ID[evaluation["stage_id"]]
    stage_ref: ControlPlaneArtifactRefV1 | None = None
    certification_ref: ControlPlaneArtifactRefV1 | None = None
    transition_ref: ControlPlaneArtifactRefV1 | None = None
    supersedes_ref = None
    if supersedes_transition_record is not None and str(supersedes_transition_record).strip():
        scope = _build_scope(
            repo_root=REPO_ROOT,
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            candidate_path=(Path(candidate_path).resolve() if candidate_path is not None and str(candidate_path).strip() else None),
            canonical_truth_root=canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root,
        )
        supersedes_loaded = _load_transition_record_ref(scope=scope, path=Path(supersedes_transition_record).resolve())
        supersedes_ref = _ref_from_control_artifact(supersedes_loaded)

    if evaluation["admissible_boolean"] and policy in {POLICY_ADMIT_AND_CERTIFY, POLICY_SUPERSEDE_FROM_NEW_INPUTS}:
        upstream_ref_payload = evaluation.get("upstream_stage_ref")
        upstream_ref = None
        if isinstance(upstream_ref_payload, Mapping):
            upstream_ref = ControlPlaneArtifactRefV1(
                artifact_id=str(upstream_ref_payload["artifact_id"]),
                path=Path(str(upstream_ref_payload["artifact_path"])).resolve(),
                payload={},
                sha256=str(upstream_ref_payload["artifact_sha256"]),
            )
        stage_ref = write_control_stage_admitted_v1(
            artifact_id=stage_definition.artifact_id,
            stage_id=stage_definition.stage_id,
            family_report=evaluation["validator_results"]["family_validator_result"],
            canonical_truth_root=Path(str(evaluation["canonical_truth_root"])).resolve(),
            upstream_stage_ref=upstream_ref,
        )

    if evaluation["certifiable_boolean"] and policy in {POLICY_ADMIT_AND_CERTIFY, POLICY_CERTIFY_ONLY, POLICY_SUPERSEDE_FROM_NEW_INPUTS}:
        cert_stage_ref = stage_ref
        if cert_stage_ref is None:
            existing = evaluation.get("existing_stage_admission_ref")
            if isinstance(existing, Mapping):
                cert_stage_ref = ControlPlaneArtifactRefV1(
                    artifact_id=str(existing["artifact_id"]),
                    path=Path(str(existing["artifact_path"])).resolve(),
                    payload={},
                    sha256=str(existing["artifact_sha256"]),
                )
        if cert_stage_ref is not None:
            certification_ref = write_control_stage_certification_v1(
                artifact_id=stage_definition.certification_artifact_id,
                stage_id=stage_definition.stage_id,
                family_report=evaluation["validator_results"]["family_validator_result"],
                stage_ref=cert_stage_ref,
                canonical_truth_root=Path(str(evaluation["canonical_truth_root"])).resolve(),
            )

    admission_decision = {
        "status": (
            "EMITTED"
            if stage_ref is not None
            else "REUSED_EXISTING"
            if policy == POLICY_CERTIFY_ONLY and evaluation.get("existing_stage_admission_ref")
            else "BLOCKED"
            if evaluation["blocked_reason_codes"]
            else "SKIPPED"
        ),
        "stage_admission_ref": _ref_from_control_artifact(stage_ref) if stage_ref is not None else evaluation.get("existing_stage_admission_ref"),
    }
    certification_decision = {
        "status": (
            "EMITTED"
            if certification_ref is not None
            else "BLOCKED"
            if evaluation["blocked_reason_codes"] and policy in {POLICY_ADMIT_AND_CERTIFY, POLICY_CERTIFY_ONLY, POLICY_SUPERSEDE_FROM_NEW_INPUTS}
            else "SKIPPED"
        ),
        "stage_certification_ref": _ref_from_control_artifact(certification_ref) if certification_ref is not None else None,
    }
    transition_status = _default_transition_status(
        policy,
        admissible=bool(evaluation["admissible_boolean"]),
        certifiable=bool(evaluation["certifiable_boolean"]),
    )
    if evaluation["mutation_allowed_boolean"]:
        transition_ref = _write_transition_record_v1(
            evaluation=evaluation,
            transition_status=transition_status,
            admission_decision=admission_decision,
            certification_decision=certification_decision,
            supersedes_ref=supersedes_ref,
        )
    return {
        "ok": bool(evaluation["admissible_boolean"]) and bool(evaluation["certifiable_boolean"]),
        "stage_id": stage_definition.stage_id,
        "policy": policy,
        "evaluation": evaluation,
        "transition_record_ref": _ref_from_control_artifact(transition_ref) if transition_ref is not None else None,
        "stage_ref": _ref_from_control_artifact(stage_ref) if stage_ref is not None else None,
        "stage_certification_ref": _ref_from_control_artifact(certification_ref) if certification_ref is not None else None,
        "mutation_performed": transition_ref is not None or stage_ref is not None or certification_ref is not None,
    }


def run_startup_chain_transition_spine_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    candidate_path: Path | str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    policy: str = POLICY_EVALUATE,
) -> Dict[str, Any]:
    if policy not in {POLICY_EVALUATE, POLICY_ADMIT_AND_CERTIFY}:
        raise ControlPlaneTransitionEngineError(f"UNSUPPORTED_STARTUP_CHAIN_POLICY:{policy}")
    candidate = Path(candidate_path).resolve()
    execution_scope = _build_chain_scope_or_fallback(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        candidate_path=candidate,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    boundary_report = _augment_forbidden_root_boundary_report(
        report=validate_control_plane_boundary_v1(
            repo_root=REPO_ROOT,
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            candidate_path=candidate,
            operation_type=operation_type,
            canonical_truth_root=canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root,
        ),
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    stage_results: list[Dict[str, Any]] = []
    stage_refs: list[ControlPlaneArtifactRefV1] = []
    certification_refs: list[ControlPlaneArtifactRefV1] = []
    transition_refs: list[Dict[str, Any]] = []
    upstream_stage_path: str | None = None
    stage_policy = POLICY_ADMIT_AND_CERTIFY if policy == POLICY_ADMIT_AND_CERTIFY else POLICY_EVALUATE
    continue_on_block = policy == POLICY_EVALUATE

    if not boundary_report["errors"]:
        for stage_definition in STAGE_DEFINITIONS:
            result = run_control_plane_transition_v1(
                stage_id=stage_definition.stage_id,
                policy=stage_policy,
                day_utc=execution_scope.day_utc,
                sleeve_id=execution_scope.sleeve_id,
                environment=execution_scope.environment,
                ib_account=execution_scope.ib_account,
                candidate_path=candidate if stage_definition.stage_id == CONTROL_STAGE_EXECUTION_BUILD else None,
                operation_type=execution_scope.operation_type,
                canonical_truth_root=execution_scope.canonical_truth_root,
                truth_sleeves_root=execution_scope.truth_sleeves_root,
                upstream_stage_path=upstream_stage_path,
                boundary_report_override=boundary_report,
            )
            stage_results.append(result)
            if result.get("transition_record_ref") is not None:
                transition_refs.append(dict(result["transition_record_ref"]))
            if result.get("stage_ref") is not None:
                ref = result["stage_ref"]
                stage_refs.append(
                    ControlPlaneArtifactRefV1(
                        artifact_id=str(ref["artifact_id"]),
                        path=Path(str(ref["artifact_path"])).resolve(),
                        payload={},
                        sha256=str(ref["artifact_sha256"]),
                    )
                )
                upstream_stage_path = str(ref["artifact_path"])
            else:
                upstream_stage_path = None
            if result.get("stage_certification_ref") is not None:
                ref = result["stage_certification_ref"]
                certification_refs.append(
                    ControlPlaneArtifactRefV1(
                        artifact_id=str(ref["artifact_id"]),
                        path=Path(str(ref["artifact_path"])).resolve(),
                        payload={},
                        sha256=str(ref["artifact_sha256"]),
                    )
                )
            if not result["ok"] and not continue_on_block:
                break

    family_reports = [
        dict(result["evaluation"]["validator_results"]["family_validator_result"])
        for result in stage_results
    ]
    chain_identity = {
        "target_day": execution_scope.day_utc,
        "scope_id": execution_scope.scope_id,
        "operation_type": execution_scope.operation_type,
        "sleeve_id": execution_scope.sleeve_id,
        "environment": execution_scope.environment,
        "ib_account": execution_scope.ib_account,
    }
    chain_payload = build_startup_chain_certification_payload_v1(
        canonical_truth_root=execution_scope.canonical_truth_root,
        identity=chain_identity,
        boundary_report=boundary_report,
        family_reports=family_reports,
        stage_refs=stage_refs,
        certification_refs=certification_refs,
    )
    if policy == POLICY_ADMIT_AND_CERTIFY:
        chain_ref = write_startup_chain_certification_v1(
            canonical_truth_root=execution_scope.canonical_truth_root,
            identity=chain_identity,
            boundary_report=boundary_report,
            family_reports=family_reports,
            stage_refs=stage_refs,
            certification_refs=certification_refs,
        )
    else:
        chain_ref = _prospective_control_ref(
            artifact_id=STARTUP_CHAIN_CERTIFICATION,
            path=_stage_path(
                STARTUP_CHAIN_CERTIFICATION,
                target_day=str(chain_payload["target_day"]),
                scope_id=str(chain_payload["scope_id"]),
                canonical_truth_root=execution_scope.canonical_truth_root,
            ),
            payload=chain_payload,
        )
    return {
        "ok": bool(chain_payload["certification_status"] == "CERTIFIED"),
        "policy": policy,
        "boundary_validator_result": boundary_report,
        "stage_results": stage_results,
        "family_validator_results": family_reports,
        "stage_transition_record_refs": transition_refs,
        "stage_refs": [_ref_from_control_artifact(ref) for ref in stage_refs],
        "stage_certification_refs": [_ref_from_control_artifact(ref) for ref in certification_refs],
        "startup_chain_certification_payload": chain_payload,
        "startup_chain_certification_ref": _ref_from_control_artifact(chain_ref),
    }
