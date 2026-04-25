from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_FINALIZED,
    ARTIFACT_CLASS_ADMISSION_RESULT,
    ARTIFACT_CLASS_OUTCOME_RECORD,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_governed_dependency_ref_v1,
    resolve_constitutional_artifact_path_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import (
    VALIDATOR_VERSION,
    validate_control_plane_boundary_v1,
    validate_day_activation_family_v1,
    validate_execution_build_family_v1,
    validate_global_context_family_v1,
    validate_session_authority_family_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
STAGE_ADMISSION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_stage_admission.v1.schema.json"
STAGE_CERTIFICATION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_stage_certification.v1.schema.json"
STARTUP_CHAIN_CERTIFICATION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/startup_chain_certification.v1.schema.json"
WRITER_ID = "constellation_2.common.control_plane_stage_admission_v1"

CONTROL_STAGE_DAY_ADMITTED = "control_stage_day_admitted_v1"
CONTROL_STAGE_CONTEXT_ADMITTED = "control_stage_context_admitted_v1"
CONTROL_STAGE_SESSION_ADMITTED = "control_stage_session_admitted_v1"
CONTROL_STAGE_EXECUTION_BUILD_ADMITTED = "control_stage_execution_build_admitted_v1"

CONTROL_STAGE_DAY_CERTIFICATION = "control_stage_day_certification_v1"
CONTROL_STAGE_CONTEXT_CERTIFICATION = "control_stage_context_certification_v1"
CONTROL_STAGE_SESSION_CERTIFICATION = "control_stage_session_certification_v1"
CONTROL_STAGE_EXECUTION_BUILD_CERTIFICATION = "control_stage_execution_build_certification_v1"
STARTUP_CHAIN_CERTIFICATION = "startup_chain_certification_v1"


@dataclass(frozen=True)
class ControlPlaneArtifactRefV1:
    artifact_id: str
    path: Path
    payload: Dict[str, Any]
    sha256: str


STAGE_SPECS: tuple[dict[str, str], ...] = (
    {
        "artifact_id": CONTROL_STAGE_DAY_ADMITTED,
        "certification_artifact_id": CONTROL_STAGE_DAY_CERTIFICATION,
        "stage_id": "CONTROL_STAGE_DAY_ACTIVATION",
        "scope_type": "context",
        "upstream_artifact_id": "",
    },
    {
        "artifact_id": CONTROL_STAGE_CONTEXT_ADMITTED,
        "certification_artifact_id": CONTROL_STAGE_CONTEXT_CERTIFICATION,
        "stage_id": "CONTROL_STAGE_GLOBAL_CONTEXT",
        "scope_type": "context",
        "upstream_artifact_id": CONTROL_STAGE_DAY_ADMITTED,
    },
    {
        "artifact_id": CONTROL_STAGE_SESSION_ADMITTED,
        "certification_artifact_id": CONTROL_STAGE_SESSION_CERTIFICATION,
        "stage_id": "CONTROL_STAGE_SESSION_AUTHORITY",
        "scope_type": "context",
        "upstream_artifact_id": CONTROL_STAGE_CONTEXT_ADMITTED,
    },
    {
        "artifact_id": CONTROL_STAGE_EXECUTION_BUILD_ADMITTED,
        "certification_artifact_id": CONTROL_STAGE_EXECUTION_BUILD_CERTIFICATION,
        "stage_id": "CONTROL_STAGE_EXECUTION_BUILD",
        "scope_type": "submission",
        "upstream_artifact_id": CONTROL_STAGE_SESSION_ADMITTED,
    },
)


class ControlPlaneStageAdmissionError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_codes(values: Sequence[str]) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _scope_value(report: Mapping[str, Any], scope_type: str) -> str:
    if scope_type == "submission":
        return str(report.get("scope_id") or "").strip()
    return str(report.get("scope_id") or "").strip()


def _plain_ref(artifact_id: str, path: Path, sha256: str) -> Dict[str, str]:
    return {
        "artifact_id": str(artifact_id).strip(),
        "artifact_path": str(path.resolve()),
        "artifact_sha256": str(sha256).strip(),
    }


def _ref_from_control_artifact(ref: ControlPlaneArtifactRefV1) -> Dict[str, str]:
    return _plain_ref(ref.artifact_id, ref.path, ref.sha256)


def _prospective_ref(*, artifact_id: str, path: Path, payload: Mapping[str, Any]) -> ControlPlaneArtifactRefV1:
    return ControlPlaneArtifactRefV1(
        artifact_id=artifact_id,
        path=path,
        payload=dict(payload),
        sha256=canonical_hash_for_c2_artifact_v1(dict(payload)),
    )


def _dependency_ref_from_stage(ref: ControlPlaneArtifactRefV1) -> Dict[str, Any]:
    return build_governed_dependency_ref_v1(
        repo_root=REPO_ROOT,
        artifact_id=ref.artifact_id,
        path=ref.path,
        sha256=ref.sha256,
        finality_state=FINALITY_FINALIZED,
    )


def _empty_dependency_declaration(*, artifact_id: str, artifact_class: str) -> Dict[str, Any]:
    return build_artifact_dependency_declaration_v1(
        artifact_type=artifact_id,
        artifact_class=artifact_class,
        authority_id=artifact_id,
        declared_dependency_artifacts=[],
        dependency_refs=[],
    )


def _lineage(
    *,
    artifact_id: str,
    artifact_class: str,
    generated_at_utc: str,
    run_id: str,
    input_refs: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    return build_governed_artifact_lineage_v1(
        artifact_type=artifact_id,
        artifact_version="v1",
        artifact_class=artifact_class,
        authority_id=artifact_id,
        producer_id=WRITER_ID,
        generated_at_utc=generated_at_utc,
        effective_at_utc=generated_at_utc,
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=input_refs,
        policy_snapshot_refs=[],
        code_version="UNKNOWN",
        run_id=run_id,
    )


def _validate_writer(artifact_id: str) -> Dict[str, Any]:
    return assert_constitutional_writer_allowed_v1(REPO_ROOT, artifact_id, WRITER_ID)


def _stage_path(artifact_id: str, *, target_day: str, scope_id: str, canonical_truth_root: Path) -> Path:
    return resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        day_utc=target_day,
        canonical_truth_root=canonical_truth_root,
        extra_variables={"scope_id": scope_id},
    )


def _payload_identity(report: Mapping[str, Any]) -> Dict[str, str]:
    return {
        "target_day": str(report.get("target_day") or "").strip(),
        "scope_id": str(report.get("scope_id") or "").strip(),
        "operation_type": str(report.get("operation_type") or "").strip(),
        "sleeve_id": str(report.get("sleeve_id") or "").strip(),
        "environment": str(report.get("environment") or "").strip(),
        "ib_account": str(report.get("ib_account") or "").strip(),
    }


def _required_family_refs(report: Mapping[str, Any]) -> list[Dict[str, Any]]:
    rows = report.get("validated_family_refs") or []
    normalized: list[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        normalized.append(
            {
                "artifact_id": str(row.get("artifact_id") or "").strip(),
                "path": str(row.get("path") or "").strip(),
                "sha256": str(row.get("sha256") or "").strip(),
                "root_type": str(row.get("root_type") or "").strip(),
            }
        )
    return normalized


def _current_surface_refs(report: Mapping[str, Any]) -> list[Dict[str, Any]]:
    rows = report.get("validated_current_surfaces") or []
    normalized: list[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        normalized.append(
            {
                "artifact_id": str(row.get("artifact_id") or "").strip(),
                "path": str(row.get("path") or "").strip(),
                "sha256": str(row.get("sha256") or "").strip(),
                "root_type": str(row.get("root_type") or "").strip(),
            }
        )
    return normalized


def _immutable_evidence_refs(report: Mapping[str, Any]) -> list[Dict[str, Any]]:
    rows = report.get("validated_artifacts") or []
    normalized: list[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        normalized.append(
            {
                "artifact_id": str(row.get("artifact_id") or "").strip(),
                "path": str(row.get("path") or "").strip(),
                "sha256": str(row.get("sha256") or "").strip(),
                "root_type": str(row.get("root_type") or "").strip(),
            }
        )
    return normalized


def build_control_stage_admitted_payload_v1(
    *,
    artifact_id: str,
    stage_id: str,
    family_report: Mapping[str, Any],
    upstream_stage_ref: ControlPlaneArtifactRefV1 | None = None,
) -> Dict[str, Any]:
    if not bool(family_report.get("ok")):
        raise ControlPlaneStageAdmissionError(f"STAGE_FAMILY_VALIDATION_FAILED:{artifact_id}")
    stage_spec = next((row for row in STAGE_SPECS if row["artifact_id"] == artifact_id), None)
    if stage_spec is None:
        raise ControlPlaneStageAdmissionError(f"UNKNOWN_STAGE_ARTIFACT:{artifact_id}")
    required_upstream_artifact_id = str(stage_spec.get("upstream_artifact_id") or "").strip()
    if required_upstream_artifact_id and upstream_stage_ref is None:
        raise ControlPlaneStageAdmissionError(f"UPSTREAM_STAGE_REQUIRED:{artifact_id}")
    if required_upstream_artifact_id and upstream_stage_ref is not None and upstream_stage_ref.artifact_id != required_upstream_artifact_id:
        raise ControlPlaneStageAdmissionError(
            f"UPSTREAM_STAGE_MISMATCH:{artifact_id}:expected={required_upstream_artifact_id}:actual={upstream_stage_ref.artifact_id}"
        )
    contract = _validate_writer(artifact_id)
    identity = _payload_identity(family_report)
    generated_at_utc = _utc_now()
    dependency_artifacts = [upstream_stage_ref.artifact_id] if upstream_stage_ref is not None else []
    dependency_refs = [_dependency_ref_from_stage(upstream_stage_ref)] if upstream_stage_ref is not None else []
    dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type=artifact_id,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=artifact_id,
        declared_dependency_artifacts=dependency_artifacts,
        dependency_refs=dependency_refs,
    )
    lineage = _lineage(
        artifact_id=artifact_id,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        generated_at_utc=generated_at_utc,
        run_id=identity["scope_id"],
        input_refs=dependency_refs,
    )
    payload = {
        "schema_id": "control_stage_admission",
        "schema_version": "v1",
        "stage_artifact_id": artifact_id,
        "stage_id": stage_id,
        **identity,
        "admission_status": "ADMITTED",
        "admitted_at_utc": generated_at_utc,
        "governing_validator_ids": [str(family_report.get("validator_id") or "").strip()],
        "validator_version": str(family_report.get("validator_version") or VALIDATOR_VERSION),
        "upstream_stage_admission_ref": _ref_from_control_artifact(upstream_stage_ref) if upstream_stage_ref is not None else None,
        "required_authoritative_family_refs": _required_family_refs(family_report),
        "effective_current_surface_refs": _current_surface_refs(family_report),
        "immutable_evidence_refs": _immutable_evidence_refs(family_report),
        "invariants_checked": list(family_report.get("invariants_checked") or []),
        "blocking_reason_codes": [],
        "closure_state": "COMPLETE",
        "blocking_codes": [],
        "first_blocker_code": "",
        "missing_dependency_artifacts": [],
        "constitutional_dependency_declaration": dependency_declaration,
        "constitutional_lineage": lineage,
    }
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        payload=payload,
        required_finality_states=[FINALITY_FINALIZED],
    )
    return payload


def write_control_stage_admitted_v1(
    *,
    artifact_id: str,
    stage_id: str,
    family_report: Mapping[str, Any],
    canonical_truth_root: Path,
    upstream_stage_ref: ControlPlaneArtifactRefV1 | None = None,
) -> ControlPlaneArtifactRefV1:
    payload = build_control_stage_admitted_payload_v1(
        artifact_id=artifact_id,
        stage_id=stage_id,
        family_report=family_report,
        upstream_stage_ref=upstream_stage_ref,
    )
    path = _stage_path(
        artifact_id,
        target_day=str(payload["target_day"]),
        scope_id=str(payload["scope_id"]),
        canonical_truth_root=canonical_truth_root,
    )
    ref = atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=STAGE_ADMISSION_SCHEMA,
        volatile_field_names=("admitted_at_utc",),
    )
    return ControlPlaneArtifactRefV1(artifact_id=artifact_id, path=ref.path, payload=ref.payload, sha256=ref.sha256)


def build_control_stage_certification_payload_v1(
    *,
    artifact_id: str,
    stage_id: str,
    family_report: Mapping[str, Any],
    stage_ref: ControlPlaneArtifactRefV1,
) -> Dict[str, Any]:
    if not bool(family_report.get("ok")):
        raise ControlPlaneStageAdmissionError(f"STAGE_CERTIFICATION_VALIDATION_FAILED:{artifact_id}")
    contract = _validate_writer(artifact_id)
    generated_at_utc = _utc_now()
    dependency_ref = _dependency_ref_from_stage(stage_ref)
    dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type=artifact_id,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=artifact_id,
        declared_dependency_artifacts=[stage_ref.artifact_id],
        dependency_refs=[dependency_ref],
    )
    lineage = _lineage(
        artifact_id=artifact_id,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        generated_at_utc=generated_at_utc,
        run_id=str(family_report.get("scope_id") or ""),
        input_refs=[dependency_ref],
    )
    payload = {
        "schema_id": "control_stage_certification",
        "schema_version": "v1",
        "certification_artifact_id": artifact_id,
        "stage_id": stage_id,
        "target_day": str(family_report.get("target_day") or ""),
        "scope_id": str(family_report.get("scope_id") or ""),
        "operation_type": str(family_report.get("operation_type") or ""),
        "sleeve_id": str(family_report.get("sleeve_id") or ""),
        "environment": str(family_report.get("environment") or ""),
        "ib_account": str(family_report.get("ib_account") or ""),
        "certified_at_utc": generated_at_utc,
        "certification_status": "CERTIFIED",
        "validator_id": str(family_report.get("validator_id") or ""),
        "validator_version": str(family_report.get("validator_version") or VALIDATOR_VERSION),
        "stage_admission_ref": _ref_from_control_artifact(stage_ref),
        "validator_result": {
            "validator_id": str(family_report.get("validator_id") or ""),
            "validator_version": str(family_report.get("validator_version") or VALIDATOR_VERSION),
            "ok": bool(family_report.get("ok")),
            "errors": list(family_report.get("errors") or []),
            "warnings": list(family_report.get("warnings") or []),
            "invariants_checked": list(family_report.get("invariants_checked") or []),
            "validated_artifacts": list(family_report.get("validated_artifacts") or []),
            "validated_current_surfaces": list(family_report.get("validated_current_surfaces") or []),
            "forbidden_path_hits": list(family_report.get("forbidden_path_hits") or []),
        },
        "evidence_refs": _immutable_evidence_refs(family_report),
        "blocking_reason_codes": [],
        "closure_state": "COMPLETE",
        "blocking_codes": [],
        "first_blocker_code": "",
        "missing_dependency_artifacts": [],
        "constitutional_dependency_declaration": dependency_declaration,
        "constitutional_lineage": lineage,
    }
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        payload=payload,
        required_finality_states=[FINALITY_FINALIZED],
    )
    return payload


def write_control_stage_certification_v1(
    *,
    artifact_id: str,
    stage_id: str,
    family_report: Mapping[str, Any],
    stage_ref: ControlPlaneArtifactRefV1,
    canonical_truth_root: Path,
) -> ControlPlaneArtifactRefV1:
    payload = build_control_stage_certification_payload_v1(
        artifact_id=artifact_id,
        stage_id=stage_id,
        family_report=family_report,
        stage_ref=stage_ref,
    )
    path = _stage_path(
        artifact_id,
        target_day=str(payload["target_day"]),
        scope_id=str(payload["scope_id"]),
        canonical_truth_root=canonical_truth_root,
    )
    ref = atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=STAGE_CERTIFICATION_SCHEMA,
        volatile_field_names=("certified_at_utc",),
    )
    return ControlPlaneArtifactRefV1(artifact_id=artifact_id, path=ref.path, payload=ref.payload, sha256=ref.sha256)


def build_startup_chain_certification_payload_v1(
    *,
    canonical_truth_root: Path,
    identity: Mapping[str, Any],
    boundary_report: Mapping[str, Any],
    family_reports: Sequence[Mapping[str, Any]],
    stage_refs: Sequence[ControlPlaneArtifactRefV1],
    certification_refs: Sequence[ControlPlaneArtifactRefV1],
) -> Dict[str, Any]:
    _validate_writer(STARTUP_CHAIN_CERTIFICATION)
    ok = bool(boundary_report.get("ok")) and all(bool(report.get("ok")) for report in family_reports)
    blocking_codes: list[str] = []
    for row in [boundary_report, *family_reports]:
        blocking_codes.extend([str(item) for item in row.get("errors") or []])
    generated_at_utc = _utc_now()
    payload = {
        "schema_id": "startup_chain_certification",
        "schema_version": "v1",
        "target_day": str(identity.get("target_day") or ""),
        "scope_id": str(identity.get("scope_id") or ""),
        "operation_type": str(identity.get("operation_type") or ""),
        "sleeve_id": str(identity.get("sleeve_id") or ""),
        "environment": str(identity.get("environment") or ""),
        "ib_account": str(identity.get("ib_account") or ""),
        "certified_at_utc": generated_at_utc,
        "certification_status": "CERTIFIED" if ok else "FAILED",
        "boundary_validator_result": dict(boundary_report),
        "family_validator_results": [dict(report) for report in family_reports],
        "stage_admission_refs": [_ref_from_control_artifact(ref) for ref in stage_refs],
        "stage_certification_refs": [_ref_from_control_artifact(ref) for ref in certification_refs],
        "blocking_reason_codes": _normalize_codes(blocking_codes),
        "closure_state": "COMPLETE" if ok else "BLOCKED",
        "blocking_codes": _normalize_codes(blocking_codes),
        "first_blocker_code": _normalize_codes(blocking_codes)[0] if blocking_codes else "",
        "missing_dependency_artifacts": [],
        "constitutional_dependency_declaration": _empty_dependency_declaration(
            artifact_id=STARTUP_CHAIN_CERTIFICATION,
            artifact_class=ARTIFACT_CLASS_OUTCOME_RECORD,
        ),
        "constitutional_lineage": _lineage(
            artifact_id=STARTUP_CHAIN_CERTIFICATION,
            artifact_class=ARTIFACT_CLASS_OUTCOME_RECORD,
            generated_at_utc=generated_at_utc,
            run_id=str(identity.get("scope_id") or ""),
            input_refs=[],
        ),
    }
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=STARTUP_CHAIN_CERTIFICATION,
        payload=payload,
        required_finality_states=[FINALITY_FINALIZED],
    )
    return payload


def write_startup_chain_certification_v1(
    *,
    canonical_truth_root: Path,
    identity: Mapping[str, Any],
    boundary_report: Mapping[str, Any],
    family_reports: Sequence[Mapping[str, Any]],
    stage_refs: Sequence[ControlPlaneArtifactRefV1],
    certification_refs: Sequence[ControlPlaneArtifactRefV1],
) -> ControlPlaneArtifactRefV1:
    payload = build_startup_chain_certification_payload_v1(
        canonical_truth_root=canonical_truth_root,
        identity=identity,
        boundary_report=boundary_report,
        family_reports=family_reports,
        stage_refs=stage_refs,
        certification_refs=certification_refs,
    )
    path = _stage_path(
        STARTUP_CHAIN_CERTIFICATION,
        target_day=str(payload["target_day"]),
        scope_id=str(payload["scope_id"]),
        canonical_truth_root=canonical_truth_root,
    )
    ref = atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=STARTUP_CHAIN_CERTIFICATION_SCHEMA,
        volatile_field_names=("certified_at_utc",),
    )
    return ControlPlaneArtifactRefV1(artifact_id=STARTUP_CHAIN_CERTIFICATION, path=ref.path, payload=ref.payload, sha256=ref.sha256)


def certify_startup_chain_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    candidate_path: Path | str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    emit_artifacts: bool = False,
) -> Dict[str, Any]:
    from constellation_2.common.control_plane_transition_engine_v1 import (
        run_startup_chain_transition_spine_v1,
    )

    result = run_startup_chain_transition_spine_v1(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        candidate_path=candidate_path,
        operation_type=operation_type,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
        policy="admit_and_certify" if emit_artifacts else "evaluate",
    )
    stage_payloads: list[Dict[str, Any]] = []
    stage_certification_payloads: list[Dict[str, Any]] = []
    for stage_result in result["stage_results"]:
        evaluation = dict(stage_result["evaluation"])
        family_report = dict(evaluation["validator_results"]["family_validator_result"])
        if stage_result.get("stage_ref") is not None:
            stage_payloads.append(
                build_control_stage_admitted_payload_v1(
                    artifact_id=str(evaluation["stage_artifact_id"]),
                    stage_id=str(evaluation["stage_id"]),
                    family_report=family_report,
                    upstream_stage_ref=(
                        ControlPlaneArtifactRefV1(
                            artifact_id=str(evaluation["upstream_stage_ref"]["artifact_id"]),
                            path=Path(str(evaluation["upstream_stage_ref"]["artifact_path"])).resolve(),
                            payload={},
                            sha256=str(evaluation["upstream_stage_ref"]["artifact_sha256"]),
                        )
                        if isinstance(evaluation.get("upstream_stage_ref"), Mapping)
                        else None
                    ),
                )
            )
        else:
            stage_payloads.append(
                {
                    "stage_artifact_id": str(evaluation["stage_artifact_id"]),
                    "stage_id": str(evaluation["stage_id"]),
                    "target_day": str(evaluation["target_day"]),
                    "scope_id": str(evaluation["scope_id"]),
                    "operation_type": str(evaluation["operation_type"]),
                    "sleeve_id": str(evaluation["sleeve_id"]),
                    "environment": str(evaluation["environment"]),
                    "ib_account": str(evaluation["ib_account"]),
                    "admission_status": "VALIDATION_ONLY",
                    "governing_validator_ids": [str(family_report.get("validator_id") or "").strip()],
                    "validator_version": str(family_report.get("validator_version") or VALIDATOR_VERSION),
                    "required_authoritative_family_refs": _required_family_refs(family_report),
                    "effective_current_surface_refs": _current_surface_refs(family_report),
                    "immutable_evidence_refs": _immutable_evidence_refs(family_report),
                    "invariants_checked": list(family_report.get("invariants_checked") or []),
                }
            )
        if stage_result.get("stage_certification_ref") is not None:
            stage_certification_payloads.append(
                {
                    "certification_artifact_id": str(evaluation["stage_certification_artifact_id"]),
                    "stage_id": str(evaluation["stage_id"]),
                    "target_day": str(evaluation["target_day"]),
                    "scope_id": str(evaluation["scope_id"]),
                    "operation_type": str(evaluation["operation_type"]),
                    "sleeve_id": str(evaluation["sleeve_id"]),
                    "environment": str(evaluation["environment"]),
                    "ib_account": str(evaluation["ib_account"]),
                    "certification_status": "CERTIFIED",
                    "validator_id": str(family_report.get("validator_id") or ""),
                    "validator_version": str(family_report.get("validator_version") or VALIDATOR_VERSION),
                }
            )
    return {
        "ok": bool(result["ok"]),
        "boundary_validator_result": dict(result["boundary_validator_result"]),
        "family_validator_results": [dict(row) for row in result["family_validator_results"]],
        "stage_payloads": stage_payloads,
        "stage_refs": list(result["stage_refs"]),
        "stage_transition_record_refs": list(result["stage_transition_record_refs"]),
        "stage_certification_payloads": stage_certification_payloads,
        "stage_certification_refs": list(result["stage_certification_refs"]),
        "startup_chain_certification_payload": dict(result["startup_chain_certification_payload"]),
        "startup_chain_certification_ref": dict(result["startup_chain_certification_ref"]),
        "emit_artifacts": bool(emit_artifacts),
    }
