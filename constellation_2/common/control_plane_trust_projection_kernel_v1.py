from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence

from constellation_2.common.constitutional_runtime_v1 import (
    assert_constitutional_writer_allowed_v1,
    resolve_constitutional_artifact_path_v1,
    validate_read_model_payload_v1,
)
from constellation_2.common.control_plane_advisory_truth_binding_v1 import (
    evaluate_advisory_truth_binding_v1,
)
from constellation_2.common.control_plane_explanation_mapping_v1 import (
    EXPLANATION_MAPPING_VERSION,
    map_control_plane_explanations_v1,
)
from constellation_2.common.control_plane_stage_admission_v1 import (
    CONTROL_STAGE_CONTEXT_ADMITTED,
    CONTROL_STAGE_CONTEXT_CERTIFICATION,
    CONTROL_STAGE_DAY_ADMITTED,
    CONTROL_STAGE_DAY_CERTIFICATION,
    CONTROL_STAGE_EXECUTION_BUILD_ADMITTED,
    CONTROL_STAGE_EXECUTION_BUILD_CERTIFICATION,
    CONTROL_STAGE_SESSION_ADMITTED,
    CONTROL_STAGE_SESSION_CERTIFICATION,
    STARTUP_CHAIN_CERTIFICATION,
    _stage_path,
)
from constellation_2.common.control_plane_stage_definitions_v1 import (
    CONTROL_STAGE_CONTEXT,
    CONTROL_STAGE_DAY,
    CONTROL_STAGE_EXECUTION_BUILD,
    CONTROL_STAGE_SESSION,
    STAGE_DEFINITIONS,
)
from constellation_2.common.control_plane_transition_engine_v1 import (
    TRANSITION_RECORD_ARTIFACT_ID,
)
from constellation_2.common.control_plane_validation_kernel_v1 import (
    _build_scope,
    _check_path_boundary,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_validated_surface_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
WRITER_ID = "constellation_2.common.control_plane_trust_projection_kernel_v1"
PROJECTION_VERSION = WRITER_ID

CONTROL_STAGE_ADMISSION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_stage_admission.v1.schema.json"
CONTROL_STAGE_CERTIFICATION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_stage_certification.v1.schema.json"
STARTUP_CHAIN_CERTIFICATION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/startup_chain_certification.v1.schema.json"
TRANSITION_RECORD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_stage_transition_record.v1.schema.json"

OPERATOR_STATUS_ARTIFACT_ID = "control_plane_operator_status_v1"
OPERATOR_STATUS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_plane_operator_status.v1.schema.json"
BLOCKED_VIEW_ARTIFACT_ID = "control_plane_blocked_transition_view_v1"
BLOCKED_VIEW_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_plane_blocked_transition_view.v1.schema.json"
ADVISORY_BINDING_ARTIFACT_ID = "advisory_truth_binding_status_v1"
ADVISORY_BINDING_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/advisory_truth_binding_status.v1.schema.json"
TIMELINE_ARTIFACT_ID = "transition_timeline_projection_v1"
TIMELINE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/transition_timeline_projection.v1.schema.json"

STAGE_ARTIFACT_BY_ID = {
    CONTROL_STAGE_DAY: CONTROL_STAGE_DAY_ADMITTED,
    CONTROL_STAGE_CONTEXT: CONTROL_STAGE_CONTEXT_ADMITTED,
    CONTROL_STAGE_SESSION: CONTROL_STAGE_SESSION_ADMITTED,
    CONTROL_STAGE_EXECUTION_BUILD: CONTROL_STAGE_EXECUTION_BUILD_ADMITTED,
}
STAGE_CERTIFICATION_BY_ID = {
    CONTROL_STAGE_DAY: CONTROL_STAGE_DAY_CERTIFICATION,
    CONTROL_STAGE_CONTEXT: CONTROL_STAGE_CONTEXT_CERTIFICATION,
    CONTROL_STAGE_SESSION: CONTROL_STAGE_SESSION_CERTIFICATION,
    CONTROL_STAGE_EXECUTION_BUILD: CONTROL_STAGE_EXECUTION_BUILD_CERTIFICATION,
}


class ControlPlaneTrustProjectionError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sorted_unique_codes(values: Iterable[str]) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _plain_ref(*, artifact_id: str, path: Path, sha256: str) -> Dict[str, str]:
    return {
        "artifact_id": str(artifact_id).strip(),
        "artifact_path": str(path.resolve()),
        "artifact_sha256": str(sha256).strip(),
    }


def _plain_ref_from_surface(surface: SurfaceRefV1, artifact_id: str) -> Dict[str, str]:
    return _plain_ref(artifact_id=artifact_id, path=surface.path, sha256=surface.sha256)


def _scope(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str,
    canonical_truth_root: Path | str | None,
    truth_sleeves_root: Path | str | None,
) -> Any:
    return _build_scope(
        repo_root=REPO_ROOT,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        candidate_path=None,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )


def _read_control_surface(
    *,
    scope: Any,
    path: Path,
    schema_relpath: str,
    expected_root_type: str = "canonical_truth_root",
    reject_derived: bool = True,
) -> SurfaceRefV1:
    report: Dict[str, Any] = {"errors": [], "forbidden_path_hits": []}
    _check_path_boundary(
        report,
        scope=scope,
        label="TRUST_PROJECTION_INPUT",
        path=path,
        expected_root_type=expected_root_type,
        reject_derived=reject_derived,
    )
    if report["errors"]:
        raise ControlPlaneTrustProjectionError(str(report["errors"][0]))
    return read_validated_surface_v1(path=path, schema_relpath=schema_relpath)


def _transition_root(*, canonical_truth_root: Path, day_utc: str, stage_id: str, scope_id: str) -> Path:
    return (
        canonical_truth_root
        / "reports"
        / TRANSITION_RECORD_ARTIFACT_ID
        / day_utc
        / stage_id
        / scope_id
    ).resolve()


def _transition_day_root(*, canonical_truth_root: Path, day_utc: str, stage_id: str) -> Path:
    return (
        canonical_truth_root
        / "reports"
        / TRANSITION_RECORD_ARTIFACT_ID
        / day_utc
        / stage_id
    ).resolve()


def _matches_scope_identity(payload: Mapping[str, Any], scope: Any) -> bool:
    return all(
        str(payload.get(field) or "").strip() == str(getattr(scope, attr)).strip()
        for field, attr in (
            ("target_day", "day_utc"),
            ("operation_type", "operation_type"),
            ("sleeve_id", "sleeve_id"),
            ("environment", "environment"),
            ("ib_account", "ib_account"),
        )
    )


def _load_transition_records(*, scope: Any) -> list[SurfaceRefV1]:
    refs: list[SurfaceRefV1] = []
    for stage_definition in STAGE_DEFINITIONS:
        stage_root = _transition_day_root(
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            stage_id=stage_definition.stage_id,
        )
        if not stage_root.exists():
            continue
        for path in sorted(stage_root.glob("*/*/control_stage_transition_record.v1.json")):
            ref = _read_control_surface(
                scope=scope,
                path=path,
                schema_relpath=TRANSITION_RECORD_SCHEMA,
            )
            if _matches_scope_identity(ref.payload, scope):
                refs.append(ref)
    refs.sort(
        key=lambda ref: (
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.payload.get("transition_id") or ""),
            str(ref.path),
        )
    )
    return refs


def _superseded_by_map(transitions: Sequence[SurfaceRefV1]) -> dict[str, Dict[str, str]]:
    mapping: dict[str, Dict[str, str]] = {}
    for ref in transitions:
        supersedes = ref.payload.get("supersedes_ref")
        if not isinstance(supersedes, Mapping):
            continue
        prior_path = str(supersedes.get("artifact_path") or "").strip()
        prior_id = str(supersedes.get("artifact_id") or "").strip()
        prior_sha = str(supersedes.get("artifact_sha256") or "").strip()
        if not prior_path or not prior_id or not prior_sha:
            continue
        mapping[prior_path] = _plain_ref(
            artifact_id=str(ref.payload.get("transition_record_artifact_id") or TRANSITION_RECORD_ARTIFACT_ID),
            path=ref.path,
            sha256=ref.sha256,
        )
    return mapping


def _latest_transition_by_stage(transitions: Sequence[SurfaceRefV1]) -> dict[str, SurfaceRefV1]:
    latest: dict[str, SurfaceRefV1] = {}
    for ref in transitions:
        latest[str(ref.payload.get("stage_id") or "").strip()] = ref
    return latest


def _load_stage_ref_if_present(*, scope: Any, artifact_id: str) -> SurfaceRefV1 | None:
    path = _stage_path(
        artifact_id,
        target_day=scope.day_utc,
        scope_id=scope.scope_id,
        canonical_truth_root=scope.canonical_truth_root,
    )
    if not path.exists():
        return None
    schema = CONTROL_STAGE_ADMISSION_SCHEMA if artifact_id.endswith("_admitted_v1") else CONTROL_STAGE_CERTIFICATION_SCHEMA
    return _read_control_surface(scope=scope, path=path, schema_relpath=schema)


def _load_chain_certification_if_present(*, scope: Any) -> SurfaceRefV1 | None:
    chain_root = (scope.canonical_truth_root / "reports" / STARTUP_CHAIN_CERTIFICATION / scope.day_utc).resolve()
    if not chain_root.exists():
        return None
    matches: list[SurfaceRefV1] = []
    for path in sorted(chain_root.glob("*/startup_chain_certification.v1.json")):
        ref = _read_control_surface(scope=scope, path=path, schema_relpath=STARTUP_CHAIN_CERTIFICATION_SCHEMA)
        if _matches_scope_identity(ref.payload, scope):
            matches.append(ref)
    if not matches:
        return None
    matches.sort(key=lambda ref: (str(ref.payload.get("certified_at_utc") or ""), str(ref.path)))
    return matches[-1]


def _load_ref_from_plain(*, scope: Any, row: Mapping[str, Any], schema_relpath: str) -> SurfaceRefV1:
    path = Path(str(row.get("artifact_path") or "")).resolve()
    if not str(row.get("artifact_id") or "").strip():
        raise ControlPlaneTrustProjectionError("TRUST_PROJECTION_REF_ARTIFACT_ID_MISSING")
    return _read_control_surface(scope=scope, path=path, schema_relpath=schema_relpath)


def _semantic_event(
    *,
    event_type: str,
    authority_label: str,
    freshness_state: str,
    governing_refs: Sequence[Mapping[str, Any]],
    projection_version: str,
    taxonomy: Sequence[str],
    transition: SurfaceRefV1 | None = None,
    projection_id: str = "",
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "event_type": event_type,
        "authority_label": authority_label,
        "governing_refs": [dict(row) for row in governing_refs],
        "taxonomy": list(taxonomy),
        "freshness_state": freshness_state,
        "projection_version": projection_version,
    }
    if projection_id:
        payload["projection_id"] = projection_id
    if transition is not None:
        payload["transition_id"] = str(transition.payload.get("transition_id") or "")
        payload["stage_id"] = str(transition.payload.get("stage_id") or "")
        payload["policy"] = str(transition.payload.get("transition_request_type") or "")
        payload["status"] = str(transition.payload.get("transition_status") or "")
    return payload


def _write_projection(
    *,
    artifact_id: str,
    payload: Mapping[str, Any],
    schema_relpath: str,
    canonical_truth_root: Path,
    day_utc: str,
    extra_variables: Mapping[str, Any] | None = None,
) -> SurfaceRefV1:
    assert_constitutional_writer_allowed_v1(REPO_ROOT, artifact_id, WRITER_ID)
    validation = validate_read_model_payload_v1(payload)
    if not bool(validation.get("ok")):
        raise ControlPlaneTrustProjectionError(f"INVALID_TRUST_READ_MODEL:{validation.get('errors')}")
    path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        day_utc=day_utc,
        canonical_truth_root=canonical_truth_root,
        extra_variables=extra_variables or {},
    )
    return atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=dict(payload),
        schema_relpath=schema_relpath,
        volatile_field_names=("generated_at_utc",),
    )


def _projection_response(
    *,
    artifact_id: str,
    ok: bool,
    payload: Mapping[str, Any],
    emitted_ref: SurfaceRefV1 | None,
) -> Dict[str, Any]:
    return {
        "ok": ok,
        "artifact_id": artifact_id,
        "projection": dict(payload),
        "artifact_ref": _plain_ref_from_surface(emitted_ref, artifact_id) if emitted_ref is not None else None,
    }


def materialize_transition_timeline_projection_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    emit_artifact: bool = False,
) -> Dict[str, Any]:
    scope = _scope(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    transitions = _load_transition_records(scope=scope)
    superseded_map = _superseded_by_map(transitions)
    timeline_rows: list[Dict[str, Any]] = []
    transition_refs = [_plain_ref_from_surface(ref, TRANSITION_RECORD_ARTIFACT_ID) for ref in transitions]
    stage_refs: list[Dict[str, str]] = []
    certification_refs: list[Dict[str, str]] = []
    semantic_events: list[Dict[str, Any]] = []
    for ref in transitions:
        admission_ref = ref.payload.get("admission_decision", {}).get("stage_admission_ref")
        certification_ref = ref.payload.get("certification_decision", {}).get("stage_certification_ref")
        if isinstance(admission_ref, Mapping):
            stage_refs.append(dict(admission_ref))
        if isinstance(certification_ref, Mapping):
            certification_refs.append(dict(certification_ref))
        freshness_state = "superseded" if str(ref.path.resolve()) in superseded_map else "fresh"
        timeline_rows.append(
            {
                "transition_id": str(ref.payload.get("transition_id") or ""),
                "stage_id": str(ref.payload.get("stage_id") or ""),
                "policy": str(ref.payload.get("transition_request_type") or ""),
                "transition_status": str(ref.payload.get("transition_status") or ""),
                "generated_at_utc": str(ref.payload.get("generated_at_utc") or ""),
                "freshness_state": freshness_state,
                "blocked_reason_codes": list(ref.payload.get("blocking_reason_codes") or []),
                "supersedes_ref": dict(ref.payload.get("supersedes_ref")) if isinstance(ref.payload.get("supersedes_ref"), Mapping) else None,
                "superseded_by_ref": superseded_map.get(str(ref.path.resolve())),
                "stage_ref": dict(admission_ref) if isinstance(admission_ref, Mapping) else None,
                "stage_certification_ref": dict(certification_ref) if isinstance(certification_ref, Mapping) else None,
                "transition_record_ref": _plain_ref_from_surface(ref, TRANSITION_RECORD_ARTIFACT_ID),
            }
        )
        semantic_events.append(
            _semantic_event(
                event_type="transition_start",
                authority_label="governed_derived",
                freshness_state=freshness_state,
                governing_refs=[_plain_ref_from_surface(ref, TRANSITION_RECORD_ARTIFACT_ID)],
                projection_version=PROJECTION_VERSION,
                taxonomy=list(ref.payload.get("blocking_reason_codes") or []),
                transition=ref,
            )
        )
        semantic_events.append(
            _semantic_event(
                event_type="transition_completion" if str(ref.payload.get("transition_status") or "") != "BLOCKED" else "blocked_transition",
                authority_label="governed_derived",
                freshness_state=freshness_state,
                governing_refs=[_plain_ref_from_surface(ref, TRANSITION_RECORD_ARTIFACT_ID)],
                projection_version=PROJECTION_VERSION,
                taxonomy=list(ref.payload.get("blocking_reason_codes") or []),
                transition=ref,
            )
        )
        if isinstance(ref.payload.get("supersedes_ref"), Mapping):
            semantic_events.append(
                _semantic_event(
                    event_type="supersession",
                    authority_label="governed_derived",
                    freshness_state=freshness_state,
                    governing_refs=[_plain_ref_from_surface(ref, TRANSITION_RECORD_ARTIFACT_ID)],
                    projection_version=PROJECTION_VERSION,
                    taxonomy=["SUPERSESSION"],
                    transition=ref,
                )
            )
    projection_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": TIMELINE_ARTIFACT_ID,
            "scope_id": scope.scope_id,
            "transition_refs": transition_refs,
        }
    )
    payload = {
        "schema_id": "transition_timeline_projection",
        "schema_version": "v1",
        "artifact_id": TIMELINE_ARTIFACT_ID,
        "surface_kind": "projection",
        "projection_id": projection_id,
        "projection_version": PROJECTION_VERSION,
        "generated_at_utc": _utc_now(),
        "authority_label": "governed_derived",
        "target_day": scope.day_utc,
        "scope_id": scope.scope_id,
        "operation_type": scope.operation_type,
        "sleeve_id": scope.sleeve_id,
        "environment": scope.environment,
        "ib_account": scope.ib_account,
        "freshness_state": "fresh" if transitions else "unknown",
        "timeline_rows": timeline_rows,
        "governing_stage_refs": sorted(stage_refs, key=lambda row: (row["artifact_id"], row["artifact_path"], row["artifact_sha256"])),
        "governing_transition_refs": transition_refs,
        "governing_certification_refs": sorted(
            certification_refs, key=lambda row: (row["artifact_id"], row["artifact_path"], row["artifact_sha256"])
        ),
        "evidence_refs": transition_refs,
        "semantic_events": semantic_events,
    }
    emitted_ref = None
    if emit_artifact:
        emitted_ref = _write_projection(
            artifact_id=TIMELINE_ARTIFACT_ID,
            payload=payload,
            schema_relpath=TIMELINE_SCHEMA,
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            extra_variables={"scope_id": scope.scope_id},
        )
    return _projection_response(artifact_id=TIMELINE_ARTIFACT_ID, ok=bool(transitions), payload=payload, emitted_ref=emitted_ref)


def materialize_control_plane_blocked_transition_view_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    transition_record_path: Path | str | None = None,
    emit_artifact: bool = False,
) -> Dict[str, Any]:
    scope = _scope(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    transitions = _load_transition_records(scope=scope)
    superseded_map = _superseded_by_map(transitions)
    blocked_ref: SurfaceRefV1 | None = None
    if transition_record_path is not None and str(transition_record_path).strip():
        blocked_ref = _read_control_surface(
            scope=scope,
            path=Path(transition_record_path).resolve(),
            schema_relpath=TRANSITION_RECORD_SCHEMA,
        )
    else:
        blocked_rows = [ref for ref in transitions if str(ref.payload.get("transition_status") or "") == "BLOCKED"]
        if blocked_rows:
            blocked_ref = blocked_rows[-1]
    if blocked_ref is None:
        raise ControlPlaneTrustProjectionError("BLOCKED_TRANSITION_NOT_FOUND")
    blocked_codes = list(blocked_ref.payload.get("blocking_reason_codes") or [])
    freshness_state = "superseded" if str(blocked_ref.path.resolve()) in superseded_map else "fresh"
    transition_plain = _plain_ref_from_surface(blocked_ref, TRANSITION_RECORD_ARTIFACT_ID)
    explanations = map_control_plane_explanations_v1(
        taxonomy_codes=blocked_codes,
        stage_id=str(blocked_ref.payload.get("stage_id") or ""),
        policy=str(blocked_ref.payload.get("transition_request_type") or ""),
        status=str(blocked_ref.payload.get("transition_status") or ""),
        evidence_refs=[transition_plain],
        authority_label="governed_derived",
        freshness_state=freshness_state,
    )
    stage_ref = blocked_ref.payload.get("admission_decision", {}).get("stage_admission_ref")
    cert_ref = blocked_ref.payload.get("certification_decision", {}).get("stage_certification_ref")
    projection_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": BLOCKED_VIEW_ARTIFACT_ID,
            "transition_ref": transition_plain,
            "freshness_state": freshness_state,
        }
    )
    semantic_events = [
        _semantic_event(
            event_type="blocked_transition",
            authority_label="governed_derived",
            freshness_state=freshness_state,
            governing_refs=[transition_plain],
            projection_version=PROJECTION_VERSION,
            taxonomy=blocked_codes,
            transition=blocked_ref,
            projection_id=projection_id,
        ),
        _semantic_event(
            event_type="projection_refresh",
            authority_label="governed_derived",
            freshness_state=freshness_state,
            governing_refs=[transition_plain],
            projection_version=PROJECTION_VERSION,
            taxonomy=blocked_codes,
            projection_id=projection_id,
        ),
    ]
    payload = {
        "schema_id": "control_plane_blocked_transition_view",
        "schema_version": "v1",
        "artifact_id": BLOCKED_VIEW_ARTIFACT_ID,
        "surface_kind": "projection",
        "projection_id": projection_id,
        "projection_version": PROJECTION_VERSION,
        "generated_at_utc": _utc_now(),
        "authority_label": "governed_derived",
        "target_day": scope.day_utc,
        "scope_id": scope.scope_id,
        "stage_id": str(blocked_ref.payload.get("stage_id") or ""),
        "policy": str(blocked_ref.payload.get("transition_request_type") or ""),
        "transition_status": str(blocked_ref.payload.get("transition_status") or ""),
        "freshness_state": freshness_state,
        "explanation_mapping_version": EXPLANATION_MAPPING_VERSION,
        "transition_record_ref": transition_plain,
        "stage_ref": dict(stage_ref) if isinstance(stage_ref, Mapping) else None,
        "stage_certification_ref": dict(cert_ref) if isinstance(cert_ref, Mapping) else None,
        "blocked_reason_codes": blocked_codes,
        "explanations": explanations,
        "governing_stage_refs": [dict(stage_ref)] if isinstance(stage_ref, Mapping) else [],
        "governing_transition_refs": [transition_plain],
        "governing_certification_refs": [dict(cert_ref)] if isinstance(cert_ref, Mapping) else [],
        "evidence_refs": [transition_plain],
        "semantic_events": semantic_events,
    }
    emitted_ref = None
    if emit_artifact:
        transition_id = str(blocked_ref.payload.get("transition_id") or "")
        emitted_ref = _write_projection(
            artifact_id=BLOCKED_VIEW_ARTIFACT_ID,
            payload=payload,
            schema_relpath=BLOCKED_VIEW_SCHEMA,
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            extra_variables={"scope_id": scope.scope_id, "transition_id": transition_id},
        )
    return _projection_response(artifact_id=BLOCKED_VIEW_ARTIFACT_ID, ok=True, payload=payload, emitted_ref=emitted_ref)


def materialize_control_plane_operator_status_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    emit_artifact: bool = False,
) -> Dict[str, Any]:
    scope = _scope(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    chain_ref = _load_chain_certification_if_present(scope=scope)
    transitions = _load_transition_records(scope=scope)
    superseded_map = _superseded_by_map(transitions)
    latest_by_stage = _latest_transition_by_stage(transitions)

    stage_rows: list[Dict[str, Any]] = []
    stage_refs: list[Dict[str, str]] = []
    certification_refs: list[Dict[str, str]] = []
    transition_refs: list[Dict[str, str]] = []
    for stage_definition in STAGE_DEFINITIONS:
        stage_ref = None
        cert_ref = None
        if chain_ref is not None:
            for row in chain_ref.payload.get("stage_admission_refs") or []:
                if isinstance(row, Mapping) and str(row.get("artifact_id") or "").strip() == stage_definition.artifact_id:
                    stage_ref = _load_ref_from_plain(scope=scope, row=row, schema_relpath=CONTROL_STAGE_ADMISSION_SCHEMA)
                    break
            for row in chain_ref.payload.get("stage_certification_refs") or []:
                if isinstance(row, Mapping) and str(row.get("artifact_id") or "").strip() == stage_definition.certification_artifact_id:
                    cert_ref = _load_ref_from_plain(scope=scope, row=row, schema_relpath=CONTROL_STAGE_CERTIFICATION_SCHEMA)
                    break
        if stage_ref is None:
            stage_ref = _load_stage_ref_if_present(scope=scope, artifact_id=stage_definition.artifact_id)
        if cert_ref is None:
            cert_ref = _load_stage_ref_if_present(scope=scope, artifact_id=stage_definition.certification_artifact_id)
        latest_transition = latest_by_stage.get(stage_definition.stage_id)
        if stage_ref is not None:
            stage_refs.append(_plain_ref_from_surface(stage_ref, stage_definition.artifact_id))
        if cert_ref is not None:
            certification_refs.append(_plain_ref_from_surface(cert_ref, stage_definition.certification_artifact_id))
        if latest_transition is not None:
            transition_plain = _plain_ref_from_surface(latest_transition, TRANSITION_RECORD_ARTIFACT_ID)
            transition_refs.append(transition_plain)
        freshness_state = "unknown"
        if latest_transition is not None and str(latest_transition.path.resolve()) in superseded_map:
            freshness_state = "superseded"
        elif cert_ref is not None:
            freshness_state = "fresh"
        elif stage_ref is not None:
            freshness_state = "uncertified"
        stage_row: Dict[str, Any] = {
            "stage_id": stage_definition.stage_id,
            "stage_admitted": stage_ref is not None,
            "stage_certified": cert_ref is not None,
            "freshness_state": freshness_state,
        }
        if latest_transition is not None:
            stage_row["latest_transition_status"] = str(latest_transition.payload.get("transition_status") or "")
            stage_row["latest_transition_ref"] = _plain_ref_from_surface(latest_transition, TRANSITION_RECORD_ARTIFACT_ID)
        stage_rows.append(stage_row)

    blocked_rows = [ref for ref in transitions if str(ref.payload.get("transition_status") or "") == "BLOCKED"]
    latest_blocked = blocked_rows[-1] if blocked_rows else None
    blocked_ref = _plain_ref_from_surface(latest_blocked, TRANSITION_RECORD_ARTIFACT_ID) if latest_blocked is not None else None
    blocked_explanations = []
    if latest_blocked is not None:
        blocked_explanations = map_control_plane_explanations_v1(
            taxonomy_codes=list(latest_blocked.payload.get("blocking_reason_codes") or []),
            stage_id=str(latest_blocked.payload.get("stage_id") or ""),
            policy=str(latest_blocked.payload.get("transition_request_type") or ""),
            status=str(latest_blocked.payload.get("transition_status") or ""),
            evidence_refs=[blocked_ref],
            authority_label="governed_derived",
            freshness_state="superseded" if str(latest_blocked.path.resolve()) in superseded_map else "fresh",
        )

    chain_status = "MISSING"
    if chain_ref is not None:
        chain_status = str(chain_ref.payload.get("certification_status") or "FAILED")
        certification_refs.append(_plain_ref_from_surface(chain_ref, STARTUP_CHAIN_CERTIFICATION))

    if chain_status == "CERTIFIED" and all(row["stage_certified"] for row in stage_rows):
        current_operator_status = "CERTIFIED_READY"
        freshness_state = "fresh"
    elif latest_blocked is not None:
        current_operator_status = "BLOCKED"
        freshness_state = "superseded" if str(latest_blocked.path.resolve()) in superseded_map else "uncertified"
    elif any(row["stage_admitted"] for row in stage_rows):
        current_operator_status = "PARTIAL"
        freshness_state = "uncertified"
    else:
        current_operator_status = "UNKNOWN"
        freshness_state = "unknown"

    projection_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": OPERATOR_STATUS_ARTIFACT_ID,
            "scope_id": scope.scope_id,
            "chain_ref": _plain_ref_from_surface(chain_ref, STARTUP_CHAIN_CERTIFICATION) if chain_ref is not None else None,
            "stage_rows": stage_rows,
            "latest_blocked_transition_ref": blocked_ref,
        }
    )
    lineage_rows = [
        {
            "stage_id": str(ref.payload.get("stage_id") or ""),
            "transition_record_ref": _plain_ref_from_surface(ref, TRANSITION_RECORD_ARTIFACT_ID),
            **(
                {"supersedes_ref": dict(ref.payload.get("supersedes_ref"))}
                if isinstance(ref.payload.get("supersedes_ref"), Mapping)
                else {}
            ),
            **(
                {"superseded_by_ref": superseded_map.get(str(ref.path.resolve()))}
                if str(ref.path.resolve()) in superseded_map
                else {}
            ),
        }
        for ref in transitions
        if isinstance(ref.payload.get("supersedes_ref"), Mapping) or str(ref.path.resolve()) in superseded_map
    ]
    projection_governing_refs = stage_refs + certification_refs
    if latest_blocked is not None and blocked_ref is not None:
        projection_governing_refs = [blocked_ref, *projection_governing_refs]
    semantic_events = [
        _semantic_event(
            event_type="projection_refresh",
            authority_label="governed_derived",
            freshness_state=freshness_state,
            governing_refs=projection_governing_refs,
            projection_version=PROJECTION_VERSION,
            taxonomy=_sorted_unique_codes(
                [*(latest_blocked.payload.get("blocking_reason_codes") or [])] if latest_blocked is not None else []
            ),
            projection_id=projection_id,
        )
    ]
    if chain_ref is not None:
        semantic_events.append(
            _semantic_event(
                event_type="certification_outcome",
                authority_label="governed_derived",
                freshness_state=freshness_state,
                governing_refs=[_plain_ref_from_surface(chain_ref, STARTUP_CHAIN_CERTIFICATION)],
                projection_version=PROJECTION_VERSION,
                taxonomy=[str(chain_ref.payload.get("certification_status") or "UNKNOWN")],
                projection_id=projection_id,
            )
        )
    if latest_blocked is not None:
        semantic_events.append(
            _semantic_event(
                event_type="blocked_transition",
                authority_label="governed_derived",
                freshness_state=freshness_state,
                governing_refs=[blocked_ref],
                projection_version=PROJECTION_VERSION,
                taxonomy=list(latest_blocked.payload.get("blocking_reason_codes") or []),
                transition=latest_blocked,
                projection_id=projection_id,
            )
        )
    payload = {
        "schema_id": "control_plane_operator_status",
        "schema_version": "v1",
        "artifact_id": OPERATOR_STATUS_ARTIFACT_ID,
        "surface_kind": "projection",
        "projection_id": projection_id,
        "projection_version": PROJECTION_VERSION,
        "generated_at_utc": _utc_now(),
        "authority_label": "governed_derived",
        "target_day": scope.day_utc,
        "scope_id": scope.scope_id,
        "operation_type": scope.operation_type,
        "sleeve_id": scope.sleeve_id,
        "environment": scope.environment,
        "ib_account": scope.ib_account,
        "freshness_state": freshness_state,
        "explanation_mapping_version": EXPLANATION_MAPPING_VERSION,
        "chain_certification_status": chain_status,
        "current_operator_status": current_operator_status,
        "stage_rows": stage_rows,
        "latest_blocked_explanations": blocked_explanations,
        "recovery_supersession_lineage": lineage_rows,
        "governing_stage_refs": stage_refs,
        "governing_transition_refs": transition_refs,
        "governing_certification_refs": certification_refs,
        "evidence_refs": transition_refs + stage_refs + certification_refs,
        "semantic_events": semantic_events,
    }
    if blocked_ref is not None:
        payload["latest_blocked_transition_ref"] = blocked_ref
    emitted_ref = None
    if emit_artifact:
        emitted_ref = _write_projection(
            artifact_id=OPERATOR_STATUS_ARTIFACT_ID,
            payload=payload,
            schema_relpath=OPERATOR_STATUS_SCHEMA,
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            extra_variables={"scope_id": scope.scope_id},
        )
    return _projection_response(
        artifact_id=OPERATOR_STATUS_ARTIFACT_ID,
        ok=current_operator_status != "UNKNOWN",
        payload=payload,
        emitted_ref=emitted_ref,
    )


def materialize_advisory_truth_binding_status_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    stage_path: Path | str,
    transition_record_path: Path | str,
    advisory_surface_label: str,
    advisory_authority_class: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    certification_path: Path | str | None = None,
    startup_chain_certification_path: Path | str | None = None,
    advisory_surface_path: Path | str | None = None,
    emit_artifact: bool = False,
    require_current: bool = False,
) -> Dict[str, Any]:
    scope = _scope(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=ib_account,
        operation_type=operation_type,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    stage_ref = _read_control_surface(
        scope=scope,
        path=Path(stage_path).resolve(),
        schema_relpath=CONTROL_STAGE_ADMISSION_SCHEMA,
    )
    transition_ref = _read_control_surface(
        scope=scope,
        path=Path(transition_record_path).resolve(),
        schema_relpath=TRANSITION_RECORD_SCHEMA,
    )
    certification_ref = None
    if certification_path is not None and str(certification_path).strip():
        certification_ref = _read_control_surface(
            scope=scope,
            path=Path(certification_path).resolve(),
            schema_relpath=CONTROL_STAGE_CERTIFICATION_SCHEMA,
        )
    chain_ref = None
    if startup_chain_certification_path is not None and str(startup_chain_certification_path).strip():
        chain_ref = _read_control_surface(
            scope=scope,
            path=Path(startup_chain_certification_path).resolve(),
            schema_relpath=STARTUP_CHAIN_CERTIFICATION_SCHEMA,
        )

    transitions = _load_transition_records(scope=scope)
    superseded_map = _superseded_by_map(transitions)
    transition_plain = _plain_ref_from_surface(transition_ref, TRANSITION_RECORD_ARTIFACT_ID)
    stage_plain = _plain_ref_from_surface(stage_ref, str(stage_ref.payload.get("stage_artifact_id") or ""))
    cert_plain = (
        _plain_ref_from_surface(certification_ref, str(certification_ref.payload.get("certification_artifact_id") or ""))
        if certification_ref is not None
        else None
    )
    chain_plain = _plain_ref_from_surface(chain_ref, STARTUP_CHAIN_CERTIFICATION) if chain_ref is not None else None
    transition_superseded = str(transition_ref.path.resolve()) in superseded_map

    latest_certified_stage_ref = None
    if chain_ref is not None:
        for row in chain_ref.payload.get("stage_admission_refs") or []:
            if not isinstance(row, Mapping):
                continue
            if str(row.get("artifact_id") or "").strip() == str(stage_plain["artifact_id"]):
                latest_certified_stage_ref = dict(row)
                break
    stale_relative_to_latest = latest_certified_stage_ref is not None and latest_certified_stage_ref != stage_plain
    binding_decision = evaluate_advisory_truth_binding_v1(
        advisory_authority_class=advisory_authority_class,
        stage_ref=stage_plain,
        transition_ref=transition_plain,
        certification_ref=cert_plain,
        startup_chain_certification_ref=chain_plain,
        transition_superseded=transition_superseded,
        stale_relative_to_latest_certified_basis=stale_relative_to_latest,
    )
    explanations = map_control_plane_explanations_v1(
        taxonomy_codes=binding_decision["taxonomy_codes"],
        stage_id=str(stage_ref.payload.get("stage_id") or ""),
        policy=str(transition_ref.payload.get("transition_request_type") or ""),
        status=str(binding_decision["binding_status"]),
        evidence_refs=[stage_plain, transition_plain, *( [cert_plain] if cert_plain else [] ), *( [chain_plain] if chain_plain else [] )],
        authority_label="governed_derived",
        freshness_state=str(binding_decision["freshness_state"]),
    )
    projection_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": ADVISORY_BINDING_ARTIFACT_ID,
            "advisory_surface_label": str(advisory_surface_label).strip(),
            "advisory_authority_class": str(advisory_authority_class).strip(),
            "stage_ref": stage_plain,
            "transition_ref": transition_plain,
            "certification_ref": cert_plain,
            "startup_chain_ref": chain_plain,
        }
    )
    advisory_surface_ref = None
    if advisory_surface_path is not None and str(advisory_surface_path).strip():
        advisory_surface_file = Path(advisory_surface_path).resolve()
        if advisory_surface_file.exists() and advisory_surface_file.is_file():
            advisory_surface_ref = _plain_ref(
                artifact_id="opaque_advisory_surface_ref",
                path=advisory_surface_file,
                sha256=canonical_hash_for_c2_artifact_v1({"path": str(advisory_surface_file)}),
            )
    semantic_events = [
        _semantic_event(
            event_type="projection_refresh",
            authority_label="governed_derived",
            freshness_state=str(binding_decision["freshness_state"]),
            governing_refs=[stage_plain, transition_plain, *( [cert_plain] if cert_plain else [] ), *( [chain_plain] if chain_plain else [] )],
            projection_version=PROJECTION_VERSION,
            taxonomy=list(binding_decision["taxonomy_codes"]),
            projection_id=projection_id,
        )
    ]
    if binding_decision["current_visibility"] != "current":
        semantic_events.append(
            _semantic_event(
                event_type="advisory_invalidation",
                authority_label="governed_derived",
                freshness_state=str(binding_decision["freshness_state"]),
                governing_refs=[stage_plain, transition_plain, *( [cert_plain] if cert_plain else [] ), *( [chain_plain] if chain_plain else [] )],
                projection_version=PROJECTION_VERSION,
                taxonomy=list(binding_decision["taxonomy_codes"]),
                projection_id=projection_id,
            )
        )
    payload = {
        "schema_id": "advisory_truth_binding_status",
        "schema_version": "v1",
        "artifact_id": ADVISORY_BINDING_ARTIFACT_ID,
        "surface_kind": "projection",
        "projection_id": projection_id,
        "projection_version": PROJECTION_VERSION,
        "generated_at_utc": _utc_now(),
        "authority_label": "governed_derived",
        "target_day": scope.day_utc,
        "scope_id": scope.scope_id,
        "operation_type": scope.operation_type,
        "sleeve_id": scope.sleeve_id,
        "environment": scope.environment,
        "ib_account": scope.ib_account,
        "freshness_state": binding_decision["freshness_state"],
        "explanation_mapping_version": EXPLANATION_MAPPING_VERSION,
        "advisory_surface_label": str(advisory_surface_label).strip(),
        "advisory_surface_ref": advisory_surface_ref,
        "advisory_authority_class": str(advisory_authority_class).strip(),
        "binding_status": binding_decision["binding_status"],
        "current_visibility": binding_decision["current_visibility"],
        "promotion_eligibility": bool(binding_decision["promotion_eligibility"]),
        "explanations": explanations,
        "governing_stage_refs": [stage_plain],
        "governing_transition_refs": [transition_plain],
        "governing_certification_refs": [row for row in [cert_plain] if row is not None],
        "startup_chain_certification_ref": chain_plain,
        "evidence_refs": [stage_plain, transition_plain, *( [cert_plain] if cert_plain else [] ), *( [chain_plain] if chain_plain else [] )],
        "semantic_events": semantic_events,
    }
    emitted_ref = None
    if emit_artifact:
        binding_id = projection_id
        emitted_ref = _write_projection(
            artifact_id=ADVISORY_BINDING_ARTIFACT_ID,
            payload=payload,
            schema_relpath=ADVISORY_BINDING_SCHEMA,
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            extra_variables={"scope_id": scope.scope_id, "binding_id": binding_id},
        )
    ok = True
    if require_current and str(binding_decision["current_visibility"]) != "current":
        ok = False
    return _projection_response(artifact_id=ADVISORY_BINDING_ARTIFACT_ID, ok=ok, payload=payload, emitted_ref=emitted_ref)
