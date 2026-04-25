from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence

from constellation_2.common.advisory_decision_explanation_mapping_v1 import (
    EXPLANATION_MAPPING_VERSION,
    map_advisory_decision_explanation_v1,
)
from constellation_2.common.advisory_decision_state_matrix_v1 import (
    evaluate_advisory_decision_matrix_v1,
)
from constellation_2.common.constitutional_runtime_v1 import (
    assert_constitutional_writer_allowed_v1,
    resolve_constitutional_artifact_path_v1,
    validate_read_model_payload_v1,
)
from constellation_2.common.control_plane_advisory_truth_binding_v1 import (
    ADVISORY_AUTHORITY_CLASSES,
    evaluate_advisory_truth_binding_v1,
)
from constellation_2.common.control_plane_stage_admission_v1 import (
    STARTUP_CHAIN_CERTIFICATION,
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
from constellation_2.common.tax_state_kernel_v1 import SCHEMA_RELPATH as TAX_STATE_SCHEMA
from constellation_2.common.opportunity_state_kernel_v1 import SCHEMA_RELPATH as OPPORTUNITY_STATE_SCHEMA
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
WRITER_ID = "constellation_2.common.advisory_decision_state_kernel_v1"
PROJECTION_VERSION = WRITER_ID

ARTIFACT_ID = "advisory_decision_state_v1"
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/advisory_decision_state.v1.schema.json"
CONTROL_STAGE_ADMISSION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_stage_admission.v1.schema.json"
CONTROL_STAGE_CERTIFICATION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_stage_certification.v1.schema.json"
STARTUP_CHAIN_CERTIFICATION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/startup_chain_certification.v1.schema.json"
TRANSITION_RECORD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/control_stage_transition_record.v1.schema.json"
DEPLOYMENT_STATE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/deployment_state_machine.v1.schema.json"
DEPLOYMENT_ARTIFACT_ID = "deployment_state_machine_v1"


class AdvisoryDecisionStateError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
        label="ADVISORY_DECISION_INPUT",
        path=path,
        expected_root_type=expected_root_type,
        reject_derived=reject_derived,
    )
    if report["errors"]:
        raise AdvisoryDecisionStateError(str(report["errors"][0]))
    return read_validated_surface_v1(path=path, schema_relpath=schema_relpath)


def _load_transition_records(*, scope: Any) -> list[SurfaceRefV1]:
    root = (scope.canonical_truth_root / "reports" / TRANSITION_RECORD_ARTIFACT_ID / scope.day_utc).resolve()
    refs: list[SurfaceRefV1] = []
    if not root.exists():
        return refs
    for path in sorted(root.glob("*/*/*/control_stage_transition_record.v1.json")):
        refs.append(
            _read_control_surface(
                scope=scope,
                path=path,
                schema_relpath=TRANSITION_RECORD_SCHEMA,
            )
        )
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


def _latest_certified_stage_ref(
    *,
    chain_ref: SurfaceRefV1 | None,
    stage_artifact_id: str,
) -> Mapping[str, Any] | None:
    if chain_ref is None:
        return None
    for row in chain_ref.payload.get("stage_admission_refs") or []:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("artifact_id") or "").strip() == str(stage_artifact_id or "").strip():
            return dict(row)
    return None


def _deployment_decision_state(
    deployment_ref: SurfaceRefV1 | None,
    *,
    release_relevant: bool,
) -> tuple[bool, bool, dict[str, Any]]:
    if not release_relevant:
        return False, False, {}
    if deployment_ref is None:
        return True, False, {}
    payload = deployment_ref.payload
    decision = str(payload.get("final_deployment_decision") or "").strip().upper()
    cleanliness = str(((payload.get("authoritative_source") or {}).get("authoritative_cleanliness_status")) or "").strip().upper()
    blocked = decision not in {"DEPLOY_READY", "DEPLOY_ACTIVE"} or cleanliness != "CLEAN"
    return False, blocked, {
        "final_deployment_decision": decision or "UNKNOWN",
        "authoritative_cleanliness_status": cleanliness or "UNKNOWN",
        "first_true_blocker_code": str(payload.get("first_true_blocker_code") or "").strip(),
    }


def _semantic_event(
    *,
    event_type: str,
    authority_label: str,
    decision_state: str,
    actionability_state: str,
    freshness_state: str,
    visibility_state: str,
    promotion_eligibility_state: str,
    invalidation_reason: str,
    governing_refs: Sequence[Mapping[str, Any]],
    projection_version: str,
) -> Dict[str, Any]:
    return {
        "event_type": event_type,
        "authority_label": authority_label,
        "decision_state": decision_state,
        "actionability_state": actionability_state,
        "freshness_state": freshness_state,
        "visibility_state": visibility_state,
        "promotion_eligibility_state": promotion_eligibility_state,
        "invalidation_reason": invalidation_reason,
        "governing_refs": [dict(row) for row in governing_refs],
        "projection_version": projection_version,
    }


def _write_projection(
    *,
    payload: Mapping[str, Any],
    canonical_truth_root: Path,
    day_utc: str,
    scope_id: str,
    decision_id: str,
) -> SurfaceRefV1:
    assert_constitutional_writer_allowed_v1(REPO_ROOT, ARTIFACT_ID, WRITER_ID)
    validation = validate_read_model_payload_v1(payload)
    if not bool(validation.get("ok")):
        raise AdvisoryDecisionStateError(f"INVALID_ADVISORY_DECISION_READ_MODEL:{validation.get('errors')}")
    path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=ARTIFACT_ID,
        day_utc=day_utc,
        canonical_truth_root=canonical_truth_root,
        extra_variables={"scope_id": scope_id, "decision_id": decision_id},
    )
    return atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=dict(payload),
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("generated_at_utc",),
    )


def _decision_root(*, canonical_truth_root: Path, day_utc: str, scope_id: str) -> Path:
    return (canonical_truth_root / "reports" / ARTIFACT_ID / day_utc / scope_id).resolve()


def list_advisory_decision_states_v1(
    *,
    canonical_truth_root: Path | str,
    day_utc: str,
    scope_id: str | None = None,
) -> list[SurfaceRefV1]:
    root = (Path(canonical_truth_root).resolve() / "reports" / ARTIFACT_ID / day_utc).resolve()
    refs: list[SurfaceRefV1] = []
    if not root.exists():
        return refs
    pattern = "*/*/advisory_decision_state.v1.json" if not scope_id else f"{scope_id}/*/advisory_decision_state.v1.json"
    for path in sorted(root.glob(pattern)):
        refs.append(read_validated_surface_v1(path=path, schema_relpath=SCHEMA_RELPATH))
    refs.sort(
        key=lambda ref: (
            str(ref.payload.get("generated_at_utc") or ""),
            str(ref.payload.get("decision_id") or ""),
            str(ref.path),
        )
    )
    return refs


def find_latest_advisory_decision_state_v1(
    *,
    canonical_truth_root: Path | str,
    day_utc: str,
    scope_id: str | None = None,
    advisory_item_id: str | None = None,
) -> SurfaceRefV1 | None:
    refs = list_advisory_decision_states_v1(
        canonical_truth_root=canonical_truth_root,
        day_utc=day_utc,
        scope_id=scope_id,
    )
    if advisory_item_id:
        refs = [
            ref for ref in refs if str(ref.payload.get("advisory_item_id") or "").strip() == str(advisory_item_id).strip()
        ]
    if not refs:
        return None
    superseded_paths = {
        str((ref.payload.get("supersedes_ref") or {}).get("artifact_path") or "").strip()
        for ref in refs
        if isinstance(ref.payload.get("supersedes_ref"), dict)
    }
    current_refs = [ref for ref in refs if str(ref.path.resolve()) not in superseded_paths]
    if current_refs:
        refs = current_refs
    return refs[-1]


def materialize_advisory_decision_state_v1(
    *,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    stage_path: Path | str,
    transition_record_path: Path | str,
    advisory_item_id: str,
    advisory_surface_label: str,
    advisory_authority_class: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
    certification_path: Path | str | None = None,
    startup_chain_certification_path: Path | str | None = None,
    deployment_state_path: Path | str | None = None,
    advisory_surface_path: Path | str | None = None,
    tax_state_path: Path | str | None = None,
    opportunity_state_path: Path | str | None = None,
    emit_artifact: bool = False,
) -> Dict[str, Any]:
    advisory_class = str(advisory_authority_class or "").strip()
    if advisory_class not in ADVISORY_AUTHORITY_CLASSES:
        raise AdvisoryDecisionStateError(f"UNKNOWN_ADVISORY_AUTHORITY_CLASS:{advisory_authority_class}")
    item_id = str(advisory_item_id or "").strip()
    surface_label = str(advisory_surface_label or "").strip()
    if not item_id or not surface_label:
        raise AdvisoryDecisionStateError("ADVISORY_DECISION_ITEM_ID_AND_SURFACE_LABEL_REQUIRED")

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
    deployment_ref = None
    if deployment_state_path is not None and str(deployment_state_path).strip():
        deployment_ref = _read_control_surface(
            scope=scope,
            path=Path(deployment_state_path).resolve(),
            schema_relpath=DEPLOYMENT_STATE_SCHEMA,
        )
    tax_ref = None
    if tax_state_path is not None and str(tax_state_path).strip():
        tax_ref = _read_control_surface(
            scope=scope,
            path=Path(tax_state_path).resolve(),
            schema_relpath=TAX_STATE_SCHEMA,
        )
    opportunity_ref = None
    if opportunity_state_path is not None and str(opportunity_state_path).strip():
        opportunity_ref = _read_control_surface(
            scope=scope,
            path=Path(opportunity_state_path).resolve(),
            schema_relpath=OPPORTUNITY_STATE_SCHEMA,
        )

    transitions = _load_transition_records(scope=scope)
    superseded_map = _superseded_by_map(transitions)
    transition_plain = _plain_ref_from_surface(transition_ref, TRANSITION_RECORD_ARTIFACT_ID)
    stage_artifact_id = str(stage_ref.payload.get("stage_artifact_id") or "").strip()
    stage_plain = _plain_ref_from_surface(stage_ref, stage_artifact_id)
    cert_plain = (
        _plain_ref_from_surface(
            certification_ref, str(certification_ref.payload.get("certification_artifact_id") or "")
        )
        if certification_ref is not None
        else None
    )
    chain_plain = _plain_ref_from_surface(chain_ref, STARTUP_CHAIN_CERTIFICATION) if chain_ref is not None else None
    deployment_plain = _plain_ref_from_surface(deployment_ref, DEPLOYMENT_ARTIFACT_ID) if deployment_ref is not None else None
    tax_plain = _plain_ref_from_surface(tax_ref, "tax_state_v1") if tax_ref is not None else None
    opportunity_plain = _plain_ref_from_surface(opportunity_ref, "opportunity_state_v1") if opportunity_ref is not None else None
    transition_superseded = str(transition_ref.path.resolve()) in superseded_map
    latest_stage_ref = _latest_certified_stage_ref(chain_ref=chain_ref, stage_artifact_id=stage_artifact_id)
    stale_relative = latest_stage_ref is not None and latest_stage_ref != stage_plain

    binding = evaluate_advisory_truth_binding_v1(
        advisory_authority_class=advisory_class,
        stage_ref=stage_plain,
        transition_ref=transition_plain,
        certification_ref=cert_plain,
        startup_chain_certification_ref=chain_plain,
        transition_superseded=transition_superseded,
        stale_relative_to_latest_certified_basis=stale_relative,
    )
    release_basis_missing, release_blocked, release_details = _deployment_decision_state(
        deployment_ref,
        release_relevant=advisory_class in {"recommendation", "promotion_eligible"},
    )
    matrix = evaluate_advisory_decision_matrix_v1(
        advisory_authority_class=advisory_class,
        freshness_state=str(binding["freshness_state"]),
        base_visibility=str(binding["current_visibility"]),
        base_promotion_eligibility=bool(binding["promotion_eligibility"]),
        release_basis_missing=release_basis_missing,
        release_blocked=release_blocked,
        transition_superseded=transition_superseded,
        stale_relative_to_latest_certified_basis=stale_relative,
        tax_effect_state=str(((tax_ref.payload if tax_ref is not None else {}).get("advisory_binding_state") or {}).get("effect_state") or "clear"),
        opportunity_effect_state=str(((opportunity_ref.payload if opportunity_ref is not None else {}).get("advisory_binding_state") or {}).get("effect_state") or "clear"),
    )
    authority_label = "governed_certified_decision"
    evidence_refs = [
        stage_plain,
        transition_plain,
        *([cert_plain] if cert_plain is not None else []),
        *([chain_plain] if chain_plain is not None else []),
        *([deployment_plain] if deployment_plain is not None else []),
        *([tax_plain] if tax_plain is not None else []),
        *([opportunity_plain] if opportunity_plain is not None else []),
    ]
    primary_explanation = map_advisory_decision_explanation_v1(
        decision_state=str(matrix["decision_state"]),
        actionability_state=str(matrix["actionability_state"]),
        freshness_state=str(binding["freshness_state"]),
        visibility_state=str(matrix["visibility_state"]),
        invalidation_reason=str(matrix["rule_id"]),
        evidence_refs=evidence_refs,
        authority_label=authority_label,
        detail_fields={
            "ordered_reason_codes": list(matrix["ordered_reason_codes"]),
            "release_details": release_details,
            "advisory_surface_label": surface_label,
            "advisory_item_id": item_id,
            "tax_binding_state": ((tax_ref.payload if tax_ref is not None else {}).get("advisory_binding_state") or {}),
            "opportunity_binding_state": ((opportunity_ref.payload if opportunity_ref is not None else {}).get("advisory_binding_state") or {}),
        },
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
    decision_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": ARTIFACT_ID,
            "advisory_item_id": item_id,
            "advisory_surface_label": surface_label,
            "advisory_authority_class": advisory_class,
            "decision_state": matrix["decision_state"],
            "freshness_state": binding["freshness_state"],
            "invalidation_rule_id": matrix["rule_id"],
            "stage_ref": stage_plain,
            "transition_ref": transition_plain,
            "certification_ref": cert_plain,
            "startup_chain_ref": chain_plain,
            "deployment_state_ref": deployment_plain,
            "tax_state_ref": tax_plain,
            "opportunity_state_ref": opportunity_plain,
        }
    )
    prior_ref = find_latest_advisory_decision_state_v1(
        canonical_truth_root=scope.canonical_truth_root,
        day_utc=scope.day_utc,
        scope_id=scope.scope_id,
        advisory_item_id=item_id,
    )
    supersedes_ref = None
    if prior_ref is not None and str(prior_ref.payload.get("decision_id") or "").strip() != decision_id:
        supersedes_ref = _plain_ref_from_surface(prior_ref, ARTIFACT_ID)

    semantic_events = [
        _semantic_event(
            event_type="advisory_decision_computed",
            authority_label=authority_label,
            decision_state=str(matrix["decision_state"]),
            actionability_state=str(matrix["actionability_state"]),
            freshness_state=str(binding["freshness_state"]),
            visibility_state=str(matrix["visibility_state"]),
            promotion_eligibility_state=str(matrix["promotion_eligibility_state"]),
            invalidation_reason=str(matrix["rule_id"]),
            governing_refs=evidence_refs,
            projection_version=PROJECTION_VERSION,
        )
    ]
    if matrix["decision_state"] in {"blocked", "uncertified", "incomplete_basis"}:
        semantic_events.append(
            _semantic_event(
                event_type="advisory_blocked",
                authority_label=authority_label,
                decision_state=str(matrix["decision_state"]),
                actionability_state=str(matrix["actionability_state"]),
                freshness_state=str(binding["freshness_state"]),
                visibility_state=str(matrix["visibility_state"]),
                promotion_eligibility_state=str(matrix["promotion_eligibility_state"]),
                invalidation_reason=str(matrix["rule_id"]),
                governing_refs=evidence_refs,
                projection_version=PROJECTION_VERSION,
            )
        )
    elif matrix["decision_state"] in {"stale", "superseded"}:
        semantic_events.append(
            _semantic_event(
                event_type="advisory_downgraded",
                authority_label=authority_label,
                decision_state=str(matrix["decision_state"]),
                actionability_state=str(matrix["actionability_state"]),
                freshness_state=str(binding["freshness_state"]),
                visibility_state=str(matrix["visibility_state"]),
                promotion_eligibility_state=str(matrix["promotion_eligibility_state"]),
                invalidation_reason=str(matrix["rule_id"]),
                governing_refs=evidence_refs,
                projection_version=PROJECTION_VERSION,
            )
        )
    if matrix["visibility_state"] == "suppressed":
        semantic_events.append(
            _semantic_event(
                event_type="advisory_suppressed",
                authority_label=authority_label,
                decision_state=str(matrix["decision_state"]),
                actionability_state=str(matrix["actionability_state"]),
                freshness_state=str(binding["freshness_state"]),
                visibility_state=str(matrix["visibility_state"]),
                promotion_eligibility_state=str(matrix["promotion_eligibility_state"]),
                invalidation_reason=str(matrix["rule_id"]),
                governing_refs=evidence_refs,
                projection_version=PROJECTION_VERSION,
            )
        )
    elif matrix["visibility_state"] == "historical_only":
        semantic_events.append(
            _semantic_event(
                event_type="advisory_historical_only",
                authority_label=authority_label,
                decision_state=str(matrix["decision_state"]),
                actionability_state=str(matrix["actionability_state"]),
                freshness_state=str(binding["freshness_state"]),
                visibility_state=str(matrix["visibility_state"]),
                promotion_eligibility_state=str(matrix["promotion_eligibility_state"]),
                invalidation_reason=str(matrix["rule_id"]),
                governing_refs=evidence_refs,
                projection_version=PROJECTION_VERSION,
            )
        )
    if matrix["promotion_eligibility_state"] == "eligible":
        semantic_events.append(
            _semantic_event(
                event_type="promotion_eligibility_changed",
                authority_label=authority_label,
                decision_state=str(matrix["decision_state"]),
                actionability_state=str(matrix["actionability_state"]),
                freshness_state=str(binding["freshness_state"]),
                visibility_state=str(matrix["visibility_state"]),
                promotion_eligibility_state=str(matrix["promotion_eligibility_state"]),
                invalidation_reason=str(matrix["rule_id"]),
                governing_refs=evidence_refs,
                projection_version=PROJECTION_VERSION,
            )
        )

    payload = {
        "schema_id": "advisory_decision_state",
        "schema_version": "v1",
        "artifact_id": ARTIFACT_ID,
        "surface_kind": "projection",
        "decision_id": decision_id,
        "projection_version": PROJECTION_VERSION,
        "generated_at_utc": _utc_now(),
        "authority_label": authority_label,
        "target_day": scope.day_utc,
        "scope_id": scope.scope_id,
        "operation_type": scope.operation_type,
        "sleeve_id": scope.sleeve_id,
        "environment": scope.environment,
        "ib_account": scope.ib_account,
        "advisory_item_id": item_id,
        "advisory_surface_label": surface_label,
        "advisory_authority_class": advisory_class,
        "decision_state": matrix["decision_state"],
        "actionability_state": matrix["actionability_state"],
        "freshness_state": binding["freshness_state"],
        "visibility_state": matrix["visibility_state"],
        "promotion_eligibility_state": matrix["promotion_eligibility_state"],
        "invalidation_rule_id": matrix["rule_id"],
        "explanation_mapping_version": EXPLANATION_MAPPING_VERSION,
        "primary_explanation": primary_explanation,
        "historical_visibility": matrix["historical_visibility"],
        "governing_stage_refs": [stage_plain],
        "governing_transition_refs": [transition_plain],
        "governing_certification_refs": [row for row in [cert_plain, chain_plain] if row is not None],
        "governing_release_refs": [row for row in [deployment_plain] if row is not None],
        "governing_tax_refs": [row for row in [tax_plain] if row is not None],
        "governing_opportunity_refs": [row for row in [opportunity_plain] if row is not None],
        "evidence_refs": evidence_refs,
        "semantic_events": semantic_events,
    }
    if tax_ref is not None:
        payload["tax_binding_state"] = dict((tax_ref.payload.get("advisory_binding_state") or {}))
    if opportunity_ref is not None:
        payload["opportunity_binding_state"] = dict((opportunity_ref.payload.get("advisory_binding_state") or {}))
    if advisory_surface_ref is not None:
        payload["advisory_surface_ref"] = advisory_surface_ref
    if supersedes_ref is not None:
        payload["supersedes_ref"] = supersedes_ref
    emitted_ref = None
    if emit_artifact:
        emitted_ref = _write_projection(
            payload=payload,
            canonical_truth_root=scope.canonical_truth_root,
            day_utc=scope.day_utc,
            scope_id=scope.scope_id,
            decision_id=decision_id,
        )
    return {
        "ok": True,
        "artifact_id": ARTIFACT_ID,
        "decision": payload,
        "artifact_ref": _plain_ref_from_surface(emitted_ref, ARTIFACT_ID) if emitted_ref is not None else None,
    }
