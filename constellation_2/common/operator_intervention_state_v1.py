from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Sequence

from constellation_2.common.upper_layer_provenance_spine_v1 import materialize_upper_layer_provenance_spine_v1
from constellation_2.common.upper_layer_shared_v1 import (
    CORE3_AUTHORITY_SCHEMA_RELPATH,
    CORE4_BOUNDARY_SCHEMA_RELPATH,
    artifact_ref_from_surface_v1,
    coerce_utc_text_v1,
    core2_refs_v1,
    day_utc_from_timestamp_v1,
    load_core2_trade_bundle_v1,
    load_surface_from_path_v1,
    logical_artifact_ref_v1,
    now_utc_text_v1,
    stable_sha256_id_v1,
    stable_unique_codes_v1,
    trade_identity_pointer_v1,
    resolve_upper_family_trade_dir_v1,
    write_validated_payload_v1,
)


OPERATOR_INTERVENTION_STATE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_intervention_state.v1.schema.json'
EXCEPTION_STATE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/exception_state.v1.schema.json'
PROVENANCE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/upper_layer_provenance_spine.v1.schema.json'

ALLOWED_SCOPE_CLASSES = {
    'NONE',
    'ACK_EXCEPTION_ONLY',
    'REQUEUE_REVIEW_WAKEUP',
    'POLICY_VERSION_SELECTION',
    'ALLOW_CORE3_REEVALUATION',
}

RC_REVIEW_MISSING = 'REVIEW_MISSING'
RC_OVERRIDE_EXPIRED = 'OVERRIDE_EXPIRED'
RC_OVERRIDE_MALFORMED = 'OVERRIDE_MALFORMED'
RC_OVERRIDE_SCOPE_INVALID = 'OVERRIDE_SCOPE_INVALID'
RC_ACKNOWLEDGEMENT_MISSING = 'ACKNOWLEDGEMENT_MISSING'
RC_INTERVENTION_DEGRADED = 'INTERVENTION_DEGRADED'


@dataclass(frozen=True)
class OperatorInterventionMaterializationV1:
    intervention_path: Path
    provenance_path: Path
    payload: Dict[str, Any]
    provenance_payload: Dict[str, Any]


def materialize_operator_intervention_state_v1(
    *,
    core2_trade_dir: Path,
    execution_root: Path,
    emitted_at_utc: str = '',
    review_status: str = 'NOT_REQUIRED',
    override_status: str = 'NONE',
    override_scope_class: str = 'NONE',
    override_expiry_utc: str = '',
    acknowledgement_status: str = 'NOT_REQUIRED',
    human_decision_class: str = 'NO_HUMAN_DECISION',
    allowed_action_codes: Sequence[str] | None = None,
    one_shot: bool = True,
    durable: bool = False,
    exception_state_path: str | Path | None = None,
    core3_authority_path: str | Path | None = None,
    core4_boundary_path: str | Path | None = None,
) -> OperatorInterventionMaterializationV1:
    core2_bundle = load_core2_trade_bundle_v1(Path(core2_trade_dir).resolve())
    emitted_utc = coerce_utc_text_v1(emitted_at_utc or now_utc_text_v1())
    trade_identity = core2_bundle.trade_identity.payload
    incorporated_state = core2_bundle.incorporated_state.payload

    reason_codes: list[str] = []
    scope_class = str(override_scope_class or 'NONE').strip() or 'NONE'
    normalized_override_status = str(override_status or 'NONE').strip().upper() or 'NONE'
    expiry_value = None
    if normalized_override_status == 'ACTIVE':
        if scope_class not in ALLOWED_SCOPE_CLASSES or scope_class == 'NONE':
            normalized_override_status = 'MALFORMED'
            scope_class = 'NONE'
            reason_codes.append(RC_OVERRIDE_SCOPE_INVALID)
        if not override_expiry_utc:
            normalized_override_status = 'MALFORMED'
            reason_codes.append(RC_OVERRIDE_MALFORMED)
        else:
            expiry_value = coerce_utc_text_v1(override_expiry_utc)
            if expiry_value <= emitted_utc:
                normalized_override_status = 'EXPIRED'
                reason_codes.append(RC_OVERRIDE_EXPIRED)
        if one_shot and durable:
            normalized_override_status = 'MALFORMED'
            reason_codes.append(RC_OVERRIDE_MALFORMED)
    if str(review_status or '').strip().upper() == 'PENDING' and normalized_override_status == 'NONE':
        reason_codes.append(RC_REVIEW_MISSING)
    if exception_state_path and str(acknowledgement_status or '').strip().upper() != 'ACKNOWLEDGED':
        reason_codes.append(RC_ACKNOWLEDGEMENT_MISSING)
    if normalized_override_status in {'MALFORMED', 'EXPIRED'}:
        reason_codes.append(RC_INTERVENTION_DEGRADED)

    exception_surface = load_surface_from_path_v1(path=exception_state_path, schema_relpath=EXCEPTION_STATE_SCHEMA_RELPATH) if exception_state_path else None
    core3_surface = load_surface_from_path_v1(path=core3_authority_path, schema_relpath=CORE3_AUTHORITY_SCHEMA_RELPATH) if core3_authority_path else None
    core4_surface = load_surface_from_path_v1(path=core4_boundary_path, schema_relpath=CORE4_BOUNDARY_SCHEMA_RELPATH) if core4_boundary_path else None
    core3_refs = [] if core3_surface is None else [logical_artifact_ref_v1(logical_name='lifecycle_action_authority_v1', path=core3_surface.path)]
    core4_refs = [] if core4_surface is None else [logical_artifact_ref_v1(logical_name='post_entry_submit_boundary_v1', path=core4_surface.path)]

    intervention_id = stable_sha256_id_v1(
        {
            'trade_identity_id': trade_identity.get('trade_identity_id'),
            'emitted_at_utc': emitted_utc,
            'review_status': review_status,
            'override_status': normalized_override_status,
            'override_scope_class': scope_class,
            'override_expiry_utc': expiry_value,
            'acknowledgement_status': acknowledgement_status,
            'human_decision_class': human_decision_class,
            'allowed_action_codes': list(allowed_action_codes or []),
            'exception_sha256': '' if exception_surface is None else exception_surface.sha256,
        }
    )
    provenance = materialize_upper_layer_provenance_spine_v1(
        execution_root=Path(execution_root).resolve(),
        artifact_family='operator_intervention_state_v1',
        artifact_id=intervention_id,
        core2_bundle=core2_bundle,
        evaluated_at_utc=emitted_utc,
        core3_refs=core3_refs,
        core4_refs=core4_refs,
        cross_upper_plane_refs=[] if exception_surface is None else [logical_artifact_ref_v1(logical_name='exception_state_v1', path=exception_surface.path)],
        rule_versions={'operator_intervention_state': 'v1'},
    )
    provenance_ref = artifact_ref_from_surface_v1(
        write_validated_payload_v1(path=provenance.path, payload=provenance.payload, schema_relpath=PROVENANCE_SCHEMA_RELPATH)
    )

    payload = {
        'schema_id': 'operator_intervention_state',
        'schema_version': 'v1',
        'authority_owner': 'operator_intervention_state_v1',
        'intervention_id': intervention_id,
        'day_utc': day_utc_from_timestamp_v1(emitted_utc),
        'emitted_at_utc': emitted_utc,
        'trade_identity_ref': trade_identity_pointer_v1(core2_bundle),
        'environment': str(trade_identity.get('environment') or incorporated_state.get('environment') or ''),
        'sleeve_id': str(trade_identity.get('sleeve_id') or incorporated_state.get('sleeve_id') or ''),
        'review_status': str(review_status or 'NOT_REQUIRED').strip().upper(),
        'override_status': normalized_override_status,
        'override_scope': {
            'scope_class': scope_class,
            'one_shot': bool(one_shot),
            'durable': bool(durable),
            'allowed_action_codes': [str(code).strip() for code in (allowed_action_codes or []) if str(code).strip()],
        },
        'override_expiry_utc': expiry_value,
        'acknowledgement_status': str(acknowledgement_status or 'NOT_REQUIRED').strip().upper(),
        'human_decision_class': str(human_decision_class or 'NO_HUMAN_DECISION').strip(),
        'reason_codes': stable_unique_codes_v1(reason_codes),
        'lower_core_refs': {
            'core2_refs': core2_refs_v1(core2_bundle),
            'core3_refs': core3_refs,
            'core4_refs': core4_refs,
        },
        'exception_refs': [] if exception_surface is None else [logical_artifact_ref_v1(logical_name='exception_state_v1', path=exception_surface.path)],
        'upper_layer_provenance_ref': provenance_ref,
        'artifact_driven_only': True,
        'direct_truth_rewrite_forbidden': True,
        'direct_transmit_forbidden': True,
        'downstream_execution_posture': 'REENTER_CORE3_AND_CORE4_REQUIRED',
    }
    out_dir = resolve_upper_family_trade_dir_v1(
        execution_root=execution_root,
        family='operator_intervention_state_v1',
        day_utc=payload['day_utc'],
        artifact_id=intervention_id,
        trade_identity_id=payload['trade_identity_ref']['trade_identity_id'],
    )
    intervention_ref = write_validated_payload_v1(
        path=out_dir / 'operator_intervention_state.v1.json',
        payload=payload,
        schema_relpath=OPERATOR_INTERVENTION_STATE_SCHEMA_RELPATH,
    )
    return OperatorInterventionMaterializationV1(
        intervention_path=intervention_ref.path,
        provenance_path=provenance.path,
        payload=payload,
        provenance_payload=provenance.payload,
    )
