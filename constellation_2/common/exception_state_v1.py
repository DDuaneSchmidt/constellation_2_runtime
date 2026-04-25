from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict

from constellation_2.common.upper_layer_provenance_spine_v1 import materialize_upper_layer_provenance_spine_v1
from constellation_2.common.upper_layer_shared_v1 import (
    CORE1_HEALTH_SCHEMA_RELPATH,
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
    resolve_core1_health_path_v1,
    resolve_upper_family_trade_dir_v1,
    stable_sha256_id_v1,
    stable_unique_codes_v1,
    trade_identity_pointer_v1,
    write_validated_payload_v1,
)


EXCEPTION_STATE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/exception_state.v1.schema.json'
PROVENANCE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/upper_layer_provenance_spine.v1.schema.json'

RC_PARTIAL_FILL_UNRESOLVED = 'PARTIAL_FILL_UNRESOLVED'
RC_RECONNECT_RECOVERY_PENDING = 'RECONNECT_RECOVERY_PENDING'
RC_MANUAL_IB_CHANGE_REVIEW_REQUIRED = 'MANUAL_IB_CHANGE_REVIEW_REQUIRED'
RC_ORPHAN_ORDER_UNRESOLVED = 'ORPHAN_ORDER_UNRESOLVED'
RC_EXCEPTION_CLASSIFICATION_DEGRADED = 'EXCEPTION_CLASSIFICATION_DEGRADED'
RC_REPAIR_RECOMMENDATION_BLOCKED = 'REPAIR_RECOMMENDATION_BLOCKED'


@dataclass(frozen=True)
class ExceptionStateMaterializationV1:
    exception_path: Path
    provenance_path: Path
    payload: Dict[str, Any]
    provenance_payload: Dict[str, Any]


def _decimal_text(value: Any) -> Decimal:
    raw = str(value or '').strip()
    return Decimal(raw or '0')


def _normal_health(value: str) -> bool:
    text = str(value or '').strip().upper()
    return text in {'NONE', 'NOT_OBSERVED', 'OK', 'HEALTHY', 'CLEAR'}


def materialize_exception_state_v1(
    *,
    core2_trade_dir: Path,
    execution_root: Path,
    evaluated_at_utc: str = '',
    core1_health_path: str | Path | None = None,
    core3_authority_path: str | Path | None = None,
    core4_boundary_path: str | Path | None = None,
) -> ExceptionStateMaterializationV1:
    core2_bundle = load_core2_trade_bundle_v1(Path(core2_trade_dir).resolve())
    evaluation_utc = coerce_utc_text_v1(evaluated_at_utc or now_utc_text_v1())
    trade_identity = core2_bundle.trade_identity.payload
    incorporated_state = core2_bundle.incorporated_state.payload
    day_utc = day_utc_from_timestamp_v1(evaluation_utc)

    health_path = Path(core1_health_path).expanduser().resolve() if core1_health_path else resolve_core1_health_path_v1(execution_root=Path(execution_root).resolve(), day_utc=day_utc)
    core1_health = load_surface_from_path_v1(path=health_path, schema_relpath=CORE1_HEALTH_SCHEMA_RELPATH) if health_path.exists() else None
    core3_surface = load_surface_from_path_v1(path=core3_authority_path, schema_relpath=CORE3_AUTHORITY_SCHEMA_RELPATH) if core3_authority_path else None
    core4_surface = load_surface_from_path_v1(path=core4_boundary_path, schema_relpath=CORE4_BOUNDARY_SCHEMA_RELPATH) if core4_boundary_path else None

    detected_subtypes: list[str] = []
    reason_codes: list[str] = []

    orphan_facts = list(incorporated_state.get('orphan_order_facts') or [])
    if orphan_facts:
        detected_subtypes.append('ORPHAN_ORDER_PRESENT')
        reason_codes.append(RC_ORPHAN_ORDER_UNRESOLVED)

    ownership = str(trade_identity.get('ownership_classification') or '')
    if ownership != 'CONSTELLATION_OWNED':
        detected_subtypes.append('MANUAL_OR_FOREIGN_ACTIVITY')
        reason_codes.append(RC_MANUAL_IB_CHANGE_REVIEW_REQUIRED)

    if core1_health is None:
        detected_subtypes.append('CORE1_HEALTH_UNAVAILABLE')
        reason_codes.append(RC_EXCEPTION_CLASSIFICATION_DEGRADED)
    else:
        health_payload = core1_health.payload
        if not _normal_health(health_payload.get('reconnect_status', 'NONE')) or not _normal_health(health_payload.get('replay_status', 'NONE')) or not _normal_health(health_payload.get('gap_status', 'NONE')):
            detected_subtypes.append('RECONNECT_OR_REPLAY_PENDING')
            reason_codes.append(RC_RECONNECT_RECOVERY_PENDING)

    current_working_orders = list(incorporated_state.get('current_working_orders') or [])
    incorporated_fills = list(incorporated_state.get('incorporated_fills') or [])
    if current_working_orders and incorporated_fills and _decimal_text(incorporated_state.get('current_quantity')) > Decimal('0'):
        detected_subtypes.append('PARTIAL_FILL_WITH_WORKING_ORDERS')
        reason_codes.append(RC_PARTIAL_FILL_UNRESOLVED)

    if not detected_subtypes:
        exception_class = 'NO_EXCEPTION'
        classification_status = 'CLEAR'
        severity = 'INFO'
        abnormal_state_subtype = 'NONE'
        repair_recommendation_class = 'NO_REPAIR_REQUIRED'
        constraint_posture = 'NONE'
    elif len(detected_subtypes) > 1:
        exception_class = 'MULTIPLE_CONSTRAINTS'
        classification_status = 'ACTIVE'
        severity = 'CRITICAL'
        abnormal_state_subtype = 'MULTIPLE_ACTIVE_CONSTRAINTS'
        repair_recommendation_class = 'REVIEW_AND_REENTER_CORE3'
        constraint_posture = 'BLOCKED'
    elif RC_ORPHAN_ORDER_UNRESOLVED in reason_codes:
        exception_class = 'ORPHAN_ORDER'
        classification_status = 'ACTIVE'
        severity = 'CRITICAL'
        abnormal_state_subtype = 'ORPHAN_ORDER_PRESENT'
        repair_recommendation_class = 'REVIEW_AND_REENTER_CORE3'
        constraint_posture = 'BLOCKED'
    elif RC_MANUAL_IB_CHANGE_REVIEW_REQUIRED in reason_codes:
        exception_class = 'MANUAL_IB_CHANGE'
        classification_status = 'ACTIVE'
        severity = 'CRITICAL'
        abnormal_state_subtype = 'MANUAL_OR_FOREIGN_ACTIVITY'
        repair_recommendation_class = 'REVIEW_AND_REENTER_CORE3'
        constraint_posture = 'BLOCKED'
    elif RC_RECONNECT_RECOVERY_PENDING in reason_codes:
        exception_class = 'RECONNECT_RECOVERY'
        classification_status = 'ACTIVE'
        severity = 'CRITICAL'
        abnormal_state_subtype = 'RECONNECT_OR_REPLAY_PENDING'
        repair_recommendation_class = 'WAIT_FOR_RECOVERY_AND_REENTER_CORE3'
        constraint_posture = 'BLOCKED'
    else:
        exception_class = 'PARTIAL_FILL'
        classification_status = 'ACTIVE'
        severity = 'REVIEW_REQUIRED'
        abnormal_state_subtype = 'PARTIAL_FILL_WITH_WORKING_ORDERS'
        repair_recommendation_class = 'REENTER_CORE3_EVALUATION'
        constraint_posture = 'CORE3_REENTRY_REQUIRED'

    core3_refs = [] if core3_surface is None else [logical_artifact_ref_v1(logical_name='lifecycle_action_authority_v1', path=core3_surface.path)]
    core4_refs = [] if core4_surface is None else [logical_artifact_ref_v1(logical_name='post_entry_submit_boundary_v1', path=core4_surface.path)]
    if classification_status != 'CLEAR' and not core3_refs:
        reason_codes.append(RC_REPAIR_RECOMMENDATION_BLOCKED)

    seed = {
        'trade_identity_id': trade_identity.get('trade_identity_id'),
        'evaluated_at_utc': evaluation_utc,
        'core2_state_sha256': core2_bundle.incorporated_state.sha256,
        'core1_health_sha256': '' if core1_health is None else core1_health.sha256,
        'core3_sha256': '' if core3_surface is None else core3_surface.sha256,
        'core4_sha256': '' if core4_surface is None else core4_surface.sha256,
        'reason_codes': stable_unique_codes_v1(reason_codes),
        'exception_class': exception_class,
    }
    exception_id = stable_sha256_id_v1(seed)

    provenance = materialize_upper_layer_provenance_spine_v1(
        execution_root=Path(execution_root).resolve(),
        artifact_family='exception_state_v1',
        artifact_id=exception_id,
        core2_bundle=core2_bundle,
        evaluated_at_utc=evaluation_utc,
        core3_refs=core3_refs,
        core4_refs=core4_refs,
        cross_upper_plane_refs=[],
        rule_versions={'exception_state': 'v1'},
    )
    provenance_ref = artifact_ref_from_surface_v1(
        write_validated_payload_v1(path=provenance.path, payload=provenance.payload, schema_relpath=PROVENANCE_SCHEMA_RELPATH)
    )

    payload = {
        'schema_id': 'exception_state',
        'schema_version': 'v1',
        'authority_owner': 'exception_state_v1',
        'exception_id': exception_id,
        'day_utc': day_utc,
        'evaluated_at_utc': evaluation_utc,
        'trade_identity_ref': trade_identity_pointer_v1(core2_bundle),
        'environment': str(trade_identity.get('environment') or incorporated_state.get('environment') or ''),
        'sleeve_id': str(trade_identity.get('sleeve_id') or incorporated_state.get('sleeve_id') or ''),
        'exception_class': exception_class,
        'classification_status': classification_status,
        'severity': severity,
        'abnormal_state_subtype': abnormal_state_subtype,
        'detected_abnormal_subtypes': detected_subtypes or ['NONE'],
        'repair_recommendation_class': repair_recommendation_class,
        'constraint_posture': constraint_posture,
        'reason_codes': stable_unique_codes_v1(reason_codes),
        'core1_refs': [] if core1_health is None else [logical_artifact_ref_v1(logical_name='broker_observation_health_v1', path=core1_health.path)],
        'core2_refs': core2_refs_v1(core2_bundle),
        'core3_refs': core3_refs,
        'core4_refs': core4_refs,
        'upper_layer_provenance_ref': provenance_ref,
        'artifact_driven_only': True,
        'direct_execution_forbidden': True,
        'downstream_execution_posture': 'REENTER_CORE3_AND_CORE4_REQUIRED',
    }
    out_dir = resolve_upper_family_trade_dir_v1(
        execution_root=execution_root,
        family='exception_state_v1',
        day_utc=day_utc,
        artifact_id=exception_id,
        trade_identity_id=payload['trade_identity_ref']['trade_identity_id'],
    )
    exception_ref = write_validated_payload_v1(
        path=out_dir / 'exception_state.v1.json',
        payload=payload,
        schema_relpath=EXCEPTION_STATE_SCHEMA_RELPATH,
    )
    return ExceptionStateMaterializationV1(
        exception_path=exception_ref.path,
        provenance_path=provenance.path,
        payload=payload,
        provenance_payload=provenance.payload,
    )
