from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from constellation_2.common.upper_layer_provenance_spine_v1 import materialize_upper_layer_provenance_spine_v1
from constellation_2.common.upper_layer_shared_v1 import (
    artifact_ref_from_surface_v1,
    cadence_window_v1,
    coerce_utc_text_v1,
    core2_refs_v1,
    day_utc_from_timestamp_v1,
    load_core2_trade_bundle_v1,
    load_surface_from_path_v1,
    logical_artifact_ref_v1,
    now_utc_text_v1,
    resolve_upper_family_trade_dir_v1,
    stable_sha256_id_v1,
    stable_unique_codes_v1,
    trade_identity_pointer_v1,
    write_validated_payload_v1,
)


ORCHESTRATION_STATE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/orchestration_state.v1.schema.json'
ORCHESTRATION_TRIGGER_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/orchestration_trigger.v1.schema.json'
PROVENANCE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/upper_layer_provenance_spine.v1.schema.json'
POLICY_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/strategy_policy_projection.v1.schema.json'
EXCEPTION_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/exception_state.v1.schema.json'
INTERVENTION_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_intervention_state.v1.schema.json'

RC_SCHEDULER_CADENCE_UNAVAILABLE = 'SCHEDULER_CADENCE_UNAVAILABLE'
RC_RETRY_SUPPRESSED = 'RETRY_SUPPRESSED'
RC_TRIGGER_DEDUPLICATED = 'TRIGGER_DEDUPLICATED'
RC_ALERT_SUPPRESSED = 'ALERT_SUPPRESSED'
RC_SESSION_BOUNDARY_BLOCKED = 'SESSION_BOUNDARY_BLOCKED'
RC_ORCHESTRATION_DEGRADED = 'ORCHESTRATION_DEGRADED'


@dataclass(frozen=True)
class OrchestrationPlaneMaterializationV1:
    state_path: Path
    trigger_path: Path | None
    state_payload: Dict[str, Any]
    trigger_payload: Dict[str, Any] | None
    state_provenance_path: Path
    trigger_provenance_path: Path | None


def materialize_orchestration_plane_v1(
    *,
    core2_trade_dir: Path,
    execution_root: Path,
    policy_projection_path: str | Path,
    evaluated_at_utc: str = '',
    cadence_seconds: int = 60,
    session_phase: str = 'REGULAR',
    exception_state_path: str | Path | None = None,
    operator_intervention_state_path: str | Path | None = None,
    prior_state_path: str | Path | None = None,
) -> OrchestrationPlaneMaterializationV1:
    core2_bundle = load_core2_trade_bundle_v1(Path(core2_trade_dir).resolve())
    evaluation_utc = coerce_utc_text_v1(evaluated_at_utc or now_utc_text_v1())
    policy_surface = load_surface_from_path_v1(path=policy_projection_path, schema_relpath=POLICY_SCHEMA_RELPATH)
    exception_surface = load_surface_from_path_v1(path=exception_state_path, schema_relpath=EXCEPTION_SCHEMA_RELPATH) if exception_state_path else None
    intervention_surface = load_surface_from_path_v1(path=operator_intervention_state_path, schema_relpath=INTERVENTION_SCHEMA_RELPATH) if operator_intervention_state_path else None
    prior_state_surface = load_surface_from_path_v1(path=prior_state_path, schema_relpath=ORCHESTRATION_STATE_SCHEMA_RELPATH) if prior_state_path else None

    trade_identity = core2_bundle.trade_identity.payload
    incorporated_state = core2_bundle.incorporated_state.payload
    day_utc = day_utc_from_timestamp_v1(evaluation_utc)
    window = cadence_window_v1(evaluated_at_utc=evaluation_utc, cadence_seconds=cadence_seconds)

    reason_codes: list[str] = []
    constraint_refs = []
    if exception_surface is not None:
        constraint_refs.append(logical_artifact_ref_v1(logical_name='exception_state_v1', path=exception_surface.path))
    if intervention_surface is not None:
        constraint_refs.append(logical_artifact_ref_v1(logical_name='operator_intervention_state_v1', path=intervention_surface.path))

    pending_retries = []
    pending_alerts = []
    prior_trigger_ids = {
        str(ref.get('logical_name') or '').split(':', 1)[-1]
        for ref in (prior_state_surface.payload.get('trigger_refs_emitted') or [])
        if isinstance(ref, dict)
    } if prior_state_surface is not None else set()

    trigger_class = 'PERIODIC_EVALUATION'
    trigger_target_type = 'CORE3_REEVALUATION_REQUEST'
    reason_class = 'CADENCE'
    reason_code = 'PERIODIC_EVALUATION_DUE'

    if str(session_phase or '').strip().upper() == 'BLOCKED':
        trigger_class = 'REVIEW'
        trigger_target_type = 'OPERATOR_REVIEW_REQUEST'
        reason_class = 'SESSION'
        reason_code = RC_SESSION_BOUNDARY_BLOCKED
        reason_codes.append(RC_SESSION_BOUNDARY_BLOCKED)
    elif exception_surface is not None and str(exception_surface.payload.get('classification_status') or '') == 'ACTIVE':
        trigger_class = 'REVIEW'
        trigger_target_type = 'OPERATOR_REVIEW_REQUEST'
        reason_class = 'EXCEPTION'
        reason_code = str((exception_surface.payload.get('reason_codes') or ['EXCEPTION_REVIEW_REQUIRED'])[0])
        pending_alerts.append({'reason_code': reason_code, 'scheduled_for_utc': window['window_ends_at_utc']})
    elif intervention_surface is not None and str(intervention_surface.payload.get('review_status') or '') == 'PENDING':
        trigger_class = 'REVIEW'
        trigger_target_type = 'OPERATOR_REVIEW_REQUEST'
        reason_class = 'INTERVENTION'
        reason_code = str((intervention_surface.payload.get('reason_codes') or ['REVIEW_REQUIRED'])[0])
    elif intervention_surface is not None and str(intervention_surface.payload.get('acknowledgement_status') or '') != 'ACKNOWLEDGED':
        trigger_class = 'ALERT'
        trigger_target_type = 'ALERT_ONLY_REQUEST'
        reason_class = 'INTERVENTION'
        reason_code = str((intervention_surface.payload.get('reason_codes') or ['ACKNOWLEDGEMENT_REQUIRED'])[0])
        pending_alerts.append({'reason_code': reason_code, 'scheduled_for_utc': window['window_ends_at_utc']})

    if prior_state_surface is not None:
        for item in (prior_state_surface.payload.get('pending_retries') or []):
            if not isinstance(item, dict):
                continue
            scheduled = str(item.get('scheduled_for_utc') or '')
            if scheduled and scheduled <= evaluation_utc and trigger_class == 'PERIODIC_EVALUATION':
                trigger_class = 'RETRY'
                trigger_target_type = 'CORE3_REEVALUATION_REQUEST'
                reason_class = 'RETRY'
                reason_code = str(item.get('reason_code') or 'RETRY_DUE')
            elif scheduled:
                pending_retries.append({'reason_code': str(item.get('reason_code') or 'RETRY_PENDING'), 'scheduled_for_utc': scheduled})

    if str(policy_surface.payload.get('policy_input_posture') or '') != 'READY' and trigger_class == 'PERIODIC_EVALUATION':
        pending_retries.append({'reason_code': str((policy_surface.payload.get('blocker_codes') or policy_surface.payload.get('degraded_codes') or ['POLICY_RETRY_REQUIRED'])[0]), 'scheduled_for_utc': window['window_ends_at_utc']})
        reason_codes.append(RC_ORCHESTRATION_DEGRADED)

    trigger_id = stable_sha256_id_v1(
        {
            'trade_identity_id': trade_identity.get('trade_identity_id'),
            'window': window,
            'trigger_class': trigger_class,
            'reason_code': reason_code,
            'policy_projection_id': policy_surface.payload.get('policy_projection_id'),
        }
    )
    emit_trigger = True
    if trigger_id in prior_trigger_ids:
        emit_trigger = False
        reason_codes.append(RC_TRIGGER_DEDUPLICATED)
    if trigger_class == 'ALERT' and prior_state_surface is not None and pending_alerts:
        reason_codes.append(RC_ALERT_SUPPRESSED if not emit_trigger else '')
    reason_codes = stable_unique_codes_v1(reason_codes)

    if RC_SESSION_BOUNDARY_BLOCKED in reason_codes:
        orchestration_posture = 'BLOCKED'
    elif reason_codes:
        orchestration_posture = 'DEGRADED'
    else:
        orchestration_posture = 'READY'

    state_id = stable_sha256_id_v1(
        {
            'trade_identity_id': trade_identity.get('trade_identity_id'),
            'window': window,
            'policy_projection_id': policy_surface.payload.get('policy_projection_id'),
            'reason_codes': reason_codes,
            'pending_retries': pending_retries,
            'pending_alerts': pending_alerts,
        }
    )
    state_provenance = materialize_upper_layer_provenance_spine_v1(
        execution_root=Path(execution_root).resolve(),
        artifact_family='orchestration_state_v1',
        artifact_id=state_id,
        core2_bundle=core2_bundle,
        evaluated_at_utc=evaluation_utc,
        cross_upper_plane_refs=[logical_artifact_ref_v1(logical_name='strategy_policy_projection_v1', path=policy_surface.path), *constraint_refs],
        rule_versions={'orchestration_state': 'v1'},
    )
    state_provenance_ref = artifact_ref_from_surface_v1(
        write_validated_payload_v1(path=state_provenance.path, payload=state_provenance.payload, schema_relpath=PROVENANCE_SCHEMA_RELPATH)
    )

    state_payload = {
        'schema_id': 'orchestration_state',
        'schema_version': 'v1',
        'authority_owner': 'orchestration_state_v1',
        'plane_owner_status': 'CANONICAL_ORCHESTRATION_STATE_PLANE_ARTIFACT',
        'orchestration_state_id': state_id,
        'day_utc': day_utc,
        'evaluated_at_utc': evaluation_utc,
        'trade_identity_ref': trade_identity_pointer_v1(core2_bundle),
        'environment': str(trade_identity.get('environment') or incorporated_state.get('environment') or ''),
        'sleeve_id': str(trade_identity.get('sleeve_id') or incorporated_state.get('sleeve_id') or ''),
        'cadence_window': window,
        'next_evaluation_at_utc': str(window['window_ends_at_utc']),
        'pending_retries': pending_retries,
        'pending_alerts': pending_alerts,
        'session_phase': str(session_phase),
        'trigger_refs_emitted': [],
        'orchestration_posture': orchestration_posture,
        'reason_codes': reason_codes,
        'policy_projection_ref': logical_artifact_ref_v1(logical_name='strategy_policy_projection_v1', path=policy_surface.path),
        'constraint_refs': constraint_refs,
        'core2_refs': core2_refs_v1(core2_bundle),
        'upper_layer_provenance_ref': state_provenance_ref,
        'artifact_driven_only': True,
        'direct_execution_forbidden': True,
        'downstream_execution_posture': 'REENTER_CORE3_AND_CORE4_REQUIRED',
    }
    state_dir = resolve_upper_family_trade_dir_v1(
        execution_root=execution_root,
        family='orchestration_state_v1',
        day_utc=day_utc,
        artifact_id=state_id,
        trade_identity_id=state_payload['trade_identity_ref']['trade_identity_id'],
    )
    state_ref = write_validated_payload_v1(
        path=state_dir / 'orchestration_state.v1.json',
        payload=state_payload,
        schema_relpath=ORCHESTRATION_STATE_SCHEMA_RELPATH,
    )

    trigger_path = None
    trigger_payload = None
    trigger_provenance_path = None
    if emit_trigger:
        trigger_provenance = materialize_upper_layer_provenance_spine_v1(
            execution_root=Path(execution_root).resolve(),
            artifact_family='orchestration_trigger_v1',
            artifact_id=trigger_id,
            core2_bundle=core2_bundle,
            evaluated_at_utc=evaluation_utc,
            cross_upper_plane_refs=[
                logical_artifact_ref_v1(logical_name='orchestration_state_v1', path=state_ref.path),
                logical_artifact_ref_v1(logical_name='strategy_policy_projection_v1', path=policy_surface.path),
                *constraint_refs,
            ],
            rule_versions={'orchestration_trigger': 'v1'},
        )
        trigger_provenance_ref = artifact_ref_from_surface_v1(
            write_validated_payload_v1(path=trigger_provenance.path, payload=trigger_provenance.payload, schema_relpath=PROVENANCE_SCHEMA_RELPATH)
        )
        trigger_payload = {
            'schema_id': 'orchestration_trigger',
            'schema_version': 'v1',
            'authority_owner': 'orchestration_trigger_v1',
            'trigger_id': trigger_id,
            'day_utc': day_utc,
            'emitted_at_utc': evaluation_utc,
            'trade_identity_ref': trade_identity_pointer_v1(core2_bundle),
            'trigger_class': trigger_class,
            'reason_class': reason_class,
            'reason_code': reason_code,
            'trigger_target_type': trigger_target_type,
            'orchestration_state_ref': logical_artifact_ref_v1(logical_name=f'orchestration_state:{state_id}', path=state_ref.path),
            'policy_projection_ref': logical_artifact_ref_v1(logical_name='strategy_policy_projection_v1', path=policy_surface.path),
            'constraint_refs': constraint_refs,
            'core2_refs': core2_refs_v1(core2_bundle),
            'upper_layer_provenance_ref': trigger_provenance_ref,
            'artifact_driven_only': True,
            'direct_transport_forbidden': True,
            'downstream_execution_posture': 'REENTER_CORE3_AND_CORE4_REQUIRED',
        }
        trigger_dir = resolve_upper_family_trade_dir_v1(
            execution_root=execution_root,
            family='orchestration_trigger_v1',
            day_utc=day_utc,
            artifact_id=trigger_id,
            trade_identity_id=trigger_payload['trade_identity_ref']['trade_identity_id'],
        )
        trigger_ref = write_validated_payload_v1(
            path=trigger_dir / 'orchestration_trigger.v1.json',
            payload=trigger_payload,
            schema_relpath=ORCHESTRATION_TRIGGER_SCHEMA_RELPATH,
        )
        trigger_path = trigger_ref.path
        trigger_provenance_path = trigger_provenance.path
        state_payload['trigger_refs_emitted'] = [logical_artifact_ref_v1(logical_name=f'orchestration_trigger:{trigger_id}', path=trigger_ref.path)]
        state_ref = write_validated_payload_v1(
            path=state_ref.path,
            payload=state_payload,
            schema_relpath=ORCHESTRATION_STATE_SCHEMA_RELPATH,
        )

    return OrchestrationPlaneMaterializationV1(
        state_path=state_ref.path,
        trigger_path=trigger_path,
        state_payload=state_payload,
        trigger_payload=trigger_payload,
        state_provenance_path=state_provenance.path,
        trigger_provenance_path=trigger_provenance_path,
    )
