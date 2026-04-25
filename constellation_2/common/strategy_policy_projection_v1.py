from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from constellation_2.common.upper_layer_provenance_spine_v1 import materialize_upper_layer_provenance_spine_v1
from constellation_2.common.upper_layer_shared_v1 import (
    UpperCore2BundleV1,
    artifact_ref_from_surface_v1,
    core2_refs_v1,
    coerce_utc_text_v1,
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


STRATEGY_POLICY_PROJECTION_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/strategy_policy_projection.v1.schema.json'
EXCEPTION_STATE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/exception_state.v1.schema.json'
OPERATOR_INTERVENTION_STATE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_intervention_state.v1.schema.json'

RC_POLICY_INPUT_STALE = 'POLICY_INPUT_STALE'
RC_POLICY_INPUT_BLOCKED = 'POLICY_INPUT_BLOCKED'
RC_POLICY_VERSION_MISSING = 'POLICY_VERSION_MISSING'
RC_CONFLICTING_POLICY_POSTURE = 'CONFLICTING_POLICY_POSTURE'
RC_POLICY_PROJECTION_DEGRADED = 'POLICY_PROJECTION_DEGRADED'
RC_POLICY_PROJECTION_BLOCKED = 'POLICY_PROJECTION_BLOCKED'

DEFAULT_POLICY_VERSION_SET = {
    'stop_policy_version': 'stop_policy.v1',
    'trailing_policy_version': 'trailing_policy.v1',
    'scaling_policy_version': 'scaling_policy.v1',
    'time_exit_policy_version': 'time_exit_policy.v1',
    'risk_rule_version': 'risk_rule.v1',
    'selection_policy_version': 'selection_policy.v1',
}


@dataclass(frozen=True)
class StrategyPolicyProjectionMaterializationV1:
    policy_path: Path
    provenance_path: Path
    payload: Dict[str, Any]
    provenance_payload: Dict[str, Any]


def _policy_posture(*, projection_status: str, projected_action_class: str, reason_codes: list[str], parameter_values: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'projection_status': projection_status,
        'projected_action_class': projected_action_class,
        'reason_codes': stable_unique_codes_v1(reason_codes),
        'parameter_values': dict(parameter_values),
    }


def _selected_policy_versions(*, base_versions: Dict[str, str], intervention_payload: Dict[str, Any] | None) -> Dict[str, str]:
    versions = dict(base_versions)
    if not intervention_payload:
        return versions
    if str(intervention_payload.get('override_status') or '') == 'ACTIVE':
        override_scope = intervention_payload.get('override_scope') or {}
        if str(override_scope.get('scope_class') or '') == 'POLICY_VERSION_SELECTION':
            decision = str(intervention_payload.get('human_decision_class') or '').strip()
            if decision:
                versions['selection_policy_version'] = f'override::{decision}'
    return versions


def build_strategy_policy_projection_payload_v1(
    *,
    core2_bundle: UpperCore2BundleV1,
    evaluated_at_utc: str,
    policy_version_set: Dict[str, str] | None = None,
    exception_payload: Dict[str, Any] | None = None,
    exception_path: Path | None = None,
    intervention_payload: Dict[str, Any] | None = None,
    intervention_path: Path | None = None,
    upper_layer_provenance_ref: Dict[str, str],
) -> Dict[str, Any]:
    trade_identity = core2_bundle.trade_identity.payload
    incorporated_state = core2_bundle.incorporated_state.payload
    description = core2_bundle.reconciled_description.payload
    health = core2_bundle.reconciliation_health.payload

    evaluation_utc = coerce_utc_text_v1(evaluated_at_utc)
    versions = _selected_policy_versions(
        base_versions=dict(policy_version_set or DEFAULT_POLICY_VERSION_SET),
        intervention_payload=intervention_payload,
    )

    blocker_codes: list[str] = []
    degraded_codes: list[str] = []
    for key in DEFAULT_POLICY_VERSION_SET:
        if not str(versions.get(key) or '').strip():
            blocker_codes.append(RC_POLICY_VERSION_MISSING)
            break

    ownership = str(trade_identity.get('ownership_classification') or '')
    health_state = str(health.get('current_state') or '')
    freshness_status = str(health.get('freshness_status') or '')
    downstream_posture = str(health.get('downstream_action_posture') or '')
    lifecycle_status = str(description.get('lifecycle_status') or '')
    protection_status = str(description.get('protection_status') or '')
    open_position = lifecycle_status in {
        'WORKING_ENTRY',
        'OPEN_LONG',
        'OPEN_SHORT',
        'OPEN_LONG_WITH_WORKING_EXIT',
        'OPEN_SHORT_WITH_WORKING_EXIT',
    }

    if ownership != 'CONSTELLATION_OWNED' or health_state == 'BLOCKED' or downstream_posture == 'BLOCKED':
        blocker_codes.append(RC_POLICY_INPUT_BLOCKED)
    if freshness_status != 'FRESH' or health_state == 'DEGRADED':
        degraded_codes.append(RC_POLICY_INPUT_STALE)
    if exception_payload:
        posture = str(exception_payload.get('constraint_posture') or '')
        if posture == 'BLOCKED':
            blocker_codes.append(RC_POLICY_PROJECTION_BLOCKED)
        elif posture in {'REVIEW_REQUIRED', 'CORE3_REENTRY_REQUIRED'}:
            degraded_codes.append(RC_POLICY_PROJECTION_DEGRADED)
    if intervention_payload:
        override_status = str(intervention_payload.get('override_status') or '')
        if override_status in {'EXPIRED', 'MALFORMED'}:
            degraded_codes.append(RC_POLICY_PROJECTION_DEGRADED)
        override_scope = intervention_payload.get('override_scope') or {}
        if (
            exception_payload
            and str(exception_payload.get('constraint_posture') or '') == 'BLOCKED'
            and override_status == 'ACTIVE'
            and str(override_scope.get('scope_class') or '') == 'POLICY_VERSION_SELECTION'
            and str(intervention_payload.get('human_decision_class') or '').startswith('FORCE_')
        ):
            blocker_codes.append(RC_CONFLICTING_POLICY_POSTURE)

    blocker_codes = stable_unique_codes_v1(blocker_codes)
    degraded_codes = stable_unique_codes_v1(degraded_codes)
    if blocker_codes:
        policy_input_posture = 'BLOCKED'
    elif degraded_codes:
        policy_input_posture = 'DEGRADED'
    else:
        policy_input_posture = 'READY'

    common_reason_codes = blocker_codes or degraded_codes
    stop_posture = _policy_posture(
        projection_status='BLOCKED' if policy_input_posture == 'BLOCKED' else ('INACTIVE' if not open_position else 'PROJECTED'),
        projected_action_class='ADD_INITIAL_PROTECTION' if open_position and protection_status == 'PROTECTION_NOT_OBSERVED' else 'AMEND_PROTECTION',
        reason_codes=common_reason_codes if policy_input_posture != 'READY' else (['STOP_PROJECTION_ACTIVE'] if open_position else ['STOP_NOT_APPLICABLE']),
        parameter_values={'policy_version': versions['stop_policy_version'], 'stop_style': 'PROTECTION_FIRST'},
    )
    trailing_posture = _policy_posture(
        projection_status='BLOCKED' if policy_input_posture == 'BLOCKED' else ('INACTIVE' if not open_position else ('PROJECTED' if protection_status == 'PROTECTION_WORKING_PRESENT' else 'DEGRADED')),
        projected_action_class='AMEND_PROTECTION',
        reason_codes=common_reason_codes if policy_input_posture != 'READY' else (['TRAILING_PROJECTION_ACTIVE'] if protection_status == 'PROTECTION_WORKING_PRESENT' else ['TRAILING_WAITING_FOR_PROTECTION']),
        parameter_values={'policy_version': versions['trailing_policy_version'], 'trail_mode': 'LOCKED_AFTER_PROTECTION'},
    )
    scaling_posture = _policy_posture(
        projection_status='BLOCKED' if policy_input_posture == 'BLOCKED' else ('INACTIVE' if not open_position else 'PROJECTED'),
        projected_action_class='REDUCE_POSITION',
        reason_codes=common_reason_codes if policy_input_posture != 'READY' else ['SCALING_MONITOR_ONLY'],
        parameter_values={'policy_version': versions['scaling_policy_version'], 'scaling_mode': 'GOVERNED_REDUCTION_ONLY'},
    )
    time_exit_posture = _policy_posture(
        projection_status='BLOCKED' if policy_input_posture == 'BLOCKED' else ('INACTIVE' if not open_position else 'PROJECTED'),
        projected_action_class='CLOSE_POSITION',
        reason_codes=common_reason_codes if policy_input_posture != 'READY' else ['TIME_EXIT_MONITOR_ACTIVE'],
        parameter_values={'policy_version': versions['time_exit_policy_version'], 'exit_window': 'SESSION_CONTROLLED'},
    )
    risk_rule_posture = _policy_posture(
        projection_status='BLOCKED' if policy_input_posture == 'BLOCKED' else ('DEGRADED' if policy_input_posture == 'DEGRADED' else 'PROJECTED'),
        projected_action_class='HOLD',
        reason_codes=common_reason_codes if policy_input_posture != 'READY' else ['RISK_RULES_HEALTHY'],
        parameter_values={'policy_version': versions['risk_rule_version'], 'risk_mode': 'CONSTRAIN_ONLY'},
    )

    constraint_refs = []
    if exception_path is not None:
        constraint_refs.append(logical_artifact_ref_v1(logical_name='exception_state_v1', path=exception_path))
    if intervention_path is not None:
        constraint_refs.append(logical_artifact_ref_v1(logical_name='operator_intervention_state_v1', path=intervention_path))

    seed = {
        'trade_identity_id': trade_identity.get('trade_identity_id'),
        'evaluated_at_utc': evaluation_utc,
        'core2_state_sha256': core2_bundle.incorporated_state.sha256,
        'policy_versions': versions,
        'constraint_refs': constraint_refs,
        'policy_input_posture': policy_input_posture,
        'blocker_codes': blocker_codes,
        'degraded_codes': degraded_codes,
    }
    policy_projection_id = stable_sha256_id_v1(seed)

    return {
        'schema_id': 'strategy_policy_projection',
        'schema_version': 'v1',
        'authority_owner': 'strategy_policy_projection_v1',
        'plane_owner_status': 'CANONICAL_POLICY_PROJECTION_PLANE_ARTIFACT',
        'policy_projection_id': policy_projection_id,
        'day_utc': day_utc_from_timestamp_v1(evaluation_utc),
        'evaluated_at_utc': evaluation_utc,
        'trade_identity_ref': trade_identity_pointer_v1(core2_bundle),
        'environment': str(trade_identity.get('environment') or incorporated_state.get('environment') or ''),
        'sleeve_id': str(trade_identity.get('sleeve_id') or incorporated_state.get('sleeve_id') or ''),
        'policy_input_posture': policy_input_posture,
        'stop_posture': stop_posture,
        'trailing_posture': trailing_posture,
        'scaling_posture': scaling_posture,
        'time_exit_posture': time_exit_posture,
        'risk_rule_posture': risk_rule_posture,
        'policy_version_set': versions,
        'constraint_refs': constraint_refs,
        'core2_refs': core2_refs_v1(core2_bundle),
        'upper_layer_provenance_ref': dict(upper_layer_provenance_ref),
        'artifact_driven_only': True,
        'direct_execution_forbidden': True,
        'downstream_execution_posture': 'REENTER_CORE3_AND_CORE4_REQUIRED',
        'blocker_codes': blocker_codes,
        'degraded_codes': degraded_codes,
    }


def materialize_strategy_policy_projection_v1(
    *,
    core2_trade_dir: Path,
    execution_root: Path,
    evaluated_at_utc: str = '',
    policy_version_set: Dict[str, str] | None = None,
    exception_state_path: str | Path | None = None,
    operator_intervention_state_path: str | Path | None = None,
) -> StrategyPolicyProjectionMaterializationV1:
    core2_bundle = load_core2_trade_bundle_v1(Path(core2_trade_dir).resolve())
    evaluation_utc = coerce_utc_text_v1(evaluated_at_utc or now_utc_text_v1())
    exception_surface = None
    exception_path = None
    if exception_state_path:
        exception_surface = load_surface_from_path_v1(path=exception_state_path, schema_relpath=EXCEPTION_STATE_SCHEMA_RELPATH)
        exception_path = exception_surface.path
    intervention_surface = None
    intervention_path = None
    if operator_intervention_state_path:
        intervention_surface = load_surface_from_path_v1(path=operator_intervention_state_path, schema_relpath=OPERATOR_INTERVENTION_STATE_SCHEMA_RELPATH)
        intervention_path = intervention_surface.path

    provisional_payload = build_strategy_policy_projection_payload_v1(
        core2_bundle=core2_bundle,
        evaluated_at_utc=evaluation_utc,
        policy_version_set=policy_version_set,
        exception_payload=None if exception_surface is None else exception_surface.payload,
        exception_path=exception_path,
        intervention_payload=None if intervention_surface is None else intervention_surface.payload,
        intervention_path=intervention_path,
        upper_layer_provenance_ref={'artifact_path': '/tmp/provisional_upper_layer_provenance_spine.v1.json', 'artifact_sha256': '0' * 64},
    )
    provenance = materialize_upper_layer_provenance_spine_v1(
        execution_root=Path(execution_root).resolve(),
        artifact_family='strategy_policy_projection_v1',
        artifact_id=provisional_payload['policy_projection_id'],
        core2_bundle=core2_bundle,
        evaluated_at_utc=evaluation_utc,
        cross_upper_plane_refs=[
            *( [] if exception_path is None else [logical_artifact_ref_v1(logical_name='exception_state_v1', path=exception_path)] ),
            *( [] if intervention_path is None else [logical_artifact_ref_v1(logical_name='operator_intervention_state_v1', path=intervention_path)] ),
        ],
        rule_versions={'strategy_policy_projection': 'v1'},
    )
    provenance_ref = artifact_ref_from_surface_v1(
        write_validated_payload_v1(
            path=provenance.path,
            payload=provenance.payload,
            schema_relpath='governance/04_DATA/SCHEMAS/C2/TRADE_STATE/upper_layer_provenance_spine.v1.schema.json',
        )
    )
    payload = build_strategy_policy_projection_payload_v1(
        core2_bundle=core2_bundle,
        evaluated_at_utc=evaluation_utc,
        policy_version_set=policy_version_set,
        exception_payload=None if exception_surface is None else exception_surface.payload,
        exception_path=exception_path,
        intervention_payload=None if intervention_surface is None else intervention_surface.payload,
        intervention_path=intervention_path,
        upper_layer_provenance_ref=provenance_ref,
    )
    trade_id = payload['trade_identity_ref']['trade_identity_id']
    out_dir = resolve_upper_family_trade_dir_v1(
        execution_root=execution_root,
        family='strategy_policy_projection_v1',
        day_utc=payload['day_utc'],
        artifact_id=payload['policy_projection_id'],
        trade_identity_id=trade_id,
    )
    policy_ref = write_validated_payload_v1(
        path=out_dir / 'strategy_policy_projection.v1.json',
        payload=payload,
        schema_relpath=STRATEGY_POLICY_PROJECTION_SCHEMA_RELPATH,
    )
    return StrategyPolicyProjectionMaterializationV1(
        policy_path=policy_ref.path,
        provenance_path=provenance.path,
        payload=payload,
        provenance_payload=provenance.payload,
    )
