from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.orchestration_plane_v1 import materialize_orchestration_plane_v1
from constellation_2.common.strategy_policy_projection_v1 import materialize_strategy_policy_projection_v1
from constellation_2.common.tests.upper_layer_test_support_v1 import prepare_upper_layer_stack


def test_strategy_policy_projection_is_deterministic_and_ref_preserving(tmp_path: Path) -> None:
    fixture = prepare_upper_layer_stack(tmp_path)
    first = materialize_strategy_policy_projection_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:40:00Z',
    )
    second = materialize_strategy_policy_projection_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:40:00Z',
    )

    assert first.payload == second.payload
    assert first.payload['artifact_driven_only'] is True
    assert first.payload['direct_execution_forbidden'] is True
    assert first.payload['downstream_execution_posture'] == 'REENTER_CORE3_AND_CORE4_REQUIRED'
    assert first.payload['core2_refs']['trade_identity_ref']['artifact_path'].endswith('trade_identity.v1.json')
    assert 'transmission_authorization_status' not in json.dumps(first.payload, sort_keys=True)


def test_conflicting_policy_postures_are_explicit(tmp_path: Path) -> None:
    fixture = prepare_upper_layer_stack(tmp_path, orphan_count=1)
    from constellation_2.common.exception_state_v1 import materialize_exception_state_v1
    from constellation_2.common.operator_intervention_state_v1 import materialize_operator_intervention_state_v1

    exception = materialize_exception_state_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:41:00Z',
        core1_health_path=fixture['core1_health_path'],
        core3_authority_path=fixture['core3_path'],
        core4_boundary_path=fixture['core4_boundary_path'],
    )
    intervention = materialize_operator_intervention_state_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        emitted_at_utc='2026-04-11T14:41:30Z',
        review_status='COMPLETED',
        override_status='ACTIVE',
        override_scope_class='POLICY_VERSION_SELECTION',
        override_expiry_utc='2026-04-11T15:41:30Z',
        acknowledgement_status='ACKNOWLEDGED',
        human_decision_class='FORCE_POLICY_VERSION_SELECTION',
        exception_state_path=exception.exception_path,
        core3_authority_path=fixture['core3_path'],
        core4_boundary_path=fixture['core4_boundary_path'],
    )
    projection = materialize_strategy_policy_projection_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:42:00Z',
        exception_state_path=exception.exception_path,
        operator_intervention_state_path=intervention.intervention_path,
    )

    assert 'CONFLICTING_POLICY_POSTURE' in projection.payload['blocker_codes']


def test_orchestration_trigger_generation_is_deterministic_and_deduplicated(tmp_path: Path) -> None:
    fixture = prepare_upper_layer_stack(tmp_path)
    policy = materialize_strategy_policy_projection_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:43:00Z',
    )
    first = materialize_orchestration_plane_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        policy_projection_path=policy.policy_path,
        evaluated_at_utc='2026-04-11T14:44:00Z',
        cadence_seconds=60,
        session_phase='REGULAR',
    )
    second = materialize_orchestration_plane_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        policy_projection_path=policy.policy_path,
        evaluated_at_utc='2026-04-11T14:44:00Z',
        cadence_seconds=60,
        session_phase='REGULAR',
        prior_state_path=first.state_path,
    )

    assert first.trigger_payload is not None
    assert first.trigger_payload['trigger_target_type'] == 'CORE3_REEVALUATION_REQUEST'
    assert second.trigger_path is None
    assert 'TRIGGER_DEDUPLICATED' in second.state_payload['reason_codes']
    assert second.state_payload['artifact_driven_only'] is True
    assert 'authorized_post_entry_payload' not in json.dumps(second.state_payload, sort_keys=True)


def test_orchestration_alert_cadence_is_explicit_and_artifact_only(tmp_path: Path) -> None:
    fixture = prepare_upper_layer_stack(tmp_path, orphan_count=1)
    from constellation_2.common.exception_state_v1 import materialize_exception_state_v1

    exception = materialize_exception_state_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:45:00Z',
        core1_health_path=fixture['core1_health_path'],
        core3_authority_path=fixture['core3_path'],
        core4_boundary_path=fixture['core4_boundary_path'],
    )
    policy = materialize_strategy_policy_projection_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:45:30Z',
        exception_state_path=exception.exception_path,
    )
    orchestration = materialize_orchestration_plane_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        policy_projection_path=policy.policy_path,
        evaluated_at_utc='2026-04-11T14:46:00Z',
        cadence_seconds=60,
        session_phase='REGULAR',
        exception_state_path=exception.exception_path,
    )

    assert orchestration.state_payload['pending_alerts']
    assert orchestration.trigger_payload is not None
    assert orchestration.trigger_payload['trigger_class'] == 'REVIEW'
    assert orchestration.trigger_payload['trigger_target_type'] == 'OPERATOR_REVIEW_REQUEST'
