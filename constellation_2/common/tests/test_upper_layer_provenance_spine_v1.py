from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.exception_state_v1 import materialize_exception_state_v1
from constellation_2.common.operator_intervention_state_v1 import materialize_operator_intervention_state_v1
from constellation_2.common.orchestration_plane_v1 import materialize_orchestration_plane_v1
from constellation_2.common.strategy_policy_projection_v1 import materialize_strategy_policy_projection_v1
from constellation_2.common.tests.upper_layer_test_support_v1 import prepare_upper_layer_stack


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding='utf-8'))


def test_upper_artifacts_carry_lower_core_refs_and_reentry_paths(tmp_path: Path) -> None:
    fixture = prepare_upper_layer_stack(tmp_path, orphan_count=1)
    exception = materialize_exception_state_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:51:00Z',
        core1_health_path=fixture['core1_health_path'],
        core3_authority_path=fixture['core3_path'],
        core4_boundary_path=fixture['core4_boundary_path'],
    )
    intervention = materialize_operator_intervention_state_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        emitted_at_utc='2026-04-11T14:51:30Z',
        review_status='COMPLETED',
        override_status='ACTIVE',
        override_scope_class='ALLOW_CORE3_REEVALUATION',
        override_expiry_utc='2026-04-11T15:51:30Z',
        acknowledgement_status='ACKNOWLEDGED',
        human_decision_class='REQUEST_REEVALUATION',
        exception_state_path=exception.exception_path,
        core3_authority_path=fixture['core3_path'],
        core4_boundary_path=fixture['core4_boundary_path'],
    )
    policy = materialize_strategy_policy_projection_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:52:00Z',
        exception_state_path=exception.exception_path,
        operator_intervention_state_path=intervention.intervention_path,
    )
    orchestration = materialize_orchestration_plane_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        policy_projection_path=policy.policy_path,
        evaluated_at_utc='2026-04-11T14:52:30Z',
        cadence_seconds=60,
        session_phase='REGULAR',
        exception_state_path=exception.exception_path,
        operator_intervention_state_path=intervention.intervention_path,
    )

    policy_prov = _load(policy.provenance_path)
    exception_prov = _load(exception.provenance_path)
    intervention_prov = _load(intervention.provenance_path)
    trigger_prov = _load(orchestration.trigger_provenance_path) if orchestration.trigger_provenance_path else {}

    assert policy.payload['core2_refs']['incorporated_state_ref']['artifact_path'].endswith('incorporated_broker_trade_state.v1.json')
    assert policy_prov['core2_refs']['trade_identity_ref']['artifact_path'].endswith('trade_identity.v1.json')
    assert exception_prov['executable_reentry_path_refs']['core3_contract_ref']['artifact_path'].endswith('lifecycle_action_authority_v1.contract.md')
    assert intervention_prov['executable_reentry_path_refs']['current_core3_refs']
    assert intervention_prov['executable_reentry_path_refs']['current_core4_refs']
    assert trigger_prov['cross_upper_plane_refs']


def test_no_upper_layer_artifact_directly_authorizes_transmission_or_becomes_operator_truth(tmp_path: Path) -> None:
    fixture = prepare_upper_layer_stack(tmp_path)
    policy = materialize_strategy_policy_projection_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:53:00Z',
    )
    orchestration = materialize_orchestration_plane_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        policy_projection_path=policy.policy_path,
        evaluated_at_utc='2026-04-11T14:53:30Z',
        cadence_seconds=60,
        session_phase='REGULAR',
    )
    payloads = [policy.payload, orchestration.state_payload]
    if orchestration.trigger_payload is not None:
        payloads.append(orchestration.trigger_payload)

    for payload in payloads:
        text = json.dumps(payload, sort_keys=True)
        assert 'transmission_authorization_status' not in text
        assert 'authorized_post_entry_payload' not in text
        assert 'operator_trade_health' not in text
