from __future__ import annotations

import hashlib
from pathlib import Path

from constellation_2.common.exception_state_v1 import materialize_exception_state_v1
from constellation_2.common.operator_intervention_state_v1 import materialize_operator_intervention_state_v1
from constellation_2.common.tests.upper_layer_test_support_v1 import prepare_upper_layer_stack


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_partial_fill_exception_classification(tmp_path: Path) -> None:
    fixture = prepare_upper_layer_stack(tmp_path, current_working_orders=[{'order_id': '101', 'status': 'SUBMITTED'}])
    result = materialize_exception_state_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:47:00Z',
        core1_health_path=fixture['core1_health_path'],
    )

    assert result.payload['exception_class'] == 'PARTIAL_FILL'
    assert result.payload['constraint_posture'] == 'CORE3_REENTRY_REQUIRED'


def test_reconnect_recovery_classification(tmp_path: Path) -> None:
    fixture = prepare_upper_layer_stack(tmp_path)
    health_path = fixture['core1_health_path']
    payload = __import__('json').loads(Path(health_path).read_text(encoding='utf-8'))
    payload['reconnect_status'] = 'RECONNECT_RECOVERED'
    payload['gap_status'] = 'GAP_UNRESOLVED'
    Path(health_path).write_text(__import__('json').dumps(payload, sort_keys=True, separators=(',', ':')) + '\n', encoding='utf-8')
    result = materialize_exception_state_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:48:00Z',
        core1_health_path=health_path,
    )

    assert result.payload['exception_class'] == 'RECONNECT_RECOVERY'
    assert 'RECONNECT_RECOVERY_PENDING' in result.payload['reason_codes']


def test_manual_ib_change_and_orphan_classification_are_explicit(tmp_path: Path) -> None:
    manual_fixture = prepare_upper_layer_stack(tmp_path / 'manual', ownership_classification='FOREIGN_MANUAL')
    manual = materialize_exception_state_v1(
        core2_trade_dir=manual_fixture['core2_trade_dir'],
        execution_root=manual_fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:49:00Z',
        core1_health_path=manual_fixture['core1_health_path'],
    )
    orphan_fixture = prepare_upper_layer_stack(tmp_path / 'orphan', orphan_count=1)
    orphan = materialize_exception_state_v1(
        core2_trade_dir=orphan_fixture['core2_trade_dir'],
        execution_root=orphan_fixture['execution_root'],
        evaluated_at_utc='2026-04-11T14:49:30Z',
        core1_health_path=orphan_fixture['core1_health_path'],
    )

    assert manual.payload['exception_class'] == 'MANUAL_IB_CHANGE'
    assert orphan.payload['exception_class'] == 'ORPHAN_ORDER'
    assert orphan.payload['classification_status'] != 'CLEAR'


def test_intervention_records_review_expiry_and_no_truth_rewrite(tmp_path: Path) -> None:
    fixture = prepare_upper_layer_stack(tmp_path)
    core2_state_path = fixture['core2_trade_dir'] / 'incorporated_broker_trade_state.v1.json'
    before_sha = _sha256(core2_state_path)
    result = materialize_operator_intervention_state_v1(
        core2_trade_dir=fixture['core2_trade_dir'],
        execution_root=fixture['execution_root'],
        emitted_at_utc='2026-04-11T14:50:00Z',
        review_status='PENDING',
        override_status='ACTIVE',
        override_scope_class='ALLOW_CORE3_REEVALUATION',
        override_expiry_utc='2026-04-11T14:40:00Z',
        acknowledgement_status='PENDING',
        human_decision_class='REQUEST_REEVALUATION',
        core3_authority_path=fixture['core3_path'],
        core4_boundary_path=fixture['core4_boundary_path'],
    )
    after_sha = _sha256(core2_state_path)

    assert result.payload['review_status'] == 'PENDING'
    assert result.payload['override_status'] == 'EXPIRED'
    assert result.payload['downstream_execution_posture'] == 'REENTER_CORE3_AND_CORE4_REQUIRED'
    assert before_sha == after_sha
