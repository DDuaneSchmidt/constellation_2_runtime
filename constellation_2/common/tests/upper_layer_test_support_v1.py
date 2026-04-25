from __future__ import annotations

from pathlib import Path

from constellation_2.common.lifecycle_action_authority_v1 import materialize_lifecycle_action_authority_set_v1
from constellation_2.common.paper_session_fact_plane_v1 import atomic_write_validated_json_v1
from constellation_2.common.post_entry_action_request_v1 import seal_post_entry_action_request_v1
from constellation_2.common.post_entry_boundary_snapshot_binding_v1 import build_post_entry_boundary_snapshot_binding_v1
from constellation_2.common.post_entry_submit_boundary_v1 import evaluate_post_entry_submit_boundary_v1
from constellation_2.common.tests.test_lifecycle_action_authority_v1 import (
    DAY_UTC,
    MATERIALIZATION_SET_ID,
    TRADE_ID,
    _write_core2_trade,
)
from constellation_2.common.tests.test_post_entry_submit_boundary_v1 import (
    _core2_snapshot,
    _core3_projection,
    _identity_snapshot,
    _request_payload,
)


def prepare_upper_layer_stack(
    tmp_path: Path,
    *,
    ownership_classification: str = 'CONSTELLATION_OWNED',
    orphan_count: int = 0,
    current_working_orders: list[dict] | None = None,
) -> dict:
    execution_root = (tmp_path / 'truth_sleeves' / 'PRIMARY' / 'PAPER').resolve()
    normalized_working_orders = None
    if current_working_orders is not None:
        normalized_working_orders = []
        for index, row in enumerate(current_working_orders):
            normalized_working_orders.append({
                'order_key': str(row.get('order_key') or f'WORKING-{index}'),
                'order_id': str(row.get('order_id') or '101'),
                'perm_id': str(row.get('perm_id') or '555001'),
                'status': str(row.get('status') or 'SUBMITTED'),
                'action': str(row.get('action') or 'SELL'),
                'total_quantity': str(row.get('total_quantity') or '10'),
                'filled_quantity': str(row.get('filled_quantity') or '5'),
                'remaining_quantity': str(row.get('remaining_quantity') or '5'),
                'observed_utc': str(row.get('observed_utc') or f'{DAY_UTC}T14:29:30Z'),
                'fact_record_ids': list(row.get('fact_record_ids') or ['a' * 64]),
            })
    core2_materialization_dir = _write_core2_trade(
        execution_root,
        ownership_classification=ownership_classification,
        orphan_count=orphan_count,
        current_working_orders=normalized_working_orders,
    )
    core2_trade_dir = core2_materialization_dir / 'trades' / TRADE_ID
    core3_result = materialize_lifecycle_action_authority_set_v1(
        core2_materialization_dir=core2_materialization_dir,
        execution_root=execution_root,
        evaluated_at_utc=f'{DAY_UTC}T14:31:00Z',
    )
    core3_path = (
        execution_root
        / 'lifecycle_action_authority_v1'
        / 'materializations'
        / DAY_UTC
        / core3_result['materialization_set_id']
        / 'trades'
        / TRADE_ID
        / 'lifecycle_action_authority.v1.json'
    ).resolve()
    core1_health_ref = atomic_write_validated_json_v1(
        path=execution_root / 'reports' / 'broker_observation_health_v1' / DAY_UTC / 'broker_observation_health.v1.json',
        payload={
            'schema_id': 'broker_observation_health',
            'schema_version': 'v1',
            'generated_utc': f'{DAY_UTC}T14:29:00Z',
            'day_utc': DAY_UTC,
            'evaluation_utc': f'{DAY_UTC}T14:29:00Z',
            'authority_owner': 'broker_observation_health_v1',
            'trust_rule_owner': 'broker_observation_health_v1',
            'execution_root_path': str(execution_root),
            'raw_journal_ref': {'artifact_path': '/tmp/broker_raw.jsonl', 'artifact_sha256': '1' * 64},
            'fact_ledger_refs': {
                'observation_session_fact': {'artifact_path': '/tmp/observation_session_fact.jsonl', 'artifact_sha256': '2' * 64},
                'observed_order_fact': {'artifact_path': '/tmp/observed_order_fact.jsonl', 'artifact_sha256': '3' * 64},
                'observed_order_status_fact': {'artifact_path': '/tmp/observed_order_status_fact.jsonl', 'artifact_sha256': '4' * 64},
                'observed_fill_fact': {'artifact_path': '/tmp/observed_fill_fact.jsonl', 'artifact_sha256': '5' * 64},
                'observed_position_fact': {'artifact_path': '/tmp/observed_position_fact.jsonl', 'artifact_sha256': '6' * 64},
            },
            'current_state': 'TRUSTED',
            'downstream_trust_verdict': 'TRUSTED',
            'downstream_consumption_posture': 'NORMAL_CONSUMPTION',
            'may_consume_normally': True,
            'may_consume_with_degraded_posture': True,
            'must_fail_closed': False,
            'freshness_status': 'FRESH',
            'freshness_age_seconds': 15,
            'session_status': 'HEALTHY',
            'sequence_status': 'OK',
            'replay_status': 'NOT_OBSERVED',
            'reconnect_status': 'NONE',
            'gap_status': 'NONE',
            'attribution_status': 'ATTRIBUTED',
            'event_identity_rule_version': 'broker_fact_identity_v1',
            'blocker_codes': [],
            'degraded_codes': [],
            'counts': {
                'raw_record_count': 1,
                'observation_session_fact_count': 1,
                'observed_order_fact_count': 1,
                'observed_order_status_fact_count': 1,
                'observed_fill_fact_count': 1,
                'observed_position_fact_count': 1,
                'attributed_raw_record_count': 1,
                'partial_raw_record_count': 0,
                'ambiguous_raw_record_count': 0,
                'foreign_raw_record_count': 0,
                'unresolved_raw_record_count': 0,
                'duplicate_fact_count': 0,
                'replay_overlap_fact_count': 0,
                'conflicting_duplicate_fact_count': 0,
                'replay_session_fact_count': 0,
                'replay_uncertain_fact_count': 0,
            },
            'summary': 'Upper-layer fixture core1 health.',
            'recommended_operator_action': 'NONE',
        },
        schema_relpath='governance/04_DATA/SCHEMAS/C2/REPORTS/broker_observation_health.v1.schema.json',
    )
    request = seal_post_entry_action_request_v1(_request_payload())
    binding, bound_core2, bound_core3, bound_identity = build_post_entry_boundary_snapshot_binding_v1(
        request=request,
        core2_snapshot=_core2_snapshot(),
        core2_artifact_path='/tmp/core2_snapshot.json',
        core3_projection=_core3_projection(),
        execution_identity_snapshot=_identity_snapshot(),
        evaluated_at_utc=f'{DAY_UTC}T14:32:00Z',
    )
    boundary_bundle = evaluate_post_entry_submit_boundary_v1(
        request=request,
        binding=binding,
        core2_snapshot=bound_core2,
        core3_projection=bound_core3,
        execution_identity_snapshot=bound_identity,
        evaluated_at_utc=f'{DAY_UTC}T14:32:00Z',
    )
    core4_ref = atomic_write_validated_json_v1(
        path=execution_root / 'reports' / 'post_entry_submit_boundary_v1' / DAY_UTC / 'post_entry_submit_boundary.v1.json',
        payload=boundary_bundle.boundary.to_dict(),
        schema_relpath='governance/04_DATA/SCHEMAS/C2/TRADE_STATE/post_entry_submit_boundary.v1.schema.json',
    )
    return {
        'execution_root': execution_root,
        'core2_trade_dir': core2_trade_dir,
        'core2_materialization_dir': core2_materialization_dir,
        'core3_path': core3_path,
        'core1_health_path': core1_health_ref.path,
        'core4_boundary_path': core4_ref.path,
        'day_utc': DAY_UTC,
        'trade_identity_id': TRADE_ID,
        'core2_materialization_set_id': MATERIALIZATION_SET_ID,
        'core3_materialization_set_id': core3_result['materialization_set_id'],
    }
