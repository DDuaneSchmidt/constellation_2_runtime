from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.advisory.assumption_manifest_service_v1 import write_assumption_manifest_v1
from constellation_2.common.advisory.execution_intent_service_v1 import build_execution_intent_v1
from constellation_2.common.advisory.household_snapshot_service_v1 import build_household_snapshot_v1
from constellation_2.common.advisory.investor_intent_service_v1 import build_investor_intent_v1
from constellation_2.common.advisory.policy_service_v1 import build_policy_v1
from constellation_2.common.advisory.portfolio_intent_service_v1 import build_portfolio_intent_v1
from constellation_2.common.advisory.promotion_decision_service_v1 import build_promotion_decision_v1
from constellation_2.common.advisor_bridge.promotion_record_service_v2 import build_promotion_record_v2


def _raw_intake() -> dict[str, object]:
    return {
        'household_id': 'household-allocation-1',
        'intent_version': 'v1',
        'created_at': '2026-04-12T12:00:00Z',
        'effective_at': '2026-04-12T12:00:00Z',
        'actor_source': 'advisor_operator',
        'goals': ['fund_retirement'],
        'constraints': {
            'time_horizon': 'long_term',
            'risk_preference': 'balanced',
            'liquidity_requirement': 'moderate',
            'concentration_preference': 'diversified',
            'prohibited_exposures': [],
            'account_role_preferences': ['taxable_growth'],
        },
        'operating_mode': 'automation_allowed',
        'approval_preferences': {
            'automation_preference': 'proposal_only',
            'approval_preference': 'explicit_approval_required',
        },
        'compiler_inputs_manifest_refs': ['intake_normalization_manifest_v1'],
        'lineage_parent_refs': [],
    }


def _raw_manifest() -> dict[str, object]:
    return {
        'manifest_version': 'v1',
        'manifest_type': 'policy_rule_interpretation',
        'created_at': '2026-04-12T12:05:00Z',
        'effective_at': '2026-04-12T12:05:00Z',
        'actor_source': 'governed_advisory_operator',
        'compatible_compiler_versions': ['policy_compiler_v1'],
        'assumption_payload': {
            'allowed_exposure_rules_by_risk': {
                'conservative': ['allow:cash', 'allow:investment_grade_bonds', 'allow:broad_equity'],
                'balanced': ['allow:cash', 'allow:investment_grade_bonds', 'allow:broad_equity', 'allow:diversified_equity'],
                'growth': ['allow:cash', 'allow:broad_equity', 'allow:diversified_equity', 'allow:higher_volatility_equity'],
            },
            'concentration_cap_rules_by_preference': {
                'diversified': ['max_single_exposure_pct:5'],
                'moderate': ['max_single_exposure_pct:10'],
                'concentrated': ['max_single_exposure_pct:20'],
            },
            'liquidity_floor_rules_by_requirement': {
                'low': ['min_liquidity_bucket:low'],
                'moderate': ['min_liquidity_bucket:moderate'],
                'high': ['min_liquidity_bucket:high'],
            },
            'rebalance_philosophy_by_operating_mode': {
                'advisory_only': 'manual_rebalance_only',
                'manual_execution_only': 'manual_rebalance_only',
                'automation_allowed': 'policy_guided_rebalance',
            },
            'promotion_gate_rules_by_operating_mode': {
                'advisory_only': ['min_trade_shares:1', 'max_trade_entries:1'],
                'manual_execution_only': ['min_trade_shares:1', 'max_trade_entries:1'],
                'automation_allowed': ['require_execution_eligible_snapshot', 'min_trade_shares:1', 'max_trade_entries:1'],
            },
        },
        'reason_codes': ['ASSUMPTION_MANIFEST_ACTIVE'],
        'status': 'ACTIVE',
    }


def _positions_snapshot(*, day_utc: str, qty: int) -> dict[str, object]:
    return {
        'schema_id': 'C2_POSITIONS_SNAPSHOT_V4',
        'schema_version': 4,
        'produced_utc': f'{day_utc}T13:00:00Z',
        'day_utc': day_utc,
        'producer': {
            'repo': 'constellation',
            'git_sha': 'abcdef1',
            'module': 'positions_snapshot_builder_v4',
        },
        'status': 'OK',
        'reason_codes': ['POSITIONS_OK'],
        'input_manifest': [
            {
                'type': 'execution_evidence',
                'path': '/tmp/input.json',
                'sha256': 'a' * 64,
                'day_utc': day_utc,
                'producer': 'test',
            }
        ],
        'positions': {
            'currency': 'USD',
            'asof_utc': f'{day_utc}T12:59:00Z',
            'notes': [],
            'items': [
                {
                    'position_id': 'position-0000000001',
                    'engine_id': 'engine-alpha',
                    'instrument': {
                        'kind': 'EQUITY',
                        'symbol': 'SPY',
                        'currency': 'USD',
                        'ib_conId': 756733,
                        'ib_localSymbol': 'SPY',
                    },
                    'qty': qty,
                    'avg_cost_cents': 5000,
                    'market_exposure_type': 'DEFINED_RISK',
                    'max_loss_cents': qty * 5000,
                    'opened_day_utc': day_utc,
                    'status': 'OPEN',
                }
            ],
        },
    }


def _cash_snapshot(*, day_utc: str, account_id: str, cash_total_cents: int) -> dict[str, object]:
    return {
        'schema_id': 'C2_CASH_LEDGER_SNAPSHOT_V1',
        'schema_version': 1,
        'produced_utc': f'{day_utc}T13:05:00Z',
        'day_utc': day_utc,
        'authority_basis': 'broker_account_values',
        'producer': {
            'repo': 'constellation',
            'git_sha': 'abcdef1',
            'module': 'cash_ledger_snapshot_builder_v1',
        },
        'status': 'OK',
        'reason_codes': ['CASH_OK'],
        'input_manifest': [
            {
                'type': 'broker_account_values',
                'path': '/tmp/cash.json',
                'sha256': 'b' * 64,
                'day_utc': day_utc,
                'producer': 'test',
            }
        ],
        'snapshot': {
            'observed_at_utc': f'{day_utc}T13:04:00Z',
            'currency': 'USD',
            'cash_total_cents': cash_total_cents,
            'nlv_total_cents': cash_total_cents,
            'available_funds_cents': cash_total_cents,
            'excess_liquidity_cents': cash_total_cents,
            'account_id': account_id,
            'notes': [],
        },
    }


def _asset_marks(*, asof_utc: str, spy_mark_price_cents: int) -> list[dict[str, object]]:
    return [
        {
            'record_ref': 'asset_marks:2026-04-12',
            'snapshot': {
                'asof_utc': asof_utc,
                'marks': [
                    {
                        'symbol': 'SPY',
                        'asset_type': 'EQUITY',
                        'currency': 'USD',
                        'mark_price_cents': spy_mark_price_cents,
                    }
                ],
            },
        }
    ]


def _account_registry_snapshot() -> dict[str, object]:
    return {
        'source_refs': ['registry_ref:BROKER_ACCOUNT_SCOPE_V1'],
        'accounts': [
            {
                'account_id': 'DU1234567',
                'scope_role': 'taxable_primary',
                'source_ref': 'account_registry:DU1234567',
                'source_type': 'ib_account_registry',
                'verification_status': 'VERIFIED',
            }
        ],
    }


def _build_policy(
    tmp_path: Path,
    *,
    allocation_targets: list[dict[str, object]] | None = None,
    rebalance_threshold: str | None = None,
    minimum_trade_value_cents: int | None = None,
):
    intent = build_investor_intent_v1(_raw_intake())
    manifest, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(), output_root=str(tmp_path))
    return build_policy_v1(
        investor_intent=intent,
        compiler_version='policy_compiler_v1',
        assumption_manifest_refs=[manifest.assumption_manifest_id],
        regime_rule_refs=['retail_household_regime_v1'],
        output_root=str(tmp_path),
        allocation_targets=[] if allocation_targets is None else allocation_targets,
        rebalance_threshold=rebalance_threshold,
        minimum_trade_value_cents=minimum_trade_value_cents,
    )


def _build_snapshot(
    policy,
    *,
    qty: int,
    cash_total_cents: int,
    mark_price_cents: int | None,
    mark_asof_utc: str = '2026-04-12T13:10:00Z',
):
    governed_asset_mark_inputs = [] if mark_price_cents is None else _asset_marks(asof_utc=mark_asof_utc, spy_mark_price_cents=mark_price_cents)
    return build_household_snapshot_v1(
        policy=policy,
        created_at='2026-04-12T14:00:00Z',
        effective_at='2026-04-12T14:00:00Z',
        actor_source='household_snapshot_builder',
        account_registry_snapshot=_account_registry_snapshot(),
        verified_positions_inputs=[
            {
                'account_id': 'DU1234567',
                'record_ref': 'positions_snapshot:DU1234567:2026-04-12',
                'snapshot': _positions_snapshot(day_utc='2026-04-12', qty=qty),
            }
        ],
        verified_cash_inputs=[
            {
                'account_id': 'DU1234567',
                'record_ref': 'cash_snapshot:DU1234567:2026-04-12',
                'snapshot': _cash_snapshot(day_utc='2026-04-12', account_id='DU1234567', cash_total_cents=cash_total_cents),
            }
        ],
        governed_asset_mark_inputs=governed_asset_mark_inputs,
    )


def _execution_profile() -> dict[str, object]:
    return {
        'environment': 'PAPER',
        'sleeve_id': 'CORE',
        'operation_type': 'fresh_paper_entry',
        'account_id': 'DU1234567',
        'engine_id': 'engine-alpha',
        'order_terms': {
            'order_type': 'MARKET',
            'limit_price': None,
            'time_in_force': 'DAY',
        },
    }


def test_value_basis_valid_deterministic_and_mark_sensitive(tmp_path: Path) -> None:
    policy = _build_policy(tmp_path)
    snapshot_a = _build_snapshot(policy, qty=10, cash_total_cents=50000, mark_price_cents=5000)
    snapshot_b = _build_snapshot(policy, qty=10, cash_total_cents=50000, mark_price_cents=5000)
    snapshot_c = _build_snapshot(policy, qty=10, cash_total_cents=50000, mark_price_cents=5100)

    assert snapshot_a.value_basis['status'] == 'VALID'
    assert snapshot_a.value_basis['value_basis_id'] == snapshot_b.value_basis['value_basis_id']
    assert snapshot_a.household_snapshot_id == snapshot_b.household_snapshot_id
    assert snapshot_a.value_basis['value_basis_id'] != snapshot_c.value_basis['value_basis_id']
    assert snapshot_a.household_snapshot_id != snapshot_c.household_snapshot_id


def test_value_basis_missing_and_stale_marks_block_value_allocation(tmp_path: Path) -> None:
    policy = _build_policy(
        tmp_path,
        allocation_targets=[
            {'symbol': 'SPY', 'asset_type': 'EQUITY', 'currency': 'USD', 'target_weight': '0.500000'},
            {'symbol': 'CASH_USD', 'asset_type': 'CASH', 'currency': 'USD', 'target_weight': '0.500000'},
        ],
        rebalance_threshold='0.050000',
        minimum_trade_value_cents=1000,
    )
    missing_marks_snapshot = _build_snapshot(policy, qty=10, cash_total_cents=50000, mark_price_cents=None)
    missing_marks_intent = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=missing_marks_snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
    )
    assert missing_marks_snapshot.value_basis['status'] == 'BLOCKED'
    assert missing_marks_intent.current_value_allocation['status'] == 'BLOCKED'

    stale_marks_snapshot = _build_snapshot(
        policy,
        qty=10,
        cash_total_cents=50000,
        mark_price_cents=5000,
        mark_asof_utc='2026-04-11T13:10:00Z',
    )
    stale_marks_intent = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=stale_marks_snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
    )
    assert 'GOVERNED_MARKS_STALE' in stale_marks_snapshot.value_basis['reason_codes']
    assert stale_marks_intent.current_value_allocation['status'] == 'BLOCKED'


def test_policy_target_allocation_validation_rules(tmp_path: Path) -> None:
    valid_policy = _build_policy(
        tmp_path / 'valid',
        allocation_targets=[
            {'symbol': 'SPY', 'asset_type': 'EQUITY', 'currency': 'USD', 'target_weight': '0.600000'},
            {'symbol': 'CASH_USD', 'asset_type': 'CASH', 'currency': 'USD', 'target_weight': '0.400000'},
        ],
        rebalance_threshold='0.050000',
        minimum_trade_value_cents=1000,
    )
    assert valid_policy.allocation_policy_mode == 'TARGET_WEIGHTS'
    assert len(valid_policy.allocation_targets) == 2

    with pytest.raises(ValueError, match='DUPLICATE_ALLOCATION_TARGET_SYMBOL'):
        _build_policy(
            tmp_path / 'dupe',
            allocation_targets=[
                {'symbol': 'SPY', 'asset_type': 'EQUITY', 'currency': 'USD', 'target_weight': '0.500000'},
                {'symbol': 'SPY', 'asset_type': 'EQUITY', 'currency': 'USD', 'target_weight': '0.500000'},
            ],
            rebalance_threshold='0.050000',
            minimum_trade_value_cents=1000,
        )

    with pytest.raises(ValueError, match='ALLOCATION_TARGETS_SUM_INVALID'):
        _build_policy(
            tmp_path / 'sum',
            allocation_targets=[
                {'symbol': 'SPY', 'asset_type': 'EQUITY', 'currency': 'USD', 'target_weight': '0.700000'},
                {'symbol': 'CASH_USD', 'asset_type': 'CASH', 'currency': 'USD', 'target_weight': '0.200000'},
            ],
            rebalance_threshold='0.050000',
            minimum_trade_value_cents=1000,
        )

    with pytest.raises(ValueError, match='ALLOCATION_POLICY_FIELDS_INCOMPLETE'):
        _build_policy(
            tmp_path / 'partial',
            allocation_targets=[
                {'symbol': 'SPY', 'asset_type': 'EQUITY', 'currency': 'USD', 'target_weight': '1.000000'},
            ],
            rebalance_threshold=None,
            minimum_trade_value_cents=1000,
        )


def test_allocation_portfolio_intent_perfect_alignment_no_action(tmp_path: Path) -> None:
    policy = _build_policy(
        tmp_path,
        allocation_targets=[
            {'symbol': 'SPY', 'asset_type': 'EQUITY', 'currency': 'USD', 'target_weight': '0.500000'},
            {'symbol': 'CASH_USD', 'asset_type': 'CASH', 'currency': 'USD', 'target_weight': '0.500000'},
        ],
        rebalance_threshold='0.050000',
        minimum_trade_value_cents=1000,
    )
    snapshot = _build_snapshot(policy, qty=10, cash_total_cents=50000, mark_price_cents=5000)
    intent = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
    )
    decision = build_promotion_decision_v1(
        policy=policy,
        household_snapshot=snapshot,
        portfolio_intent=intent,
        created_at='2026-04-12T15:05:00Z',
        effective_at='2026-04-12T15:05:00Z',
        actor_source='promotion_decision_builder',
    )

    assert intent.current_value_allocation['status'] == 'READY'
    assert intent.allocation_drift['status'] == 'READY'
    assert intent.action_needed is False
    assert intent.allocation_drift['max_abs_drift_weight'] == '0.000000'
    assert decision.outcome == 'no_action'


def test_allocation_actionability_promotion_and_execution_shaping(tmp_path: Path) -> None:
    policy = _build_policy(
        tmp_path,
        allocation_targets=[
            {'symbol': 'CASH_USD', 'asset_type': 'CASH', 'currency': 'USD', 'target_weight': '1.000000'},
        ],
        rebalance_threshold='0.050000',
        minimum_trade_value_cents=1000,
    )
    snapshot = _build_snapshot(policy, qty=10, cash_total_cents=40000, mark_price_cents=6000)
    intent = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
    )
    decision = build_promotion_decision_v1(
        policy=policy,
        household_snapshot=snapshot,
        portfolio_intent=intent,
        created_at='2026-04-12T15:05:00Z',
        effective_at='2026-04-12T15:05:00Z',
        actor_source='promotion_decision_builder',
    )
    record = build_promotion_record_v2(
        policy=policy,
        portfolio_intent=intent,
        promotion_decision=decision,
        produced_utc='2026-04-12T15:10:00Z',
        run_id='allocation-run-1',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    execution_intent = build_execution_intent_v1(
        promotion_record=record,
        created_at_utc='2026-04-12T15:11:00Z',
        effective_at_utc='2026-04-12T15:11:00Z',
        actor_source='execution_intent_builder',
    )

    assert intent.action_needed is True
    assert decision.outcome == 'promote'
    assert len(decision.approved_change_ids) == 1
    assert len(record.approved_delta) == 1
    assert record.approved_delta[0]['side'] == 'SELL'
    assert record.approved_delta[0]['quantity_shares'] == 10
    assert execution_intent.side == 'SELL'
    assert execution_intent.quantity_shares == 10
    assert execution_intent.idempotency_key == record.idempotency_key


def test_below_threshold_drift_produces_no_execution_artifact(tmp_path: Path) -> None:
    policy = _build_policy(
        tmp_path,
        allocation_targets=[
            {'symbol': 'CASH_USD', 'asset_type': 'CASH', 'currency': 'USD', 'target_weight': '1.000000'},
        ],
        rebalance_threshold='0.700000',
        minimum_trade_value_cents=1000,
    )
    snapshot = _build_snapshot(policy, qty=10, cash_total_cents=40000, mark_price_cents=6000)
    intent = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
    )
    decision = build_promotion_decision_v1(
        policy=policy,
        household_snapshot=snapshot,
        portfolio_intent=intent,
        created_at='2026-04-12T15:05:00Z',
        effective_at='2026-04-12T15:05:00Z',
        actor_source='promotion_decision_builder',
    )

    assert decision.outcome == 'no_action'
    record = build_promotion_record_v2(
        policy=policy,
        portfolio_intent=intent,
        promotion_decision=decision,
        produced_utc='2026-04-12T15:10:00Z',
        run_id='allocation-run-2',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    assert record.status == 'NO_ACTION'
    assert not record.approved_delta
    with pytest.raises(ValueError, match='PROMOTION_RECORD_NOT_EXECUTABLE'):
        build_execution_intent_v1(
            promotion_record=record,
            created_at_utc='2026-04-12T15:11:00Z',
            effective_at_utc='2026-04-12T15:11:00Z',
            actor_source='execution_intent_builder',
        )


def test_replay_determinism_allocation_chain(tmp_path: Path) -> None:
    policy = _build_policy(
        tmp_path,
        allocation_targets=[
            {'symbol': 'CASH_USD', 'asset_type': 'CASH', 'currency': 'USD', 'target_weight': '1.000000'},
        ],
        rebalance_threshold='0.050000',
        minimum_trade_value_cents=1000,
    )
    snapshot = _build_snapshot(policy, qty=10, cash_total_cents=40000, mark_price_cents=6000)

    intent_a = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
    )
    intent_b = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
    )
    decision_a = build_promotion_decision_v1(
        policy=policy,
        household_snapshot=snapshot,
        portfolio_intent=intent_a,
        created_at='2026-04-12T15:05:00Z',
        effective_at='2026-04-12T15:05:00Z',
        actor_source='promotion_decision_builder',
    )
    decision_b = build_promotion_decision_v1(
        policy=policy,
        household_snapshot=snapshot,
        portfolio_intent=intent_b,
        created_at='2026-04-12T15:05:00Z',
        effective_at='2026-04-12T15:05:00Z',
        actor_source='promotion_decision_builder',
    )
    record_a = build_promotion_record_v2(
        policy=policy,
        portfolio_intent=intent_a,
        promotion_decision=decision_a,
        produced_utc='2026-04-12T15:10:00Z',
        run_id='allocation-run-deterministic',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    record_b = build_promotion_record_v2(
        policy=policy,
        portfolio_intent=intent_b,
        promotion_decision=decision_b,
        produced_utc='2026-04-12T15:10:00Z',
        run_id='allocation-run-deterministic',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )

    assert intent_a.portfolio_intent_id == intent_b.portfolio_intent_id
    assert decision_a.promotion_decision_id == decision_b.promotion_decision_id
    assert record_a.promotion_record_id == record_b.promotion_record_id
