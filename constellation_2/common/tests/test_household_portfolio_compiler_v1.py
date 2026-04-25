from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.advisory.assumption_manifest_service_v1 import write_assumption_manifest_v1
from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.advisory.household_portfolio_compiler_v1 import (
    build_compiled_constraints_v1,
    build_household_state_snapshot_v1,
    build_policy_snapshot_v1,
    evaluate_portfolio_authorization_v1,
    write_household_portfolio_bundle_v1,
)
from constellation_2.common.advisory.household_snapshot_v1 import HouseholdSnapshotV1
from constellation_2.common.advisory.household_snapshot_service_v1 import write_household_snapshot_v1
from constellation_2.common.advisory.investor_intent_service_v1 import write_investor_intent_v1
from constellation_2.common.advisory.policy_service_v1 import write_policy_v1


def _raw_intake(*, household_id: str = 'household-1') -> dict[str, object]:
    return {
        'household_id': household_id,
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
            'prohibited_exposures': ['single_stock_margin'],
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


def _account_registry_snapshot(*account_ids: str) -> dict[str, object]:
    return {
        'source_refs': ['registry_ref:BROKER_ACCOUNT_SCOPE_V1'],
        'accounts': [
            {
                'account_id': account_id,
                'scope_role': 'taxable_primary',
                'source_ref': f'account_registry:{account_id}',
                'source_type': 'ib_account_registry',
                'verification_status': 'VERIFIED',
            }
            for account_id in account_ids
        ],
    }


def _positions_snapshot(*, day_utc: str, symbol: str = 'SPY', position_id: str = 'position-0000000001') -> dict[str, object]:
    return {
        'schema_id': 'C2_POSITIONS_SNAPSHOT_V4',
        'schema_version': 4,
        'produced_utc': f'{day_utc}T13:00:00Z',
        'day_utc': day_utc,
        'producer': {'repo': 'constellation', 'git_sha': 'abcdef1', 'module': 'positions_snapshot_builder_v4'},
        'status': 'OK',
        'reason_codes': ['POSITIONS_OK'],
        'input_manifest': [{'type': 'execution_evidence', 'path': '/tmp/input.json', 'sha256': 'a' * 64, 'day_utc': day_utc, 'producer': 'test'}],
        'positions': {
            'currency': 'USD',
            'asof_utc': f'{day_utc}T12:59:00Z',
            'notes': [],
            'items': [
                {
                    'position_id': position_id,
                    'engine_id': 'engine-alpha',
                    'instrument': {
                        'kind': 'EQUITY',
                        'symbol': symbol,
                        'currency': 'USD',
                        'ib_conId': 756733,
                        'ib_localSymbol': symbol,
                    },
                    'qty': 10,
                    'avg_cost_cents': 50000,
                    'market_exposure_type': 'DEFINED_RISK',
                    'max_loss_cents': 500000,
                    'opened_day_utc': day_utc,
                    'status': 'OPEN',
                }
            ],
        },
    }


def _cash_snapshot(*, day_utc: str, account_id: str, observed_day_utc: str | None = None, cash_total_cents: int = 1_000_000) -> dict[str, object]:
    observed_day = observed_day_utc or day_utc
    return {
        'schema_id': 'C2_CASH_LEDGER_SNAPSHOT_V1',
        'schema_version': 1,
        'produced_utc': f'{day_utc}T13:05:00Z',
        'day_utc': day_utc,
        'authority_basis': 'broker_account_values',
        'producer': {'repo': 'constellation', 'git_sha': 'abcdef1', 'module': 'cash_ledger_snapshot_builder_v1'},
        'status': 'OK',
        'reason_codes': ['CASH_OK'],
        'input_manifest': [{'type': 'broker_account_values', 'path': '/tmp/cash.json', 'sha256': 'b' * 64, 'day_utc': day_utc, 'producer': 'test'}],
        'snapshot': {
            'observed_at_utc': f'{observed_day}T13:04:00Z',
            'currency': 'USD',
            'cash_total_cents': cash_total_cents,
            'nlv_total_cents': cash_total_cents + 500000,
            'available_funds_cents': max(0, cash_total_cents - 10000),
            'excess_liquidity_cents': max(0, cash_total_cents - 5000),
            'account_id': account_id,
            'notes': [],
        },
    }


def _asset_mark_snapshot(*, day_utc: str, symbol: str = 'SPY', mark_price_cents: int = 50000) -> dict[str, object]:
    return {
        'record_ref': f'asset_marks:{day_utc}:{symbol}',
        'snapshot': {
            'asof_utc': f'{day_utc}T13:01:00Z',
            'marks': [
                {
                    'symbol': symbol,
                    'asset_type': 'EQUITY',
                    'currency': 'USD',
                    'mark_price_cents': mark_price_cents,
                }
            ],
        },
    }


def _policy_inputs(
    *,
    max_single_name_concentration: str = '1.000000',
    max_portfolio_drawdown: str = '0.250000',
    disallowed_asset_classes: tuple[str, ...] = (),
    protected_position_rules: tuple[dict[str, object], ...] = (),
    account_restrictions: tuple[dict[str, object], ...] = (),
) -> dict[str, object]:
    return {
        'household_base_currency': 'USD',
        'target_return_band': {'minimum': '0.040000', 'maximum': '0.090000'},
        'max_portfolio_drawdown': max_portfolio_drawdown,
        'liquidity_floor_cash': '100000',
        'liquidity_floor_months_expenses': '6',
        'max_single_name_concentration': max_single_name_concentration,
        'max_sleeve_concentration': '0.700000',
        'max_correlated_cluster_concentration': '1.000000',
        'allowed_asset_classes': ['EQUITY'],
        'disallowed_asset_classes': list(disallowed_asset_classes),
        'account_restrictions': list(account_restrictions),
        'imported_position_policy': {'default_management_mode': 'observe_only'},
        'protected_position_rules': list(protected_position_rules),
        'emergency_derisk_policy': {'allow_emergency_reductions': True},
        'override_policy': {'operator_override_required': True},
    }


def _execution_intent(
    *,
    household_id: str,
    side: str = 'BUY',
    symbol: str = 'SPY',
    account_id: str = 'DU1234567',
    quantity_shares: int = 1,
    limit_price: str = '500.00',
) -> ExecutionIntentV1:
    return ExecutionIntentV1.from_dict(
        {
            'schema_id': 'execution_intent',
            'schema_version': 'v1',
            'record_id': f'execution-intent:{household_id}:{side}:{symbol}:{account_id}',
            'execution_intent_id': f'execution-intent:{household_id}:{side}:{symbol}:{account_id}',
            'promotion_record_id': 'promotion-record-1',
            'household_id': household_id,
            'created_at_utc': '2026-04-12T15:00:00Z',
            'effective_at_utc': '2026-04-12T15:00:00Z',
            'actor_source': 'household_portfolio_test',
            'contract_version': 'ADVISORY_EXECUTION_INTENT_CONTRACT_V1',
            'builder_version': 'execution_intent_builder_v1',
            'idempotency_key': f'{household_id}:{side}:{symbol}:{account_id}:{quantity_shares}',
            'operation_type': 'fresh_paper_entry_v1',
            'day_utc': '2026-04-12',
            'environment': 'PAPER',
            'sleeve_id': 'PRIMARY',
            'account_id': account_id,
            'engine_id': 'C2_TREND_EQ_PRIMARY_V1',
            'instrument': {'kind': 'EQUITY', 'symbol': symbol, 'currency': 'USD', 'ib_conId': 756733, 'ib_localSymbol': symbol},
            'side': side,
            'quantity_shares': quantity_shares,
            'order_terms': {'order_type': 'LIMIT', 'limit_price': limit_price, 'time_in_force': 'DAY'},
            'parent_lineage_refs': ['promotion_record_id:promotion-record-1'],
            'source_artifact_refs': ['promotion_record_id:promotion-record-1'],
            'canonical_json_hash': 'c' * 64,
        }
    )


def _write_policy_and_snapshot(
    tmp_path: Path,
    *,
    household_id: str = 'household-1',
    stale_cash: bool = False,
    cash_total_cents: int = 1_000_000,
) -> tuple[object, object]:
    intent, _ = write_investor_intent_v1(raw_intake=_raw_intake(household_id=household_id), output_root=str(tmp_path))
    manifest, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(), output_root=str(tmp_path))
    policy, _ = write_policy_v1(
        investor_intent=intent,
        compiler_version='policy_compiler_v1',
        assumption_manifest_refs=[manifest.assumption_manifest_id],
        regime_rule_refs=['retail_household_regime_v1'],
        output_root=str(tmp_path),
    )
    snapshot, _ = write_household_snapshot_v1(
        policy=policy,
        created_at='2026-04-12T14:00:00Z',
        effective_at='2026-04-12T14:00:00Z',
        actor_source='household_snapshot_builder',
        account_registry_snapshot=_account_registry_snapshot('DU1234567'),
        verified_positions_inputs=[
            {
                'account_id': 'DU1234567',
                'record_ref': 'positions_snapshot:DU1234567:2026-04-12',
                'snapshot': _positions_snapshot(day_utc='2026-04-12'),
            }
        ],
        verified_cash_inputs=[
            {
                'account_id': 'DU1234567',
                'record_ref': 'cash_snapshot:DU1234567:2026-04-12',
                'snapshot': _cash_snapshot(
                    day_utc='2026-04-12',
                    account_id='DU1234567',
                    cash_total_cents=cash_total_cents,
                ),
            }
        ],
        governed_asset_mark_inputs=[_asset_mark_snapshot(day_utc='2026-04-12')],
        classification_refs=['classification_manifest_v1'],
        output_root=str(tmp_path),
    )
    if stale_cash:
        stale_obj = snapshot.to_dict()
        stale_obj['freshness_status'] = 'STALE'
        stale_obj['freshness_attestations'] = [
            {
                **dict(item),
                'freshness_status': 'STALE' if item.get('component_type') == 'verified_cash_snapshot' else item.get('freshness_status'),
                'observed_at': '2026-04-11T13:04:00Z' if item.get('component_type') == 'verified_cash_snapshot' else item.get('observed_at'),
            }
            for item in stale_obj['freshness_attestations']
        ]
        stale_obj['reason_codes'] = sorted(set(list(stale_obj['reason_codes']) + ['SNAPSHOT_STALE_VERIFIED_CORE']))
        snapshot = HouseholdSnapshotV1.from_dict(stale_obj)
    return policy, snapshot


def _bundle(
    tmp_path: Path,
    *,
    execution_intent: ExecutionIntentV1,
    policy_inputs: dict[str, object] | None = None,
    imported_positions: tuple[dict[str, object], ...] = (),
    lot_rows: tuple[dict[str, object], ...] = (),
    stale_cash: bool = False,
    cash_total_cents: int = 1_000_000,
    drawdown_state: dict[str, object] | None = None,
    tax_data_available: bool = True,
    exposures_by_sector: dict[str, str] | None = None,
) -> dict[str, object]:
    policy, snapshot = _write_policy_and_snapshot(
        tmp_path,
        household_id=execution_intent.household_id,
        stale_cash=stale_cash,
        cash_total_cents=cash_total_cents,
    )
    return write_household_portfolio_bundle_v1(
        output_root=str(tmp_path),
        policy=policy,
        household_snapshot=snapshot,
        execution_intent=execution_intent,
        policy_inputs=policy_inputs or _policy_inputs(),
        snapshot_at='2026-04-12T15:05:00Z',
        valuation_timestamp='2026-04-12T15:05:00Z',
        issued_at='2026-04-12T15:06:00Z',
        created_at='2026-04-12T15:06:00Z',
        actor_source='household_portfolio_compiler_v1',
        imported_positions=imported_positions,
        lot_rows=lot_rows,
        drawdown_state=drawdown_state,
        exposures_by_sector=exposures_by_sector,
        sleeve_demands=[{'sleeve_id': 'PRIMARY', 'requested_capital_cents': 200000}],
        tax_data_available=tax_data_available,
    )


def test_policy_snapshot_requires_complete_inputs(tmp_path: Path) -> None:
    policy, _ = _write_policy_and_snapshot(tmp_path)
    policy_inputs = _policy_inputs()
    del policy_inputs['override_policy']
    with pytest.raises(ValueError, match='POLICY_SNAPSHOT_INPUTS_MISSING'):
        build_policy_snapshot_v1(
            policy=policy,
            policy_inputs=policy_inputs,
            created_at='2026-04-12T15:00:00Z',
            effective_at='2026-04-12T15:00:00Z',
            actor_source='household_portfolio_compiler_v1',
        )


def test_household_state_snapshot_marks_stale_and_incomplete_imported_positions(tmp_path: Path) -> None:
    _, snapshot = _write_policy_and_snapshot(tmp_path, stale_cash=True)
    state = build_household_state_snapshot_v1(
        household_snapshot=snapshot,
        snapshot_at='2026-04-12T15:05:00Z',
        valuation_timestamp='2026-04-12T15:05:00Z',
        imported_positions=[
            {
                'symbol': 'BND',
                'account_id': 'DU1234567',
                'owner_account_id': '',
                'lifecycle_status': 'restricted',
                'management_mode': 'risk_counted_no_trade',
                'market_value_cents': 250000,
                'data_complete': False,
            }
        ],
    )
    assert 'STATE_STALE_PRICE_INPUT' in state['reason_codes']
    assert 'STATE_INCOMPLETE_IMPORTED_POSITION' in state['reason_codes']
    assert 'STATE_UNMAPPED_POSITION_OWNER' in state['reason_codes']
    assert 'stale_verified_inputs' in state['completeness_flags']


def test_compiled_constraints_fail_closed_on_stale_state_and_missing_cash(tmp_path: Path) -> None:
    policy, snapshot = _write_policy_and_snapshot(tmp_path, stale_cash=True, cash_total_cents=0)
    policy_snapshot = build_policy_snapshot_v1(
        policy=policy,
        policy_inputs=_policy_inputs(),
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='household_portfolio_compiler_v1',
    )
    state = build_household_state_snapshot_v1(
        household_snapshot=snapshot,
        snapshot_at='2026-04-12T15:05:00Z',
        valuation_timestamp='2026-04-12T15:05:00Z',
    )
    constraints = build_compiled_constraints_v1(
        policy_snapshot=policy_snapshot,
        household_state_snapshot=state,
    )
    assert constraints['deployable_capital_cap'] == '0.00'
    assert 'AUTH_STALE_ARTIFACT' in constraints['hard_blocks']
    assert 'STATE_MISSING_ACCOUNT_BALANCE' in constraints['hard_blocks']


def test_healthy_normal_allocation_issues_explicit_buy_authorization(tmp_path: Path) -> None:
    execution_intent = _execution_intent(household_id='household-healthy', side='BUY', symbol='QQQ')
    bundle = _bundle(
        tmp_path,
        execution_intent=execution_intent,
        policy_inputs=_policy_inputs(max_single_name_concentration='1.000000'),
        tax_data_available=True,
        exposures_by_sector={'TECH': '500000'},
    )
    authorization = bundle['portfolio_authorization']
    assert authorization['allowed_actions']
    assert not authorization['blocked_actions']
    assert authorization['emergency_mode'] is False
    evaluation = evaluate_portfolio_authorization_v1(
        portfolio_authorization=authorization,
        execution_intent=execution_intent,
        eval_time_utc='2026-04-12T15:20:00Z',
    )
    assert evaluation['allowed'] is True


def test_concentration_breach_blocks_further_buys_and_requires_derisk(tmp_path: Path) -> None:
    execution_intent = _execution_intent(household_id='household-breach', side='BUY', symbol='SPY')
    bundle = _bundle(
        tmp_path,
        execution_intent=execution_intent,
        policy_inputs=_policy_inputs(max_single_name_concentration='0.300000'),
        tax_data_available=True,
        exposures_by_sector={'TECH': '500000'},
    )
    assert 'POLICY_MAX_SINGLE_NAME_LIMIT' in bundle['compiled_constraints']['hard_blocks']
    assert bundle['risk_envelope']['required_derisk_actions']
    assert bundle['portfolio_authorization']['blocked_actions']


def test_drawdown_throttle_reduces_deployable_capital_and_sleeve_budget(tmp_path: Path) -> None:
    execution_intent = _execution_intent(household_id='household-drawdown', side='BUY', symbol='QQQ')
    bundle = _bundle(
        tmp_path,
        execution_intent=execution_intent,
        policy_inputs=_policy_inputs(max_portfolio_drawdown='0.100000'),
        drawdown_state={'current_drawdown': '0.150000'},
        tax_data_available=True,
        exposures_by_sector={'TECH': '500000'},
    )
    assert bundle['compiled_constraints']['drawdown_throttle_level'] == 'BLOCK_NEW_RISK'
    assert bundle['allocation_plan']['total_capital_assignable_to_constellation'] == '0.00'
    assert 'RISK_DRAWDOWN_THROTTLE' in bundle['risk_envelope']['reason_codes']


def test_imported_position_incomplete_blocks_discretionary_trade_on_that_symbol(tmp_path: Path) -> None:
    execution_intent = _execution_intent(household_id='household-imported', side='SELL', symbol='BND')
    bundle = _bundle(
        tmp_path,
        execution_intent=execution_intent,
        imported_positions=(
            {
                'symbol': 'BND',
                'account_id': 'DU1234567',
                'owner_account_id': '',
                'lifecycle_status': 'restricted',
                'management_mode': 'risk_counted_no_trade',
                'market_value_cents': 250000,
                'data_complete': False,
            },
        ),
        tax_data_available=True,
        exposures_by_sector={'TECH': '500000'},
    )
    assert 'STATE_INCOMPLETE_IMPORTED_POSITION' in bundle['household_state_snapshot']['reason_codes']
    assert bundle['portfolio_authorization']['blocked_actions']


def test_tax_data_unavailable_allows_only_urgent_risk_reductions(tmp_path: Path) -> None:
    sell_intent = _execution_intent(household_id='household-tax', side='SELL', symbol='SPY')
    sell_bundle = _bundle(
        tmp_path / 'sell',
        execution_intent=sell_intent,
        policy_inputs=_policy_inputs(max_single_name_concentration='0.300000'),
        tax_data_available=False,
        exposures_by_sector={'TECH': '500000'},
    )
    assert sell_bundle['tax_adjudicated_rebalance']['approved_actions']
    assert sell_bundle['tax_adjudicated_rebalance']['deferred_actions']
    assert 'TAX_URGENT_RISK_EXCEPTION' in sell_bundle['tax_adjudicated_rebalance']['tax_reason_codes']
    assert sell_bundle['portfolio_authorization']['allowed_actions']

    buy_intent = _execution_intent(household_id='household-tax', side='BUY', symbol='QQQ')
    buy_bundle = _bundle(
        tmp_path / 'buy',
        execution_intent=buy_intent,
        policy_inputs=_policy_inputs(max_single_name_concentration='0.300000'),
        tax_data_available=False,
        exposures_by_sector={'TECH': '500000'},
    )
    assert buy_bundle['portfolio_authorization']['blocked_actions']
    assert 'AUTH_PREREQUISITE_ACTION_REQUIRED' in buy_bundle['portfolio_authorization']['reason_codes']


def test_stale_state_snapshot_blocks_new_authorization(tmp_path: Path) -> None:
    execution_intent = _execution_intent(household_id='household-stale', side='BUY', symbol='QQQ')
    bundle = _bundle(
        tmp_path,
        execution_intent=execution_intent,
        stale_cash=True,
        tax_data_available=True,
        exposures_by_sector={'TECH': '500000'},
    )
    assert 'AUTH_STALE_ARTIFACT' in bundle['compiled_constraints']['hard_blocks']
    assert bundle['portfolio_authorization']['blocked_actions']
    evaluation = evaluate_portfolio_authorization_v1(
        portfolio_authorization=bundle['portfolio_authorization'],
        execution_intent=execution_intent,
        eval_time_utc='2026-04-12T16:30:00Z',
    )
    assert evaluation['allowed'] is False


def test_rebalance_order_and_bundle_replay_are_deterministic(tmp_path: Path) -> None:
    execution_intent = _execution_intent(household_id='household-replay', side='SELL', symbol='SPY')
    imported_positions = (
        {
            'symbol': 'BND',
            'account_id': 'DU1234567',
            'owner_account_id': 'DU1234567',
            'lifecycle_status': 'managed',
            'management_mode': 'managed',
            'market_value_cents': 250000,
            'data_complete': True,
        },
        {
            'symbol': 'TLT',
            'account_id': 'DU1234567',
            'owner_account_id': 'DU1234567',
            'lifecycle_status': 'managed',
            'management_mode': 'managed',
            'market_value_cents': 250000,
            'data_complete': True,
        },
    )
    bundle_a = _bundle(
        tmp_path / 'a',
        execution_intent=execution_intent,
        policy_inputs=_policy_inputs(max_single_name_concentration='0.200000'),
        imported_positions=imported_positions,
        lot_rows=({'account_id': 'DU1234567', 'symbol': 'SPY', 'lot_id': 'lot-1'},),
        tax_data_available=False,
        exposures_by_sector={'TECH': '500000', 'BONDS': '500000'},
    )
    bundle_b = _bundle(
        tmp_path / 'b',
        execution_intent=execution_intent,
        policy_inputs=_policy_inputs(max_single_name_concentration='0.200000'),
        imported_positions=tuple(reversed(imported_positions)),
        lot_rows=({'account_id': 'DU1234567', 'symbol': 'SPY', 'lot_id': 'lot-1'},),
        tax_data_available=False,
        exposures_by_sector={'BONDS': '500000', 'TECH': '500000'},
    )
    assert bundle_a['rebalance_candidates']['candidate_actions'] == bundle_b['rebalance_candidates']['candidate_actions']
    assert bundle_a['portfolio_authorization'] == bundle_b['portfolio_authorization']
    assert bundle_a['portfolio_decision_record'] == bundle_b['portfolio_decision_record']
