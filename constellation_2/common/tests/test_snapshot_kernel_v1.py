from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import constellation_2.common.advisory.household_snapshot_service_v1 as household_service_module
from constellation_2.common.advisory.assumption_manifest_service_v1 import write_assumption_manifest_v1
from constellation_2.common.advisory.household_snapshot_service_v1 import build_household_snapshot_v1
from constellation_2.common.advisory.investor_intent_service_v1 import write_investor_intent_v1
from constellation_2.common.advisory.policy_service_v1 import write_policy_v1
from constellation_2.common.advisory.portfolio_intent_service_v1 import build_portfolio_intent_v1
from constellation_2.common.advisory.snapshot_kernel_runner_v1 import run_snapshot_kernel_v1


DAY = '2026-04-15'


def _raw_intake(*, household_id: str = 'household-1') -> dict[str, object]:
    return {
        'household_id': household_id,
        'intent_version': 'v1',
        'created_at': f'{DAY}T12:00:00Z',
        'effective_at': f'{DAY}T12:00:00Z',
        'actor_source': 'advisor_operator',
        'goals': ['reduce_prohibited_exposure'],
        'constraints': {
            'time_horizon': 'long_term',
            'risk_preference': 'balanced',
            'liquidity_requirement': 'moderate',
            'concentration_preference': 'diversified',
            'prohibited_exposures': ['symbol:SPY'],
            'account_role_preferences': ['taxable_growth'],
        },
        'operating_mode': 'automation_allowed',
        'approval_preferences': {
            'automation_preference': 'proposal_only',
            'approval_preference': 'delegate_allowed',
        },
        'compiler_inputs_manifest_refs': ['intake_normalization_manifest_v1'],
        'lineage_parent_refs': [],
    }


def _raw_manifest() -> dict[str, object]:
    return {
        'manifest_version': 'v1',
        'manifest_type': 'policy_rule_interpretation',
        'created_at': f'{DAY}T12:05:00Z',
        'effective_at': f'{DAY}T12:05:00Z',
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


def _write_policy(tmp_path: Path):
    intent, _ = write_investor_intent_v1(raw_intake=_raw_intake(), output_root=str(tmp_path))
    manifest, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(), output_root=str(tmp_path))
    policy, _ = write_policy_v1(
        investor_intent=intent,
        compiler_version='policy_compiler_v1',
        assumption_manifest_refs=[manifest.assumption_manifest_id],
        regime_rule_refs=['retail_household_regime_v1'],
        output_root=str(tmp_path),
    )
    return policy


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


def _positions_snapshot(
    *,
    day_utc: str,
    qty: int = 10,
    symbol: str = 'SPY',
    position_id: str = 'position-0000000001',
) -> dict[str, object]:
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
                    'instrument': {'kind': 'EQUITY', 'symbol': symbol, 'currency': 'USD', 'ib_conId': 756733, 'ib_localSymbol': symbol},
                    'qty': qty,
                    'avg_cost_cents': 50000,
                    'market_exposure_type': 'DEFINED_RISK',
                    'max_loss_cents': 500000,
                    'opened_day_utc': day_utc,
                    'status': 'OPEN',
                }
            ],
        },
    }


def _cash_snapshot(*, day_utc: str, account_id: str, cash_total_cents: int = 1000000) -> dict[str, object]:
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
            'observed_at_utc': f'{day_utc}T13:04:00Z',
            'currency': 'USD',
            'cash_total_cents': cash_total_cents,
            'nlv_total_cents': cash_total_cents + 500000,
            'available_funds_cents': cash_total_cents - 10000,
            'excess_liquidity_cents': cash_total_cents - 5000,
            'account_id': account_id,
            'notes': [],
        },
    }


def _valid_kernel_kwargs(tmp_path: Path) -> dict[str, object]:
    return {
        'repo_root': SOURCE_ROOT,
        'output_root': str(tmp_path),
        'snapshot_run_id': 'snapshot-run-1',
        'policy': _write_policy(tmp_path),
        'produced_utc': f'{DAY}T14:00:00Z',
        'effective_at': f'{DAY}T14:00:00Z',
        'actor_source': 'snapshot_kernel_builder',
        'account_registry_snapshot': _account_registry_snapshot('DU1234567'),
        'verified_positions_inputs': [
            {
                'account_id': 'DU1234567',
                'record_ref': f'positions_snapshot:DU1234567:{DAY}',
                'snapshot': _positions_snapshot(day_utc=DAY),
            }
        ],
        'verified_cash_inputs': [
            {
                'account_id': 'DU1234567',
                'record_ref': f'cash_snapshot:DU1234567:{DAY}',
                'snapshot': _cash_snapshot(day_utc=DAY, account_id='DU1234567'),
            }
        ],
        'classification_refs': ['classification_manifest_v1'],
    }


def test_snapshot_kernel_happy_path_writes_snapshot_and_envelope(tmp_path: Path) -> None:
    result = run_snapshot_kernel_v1(**_valid_kernel_kwargs(tmp_path))

    assert result['snapshot_validation_decision'].outcome == 'valid'
    assert result['household_snapshot'] is not None
    assert result['household_snapshot'].validity_tier == 'VALID_EXECUTION_ELIGIBLE'
    assert result['snapshot_run_envelope'].run_outcome == 'success'
    assert result['snapshot_run_envelope'].artifact_refs['household_snapshot_id'] == result['household_snapshot'].household_snapshot_id


def test_snapshot_kernel_blocked_on_missing_positions(tmp_path: Path) -> None:
    kwargs = _valid_kernel_kwargs(tmp_path)
    kwargs['verified_positions_inputs'] = []

    result = run_snapshot_kernel_v1(**kwargs)

    assert result['snapshot_validation_decision'].outcome == 'blocked'
    assert 'SNAPSHOT_INCOMPLETE_ACCOUNT_SCOPE' in result['snapshot_validation_decision'].reason_codes
    assert result['household_snapshot'] is None
    assert result['snapshot_run_envelope'].run_outcome == 'blocked'


def test_snapshot_kernel_blocked_on_missing_cash(tmp_path: Path) -> None:
    kwargs = _valid_kernel_kwargs(tmp_path)
    kwargs['verified_cash_inputs'] = []

    result = run_snapshot_kernel_v1(**kwargs)

    assert result['snapshot_validation_decision'].outcome == 'blocked'
    assert 'SNAPSHOT_INCOMPLETE_ACCOUNT_SCOPE' in result['snapshot_validation_decision'].reason_codes
    assert result['household_snapshot'] is None


def test_snapshot_kernel_blocked_on_stale_verified_core_inputs(tmp_path: Path) -> None:
    kwargs = _valid_kernel_kwargs(tmp_path)
    kwargs['verified_positions_inputs'] = [
        {
            'account_id': 'DU1234567',
            'record_ref': 'positions_snapshot:DU1234567:2026-04-14',
            'snapshot': _positions_snapshot(day_utc='2026-04-14'),
        }
    ]

    result = run_snapshot_kernel_v1(**kwargs)

    assert result['snapshot_validation_decision'].outcome == 'blocked'
    assert 'SNAPSHOT_STALE_VERIFIED_CORE' in result['snapshot_validation_decision'].reason_codes
    assert result['household_snapshot'] is None


def test_household_snapshot_identity_changes_when_cash_changes(tmp_path: Path) -> None:
    policy = _write_policy(tmp_path)
    snapshot_a = build_household_snapshot_v1(
        policy=policy,
        created_at=f'{DAY}T14:00:00Z',
        effective_at=f'{DAY}T14:00:00Z',
        actor_source='snapshot_builder',
        account_registry_snapshot=_account_registry_snapshot('DU1234567'),
        verified_positions_inputs=[{'account_id': 'DU1234567', 'record_ref': f'positions_snapshot:DU1234567:{DAY}', 'snapshot': _positions_snapshot(day_utc=DAY)}],
        verified_cash_inputs=[{'account_id': 'DU1234567', 'record_ref': f'cash_snapshot:DU1234567:{DAY}', 'snapshot': _cash_snapshot(day_utc=DAY, account_id='DU1234567', cash_total_cents=1000000)}],
    )
    snapshot_b = build_household_snapshot_v1(
        policy=policy,
        created_at=f'{DAY}T14:00:00Z',
        effective_at=f'{DAY}T14:00:00Z',
        actor_source='snapshot_builder',
        account_registry_snapshot=_account_registry_snapshot('DU1234567'),
        verified_positions_inputs=[{'account_id': 'DU1234567', 'record_ref': f'positions_snapshot:DU1234567:{DAY}', 'snapshot': _positions_snapshot(day_utc=DAY)}],
        verified_cash_inputs=[{'account_id': 'DU1234567', 'record_ref': f'cash_snapshot:DU1234567:{DAY}', 'snapshot': _cash_snapshot(day_utc=DAY, account_id='DU1234567', cash_total_cents=900000)}],
    )

    assert snapshot_a.household_snapshot_id != snapshot_b.household_snapshot_id


def test_snapshot_kernel_replay_is_deterministic_and_advisory_uses_frozen_snapshot_only(tmp_path: Path, monkeypatch) -> None:
    kwargs = _valid_kernel_kwargs(tmp_path)
    first = run_snapshot_kernel_v1(**kwargs)
    second = run_snapshot_kernel_v1(**kwargs)

    assert first['household_snapshot'].household_snapshot_id == second['household_snapshot'].household_snapshot_id

    monkeypatch.setattr(household_service_module, '_validate_positions_snapshot', lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('live reread not allowed')))
    monkeypatch.setattr(household_service_module, '_normalize_cash_inputs', lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('live reread not allowed')))

    portfolio_intent = build_portfolio_intent_v1(
        policy=kwargs['policy'],
        household_snapshot=first['household_snapshot'],
        created_at=f'{DAY}T15:00:00Z',
        effective_at=f'{DAY}T15:00:00Z',
        actor_source='portfolio_intent_builder',
        classification_refs=list(first['household_snapshot'].classification_refs),
    )

    assert portfolio_intent.parent_household_snapshot_id == first['household_snapshot'].household_snapshot_id
