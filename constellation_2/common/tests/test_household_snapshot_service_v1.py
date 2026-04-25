from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.advisory.advisory_storage_v1 import load_household_snapshot_by_id_v1, load_policy_by_id_v1
from constellation_2.common.advisory.assumption_manifest_service_v1 import write_assumption_manifest_v1
from constellation_2.common.advisory.household_snapshot_service_v1 import build_household_snapshot_v1, write_household_snapshot_v1
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


def _write_policy(tmp_path: Path, *, household_id: str = 'household-1'):
    intent, _ = write_investor_intent_v1(raw_intake=_raw_intake(household_id=household_id), output_root=str(tmp_path))
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


def _positions_snapshot(*, day_utc: str, position_id: str = 'position-0000000001') -> dict[str, object]:
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
                    'position_id': position_id,
                    'engine_id': 'engine-alpha',
                    'instrument': {
                        'kind': 'EQUITY',
                        'symbol': 'SPY',
                        'currency': 'USD',
                        'ib_conId': 756733,
                        'ib_localSymbol': 'SPY',
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


def _cash_snapshot(*, day_utc: str, account_id: str, cash_total_cents: int = 1000000) -> dict[str, object]:
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
            'nlv_total_cents': cash_total_cents + 500000,
            'available_funds_cents': cash_total_cents - 10000,
            'excess_liquidity_cents': cash_total_cents - 5000,
            'account_id': account_id,
            'notes': [],
        },
    }


def _external_holdings_artifact(*, day_utc: str, account_id: str, holding_id: str = 'holding-1') -> dict[str, object]:
    return {
        'schema_id': 'normalized_holdings_artifact',
        'schema_version': 'v1',
        'artifact_id': f'artifact-{account_id}',
        'engine_version': 'legacy_holdings_normalizer_v1',
        'policy_version': 'policy-v1',
        'authority_status': 'candidate',
        'execution_authority': False,
        'source_upload_ref': f'upload:{account_id}',
        'account_id': account_id,
        'as_of_date': day_utc,
        'holdings': [
            {
                'holding_id': holding_id,
                'symbol': 'BND',
                'security_type': 'ETF',
                'issuer_name': 'Example Issuer',
                'quantity': '10',
                'market_value': '1000.00',
                'currency': 'USD',
                'maturity_date': None,
                'is_corporate_bond': False,
                'excluded_from_active_sleeve_indicator': False,
            }
        ],
        'input_warnings': [],
        'created_at': f'{day_utc}T09:00:00Z',
    }


def test_household_snapshot_valid_build_and_lineage_resolution(tmp_path: Path) -> None:
    policy = _write_policy(tmp_path)
    snapshot, path = write_household_snapshot_v1(
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
                'snapshot': _cash_snapshot(day_utc='2026-04-12', account_id='DU1234567'),
            }
        ],
        classification_refs=['classification_manifest_v1'],
        output_root=str(tmp_path),
    )

    assert Path(path).exists()
    assert snapshot.parent_policy_id == policy.policy_id
    assert snapshot.validity_tier == 'VALID_EXECUTION_ELIGIBLE'
    assert snapshot.reason_codes == ('HOUSEHOLD_SNAPSHOT_COMPLETE',)
    assert len(snapshot.verified_components) == 2
    assert not snapshot.unverified_components
    loaded_snapshot = load_household_snapshot_by_id_v1(str(tmp_path), policy.household_id, snapshot.household_snapshot_id)
    loaded_policy = load_policy_by_id_v1(str(tmp_path), policy.household_id, loaded_snapshot.parent_policy_id)
    assert loaded_snapshot.to_dict() == snapshot.to_dict()
    assert loaded_policy.policy_id == policy.policy_id


def test_household_snapshot_degraded_with_unverified_external_component(tmp_path: Path) -> None:
    policy = _write_policy(tmp_path)
    snapshot = build_household_snapshot_v1(
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
                'snapshot': _cash_snapshot(day_utc='2026-04-12', account_id='DU1234567'),
            }
        ],
        external_holdings_inputs=[
            {
                'account_id': 'DU1234567',
                'record_ref': 'normalized_holdings:DU1234567:2026-04-12',
                'artifact': _external_holdings_artifact(day_utc='2026-04-12', account_id='DU1234567'),
            }
        ],
    )

    assert snapshot.validity_tier == 'VALID_ADVISORY_ONLY'
    assert 'UNVERIFIED_COMPONENTS_PRESENT' in snapshot.reason_codes
    assert snapshot.verified_components[0]['component_type'].startswith('verified_')
    assert snapshot.unverified_components[0]['component_type'] == 'unverified_external_holdings'
    assert snapshot.investable_asset_summary['unverified_external_holding_count'] == 1


def test_household_snapshot_hard_stops_on_missing_required_core_scope(tmp_path: Path) -> None:
    policy = _write_policy(tmp_path)
    with pytest.raises(ValueError, match='ACCOUNT_REGISTRY_ACCOUNTS_REQUIRED'):
        build_household_snapshot_v1(
            policy=policy,
            created_at='2026-04-12T14:00:00Z',
            effective_at='2026-04-12T14:00:00Z',
            actor_source='household_snapshot_builder',
            account_registry_snapshot={'source_refs': ['registry_ref:BROKER_ACCOUNT_SCOPE_V1'], 'accounts': []},
            verified_positions_inputs=[],
            verified_cash_inputs=[],
        )


def test_household_snapshot_is_deterministic_for_same_semantic_inputs(tmp_path: Path) -> None:
    policy = _write_policy(tmp_path)
    snapshot_a = build_household_snapshot_v1(
        policy=policy,
        created_at='2026-04-12T14:00:00Z',
        effective_at='2026-04-12T14:00:00Z',
        actor_source='household_snapshot_builder',
        account_registry_snapshot={
            'source_refs': ['registry_ref:SECOND', 'registry_ref:FIRST'],
            'accounts': [
                {
                    'account_id': 'DU7654321',
                    'scope_role': 'ira_secondary',
                    'source_ref': 'account_registry:DU7654321',
                    'source_type': 'ib_account_registry',
                    'verification_status': 'VERIFIED',
                },
                {
                    'account_id': 'DU1234567',
                    'scope_role': 'taxable_primary',
                    'source_ref': 'account_registry:DU1234567',
                    'source_type': 'ib_account_registry',
                    'verification_status': 'VERIFIED',
                },
            ],
        },
        verified_positions_inputs=[
            {
                'account_id': 'DU7654321',
                'record_ref': 'positions_snapshot:DU7654321:2026-04-12',
                'snapshot': _positions_snapshot(day_utc='2026-04-12', position_id='position-0000000002'),
            },
            {
                'account_id': 'DU1234567',
                'record_ref': 'positions_snapshot:DU1234567:2026-04-12',
                'snapshot': _positions_snapshot(day_utc='2026-04-12', position_id='position-0000000001'),
            },
        ],
        verified_cash_inputs=[
            {
                'account_id': 'DU7654321',
                'record_ref': 'cash_snapshot:DU7654321:2026-04-12',
                'snapshot': _cash_snapshot(day_utc='2026-04-12', account_id='DU7654321', cash_total_cents=700000),
            },
            {
                'account_id': 'DU1234567',
                'record_ref': 'cash_snapshot:DU1234567:2026-04-12',
                'snapshot': _cash_snapshot(day_utc='2026-04-12', account_id='DU1234567', cash_total_cents=1000000),
            },
        ],
        classification_refs=['class_b', 'class_a', 'class_b'],
    )
    snapshot_b = build_household_snapshot_v1(
        policy=policy,
        created_at='2026-04-12T14:00:00Z',
        effective_at='2026-04-12T14:00:00Z',
        actor_source='household_snapshot_builder',
        account_registry_snapshot={
            'source_refs': ['registry_ref:FIRST', 'registry_ref:SECOND'],
            'accounts': [
                {
                    'account_id': 'DU1234567',
                    'scope_role': 'taxable_primary',
                    'source_ref': 'account_registry:DU1234567',
                    'source_type': 'ib_account_registry',
                    'verification_status': 'VERIFIED',
                },
                {
                    'account_id': 'DU7654321',
                    'scope_role': 'ira_secondary',
                    'source_ref': 'account_registry:DU7654321',
                    'source_type': 'ib_account_registry',
                    'verification_status': 'VERIFIED',
                },
            ],
        },
        verified_positions_inputs=[
            {
                'account_id': 'DU1234567',
                'record_ref': 'positions_snapshot:DU1234567:2026-04-12',
                'snapshot': _positions_snapshot(day_utc='2026-04-12', position_id='position-0000000001'),
            },
            {
                'account_id': 'DU7654321',
                'record_ref': 'positions_snapshot:DU7654321:2026-04-12',
                'snapshot': _positions_snapshot(day_utc='2026-04-12', position_id='position-0000000002'),
            },
        ],
        verified_cash_inputs=[
            {
                'account_id': 'DU1234567',
                'record_ref': 'cash_snapshot:DU1234567:2026-04-12',
                'snapshot': _cash_snapshot(day_utc='2026-04-12', account_id='DU1234567', cash_total_cents=1000000),
            },
            {
                'account_id': 'DU7654321',
                'record_ref': 'cash_snapshot:DU7654321:2026-04-12',
                'snapshot': _cash_snapshot(day_utc='2026-04-12', account_id='DU7654321', cash_total_cents=700000),
            },
        ],
        classification_refs=['class_a', 'class_b'],
    )

    assert snapshot_a.household_snapshot_id == snapshot_b.household_snapshot_id
    assert snapshot_a.to_dict() == snapshot_b.to_dict()


def test_household_snapshot_immutable_rewrite_rejected(tmp_path: Path) -> None:
    policy = _write_policy(tmp_path)
    kwargs = {
        'policy': policy,
        'created_at': '2026-04-12T14:00:00Z',
        'effective_at': '2026-04-12T14:00:00Z',
        'account_registry_snapshot': _account_registry_snapshot('DU1234567'),
        'verified_positions_inputs': [
            {
                'account_id': 'DU1234567',
                'record_ref': 'positions_snapshot:DU1234567:2026-04-12',
                'snapshot': _positions_snapshot(day_utc='2026-04-12'),
            }
        ],
        'verified_cash_inputs': [
            {
                'account_id': 'DU1234567',
                'record_ref': 'cash_snapshot:DU1234567:2026-04-12',
                'snapshot': _cash_snapshot(day_utc='2026-04-12', account_id='DU1234567'),
            }
        ],
        'output_root': str(tmp_path),
    }
    first, _ = write_household_snapshot_v1(actor_source='household_snapshot_builder_a', **kwargs)

    with pytest.raises(ValueError, match='IMMUTABLE_CONFLICT'):
        write_household_snapshot_v1(actor_source='household_snapshot_builder_b', **kwargs)

    loaded = load_household_snapshot_by_id_v1(str(tmp_path), policy.household_id, first.household_snapshot_id)
    assert loaded.actor_source == 'household_snapshot_builder_a'
