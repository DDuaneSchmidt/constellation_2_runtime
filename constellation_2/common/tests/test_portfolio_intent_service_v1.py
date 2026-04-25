from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.advisory.advisory_storage_v1 import (
    load_household_snapshot_by_id_v1,
    load_policy_by_id_v1,
    load_portfolio_intent_by_id_v1,
)
from constellation_2.common.advisory.assumption_manifest_service_v1 import write_assumption_manifest_v1
from constellation_2.common.advisory.household_snapshot_service_v1 import (
    build_household_snapshot_candidate_v1,
    write_household_snapshot_v1,
)
from constellation_2.common.advisory.household_snapshot_v1 import HouseholdSnapshotV1
from constellation_2.common.advisory.investor_intent_service_v1 import write_investor_intent_v1
from constellation_2.common.advisory.policy_service_v1 import write_policy_v1
from constellation_2.common.advisory.portfolio_intent_service_v1 import build_portfolio_intent_v1, write_portfolio_intent_v1
from constellation_2.common.advisory.policy_v1 import PolicyV1


def _raw_intake(*, household_id: str = 'household-1', risk_preference: str = 'balanced') -> dict[str, object]:
    return {
        'household_id': household_id,
        'intent_version': 'v1',
        'created_at': '2026-04-12T12:00:00Z',
        'effective_at': '2026-04-12T12:00:00Z',
        'actor_source': 'advisor_operator',
        'goals': ['fund_retirement'],
        'constraints': {
            'time_horizon': 'long_term',
            'risk_preference': risk_preference,
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


def _cash_snapshot(*, day_utc: str, account_id: str, observed_day_utc: str | None = None, cash_total_cents: int = 1000000) -> dict[str, object]:
    observed_day = observed_day_utc or day_utc
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
            'observed_at_utc': f'{observed_day}T13:04:00Z',
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


def _write_policy_and_snapshot(
    tmp_path: Path,
    *,
    household_id: str = 'household-1',
    risk_preference: str = 'balanced',
    include_external: bool = False,
    stale_cash: bool = False,
    second_position_id: str = 'position-0000000001',
) -> tuple[PolicyV1, HouseholdSnapshotV1]:
    intent, _ = write_investor_intent_v1(raw_intake=_raw_intake(household_id=household_id, risk_preference=risk_preference), output_root=str(tmp_path))
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
                'snapshot': _positions_snapshot(day_utc='2026-04-12', position_id=second_position_id),
            }
        ],
        verified_cash_inputs=[
            {
                'account_id': 'DU1234567',
                'record_ref': 'cash_snapshot:DU1234567:2026-04-12',
                'snapshot': _cash_snapshot(
                    day_utc='2026-04-12',
                    observed_day_utc='2026-04-11' if stale_cash else None,
                    account_id='DU1234567',
                ),
            }
        ],
        external_holdings_inputs=[] if not include_external else [
            {
                'account_id': 'DU1234567',
                'record_ref': 'normalized_holdings:DU1234567:2026-04-12',
                'artifact': _external_holdings_artifact(day_utc='2026-04-12', account_id='DU1234567'),
            }
        ],
        classification_refs=['classification_manifest_v1'],
        output_root=str(tmp_path),
    )
    return policy, snapshot


def test_portfolio_intent_valid_build_and_lineage_resolution(tmp_path: Path) -> None:
    policy, snapshot = _write_policy_and_snapshot(tmp_path)
    intent, path = write_portfolio_intent_v1(
        policy=policy,
        household_snapshot=snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
        classification_refs=['classification_manifest_v1'],
        output_root=str(tmp_path),
    )

    assert Path(path).exists()
    assert intent.parent_policy_id == policy.policy_id
    assert intent.parent_household_snapshot_id == snapshot.household_snapshot_id
    assert intent.validity_tier == 'VALID_EXECUTION_ELIGIBLE'
    assert intent.execution_eligibility == 'EXECUTION_ELIGIBLE'
    assert intent.action_needed is False
    loaded_intent = load_portfolio_intent_by_id_v1(str(tmp_path), policy.household_id, intent.portfolio_intent_id)
    loaded_policy = load_policy_by_id_v1(str(tmp_path), policy.household_id, intent.parent_policy_id)
    loaded_snapshot = load_household_snapshot_by_id_v1(str(tmp_path), policy.household_id, intent.parent_household_snapshot_id)
    assert loaded_intent.to_dict() == intent.to_dict()
    assert loaded_policy.policy_id == policy.policy_id
    assert loaded_snapshot.household_snapshot_id == snapshot.household_snapshot_id


def test_portfolio_intent_advisory_only_from_degraded_household_snapshot(tmp_path: Path) -> None:
    policy, snapshot = _write_policy_and_snapshot(tmp_path, include_external=True)
    intent = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
        classification_refs=['classification_manifest_v1'],
    )

    assert intent.validity_tier == 'VALID_ADVISORY_ONLY'
    assert intent.execution_eligibility == 'ADVISORY_ONLY'
    assert intent.action_needed is False
    assert intent.blocked_conditions[0]['condition_code'] == 'UPSTREAM_HOUSEHOLD_NOT_EXECUTION_ELIGIBLE'
    assert intent.constrained_deviations[0]['deviation_code'] == 'UNVERIFIED_COMPONENTS_EXCLUDED_FROM_EXECUTION_SCOPE'


def test_portfolio_intent_hard_stops_on_invalid_upstream_household(tmp_path: Path) -> None:
    policy, snapshot = _write_policy_and_snapshot(tmp_path)
    invalid_snapshot = HouseholdSnapshotV1.from_dict(
        {
            **snapshot.to_dict(),
            'validity_tier': 'INVALID_REMEDIABLE',
            'reason_codes': ['PARTIAL_HOUSEHOLD_COVERAGE'],
        }
    )

    with pytest.raises(ValueError, match='PARENT_HOUSEHOLD_SNAPSHOT_INVALID_TIER'):
        build_portfolio_intent_v1(
            policy=policy,
            household_snapshot=invalid_snapshot,
            created_at='2026-04-12T15:00:00Z',
            effective_at='2026-04-12T15:00:00Z',
            actor_source='portfolio_intent_builder',
            classification_refs=['classification_manifest_v1'],
        )


def test_portfolio_intent_is_deterministic_for_same_semantic_inputs(tmp_path: Path) -> None:
    policy, snapshot = _write_policy_and_snapshot(tmp_path)
    intent_a = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
        classification_refs=['classification_manifest_v1', 'classification_manifest_v1'],
    )
    intent_b = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
        classification_refs=['classification_manifest_v1'],
    )

    assert intent_a.portfolio_intent_id == intent_b.portfolio_intent_id
    assert intent_a.to_dict() == intent_b.to_dict()


def test_portfolio_intent_immutable_rewrite_rejected(tmp_path: Path) -> None:
    policy, snapshot = _write_policy_and_snapshot(tmp_path)
    kwargs = {
        'policy': policy,
        'household_snapshot': snapshot,
        'created_at': '2026-04-12T15:00:00Z',
        'effective_at': '2026-04-12T15:00:00Z',
        'classification_refs': ['classification_manifest_v1'],
        'output_root': str(tmp_path),
    }
    first, _ = write_portfolio_intent_v1(actor_source='portfolio_intent_builder_a', **kwargs)

    with pytest.raises(ValueError, match='IMMUTABLE_CONFLICT'):
        write_portfolio_intent_v1(actor_source='portfolio_intent_builder_b', **kwargs)

    loaded = load_portfolio_intent_by_id_v1(str(tmp_path), policy.household_id, first.portfolio_intent_id)
    assert loaded.actor_source == 'portfolio_intent_builder_a'


def test_portfolio_intent_constrained_deviation_explicit_when_snapshot_is_stale(tmp_path: Path) -> None:
    policy, _ = _write_policy_and_snapshot(tmp_path)
    snapshot = build_household_snapshot_candidate_v1(
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
                'snapshot': _cash_snapshot(day_utc='2026-04-12', account_id='DU1234567', observed_day_utc='2026-04-11'),
            }
        ],
        classification_refs=['classification_manifest_v1'],
    )
    intent = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=snapshot,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
        classification_refs=['classification_manifest_v1'],
    )

    assert intent.validity_tier == 'VALID_ADVISORY_ONLY'
    assert intent.execution_eligibility == 'ADVISORY_ONLY'
    assert any(item['deviation_code'] == 'STALE_COMPONENTS_BLOCK_EXECUTION_SCOPE' for item in intent.constrained_deviations)


def test_portfolio_intent_fingerprint_stable_and_changes_with_effective_target_change(tmp_path: Path) -> None:
    policy_a, snapshot_a = _write_policy_and_snapshot(tmp_path / 'a', risk_preference='balanced')
    intent_a = build_portfolio_intent_v1(
        policy=policy_a,
        household_snapshot=snapshot_a,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
        classification_refs=['classification_manifest_v1'],
    )
    intent_a_repeat = build_portfolio_intent_v1(
        policy=policy_a,
        household_snapshot=snapshot_a,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
        classification_refs=['classification_manifest_v1'],
    )
    policy_b, snapshot_b = _write_policy_and_snapshot(tmp_path / 'b', risk_preference='growth', second_position_id='position-0000000002')
    intent_b = build_portfolio_intent_v1(
        policy=policy_b,
        household_snapshot=snapshot_b,
        created_at='2026-04-12T15:00:00Z',
        effective_at='2026-04-12T15:00:00Z',
        actor_source='portfolio_intent_builder',
        classification_refs=['classification_manifest_v1'],
    )

    assert intent_a.portfolio_intent_fingerprint == intent_a_repeat.portfolio_intent_fingerprint
    assert intent_a.portfolio_intent_fingerprint != intent_b.portfolio_intent_fingerprint
    assert intent_a.portfolio_intent_id != intent_b.portfolio_intent_id
