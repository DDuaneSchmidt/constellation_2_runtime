from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.advisory.assumption_manifest_service_v1 import write_assumption_manifest_v1  # noqa: E402
from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1  # noqa: E402
from constellation_2.common.advisory.execution_package_builder_from_execution_intent_v1 import (  # noqa: E402
    derive_execution_submission_identity_from_execution_intent_v1,
)
from constellation_2.common.advisory.household_snapshot_service_v1 import build_household_snapshot_v1  # noqa: E402
from constellation_2.common.advisory.investor_intent_service_v1 import build_investor_intent_v1  # noqa: E402
from constellation_2.common.advisory.policy_service_v1 import build_policy_v1  # noqa: E402
from constellation_2.common.advisory.portfolio_intent_service_v1 import build_portfolio_intent_v1  # noqa: E402
from constellation_2.common.advisory.promotion_decision_service_v1 import build_promotion_decision_v1  # noqa: E402
from constellation_2.common.advisor_bridge.promotion_record_service_v2 import build_promotion_record_v2  # noqa: E402
from constellation_2.common.advisor_bridge.promotion_record_v2 import PromotionRecordV2  # noqa: E402
from constellation_2.common.execution_kernel.approved_change_set_service_v1 import (  # noqa: E402
    build_approved_change_set_v1,
)
from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (  # noqa: E402
    execution_submission_record_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.execution_kernel.execution_set_intent_service_v1 import build_execution_set_intent_v1  # noqa: E402
from constellation_2.common.execution_kernel.execution_submission_decision_v1 import (  # noqa: E402
    build_execution_submission_decision_v1,
)
from constellation_2.common.execution_kernel.execution_submission_record_v1 import ExecutionSubmissionRecordV1  # noqa: E402
from constellation_2.common.execution_kernel.execution_state_record_v1 import (  # noqa: E402
    build_execution_state_record_v1,
    write_execution_state_record_v1,
)
from constellation_2.common.execution_kernel.multi_delta_execution_decision_service_v1 import (  # noqa: E402
    build_multi_delta_execution_decision_v1,
)
from constellation_2.common.execution_kernel.multi_delta_execution_kernel_runner_v1 import (  # noqa: E402
    run_multi_delta_execution_kernel_v1,
)
from constellation_2.common.execution_kernel.multi_delta_execution_record_service_v1 import (  # noqa: E402
    build_multi_delta_execution_record_v1,
    write_multi_delta_execution_record_v1,
)
from constellation_2.common.execution_kernel.multi_delta_execution_status_service_v1 import (  # noqa: E402
    summarize_execution_set_status_v1,
)


DAY = '2026-04-12'


def _raw_intake() -> dict[str, object]:
    return {
        'household_id': 'household-multi-delta-1',
        'intent_version': 'v1',
        'created_at': f'{DAY}T12:00:00Z',
        'effective_at': f'{DAY}T12:00:00Z',
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


def _raw_manifest(*, max_trade_entries: int) -> dict[str, object]:
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
                'diversified': ['max_single_exposure_pct:20'],
                'moderate': ['max_single_exposure_pct:25'],
                'concentrated': ['max_single_exposure_pct:40'],
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
                'advisory_only': ['min_trade_shares:1', f'max_trade_entries:{max_trade_entries}'],
                'manual_execution_only': ['min_trade_shares:1', f'max_trade_entries:{max_trade_entries}'],
                'automation_allowed': [
                    'require_execution_eligible_snapshot',
                    'min_trade_shares:1',
                    f'max_trade_entries:{max_trade_entries}',
                ],
            },
        },
        'reason_codes': ['ASSUMPTION_MANIFEST_ACTIVE'],
        'status': 'ACTIVE',
    }


def _positions_snapshot(*, positions: list[dict[str, object]]) -> dict[str, object]:
    return {
        'schema_id': 'C2_POSITIONS_SNAPSHOT_V4',
        'schema_version': 4,
        'produced_utc': f'{DAY}T13:00:00Z',
        'day_utc': DAY,
        'producer': {'repo': 'constellation', 'git_sha': 'abcdef1', 'module': 'positions_snapshot_builder_v4'},
        'status': 'OK',
        'reason_codes': ['POSITIONS_OK'],
        'input_manifest': [{'type': 'execution_evidence', 'path': '/tmp/input.json', 'sha256': 'a' * 64, 'day_utc': DAY, 'producer': 'test'}],
        'positions': {'currency': 'USD', 'asof_utc': f'{DAY}T12:59:00Z', 'notes': [], 'items': positions},
    }


def _equity_position(*, symbol: str, qty: int, avg_cost_cents: int) -> dict[str, object]:
    position_id = f'position-{symbol.lower():0<12}'
    return {
        'position_id': position_id,
        'engine_id': 'engine-alpha',
        'instrument': {'kind': 'EQUITY', 'symbol': symbol, 'currency': 'USD', 'ib_conId': 1000 + len(symbol), 'ib_localSymbol': symbol},
        'qty': qty,
        'avg_cost_cents': avg_cost_cents,
        'market_exposure_type': 'DEFINED_RISK',
        'max_loss_cents': qty * avg_cost_cents,
        'opened_day_utc': DAY,
        'status': 'OPEN',
    }


def _cash_snapshot(*, cash_total_cents: int) -> dict[str, object]:
    return {
        'schema_id': 'C2_CASH_LEDGER_SNAPSHOT_V1',
        'schema_version': 1,
        'produced_utc': f'{DAY}T13:05:00Z',
        'day_utc': DAY,
        'authority_basis': 'broker_account_values',
        'producer': {'repo': 'constellation', 'git_sha': 'abcdef1', 'module': 'cash_ledger_snapshot_builder_v1'},
        'status': 'OK',
        'reason_codes': ['CASH_OK'],
        'input_manifest': [{'type': 'broker_account_values', 'path': '/tmp/cash.json', 'sha256': 'b' * 64, 'day_utc': DAY, 'producer': 'test'}],
        'snapshot': {
            'observed_at_utc': f'{DAY}T13:04:00Z',
            'currency': 'USD',
            'cash_total_cents': cash_total_cents,
            'nlv_total_cents': cash_total_cents,
            'available_funds_cents': cash_total_cents,
            'excess_liquidity_cents': cash_total_cents,
            'account_id': 'DU1234567',
            'notes': [],
        },
    }


def _asset_marks(*, marks: list[tuple[str, int]]) -> list[dict[str, object]]:
    return [
        {
            'record_ref': 'asset_marks:2026-04-12',
            'snapshot': {
                'asof_utc': f'{DAY}T13:10:00Z',
                'marks': [
                    {'symbol': symbol, 'asset_type': 'EQUITY', 'currency': 'USD', 'mark_price_cents': price_cents}
                    for symbol, price_cents in marks
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
    allocation_targets: list[dict[str, object]],
    rebalance_threshold: str = '0.050000',
    minimum_trade_value_cents: int = 1000,
    max_trade_entries: int = 4,
):
    intent = build_investor_intent_v1(_raw_intake())
    manifest, _ = write_assumption_manifest_v1(raw_manifest=_raw_manifest(max_trade_entries=max_trade_entries), output_root=str(tmp_path))
    return build_policy_v1(
        investor_intent=intent,
        compiler_version='policy_compiler_v1',
        assumption_manifest_refs=[manifest.assumption_manifest_id],
        regime_rule_refs=['retail_household_regime_v1'],
        output_root=str(tmp_path),
        allocation_targets=allocation_targets,
        rebalance_threshold=rebalance_threshold,
        minimum_trade_value_cents=minimum_trade_value_cents,
    )


def _build_snapshot(
    policy,
    *,
    positions: list[dict[str, object]],
    cash_total_cents: int,
    marks: list[tuple[str, int]] | None,
):
    return build_household_snapshot_v1(
        policy=policy,
        created_at=f'{DAY}T14:00:00Z',
        effective_at=f'{DAY}T14:00:00Z',
        actor_source='household_snapshot_builder',
        account_registry_snapshot=_account_registry_snapshot(),
        verified_positions_inputs=[
            {
                'account_id': 'DU1234567',
                'record_ref': 'positions_snapshot:DU1234567:2026-04-12',
                'snapshot': _positions_snapshot(positions=positions),
            }
        ],
        verified_cash_inputs=[
            {
                'account_id': 'DU1234567',
                'record_ref': 'cash_snapshot:DU1234567:2026-04-12',
                'snapshot': _cash_snapshot(cash_total_cents=cash_total_cents),
            }
        ],
        governed_asset_mark_inputs=[] if marks is None else _asset_marks(marks=marks),
    )


def _build_chain(tmp_path: Path, *, max_trade_entries: int = 4, marks: list[tuple[str, int]] | None = None):
    policy = _build_policy(
        tmp_path,
        allocation_targets=[
            {'symbol': 'SPY', 'asset_type': 'EQUITY', 'currency': 'USD', 'target_weight': '0.200000'},
            {'symbol': 'QQQ', 'asset_type': 'EQUITY', 'currency': 'USD', 'target_weight': '0.400000'},
            {'symbol': 'CASH_USD', 'asset_type': 'CASH', 'currency': 'USD', 'target_weight': '0.400000'},
        ],
        max_trade_entries=max_trade_entries,
    )
    snapshot = _build_snapshot(
        policy,
        positions=[_equity_position(symbol='SPY', qty=10, avg_cost_cents=5000)],
        cash_total_cents=40000,
        marks=[('SPY', 6000), ('QQQ', 10000)] if marks is None else marks,
    )
    intent = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=snapshot,
        created_at=f'{DAY}T15:00:00Z',
        effective_at=f'{DAY}T15:00:00Z',
        actor_source='portfolio_intent_builder',
    )
    decision = build_promotion_decision_v1(
        policy=policy,
        household_snapshot=snapshot,
        portfolio_intent=intent,
        created_at=f'{DAY}T15:05:00Z',
        effective_at=f'{DAY}T15:05:00Z',
        actor_source='promotion_decision_builder',
    )
    record = build_promotion_record_v2(
        policy=policy,
        portfolio_intent=intent,
        promotion_decision=decision,
        produced_utc=f'{DAY}T15:10:00Z',
        run_id='multi-delta-run-1',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    return policy, snapshot, intent, decision, record


def _execution_profile() -> dict[str, object]:
    return {
        'environment': 'PAPER',
        'sleeve_id': 'CORE',
        'operation_type': 'fresh_paper_entry',
        'account_id': 'DU1234567',
        'engine_id': 'engine-alpha',
        'order_terms': {'order_type': 'MARKET', 'limit_price': None, 'time_in_force': 'DAY'},
    }


def _submission_record_for_member(*, member: dict[str, object], household_id: str, promotion_record_id: str) -> ExecutionSubmissionRecordV1:
    submission_id = str(member['predicted_submission_id'])
    execution_intent_id = str(member['execution_intent']['execution_intent_id'])
    obj = {
        'schema_id': 'execution_submission_record',
        'schema_version': 'v1',
        'record_id': submission_id,
        'submission_record_id': submission_id,
        'execution_intent_id': execution_intent_id,
        'promotion_record_id': promotion_record_id,
        'household_id': household_id,
        'day_utc': DAY,
        'produced_utc': f'{DAY}T16:00:00Z',
        'contract_version': 'execution_submission_record_contract_v1',
        'builder_version': 'execution_submission_record_builder_v1',
        'submission_id': submission_id,
        'trade_instance_id': str(member['predicted_trade_instance_id']),
        'idempotency_key': str(member['execution_intent']['idempotency_key']),
        'status': 'READY_TO_SUBMIT',
        'candidate_ref': {'path': '/tmp/fake-candidate', 'sha256': '1' * 64},
        'downstream_payload_ref': {'path': '/tmp/fake-plan.json', 'sha256': '2' * 64},
        'execution_build_ref': {'path': '/tmp/fake-build.json', 'sha256': '3' * 64},
        'execution_package_ref': {'path': '/tmp/fake-package.json', 'sha256': '4' * 64},
        'input_record_refs': [f'execution_intent_id:{execution_intent_id}'],
        'parent_lineage_refs': list(member['execution_intent']['parent_lineage_refs']),
        'source_artifact_refs': ['execution_package_path:/tmp/fake-package.json'],
        'canonical_json_hash': '5' * 64,
    }
    return ExecutionSubmissionRecordV1.from_dict(obj)


def test_approved_change_set_identity_and_value_basis_sensitivity(tmp_path: Path) -> None:
    policy, _, intent, decision, record = _build_chain(tmp_path / 'a')
    change_set_a = build_approved_change_set_v1(
        policy=policy,
        portfolio_intent=intent,
        promotion_decision=decision,
        promotion_record=record,
        produced_utc=f'{DAY}T15:20:00Z',
    )
    change_set_b = build_approved_change_set_v1(
        policy=policy,
        portfolio_intent=intent,
        promotion_decision=decision,
        promotion_record=record,
        produced_utc=f'{DAY}T15:20:00Z',
    )

    policy2, _, intent2, decision2, record2 = _build_chain(tmp_path / 'b', marks=[('SPY', 6500), ('QQQ', 10000)])
    change_set_c = build_approved_change_set_v1(
        policy=policy2,
        portfolio_intent=intent2,
        promotion_decision=decision2,
        promotion_record=record2,
        produced_utc=f'{DAY}T15:20:00Z',
    )

    assert decision.outcome == 'promote'
    assert len(record.approved_delta) == 2
    assert change_set_a.approved_change_set_id == change_set_b.approved_change_set_id
    assert change_set_a.approved_change_set_id != change_set_c.approved_change_set_id
    assert [row['instrument']['symbol'] for row in change_set_a.ordered_changes] == ['QQQ', 'SPY']


def test_approved_change_set_blocks_missing_value_basis_and_ambiguous_ordering(tmp_path: Path) -> None:
    policy, _, intent, decision, record = _build_chain(tmp_path / 'valid')
    missing_snapshot = _build_snapshot(
        policy,
        positions=[_equity_position(symbol='SPY', qty=10, avg_cost_cents=5000)],
        cash_total_cents=40000,
        marks=None,
    )
    missing_intent = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=missing_snapshot,
        created_at=f'{DAY}T15:00:00Z',
        effective_at=f'{DAY}T15:00:00Z',
        actor_source='portfolio_intent_builder',
    )
    missing_decision = build_promotion_decision_v1(
        policy=policy,
        household_snapshot=missing_snapshot,
        portfolio_intent=missing_intent,
        created_at=f'{DAY}T15:05:00Z',
        effective_at=f'{DAY}T15:05:00Z',
        actor_source='promotion_decision_builder',
    )
    missing_record = build_promotion_record_v2(
        policy=policy,
        portfolio_intent=missing_intent,
        promotion_decision=missing_decision,
        produced_utc=f'{DAY}T15:10:00Z',
        run_id='multi-delta-run-missing',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    blocked_change_set = build_approved_change_set_v1(
        policy=policy,
        portfolio_intent=missing_intent,
        promotion_decision=missing_decision,
        promotion_record=missing_record,
        produced_utc=f'{DAY}T15:20:00Z',
    )

    ambiguous_record = PromotionRecordV2.from_dict(
        {
            **record.to_dict(),
            'approved_delta': [dict(record.approved_delta[0]), dict(record.approved_delta[0])],
        }
    )
    ambiguous_change_set = build_approved_change_set_v1(
        policy=policy,
        portfolio_intent=intent,
        promotion_decision=decision,
        promotion_record=ambiguous_record,
        produced_utc=f'{DAY}T15:20:00Z',
    )

    assert blocked_change_set.status == 'BLOCKED'
    assert 'APPROVED_CHANGE_SET_VALUE_BASIS_MISSING' in blocked_change_set.reason_codes
    assert ambiguous_change_set.status == 'BLOCKED'
    assert 'APPROVED_CHANGE_SET_ORDERING_AMBIGUOUS' in ambiguous_change_set.reason_codes


def test_multi_delta_execution_decision_outcomes(tmp_path: Path) -> None:
    policy, _, intent, decision, record = _build_chain(tmp_path / 'promote')
    change_set = build_approved_change_set_v1(
        policy=policy,
        portfolio_intent=intent,
        promotion_decision=decision,
        promotion_record=record,
        produced_utc=f'{DAY}T15:20:00Z',
    )
    promote = build_multi_delta_execution_decision_v1(
        policy=policy,
        approved_change_set=change_set,
        produced_utc=f'{DAY}T15:25:00Z',
        truth_root=tmp_path / 'truth',
    )

    strict_policy, _, strict_intent, strict_decision, strict_record = _build_chain(tmp_path / 'blocked', max_trade_entries=1)
    strict_change_set = build_approved_change_set_v1(
        policy=strict_policy,
        portfolio_intent=strict_intent,
        promotion_decision=strict_decision,
        promotion_record=strict_record,
        produced_utc=f'{DAY}T15:20:00Z',
    )
    blocked = build_multi_delta_execution_decision_v1(
        policy=strict_policy,
        approved_change_set=strict_change_set,
        produced_utc=f'{DAY}T15:25:00Z',
        truth_root=tmp_path / 'truth',
    )

    no_action_policy = _build_policy(
        tmp_path / 'no-action',
        allocation_targets=[
            {'symbol': 'SPY', 'asset_type': 'EQUITY', 'currency': 'USD', 'target_weight': '0.600000'},
            {'symbol': 'CASH_USD', 'asset_type': 'CASH', 'currency': 'USD', 'target_weight': '0.400000'},
        ],
    )
    no_action_snapshot = _build_snapshot(
        no_action_policy,
        positions=[_equity_position(symbol='SPY', qty=10, avg_cost_cents=5000)],
        cash_total_cents=40000,
        marks=[('SPY', 6000)],
    )
    no_action_intent = build_portfolio_intent_v1(
        policy=no_action_policy,
        household_snapshot=no_action_snapshot,
        created_at=f'{DAY}T15:00:00Z',
        effective_at=f'{DAY}T15:00:00Z',
        actor_source='portfolio_intent_builder',
    )
    no_action_decision = build_promotion_decision_v1(
        policy=no_action_policy,
        household_snapshot=no_action_snapshot,
        portfolio_intent=no_action_intent,
        created_at=f'{DAY}T15:05:00Z',
        effective_at=f'{DAY}T15:05:00Z',
        actor_source='promotion_decision_builder',
    )
    no_action_record = build_promotion_record_v2(
        policy=no_action_policy,
        portfolio_intent=no_action_intent,
        promotion_decision=no_action_decision,
        produced_utc=f'{DAY}T15:10:00Z',
        run_id='multi-delta-run-no-action',
        execution_profile=_execution_profile(),
        approval_confirmed=True,
    )
    empty_change_set = build_approved_change_set_v1(
        policy=no_action_policy,
        portfolio_intent=no_action_intent,
        promotion_decision=no_action_decision,
        promotion_record=no_action_record,
        produced_utc=f'{DAY}T15:20:00Z',
    )
    no_action = build_multi_delta_execution_decision_v1(
        policy=no_action_policy,
        approved_change_set=empty_change_set,
        produced_utc=f'{DAY}T15:25:00Z',
        truth_root=tmp_path / 'truth',
    )

    truth_root = tmp_path / 'truth-duplicate'
    first_record, _, _ = write_multi_delta_execution_record_v1(
        approved_change_set=change_set,
        decision=promote,
        produced_utc=f'{DAY}T15:30:00Z',
        truth_root=truth_root,
    )
    duplicate = build_multi_delta_execution_decision_v1(
        policy=policy,
        approved_change_set=change_set,
        produced_utc=f'{DAY}T15:31:00Z',
        truth_root=truth_root,
    )

    assert promote.outcome == 'promote'
    assert blocked.outcome == 'blocked'
    assert no_action.outcome == 'no_action'
    assert duplicate.outcome == 'duplicate'
    assert duplicate.duplicate_multi_delta_execution_record_id == first_record.multi_delta_execution_record_id


def test_multi_delta_execution_record_and_execution_set_intent_are_deterministic(tmp_path: Path) -> None:
    policy, _, intent, decision, record = _build_chain(tmp_path)
    change_set = build_approved_change_set_v1(
        policy=policy,
        portfolio_intent=intent,
        promotion_decision=decision,
        promotion_record=record,
        produced_utc=f'{DAY}T15:20:00Z',
    )
    execution_decision = build_multi_delta_execution_decision_v1(
        policy=policy,
        approved_change_set=change_set,
        produced_utc=f'{DAY}T15:25:00Z',
        truth_root=tmp_path / 'truth',
    )
    record_a = build_multi_delta_execution_record_v1(
        approved_change_set=change_set,
        decision=execution_decision,
        produced_utc=f'{DAY}T15:30:00Z',
    )
    record_b = build_multi_delta_execution_record_v1(
        approved_change_set=change_set,
        decision=execution_decision,
        produced_utc=f'{DAY}T15:30:00Z',
    )
    set_intent = build_execution_set_intent_v1(
        record=record_a,
        created_at_utc=f'{DAY}T15:40:00Z',
        effective_at_utc=f'{DAY}T15:40:00Z',
        actor_source='execution_set_intent_builder',
    )

    assert record_a.multi_delta_execution_record_id == record_b.multi_delta_execution_record_id
    assert [row['change_id'] for row in set_intent.member_intents] == [row['change_id'] for row in change_set.ordered_changes]
    for member in set_intent.member_intents:
        execution_intent = member['execution_intent']
        assert f'multi_delta_execution_record_id:{record_a.multi_delta_execution_record_id}' in execution_intent['parent_lineage_refs']
        submission_decision = build_execution_submission_decision_v1(
            execution_intent=ExecutionIntentV1.from_dict(execution_intent),
            produced_utc=f'{DAY}T15:45:00Z',
            truth_root=tmp_path / 'submission-truth',
        )
        assert submission_decision.outcome == 'submit'


def test_set_level_partial_progress_visibility(tmp_path: Path) -> None:
    policy, _, intent, decision, record = _build_chain(tmp_path)
    change_set = build_approved_change_set_v1(
        policy=policy,
        portfolio_intent=intent,
        promotion_decision=decision,
        promotion_record=record,
        produced_utc=f'{DAY}T15:20:00Z',
    )
    execution_decision = build_multi_delta_execution_decision_v1(
        policy=policy,
        approved_change_set=change_set,
        produced_utc=f'{DAY}T15:25:00Z',
        truth_root=tmp_path / 'truth',
    )
    execution_record = build_multi_delta_execution_record_v1(
        approved_change_set=change_set,
        decision=execution_decision,
        produced_utc=f'{DAY}T15:30:00Z',
    )
    set_intent = build_execution_set_intent_v1(
        record=execution_record,
        created_at_utc=f'{DAY}T15:40:00Z',
        effective_at_utc=f'{DAY}T15:40:00Z',
        actor_source='execution_set_intent_builder',
    )

    first_member = set_intent.member_intents[0]
    submission_record = _submission_record_for_member(
        member=first_member,
        household_id=execution_record.household_id,
        promotion_record_id=execution_record.promotion_record_id,
    )
    write_immutable_json_v1(
        execution_submission_record_path_v1(
            truth_root=tmp_path / 'truth',
            day_utc=DAY,
            submission_id=submission_record.submission_id,
        ),
        submission_record.to_dict(),
    )
    state_record = build_execution_state_record_v1(
        submission_record=submission_record,
        produced_utc=f'{DAY}T16:05:00Z',
        lifecycle_status='FILLED',
        source_lifecycle_status='Filled',
        terminal_state=True,
        transition_index=1,
        previous_execution_state_record_id=None,
        evidence_basis='FILL_LEDGER',
        evidence_fingerprint='6' * 64,
        broker_order_id='101',
        perm_id='202',
        filled_qty=first_member['execution_intent']['quantity_shares'],
        remaining_qty=0,
        avg_fill_price='100.00',
        broker_submission_ref=None,
        execution_event_ref=None,
        fill_ledger_ref=None,
        reason_codes=['FILLED'],
    )
    write_execution_state_record_v1(record=state_record, truth_root=tmp_path / 'truth')

    summary = summarize_execution_set_status_v1(
        truth_root=tmp_path / 'truth',
        execution_set_intent=set_intent,
    )

    assert summary['set_status'] == 'PARTIAL'
    assert summary['submitted_count'] == 1
    assert summary['terminal_count'] == 1
    assert summary['members'][0]['lifecycle_status'] == 'FILLED'
    assert summary['members'][1]['submission_record_present'] is False


def test_multi_delta_runner_preserves_order_and_duplicate_blocks_handoff(tmp_path: Path, monkeypatch) -> None:
    policy, _, intent, decision, record = _build_chain(tmp_path / 'chain')
    calls: list[str] = []

    def _fake_run_execution_kernel_v1(*, truth_root, execution_intent, **kwargs):
        derived = derive_execution_submission_identity_from_execution_intent_v1(execution_intent=execution_intent)
        calls.append(execution_intent.instrument['symbol'])
        submission_record = _submission_record_for_member(
            member={
                'predicted_submission_id': derived['submission_id'],
                'predicted_trade_instance_id': derived['trade_instance_id'],
                'execution_intent': execution_intent.to_dict(),
            },
            household_id=execution_intent.household_id,
            promotion_record_id=execution_intent.promotion_record_id,
        )
        write_immutable_json_v1(
            execution_submission_record_path_v1(
                truth_root=truth_root,
                day_utc=execution_intent.day_utc,
                submission_id=submission_record.submission_id,
            ),
            submission_record.to_dict(),
        )
        return {'submission_record': submission_record, 'submission_decision': {'outcome': 'submit'}}

    monkeypatch.setattr(
        'constellation_2.common.execution_kernel.multi_delta_execution_kernel_runner_v1.run_execution_kernel_v1',
        _fake_run_execution_kernel_v1,
    )

    result = run_multi_delta_execution_kernel_v1(
        repo_root=SOURCE_ROOT,
        truth_root=tmp_path / 'truth',
        run_id='multi-delta-e2e',
        policy=policy,
        portfolio_intent=intent,
        promotion_decision=decision,
        promotion_record=record,
        produced_utc=f'{DAY}T16:00:00Z',
        created_at_utc=f'{DAY}T16:00:00Z',
        effective_at_utc=f'{DAY}T16:00:00Z',
        actor_source='execution_set_intent_builder',
        eval_time_utc=f'{DAY}T16:00:00Z',
        risk_budget_path=tmp_path / 'risk.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=12,
        dry_run=True,
    )
    duplicate = run_multi_delta_execution_kernel_v1(
        repo_root=SOURCE_ROOT,
        truth_root=tmp_path / 'truth',
        run_id='multi-delta-e2e-duplicate',
        policy=policy,
        portfolio_intent=intent,
        promotion_decision=decision,
        promotion_record=record,
        produced_utc=f'{DAY}T16:01:00Z',
        created_at_utc=f'{DAY}T16:01:00Z',
        effective_at_utc=f'{DAY}T16:01:00Z',
        actor_source='execution_set_intent_builder',
        eval_time_utc=f'{DAY}T16:01:00Z',
        risk_budget_path=tmp_path / 'risk.json',
        ib_host='127.0.0.1',
        ib_port=4002,
        ib_client_id=12,
        dry_run=True,
    )

    assert result['multi_delta_execution_decision'].outcome == 'promote'
    assert calls == ['QQQ', 'SPY']
    assert duplicate['multi_delta_execution_decision'].outcome == 'duplicate'
    assert duplicate['member_results'] == []
