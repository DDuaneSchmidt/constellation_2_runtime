from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.advisory.advisory_domain_constants_v1 import (
    ADVISORY_VALIDITY_TIERS_V1,
    ALLOCATION_ADVISORY_STATUSES_V1,
    POLICY_ALLOCATION_MODES_V1,
    PORTFOLIO_INTENT_BUILDER_VERSION_V1,
    PORTFOLIO_INTENT_CONTRACT_VERSION_V1,
    PORTFOLIO_INTENT_EXECUTION_ELIGIBILITY_V1,
)
from constellation_2.common.advisory.advisory_storage_v1 import (
    portfolio_intent_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.advisory.household_snapshot_v1 import HouseholdSnapshotV1
from constellation_2.common.advisory.policy_v1 import PolicyV1
from constellation_2.common.advisory.portfolio_intent_v1 import PortfolioIntentV1


def _require_nonempty_str(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field.upper()}_REQUIRED')
    return value.strip()


def _normalize_string_list(value: list[str] | tuple[str, ...], *, field: str) -> tuple[str, ...]:
    items: list[str] = []
    for raw in value:
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError(f'{field.upper()}_ITEM_INVALID')
        items.append(raw.strip())
    return tuple(sorted(set(items)))


def _blocked_condition(*, code: str, reason_codes: list[str], detail_refs: list[str]) -> dict[str, Any]:
    return {
        'condition_code': code,
        'reason_codes': sorted(set(reason_codes)),
        'detail_refs': sorted(set(detail_refs)),
    }


def _constrained_deviation(*, code: str, reason_codes: list[str], detail_refs: list[str]) -> dict[str, Any]:
    return {
        'deviation_code': code,
        'reason_codes': sorted(set(reason_codes)),
        'detail_refs': sorted(set(detail_refs)),
    }


def _account_current_summary(account: dict[str, Any]) -> dict[str, Any]:
    return {
        'account_id': str(account['account_id']),
        'scope_role': str(account['scope_role']),
        'verified_open_position_count': int(account['verified_open_position_count']),
        'verified_cash_total_cents': account['verified_cash_total_cents'],
        'unverified_external_holding_count': int(account['unverified_external_holding_count']),
    }


def _prohibited_symbol_rules(policy: PolicyV1) -> tuple[str, ...]:
    symbols = []
    for rule in policy.prohibited_exposure_rules:
        if not isinstance(rule, str):
            continue
        text = rule.strip()
        if not text.lower().startswith('symbol:'):
            continue
        symbol = text.split(':', 1)[1].strip().upper()
        if symbol:
            symbols.append(symbol)
    return tuple(sorted(set(symbols)))


def _account_target_summary(account: dict[str, Any], *, prohibited_symbols: tuple[str, ...]) -> dict[str, Any]:
    target_position_ids = []
    for position in account['verified_positions']:
        symbol = str(position['instrument'].get('symbol') or '').upper()
        if symbol and symbol in prohibited_symbols:
            continue
        target_position_ids.append(str(position['position_id']))
    return {
        'account_id': str(account['account_id']),
        'scope_role': str(account['scope_role']),
        'target_verified_position_ids': sorted(set(target_position_ids)),
        'target_verified_cash_total_cents': account['verified_cash_total_cents'],
        'unverified_external_components_in_scope': False,
    }


def _weight_text(value: Decimal) -> str:
    return str(value.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))


def _weight_from_cents(*, value_cents: int, total_value_cents: int) -> str:
    if total_value_cents <= 0:
        return '0.000000'
    return _weight_text(Decimal(value_cents) / Decimal(total_value_cents))


def _allocate_target_values(*, total_value_cents: int, targets: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    remaining = int(total_value_cents)
    for idx, target in enumerate(targets):
        if idx == len(targets) - 1:
            value_cents = remaining
        else:
            value_cents = int((Decimal(total_value_cents) * Decimal(str(target['target_weight']))).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
            remaining -= value_cents
        entries.append(
            {
                'symbol': str(target['symbol']),
                'asset_type': str(target['asset_type']),
                'currency': str(target['currency']),
                'value_cents': value_cents,
                'weight': str(target['target_weight']),
                'reference_price_cents': None,
                'current_quantity_shares': None,
                'routing_account_id': None,
            }
        )
    return entries


def _disabled_current_value_allocation(*, allocation_policy_mode: str, reason_code: str) -> dict[str, Any]:
    return {
        'status': 'DISABLED',
        'allocation_policy_mode': allocation_policy_mode,
        'value_basis_id': None,
        'valuation_mode': None,
        'effective_at': None,
        'total_portfolio_value_cents': None,
        'total_cash_value_cents': None,
        'total_position_value_cents': None,
        'allocations': [],
        'reason_codes': [reason_code],
    }


def _blocked_allocation_surface(*, policy: PolicyV1, household_snapshot: HouseholdSnapshotV1, reason_codes: list[str]) -> tuple[dict[str, Any], dict[str, Any]]:
    current_value_allocation = {
        'status': 'BLOCKED',
        'allocation_policy_mode': policy.allocation_policy_mode,
        'value_basis_id': household_snapshot.value_basis.get('value_basis_id'),
        'valuation_mode': household_snapshot.value_basis.get('valuation_mode'),
        'effective_at': household_snapshot.value_basis.get('effective_at'),
        'total_portfolio_value_cents': household_snapshot.value_basis.get('total_portfolio_value_cents'),
        'total_cash_value_cents': household_snapshot.value_basis.get('cash_value_cents'),
        'total_position_value_cents': None,
        'allocations': [],
        'reason_codes': sorted(set(reason_codes)),
    }
    allocation_drift = {
        'status': 'BLOCKED',
        'drift_metric': 'ABSOLUTE_WEIGHT_DELTA',
        'max_abs_drift_weight': None,
        'drift_entries': [],
        'reason_codes': sorted(set(reason_codes)),
    }
    return current_value_allocation, allocation_drift


def build_portfolio_intent_v1(
    *,
    policy: PolicyV1,
    household_snapshot: HouseholdSnapshotV1,
    created_at: str,
    effective_at: str,
    actor_source: str,
    classification_refs: list[str] | tuple[str, ...] = (),
    builder_version: str = PORTFOLIO_INTENT_BUILDER_VERSION_V1,
) -> PortfolioIntentV1:
    if policy.policy_completeness_status != 'VALID':
        raise ValueError('PARENT_POLICY_NOT_VALID')
    if policy.validity_tier != 'VALID_ADVISORY_ONLY':
        raise ValueError('PARENT_POLICY_INVALID_TIER')
    if household_snapshot.parent_policy_id != policy.policy_id:
        raise ValueError('PARENT_POLICY_SNAPSHOT_MISMATCH')
    if household_snapshot.household_id != policy.household_id:
        raise ValueError('HOUSEHOLD_SCOPE_MISMATCH')
    if household_snapshot.validity_tier not in {'VALID_EXECUTION_ELIGIBLE', 'VALID_ADVISORY_ONLY'}:
        raise ValueError('PARENT_HOUSEHOLD_SNAPSHOT_INVALID_TIER')

    created_at_clean = _require_nonempty_str(created_at, field='created_at')
    effective_at_clean = _require_nonempty_str(effective_at, field='effective_at')
    actor_source_clean = _require_nonempty_str(actor_source, field='actor_source')
    builder_version_clean = _require_nonempty_str(builder_version, field='builder_version')
    if builder_version_clean != PORTFOLIO_INTENT_BUILDER_VERSION_V1:
        raise ValueError('UNSUPPORTED_PORTFOLIO_INTENT_BUILDER_VERSION')

    classification_refs_norm = _normalize_string_list(list(classification_refs), field='classification_refs')
    missing_classification_refs = sorted(set(classification_refs_norm) - set(household_snapshot.classification_refs))
    if missing_classification_refs:
        raise ValueError(f'CLASSIFICATION_REF_NOT_BOUND_IN_HOUSEHOLD_SNAPSHOT:{missing_classification_refs[0]}')
    prohibited_symbols = _prohibited_symbol_rules(policy)

    current_account_summaries = sorted(
        (_account_current_summary(account) for account in household_snapshot.holdings_by_account),
        key=lambda item: (item['account_id'], item['scope_role']),
    )
    target_account_summaries = sorted(
        (_account_target_summary(account, prohibited_symbols=prohibited_symbols) for account in household_snapshot.holdings_by_account),
        key=lambda item: (item['account_id'], item['scope_role']),
    )

    current_allocation_summary = {
        'verified_open_position_count': int(household_snapshot.investable_asset_summary['verified_open_position_count']),
        'verified_defined_risk_position_count': int(household_snapshot.investable_asset_summary['verified_defined_risk_position_count']),
        'verified_undefined_risk_position_count': int(household_snapshot.investable_asset_summary['verified_undefined_risk_position_count']),
        'unverified_external_holding_count': int(household_snapshot.investable_asset_summary['unverified_external_holding_count']),
        'verified_cash_total_cents': int(household_snapshot.cash_liquidity_summary['verified_cash_total_cents']),
        'verified_available_funds_total_cents': household_snapshot.cash_liquidity_summary['verified_available_funds_total_cents'],
        'verified_excess_liquidity_total_cents': household_snapshot.cash_liquidity_summary['verified_excess_liquidity_total_cents'],
        'verified_cash_account_count': int(household_snapshot.cash_liquidity_summary['verified_cash_account_count']),
        'accounts': current_account_summaries,
    }

    required_directional_changes: list[dict[str, Any]] = []
    blocked_conditions: list[dict[str, Any]] = []
    constrained_deviations: list[dict[str, Any]] = []

    if policy.allocation_policy_mode not in POLICY_ALLOCATION_MODES_V1:
        raise ValueError('POLICY_ALLOCATION_MODE_INVALID')

    current_value_allocation = _disabled_current_value_allocation(
        allocation_policy_mode=policy.allocation_policy_mode,
        reason_code='ALLOCATION_POLICY_MODE_EXIT_ONLY',
    )
    allocation_drift = {
        'status': 'DISABLED',
        'drift_metric': 'ABSOLUTE_WEIGHT_DELTA',
        'max_abs_drift_weight': None,
        'drift_entries': [],
        'reason_codes': ['ALLOCATION_POLICY_MODE_EXIT_ONLY'],
    }

    if policy.allocation_policy_mode == 'TARGET_WEIGHTS':
        if household_snapshot.value_basis.get('status') != 'VALID':
            target_allocation = {
                'construction_mode': 'policy_target_weights_v1',
                'rebalance_philosophy': policy.rebalance_philosophy,
                'allocation_policy_mode': policy.allocation_policy_mode,
                'rebalance_threshold': str(policy.rebalance_threshold),
                'minimum_trade_value_cents': int(policy.minimum_trade_value_cents),
                'targets': _allocate_target_values(total_value_cents=0, targets=policy.allocation_targets),
            }
            current_value_allocation, allocation_drift = _blocked_allocation_surface(
                policy=policy,
                household_snapshot=household_snapshot,
                reason_codes=list(household_snapshot.value_basis.get('reason_codes') or ['VALUE_BASIS_BLOCKED']),
            )
            blocked_conditions.append(
                _blocked_condition(
                    code='VALUE_BASIS_BLOCKED',
                    reason_codes=list(current_value_allocation['reason_codes']),
                    detail_refs=[f'value_basis_id:{household_snapshot.value_basis.get("value_basis_id")}'],
                )
            )
        else:
            total_portfolio_value_cents = household_snapshot.value_basis.get('total_portfolio_value_cents')
            if not isinstance(total_portfolio_value_cents, int) or total_portfolio_value_cents <= 0:
                target_allocation = {
                    'construction_mode': 'policy_target_weights_v1',
                    'rebalance_philosophy': policy.rebalance_philosophy,
                    'allocation_policy_mode': policy.allocation_policy_mode,
                    'rebalance_threshold': str(policy.rebalance_threshold),
                    'minimum_trade_value_cents': int(policy.minimum_trade_value_cents),
                    'targets': _allocate_target_values(total_value_cents=0, targets=policy.allocation_targets),
                }
                current_value_allocation, allocation_drift = _blocked_allocation_surface(
                    policy=policy,
                    household_snapshot=household_snapshot,
                    reason_codes=['TOTAL_PORTFOLIO_VALUE_UNAVAILABLE'],
                )
                blocked_conditions.append(
                    _blocked_condition(
                        code='VALUE_BASIS_BLOCKED',
                        reason_codes=['TOTAL_PORTFOLIO_VALUE_UNAVAILABLE'],
                        detail_refs=[f'value_basis_id:{household_snapshot.value_basis.get("value_basis_id")}'],
                    )
                )
            else:
                routing_account_id = None
                account_ids = sorted(str(item['account_id']) for item in household_snapshot.holdings_by_account)
                if len(account_ids) == 1:
                    routing_account_id = account_ids[0]
                asset_mark_map = {
                    str(item['symbol']): dict(item)
                    for item in household_snapshot.value_basis.get('asset_marks', [])
                    if isinstance(item, dict)
                }
                current_buckets: dict[str, dict[str, Any]] = {}
                for item in household_snapshot.value_basis.get('position_values', []):
                    symbol = str(item['symbol'])
                    bucket = current_buckets.setdefault(
                        symbol,
                        {
                            'symbol': symbol,
                            'asset_type': str(item['asset_type']),
                            'currency': str(item['currency']),
                            'value_cents': 0,
                            'reference_price_cents': int(item['mark_price_cents']),
                            'current_quantity_shares': 0,
                            'routing_account_id': routing_account_id,
                        },
                    )
                    bucket['value_cents'] += int(item['market_value_cents'])
                    bucket['current_quantity_shares'] += int(item['quantity_shares'])
                cash_value_cents = int(household_snapshot.value_basis['cash_value_cents'])
                current_buckets['CASH_USD'] = {
                    'symbol': 'CASH_USD',
                    'asset_type': 'CASH',
                    'currency': 'USD',
                    'value_cents': cash_value_cents,
                    'reference_price_cents': None,
                    'current_quantity_shares': None,
                    'routing_account_id': routing_account_id,
                }
                current_allocations = sorted(
                    (
                        {
                            'symbol': str(item['symbol']),
                            'asset_type': str(item['asset_type']),
                            'currency': str(item['currency']),
                            'value_cents': int(item['value_cents']),
                            'weight': _weight_from_cents(value_cents=int(item['value_cents']), total_value_cents=total_portfolio_value_cents),
                            'reference_price_cents': item['reference_price_cents'],
                            'current_quantity_shares': item['current_quantity_shares'],
                            'routing_account_id': item['routing_account_id'],
                        }
                        for item in current_buckets.values()
                    ),
                    key=lambda item: (item['asset_type'], item['symbol']),
                )
                target_entries = _allocate_target_values(total_value_cents=total_portfolio_value_cents, targets=policy.allocation_targets)
                target_entries_enriched = []
                for entry in target_entries:
                    mark = asset_mark_map.get(str(entry['symbol']))
                    target_entries_enriched.append(
                        {
                            **entry,
                            'reference_price_cents': None if mark is None else int(mark['mark_price_cents']),
                            'routing_account_id': routing_account_id,
                        }
                    )
                target_allocation = {
                    'construction_mode': 'policy_target_weights_v1',
                    'rebalance_philosophy': policy.rebalance_philosophy,
                    'allocation_policy_mode': policy.allocation_policy_mode,
                    'rebalance_threshold': str(policy.rebalance_threshold),
                    'minimum_trade_value_cents': int(policy.minimum_trade_value_cents),
                    'targets': target_entries_enriched,
                }
                current_value_allocation = {
                    'status': 'READY',
                    'allocation_policy_mode': policy.allocation_policy_mode,
                    'value_basis_id': household_snapshot.value_basis.get('value_basis_id'),
                    'valuation_mode': household_snapshot.value_basis.get('valuation_mode'),
                    'effective_at': household_snapshot.value_basis.get('effective_at'),
                    'total_portfolio_value_cents': total_portfolio_value_cents,
                    'total_cash_value_cents': cash_value_cents,
                    'total_position_value_cents': total_portfolio_value_cents - cash_value_cents,
                    'allocations': current_allocations,
                    'reason_codes': ['CURRENT_VALUE_ALLOCATION_READY'],
                }
                target_map = {str(item['symbol']): dict(item) for item in target_entries_enriched}
                current_map = {str(item['symbol']): dict(item) for item in current_allocations}
                drift_entries = []
                max_abs_drift_weight = Decimal('0')
                for symbol in sorted(set(target_map) | set(current_map)):
                    target_entry = target_map.get(symbol)
                    current_entry = current_map.get(symbol)
                    current_value_cents = 0 if current_entry is None else int(current_entry['value_cents'])
                    target_value_cents = 0 if target_entry is None else int(target_entry['value_cents'])
                    current_weight = '0.000000' if current_entry is None else str(current_entry['weight'])
                    target_weight = '0.000000' if target_entry is None else str(target_entry['weight'])
                    drift_weight_decimal = Decimal(target_weight) - Decimal(current_weight)
                    max_abs_drift_weight = max(max_abs_drift_weight, abs(drift_weight_decimal))
                    reference_price_cents = None
                    if current_entry is not None:
                        reference_price_cents = current_entry.get('reference_price_cents')
                    elif target_entry is not None:
                        reference_price_cents = target_entry.get('reference_price_cents')
                    drift_scope = {
                        'symbol': symbol,
                        'asset_type': str((target_entry or current_entry or {}).get('asset_type') or ''),
                        'currency': str((target_entry or current_entry or {}).get('currency') or ''),
                        'current_value_cents': current_value_cents,
                        'target_value_cents': target_value_cents,
                        'delta_value_cents': target_value_cents - current_value_cents,
                        'current_weight': current_weight,
                        'target_weight': target_weight,
                        'drift_weight': _weight_text(drift_weight_decimal),
                        'reference_price_cents': reference_price_cents,
                        'routing_account_id': (target_entry or current_entry or {}).get('routing_account_id'),
                    }
                    drift_entries.append({'drift_id': canonical_sha256_hex_v1(drift_scope), **drift_scope})
                allocation_drift = {
                    'status': 'READY',
                    'drift_metric': 'ABSOLUTE_WEIGHT_DELTA',
                    'max_abs_drift_weight': _weight_text(max_abs_drift_weight),
                    'drift_entries': drift_entries,
                    'reason_codes': ['ALLOCATION_DRIFT_READY'],
                }
        required_directional_changes = []
        action_needed = any(int(item['delta_value_cents']) != 0 for item in allocation_drift['drift_entries']) if allocation_drift['status'] == 'READY' else False
        execution_eligibility = 'ADVISORY_ONLY'
    else:
        target_allocation = {
            'construction_mode': 'policy_constrained_exit_only',
            'rebalance_philosophy': policy.rebalance_philosophy,
            'liquidity_floor_rules': list(policy.liquidity_floor_rules),
            'concentration_cap_rules': list(policy.concentration_cap_rules),
            'allowed_exposure_rules': list(policy.allowed_exposure_rules),
            'prohibited_exposure_rules': list(policy.prohibited_exposure_rules),
            'account_treatment_rules': list(policy.account_treatment_rules),
            'target_accounts': target_account_summaries,
        }
        for account in household_snapshot.holdings_by_account:
            for position in account['verified_positions']:
                symbol = str(position['instrument'].get('symbol') or '').upper()
                if symbol not in prohibited_symbols:
                    continue
                if str(position['instrument'].get('kind') or '').upper() != 'EQUITY':
                    blocked_conditions.append(
                        _blocked_condition(
                            code='UNSUPPORTED_PROHIBITED_POSITION_KIND',
                            reason_codes=['PROHIBITED_POSITION_REQUIRES_UNSUPPORTED_EXECUTION_KIND'],
                            detail_refs=[f'position_id:{position["position_id"]}', f'policy_prohibition:symbol:{symbol}'],
                        )
                    )
                    continue
                qty = int(position['qty'])
                if qty <= 0:
                    continue
                change_scope = {
                    'account_id': str(account['account_id']),
                    'position_id': str(position['position_id']),
                    'symbol': symbol,
                    'qty': qty,
                    'side': 'SELL',
                    'change_code': 'exit_prohibited_symbol',
                }
                change_id = canonical_sha256_hex_v1(change_scope)
                required_directional_changes.append(
                    {
                        'change_id': change_id,
                        'change_code': 'exit_prohibited_symbol',
                        'account_id': str(account['account_id']),
                        'direction': 'decrease',
                        'side': 'SELL',
                        'quantity_shares': qty,
                        'instrument': {
                            'kind': str(position['instrument'].get('kind') or ''),
                            'symbol': str(position['instrument'].get('symbol') or ''),
                            'currency': str(position['instrument'].get('currency') or ''),
                            'ib_conId': position['instrument'].get('ib_conId'),
                            'ib_localSymbol': position['instrument'].get('ib_localSymbol'),
                        },
                        'reason_codes': ['PROHIBITED_SYMBOL_EXIT_REQUIRED'],
                        'detail_refs': [f'position_id:{position["position_id"]}', f'policy_prohibition:symbol:{symbol}'],
                    }
                )
        required_directional_changes = sorted(
            required_directional_changes,
            key=lambda item: (item['account_id'], item['instrument']['symbol'], item['side'], item['quantity_shares'], item['change_id']),
        )
        action_needed = bool(required_directional_changes)
        hard_blocked = any(item['condition_code'] == 'UNSUPPORTED_PROHIBITED_POSITION_KIND' for item in blocked_conditions)
        if hard_blocked:
            execution_eligibility = 'BLOCKED'
        elif household_snapshot.validity_tier == 'VALID_EXECUTION_ELIGIBLE':
            execution_eligibility = 'EXECUTION_ELIGIBLE'
        elif household_snapshot.validity_tier == 'VALID_ADVISORY_ONLY':
            execution_eligibility = 'ADVISORY_ONLY'
        else:
            execution_eligibility = 'BLOCKED'

    if household_snapshot.validity_tier == 'VALID_ADVISORY_ONLY':
        blocked_conditions.append(
            _blocked_condition(
                code='UPSTREAM_HOUSEHOLD_NOT_EXECUTION_ELIGIBLE',
                reason_codes=list(household_snapshot.reason_codes),
                detail_refs=list(household_snapshot.input_record_refs),
            )
        )

    if household_snapshot.unverified_components:
        constrained_deviations.append(
            _constrained_deviation(
                code='UNVERIFIED_COMPONENTS_EXCLUDED_FROM_EXECUTION_SCOPE',
                reason_codes=['UNVERIFIED_COMPONENTS_PRESENT'],
                detail_refs=[str(item['record_ref']) for item in household_snapshot.unverified_components],
            )
        )
    stale_component_refs = [
        str(item['record_ref'])
        for item in household_snapshot.freshness_attestations
        if str(item['freshness_status']) != 'CURRENT'
    ]
    if stale_component_refs:
        constrained_deviations.append(
            _constrained_deviation(
                code='STALE_COMPONENTS_BLOCK_EXECUTION_SCOPE',
                reason_codes=['STALE_VERIFIED_COMPONENTS_PRESENT'],
                detail_refs=stale_component_refs,
            )
        )
    if execution_eligibility not in PORTFOLIO_INTENT_EXECUTION_ELIGIBILITY_V1:
        raise ValueError('INVALID_EXECUTION_ELIGIBILITY_CONFIGURATION')
    if str(current_value_allocation['status']) not in ALLOCATION_ADVISORY_STATUSES_V1:
        raise ValueError('CURRENT_VALUE_ALLOCATION_STATUS_INVALID')
    if str(allocation_drift['status']) not in ALLOCATION_ADVISORY_STATUSES_V1:
        raise ValueError('ALLOCATION_DRIFT_STATUS_INVALID')

    validity_tier = household_snapshot.validity_tier
    if validity_tier not in ADVISORY_VALIDITY_TIERS_V1:
        raise ValueError('INVALID_VALIDITY_TIER_CONFIGURATION')

    effective_change_scope = {
        'target_allocation': target_allocation,
        'current_value_allocation': current_value_allocation,
        'allocation_drift': allocation_drift,
        'required_directional_changes': [dict(item) for item in required_directional_changes],
        'blocked_conditions': sorted(blocked_conditions, key=lambda item: (item['condition_code'], tuple(item['detail_refs']))),
        'constrained_deviations': sorted(constrained_deviations, key=lambda item: (item['deviation_code'], tuple(item['detail_refs']))),
        'action_needed': action_needed,
        'execution_eligibility': execution_eligibility,
    }
    portfolio_intent_fingerprint = canonical_sha256_hex_v1(effective_change_scope)

    input_record_refs = sorted(
        {
            f'policy_id:{policy.policy_id}',
            f'household_snapshot_id:{household_snapshot.household_snapshot_id}',
            *household_snapshot.input_record_refs,
            *[f'classification_ref:{item}' for item in classification_refs_norm],
        }
    )

    intent_scope = {
        'household_id': policy.household_id,
        'parent_policy_id': policy.policy_id,
        'parent_household_snapshot_id': household_snapshot.household_snapshot_id,
        'builder_version': builder_version_clean,
        'classification_refs': list(classification_refs_norm),
        'portfolio_intent_fingerprint': portfolio_intent_fingerprint,
        'contract_version': PORTFOLIO_INTENT_CONTRACT_VERSION_V1,
    }
    portfolio_intent_id = canonical_sha256_hex_v1(intent_scope)

    reason_codes = ['PORTFOLIO_INTENT_COMPILED']
    if policy.allocation_policy_mode == 'TARGET_WEIGHTS':
        reason_codes.append('POLICY_TARGET_WEIGHT_ALLOCATION')
        if current_value_allocation['status'] == 'READY':
            reason_codes.append('CURRENT_VALUE_ALLOCATION_READY')
        elif current_value_allocation['status'] == 'BLOCKED':
            reason_codes.append('CURRENT_VALUE_ALLOCATION_BLOCKED')
    else:
        reason_codes.append('POLICY_CONSTRAINED_EXIT_ONLY_TARGET')
    if action_needed:
        reason_codes.append('ACTION_NEEDED')
    else:
        reason_codes.append('NO_MATERIAL_PORTFOLIO_CHANGES')
    if execution_eligibility == 'EXECUTION_ELIGIBLE':
        reason_codes.append('PORTFOLIO_INTENT_EXECUTION_ELIGIBLE')
    elif execution_eligibility == 'ADVISORY_ONLY':
        reason_codes.append('PORTFOLIO_INTENT_ADVISORY_ONLY')
    if constrained_deviations:
        reason_codes.append('CONSTRAINED_DEVIATIONS_PRESENT')
    if blocked_conditions:
        reason_codes.append('BLOCKED_CONDITIONS_PRESENT')

    obj = {
        'schema_id': 'portfolio_intent',
        'schema_version': 'v1',
        'record_id': portfolio_intent_id,
        'portfolio_intent_id': portfolio_intent_id,
        'household_id': policy.household_id,
        'parent_policy_id': policy.policy_id,
        'parent_household_snapshot_id': household_snapshot.household_snapshot_id,
        'intent_version': 'v1',
        'created_at': created_at_clean,
        'effective_at': effective_at_clean,
        'actor_source': actor_source_clean,
        'timestamp_basis': 'policy_and_household_snapshot_effective_timestamps',
        'contract_version': PORTFOLIO_INTENT_CONTRACT_VERSION_V1,
        'builder_version': builder_version_clean,
        'parent_lineage_refs': [
            f'policy_id:{policy.policy_id}',
            f'household_snapshot_id:{household_snapshot.household_snapshot_id}',
        ],
        'classification_refs': list(classification_refs_norm),
        'target_allocation': target_allocation,
        'current_allocation_summary': current_allocation_summary,
        'current_value_allocation': current_value_allocation,
        'allocation_drift': allocation_drift,
        'required_directional_changes': [dict(item) for item in required_directional_changes],
        'blocked_conditions': sorted(blocked_conditions, key=lambda item: (item['condition_code'], tuple(item['detail_refs']))),
        'constrained_deviations': sorted(constrained_deviations, key=lambda item: (item['deviation_code'], tuple(item['detail_refs']))),
        'action_needed': action_needed,
        'execution_eligibility': execution_eligibility,
        'validity_tier': validity_tier,
        'reason_codes': sorted(set(reason_codes)),
        'input_record_refs': input_record_refs,
        'portfolio_intent_fingerprint': portfolio_intent_fingerprint,
    }
    return PortfolioIntentV1.from_dict(obj)


def write_portfolio_intent_v1(
    *,
    policy: PolicyV1,
    household_snapshot: HouseholdSnapshotV1,
    created_at: str,
    effective_at: str,
    actor_source: str,
    classification_refs: list[str] | tuple[str, ...] = (),
    builder_version: str = PORTFOLIO_INTENT_BUILDER_VERSION_V1,
    output_root: str = '',
) -> tuple[PortfolioIntentV1, str]:
    portfolio_intent = build_portfolio_intent_v1(
        policy=policy,
        household_snapshot=household_snapshot,
        created_at=created_at,
        effective_at=effective_at,
        actor_source=actor_source,
        classification_refs=classification_refs,
        builder_version=builder_version,
    )
    path = portfolio_intent_path_v1(output_root, portfolio_intent.household_id, portfolio_intent.portfolio_intent_id)
    written = write_immutable_json_v1(path, portfolio_intent.to_dict())
    return portfolio_intent, str(written)
