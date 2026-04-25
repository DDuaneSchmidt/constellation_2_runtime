from __future__ import annotations

from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.advisory.advisory_storage_v1 import (
    promotion_record_path_v2,
    write_immutable_json_v1,
)
from constellation_2.common.advisory.policy_v1 import PolicyV1
from constellation_2.common.advisory.portfolio_intent_v1 import PortfolioIntentV1
from constellation_2.common.advisory.promotion_decision_v1 import PromotionDecisionV1
from constellation_2.common.advisor_bridge.promotion_record_v2 import PromotionRecordV2


def _require_nonempty_str(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field.upper()}_REQUIRED')
    return value.strip()


def _normalize_order_terms(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError('EXECUTION_PROFILE_ORDER_TERMS_REQUIRED')
    order_type = _require_nonempty_str(raw.get('order_type'), field='order_type').upper()
    time_in_force = _require_nonempty_str(raw.get('time_in_force'), field='time_in_force').upper()
    limit_price = raw.get('limit_price')
    if order_type not in {'LIMIT', 'MARKET'}:
        raise ValueError('ORDER_TYPE_INVALID')
    if time_in_force not in {'DAY', 'GTC'}:
        raise ValueError('TIME_IN_FORCE_INVALID')
    if order_type == 'LIMIT':
        if not isinstance(limit_price, str) or not limit_price.strip():
            raise ValueError('LIMIT_PRICE_REQUIRED')
        limit_price_value: str | None = limit_price.strip()
    else:
        if limit_price is not None:
            raise ValueError('MARKET_ORDER_LIMIT_PRICE_MUST_BE_NULL')
        limit_price_value = None
    return {
        'order_type': order_type,
        'limit_price': limit_price_value,
        'time_in_force': time_in_force,
    }


def _canonicalize_approved_delta(
    *,
    portfolio_intent: PortfolioIntentV1,
    approved_change_ids: tuple[str, ...],
    execution_profile: dict[str, Any],
) -> tuple[dict[str, Any], ...]:
    change_map = {str(item['change_id']): dict(item) for item in portfolio_intent.required_directional_changes}
    if len(change_map) != len(portfolio_intent.required_directional_changes):
        raise ValueError('PORTFOLIO_INTENT_DUPLICATE_CHANGE_IDS')
    environment = _require_nonempty_str(execution_profile.get('environment'), field='environment').upper()
    sleeve_id = _require_nonempty_str(execution_profile.get('sleeve_id'), field='sleeve_id').upper()
    operation_type = _require_nonempty_str(execution_profile.get('operation_type'), field='operation_type')
    account_id = _require_nonempty_str(execution_profile.get('account_id'), field='account_id')
    engine_id = _require_nonempty_str(execution_profile.get('engine_id'), field='engine_id')
    order_terms = _normalize_order_terms(execution_profile.get('order_terms'))
    entries: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for change_id in sorted(set(approved_change_ids)):
        change = change_map.get(change_id)
        if change is None:
            raise ValueError(f'PROMOTION_RECORD_CHANGE_NOT_FOUND:{change_id}')
        quantity_shares = int(change['quantity_shares'])
        if quantity_shares <= 0:
            raise ValueError(f'PROMOTION_RECORD_ZERO_OR_NEGATIVE_CHANGE:{change_id}')
        side = _require_nonempty_str(change.get('side'), field='side').upper()
        if side not in {'BUY', 'SELL'}:
            raise ValueError(f'PROMOTION_RECORD_SIDE_INVALID:{change_id}')
        instrument = dict(change['instrument'])
        if _require_nonempty_str(instrument.get('kind'), field='instrument.kind').upper() != 'EQUITY':
            raise ValueError(f'PROMOTION_RECORD_UNSUPPORTED_INSTRUMENT_KIND:{change_id}')
        entry = {
            'change_id': change_id,
            'account_id': account_id,
            'environment': environment,
            'sleeve_id': sleeve_id,
            'operation_type': operation_type,
            'engine_id': engine_id,
            'instrument': {
                'kind': str(instrument.get('kind') or ''),
                'symbol': str(instrument.get('symbol') or ''),
                'currency': str(instrument.get('currency') or ''),
                'ib_conId': instrument.get('ib_conId'),
                'ib_localSymbol': instrument.get('ib_localSymbol'),
            },
            'side': side,
            'quantity_shares': quantity_shares,
            'order_terms': order_terms,
            'reason_codes': sorted(set(str(item) for item in change.get('reason_codes', []))),
            'detail_refs': sorted(set(str(item) for item in change.get('detail_refs', []))),
        }
        entry_key = canonical_sha256_hex_v1(entry)
        if entry_key in seen_keys:
            raise ValueError(f'PROMOTION_RECORD_DUPLICATE_DELTA:{change_id}')
        seen_keys.add(entry_key)
        entries.append(entry)
    return tuple(sorted(entries, key=lambda item: (item['account_id'], item['instrument']['symbol'], item['side'], item['quantity_shares'], item['change_id'])))


def _canonicalize_allocation_delta(
    *,
    portfolio_intent: PortfolioIntentV1,
    approved_change_ids: tuple[str, ...],
    execution_profile: dict[str, Any],
) -> tuple[dict[str, Any], ...]:
    environment = _require_nonempty_str(execution_profile.get('environment'), field='environment').upper()
    sleeve_id = _require_nonempty_str(execution_profile.get('sleeve_id'), field='sleeve_id').upper()
    operation_type = _require_nonempty_str(execution_profile.get('operation_type'), field='operation_type')
    account_id = _require_nonempty_str(execution_profile.get('account_id'), field='account_id')
    engine_id = _require_nonempty_str(execution_profile.get('engine_id'), field='engine_id')
    order_terms = _normalize_order_terms(execution_profile.get('order_terms'))
    drift_map = {str(item['drift_id']): dict(item) for item in portfolio_intent.allocation_drift.get('drift_entries', [])}
    entries: list[dict[str, Any]] = []
    for drift_id in tuple(sorted(set(approved_change_ids))):
        drift = drift_map.get(drift_id)
        if drift is None:
            raise ValueError(f'ALLOCATION_DRIFT_NOT_FOUND:{drift_id}')
        if str(drift.get('asset_type') or '').upper() != 'EQUITY':
            raise ValueError(f'ALLOCATION_NON_EQUITY_EXECUTION_UNSUPPORTED:{drift_id}')
        routing_account_id = drift.get('routing_account_id')
        if not isinstance(routing_account_id, str) or routing_account_id.strip() != account_id:
            raise ValueError(f'ALLOCATION_ROUTING_ACCOUNT_UNAVAILABLE:{drift_id}')
        reference_price_cents = drift.get('reference_price_cents')
        if not isinstance(reference_price_cents, int) or reference_price_cents <= 0:
            raise ValueError(f'ALLOCATION_REFERENCE_PRICE_REQUIRED:{drift_id}')
        delta_value_cents = int(drift['delta_value_cents'])
        quantity_shares = abs(delta_value_cents) // int(reference_price_cents)
        if quantity_shares <= 0:
            raise ValueError(f'ALLOCATION_DELTA_TOO_SMALL_FOR_ONE_SHARE:{drift_id}')
        side = 'BUY' if delta_value_cents > 0 else 'SELL'
        entries.append(
            {
                'change_id': drift_id,
                'account_id': account_id,
                'environment': environment,
                'sleeve_id': sleeve_id,
                'operation_type': operation_type,
                'engine_id': engine_id,
                'instrument': {
                    'kind': 'EQUITY',
                    'symbol': str(drift['symbol']),
                    'currency': str(drift['currency']),
                    'ib_conId': None,
                    'ib_localSymbol': None,
                },
                'side': side,
                'quantity_shares': quantity_shares,
                'order_terms': order_terms,
                'reason_codes': ['ALLOCATION_REBALANCE_DELTA'],
                'detail_refs': [f'drift_id:{drift_id}', f'delta_value_cents:{delta_value_cents}', 'share_rounding:floor_toward_zero'],
            }
        )
    return tuple(sorted(entries, key=lambda item: (item['account_id'], item['instrument']['symbol'], item['side'], item['quantity_shares'], item['change_id'])))


def build_promotion_record_v2(
    *,
    policy: PolicyV1,
    portfolio_intent: PortfolioIntentV1,
    promotion_decision: PromotionDecisionV1,
    produced_utc: str,
    run_id: str,
    execution_profile: dict[str, Any],
    approval_confirmed: bool,
) -> PromotionRecordV2:
    produced_utc_clean = _require_nonempty_str(produced_utc, field='produced_utc')
    run_id_clean = _require_nonempty_str(run_id, field='run_id')
    if portfolio_intent.parent_policy_id != policy.policy_id:
        raise ValueError('PROMOTION_RECORD_POLICY_INTENT_MISMATCH')
    if promotion_decision.parent_policy_id != policy.policy_id:
        raise ValueError('PROMOTION_RECORD_POLICY_DECISION_MISMATCH')
    if promotion_decision.parent_portfolio_intent_id != portfolio_intent.portfolio_intent_id:
        raise ValueError('PROMOTION_RECORD_DECISION_INTENT_MISMATCH')
    if portfolio_intent.household_id != policy.household_id or promotion_decision.household_id != policy.household_id:
        raise ValueError('PROMOTION_RECORD_HOUSEHOLD_SCOPE_MISMATCH')

    source_artifact_refs = (
        f'policy_id:{policy.policy_id}',
        f'portfolio_intent_id:{portfolio_intent.portfolio_intent_id}',
        f'promotion_decision_id:{promotion_decision.promotion_decision_id}',
    )

    if promotion_decision.outcome == 'promote':
        if 'explicit_approval_required' in policy.approval_requirements and not approval_confirmed:
            status = 'BLOCKED'
            validity_status = 'INVALID_BLOCKED'
            approved_delta: tuple[dict[str, Any], ...] = ()
            reason_codes = ('APPROVAL_REQUIRED_BEFORE_AUTHORIZATION', 'PROMOTION_BLOCKED')
            blocked_scope = tuple(f'portfolio_change_id:{item}' for item in promotion_decision.approved_change_ids)
        else:
            if policy.allocation_policy_mode == 'TARGET_WEIGHTS':
                approved_delta = _canonicalize_allocation_delta(
                    portfolio_intent=portfolio_intent,
                    approved_change_ids=promotion_decision.approved_change_ids,
                    execution_profile=execution_profile,
                )
            else:
                approved_delta = _canonicalize_approved_delta(
                    portfolio_intent=portfolio_intent,
                    approved_change_ids=promotion_decision.approved_change_ids,
                    execution_profile=execution_profile,
                )
            if not approved_delta:
                raise ValueError('AUTHORIZED_PROMOTION_REQUIRES_APPROVED_DELTA')
            status = 'AUTHORIZED'
            validity_status = 'VALID'
            reason_codes = ('PROMOTION_AUTHORIZED',)
            blocked_scope = ()
    elif promotion_decision.outcome == 'no_action':
        status = 'NO_ACTION'
        validity_status = 'INVALID_NO_ACTION'
        approved_delta = ()
        reason_codes = ('PROMOTION_NO_ACTION',)
        blocked_scope = tuple(f'portfolio_change_id:{item}' for item in promotion_decision.blocked_change_ids)
    else:
        status = 'BLOCKED'
        validity_status = 'INVALID_BLOCKED'
        approved_delta = ()
        reason_codes = ('PROMOTION_BLOCKED',)
        blocked_scope = tuple(f'portfolio_change_id:{item}' for item in promotion_decision.blocked_change_ids)

    delta_scope = {
        'portfolio_intent_id': portfolio_intent.portfolio_intent_id,
        'promotion_decision_id': promotion_decision.promotion_decision_id,
        'status': status,
        'approved_delta': [dict(item) for item in approved_delta],
        'blocked_scope': list(blocked_scope),
        'contract_version': 'promotion_record_v2',
    }
    idempotency_key = canonical_sha256_hex_v1(delta_scope)
    obj = {
        'schema_id': 'promotion_record',
        'schema_version': 'v2',
        'produced_utc': produced_utc_clean,
        'run_id': run_id_clean,
        'promotion_record_id': idempotency_key,
        'household_id': portfolio_intent.household_id,
        'portfolio_intent_id': portfolio_intent.portfolio_intent_id,
        'promotion_decision_id': promotion_decision.promotion_decision_id,
        'parent_lineage_refs': [
            f'portfolio_intent_id:{portfolio_intent.portfolio_intent_id}',
            f'promotion_decision_id:{promotion_decision.promotion_decision_id}',
        ],
        'approved_delta': [dict(item) for item in approved_delta],
        'blocked_scope': list(blocked_scope),
        'reason_codes': sorted(set((*reason_codes, *promotion_decision.reason_codes))),
        'status': status,
        'idempotency_key': idempotency_key,
        'timestamp_utc': produced_utc_clean,
        'validity_status': validity_status,
        'source_artifact_refs': list(source_artifact_refs),
        'notes': ['kernel_authorization_record_v2', 'approved_delta_canonicalized'],
    }
    return PromotionRecordV2.from_dict(obj)


def write_promotion_record_v2(
    *,
    policy: PolicyV1,
    portfolio_intent: PortfolioIntentV1,
    promotion_decision: PromotionDecisionV1,
    produced_utc: str,
    run_id: str,
    execution_profile: dict[str, Any],
    approval_confirmed: bool,
    output_root: str = '',
) -> tuple[PromotionRecordV2, str]:
    record = build_promotion_record_v2(
        policy=policy,
        portfolio_intent=portfolio_intent,
        promotion_decision=promotion_decision,
        produced_utc=produced_utc,
        run_id=run_id,
        execution_profile=execution_profile,
        approval_confirmed=approval_confirmed,
    )
    path = promotion_record_path_v2(output_root, record.household_id, record.promotion_record_id)
    written = write_immutable_json_v1(path, record.to_dict())
    return record, str(written)
