from __future__ import annotations

from decimal import Decimal
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.advisory.advisory_domain_constants_v1 import (
    PORTFOLIO_INTENT_EXECUTION_ELIGIBILITY_V1,
    PROMOTION_DECISION_CONTRACT_VERSION_V1,
    PROMOTION_DECISION_EVALUATOR_VERSION_V1,
    PROMOTION_DECISION_OUTCOMES_V1,
)
from constellation_2.common.advisory.advisory_storage_v1 import (
    promotion_decision_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.advisory.household_snapshot_v1 import HouseholdSnapshotV1
from constellation_2.common.advisory.policy_v1 import PolicyV1
from constellation_2.common.advisory.portfolio_intent_v1 import PortfolioIntentV1
from constellation_2.common.advisory.promotion_decision_v1 import PromotionDecisionV1


def _require_nonempty_str(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field.upper()}_REQUIRED')
    return value.strip()


def _parse_positive_int_rule(rule: str, *, prefix: str) -> int | None:
    if not rule.startswith(prefix):
        return None
    try:
        value = int(rule.split(':', 1)[1])
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f'POLICY_GATE_RULE_INVALID:{rule}') from exc
    if value <= 0:
        raise ValueError(f'POLICY_GATE_RULE_INVALID:{rule}')
    return value


def _gate_config(policy: PolicyV1) -> dict[str, Any]:
    config: dict[str, Any] = {
        'require_execution_eligible_snapshot': False,
        'min_trade_shares': None,
        'max_trade_entries': None,
    }
    for rule in policy.promotion_gate_rules:
        text = str(rule).strip()
        if text == 'require_execution_eligible_snapshot':
            config['require_execution_eligible_snapshot'] = True
            continue
        min_shares = _parse_positive_int_rule(text, prefix='min_trade_shares:')
        if min_shares is not None:
            config['min_trade_shares'] = min_shares
            continue
        max_entries = _parse_positive_int_rule(text, prefix='max_trade_entries:')
        if max_entries is not None:
            config['max_trade_entries'] = max_entries
            continue
        raise ValueError(f'UNSUPPORTED_POLICY_GATE_RULE:{text}')
    return config


def build_promotion_decision_v1(
    *,
    policy: PolicyV1,
    household_snapshot: HouseholdSnapshotV1,
    portfolio_intent: PortfolioIntentV1,
    created_at: str,
    effective_at: str,
    actor_source: str,
    evaluator_version: str = PROMOTION_DECISION_EVALUATOR_VERSION_V1,
) -> PromotionDecisionV1:
    if policy.policy_completeness_status != 'VALID':
        raise ValueError('PARENT_POLICY_NOT_VALID')
    if household_snapshot.parent_policy_id != policy.policy_id:
        raise ValueError('PROMOTION_DECISION_POLICY_SNAPSHOT_MISMATCH')
    if portfolio_intent.parent_policy_id != policy.policy_id:
        raise ValueError('PROMOTION_DECISION_POLICY_INTENT_MISMATCH')
    if portfolio_intent.parent_household_snapshot_id != household_snapshot.household_snapshot_id:
        raise ValueError('PROMOTION_DECISION_HOUSEHOLD_INTENT_MISMATCH')
    if portfolio_intent.household_id != household_snapshot.household_id or portfolio_intent.household_id != policy.household_id:
        raise ValueError('PROMOTION_DECISION_HOUSEHOLD_SCOPE_MISMATCH')
    if portfolio_intent.execution_eligibility not in PORTFOLIO_INTENT_EXECUTION_ELIGIBILITY_V1:
        raise ValueError('PROMOTION_DECISION_PORTFOLIO_INTENT_EXECUTION_ELIGIBILITY_INVALID')

    created_at_clean = _require_nonempty_str(created_at, field='created_at')
    effective_at_clean = _require_nonempty_str(effective_at, field='effective_at')
    actor_source_clean = _require_nonempty_str(actor_source, field='actor_source')
    evaluator_version_clean = _require_nonempty_str(evaluator_version, field='evaluator_version')
    if evaluator_version_clean != PROMOTION_DECISION_EVALUATOR_VERSION_V1:
        raise ValueError('UNSUPPORTED_PROMOTION_DECISION_EVALUATOR_VERSION')

    gate_config = _gate_config(policy)
    reason_codes: list[str] = ['PROMOTION_DECISION_EVALUATED']
    blocked_change_ids: list[str] = []
    approved_change_ids: list[str] = []
    if policy.allocation_policy_mode == 'TARGET_WEIGHTS':
        drifts = sorted(
            (dict(item) for item in portfolio_intent.allocation_drift.get('drift_entries', [])),
            key=lambda item: (str(item['asset_type']), str(item['symbol']), str(item['drift_id'])),
        )
        tradable_drifts = [item for item in drifts if str(item.get('asset_type') or '').upper() != 'CASH']
        drift_ids = [str(item['drift_id']) for item in tradable_drifts]
        if household_snapshot.validity_tier == 'INVALID_HARD_STOP':
            outcome = 'blocked'
            reason_codes.append('HOUSEHOLD_SNAPSHOT_HARD_STOP')
            blocked_change_ids = drift_ids
        elif portfolio_intent.validity_tier == 'INVALID_HARD_STOP' or portfolio_intent.execution_eligibility == 'BLOCKED':
            outcome = 'blocked'
            reason_codes.append('PORTFOLIO_INTENT_BLOCKED')
            blocked_change_ids = drift_ids
        elif gate_config['require_execution_eligible_snapshot'] and household_snapshot.validity_tier != 'VALID_EXECUTION_ELIGIBLE':
            outcome = 'blocked'
            reason_codes.append('POLICY_REQUIRES_EXECUTION_ELIGIBLE_HOUSEHOLD')
            blocked_change_ids = drift_ids
        elif portfolio_intent.blocked_conditions:
            outcome = 'blocked'
            reason_codes.append('PORTFOLIO_INTENT_BLOCKED_CONDITIONS_PRESENT')
            blocked_change_ids = drift_ids
        elif str(portfolio_intent.allocation_drift.get('status') or '') == 'BLOCKED':
            outcome = 'blocked'
            reason_codes.append('ALLOCATION_DRIFT_BLOCKED')
            blocked_change_ids = drift_ids
        elif not portfolio_intent.action_needed or not tradable_drifts:
            outcome = 'no_action'
            reason_codes.append('NO_ACTION_REQUIRED')
        else:
            rebalance_threshold = Decimal(str(policy.rebalance_threshold))
            minimum_trade_value_cents = int(policy.minimum_trade_value_cents)
            under_threshold = []
            material = []
            for item in tradable_drifts:
                abs_weight = abs(Decimal(str(item['drift_weight'])))
                abs_value = abs(int(item['delta_value_cents']))
                if abs_weight < rebalance_threshold or abs_value < minimum_trade_value_cents:
                    under_threshold.append(str(item['drift_id']))
                else:
                    material.append(str(item['drift_id']))
            if not material:
                outcome = 'no_action'
                reason_codes.append('ALL_DRIFT_UNDER_POLICY_THRESHOLD')
                blocked_change_ids = under_threshold
            elif gate_config['max_trade_entries'] is not None and len(material) > int(gate_config['max_trade_entries']):
                outcome = 'blocked'
                reason_codes.append('POLICY_MAX_TRADE_ENTRIES_EXCEEDED')
                blocked_change_ids = drift_ids
            else:
                outcome = 'promote'
                reason_codes.append('PROMOTION_ELIGIBLE')
                approved_change_ids = material
                blocked_change_ids = under_threshold
    else:
        changes = sorted(
            (dict(item) for item in portfolio_intent.required_directional_changes),
            key=lambda item: (str(item['account_id']), str(item['instrument']['symbol']), str(item['side']), int(item['quantity_shares']), str(item['change_id'])),
        )
        change_ids = [str(item['change_id']) for item in changes]
        if household_snapshot.validity_tier == 'INVALID_HARD_STOP':
            outcome = 'blocked'
            reason_codes.append('HOUSEHOLD_SNAPSHOT_HARD_STOP')
            blocked_change_ids = change_ids
        elif portfolio_intent.validity_tier == 'INVALID_HARD_STOP' or portfolio_intent.execution_eligibility == 'BLOCKED':
            outcome = 'blocked'
            reason_codes.append('PORTFOLIO_INTENT_BLOCKED')
            blocked_change_ids = change_ids
        elif gate_config['require_execution_eligible_snapshot'] and household_snapshot.validity_tier != 'VALID_EXECUTION_ELIGIBLE':
            outcome = 'blocked'
            reason_codes.append('POLICY_REQUIRES_EXECUTION_ELIGIBLE_HOUSEHOLD')
            blocked_change_ids = change_ids
        elif portfolio_intent.blocked_conditions:
            outcome = 'blocked'
            reason_codes.append('PORTFOLIO_INTENT_BLOCKED_CONDITIONS_PRESENT')
            blocked_change_ids = change_ids
        elif not portfolio_intent.action_needed or not changes:
            outcome = 'no_action'
            reason_codes.append('NO_ACTION_REQUIRED')
        elif gate_config['max_trade_entries'] is not None and len(changes) > int(gate_config['max_trade_entries']):
            outcome = 'blocked'
            reason_codes.append('POLICY_MAX_TRADE_ENTRIES_EXCEEDED')
            blocked_change_ids = change_ids
        else:
            min_trade_shares = gate_config['min_trade_shares']
            under_threshold = [
                str(item['change_id'])
                for item in changes
                if min_trade_shares is not None and int(item['quantity_shares']) < int(min_trade_shares)
            ]
            if under_threshold and len(under_threshold) == len(changes):
                outcome = 'no_action'
                reason_codes.append('ALL_CHANGES_UNDER_POLICY_MIN_TRADE_THRESHOLD')
                blocked_change_ids = under_threshold
            elif under_threshold:
                outcome = 'blocked'
                reason_codes.append('MIXED_THRESHOLD_SCOPE_UNSUPPORTED')
                blocked_change_ids = change_ids
            else:
                outcome = 'promote'
                reason_codes.append('PROMOTION_ELIGIBLE')
                approved_change_ids = change_ids

    if outcome not in PROMOTION_DECISION_OUTCOMES_V1:
        raise ValueError('PROMOTION_DECISION_OUTCOME_INVALID')

    decision_scope = {
        'household_id': portfolio_intent.household_id,
        'parent_policy_id': policy.policy_id,
        'parent_portfolio_intent_id': portfolio_intent.portfolio_intent_id,
        'approved_change_ids': approved_change_ids,
        'blocked_change_ids': blocked_change_ids,
        'outcome': outcome,
        'reason_codes': sorted(set(reason_codes)),
        'contract_version': PROMOTION_DECISION_CONTRACT_VERSION_V1,
        'evaluator_version': evaluator_version_clean,
    }
    promotion_decision_id = canonical_sha256_hex_v1(decision_scope)
    obj = {
        'schema_id': 'promotion_decision',
        'schema_version': 'v1',
        'record_id': promotion_decision_id,
        'promotion_decision_id': promotion_decision_id,
        'household_id': portfolio_intent.household_id,
        'parent_policy_id': policy.policy_id,
        'parent_portfolio_intent_id': portfolio_intent.portfolio_intent_id,
        'created_at': created_at_clean,
        'effective_at': effective_at_clean,
        'actor_source': actor_source_clean,
        'contract_version': PROMOTION_DECISION_CONTRACT_VERSION_V1,
        'evaluator_version': evaluator_version_clean,
        'parent_lineage_refs': [
            f'policy_id:{policy.policy_id}',
            f'portfolio_intent_id:{portfolio_intent.portfolio_intent_id}',
        ],
        'approved_change_ids': sorted(set(approved_change_ids)),
        'blocked_change_ids': sorted(set(blocked_change_ids)),
        'outcome': outcome,
        'reason_codes': sorted(set(reason_codes)),
        'input_record_refs': sorted(
            {
                f'policy_id:{policy.policy_id}',
                f'household_snapshot_id:{household_snapshot.household_snapshot_id}',
                f'portfolio_intent_id:{portfolio_intent.portfolio_intent_id}',
            }
        ),
    }
    return PromotionDecisionV1.from_dict(obj)


def write_promotion_decision_v1(
    *,
    policy: PolicyV1,
    household_snapshot: HouseholdSnapshotV1,
    portfolio_intent: PortfolioIntentV1,
    created_at: str,
    effective_at: str,
    actor_source: str,
    evaluator_version: str = PROMOTION_DECISION_EVALUATOR_VERSION_V1,
    output_root: str = '',
) -> tuple[PromotionDecisionV1, str]:
    decision = build_promotion_decision_v1(
        policy=policy,
        household_snapshot=household_snapshot,
        portfolio_intent=portfolio_intent,
        created_at=created_at,
        effective_at=effective_at,
        actor_source=actor_source,
        evaluator_version=evaluator_version,
    )
    path = promotion_decision_path_v1(output_root, decision.household_id, decision.promotion_decision_id)
    written = write_immutable_json_v1(path, decision.to_dict())
    return decision, str(written)
