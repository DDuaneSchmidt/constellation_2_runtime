from __future__ import annotations

from typing import Any

from constellation_2.common.advisory.policy_v1 import PolicyV1
from constellation_2.common.advisory.portfolio_intent_v1 import PortfolioIntentV1
from constellation_2.common.advisory.promotion_decision_v1 import PromotionDecisionV1
from constellation_2.common.advisor_bridge.promotion_record_v2 import PromotionRecordV2
from constellation_2.common.execution_kernel.approved_change_set_v1 import ApprovedChangeSetV1
from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    approved_change_set_path_v1,
    write_immutable_json_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


CONTRACT_VERSION = 'approved_change_set_contract_v1'


def _sorted_unique(items: list[str] | tuple[str, ...]) -> list[str]:
    return sorted(set(str(item) for item in items if str(item).strip()))


def _coherent_ordering(changes: list[dict[str, Any]]) -> bool:
    keys = [
        (
            str(item['account_id']),
            str((item.get('instrument') or {}).get('symbol') or ''),
            str(item['side']),
            int(item['quantity_shares']),
        )
        for item in changes
    ]
    return len(keys) == len(set(keys))


def build_approved_change_set_v1(
    *,
    policy: PolicyV1,
    portfolio_intent: PortfolioIntentV1,
    promotion_decision: PromotionDecisionV1,
    promotion_record: PromotionRecordV2,
    produced_utc: str,
) -> ApprovedChangeSetV1:
    if portfolio_intent.parent_policy_id != policy.policy_id:
        raise ValueError('APPROVED_CHANGE_SET_POLICY_INTENT_MISMATCH')
    if promotion_decision.parent_policy_id != policy.policy_id:
        raise ValueError('APPROVED_CHANGE_SET_POLICY_DECISION_MISMATCH')
    if promotion_record.portfolio_intent_id != portfolio_intent.portfolio_intent_id:
        raise ValueError('APPROVED_CHANGE_SET_RECORD_INTENT_MISMATCH')
    if promotion_record.promotion_decision_id != promotion_decision.promotion_decision_id:
        raise ValueError('APPROVED_CHANGE_SET_RECORD_DECISION_MISMATCH')

    value_basis_id = portfolio_intent.current_value_allocation.get('value_basis_id')
    reason_codes: list[str] = []
    status = 'READY'
    ordered_changes: list[dict[str, Any]] = []

    if portfolio_intent.current_value_allocation.get('status') != 'READY' or not isinstance(value_basis_id, str) or not value_basis_id.strip():
        status = 'BLOCKED'
        reason_codes.append('APPROVED_CHANGE_SET_VALUE_BASIS_MISSING')
    elif promotion_record.status == 'BLOCKED':
        status = 'BLOCKED'
        reason_codes.append('APPROVED_CHANGE_SET_UPSTREAM_BLOCKED')
    else:
        for item in promotion_record.approved_delta:
            reference_price_cents = None
            delta_value_cents = None
            drift_id = str(item['change_id'])
            for drift in portfolio_intent.allocation_drift.get('drift_entries', []):
                if str(drift.get('drift_id') or '') == drift_id:
                    reference_price_cents = drift.get('reference_price_cents')
                    delta_value_cents = drift.get('delta_value_cents')
                    break
            if not isinstance(reference_price_cents, int) or reference_price_cents <= 0:
                status = 'BLOCKED'
                reason_codes.append(f'APPROVED_CHANGE_SET_REFERENCE_PRICE_MISSING:{drift_id}')
                continue
            if not isinstance(delta_value_cents, int):
                status = 'BLOCKED'
                reason_codes.append(f'APPROVED_CHANGE_SET_DELTA_VALUE_MISSING:{drift_id}')
                continue
            routing_account_id = str(item.get('account_id') or '').strip()
            if not routing_account_id:
                status = 'BLOCKED'
                reason_codes.append(f'APPROVED_CHANGE_SET_ROUTING_ACCOUNT_MISSING:{drift_id}')
                continue
            if int(item['quantity_shares']) <= 0:
                status = 'BLOCKED'
                reason_codes.append(f'APPROVED_CHANGE_SET_ZERO_MEMBER:{drift_id}')
                continue
            ordered_changes.append(
                {
                    'change_id': drift_id,
                    'member_order': 0,
                    'account_id': routing_account_id,
                    'environment': str(item['environment']),
                    'sleeve_id': str(item['sleeve_id']),
                    'operation_type': str(item['operation_type']),
                    'engine_id': str(item['engine_id']),
                    'instrument': dict(item['instrument']),
                    'side': str(item['side']),
                    'quantity_shares': int(item['quantity_shares']),
                    'order_terms': dict(item['order_terms']),
                    'reference_price_cents': int(reference_price_cents),
                    'delta_value_cents': int(delta_value_cents),
                    'routing_account_id': routing_account_id,
                    'value_basis_id': str(value_basis_id),
                    'reason_codes': sorted(set(str(code) for code in item.get('reason_codes', []))) or ['APPROVED_CHANGE_SET_MEMBER'],
                    'detail_refs': sorted(set(str(ref) for ref in item.get('detail_refs', []))) or [f'change_id:{drift_id}'],
                }
            )

        ordered_changes = sorted(
            ordered_changes,
            key=lambda item: (
                str(item['account_id']),
                str(item['instrument']['symbol']),
                str(item['side']),
                int(item['quantity_shares']),
                str(item['change_id']),
            ),
        )
        for index, item in enumerate(ordered_changes):
            item['member_order'] = index
        if ordered_changes and not _coherent_ordering(ordered_changes):
            status = 'BLOCKED'
            reason_codes.append('APPROVED_CHANGE_SET_ORDERING_AMBIGUOUS')

    if not reason_codes:
        reason_codes.append('APPROVED_CHANGE_SET_READY' if ordered_changes else 'APPROVED_CHANGE_SET_EMPTY')

    parent_lineage_refs = _sorted_unique(
        [
            f'portfolio_intent_id:{portfolio_intent.portfolio_intent_id}',
            f'promotion_decision_id:{promotion_decision.promotion_decision_id}',
            f'promotion_record_id:{promotion_record.promotion_record_id}',
            *promotion_record.parent_lineage_refs,
        ]
    )
    input_record_refs = _sorted_unique(
        [
            f'portfolio_intent_id:{portfolio_intent.portfolio_intent_id}',
            f'promotion_decision_id:{promotion_decision.promotion_decision_id}',
            f'promotion_record_id:{promotion_record.promotion_record_id}',
        ]
    )
    change_scope = {
        'portfolio_intent_id': portfolio_intent.portfolio_intent_id,
        'promotion_decision_id': promotion_decision.promotion_decision_id,
        'promotion_record_id': promotion_record.promotion_record_id,
        'value_basis_id': value_basis_id,
        'status': status,
        'ordered_changes': ordered_changes,
    }
    approved_change_set_id = canonical_hash_for_c2_artifact_v1(change_scope)
    stable_produced_utc = str(promotion_record.produced_utc)
    obj = {
        'schema_id': 'approved_change_set',
        'schema_version': 'v1',
        'record_id': approved_change_set_id,
        'approved_change_set_id': approved_change_set_id,
        'household_id': portfolio_intent.household_id,
        'day_utc': stable_produced_utc[:10],
        'produced_utc': stable_produced_utc,
        'contract_version': CONTRACT_VERSION,
        'status': status,
        'portfolio_intent_id': portfolio_intent.portfolio_intent_id,
        'promotion_decision_id': promotion_decision.promotion_decision_id,
        'promotion_record_id': promotion_record.promotion_record_id,
        'value_basis_id': None if value_basis_id is None else str(value_basis_id),
        'ordered_changes': ordered_changes,
        'reason_codes': _sorted_unique(reason_codes),
        'input_record_refs': input_record_refs,
        'parent_lineage_refs': parent_lineage_refs,
        'source_artifact_refs': _sorted_unique(
            [
                *promotion_record.source_artifact_refs,
                f'value_basis_id:{value_basis_id}' if value_basis_id else 'value_basis_id:none',
            ]
        ),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return ApprovedChangeSetV1.from_dict(obj)


def write_approved_change_set_v1(
    *,
    policy: PolicyV1,
    portfolio_intent: PortfolioIntentV1,
    promotion_decision: PromotionDecisionV1,
    promotion_record: PromotionRecordV2,
    produced_utc: str,
    truth_root: str | None = None,
) -> tuple[ApprovedChangeSetV1, str]:
    change_set = build_approved_change_set_v1(
        policy=policy,
        portfolio_intent=portfolio_intent,
        promotion_decision=promotion_decision,
        promotion_record=promotion_record,
        produced_utc=produced_utc,
    )
    path = approved_change_set_path_v1(
        truth_root=truth_root,
        day_utc=change_set.day_utc,
        approved_change_set_id=change_set.approved_change_set_id,
    )
    written = write_immutable_json_v1(path, change_set.to_dict())
    return change_set, str(written.path)
