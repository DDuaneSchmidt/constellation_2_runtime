from __future__ import annotations

from pathlib import Path

from constellation_2.common.advisory.policy_v1 import PolicyV1
from constellation_2.common.execution_kernel.approved_change_set_v1 import ApprovedChangeSetV1
from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    multi_delta_execution_decision_path_v1,
    multi_delta_execution_record_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.execution_kernel.multi_delta_execution_decision_v1 import MultiDeltaExecutionDecisionV1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


CONTRACT_VERSION = 'multi_delta_execution_decision_contract_v1'


def _sorted_unique(items: list[str]) -> list[str]:
    return sorted(set(str(item) for item in items if str(item).strip()))


def _max_trade_entries(policy: PolicyV1) -> int | None:
    for rule in policy.promotion_gate_rules:
        text = str(rule)
        if text.startswith('max_trade_entries:'):
            try:
                return int(text.split(':', 1)[1])
            except Exception:
                return None
    return None


def _predicted_record_id(change_set: ApprovedChangeSetV1) -> str:
    return canonical_hash_for_c2_artifact_v1(
        {
            'approved_change_set_id': change_set.approved_change_set_id,
            'ordered_changes': [dict(item) for item in change_set.ordered_changes],
            'status': 'AUTHORIZED',
        }
    )


def build_multi_delta_execution_decision_v1(
    *,
    policy: PolicyV1,
    approved_change_set: ApprovedChangeSetV1,
    produced_utc: str,
    truth_root: str | Path | None = None,
) -> MultiDeltaExecutionDecisionV1:
    outcome = 'promote'
    reason_codes: list[str] = []
    duplicate_record_id = None
    max_trade_entries = _max_trade_entries(policy)

    if approved_change_set.status != 'READY':
        outcome = 'blocked'
        reason_codes.append('APPROVED_CHANGE_SET_NOT_READY')
    elif approved_change_set.value_basis_id is None:
        outcome = 'blocked'
        reason_codes.append('APPROVED_CHANGE_SET_VALUE_BASIS_MISSING')
    elif not approved_change_set.ordered_changes:
        outcome = 'no_action'
        reason_codes.append('APPROVED_CHANGE_SET_EMPTY')
    elif max_trade_entries is not None and len(approved_change_set.ordered_changes) > max_trade_entries:
        outcome = 'blocked'
        reason_codes.append('APPROVED_CHANGE_SET_TOO_MANY_CHANGES')
    else:
        candidate_record_id = _predicted_record_id(approved_change_set)
        candidate_path = multi_delta_execution_record_path_v1(
            truth_root=truth_root,
            day_utc=approved_change_set.day_utc,
            multi_delta_execution_record_id=candidate_record_id,
        )
        if candidate_path.exists():
            outcome = 'duplicate'
            duplicate_record_id = candidate_record_id
            reason_codes.append('MULTI_DELTA_EXECUTION_RECORD_EXISTS')
        else:
            reason_codes.append('MULTI_DELTA_EXECUTION_PROMOTE')

    obj = {
        'schema_id': 'multi_delta_execution_decision',
        'schema_version': 'v1',
        'record_id': canonical_hash_for_c2_artifact_v1(
            {
                'approved_change_set_id': approved_change_set.approved_change_set_id,
                'outcome': outcome,
                'duplicate_multi_delta_execution_record_id': duplicate_record_id,
            }
        ),
        'multi_delta_execution_decision_id': canonical_hash_for_c2_artifact_v1(
            {
                'approved_change_set_id': approved_change_set.approved_change_set_id,
                'outcome': outcome,
            }
        ),
        'approved_change_set_id': approved_change_set.approved_change_set_id,
        'household_id': approved_change_set.household_id,
        'day_utc': approved_change_set.day_utc,
        'produced_utc': str(produced_utc),
        'contract_version': CONTRACT_VERSION,
        'outcome': outcome,
        'reason_codes': _sorted_unique(reason_codes),
        'duplicate_multi_delta_execution_record_id': duplicate_record_id,
        'input_record_refs': _sorted_unique(
            [
                f'approved_change_set_id:{approved_change_set.approved_change_set_id}',
                *approved_change_set.input_record_refs,
            ]
        ),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return MultiDeltaExecutionDecisionV1.from_dict(obj)


def write_multi_delta_execution_decision_v1(
    *,
    policy: PolicyV1,
    approved_change_set: ApprovedChangeSetV1,
    produced_utc: str,
    truth_root: str | Path | None = None,
) -> tuple[MultiDeltaExecutionDecisionV1, str]:
    decision = build_multi_delta_execution_decision_v1(
        policy=policy,
        approved_change_set=approved_change_set,
        produced_utc=produced_utc,
        truth_root=truth_root,
    )
    path = multi_delta_execution_decision_path_v1(
        truth_root=truth_root,
        day_utc=decision.day_utc,
        multi_delta_execution_decision_id=decision.multi_delta_execution_decision_id,
    )
    written = write_immutable_json_v1(path, decision.to_dict())
    return decision, str(written.path)
