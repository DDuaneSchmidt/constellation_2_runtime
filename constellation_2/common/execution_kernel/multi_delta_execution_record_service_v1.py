from __future__ import annotations

from pathlib import Path

from constellation_2.common.execution_kernel.approved_change_set_v1 import ApprovedChangeSetV1
from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    multi_delta_execution_record_path_v1,
    write_exclusive_immutable_json_v1,
)
from constellation_2.common.execution_kernel.multi_delta_execution_decision_v1 import MultiDeltaExecutionDecisionV1
from constellation_2.common.execution_kernel.multi_delta_execution_record_v1 import MultiDeltaExecutionRecordV1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


CONTRACT_VERSION = 'multi_delta_execution_record_contract_v1'


def _sorted_unique(items: list[str]) -> list[str]:
    return sorted(set(str(item) for item in items if str(item).strip()))


def build_multi_delta_execution_record_v1(
    *,
    approved_change_set: ApprovedChangeSetV1,
    decision: MultiDeltaExecutionDecisionV1,
    produced_utc: str,
) -> MultiDeltaExecutionRecordV1:
    if decision.outcome != 'promote':
        raise ValueError('MULTI_DELTA_EXECUTION_DECISION_NOT_PROMOTE')
    if approved_change_set.status != 'READY':
        raise ValueError('APPROVED_CHANGE_SET_NOT_READY')
    if not approved_change_set.ordered_changes:
        raise ValueError('MULTI_DELTA_EXECUTION_RECORD_REQUIRES_MEMBERS')

    record_id = canonical_hash_for_c2_artifact_v1(
        {
            'approved_change_set_id': approved_change_set.approved_change_set_id,
            'ordered_changes': [dict(item) for item in approved_change_set.ordered_changes],
            'status': 'AUTHORIZED',
        }
    )
    obj = {
        'schema_id': 'multi_delta_execution_record',
        'schema_version': 'v1',
        'record_id': record_id,
        'multi_delta_execution_record_id': record_id,
        'approved_change_set_id': approved_change_set.approved_change_set_id,
        'multi_delta_execution_decision_id': decision.multi_delta_execution_decision_id,
        'promotion_record_id': approved_change_set.promotion_record_id,
        'household_id': approved_change_set.household_id,
        'day_utc': approved_change_set.day_utc,
        'produced_utc': str(produced_utc),
        'contract_version': CONTRACT_VERSION,
        'status': 'AUTHORIZED',
        'ordered_changes': [dict(item) for item in approved_change_set.ordered_changes],
        'input_record_refs': _sorted_unique(
            [
                f'approved_change_set_id:{approved_change_set.approved_change_set_id}',
                f'multi_delta_execution_decision_id:{decision.multi_delta_execution_decision_id}',
                *decision.input_record_refs,
            ]
        ),
        'parent_lineage_refs': list(approved_change_set.parent_lineage_refs),
        'source_artifact_refs': list(approved_change_set.source_artifact_refs),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return MultiDeltaExecutionRecordV1.from_dict(obj)


def write_multi_delta_execution_record_v1(
    *,
    approved_change_set: ApprovedChangeSetV1,
    decision: MultiDeltaExecutionDecisionV1,
    produced_utc: str,
    truth_root: str | Path | None = None,
) -> tuple[MultiDeltaExecutionRecordV1, str, str]:
    record = build_multi_delta_execution_record_v1(
        approved_change_set=approved_change_set,
        decision=decision,
        produced_utc=produced_utc,
    )
    path = multi_delta_execution_record_path_v1(
        truth_root=truth_root,
        day_utc=record.day_utc,
        multi_delta_execution_record_id=record.multi_delta_execution_record_id,
    )
    written = write_exclusive_immutable_json_v1(path, record.to_dict())
    return record, str(written.path), str(written.action)
