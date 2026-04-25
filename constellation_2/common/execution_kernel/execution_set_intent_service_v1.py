from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.advisory.advisory_domain_constants_v1 import (
    EXECUTION_INTENT_BUILDER_VERSION_V1,
    EXECUTION_INTENT_CONTRACT_VERSION_V1,
)
from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.advisory.execution_package_builder_from_execution_intent_v1 import (
    derive_execution_submission_identity_from_execution_intent_v1,
)
from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    execution_set_intent_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.execution_kernel.execution_set_intent_v1 import ExecutionSetIntentV1
from constellation_2.common.execution_kernel.multi_delta_execution_record_v1 import MultiDeltaExecutionRecordV1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


CONTRACT_VERSION = 'execution_set_intent_contract_v1'


def _sorted_unique(items: list[str]) -> list[str]:
    return sorted(set(str(item) for item in items if str(item).strip()))


def _build_execution_intent_from_change(
    *,
    record: MultiDeltaExecutionRecordV1,
    change: dict[str, Any],
    created_at_utc: str,
    effective_at_utc: str,
    actor_source: str,
) -> ExecutionIntentV1:
    execution_intent_id = canonical_hash_for_c2_artifact_v1(
        {
            'multi_delta_execution_record_id': record.multi_delta_execution_record_id,
            'member_order': int(change['member_order']),
            'change_id': str(change['change_id']),
            'delta': dict(change),
        }
    )
    obj = {
        'schema_id': 'execution_intent',
        'schema_version': 'v1',
        'record_id': execution_intent_id,
        'execution_intent_id': execution_intent_id,
        'promotion_record_id': record.promotion_record_id,
        'household_id': record.household_id,
        'created_at_utc': str(created_at_utc),
        'effective_at_utc': str(effective_at_utc),
        'actor_source': str(actor_source),
        'contract_version': EXECUTION_INTENT_CONTRACT_VERSION_V1,
        'builder_version': EXECUTION_INTENT_BUILDER_VERSION_V1,
        'idempotency_key': execution_intent_id,
        'operation_type': str(change['operation_type']),
        'day_utc': str(effective_at_utc)[:10],
        'environment': str(change['environment']),
        'sleeve_id': str(change['sleeve_id']),
        'account_id': str(change['account_id']),
        'engine_id': str(change['engine_id']),
        'instrument': dict(change['instrument']),
        'side': str(change['side']),
        'quantity_shares': int(change['quantity_shares']),
        'order_terms': dict(change['order_terms']),
        'parent_lineage_refs': _sorted_unique(
            [
                f'multi_delta_execution_record_id:{record.multi_delta_execution_record_id}',
                f'approved_change_id:{change["change_id"]}',
                *record.parent_lineage_refs,
            ]
        ),
        'source_artifact_refs': _sorted_unique(
            [
                *record.source_artifact_refs,
                f'approved_change_set_id:{record.approved_change_set_id}',
            ]
        ),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return ExecutionIntentV1.from_dict(obj)


def build_execution_set_intent_v1(
    *,
    record: MultiDeltaExecutionRecordV1,
    created_at_utc: str,
    effective_at_utc: str,
    actor_source: str,
) -> ExecutionSetIntentV1:
    if record.status != 'AUTHORIZED':
        raise ValueError('MULTI_DELTA_EXECUTION_RECORD_NOT_AUTHORIZED')
    member_rows: list[dict[str, Any]] = []
    for change in record.ordered_changes:
        execution_intent = _build_execution_intent_from_change(
            record=record,
            change=dict(change),
            created_at_utc=created_at_utc,
            effective_at_utc=effective_at_utc,
            actor_source=actor_source,
        )
        derived = derive_execution_submission_identity_from_execution_intent_v1(execution_intent=execution_intent)
        member_rows.append(
            {
                'member_order': int(change['member_order']),
                'change_id': str(change['change_id']),
                'execution_intent': execution_intent.to_dict(),
                'predicted_submission_id': str(derived['submission_id']),
                'predicted_trade_instance_id': str(derived['trade_instance_id']),
                'reason_codes': ['EXECUTION_SET_MEMBER_READY'],
            }
        )
    obj = {
        'schema_id': 'execution_set_intent',
        'schema_version': 'v1',
        'record_id': canonical_hash_for_c2_artifact_v1(
            {
                'multi_delta_execution_record_id': record.multi_delta_execution_record_id,
                'member_execution_intent_ids': [row['execution_intent']['execution_intent_id'] for row in member_rows],
            }
        ),
        'execution_set_intent_id': canonical_hash_for_c2_artifact_v1(
            {
                'multi_delta_execution_record_id': record.multi_delta_execution_record_id,
                'member_count': len(member_rows),
            }
        ),
        'multi_delta_execution_record_id': record.multi_delta_execution_record_id,
        'promotion_record_id': record.promotion_record_id,
        'household_id': record.household_id,
        'day_utc': record.day_utc,
        'produced_utc': str(created_at_utc),
        'contract_version': CONTRACT_VERSION,
        'status': 'READY',
        'member_intents': member_rows,
        'input_record_refs': _sorted_unique(
            [
                f'multi_delta_execution_record_id:{record.multi_delta_execution_record_id}',
                *record.input_record_refs,
            ]
        ),
        'parent_lineage_refs': list(record.parent_lineage_refs),
        'source_artifact_refs': list(record.source_artifact_refs),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return ExecutionSetIntentV1.from_dict(obj)


def write_execution_set_intent_v1(
    *,
    record: MultiDeltaExecutionRecordV1,
    created_at_utc: str,
    effective_at_utc: str,
    actor_source: str,
    truth_root: str | Path | None = None,
) -> tuple[ExecutionSetIntentV1, str]:
    execution_set_intent = build_execution_set_intent_v1(
        record=record,
        created_at_utc=created_at_utc,
        effective_at_utc=effective_at_utc,
        actor_source=actor_source,
    )
    path = execution_set_intent_path_v1(
        truth_root=truth_root,
        day_utc=execution_set_intent.day_utc,
        execution_set_intent_id=execution_set_intent.execution_set_intent_id,
    )
    written = write_immutable_json_v1(path, execution_set_intent.to_dict())
    return execution_set_intent, str(written.path)
