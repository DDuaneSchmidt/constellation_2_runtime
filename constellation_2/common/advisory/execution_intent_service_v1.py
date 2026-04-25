from __future__ import annotations

from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1, canonical_sha256_hex_v1
from constellation_2.common.advisory.advisory_domain_constants_v1 import (
    EXECUTION_INTENT_BUILDER_VERSION_V1,
    EXECUTION_INTENT_CONTRACT_VERSION_V1,
)
from constellation_2.common.advisory.advisory_storage_v1 import (
    execution_intent_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.advisor_bridge.promotion_record_v2 import PromotionRecordV2


def _require_nonempty_str(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field.upper()}_REQUIRED')
    return value.strip()


def build_execution_intent_v1(
    *,
    promotion_record: PromotionRecordV2,
    created_at_utc: str,
    effective_at_utc: str,
    actor_source: str,
    builder_version: str = EXECUTION_INTENT_BUILDER_VERSION_V1,
) -> ExecutionIntentV1:
    if promotion_record.status != 'AUTHORIZED' or promotion_record.validity_status != 'VALID':
        raise ValueError('PROMOTION_RECORD_NOT_EXECUTABLE')
    if len(promotion_record.approved_delta) != 1:
        raise ValueError('MULTI_DELTA_EXECUTION_INTENT_UNSUPPORTED')
    created_at_clean = _require_nonempty_str(created_at_utc, field='created_at_utc')
    effective_at_clean = _require_nonempty_str(effective_at_utc, field='effective_at_utc')
    actor_source_clean = _require_nonempty_str(actor_source, field='actor_source')
    builder_version_clean = _require_nonempty_str(builder_version, field='builder_version')
    if builder_version_clean != EXECUTION_INTENT_BUILDER_VERSION_V1:
        raise ValueError('UNSUPPORTED_EXECUTION_INTENT_BUILDER_VERSION')

    delta = dict(promotion_record.approved_delta[0])
    execution_intent_id = promotion_record.idempotency_key
    obj = {
        'schema_id': 'execution_intent',
        'schema_version': 'v1',
        'record_id': execution_intent_id,
        'execution_intent_id': execution_intent_id,
        'promotion_record_id': promotion_record.promotion_record_id,
        'household_id': promotion_record.household_id,
        'created_at_utc': created_at_clean,
        'effective_at_utc': effective_at_clean,
        'actor_source': actor_source_clean,
        'contract_version': EXECUTION_INTENT_CONTRACT_VERSION_V1,
        'builder_version': builder_version_clean,
        'idempotency_key': promotion_record.idempotency_key,
        'operation_type': str(delta['operation_type']),
        'day_utc': effective_at_clean[:10],
        'environment': str(delta['environment']),
        'sleeve_id': str(delta['sleeve_id']),
        'account_id': str(delta['account_id']),
        'engine_id': str(delta['engine_id']),
        'instrument': dict(delta['instrument']),
        'side': str(delta['side']),
        'quantity_shares': int(delta['quantity_shares']),
        'order_terms': dict(delta['order_terms']),
        'parent_lineage_refs': [
            f'promotion_record_id:{promotion_record.promotion_record_id}',
            *promotion_record.parent_lineage_refs,
        ],
        'source_artifact_refs': list(promotion_record.source_artifact_refs),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_sha256_hex_v1({
        'execution_intent': canonical_json_bytes_v1({**obj, 'canonical_json_hash': None}).decode('utf-8'),
    })
    return ExecutionIntentV1.from_dict(obj)


def write_execution_intent_v1(
    *,
    promotion_record: PromotionRecordV2,
    created_at_utc: str,
    effective_at_utc: str,
    actor_source: str,
    builder_version: str = EXECUTION_INTENT_BUILDER_VERSION_V1,
    output_root: str = '',
) -> tuple[ExecutionIntentV1, str]:
    execution_intent = build_execution_intent_v1(
        promotion_record=promotion_record,
        created_at_utc=created_at_utc,
        effective_at_utc=effective_at_utc,
        actor_source=actor_source,
        builder_version=builder_version,
    )
    path = execution_intent_path_v1(output_root, execution_intent.household_id, execution_intent.execution_intent_id)
    written = write_immutable_json_v1(path, execution_intent.to_dict())
    return execution_intent, str(written)
