from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISORY/execution_intent.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class ExecutionIntentV1:
    schema_id: str
    schema_version: str
    record_id: str
    execution_intent_id: str
    promotion_record_id: str
    household_id: str
    created_at_utc: str
    effective_at_utc: str
    actor_source: str
    contract_version: str
    builder_version: str
    idempotency_key: str
    operation_type: str
    day_utc: str
    environment: str
    sleeve_id: str
    account_id: str
    engine_id: str
    instrument: dict[str, Any]
    side: str
    quantity_shares: int
    order_terms: dict[str, Any]
    parent_lineage_refs: tuple[str, ...]
    source_artifact_refs: tuple[str, ...]
    canonical_json_hash: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ExecutionIntentV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['execution_intent_id']),
            str(obj['promotion_record_id']),
            str(obj['household_id']),
            str(obj['created_at_utc']),
            str(obj['effective_at_utc']),
            str(obj['actor_source']),
            str(obj['contract_version']),
            str(obj['builder_version']),
            str(obj['idempotency_key']),
            str(obj['operation_type']),
            str(obj['day_utc']),
            str(obj['environment']),
            str(obj['sleeve_id']),
            str(obj['account_id']),
            str(obj['engine_id']),
            dict(obj['instrument']),
            str(obj['side']),
            int(obj['quantity_shares']),
            dict(obj['order_terms']),
            tuple(str(item) for item in obj['parent_lineage_refs']),
            tuple(str(item) for item in obj['source_artifact_refs']),
            str(obj['canonical_json_hash']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'ExecutionIntentV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'execution_intent_id': self.execution_intent_id,
            'promotion_record_id': self.promotion_record_id,
            'household_id': self.household_id,
            'created_at_utc': self.created_at_utc,
            'effective_at_utc': self.effective_at_utc,
            'actor_source': self.actor_source,
            'contract_version': self.contract_version,
            'builder_version': self.builder_version,
            'idempotency_key': self.idempotency_key,
            'operation_type': self.operation_type,
            'day_utc': self.day_utc,
            'environment': self.environment,
            'sleeve_id': self.sleeve_id,
            'account_id': self.account_id,
            'engine_id': self.engine_id,
            'instrument': dict(self.instrument),
            'side': self.side,
            'quantity_shares': self.quantity_shares,
            'order_terms': dict(self.order_terms),
            'parent_lineage_refs': list(self.parent_lineage_refs),
            'source_artifact_refs': list(self.source_artifact_refs),
            'canonical_json_hash': self.canonical_json_hash,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
