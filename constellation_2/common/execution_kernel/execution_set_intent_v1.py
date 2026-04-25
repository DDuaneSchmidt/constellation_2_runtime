from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import read_json_obj_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_set_intent.v1.schema.json'


@dataclass(frozen=True, slots=True)
class ExecutionSetIntentV1:
    schema_id: str
    schema_version: str
    record_id: str
    execution_set_intent_id: str
    multi_delta_execution_record_id: str
    promotion_record_id: str
    household_id: str
    day_utc: str
    produced_utc: str
    contract_version: str
    status: str
    member_intents: tuple[dict[str, Any], ...]
    input_record_refs: tuple[str, ...]
    parent_lineage_refs: tuple[str, ...]
    source_artifact_refs: tuple[str, ...]
    canonical_json_hash: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ExecutionSetIntentV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['execution_set_intent_id']),
            str(obj['multi_delta_execution_record_id']),
            str(obj['promotion_record_id']),
            str(obj['household_id']),
            str(obj['day_utc']),
            str(obj['produced_utc']),
            str(obj['contract_version']),
            str(obj['status']),
            tuple(dict(item) for item in obj['member_intents']),
            tuple(str(item) for item in obj['input_record_refs']),
            tuple(str(item) for item in obj['parent_lineage_refs']),
            tuple(str(item) for item in obj['source_artifact_refs']),
            str(obj['canonical_json_hash']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'ExecutionSetIntentV1':
        return cls.from_dict(read_json_obj_v1(path))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'execution_set_intent_id': self.execution_set_intent_id,
            'multi_delta_execution_record_id': self.multi_delta_execution_record_id,
            'promotion_record_id': self.promotion_record_id,
            'household_id': self.household_id,
            'day_utc': self.day_utc,
            'produced_utc': self.produced_utc,
            'contract_version': self.contract_version,
            'status': self.status,
            'member_intents': [dict(item) for item in self.member_intents],
            'input_record_refs': list(self.input_record_refs),
            'parent_lineage_refs': list(self.parent_lineage_refs),
            'source_artifact_refs': list(self.source_artifact_refs),
            'canonical_json_hash': self.canonical_json_hash,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
