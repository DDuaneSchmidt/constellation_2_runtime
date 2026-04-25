from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import read_json_obj_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/EXECUTION/multi_delta_execution_decision.v1.schema.json'


@dataclass(frozen=True, slots=True)
class MultiDeltaExecutionDecisionV1:
    schema_id: str
    schema_version: str
    record_id: str
    multi_delta_execution_decision_id: str
    approved_change_set_id: str
    household_id: str
    day_utc: str
    produced_utc: str
    contract_version: str
    outcome: str
    reason_codes: tuple[str, ...]
    duplicate_multi_delta_execution_record_id: str | None
    input_record_refs: tuple[str, ...]
    canonical_json_hash: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'MultiDeltaExecutionDecisionV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['multi_delta_execution_decision_id']),
            str(obj['approved_change_set_id']),
            str(obj['household_id']),
            str(obj['day_utc']),
            str(obj['produced_utc']),
            str(obj['contract_version']),
            str(obj['outcome']),
            tuple(str(item) for item in obj['reason_codes']),
            None if obj.get('duplicate_multi_delta_execution_record_id') is None else str(obj['duplicate_multi_delta_execution_record_id']),
            tuple(str(item) for item in obj['input_record_refs']),
            str(obj['canonical_json_hash']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'MultiDeltaExecutionDecisionV1':
        return cls.from_dict(read_json_obj_v1(path))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'multi_delta_execution_decision_id': self.multi_delta_execution_decision_id,
            'approved_change_set_id': self.approved_change_set_id,
            'household_id': self.household_id,
            'day_utc': self.day_utc,
            'produced_utc': self.produced_utc,
            'contract_version': self.contract_version,
            'outcome': self.outcome,
            'reason_codes': list(self.reason_codes),
            'duplicate_multi_delta_execution_record_id': self.duplicate_multi_delta_execution_record_id,
            'input_record_refs': list(self.input_record_refs),
            'canonical_json_hash': self.canonical_json_hash,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
