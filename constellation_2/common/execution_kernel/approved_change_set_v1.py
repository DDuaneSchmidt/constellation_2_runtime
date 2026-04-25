from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import read_json_obj_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/EXECUTION/approved_change_set.v1.schema.json'


@dataclass(frozen=True, slots=True)
class ApprovedChangeSetV1:
    schema_id: str
    schema_version: str
    record_id: str
    approved_change_set_id: str
    household_id: str
    day_utc: str
    produced_utc: str
    contract_version: str
    status: str
    portfolio_intent_id: str
    promotion_decision_id: str
    promotion_record_id: str
    value_basis_id: str | None
    ordered_changes: tuple[dict[str, Any], ...]
    reason_codes: tuple[str, ...]
    input_record_refs: tuple[str, ...]
    parent_lineage_refs: tuple[str, ...]
    source_artifact_refs: tuple[str, ...]
    canonical_json_hash: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ApprovedChangeSetV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['approved_change_set_id']),
            str(obj['household_id']),
            str(obj['day_utc']),
            str(obj['produced_utc']),
            str(obj['contract_version']),
            str(obj['status']),
            str(obj['portfolio_intent_id']),
            str(obj['promotion_decision_id']),
            str(obj['promotion_record_id']),
            None if obj.get('value_basis_id') is None else str(obj['value_basis_id']),
            tuple(dict(item) for item in obj['ordered_changes']),
            tuple(str(item) for item in obj['reason_codes']),
            tuple(str(item) for item in obj['input_record_refs']),
            tuple(str(item) for item in obj['parent_lineage_refs']),
            tuple(str(item) for item in obj['source_artifact_refs']),
            str(obj['canonical_json_hash']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'ApprovedChangeSetV1':
        return cls.from_dict(read_json_obj_v1(path))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'approved_change_set_id': self.approved_change_set_id,
            'household_id': self.household_id,
            'day_utc': self.day_utc,
            'produced_utc': self.produced_utc,
            'contract_version': self.contract_version,
            'status': self.status,
            'portfolio_intent_id': self.portfolio_intent_id,
            'promotion_decision_id': self.promotion_decision_id,
            'promotion_record_id': self.promotion_record_id,
            'value_basis_id': self.value_basis_id,
            'ordered_changes': [dict(item) for item in self.ordered_changes],
            'reason_codes': list(self.reason_codes),
            'input_record_refs': list(self.input_record_refs),
            'parent_lineage_refs': list(self.parent_lineage_refs),
            'source_artifact_refs': list(self.source_artifact_refs),
            'canonical_json_hash': self.canonical_json_hash,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
