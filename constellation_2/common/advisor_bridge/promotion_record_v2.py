from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_BRIDGE/promotion_record.v2.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class PromotionRecordV2:
    schema_id: str
    schema_version: str
    produced_utc: str
    run_id: str
    promotion_record_id: str
    household_id: str
    portfolio_intent_id: str
    promotion_decision_id: str
    parent_lineage_refs: tuple[str, ...]
    approved_delta: tuple[dict[str, Any], ...]
    blocked_scope: tuple[str, ...]
    reason_codes: tuple[str, ...]
    status: str
    idempotency_key: str
    timestamp_utc: str
    validity_status: str
    source_artifact_refs: tuple[str, ...]
    notes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'PromotionRecordV2':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['produced_utc']),
            str(obj['run_id']),
            str(obj['promotion_record_id']),
            str(obj['household_id']),
            str(obj['portfolio_intent_id']),
            str(obj['promotion_decision_id']),
            tuple(str(item) for item in obj['parent_lineage_refs']),
            tuple(dict(item) for item in obj['approved_delta']),
            tuple(str(item) for item in obj['blocked_scope']),
            tuple(str(item) for item in obj['reason_codes']),
            str(obj['status']),
            str(obj['idempotency_key']),
            str(obj['timestamp_utc']),
            str(obj['validity_status']),
            tuple(str(item) for item in obj['source_artifact_refs']),
            tuple(str(item) for item in obj['notes']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'PromotionRecordV2':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'produced_utc': self.produced_utc,
            'run_id': self.run_id,
            'promotion_record_id': self.promotion_record_id,
            'household_id': self.household_id,
            'portfolio_intent_id': self.portfolio_intent_id,
            'promotion_decision_id': self.promotion_decision_id,
            'parent_lineage_refs': list(self.parent_lineage_refs),
            'approved_delta': [dict(item) for item in self.approved_delta],
            'blocked_scope': list(self.blocked_scope),
            'reason_codes': list(self.reason_codes),
            'status': self.status,
            'idempotency_key': self.idempotency_key,
            'timestamp_utc': self.timestamp_utc,
            'validity_status': self.validity_status,
            'source_artifact_refs': list(self.source_artifact_refs),
            'notes': list(self.notes),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
