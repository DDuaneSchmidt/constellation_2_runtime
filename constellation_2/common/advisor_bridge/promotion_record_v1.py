from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_BRIDGE/promotion_record.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class PromotionRecordV1:
    schema_id: str
    schema_version: str
    produced_utc: str
    run_id: str
    promotion_id: str
    candidate_id: str
    review_id: str
    manual_review_id: str
    gate_result_id: str
    proposal_id: str
    planning_snapshot_id: str
    decision_plan_id: str
    parent_lineage_refs: tuple[str, ...]
    eligible_scope: tuple[str, ...]
    blocked_scope: tuple[str, ...]
    reason_codes: tuple[str, ...]
    approval_state: str
    execution_readiness_state: str
    idempotency_key: str
    timestamp_utc: str
    validity_status: str
    source_artifact_refs: tuple[str, ...]
    notes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'PromotionRecordV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['produced_utc']),
            str(obj['run_id']),
            str(obj['promotion_id']),
            str(obj['candidate_id']),
            str(obj['review_id']),
            str(obj['manual_review_id']),
            str(obj['gate_result_id']),
            str(obj['proposal_id']),
            str(obj['planning_snapshot_id']),
            str(obj['decision_plan_id']),
            tuple(str(item) for item in obj['parent_lineage_refs']),
            tuple(str(item) for item in obj['eligible_scope']),
            tuple(str(item) for item in obj['blocked_scope']),
            tuple(str(item) for item in obj['reason_codes']),
            str(obj['approval_state']),
            str(obj['execution_readiness_state']),
            str(obj['idempotency_key']),
            str(obj['timestamp_utc']),
            str(obj['validity_status']),
            tuple(str(item) for item in obj['source_artifact_refs']),
            tuple(str(item) for item in obj['notes']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'PromotionRecordV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'produced_utc': self.produced_utc,
            'run_id': self.run_id,
            'promotion_id': self.promotion_id,
            'candidate_id': self.candidate_id,
            'review_id': self.review_id,
            'manual_review_id': self.manual_review_id,
            'gate_result_id': self.gate_result_id,
            'proposal_id': self.proposal_id,
            'planning_snapshot_id': self.planning_snapshot_id,
            'decision_plan_id': self.decision_plan_id,
            'parent_lineage_refs': list(self.parent_lineage_refs),
            'eligible_scope': list(self.eligible_scope),
            'blocked_scope': list(self.blocked_scope),
            'reason_codes': list(self.reason_codes),
            'approval_state': self.approval_state,
            'execution_readiness_state': self.execution_readiness_state,
            'idempotency_key': self.idempotency_key,
            'timestamp_utc': self.timestamp_utc,
            'validity_status': self.validity_status,
            'source_artifact_refs': list(self.source_artifact_refs),
            'notes': list(self.notes),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
