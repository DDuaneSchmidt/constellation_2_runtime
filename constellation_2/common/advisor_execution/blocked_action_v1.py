from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_EXECUTION/blocked_action.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class BlockedActionV1:
    action_id: str
    plan_id: str
    recommendation_id: str
    domain_id: str
    action_type: str
    support_status: str
    blockers: tuple[str, ...]
    rationale_refs: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    created_at: str
    version: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'BlockedActionV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            action_id=str(obj['action_id']),
            plan_id=str(obj['plan_id']),
            recommendation_id=str(obj['recommendation_id']),
            domain_id=str(obj['domain_id']),
            action_type=str(obj['action_type']),
            support_status=str(obj['support_status']),
            blockers=tuple(str(item) for item in obj['blockers']),
            rationale_refs=tuple(str(item) for item in obj['rationale_refs']),
            evidence_refs=tuple(str(item) for item in obj['evidence_refs']),
            created_at=str(obj['created_at']),
            version=str(obj['version']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'BlockedActionV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'action_id': self.action_id,
            'plan_id': self.plan_id,
            'recommendation_id': self.recommendation_id,
            'domain_id': self.domain_id,
            'action_type': self.action_type,
            'support_status': self.support_status,
            'blockers': list(self.blockers),
            'rationale_refs': list(self.rationale_refs),
            'evidence_refs': list(self.evidence_refs),
            'created_at': self.created_at,
            'version': self.version,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
