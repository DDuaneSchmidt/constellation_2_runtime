from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_EXECUTION/decision_action.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class DecisionActionV1:
    action_id: str
    plan_id: str
    recommendation_id: str
    domain_id: str
    action_type: str
    action_status: str
    support_status: str
    priority: int
    source_account: str | None
    destination_account: str | None
    annual_amount: int
    periodic_amount: int
    periodicity: str
    constraints: tuple[str, ...]
    blockers: tuple[str, ...]
    rationale_refs: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    created_at: str
    version: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'DecisionActionV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            action_id=str(obj['action_id']),
            plan_id=str(obj['plan_id']),
            recommendation_id=str(obj['recommendation_id']),
            domain_id=str(obj['domain_id']),
            action_type=str(obj['action_type']),
            action_status=str(obj['action_status']),
            support_status=str(obj['support_status']),
            priority=int(obj['priority']),
            source_account=obj['source_account'],
            destination_account=obj['destination_account'],
            annual_amount=int(obj['annual_amount']),
            periodic_amount=int(obj['periodic_amount']),
            periodicity=str(obj['periodicity']),
            constraints=tuple(str(item) for item in obj['constraints']),
            blockers=tuple(str(item) for item in obj['blockers']),
            rationale_refs=tuple(str(item) for item in obj['rationale_refs']),
            evidence_refs=tuple(str(item) for item in obj['evidence_refs']),
            created_at=str(obj['created_at']),
            version=str(obj['version']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'DecisionActionV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'action_id': self.action_id,
            'plan_id': self.plan_id,
            'recommendation_id': self.recommendation_id,
            'domain_id': self.domain_id,
            'action_type': self.action_type,
            'action_status': self.action_status,
            'support_status': self.support_status,
            'priority': self.priority,
            'source_account': self.source_account,
            'destination_account': self.destination_account,
            'annual_amount': self.annual_amount,
            'periodic_amount': self.periodic_amount,
            'periodicity': self.periodicity,
            'constraints': list(self.constraints),
            'blockers': list(self.blockers),
            'rationale_refs': list(self.rationale_refs),
            'evidence_refs': list(self.evidence_refs),
            'created_at': self.created_at,
            'version': self.version,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
