from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_EXECUTION/official_recommendation_set.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class OfficialRecommendationV1:
    recommendation_id: str
    domain_id: str
    action_type: str
    priority: int
    recommendation_status: str
    rationale_refs: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    constraints: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'OfficialRecommendationV1':
        return cls(
            recommendation_id=str(obj['recommendation_id']),
            domain_id=str(obj['domain_id']),
            action_type=str(obj['action_type']),
            priority=int(obj['priority']),
            recommendation_status=str(obj['recommendation_status']),
            rationale_refs=tuple(str(item) for item in obj['rationale_refs']),
            evidence_refs=tuple(str(item) for item in obj['evidence_refs']),
            constraints=tuple(str(item) for item in obj.get('constraints', [])),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            'recommendation_id': self.recommendation_id,
            'domain_id': self.domain_id,
            'action_type': self.action_type,
            'priority': self.priority,
            'recommendation_status': self.recommendation_status,
            'rationale_refs': list(self.rationale_refs),
            'evidence_refs': list(self.evidence_refs),
            'constraints': list(self.constraints),
        }


@dataclass(frozen=True, slots=True)
class OfficialRecommendationSetV1:
    advisory_packet_id: str
    created_at: str
    version: str
    recommendations: tuple[OfficialRecommendationV1, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'OfficialRecommendationSetV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            advisory_packet_id=str(obj['advisory_packet_id']),
            created_at=str(obj['created_at']),
            version=str(obj['version']),
            recommendations=tuple(OfficialRecommendationV1.from_dict(item) for item in obj['recommendations']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'OfficialRecommendationSetV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': 'official_recommendation_set',
            'schema_version': 'v1',
            'advisory_packet_id': self.advisory_packet_id,
            'created_at': self.created_at,
            'version': self.version,
            'recommendations': [item.to_dict() for item in self.recommendations],
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
