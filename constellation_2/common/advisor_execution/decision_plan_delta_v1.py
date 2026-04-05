from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_EXECUTION/decision_plan_delta.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class DecisionPlanDeltaV1:
    plan_delta_id: str
    previous_plan_id: str
    current_plan_id: str
    delta_type: str
    changed_action_ids: tuple[str, ...]
    summary: str
    version: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'DecisionPlanDeltaV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            plan_delta_id=str(obj['plan_delta_id']),
            previous_plan_id=str(obj['previous_plan_id']),
            current_plan_id=str(obj['current_plan_id']),
            delta_type=str(obj['delta_type']),
            changed_action_ids=tuple(str(item) for item in obj['changed_action_ids']),
            summary=str(obj['summary']),
            version=str(obj['version']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'DecisionPlanDeltaV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': 'decision_plan_delta',
            'schema_version': 'v1',
            'plan_delta_id': self.plan_delta_id,
            'previous_plan_id': self.previous_plan_id,
            'current_plan_id': self.current_plan_id,
            'delta_type': self.delta_type,
            'changed_action_ids': list(self.changed_action_ids),
            'summary': self.summary,
            'version': self.version,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
