from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_KERNEL/quarterly_action_summary.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class QuarterlyActionSummaryV1:
    schema_id: str
    schema_version: str
    authority_class: str
    support_status: str
    produced_utc: str
    run_id: str
    decision_plan_id: str
    quarter_label: str
    action_count: int
    blocked_action_count: int
    summary_lines: tuple[Any, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'QuarterlyActionSummaryV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            schema_id=str(obj['schema_id']),
            schema_version=str(obj['schema_version']),
            authority_class=str(obj['authority_class']),
            support_status=str(obj['support_status']),
            produced_utc=str(obj['produced_utc']),
            run_id=str(obj['run_id']),
            decision_plan_id=str(obj['decision_plan_id']),
            quarter_label=str(obj['quarter_label']),
            action_count=int(obj['action_count']),
            blocked_action_count=int(obj['blocked_action_count']),
            summary_lines=tuple(obj['summary_lines']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'QuarterlyActionSummaryV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'authority_class': self.authority_class,
            'support_status': self.support_status,
            'produced_utc': self.produced_utc,
            'run_id': self.run_id,
            'decision_plan_id': self.decision_plan_id,
            'quarter_label': self.quarter_label,
            'action_count': self.action_count,
            'blocked_action_count': self.blocked_action_count,
            'summary_lines': list(self.summary_lines),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
