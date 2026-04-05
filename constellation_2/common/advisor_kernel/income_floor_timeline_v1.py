from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_KERNEL/income_floor_timeline.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class IncomeFloorTimelineV1:
    schema_id: str
    schema_version: str
    authority_class: str
    support_status: str
    produced_utc: str
    run_id: str
    planning_snapshot_id: str
    guaranteed_monthly_income_cents: int
    required_monthly_spending_cents: int
    floor_gap_monthly_cents: int
    timeline: tuple[Any, ...]
    reason_codes: tuple[Any, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'IncomeFloorTimelineV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            schema_id=str(obj['schema_id']),
            schema_version=str(obj['schema_version']),
            authority_class=str(obj['authority_class']),
            support_status=str(obj['support_status']),
            produced_utc=str(obj['produced_utc']),
            run_id=str(obj['run_id']),
            planning_snapshot_id=str(obj['planning_snapshot_id']),
            guaranteed_monthly_income_cents=int(obj['guaranteed_monthly_income_cents']),
            required_monthly_spending_cents=int(obj['required_monthly_spending_cents']),
            floor_gap_monthly_cents=int(obj['floor_gap_monthly_cents']),
            timeline=tuple(obj['timeline']),
            reason_codes=tuple(obj['reason_codes']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'IncomeFloorTimelineV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'authority_class': self.authority_class,
            'support_status': self.support_status,
            'produced_utc': self.produced_utc,
            'run_id': self.run_id,
            'planning_snapshot_id': self.planning_snapshot_id,
            'guaranteed_monthly_income_cents': self.guaranteed_monthly_income_cents,
            'required_monthly_spending_cents': self.required_monthly_spending_cents,
            'floor_gap_monthly_cents': self.floor_gap_monthly_cents,
            'timeline': list(self.timeline),
            'reason_codes': list(self.reason_codes),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
