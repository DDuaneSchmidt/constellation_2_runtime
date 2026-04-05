from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_BRIDGE/promotion_gate_result.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class PromotionGateResultV1:
    schema_id: str
    schema_version: str
    produced_utc: str
    run_id: str
    gate_result_id: str
    candidate_id: str
    review_id: str
    manual_review_id: str
    gate_status: str
    reason_codes: tuple[str, ...]
    source_artifact_refs: tuple[str, ...]
    selection_basis: str
    notes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'PromotionGateResultV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['produced_utc']),
            str(obj['run_id']),
            str(obj['gate_result_id']),
            str(obj['candidate_id']),
            str(obj['review_id']),
            str(obj['manual_review_id']),
            str(obj['gate_status']),
            tuple(str(item) for item in obj['reason_codes']),
            tuple(str(item) for item in obj['source_artifact_refs']),
            str(obj['selection_basis']),
            tuple(str(item) for item in obj['notes']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'PromotionGateResultV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'produced_utc': self.produced_utc,
            'run_id': self.run_id,
            'gate_result_id': self.gate_result_id,
            'candidate_id': self.candidate_id,
            'review_id': self.review_id,
            'manual_review_id': self.manual_review_id,
            'gate_status': self.gate_status,
            'reason_codes': list(self.reason_codes),
            'source_artifact_refs': list(self.source_artifact_refs),
            'selection_basis': self.selection_basis,
            'notes': list(self.notes),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
