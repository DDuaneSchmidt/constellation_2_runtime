from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_KERNEL/publication_gate_result.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class PublicationGateResultV1:
    schema_id: str
    schema_version: str
    authority_class: str
    support_status: str
    produced_utc: str
    run_id: str
    artifact_family: str
    artifact_ref: str
    publication_status: str
    publication_class: str
    reason_codes: tuple[Any, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'PublicationGateResultV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            schema_id=str(obj['schema_id']),
            schema_version=str(obj['schema_version']),
            authority_class=str(obj['authority_class']),
            support_status=str(obj['support_status']),
            produced_utc=str(obj['produced_utc']),
            run_id=str(obj['run_id']),
            artifact_family=str(obj['artifact_family']),
            artifact_ref=str(obj['artifact_ref']),
            publication_status=str(obj['publication_status']),
            publication_class=str(obj['publication_class']),
            reason_codes=tuple(obj['reason_codes']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'PublicationGateResultV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'authority_class': self.authority_class,
            'support_status': self.support_status,
            'produced_utc': self.produced_utc,
            'run_id': self.run_id,
            'artifact_family': self.artifact_family,
            'artifact_ref': self.artifact_ref,
            'publication_status': self.publication_status,
            'publication_class': self.publication_class,
            'reason_codes': list(self.reason_codes),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
