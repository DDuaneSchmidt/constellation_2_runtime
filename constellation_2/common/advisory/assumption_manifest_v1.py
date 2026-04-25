from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISORY/assumption_manifest.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class AssumptionManifestV1:
    schema_id: str
    schema_version: str
    record_id: str
    assumption_manifest_id: str
    manifest_version: str
    manifest_type: str
    created_at: str
    effective_at: str
    actor_source: str
    timestamp_basis: str
    contract_version: str
    compatible_compiler_versions: tuple[str, ...]
    assumption_payload: dict[str, Any]
    reason_codes: tuple[str, ...]
    supersedes_manifest_id: str | None
    status: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'AssumptionManifestV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['assumption_manifest_id']),
            str(obj['manifest_version']),
            str(obj['manifest_type']),
            str(obj['created_at']),
            str(obj['effective_at']),
            str(obj['actor_source']),
            str(obj['timestamp_basis']),
            str(obj['contract_version']),
            tuple(str(item) for item in obj['compatible_compiler_versions']),
            dict(obj['assumption_payload']),
            tuple(str(item) for item in obj['reason_codes']),
            None if obj.get('supersedes_manifest_id') is None else str(obj['supersedes_manifest_id']),
            str(obj['status']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'AssumptionManifestV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'assumption_manifest_id': self.assumption_manifest_id,
            'manifest_version': self.manifest_version,
            'manifest_type': self.manifest_type,
            'created_at': self.created_at,
            'effective_at': self.effective_at,
            'actor_source': self.actor_source,
            'timestamp_basis': self.timestamp_basis,
            'contract_version': self.contract_version,
            'compatible_compiler_versions': list(self.compatible_compiler_versions),
            'assumption_payload': self.assumption_payload,
            'reason_codes': list(self.reason_codes),
            'supersedes_manifest_id': self.supersedes_manifest_id,
            'status': self.status,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
