from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISORY/kernel_run_envelope.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class KernelRunEnvelopeV1:
    schema_id: str
    schema_version: str
    record_id: str
    kernel_run_id: str
    household_id: str
    produced_utc: str
    day_utc: str
    contract_version: str
    envelope_version: str
    run_outcome: str
    stage_status: dict[str, Any]
    artifact_refs: dict[str, Any]
    handoff_status: dict[str, Any]
    reason_codes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'KernelRunEnvelopeV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['kernel_run_id']),
            str(obj['household_id']),
            str(obj['produced_utc']),
            str(obj['day_utc']),
            str(obj['contract_version']),
            str(obj['envelope_version']),
            str(obj['run_outcome']),
            dict(obj['stage_status']),
            dict(obj['artifact_refs']),
            dict(obj['handoff_status']),
            tuple(str(item) for item in obj['reason_codes']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'KernelRunEnvelopeV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'kernel_run_id': self.kernel_run_id,
            'household_id': self.household_id,
            'produced_utc': self.produced_utc,
            'day_utc': self.day_utc,
            'contract_version': self.contract_version,
            'envelope_version': self.envelope_version,
            'run_outcome': self.run_outcome,
            'stage_status': dict(self.stage_status),
            'artifact_refs': dict(self.artifact_refs),
            'handoff_status': dict(self.handoff_status),
            'reason_codes': list(self.reason_codes),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
