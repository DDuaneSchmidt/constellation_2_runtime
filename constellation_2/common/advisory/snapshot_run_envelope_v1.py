from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.common.advisory.advisory_domain_constants_v1 import (
    SNAPSHOT_RUN_ENVELOPE_CONTRACT_VERSION_V1,
    SNAPSHOT_RUN_ENVELOPE_VERSION_V1,
)
from constellation_2.common.advisory.advisory_storage_v1 import (
    snapshot_run_envelope_path_v1,
    write_immutable_json_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISORY/snapshot_run_envelope.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class SnapshotRunEnvelopeV1:
    schema_id: str
    schema_version: str
    record_id: str
    snapshot_run_id: str
    household_id: str
    produced_utc: str
    effective_at: str
    contract_version: str
    envelope_version: str
    run_outcome: str
    artifact_refs: dict[str, Any]
    reason_codes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'SnapshotRunEnvelopeV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['snapshot_run_id']),
            str(obj['household_id']),
            str(obj['produced_utc']),
            str(obj['effective_at']),
            str(obj['contract_version']),
            str(obj['envelope_version']),
            str(obj['run_outcome']),
            dict(obj['artifact_refs']),
            tuple(str(item) for item in obj['reason_codes']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'SnapshotRunEnvelopeV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'snapshot_run_id': self.snapshot_run_id,
            'household_id': self.household_id,
            'produced_utc': self.produced_utc,
            'effective_at': self.effective_at,
            'contract_version': self.contract_version,
            'envelope_version': self.envelope_version,
            'run_outcome': self.run_outcome,
            'artifact_refs': self.artifact_refs,
            'reason_codes': list(self.reason_codes),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj


def write_snapshot_run_envelope_v1(
    *,
    snapshot_run_id: str,
    household_id: str,
    produced_utc: str,
    effective_at: str,
    run_outcome: str,
    artifact_refs: dict[str, Any],
    reason_codes: list[str] | tuple[str, ...],
    output_root: str = '',
) -> tuple[SnapshotRunEnvelopeV1, str]:
    obj = {
        'schema_id': 'snapshot_run_envelope',
        'schema_version': 'v1',
        'record_id': canonical_sha256_hex_v1(
            {
                'snapshot_run_id': snapshot_run_id,
                'household_id': household_id,
                'run_outcome': run_outcome,
                'artifact_refs': artifact_refs,
            }
        ),
        'snapshot_run_id': snapshot_run_id,
        'household_id': household_id,
        'produced_utc': produced_utc,
        'effective_at': effective_at,
        'contract_version': SNAPSHOT_RUN_ENVELOPE_CONTRACT_VERSION_V1,
        'envelope_version': SNAPSHOT_RUN_ENVELOPE_VERSION_V1,
        'run_outcome': run_outcome,
        'artifact_refs': artifact_refs,
        'reason_codes': sorted(set(str(item) for item in reason_codes)),
    }
    envelope = SnapshotRunEnvelopeV1.from_dict(obj)
    path = snapshot_run_envelope_path_v1(output_root, household_id, snapshot_run_id)
    written = write_immutable_json_v1(path, envelope.to_dict())
    return envelope, str(written)
