from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    execution_lifecycle_run_envelope_path_v1,
    write_immutable_json_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_lifecycle_run_envelope.v1.schema.json'
CONTRACT_VERSION = 'execution_lifecycle_run_envelope_contract_v1'


@dataclass(frozen=True, slots=True)
class ExecutionLifecycleRunEnvelopeV1:
    schema_id: str
    schema_version: str
    record_id: str
    run_id: str
    day_utc: str
    produced_utc: str
    contract_version: str
    run_outcome: str
    artifact_refs: dict[str, Any]
    reason_codes: tuple[str, ...]
    canonical_json_hash: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ExecutionLifecycleRunEnvelopeV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['run_id']),
            str(obj['day_utc']),
            str(obj['produced_utc']),
            str(obj['contract_version']),
            str(obj['run_outcome']),
            dict(obj['artifact_refs']),
            tuple(str(item) for item in obj['reason_codes']),
            str(obj['canonical_json_hash']),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'run_id': self.run_id,
            'day_utc': self.day_utc,
            'produced_utc': self.produced_utc,
            'contract_version': self.contract_version,
            'run_outcome': self.run_outcome,
            'artifact_refs': dict(self.artifact_refs),
            'reason_codes': list(self.reason_codes),
            'canonical_json_hash': self.canonical_json_hash,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj


def emit_execution_lifecycle_run_envelope_v1(
    *,
    truth_root: str | Path | None,
    run_id: str,
    day_utc: str,
    produced_utc: str,
    run_outcome: str,
    artifact_refs: dict[str, Any],
    reason_codes: list[str] | tuple[str, ...],
) -> tuple[ExecutionLifecycleRunEnvelopeV1, str]:
    obj = {
        'schema_id': 'execution_lifecycle_run_envelope',
        'schema_version': 'v1',
        'record_id': canonical_hash_for_c2_artifact_v1(
            {
                'run_id': run_id,
                'run_outcome': run_outcome,
                'artifact_refs': artifact_refs,
            }
        ),
        'run_id': str(run_id),
        'day_utc': str(day_utc),
        'produced_utc': str(produced_utc),
        'contract_version': CONTRACT_VERSION,
        'run_outcome': str(run_outcome),
        'artifact_refs': dict(artifact_refs),
        'reason_codes': sorted(set(str(item) for item in reason_codes if str(item).strip())),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    envelope = ExecutionLifecycleRunEnvelopeV1.from_dict(obj)
    path = execution_lifecycle_run_envelope_path_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        run_id=run_id,
    )
    written = write_immutable_json_v1(path, envelope.to_dict())
    return envelope, str(written.path)
