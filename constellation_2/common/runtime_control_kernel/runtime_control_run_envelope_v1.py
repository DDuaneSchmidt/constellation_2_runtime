from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.runtime_control_kernel.runtime_control_storage_v1 import (
    read_json_obj_v1,
    runtime_control_run_envelope_path_v1,
    write_immutable_json_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_run_envelope.v1.schema.json'
CONTRACT_VERSION = 'runtime_control_run_envelope_contract_v1'


def assert_runtime_control_run_envelope_writable_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    run_id: str,
) -> Path:
    path = runtime_control_run_envelope_path_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        run_id=run_id,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise ValueError(f'RUNTIME_CONTROL_RUN_ENVELOPE_ALREADY_EXISTS:{path}')
    fd, tmp_path = tempfile.mkstemp(prefix=f'.{path.name}.probe.', dir=str(path.parent))
    try:
        os.write(fd, b'probe')
        os.fsync(fd)
    finally:
        os.close(fd)
        os.unlink(tmp_path)
    return path


@dataclass(frozen=True, slots=True)
class RuntimeControlRunEnvelopeV1:
    schema_id: str
    schema_version: str
    record_id: str
    run_id: str
    day_utc: str
    produced_utc: str
    contract_version: str
    capability_scope: str
    environment: str
    ib_account: str
    sleeve_id: str
    run_outcome: str
    stage_status: dict[str, Any]
    artifact_refs: dict[str, Any]
    reason_codes: tuple[str, ...]
    canonical_json_hash: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'RuntimeControlRunEnvelopeV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['run_id']),
            str(obj['day_utc']),
            str(obj['produced_utc']),
            str(obj['contract_version']),
            str(obj['capability_scope']),
            str(obj['environment']),
            str(obj['ib_account']),
            str(obj['sleeve_id']),
            str(obj['run_outcome']),
            dict(obj['stage_status']),
            dict(obj['artifact_refs']),
            tuple(str(item) for item in obj['reason_codes']),
            str(obj['canonical_json_hash']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'RuntimeControlRunEnvelopeV1':
        return cls.from_dict(read_json_obj_v1(path))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'run_id': self.run_id,
            'day_utc': self.day_utc,
            'produced_utc': self.produced_utc,
            'contract_version': self.contract_version,
            'capability_scope': self.capability_scope,
            'environment': self.environment,
            'ib_account': self.ib_account,
            'sleeve_id': self.sleeve_id,
            'run_outcome': self.run_outcome,
            'stage_status': dict(self.stage_status),
            'artifact_refs': dict(self.artifact_refs),
            'reason_codes': list(self.reason_codes),
            'canonical_json_hash': self.canonical_json_hash,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj


def emit_runtime_control_run_envelope_v1(
    *,
    truth_root: str | Path | None,
    run_id: str,
    day_utc: str,
    produced_utc: str,
    capability_scope: str,
    environment: str,
    ib_account: str,
    sleeve_id: str,
    run_outcome: str,
    stage_status: dict[str, Any],
    artifact_refs: dict[str, Any],
    reason_codes: list[str] | tuple[str, ...],
) -> tuple[RuntimeControlRunEnvelopeV1, str]:
    obj = {
        'schema_id': 'runtime_control_run_envelope',
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
        'capability_scope': str(capability_scope),
        'environment': str(environment),
        'ib_account': str(ib_account),
        'sleeve_id': str(sleeve_id),
        'run_outcome': str(run_outcome),
        'stage_status': dict(stage_status),
        'artifact_refs': dict(artifact_refs),
        'reason_codes': sorted(set(str(item) for item in reason_codes if str(item).strip())),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    envelope = RuntimeControlRunEnvelopeV1.from_dict(obj)
    path = runtime_control_run_envelope_path_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        run_id=run_id,
    )
    written = write_immutable_json_v1(path, envelope.to_dict())
    return envelope, str(written.path)
