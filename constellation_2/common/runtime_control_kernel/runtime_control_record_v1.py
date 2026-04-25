from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.runtime_control_kernel.runtime_control_decision_v1 import RuntimeControlInputsV1
from constellation_2.common.runtime_control_kernel.runtime_control_storage_v1 import (
    read_json_obj_v1,
    runtime_control_record_path_v1,
    write_exclusive_immutable_json_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_record.v1.schema.json'
CONTRACT_VERSION = 'runtime_control_record_contract_v1'


@dataclass(frozen=True, slots=True)
class RuntimeControlRecordV1:
    schema_id: str
    schema_version: str
    record_id: str
    runtime_control_record_id: str
    day_utc: str
    produced_utc: str
    contract_version: str
    capability_scope: str
    environment: str
    ib_account: str
    sleeve_id: str
    control_state: str
    effective_at_utc: str
    kill_switch_state: str
    allow_entries: bool
    readiness_state: str
    readiness_ok: bool
    readiness_as_of_utc: str
    readiness_expires_utc: str
    evidence_fingerprint: str
    source_artifact_refs: tuple[dict[str, str], ...]
    reason_codes: tuple[str, ...]
    canonical_json_hash: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'RuntimeControlRecordV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['runtime_control_record_id']),
            str(obj['day_utc']),
            str(obj['produced_utc']),
            str(obj['contract_version']),
            str(obj['capability_scope']),
            str(obj['environment']),
            str(obj['ib_account']),
            str(obj['sleeve_id']),
            str(obj['control_state']),
            str(obj['effective_at_utc']),
            str(obj['kill_switch_state']),
            bool(obj['allow_entries']),
            str(obj['readiness_state']),
            bool(obj['readiness_ok']),
            str(obj['readiness_as_of_utc']),
            str(obj['readiness_expires_utc']),
            str(obj['evidence_fingerprint']),
            tuple(dict(item) for item in obj['source_artifact_refs']),
            tuple(str(item) for item in obj['reason_codes']),
            str(obj['canonical_json_hash']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'RuntimeControlRecordV1':
        return cls.from_dict(read_json_obj_v1(path))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'runtime_control_record_id': self.runtime_control_record_id,
            'day_utc': self.day_utc,
            'produced_utc': self.produced_utc,
            'contract_version': self.contract_version,
            'capability_scope': self.capability_scope,
            'environment': self.environment,
            'ib_account': self.ib_account,
            'sleeve_id': self.sleeve_id,
            'control_state': self.control_state,
            'effective_at_utc': self.effective_at_utc,
            'kill_switch_state': self.kill_switch_state,
            'allow_entries': self.allow_entries,
            'readiness_state': self.readiness_state,
            'readiness_ok': self.readiness_ok,
            'readiness_as_of_utc': self.readiness_as_of_utc,
            'readiness_expires_utc': self.readiness_expires_utc,
            'evidence_fingerprint': self.evidence_fingerprint,
            'source_artifact_refs': [dict(item) for item in self.source_artifact_refs],
            'reason_codes': list(self.reason_codes),
            'canonical_json_hash': self.canonical_json_hash,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj


def build_runtime_control_record_v1(
    *,
    produced_utc: str,
    inputs: RuntimeControlInputsV1,
) -> RuntimeControlRecordV1:
    if not inputs.valid_for_record or inputs.control_state is None:
        raise ValueError('RUNTIME_CONTROL_INPUTS_NOT_RECORDABLE')

    obj = {
        'schema_id': 'runtime_control_record',
        'schema_version': 'v1',
        'record_id': canonical_hash_for_c2_artifact_v1(
            {
                'capability_scope': inputs.capability_scope,
                'day_utc': inputs.day_utc,
                'environment': inputs.environment,
                'ib_account': inputs.ib_account,
                'sleeve_id': inputs.sleeve_id,
                'control_state': inputs.control_state,
                'evidence_fingerprint': inputs.evidence_fingerprint,
            }
        ),
        'runtime_control_record_id': canonical_hash_for_c2_artifact_v1(
            {
                'capability_scope': inputs.capability_scope,
                'day_utc': inputs.day_utc,
                'environment': inputs.environment,
                'ib_account': inputs.ib_account,
                'sleeve_id': inputs.sleeve_id,
                'control_state': inputs.control_state,
                'evidence_fingerprint': inputs.evidence_fingerprint,
            }
        ),
        'day_utc': inputs.day_utc,
        'produced_utc': str(produced_utc),
        'contract_version': CONTRACT_VERSION,
        'capability_scope': inputs.capability_scope,
        'environment': inputs.environment,
        'ib_account': inputs.ib_account,
        'sleeve_id': inputs.sleeve_id,
        'control_state': inputs.control_state,
        'effective_at_utc': inputs.effective_at_utc,
        'kill_switch_state': str(inputs.kill_switch_state or ''),
        'allow_entries': bool(inputs.allow_entries is True),
        'readiness_state': str(inputs.readiness_state or ''),
        'readiness_ok': bool(inputs.readiness_ok is True),
        'readiness_as_of_utc': str(inputs.readiness_as_of_utc or ''),
        'readiness_expires_utc': str(inputs.readiness_expires_utc or ''),
        'evidence_fingerprint': inputs.evidence_fingerprint,
        'source_artifact_refs': [dict(item) for item in inputs.source_artifact_refs],
        'reason_codes': sorted(set(str(item) for item in inputs.reason_codes if str(item).strip())),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return RuntimeControlRecordV1.from_dict(obj)


def write_runtime_control_record_v1(
    *,
    truth_root: str | Path | None,
    produced_utc: str,
    inputs: RuntimeControlInputsV1,
) -> tuple[RuntimeControlRecordV1, str, str]:
    record = build_runtime_control_record_v1(
        produced_utc=produced_utc,
        inputs=inputs,
    )
    path = runtime_control_record_path_v1(
        truth_root=truth_root,
        day_utc=record.day_utc,
        environment=record.environment,
        ib_account=record.ib_account,
        runtime_control_record_id=record.runtime_control_record_id,
    )
    written = write_exclusive_immutable_json_v1(path, record.to_dict())
    return record, str(written.path), str(written.action)

