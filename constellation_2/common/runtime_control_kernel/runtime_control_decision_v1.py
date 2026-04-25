from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.kill_switch_authority_v1 import (
    STATUS_PASS as KILL_SWITCH_STATUS_PASS,
    resolve_kill_switch_authority_v1,
)
from constellation_2.common.runtime_control_kernel.runtime_control_storage_v1 import (
    read_json_obj_v1,
    runtime_control_decision_path_v1,
    runtime_control_record_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    validate_trade_submit_readiness_status_obj,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_decision.v1.schema.json'
CONTRACT_VERSION = 'runtime_control_decision_contract_v1'
CAPABILITY_SCOPE_PAPER_TRADE_SUBMIT_ENTRY_V1 = 'paper_trade_submit_entry_v1'

RC_RUNTIME_CONTROL_KILL_SWITCH_CONTRADICTORY = 'RUNTIME_CONTROL_KILL_SWITCH_CONTRADICTORY'
RC_RUNTIME_CONTROL_READINESS_CONTRADICTORY = 'RUNTIME_CONTROL_READINESS_CONTRADICTORY'
RC_RUNTIME_CONTROL_KILL_SWITCH_AUTHORITY_UNAVAILABLE = 'RUNTIME_CONTROL_KILL_SWITCH_AUTHORITY_UNAVAILABLE'
RC_RUNTIME_CONTROL_READINESS_UNAVAILABLE = 'RUNTIME_CONTROL_READINESS_UNAVAILABLE'
RC_RUNTIME_CONTROL_KILL_SWITCH_ACTIVE = 'RUNTIME_CONTROL_KILL_SWITCH_ACTIVE'
RC_RUNTIME_CONTROL_READINESS_NOT_OK = 'RUNTIME_CONTROL_READINESS_NOT_OK'


def _ref_from_path(*, path: str | Path, sha256: str) -> dict[str, str]:
    return {
        'path': str(Path(path).expanduser().resolve()),
        'sha256': str(sha256),
    }


def _read_json_obj(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT:{path}')
    return obj


def _sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _read_trade_submit_readiness_status_v1(
    *,
    execution_truth_root: Path,
    environment: str,
    ib_account: str,
    day_utc: str,
) -> tuple[dict[str, Any], Path]:
    env = str(environment or '').strip().upper()
    account = str(ib_account or '').strip()
    status_path = (
        execution_truth_root
        / 'trade_submit_readiness_c2_v1'
        / env
        / account
        / 'status.json'
    ).resolve()
    payload = _read_json_obj(status_path)
    if str(payload.get('schema_id') or '').strip() != 'trade_submit_readiness_c2':
        raise ValueError(f'TRADE_SUBMIT_READINESS_SCHEMA_ID_INVALID:path={status_path}')
    if str(payload.get('schema_version') or '').strip() != 'v1':
        raise ValueError(f'TRADE_SUBMIT_READINESS_SCHEMA_VERSION_INVALID:path={status_path}')
    if str(payload.get('environment') or '').strip().upper() != env:
        raise ValueError(f'TRADE_SUBMIT_READINESS_ENVIRONMENT_MISMATCH:path={status_path}')
    if str(payload.get('ib_account') or '').strip() != account:
        raise ValueError(f'TRADE_SUBMIT_READINESS_ACCOUNT_MISMATCH:path={status_path}')
    provenance = payload.get('provenance')
    truth_root_value = ''
    if isinstance(provenance, dict):
        truth_root_value = str(provenance.get('truth_root') or '').strip()
    if truth_root_value != str(execution_truth_root.resolve()):
        raise ValueError(
            f'TRADE_SUBMIT_READINESS_NONAUTHORITATIVE:provenance_truth_root={truth_root_value!r}:expected={str(execution_truth_root.resolve())!r}:path={status_path}'
        )
    try:
        validate_trade_submit_readiness_status_obj(payload)
    except Exception:
        # The active submit boundary historically enforced only the minimal current-alias contract.
        # Preserve that behavior for the control kernel rather than broadening readiness requirements.
        pass
    return payload, status_path


@dataclass(frozen=True, slots=True)
class RuntimeControlInputsV1:
    capability_scope: str
    day_utc: str
    environment: str
    ib_account: str
    sleeve_id: str
    effective_at_utc: str
    control_state: str | None
    source_artifact_refs: tuple[dict[str, str], ...]
    source_artifact_ref_strings: tuple[str, ...]
    reason_codes: tuple[str, ...]
    kill_switch_state: str | None
    allow_entries: bool | None
    readiness_state: str | None
    readiness_ok: bool | None
    readiness_as_of_utc: str | None
    readiness_expires_utc: str | None
    evidence_fingerprint: str
    valid_for_record: bool


@dataclass(frozen=True, slots=True)
class RuntimeControlDecisionV1:
    schema_id: str
    schema_version: str
    record_id: str
    runtime_control_decision_id: str
    day_utc: str
    produced_utc: str
    contract_version: str
    capability_scope: str
    environment: str
    ib_account: str
    sleeve_id: str
    outcome: str
    candidate_control_state: str | None
    effective_at_utc: str
    source_artifact_refs: tuple[str, ...]
    existing_record_ref: dict[str, Any] | None
    reason_codes: tuple[str, ...]
    evidence_fingerprint: str
    canonical_json_hash: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'RuntimeControlDecisionV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        existing_record_ref = obj.get('existing_record_ref')
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['runtime_control_decision_id']),
            str(obj['day_utc']),
            str(obj['produced_utc']),
            str(obj['contract_version']),
            str(obj['capability_scope']),
            str(obj['environment']),
            str(obj['ib_account']),
            str(obj['sleeve_id']),
            str(obj['outcome']),
            None if obj.get('candidate_control_state') is None else str(obj['candidate_control_state']),
            str(obj['effective_at_utc']),
            tuple(str(item) for item in obj['source_artifact_refs']),
            None if existing_record_ref is None else dict(existing_record_ref),
            tuple(str(item) for item in obj['reason_codes']),
            str(obj['evidence_fingerprint']),
            str(obj['canonical_json_hash']),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'runtime_control_decision_id': self.runtime_control_decision_id,
            'day_utc': self.day_utc,
            'produced_utc': self.produced_utc,
            'contract_version': self.contract_version,
            'capability_scope': self.capability_scope,
            'environment': self.environment,
            'ib_account': self.ib_account,
            'sleeve_id': self.sleeve_id,
            'outcome': self.outcome,
            'candidate_control_state': self.candidate_control_state,
            'effective_at_utc': self.effective_at_utc,
            'source_artifact_refs': list(self.source_artifact_refs),
            'existing_record_ref': None if self.existing_record_ref is None else dict(self.existing_record_ref),
            'reason_codes': list(self.reason_codes),
            'evidence_fingerprint': self.evidence_fingerprint,
            'canonical_json_hash': self.canonical_json_hash,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj


def assemble_runtime_control_inputs_v1(
    *,
    canonical_truth_root: str | Path,
    execution_truth_root: str | Path,
    day_utc: str,
    environment: str,
    ib_account: str,
    sleeve_id: str = 'PRIMARY',
    capability_scope: str = CAPABILITY_SCOPE_PAPER_TRADE_SUBMIT_ENTRY_V1,
) -> RuntimeControlInputsV1:
    canonical_truth_root = Path(canonical_truth_root).expanduser().resolve()
    execution_truth_root = Path(execution_truth_root).expanduser().resolve()
    env = str(environment or '').strip().upper()
    account = str(ib_account or '').strip()
    sleeve = str(sleeve_id or '').strip().upper()

    try:
        kill_result = resolve_kill_switch_authority_v1(
            canonical_truth_root=canonical_truth_root,
            day_utc=str(day_utc),
        )
    except Exception as exc:
        return RuntimeControlInputsV1(
            capability_scope=capability_scope,
            day_utc=str(day_utc),
            environment=env,
            ib_account=account,
            sleeve_id=sleeve,
            effective_at_utc=f'{day_utc}T00:00:00Z',
            control_state=None,
            source_artifact_refs=(),
            source_artifact_ref_strings=(),
            reason_codes=(f'{RC_RUNTIME_CONTROL_KILL_SWITCH_AUTHORITY_UNAVAILABLE}:{type(exc).__name__}',),
            kill_switch_state=None,
            allow_entries=None,
            readiness_state=None,
            readiness_ok=None,
            readiness_as_of_utc=None,
            readiness_expires_utc=None,
            evidence_fingerprint=canonical_hash_for_c2_artifact_v1({'error': f'kill_switch:{type(exc).__name__}', 'day_utc': day_utc, 'environment': env, 'ib_account': account}),
            valid_for_record=False,
        )

    if kill_result.status != KILL_SWITCH_STATUS_PASS:
        kill_codes = tuple(str(code).strip() for code in kill_result.reason_codes if str(code).strip())
        reason_codes = kill_codes or (RC_RUNTIME_CONTROL_KILL_SWITCH_AUTHORITY_UNAVAILABLE,)
        return RuntimeControlInputsV1(
            capability_scope=capability_scope,
            day_utc=str(day_utc),
            environment=env,
            ib_account=account,
            sleeve_id=sleeve,
            effective_at_utc=f'{day_utc}T00:00:00Z',
            control_state=None,
            source_artifact_refs=(
                _ref_from_path(path=kill_result.canonical_path, sha256=kill_result.canonical_sha256),
            ),
            source_artifact_ref_strings=(f'kill_switch:{kill_result.canonical_path}',),
            reason_codes=reason_codes,
            kill_switch_state=kill_result.state,
            allow_entries=kill_result.allow_entries,
            readiness_state=None,
            readiness_ok=None,
            readiness_as_of_utc=None,
            readiness_expires_utc=None,
            evidence_fingerprint=canonical_hash_for_c2_artifact_v1({'kill_switch_sha256': kill_result.canonical_sha256, 'day_utc': day_utc, 'environment': env, 'ib_account': account}),
            valid_for_record=False,
        )

    kill_switch_ref = _ref_from_path(path=kill_result.canonical_path, sha256=kill_result.canonical_sha256)
    kill_switch_payload = kill_result.payload if isinstance(kill_result.payload, dict) else {}
    kill_switch_effective_at = str(kill_switch_payload.get('produced_utc') or f'{day_utc}T00:00:00Z')

    try:
        readiness_payload, readiness_path = _read_trade_submit_readiness_status_v1(
            execution_truth_root=execution_truth_root,
            environment=env,
            ib_account=account,
            day_utc=str(day_utc),
        )
    except Exception as exc:
        return RuntimeControlInputsV1(
            capability_scope=capability_scope,
            day_utc=str(day_utc),
            environment=env,
            ib_account=account,
            sleeve_id=sleeve,
            effective_at_utc=kill_switch_effective_at,
            control_state=None,
            source_artifact_refs=(kill_switch_ref,),
            source_artifact_ref_strings=(f'kill_switch:{kill_result.canonical_path}',),
            reason_codes=(f'{RC_RUNTIME_CONTROL_READINESS_UNAVAILABLE}:{type(exc).__name__}',),
            kill_switch_state=kill_result.state,
            allow_entries=kill_result.allow_entries,
            readiness_state=None,
            readiness_ok=None,
            readiness_as_of_utc=None,
            readiness_expires_utc=None,
            evidence_fingerprint=canonical_hash_for_c2_artifact_v1({'kill_switch_sha256': kill_result.canonical_sha256, 'readiness_error': type(exc).__name__, 'day_utc': day_utc, 'environment': env, 'ib_account': account}),
            valid_for_record=False,
        )

    readiness_ref = _ref_from_path(path=readiness_path, sha256=_sha256_file(readiness_path))
    readiness_ok = bool(readiness_payload.get('ok') is True)
    readiness_state = str(readiness_payload.get('state') or '').strip().upper()
    readiness_as_of_utc = str(readiness_payload.get('as_of_utc') or f'{day_utc}T00:00:00Z')
    readiness_expires_utc = str(readiness_payload.get('expires_utc') or '')

    if (kill_result.state == 'ACTIVE' and kill_result.allow_entries is True) or (
        kill_result.state == 'INACTIVE' and kill_result.allow_entries is not True
    ):
        reason_codes = (RC_RUNTIME_CONTROL_KILL_SWITCH_CONTRADICTORY,)
        control_state = None
        valid_for_record = False
    elif (readiness_ok and readiness_state != 'OK') or ((not readiness_ok) and readiness_state == 'OK'):
        reason_codes = (RC_RUNTIME_CONTROL_READINESS_CONTRADICTORY,)
        control_state = None
        valid_for_record = False
    elif kill_result.state != 'INACTIVE' or kill_result.allow_entries is not True:
        reason_codes = tuple(
            sorted(
                set(
                    [RC_RUNTIME_CONTROL_KILL_SWITCH_ACTIVE]
                    + [str(code).strip() for code in kill_result.reason_codes if str(code).strip()]
                )
            )
        )
        control_state = 'BLOCKED'
        valid_for_record = True
    elif not readiness_ok or readiness_state != 'OK':
        readiness_codes = [
            str(code).strip()
            for code in (readiness_payload.get('reasons') or [])
            if str(code).strip()
        ]
        reason_codes = tuple(sorted(set([RC_RUNTIME_CONTROL_READINESS_NOT_OK] + readiness_codes)))
        control_state = 'BLOCKED'
        valid_for_record = True
    else:
        reason_codes = ()
        control_state = 'ALLOW'
        valid_for_record = True

    effective_at_utc = max(kill_switch_effective_at, readiness_as_of_utc)
    source_artifact_refs = (kill_switch_ref, readiness_ref)
    source_artifact_ref_strings = tuple(
        sorted(
            [
                f'kill_switch:{kill_result.canonical_path}',
                f'trade_submit_readiness:{readiness_path}',
            ]
        )
    )
    evidence_fingerprint = canonical_hash_for_c2_artifact_v1(
        {
            'capability_scope': capability_scope,
            'day_utc': day_utc,
            'environment': env,
            'ib_account': account,
            'sleeve_id': sleeve,
            'kill_switch_sha256': kill_result.canonical_sha256,
            'readiness_sha256': readiness_ref['sha256'],
            'control_state': control_state,
            'reason_codes': list(reason_codes),
        }
    )
    return RuntimeControlInputsV1(
        capability_scope=capability_scope,
        day_utc=str(day_utc),
        environment=env,
        ib_account=account,
        sleeve_id=sleeve,
        effective_at_utc=effective_at_utc,
        control_state=control_state,
        source_artifact_refs=source_artifact_refs,
        source_artifact_ref_strings=source_artifact_ref_strings,
        reason_codes=tuple(reason_codes),
        kill_switch_state=kill_result.state,
        allow_entries=kill_result.allow_entries,
        readiness_state=readiness_state,
        readiness_ok=readiness_ok,
        readiness_as_of_utc=readiness_as_of_utc,
        readiness_expires_utc=readiness_expires_utc,
        evidence_fingerprint=evidence_fingerprint,
        valid_for_record=valid_for_record,
    )


def build_runtime_control_decision_v1(
    *,
    truth_root: str | Path | None,
    produced_utc: str,
    inputs: RuntimeControlInputsV1,
) -> RuntimeControlDecisionV1:
    existing_record_ref = None
    outcome = 'blocked'
    candidate_control_state = inputs.control_state
    if inputs.valid_for_record and inputs.control_state is not None:
        candidate_record_id = canonical_hash_for_c2_artifact_v1(
            {
                'capability_scope': inputs.capability_scope,
                'day_utc': inputs.day_utc,
                'environment': inputs.environment,
                'ib_account': inputs.ib_account,
                'sleeve_id': inputs.sleeve_id,
                'control_state': inputs.control_state,
                'evidence_fingerprint': inputs.evidence_fingerprint,
            }
        )
        candidate_path = runtime_control_record_path_v1(
            truth_root=truth_root,
            day_utc=inputs.day_utc,
            environment=inputs.environment,
            ib_account=inputs.ib_account,
            runtime_control_record_id=candidate_record_id,
        )
        if candidate_path.exists() and candidate_path.is_file():
            existing_record_obj = read_json_obj_v1(candidate_path)
            existing_record_ref = {
                'path': str(candidate_path),
                'sha256': str(existing_record_obj.get('canonical_json_hash') or ''),
                'runtime_control_record_id': candidate_record_id,
            }
            outcome = 'duplicate'
        elif inputs.control_state == 'ALLOW':
            outcome = 'allow'
        else:
            outcome = 'blocked'
    else:
        outcome = 'blocked'
        candidate_control_state = None

    obj = {
        'schema_id': 'runtime_control_decision',
        'schema_version': 'v1',
        'record_id': canonical_hash_for_c2_artifact_v1(
            {
                'capability_scope': inputs.capability_scope,
                'day_utc': inputs.day_utc,
                'environment': inputs.environment,
                'ib_account': inputs.ib_account,
                'sleeve_id': inputs.sleeve_id,
                'evidence_fingerprint': inputs.evidence_fingerprint,
                'outcome': outcome,
                'produced_utc': str(produced_utc),
            }
        ),
        'runtime_control_decision_id': canonical_hash_for_c2_artifact_v1(
            {
                'capability_scope': inputs.capability_scope,
                'day_utc': inputs.day_utc,
                'environment': inputs.environment,
                'ib_account': inputs.ib_account,
                'sleeve_id': inputs.sleeve_id,
                'evidence_fingerprint': inputs.evidence_fingerprint,
                'outcome': outcome,
                'produced_utc': str(produced_utc),
            }
        ),
        'day_utc': inputs.day_utc,
        'produced_utc': str(produced_utc),
        'contract_version': CONTRACT_VERSION,
        'capability_scope': inputs.capability_scope,
        'environment': inputs.environment,
        'ib_account': inputs.ib_account,
        'sleeve_id': inputs.sleeve_id,
        'outcome': outcome,
        'candidate_control_state': candidate_control_state,
        'effective_at_utc': inputs.effective_at_utc,
        'source_artifact_refs': list(inputs.source_artifact_ref_strings),
        'existing_record_ref': existing_record_ref,
        'reason_codes': list(inputs.reason_codes),
        'evidence_fingerprint': inputs.evidence_fingerprint,
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return RuntimeControlDecisionV1.from_dict(obj)


def write_runtime_control_decision_v1(
    *,
    truth_root: str | Path | None,
    produced_utc: str,
    inputs: RuntimeControlInputsV1,
) -> tuple[RuntimeControlDecisionV1, str]:
    decision = build_runtime_control_decision_v1(
        truth_root=truth_root,
        produced_utc=produced_utc,
        inputs=inputs,
    )
    path = runtime_control_decision_path_v1(
        truth_root=truth_root,
        day_utc=decision.day_utc,
        runtime_control_decision_id=decision.runtime_control_decision_id,
    )
    written = write_immutable_json_v1(path, decision.to_dict())
    return decision, str(written.path)
