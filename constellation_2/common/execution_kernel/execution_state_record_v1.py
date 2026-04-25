from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    execution_state_record_dir_v1,
    execution_state_record_path_v1,
    read_json_obj_v1,
    write_immutable_json_v1,
)
from constellation_2.common.execution_kernel.execution_submission_record_v1 import ExecutionSubmissionRecordV1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_state_record.v1.schema.json'
CONTRACT_VERSION = 'execution_state_record_contract_v1'
EVIDENCE_BASES = {'HANDOFF_ATTEMPT', 'BROKER_SUBMISSION_RECORD', 'EXECUTION_EVENT_RECORD', 'FILL_LEDGER'}
TERMINAL_LIFECYCLE_STATUSES = {'FILLED', 'REJECTED', 'CANCELLED', 'INACTIVE'}


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def ref_for_path_v1(path: str | Path) -> dict[str, str]:
    resolved = Path(path).expanduser().resolve()
    return {'path': str(resolved), 'sha256': _sha256_file(resolved)}


@dataclass(frozen=True, slots=True)
class ExecutionStateRecordV1:
    schema_id: str
    schema_version: str
    record_id: str
    execution_state_record_id: str
    submission_record_id: str
    submission_id: str
    execution_intent_id: str
    day_utc: str
    produced_utc: str
    contract_version: str
    current_state: str
    lifecycle_status: str
    source_lifecycle_status: str | None
    terminal_state: bool
    transition_index: int
    previous_execution_state_record_id: str | None
    evidence_basis: str
    evidence_fingerprint: str
    broker_order_id: str | None
    perm_id: str | None
    filled_qty: int
    remaining_qty: int | None
    avg_fill_price: str | None
    broker_submission_ref: dict[str, Any] | None
    execution_event_ref: dict[str, Any] | None
    fill_ledger_ref: dict[str, Any] | None
    reason_codes: tuple[str, ...]
    canonical_json_hash: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ExecutionStateRecordV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['execution_state_record_id']),
            str(obj['submission_record_id']),
            str(obj['submission_id']),
            str(obj['execution_intent_id']),
            str(obj['day_utc']),
            str(obj['produced_utc']),
            str(obj['contract_version']),
            str(obj['current_state']),
            str(obj['lifecycle_status']),
            None if obj.get('source_lifecycle_status') is None else str(obj['source_lifecycle_status']),
            bool(obj['terminal_state']),
            int(obj['transition_index']),
            None if obj.get('previous_execution_state_record_id') is None else str(obj['previous_execution_state_record_id']),
            str(obj['evidence_basis']),
            str(obj['evidence_fingerprint']),
            None if obj.get('broker_order_id') is None else str(obj['broker_order_id']),
            None if obj.get('perm_id') is None else str(obj['perm_id']),
            int(obj['filled_qty']),
            None if obj.get('remaining_qty') is None else int(obj['remaining_qty']),
            None if obj.get('avg_fill_price') is None else str(obj['avg_fill_price']),
            None if obj.get('broker_submission_ref') is None else dict(obj['broker_submission_ref']),
            None if obj.get('execution_event_ref') is None else dict(obj['execution_event_ref']),
            None if obj.get('fill_ledger_ref') is None else dict(obj['fill_ledger_ref']),
            tuple(str(item) for item in obj['reason_codes']),
            str(obj['canonical_json_hash']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'ExecutionStateRecordV1':
        return cls.from_dict(read_json_obj_v1(path))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'execution_state_record_id': self.execution_state_record_id,
            'submission_record_id': self.submission_record_id,
            'submission_id': self.submission_id,
            'execution_intent_id': self.execution_intent_id,
            'day_utc': self.day_utc,
            'produced_utc': self.produced_utc,
            'contract_version': self.contract_version,
            'current_state': self.current_state,
            'lifecycle_status': self.lifecycle_status,
            'source_lifecycle_status': self.source_lifecycle_status,
            'terminal_state': self.terminal_state,
            'transition_index': self.transition_index,
            'previous_execution_state_record_id': self.previous_execution_state_record_id,
            'evidence_basis': self.evidence_basis,
            'evidence_fingerprint': self.evidence_fingerprint,
            'broker_order_id': self.broker_order_id,
            'perm_id': self.perm_id,
            'filled_qty': self.filled_qty,
            'remaining_qty': self.remaining_qty,
            'avg_fill_price': self.avg_fill_price,
            'broker_submission_ref': None if self.broker_submission_ref is None else dict(self.broker_submission_ref),
            'execution_event_ref': None if self.execution_event_ref is None else dict(self.execution_event_ref),
            'fill_ledger_ref': None if self.fill_ledger_ref is None else dict(self.fill_ledger_ref),
            'reason_codes': list(self.reason_codes),
            'canonical_json_hash': self.canonical_json_hash,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj


def load_execution_state_record_by_id_v1(path: str | Path) -> ExecutionStateRecordV1:
    return ExecutionStateRecordV1.load_file(path)


def load_latest_execution_state_record_v1(
    *,
    truth_root: str | Path | None,
    day_utc: str,
    submission_id: str,
) -> ExecutionStateRecordV1 | None:
    state_dir = execution_state_record_dir_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        submission_id=submission_id,
    )
    if not state_dir.exists() or not state_dir.is_dir():
        return None
    records = [
        ExecutionStateRecordV1.load_file(path)
        for path in sorted(state_dir.glob('*.execution_state_record.v1.json'))
    ]
    if not records:
        return None
    max_index = max(record.transition_index for record in records)
    max_records = [record for record in records if record.transition_index == max_index]
    if len(max_records) > 1:
        unique_ids = {record.execution_state_record_id for record in max_records}
        if len(unique_ids) != 1:
            raise ValueError(f'AMBIGUOUS_CURRENT_EXECUTION_STATE:{submission_id}:{max_index}')
    return sorted(
        max_records,
        key=lambda record: (record.produced_utc, record.execution_state_record_id),
    )[-1]


def build_execution_state_record_v1(
    *,
    submission_record: ExecutionSubmissionRecordV1,
    produced_utc: str,
    lifecycle_status: str,
    source_lifecycle_status: str | None,
    terminal_state: bool,
    transition_index: int,
    previous_execution_state_record_id: str | None,
    evidence_basis: str,
    evidence_fingerprint: str,
    broker_order_id: str | None,
    perm_id: str | None,
    filled_qty: int,
    remaining_qty: int | None,
    avg_fill_price: str | None,
    broker_submission_ref: dict[str, Any] | None,
    execution_event_ref: dict[str, Any] | None,
    fill_ledger_ref: dict[str, Any] | None,
    reason_codes: list[str] | tuple[str, ...],
) -> ExecutionStateRecordV1:
    lifecycle_status_clean = str(lifecycle_status).strip().upper()
    if not lifecycle_status_clean:
        raise ValueError('LIFECYCLE_STATUS_REQUIRED')
    evidence_basis_clean = str(evidence_basis).strip().upper()
    if evidence_basis_clean not in EVIDENCE_BASES:
        raise ValueError('EVIDENCE_BASIS_INVALID')
    evidence_fingerprint_clean = str(evidence_fingerprint).strip().lower()
    if len(evidence_fingerprint_clean) != 64:
        raise ValueError('EVIDENCE_FINGERPRINT_INVALID')
    if transition_index < 0:
        raise ValueError('TRANSITION_INDEX_INVALID')

    state_scope = {
        'submission_id': submission_record.submission_id,
        'transition_index': int(transition_index),
        'previous_execution_state_record_id': previous_execution_state_record_id,
        'lifecycle_status': lifecycle_status_clean,
        'source_lifecycle_status': None if source_lifecycle_status is None else str(source_lifecycle_status).strip(),
        'terminal_state': bool(terminal_state),
        'evidence_basis': evidence_basis_clean,
        'evidence_fingerprint': evidence_fingerprint_clean,
        'broker_order_id': None if broker_order_id is None else str(broker_order_id),
        'perm_id': None if perm_id is None else str(perm_id),
        'filled_qty': int(filled_qty),
        'remaining_qty': None if remaining_qty is None else int(remaining_qty),
        'avg_fill_price': None if avg_fill_price is None else str(avg_fill_price),
        'broker_submission_ref': broker_submission_ref,
        'execution_event_ref': execution_event_ref,
        'fill_ledger_ref': fill_ledger_ref,
    }
    execution_state_record_id = canonical_hash_for_c2_artifact_v1(state_scope)
    obj = {
        'schema_id': 'execution_state_record',
        'schema_version': 'v1',
        'record_id': canonical_hash_for_c2_artifact_v1(
            {
                'execution_state_record_id': execution_state_record_id,
                'submission_record_id': submission_record.submission_record_id,
                'produced_utc': str(produced_utc),
            }
        ),
        'execution_state_record_id': execution_state_record_id,
        'submission_record_id': submission_record.submission_record_id,
        'submission_id': submission_record.submission_id,
        'execution_intent_id': submission_record.execution_intent_id,
        'day_utc': submission_record.day_utc,
        'produced_utc': str(produced_utc),
        'contract_version': CONTRACT_VERSION,
        'current_state': 'RECORDED',
        'lifecycle_status': lifecycle_status_clean,
        'source_lifecycle_status': None if source_lifecycle_status is None else str(source_lifecycle_status).strip(),
        'terminal_state': bool(terminal_state),
        'transition_index': int(transition_index),
        'previous_execution_state_record_id': None if previous_execution_state_record_id is None else str(previous_execution_state_record_id),
        'evidence_basis': evidence_basis_clean,
        'evidence_fingerprint': evidence_fingerprint_clean,
        'broker_order_id': None if broker_order_id is None else str(broker_order_id),
        'perm_id': None if perm_id is None else str(perm_id),
        'filled_qty': int(filled_qty),
        'remaining_qty': None if remaining_qty is None else int(remaining_qty),
        'avg_fill_price': None if avg_fill_price is None else str(avg_fill_price),
        'broker_submission_ref': None if broker_submission_ref is None else dict(broker_submission_ref),
        'execution_event_ref': None if execution_event_ref is None else dict(execution_event_ref),
        'fill_ledger_ref': None if fill_ledger_ref is None else dict(fill_ledger_ref),
        'reason_codes': sorted(set(str(item) for item in reason_codes if str(item).strip())),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return ExecutionStateRecordV1.from_dict(obj)


def build_execution_attempt_state_record_v1(
    *,
    submission_record: ExecutionSubmissionRecordV1,
    produced_utc: str,
) -> ExecutionStateRecordV1:
    attempt_fingerprint = canonical_hash_for_c2_artifact_v1(
        {
            'submission_id': submission_record.submission_id,
            'attempt_stage': 'HANDOFF_ENTERED',
        }
    )
    return build_execution_state_record_v1(
        submission_record=submission_record,
        produced_utc=produced_utc,
        lifecycle_status='HANDOFF_ENTERED',
        source_lifecycle_status=None,
        terminal_state=False,
        transition_index=0,
        previous_execution_state_record_id=None,
        evidence_basis='HANDOFF_ATTEMPT',
        evidence_fingerprint=attempt_fingerprint,
        broker_order_id=None,
        perm_id=None,
        filled_qty=0,
        remaining_qty=None,
        avg_fill_price=None,
        broker_submission_ref=None,
        execution_event_ref=None,
        fill_ledger_ref=None,
        reason_codes=['EXECUTION_HANDOFF_ENTERED_PRE_SUBMIT'],
    )


def write_execution_attempt_state_record_v1(
    *,
    submission_record: ExecutionSubmissionRecordV1,
    produced_utc: str,
    truth_root: str | Path | None = None,
) -> tuple[ExecutionStateRecordV1, str]:
    record = build_execution_attempt_state_record_v1(
        submission_record=submission_record,
        produced_utc=produced_utc,
    )
    written = write_execution_state_record_v1(
        record=record,
        truth_root=truth_root,
    )
    return written[0], written[1]


def write_execution_state_record_v1(
    *,
    record: ExecutionStateRecordV1,
    truth_root: str | Path | None = None,
) -> tuple[ExecutionStateRecordV1, str, str]:
    path = execution_state_record_path_v1(
        truth_root=truth_root,
        day_utc=record.day_utc,
        submission_id=record.submission_id,
        execution_state_record_id=record.execution_state_record_id,
    )
    written = write_immutable_json_v1(path, record.to_dict())
    return record, str(written.path), str(written.action)
