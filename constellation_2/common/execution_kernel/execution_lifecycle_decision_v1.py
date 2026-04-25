from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    execution_lifecycle_decision_path_v1,
    fill_ledger_path_for_truth_root_v1,
    read_json_obj_v1,
    submission_evidence_dir_for_truth_root_v1,
    write_immutable_json_v1,
)
from constellation_2.common.execution_kernel.execution_state_record_v1 import (
    TERMINAL_LIFECYCLE_STATUSES,
    ExecutionStateRecordV1,
    load_latest_execution_state_record_v1,
    ref_for_path_v1,
)
from constellation_2.common.execution_kernel.execution_submission_record_v1 import ExecutionSubmissionRecordV1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_lifecycle_decision.v1.schema.json'
CONTRACT_VERSION = 'execution_lifecycle_decision_contract_v1'
OUTCOMES = {'advance', 'duplicate', 'blocked', 'no_action'}
EVIDENCE_BASIS_RANK = {
    'HANDOFF_ATTEMPT': 0,
    'BROKER_SUBMISSION_RECORD': 1,
    'EXECUTION_EVENT_RECORD': 2,
    'FILL_LEDGER': 3,
}
STATUS_RANK = {
    'HANDOFF_ENTERED': 0,
    'UNKNOWN': 1,
    'PENDINGSUBMIT': 2,
    'PRESUBMITTED': 2,
    'SUBMITTED': 2,
    'ACKNOWLEDGED': 3,
    'OPEN': 3,
    'PARTIALLY_FILLED': 4,
    'FILLED': 5,
    'REJECTED': 6,
    'CANCELLED': 6,
    'INACTIVE': 6,
}


def _status_rank(status: str) -> int:
    return STATUS_RANK.get(str(status).strip().upper(), -1)


def _read_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return read_json_obj_v1(path)


def _normalize_candidate_state(
    *,
    broker_submission: dict[str, Any],
    execution_event: dict[str, Any] | None,
    fill_ledger: dict[str, Any] | None,
) -> dict[str, Any]:
    broker_ids = broker_submission.get('broker_ids') if isinstance(broker_submission.get('broker_ids'), dict) else {}
    broker_order_id = None if broker_ids.get('order_id') is None else str(broker_ids.get('order_id'))
    perm_id = None if broker_ids.get('perm_id') is None else str(broker_ids.get('perm_id'))

    if isinstance(fill_ledger, dict):
        lifecycle_status = str(fill_ledger.get('lifecycle_status') or 'UNKNOWN').strip().upper()
        source_lifecycle_status = lifecycle_status
        filled_qty = int(fill_ledger.get('filled_qty') or 0)
        remaining_qty = int(fill_ledger.get('remaining_qty') or 0)
        avg_fill_price = str(fill_ledger.get('avg_fill_price_weighted') or '0')
        evidence_basis = 'FILL_LEDGER'
    elif isinstance(execution_event, dict):
        lifecycle_status = str(execution_event.get('status') or 'UNKNOWN').strip().upper()
        source_lifecycle_status = str(execution_event.get('raw_broker_status') or lifecycle_status).strip().upper() or None
        filled_qty = int(execution_event.get('filled_qty') or 0)
        remaining_qty = None
        avg_fill_price = str(execution_event.get('avg_price') or '0')
        event_order_id = None if execution_event.get('broker_order_id') is None else str(execution_event.get('broker_order_id'))
        event_perm_id = None if execution_event.get('perm_id') is None else str(execution_event.get('perm_id'))
        if broker_order_id is not None and event_order_id is not None and broker_order_id != event_order_id:
            raise ValueError('EXECUTION_EVENT_BROKER_ORDER_ID_MISMATCH')
        if perm_id is not None and event_perm_id is not None and perm_id != event_perm_id:
            raise ValueError('EXECUTION_EVENT_PERM_ID_MISMATCH')
        broker_order_id = event_order_id or broker_order_id
        perm_id = event_perm_id or perm_id
        evidence_basis = 'EXECUTION_EVENT_RECORD'
    else:
        lifecycle_status = str(broker_submission.get('status') or 'UNKNOWN').strip().upper()
        source_lifecycle_status = lifecycle_status
        filled_qty = 0
        remaining_qty = None
        avg_fill_price = None
        evidence_basis = 'BROKER_SUBMISSION_RECORD'

    terminal_state = lifecycle_status in TERMINAL_LIFECYCLE_STATUSES
    return {
        'lifecycle_status': lifecycle_status,
        'source_lifecycle_status': source_lifecycle_status,
        'terminal_state': terminal_state,
        'filled_qty': filled_qty,
        'remaining_qty': remaining_qty,
        'avg_fill_price': avg_fill_price,
        'evidence_basis': evidence_basis,
        'broker_order_id': broker_order_id,
        'perm_id': perm_id,
    }


def _effective_state_tuple(candidate: dict[str, Any]) -> tuple[Any, ...]:
    return (
        candidate['lifecycle_status'],
        int(candidate['filled_qty']),
        candidate['remaining_qty'],
        candidate['avg_fill_price'],
        candidate['broker_order_id'],
        candidate['perm_id'],
        bool(candidate['terminal_state']),
    )


def _evidence_fingerprint_v1(
    *,
    broker_submission_ref: dict[str, Any] | None,
    execution_event_ref: dict[str, Any] | None,
    fill_ledger_ref: dict[str, Any] | None,
    blocked_reason_codes: list[str] | tuple[str, ...] | None = None,
) -> str:
    return canonical_hash_for_c2_artifact_v1(
        {
            'broker_submission_sha256': None if broker_submission_ref is None else broker_submission_ref.get('sha256'),
            'execution_event_sha256': None if execution_event_ref is None else execution_event_ref.get('sha256'),
            'fill_ledger_sha256': None if fill_ledger_ref is None else fill_ledger_ref.get('sha256'),
            'blocked_reason_codes': None
            if blocked_reason_codes is None
            else sorted(set(str(item) for item in blocked_reason_codes if str(item).strip())),
        }
    )


def _is_legal_transition(
    *,
    current: ExecutionStateRecordV1,
    candidate: dict[str, Any],
) -> tuple[bool, str | None]:
    if current.submission_id == '' or current.execution_intent_id == '':
        return False, 'CURRENT_EXECUTION_STATE_INVALID'

    candidate_status_rank = _status_rank(candidate['lifecycle_status'])
    current_status_rank = _status_rank(current.lifecycle_status)
    if candidate_status_rank < 0:
        return False, 'CANDIDATE_LIFECYCLE_STATUS_UNSUPPORTED'
    if current_status_rank < 0:
        return False, 'CURRENT_LIFECYCLE_STATUS_UNSUPPORTED'

    if current.broker_order_id and candidate['broker_order_id'] and current.broker_order_id != candidate['broker_order_id']:
        return False, 'BROKER_ORDER_ID_CONTRADICTION'
    if current.perm_id and candidate['perm_id'] and current.perm_id != candidate['perm_id']:
        return False, 'PERM_ID_CONTRADICTION'
    if int(candidate['filled_qty']) < int(current.filled_qty):
        return False, 'FILLED_QUANTITY_REGRESSION'
    if int(current.filled_qty) > 0 and int(candidate['filled_qty']) == int(current.filled_qty):
        current_avg = None if current.avg_fill_price is None else str(current.avg_fill_price)
        candidate_avg = None if candidate['avg_fill_price'] is None else str(candidate['avg_fill_price'])
        if current_avg != candidate_avg:
            return False, 'AVERAGE_FILL_PRICE_CONTRADICTION'
    if candidate_status_rank < current_status_rank:
        same_effective_state = _effective_state_tuple(candidate) == _effective_state_tuple(
            {
                'lifecycle_status': current.lifecycle_status,
                'filled_qty': current.filled_qty,
                'remaining_qty': current.remaining_qty,
                'avg_fill_price': current.avg_fill_price,
                'broker_order_id': current.broker_order_id,
                'perm_id': current.perm_id,
                'terminal_state': current.terminal_state,
            }
        )
        if not same_effective_state:
            return False, 'ILLEGAL_LIFECYCLE_REGRESSION'
    if current.terminal_state:
        same_effective_state = _effective_state_tuple(candidate) == _effective_state_tuple(
            {
                'lifecycle_status': current.lifecycle_status,
                'filled_qty': current.filled_qty,
                'remaining_qty': current.remaining_qty,
                'avg_fill_price': current.avg_fill_price,
                'broker_order_id': current.broker_order_id,
                'perm_id': current.perm_id,
                'terminal_state': current.terminal_state,
            }
        )
        if not same_effective_state:
            return False, 'TERMINAL_STATE_REENTRY_BLOCKED'
    return True, None


@dataclass(frozen=True, slots=True)
class ExecutionLifecycleDecisionV1:
    schema_id: str
    schema_version: str
    record_id: str
    execution_lifecycle_decision_id: str
    lifecycle_input_id: str
    submission_record_id: str
    submission_id: str
    execution_intent_id: str
    day_utc: str
    produced_utc: str
    contract_version: str
    outcome: str
    current_execution_state_record_id: str | None
    candidate_lifecycle_status: str
    candidate_source_lifecycle_status: str | None
    candidate_terminal_state: bool
    evidence_basis: str
    evidence_fingerprint: str
    next_transition_index: int
    candidate_broker_order_id: str | None
    candidate_perm_id: str | None
    candidate_filled_qty: int
    candidate_remaining_qty: int | None
    candidate_avg_fill_price: str | None
    broker_submission_ref: dict[str, Any] | None
    execution_event_ref: dict[str, Any] | None
    fill_ledger_ref: dict[str, Any] | None
    input_record_refs: tuple[str, ...]
    reason_codes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ExecutionLifecycleDecisionV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['execution_lifecycle_decision_id']),
            str(obj['lifecycle_input_id']),
            str(obj['submission_record_id']),
            str(obj['submission_id']),
            str(obj['execution_intent_id']),
            str(obj['day_utc']),
            str(obj['produced_utc']),
            str(obj['contract_version']),
            str(obj['outcome']),
            None if obj.get('current_execution_state_record_id') is None else str(obj['current_execution_state_record_id']),
            str(obj['candidate_lifecycle_status']),
            None if obj.get('candidate_source_lifecycle_status') is None else str(obj['candidate_source_lifecycle_status']),
            bool(obj['candidate_terminal_state']),
            str(obj['evidence_basis']),
            str(obj['evidence_fingerprint']),
            int(obj['next_transition_index']),
            None if obj.get('candidate_broker_order_id') is None else str(obj['candidate_broker_order_id']),
            None if obj.get('candidate_perm_id') is None else str(obj['candidate_perm_id']),
            int(obj['candidate_filled_qty']),
            None if obj.get('candidate_remaining_qty') is None else int(obj['candidate_remaining_qty']),
            None if obj.get('candidate_avg_fill_price') is None else str(obj['candidate_avg_fill_price']),
            None if obj.get('broker_submission_ref') is None else dict(obj['broker_submission_ref']),
            None if obj.get('execution_event_ref') is None else dict(obj['execution_event_ref']),
            None if obj.get('fill_ledger_ref') is None else dict(obj['fill_ledger_ref']),
            tuple(str(item) for item in obj['input_record_refs']),
            tuple(str(item) for item in obj['reason_codes']),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'execution_lifecycle_decision_id': self.execution_lifecycle_decision_id,
            'lifecycle_input_id': self.lifecycle_input_id,
            'submission_record_id': self.submission_record_id,
            'submission_id': self.submission_id,
            'execution_intent_id': self.execution_intent_id,
            'day_utc': self.day_utc,
            'produced_utc': self.produced_utc,
            'contract_version': self.contract_version,
            'outcome': self.outcome,
            'current_execution_state_record_id': self.current_execution_state_record_id,
            'candidate_lifecycle_status': self.candidate_lifecycle_status,
            'candidate_source_lifecycle_status': self.candidate_source_lifecycle_status,
            'candidate_terminal_state': self.candidate_terminal_state,
            'evidence_basis': self.evidence_basis,
            'evidence_fingerprint': self.evidence_fingerprint,
            'next_transition_index': self.next_transition_index,
            'candidate_broker_order_id': self.candidate_broker_order_id,
            'candidate_perm_id': self.candidate_perm_id,
            'candidate_filled_qty': self.candidate_filled_qty,
            'candidate_remaining_qty': self.candidate_remaining_qty,
            'candidate_avg_fill_price': self.candidate_avg_fill_price,
            'broker_submission_ref': None if self.broker_submission_ref is None else dict(self.broker_submission_ref),
            'execution_event_ref': None if self.execution_event_ref is None else dict(self.execution_event_ref),
            'fill_ledger_ref': None if self.fill_ledger_ref is None else dict(self.fill_ledger_ref),
            'input_record_refs': list(self.input_record_refs),
            'reason_codes': list(self.reason_codes),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj


def _blocked_decision_v1(
    *,
    submission_record: ExecutionSubmissionRecordV1,
    produced_utc: str,
    current: ExecutionStateRecordV1 | None,
    broker_submission_ref: dict[str, Any] | None,
    execution_event_ref: dict[str, Any] | None,
    fill_ledger_ref: dict[str, Any] | None,
    reason_codes: list[str] | tuple[str, ...],
) -> ExecutionLifecycleDecisionV1:
    candidate_lifecycle_status = 'UNKNOWN' if current is None else current.lifecycle_status
    candidate_source_lifecycle_status = None if current is None else current.source_lifecycle_status
    candidate_terminal_state = False if current is None else current.terminal_state
    evidence_basis = 'BROKER_SUBMISSION_RECORD' if current is None else current.evidence_basis
    candidate_broker_order_id = None if current is None else current.broker_order_id
    candidate_perm_id = None if current is None else current.perm_id
    candidate_filled_qty = 0 if current is None else current.filled_qty
    candidate_remaining_qty = None if current is None else current.remaining_qty
    candidate_avg_fill_price = None if current is None else current.avg_fill_price
    next_transition_index = 0 if current is None else current.transition_index
    evidence_fingerprint = _evidence_fingerprint_v1(
        broker_submission_ref=broker_submission_ref,
        execution_event_ref=execution_event_ref,
        fill_ledger_ref=fill_ledger_ref,
        blocked_reason_codes=reason_codes,
    )
    lifecycle_input_id = canonical_hash_for_c2_artifact_v1(
        {
            'submission_record_id': submission_record.submission_record_id,
            'current_execution_state_record_id': None if current is None else current.execution_state_record_id,
            'evidence_fingerprint': evidence_fingerprint,
        }
    )
    obj = {
        'schema_id': 'execution_lifecycle_decision',
        'schema_version': 'v1',
        'record_id': canonical_hash_for_c2_artifact_v1(
            {
                'lifecycle_input_id': lifecycle_input_id,
                'outcome': 'blocked',
                'candidate_lifecycle_status': candidate_lifecycle_status,
                'evidence_fingerprint': evidence_fingerprint,
            }
        ),
        'execution_lifecycle_decision_id': canonical_hash_for_c2_artifact_v1(
            {
                'submission_record_id': submission_record.submission_record_id,
                'current_execution_state_record_id': None if current is None else current.execution_state_record_id,
                'candidate_lifecycle_status': candidate_lifecycle_status,
                'evidence_fingerprint': evidence_fingerprint,
            }
        ),
        'lifecycle_input_id': lifecycle_input_id,
        'submission_record_id': submission_record.submission_record_id,
        'submission_id': submission_record.submission_id,
        'execution_intent_id': submission_record.execution_intent_id,
        'day_utc': submission_record.day_utc,
        'produced_utc': str(produced_utc),
        'contract_version': CONTRACT_VERSION,
        'outcome': 'blocked',
        'current_execution_state_record_id': None if current is None else current.execution_state_record_id,
        'candidate_lifecycle_status': candidate_lifecycle_status,
        'candidate_source_lifecycle_status': candidate_source_lifecycle_status,
        'candidate_terminal_state': bool(candidate_terminal_state),
        'evidence_basis': evidence_basis,
        'evidence_fingerprint': evidence_fingerprint,
        'next_transition_index': int(next_transition_index),
        'candidate_broker_order_id': candidate_broker_order_id,
        'candidate_perm_id': candidate_perm_id,
        'candidate_filled_qty': int(candidate_filled_qty),
        'candidate_remaining_qty': candidate_remaining_qty,
        'candidate_avg_fill_price': candidate_avg_fill_price,
        'broker_submission_ref': broker_submission_ref,
        'execution_event_ref': execution_event_ref,
        'fill_ledger_ref': fill_ledger_ref,
        'input_record_refs': sorted(
            {
                f'submission_record_id:{submission_record.submission_record_id}',
                *(
                    []
                    if broker_submission_ref is None
                    else [f'broker_submission_path:{broker_submission_ref["path"]}']
                ),
                *(
                    []
                    if execution_event_ref is None
                    else [f'execution_event_path:{execution_event_ref["path"]}']
                ),
                *(
                    []
                    if fill_ledger_ref is None
                    else [f'fill_ledger_path:{fill_ledger_ref["path"]}']
                ),
            }
        ),
        'reason_codes': sorted(set(str(item) for item in reason_codes if str(item).strip())),
    }
    return ExecutionLifecycleDecisionV1.from_dict(obj)


def build_execution_lifecycle_decision_v1(
    *,
    submission_record: ExecutionSubmissionRecordV1,
    produced_utc: str,
    truth_root: str | Path | None = None,
    submission_evidence_root_override: str | Path | None = None,
) -> ExecutionLifecycleDecisionV1:
    current = load_latest_execution_state_record_v1(
        truth_root=truth_root,
        day_utc=submission_record.day_utc,
        submission_id=submission_record.submission_id,
    )
    if submission_evidence_root_override is None:
        evidence_dir = submission_evidence_dir_for_truth_root_v1(
            truth_root=truth_root,
            day_utc=submission_record.day_utc,
            submission_id=submission_record.submission_id,
        )
    else:
        evidence_dir = (Path(submission_evidence_root_override).expanduser().resolve() / submission_record.day_utc / submission_record.submission_id).resolve()

    broker_submission_path = (evidence_dir / 'broker_submission_record.v2.json').resolve()
    if not broker_submission_path.exists():
        return _blocked_decision_v1(
            submission_record=submission_record,
            produced_utc=produced_utc,
            current=current,
            broker_submission_ref=None,
            execution_event_ref=None,
            fill_ledger_ref=None,
            reason_codes=['MISSING_BROKER_SUBMISSION_RECORD'],
        )
    broker_submission = read_json_obj_v1(broker_submission_path)
    broker_submission_ref = ref_for_path_v1(broker_submission_path)
    if str(broker_submission.get('submission_id') or '').strip() != submission_record.submission_id:
        return _blocked_decision_v1(
            submission_record=submission_record,
            produced_utc=produced_utc,
            current=current,
            broker_submission_ref=broker_submission_ref,
            execution_event_ref=None,
            fill_ledger_ref=None,
            reason_codes=['BROKER_SUBMISSION_LINKAGE_MISMATCH'],
        )

    execution_event_path = (evidence_dir / 'execution_event_record.v1.json').resolve()
    execution_event = _read_optional_json(execution_event_path)
    execution_event_ref = None if execution_event is None else ref_for_path_v1(execution_event_path)
    if isinstance(execution_event, dict):
        event_submission_hash = str(execution_event.get('broker_submission_hash') or '').strip()
        broker_submission_hash = str(broker_submission.get('canonical_json_hash') or '').strip()
        if broker_submission_hash and event_submission_hash and event_submission_hash != broker_submission_hash:
            return _blocked_decision_v1(
                submission_record=submission_record,
                produced_utc=produced_utc,
                current=current,
                broker_submission_ref=broker_submission_ref,
                execution_event_ref=execution_event_ref,
                fill_ledger_ref=None,
                reason_codes=['EXECUTION_EVENT_BROKER_SUBMISSION_HASH_MISMATCH'],
            )

    fill_ledger_path = fill_ledger_path_for_truth_root_v1(
        truth_root=truth_root,
        day_utc=submission_record.day_utc,
        submission_id=submission_record.submission_id,
    )
    fill_ledger = _read_optional_json(fill_ledger_path)
    fill_ledger_ref = None if fill_ledger is None else ref_for_path_v1(fill_ledger_path)
    if isinstance(fill_ledger, dict) and str(fill_ledger.get('submission_id') or '').strip() != submission_record.submission_id:
        return _blocked_decision_v1(
            submission_record=submission_record,
            produced_utc=produced_utc,
            current=current,
            broker_submission_ref=broker_submission_ref,
            execution_event_ref=execution_event_ref,
            fill_ledger_ref=fill_ledger_ref,
            reason_codes=['FILL_LEDGER_LINKAGE_MISMATCH'],
        )

    try:
        candidate = _normalize_candidate_state(
            broker_submission=broker_submission,
            execution_event=execution_event,
            fill_ledger=fill_ledger,
        )
    except ValueError as exc:
        return _blocked_decision_v1(
            submission_record=submission_record,
            produced_utc=produced_utc,
            current=current,
            broker_submission_ref=broker_submission_ref,
            execution_event_ref=execution_event_ref,
            fill_ledger_ref=fill_ledger_ref,
            reason_codes=[str(exc)],
        )
    evidence_fingerprint = _evidence_fingerprint_v1(
        broker_submission_ref=broker_submission_ref,
        execution_event_ref=execution_event_ref,
        fill_ledger_ref=fill_ledger_ref,
    )
    lifecycle_input_id = canonical_hash_for_c2_artifact_v1(
        {
            'submission_record_id': submission_record.submission_record_id,
            'current_execution_state_record_id': None if current is None else current.execution_state_record_id,
            'evidence_fingerprint': evidence_fingerprint,
        }
    )

    reason_codes: list[str] = []
    if current is None:
        outcome = 'advance'
        next_transition_index = 1
        reason_codes.append('INITIAL_LIFECYCLE_STATE_FROM_EVIDENCE')
    else:
        legal, illegal_reason = _is_legal_transition(current=current, candidate=candidate)
        if not legal:
            outcome = 'blocked'
            next_transition_index = current.transition_index
            reason_codes.append(str(illegal_reason))
        else:
            candidate_state_identity = canonical_hash_for_c2_artifact_v1(
                {
                    'previous_execution_state_record_id': current.execution_state_record_id,
                    'transition_index': current.transition_index + 1,
                    'candidate': candidate,
                    'evidence_fingerprint': evidence_fingerprint,
                }
            )
            current_effective_state = _effective_state_tuple(
                {
                    'lifecycle_status': current.lifecycle_status,
                    'filled_qty': current.filled_qty,
                    'remaining_qty': current.remaining_qty,
                    'avg_fill_price': current.avg_fill_price,
                    'broker_order_id': current.broker_order_id,
                    'perm_id': current.perm_id,
                    'terminal_state': current.terminal_state,
                }
            )
            candidate_effective_state = _effective_state_tuple(candidate)
            if evidence_fingerprint == current.evidence_fingerprint:
                outcome = 'duplicate'
                next_transition_index = current.transition_index
                reason_codes.append('IDENTICAL_LIFECYCLE_EVIDENCE_ALREADY_RECORDED')
            elif candidate_effective_state == current_effective_state and EVIDENCE_BASIS_RANK[candidate['evidence_basis']] <= EVIDENCE_BASIS_RANK[current.evidence_basis]:
                outcome = 'duplicate'
                next_transition_index = current.transition_index
                reason_codes.append('NO_STRONGER_LIFECYCLE_TRUTH_THAN_CURRENT')
            else:
                del candidate_state_identity
                outcome = 'advance'
                next_transition_index = current.transition_index + 1
                reason_codes.append('VALID_LIFECYCLE_ADVANCE_FROM_NEW_EVIDENCE')

    if outcome not in OUTCOMES:
        raise ValueError('EXECUTION_LIFECYCLE_DECISION_OUTCOME_INVALID')
    obj = {
        'schema_id': 'execution_lifecycle_decision',
        'schema_version': 'v1',
        'record_id': canonical_hash_for_c2_artifact_v1(
            {
                'lifecycle_input_id': lifecycle_input_id,
                'outcome': outcome,
                'candidate_lifecycle_status': candidate['lifecycle_status'],
                'evidence_fingerprint': evidence_fingerprint,
            }
        ),
        'execution_lifecycle_decision_id': canonical_hash_for_c2_artifact_v1(
            {
                'submission_record_id': submission_record.submission_record_id,
                'current_execution_state_record_id': None if current is None else current.execution_state_record_id,
                'candidate_lifecycle_status': candidate['lifecycle_status'],
                'evidence_fingerprint': evidence_fingerprint,
            }
        ),
        'lifecycle_input_id': lifecycle_input_id,
        'submission_record_id': submission_record.submission_record_id,
        'submission_id': submission_record.submission_id,
        'execution_intent_id': submission_record.execution_intent_id,
        'day_utc': submission_record.day_utc,
        'produced_utc': str(produced_utc),
        'contract_version': CONTRACT_VERSION,
        'outcome': outcome,
        'current_execution_state_record_id': None if current is None else current.execution_state_record_id,
        'candidate_lifecycle_status': candidate['lifecycle_status'],
        'candidate_source_lifecycle_status': candidate['source_lifecycle_status'],
        'candidate_terminal_state': bool(candidate['terminal_state']),
        'evidence_basis': candidate['evidence_basis'],
        'evidence_fingerprint': evidence_fingerprint,
        'next_transition_index': int(next_transition_index),
        'candidate_broker_order_id': candidate['broker_order_id'],
        'candidate_perm_id': candidate['perm_id'],
        'candidate_filled_qty': int(candidate['filled_qty']),
        'candidate_remaining_qty': candidate['remaining_qty'],
        'candidate_avg_fill_price': candidate['avg_fill_price'],
        'broker_submission_ref': broker_submission_ref,
        'execution_event_ref': execution_event_ref,
        'fill_ledger_ref': fill_ledger_ref,
        'input_record_refs': sorted(
            {
                f'submission_record_id:{submission_record.submission_record_id}',
                f'broker_submission_path:{broker_submission_ref["path"]}',
                *(
                    []
                    if execution_event_ref is None
                    else [f'execution_event_path:{execution_event_ref["path"]}']
                ),
                *(
                    []
                    if fill_ledger_ref is None
                    else [f'fill_ledger_path:{fill_ledger_ref["path"]}']
                ),
            }
        ),
        'reason_codes': sorted(set(reason_codes)),
    }
    return ExecutionLifecycleDecisionV1.from_dict(obj)


def write_execution_lifecycle_decision_v1(
    *,
    submission_record: ExecutionSubmissionRecordV1,
    produced_utc: str,
    truth_root: str | Path | None = None,
    submission_evidence_root_override: str | Path | None = None,
) -> tuple[ExecutionLifecycleDecisionV1, str]:
    decision = build_execution_lifecycle_decision_v1(
        submission_record=submission_record,
        produced_utc=produced_utc,
        truth_root=truth_root,
        submission_evidence_root_override=submission_evidence_root_override,
    )
    path = execution_lifecycle_decision_path_v1(
        truth_root=truth_root,
        day_utc=decision.day_utc,
        execution_lifecycle_decision_id=decision.execution_lifecycle_decision_id,
    )
    written = write_immutable_json_v1(path, decision.to_dict())
    return decision, str(written.path)
