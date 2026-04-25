from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.execution_kernel.execution_lifecycle_decision_v1 import (
    ExecutionLifecycleDecisionV1,
    write_execution_lifecycle_decision_v1,
)
from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import execution_state_record_path_v1
from constellation_2.common.execution_kernel.execution_lifecycle_run_envelope_v1 import (
    emit_execution_lifecycle_run_envelope_v1,
)
from constellation_2.common.execution_kernel.execution_state_record_v1 import (
    ExecutionStateRecordV1,
    build_execution_state_record_v1,
    load_latest_execution_state_record_v1,
    write_execution_state_record_v1,
)
from constellation_2.common.execution_kernel.execution_submission_record_v1 import ExecutionSubmissionRecordV1


def _state_record_from_decision(
    *,
    submission_record: ExecutionSubmissionRecordV1,
    decision: ExecutionLifecycleDecisionV1,
    produced_utc: str,
    truth_root: str | Path | None,
) -> ExecutionStateRecordV1:
    current = load_latest_execution_state_record_v1(
        truth_root=truth_root,
        day_utc=submission_record.day_utc,
        submission_id=submission_record.submission_id,
    )
    previous_id = None if current is None else current.execution_state_record_id
    return build_execution_state_record_v1(
        submission_record=submission_record,
        produced_utc=produced_utc,
        lifecycle_status=decision.candidate_lifecycle_status,
        source_lifecycle_status=decision.candidate_source_lifecycle_status,
        terminal_state=decision.candidate_terminal_state,
        transition_index=decision.next_transition_index,
        previous_execution_state_record_id=previous_id,
        evidence_basis=decision.evidence_basis,
        evidence_fingerprint=decision.evidence_fingerprint,
        broker_order_id=decision.candidate_broker_order_id,
        perm_id=decision.candidate_perm_id,
        filled_qty=decision.candidate_filled_qty,
        remaining_qty=decision.candidate_remaining_qty,
        avg_fill_price=decision.candidate_avg_fill_price,
        broker_submission_ref=decision.broker_submission_ref,
        execution_event_ref=decision.execution_event_ref,
        fill_ledger_ref=decision.fill_ledger_ref,
        reason_codes=list(decision.reason_codes),
    )


def run_execution_lifecycle_v1(
    *,
    truth_root: str | Path | None,
    run_id: str,
    submission_record: ExecutionSubmissionRecordV1,
    produced_utc: str,
    submission_evidence_root_override: str | Path | None = None,
) -> dict[str, Any]:
    decision, decision_path = write_execution_lifecycle_decision_v1(
        submission_record=submission_record,
        produced_utc=produced_utc,
        truth_root=truth_root,
        submission_evidence_root_override=submission_evidence_root_override,
    )

    execution_state_record = None
    execution_state_record_path = None
    if decision.outcome == 'advance':
        record = _state_record_from_decision(
            submission_record=submission_record,
            decision=decision,
            produced_utc=produced_utc,
            truth_root=truth_root,
        )
        execution_state_record, execution_state_record_path, _ = write_execution_state_record_v1(
            record=record,
            truth_root=truth_root,
        )
    elif decision.outcome in {'duplicate', 'no_action'}:
        current = load_latest_execution_state_record_v1(
            truth_root=truth_root,
            day_utc=submission_record.day_utc,
            submission_id=submission_record.submission_id,
        )
        if current is not None:
            execution_state_record = current
            execution_state_record_path = str(
                execution_state_record_path_v1(
                    truth_root=truth_root,
                    day_utc=current.day_utc,
                    submission_id=current.submission_id,
                    execution_state_record_id=current.execution_state_record_id,
                )
            )

    envelope, envelope_path = emit_execution_lifecycle_run_envelope_v1(
        truth_root=truth_root,
        run_id=run_id,
        day_utc=submission_record.day_utc,
        produced_utc=produced_utc,
        run_outcome=decision.outcome,
        artifact_refs={
            'submission_record_id': submission_record.submission_record_id,
            'submission_id': submission_record.submission_id,
            'execution_lifecycle_decision_id': decision.execution_lifecycle_decision_id,
            'execution_lifecycle_decision_path': decision_path,
            'execution_state_record_id': None if execution_state_record is None else execution_state_record.execution_state_record_id,
            'execution_state_record_path': execution_state_record_path,
        },
        reason_codes=list(decision.reason_codes),
    )
    return {
        'execution_lifecycle_decision': decision,
        'execution_lifecycle_decision_path': decision_path,
        'execution_state_record': execution_state_record,
        'execution_state_record_path': execution_state_record_path,
        'execution_lifecycle_run_envelope': envelope,
        'execution_lifecycle_run_envelope_path': envelope_path,
    }
