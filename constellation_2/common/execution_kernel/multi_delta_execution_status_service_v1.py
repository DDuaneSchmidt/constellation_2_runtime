from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import execution_submission_record_path_v1
from constellation_2.common.execution_kernel.execution_set_intent_v1 import ExecutionSetIntentV1
from constellation_2.common.execution_kernel.execution_state_record_v1 import load_latest_execution_state_record_v1


def summarize_execution_set_status_v1(
    *,
    truth_root: str | Path | None,
    execution_set_intent: ExecutionSetIntentV1,
) -> dict[str, Any]:
    member_rows: list[dict[str, Any]] = []
    terminal_count = 0
    submitted_count = 0
    for member in execution_set_intent.member_intents:
        predicted_submission_id = str(member['predicted_submission_id'])
        submission_record_path = execution_submission_record_path_v1(
            truth_root=truth_root,
            day_utc=execution_set_intent.day_utc,
            submission_id=predicted_submission_id,
        )
        lifecycle = load_latest_execution_state_record_v1(
            truth_root=truth_root,
            day_utc=execution_set_intent.day_utc,
            submission_id=predicted_submission_id,
        )
        submission_record_exists = submission_record_path.exists()
        if submission_record_exists:
            submitted_count += 1
        if lifecycle is not None and lifecycle.terminal_state:
            terminal_count += 1
        member_rows.append(
            {
                'member_order': int(member['member_order']),
                'change_id': str(member['change_id']),
                'predicted_submission_id': predicted_submission_id,
                'submission_record_present': submission_record_exists,
                'lifecycle_status': None if lifecycle is None else lifecycle.lifecycle_status,
                'terminal_state': False if lifecycle is None else bool(lifecycle.terminal_state),
            }
        )

    total = len(member_rows)
    if total == 0:
        set_status = 'NO_ACTION'
    elif terminal_count == total:
        set_status = 'COMPLETE'
    elif submitted_count == 0:
        set_status = 'READY'
    elif 0 < terminal_count < total:
        set_status = 'PARTIAL'
    else:
        set_status = 'IN_FLIGHT'
    return {
        'execution_set_intent_id': execution_set_intent.execution_set_intent_id,
        'multi_delta_execution_record_id': execution_set_intent.multi_delta_execution_record_id,
        'member_count': total,
        'submitted_count': submitted_count,
        'terminal_count': terminal_count,
        'set_status': set_status,
        'members': member_rows,
    }
