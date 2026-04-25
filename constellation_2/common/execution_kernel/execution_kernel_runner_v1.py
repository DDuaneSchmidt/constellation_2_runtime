from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import execution_state_record_path_v1
from constellation_2.common.execution_kernel.execution_run_envelope_v1 import (
    assert_execution_run_envelope_writable_v1,
    emit_execution_run_envelope_v1,
)
from constellation_2.common.execution_kernel.execution_lifecycle_runner_v1 import run_execution_lifecycle_v1
from constellation_2.common.execution_kernel.execution_state_record_v1 import build_execution_attempt_state_record_v1
from constellation_2.common.execution_kernel.execution_submission_decision_v1 import (
    ExecutionSubmissionDecisionV1,
    write_execution_submission_decision_v1,
)
from constellation_2.common.execution_kernel.execution_submission_record_v1 import (
    ExecutionSubmissionRecordV1,
    write_execution_submission_record_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.submit_boundary_paper_v4 import run_submit_boundary_paper_v4


def submit_submission_record_v1(
    *,
    repo_root: Path,
    submission_record: ExecutionSubmissionRecordV1,
    submission_record_path: Path,
    eval_time_utc: str,
    risk_budget_path: Path,
    ib_host: str,
    ib_port: int,
    ib_client_id: int,
    ib_account: str,
    dry_run: bool,
    submissions_root_override: Path | None = None,
) -> int:
    if submission_record.status != 'READY_TO_SUBMIT':
        raise ValueError('SUBMISSION_RECORD_NOT_SUBMITTABLE')
    submission_record_path = submission_record_path.resolve()
    if not submission_record_path.exists():
        raise ValueError(f'SUBMISSION_RECORD_MISSING:{submission_record_path}')
    package_path = Path(str(submission_record.execution_package_ref.get('path') or '')).resolve()
    if not package_path.exists():
        raise ValueError(f'EXECUTION_PACKAGE_MISSING:{package_path}')
    expected_package_sha = str(submission_record.execution_package_ref.get('sha256') or '').strip()
    package_obj = json.loads(package_path.read_text(encoding='utf-8'))
    if not isinstance(package_obj, dict):
        raise ValueError('EXECUTION_PACKAGE_TOP_LEVEL_INVALID')
    actual_package_sha = canonical_hash_for_c2_artifact_v1({**package_obj, 'canonical_json_hash': None})
    recorded_package_sha = str(package_obj.get('canonical_json_hash') or '').strip()
    if recorded_package_sha != actual_package_sha:
        raise ValueError('EXECUTION_PACKAGE_CANONICAL_JSON_HASH_MISMATCH')
    if expected_package_sha and expected_package_sha != actual_package_sha:
        raise ValueError('EXECUTION_PACKAGE_SHA256_MISMATCH')
    selected_order_plan_ref = package_obj.get('selected_order_plan_ref') if isinstance(package_obj.get('selected_order_plan_ref'), dict) else {}
    selected_order_plan_path = Path(str(selected_order_plan_ref.get('path') or '')).resolve()
    if not selected_order_plan_path.exists():
        raise ValueError(f'EXECUTION_PACKAGE_SELECTED_ORDER_PLAN_MISSING:{selected_order_plan_path}')
    selected_order_plan_sha = str(selected_order_plan_ref.get('sha256') or '').strip()
    if str(submission_record.downstream_payload_ref.get('path') or '').strip() != str(selected_order_plan_path):
        raise ValueError('EXECUTION_PACKAGE_DOWNSTREAM_PAYLOAD_PATH_MISMATCH')
    if str(submission_record.downstream_payload_ref.get('sha256') or '').strip() != selected_order_plan_sha:
        raise ValueError('EXECUTION_PACKAGE_DOWNSTREAM_PAYLOAD_SHA256_MISMATCH')
    return run_submit_boundary_paper_v4(
        repo_root=repo_root.resolve(),
        eval_time_utc=str(eval_time_utc),
        phasec_out_dir=None,
        execution_package_path=package_path,
        submission_record_path=submission_record_path,
        allow_legacy_raw_candidate=False,
        risk_budget_path=risk_budget_path.resolve(),
        ib_host=str(ib_host),
        ib_port=int(ib_port),
        ib_client_id=int(ib_client_id),
        ib_account=str(ib_account),
        dry_run=bool(dry_run),
        submissions_root_override=None if submissions_root_override is None else submissions_root_override.resolve(),
    )


def _submission_record_path(record: ExecutionSubmissionRecordV1) -> Path:
    return Path(str(record.execution_package_ref.get('path') or '')).resolve().parents[1] / 'submission_record.v1.json'


def _artifact_refs(
    *,
    execution_intent: ExecutionIntentV1,
    submission_decision: ExecutionSubmissionDecisionV1,
    submission_record: ExecutionSubmissionRecordV1 | None,
    execution_state_record_path: str | None,
) -> dict[str, Any]:
    refs: dict[str, Any] = {
        'execution_intent_id': execution_intent.execution_intent_id,
        'promotion_record_id': execution_intent.promotion_record_id,
        'submission_decision_id': submission_decision.submission_decision_id,
        'submission_record_id': None,
        'submission_id': submission_decision.predicted_submission_id,
        'execution_package_path': None,
        'execution_package_sha256': None,
        'execution_state_record_path': execution_state_record_path,
    }
    if submission_record is not None:
        refs['submission_record_id'] = submission_record.submission_record_id
        refs['execution_package_path'] = submission_record.execution_package_ref['path']
        refs['execution_package_sha256'] = submission_record.execution_package_ref['sha256']
    return refs


def _attempt_state_record_path(
    *,
    submission_record: ExecutionSubmissionRecordV1,
    truth_root: str | Path | None,
    produced_utc: str,
) -> str:
    attempt_record = build_execution_attempt_state_record_v1(
        submission_record=submission_record,
        produced_utc=produced_utc,
    )
    return str(
        execution_state_record_path_v1(
            truth_root=truth_root,
            day_utc=attempt_record.day_utc,
            submission_id=attempt_record.submission_id,
            execution_state_record_id=attempt_record.execution_state_record_id,
        )
    )


def run_execution_kernel_v1(
    *,
    repo_root: Path,
    truth_root: str | Path | None,
    run_id: str,
    execution_intent: ExecutionIntentV1,
    produced_utc: str,
    eval_time_utc: str,
    risk_budget_path: Path,
    ib_host: str,
    ib_port: int,
    ib_client_id: int,
    dry_run: bool,
    submissions_root_override: Path | None = None,
) -> dict[str, Any]:
    decision, decision_path = write_execution_submission_decision_v1(
        execution_intent=execution_intent,
        produced_utc=produced_utc,
        truth_root=truth_root,
    )

    stage_status = {
        'execution_intent': 'PRESENT',
        'submission_decision': decision.outcome.upper(),
        'submission_record': 'SKIPPED',
        'submit_boundary': 'SKIPPED',
        'execution_state': 'SKIPPED',
    }
    handoff_status = {'paper_submit': 'NOT_ATTEMPTED'}
    submission_record: ExecutionSubmissionRecordV1 | None = None
    execution_state_record = None
    execution_state_record_path = None
    reason_codes = list(decision.reason_codes)
    submission_record_write_action = None

    assert_execution_run_envelope_writable_v1(
        truth_root=truth_root,
        day_utc=execution_intent.day_utc,
        run_id=run_id,
    )

    if decision.outcome == 'submit':
        submission_record, submission_record_path, submission_record_write_action = write_execution_submission_record_v1(
            repo_root=repo_root,
            execution_intent=execution_intent,
            submission_decision=decision,
            produced_utc=produced_utc,
            truth_root=truth_root,
        )
        if submission_record_write_action != 'WROTE':
            stage_status['submission_record'] = 'DUPLICATE'
            reason_codes.append('DUPLICATE_SUBMISSION_RECORD_EXISTS_AFTER_DECISION')
            run_outcome = 'duplicate'
        else:
            stage_status['submission_record'] = 'READY_TO_SUBMIT'
            try:
                rc = submit_submission_record_v1(
                    repo_root=repo_root.resolve(),
                    submission_record=submission_record,
                    submission_record_path=Path(submission_record_path),
                    eval_time_utc=str(eval_time_utc),
                    risk_budget_path=risk_budget_path.resolve(),
                    ib_host=str(ib_host),
                    ib_port=int(ib_port),
                    ib_client_id=int(ib_client_id),
                    ib_account=execution_intent.account_id,
                    dry_run=bool(dry_run),
                    submissions_root_override=None if submissions_root_override is None else submissions_root_override.resolve(),
                )
            except Exception:
                handoff_status['paper_submit'] = 'FAILED_CLOSED'
                stage_status['submit_boundary'] = 'FAILED_CLOSED'
                reason_codes.append('EXECUTION_SUBMIT_BOUNDARY_FAILURE')
                attempt_state_path = _attempt_state_record_path(
                    submission_record=submission_record,
                    truth_root=truth_root,
                    produced_utc=produced_utc,
                )
                if Path(attempt_state_path).exists():
                    execution_state_record_path = attempt_state_path
                    stage_status['execution_state'] = 'HANDOFF_ENTERED'
                    reason_codes.append('EXECUTION_HANDOFF_ENTERED_PRE_SUBMIT')
                run_outcome = 'blocked'
            else:
                handoff_status['paper_submit'] = f'RC_{rc}'
                stage_status['submit_boundary'] = 'ATTEMPTED'
                if rc in {0, 3}:
                    try:
                        lifecycle_result = run_execution_lifecycle_v1(
                            truth_root=truth_root,
                            run_id=f'{run_id}.lifecycle',
                            submission_record=submission_record,
                            produced_utc=produced_utc,
                            submission_evidence_root_override=submissions_root_override,
                        )
                    except Exception:
                        stage_status['execution_state'] = 'FAILED_CLOSED'
                        reason_codes.append('EXECUTION_STATE_RECORD_WRITE_FAILED_AFTER_HANDOFF')
                        attempt_state_path = _attempt_state_record_path(
                            submission_record=submission_record,
                            truth_root=truth_root,
                            produced_utc=produced_utc,
                        )
                        if Path(attempt_state_path).exists():
                            execution_state_record_path = attempt_state_path
                    else:
                        execution_state_record = lifecycle_result['execution_state_record']
                        execution_state_record_path = lifecycle_result['execution_state_record_path']
                        if execution_state_record is not None:
                            stage_status['execution_state'] = execution_state_record.lifecycle_status
                        else:
                            stage_status['execution_state'] = lifecycle_result['execution_lifecycle_decision'].outcome.upper()
                    run_outcome = 'submit'
                else:
                    attempt_state_path = _attempt_state_record_path(
                        submission_record=submission_record,
                        truth_root=truth_root,
                        produced_utc=produced_utc,
                    )
                    if Path(attempt_state_path).exists():
                        execution_state_record_path = attempt_state_path
                        stage_status['execution_state'] = 'HANDOFF_ENTERED'
                        reason_codes.append('EXECUTION_HANDOFF_ENTERED_PRE_SUBMIT')
                        run_outcome = 'submit'
                    else:
                        run_outcome = 'blocked'
    elif decision.outcome == 'duplicate':
        run_outcome = 'duplicate'
    elif decision.outcome == 'no_action':
        run_outcome = 'no_action'
    else:
        run_outcome = 'blocked'

    envelope, envelope_path = emit_execution_run_envelope_v1(
        truth_root=truth_root,
        run_id=run_id,
        day_utc=execution_intent.day_utc,
        produced_utc=produced_utc,
        run_outcome=run_outcome,
        stage_status=stage_status,
        artifact_refs=_artifact_refs(
            execution_intent=execution_intent,
            submission_decision=decision,
            submission_record=submission_record,
            execution_state_record_path=execution_state_record_path,
        ),
        handoff_status=handoff_status,
        reason_codes=reason_codes,
    )
    return {
        'submission_decision': decision,
        'submission_decision_path': decision_path,
        'submission_record': submission_record,
        'execution_state_record': execution_state_record,
        'execution_run_envelope': envelope,
        'execution_run_envelope_path': envelope_path,
    }
