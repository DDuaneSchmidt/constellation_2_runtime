from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.runtime_control_kernel.runtime_control_decision_v1 import (
    CAPABILITY_SCOPE_PAPER_TRADE_SUBMIT_ENTRY_V1,
    RuntimeControlDecisionV1,
    assemble_runtime_control_inputs_v1,
    write_runtime_control_decision_v1,
)
from constellation_2.common.runtime_control_kernel.runtime_control_record_v1 import (
    RuntimeControlRecordV1,
    write_runtime_control_record_v1,
)
from constellation_2.common.runtime_control_kernel.runtime_control_run_envelope_v1 import (
    RuntimeControlRunEnvelopeV1,
    emit_runtime_control_run_envelope_v1,
)
from constellation_2.common.runtime_control_kernel.runtime_control_storage_v1 import (
    read_json_obj_v1,
    runtime_control_run_envelope_path_v1,
)


def run_runtime_control_kernel_v1(
    *,
    canonical_truth_root: str | Path,
    execution_truth_root: str | Path,
    day_utc: str,
    produced_utc: str,
    run_id: str,
    environment: str,
    ib_account: str,
    sleeve_id: str = 'PRIMARY',
    capability_scope: str = CAPABILITY_SCOPE_PAPER_TRADE_SUBMIT_ENTRY_V1,
) -> dict[str, Any]:
    existing_envelope_path = runtime_control_run_envelope_path_v1(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
        run_id=run_id,
    )
    if existing_envelope_path.exists() and existing_envelope_path.is_file():
        envelope = RuntimeControlRunEnvelopeV1.load_file(existing_envelope_path)
        decision_path = str(envelope.artifact_refs.get('runtime_control_decision_path') or '').strip()
        if not decision_path:
            raise ValueError(f'RUNTIME_CONTROL_RUN_ENVELOPE_INVALID:decision_path_missing:{existing_envelope_path}')
        decision = RuntimeControlDecisionV1.from_dict(read_json_obj_v1(decision_path))
        record_path = str(envelope.artifact_refs.get('runtime_control_record_path') or '').strip()
        record = RuntimeControlRecordV1.load_file(record_path) if record_path else None
        return {
            'runtime_control_inputs': None,
            'runtime_control_decision': decision,
            'runtime_control_decision_path': decision_path,
            'runtime_control_record': record,
            'runtime_control_record_path': record_path or None,
            'runtime_control_record_write_action': 'SKIP_EXISTING_ENVELOPE',
            'runtime_control_run_envelope': envelope,
            'runtime_control_run_envelope_path': str(existing_envelope_path),
        }

    inputs = assemble_runtime_control_inputs_v1(
        canonical_truth_root=canonical_truth_root,
        execution_truth_root=execution_truth_root,
        day_utc=day_utc,
        environment=environment,
        ib_account=ib_account,
        sleeve_id=sleeve_id,
        capability_scope=capability_scope,
    )
    decision, decision_path = write_runtime_control_decision_v1(
        truth_root=canonical_truth_root,
        produced_utc=produced_utc,
        inputs=inputs,
    )

    record: RuntimeControlRecordV1 | None = None
    record_path: str | None = None
    record_write_action: str | None = None
    if inputs.valid_for_record and inputs.control_state is not None and decision.outcome != 'duplicate':
        record, record_path, record_write_action = write_runtime_control_record_v1(
            truth_root=canonical_truth_root,
            produced_utc=produced_utc,
            inputs=inputs,
        )
    elif decision.existing_record_ref is not None:
        record_path = str(decision.existing_record_ref.get('path') or '')
        if record_path:
            record = RuntimeControlRecordV1.load_file(record_path)

    if decision.outcome == 'duplicate':
        run_outcome = 'duplicate'
    elif record is not None and record.control_state == 'ALLOW':
        run_outcome = 'allow'
    elif record is not None and record.control_state == 'BLOCKED':
        run_outcome = 'blocked'
    else:
        run_outcome = 'blocked'

    stage_status = {
        'input_assembly': 'VALID' if inputs.valid_for_record else 'BLOCKED',
        'decision': decision.outcome.upper(),
        'control_record': (
            'SKIPPED'
            if record is None
            else (record.control_state if record_write_action in {'WROTE', None} else str(record_write_action))
        ),
    }
    artifact_refs = {
        'runtime_control_decision_path': decision_path,
        'runtime_control_record_path': record_path,
        'capability_scope': capability_scope,
        'source_artifact_refs': list(inputs.source_artifact_ref_strings),
    }
    envelope, envelope_path = emit_runtime_control_run_envelope_v1(
        truth_root=canonical_truth_root,
        run_id=run_id,
        day_utc=day_utc,
        produced_utc=produced_utc,
        capability_scope=capability_scope,
        environment=environment,
        ib_account=ib_account,
        sleeve_id=sleeve_id,
        run_outcome=run_outcome,
        stage_status=stage_status,
        artifact_refs=artifact_refs,
        reason_codes=list(decision.reason_codes),
    )
    return {
        'runtime_control_inputs': inputs,
        'runtime_control_decision': decision,
        'runtime_control_decision_path': decision_path,
        'runtime_control_record': record,
        'runtime_control_record_path': record_path,
        'runtime_control_record_write_action': record_write_action,
        'runtime_control_run_envelope': envelope,
        'runtime_control_run_envelope_path': envelope_path,
    }
