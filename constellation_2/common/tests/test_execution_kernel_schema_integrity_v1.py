from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


def test_governance_manifest_and_index_register_execution_kernel_contracts_and_schemas() -> None:
    manifest_text = (SOURCE_ROOT / 'governance/00_MANIFEST.yaml').read_text(encoding='utf-8')
    index_text = (SOURCE_ROOT / 'governance/00_INDEX.md').read_text(encoding='utf-8')

    required_paths = [
        'governance/contracts/EXECUTION_KERNEL_BOUNDARY_CONTRACT.md',
        'governance/contracts/EXECUTION_SUBMISSION_CONTRACT.md',
        'governance/contracts/EXECUTION_STATE_KERNEL_BOUNDARY_CONTRACT.md',
        'governance/contracts/EXECUTION_STATE_AUTHORITY_CONTRACT.md',
        'governance/contracts/MULTI_DELTA_EXECUTION_KERNEL_BOUNDARY_CONTRACT.md',
        'governance/contracts/APPROVED_CHANGE_SET_CONTRACT.md',
        'governance/contracts/MULTI_DELTA_EXECUTION_DECISION_CONTRACT.md',
        'governance/contracts/MULTI_DELTA_EXECUTION_RECORD_CONTRACT.md',
        'governance/contracts/EXECUTION_SET_INTENT_CONTRACT.md',
        'governance/contracts/OPERATOR_RUNTIME_CONTROL_KERNEL_BOUNDARY_CONTRACT.md',
        'governance/contracts/RUNTIME_CONTROL_AUTHORITY_CONTRACT.md',
        'governance/04_DATA/SCHEMAS/C2/EXECUTION/approved_change_set.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/EXECUTION/multi_delta_execution_decision.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/EXECUTION/multi_delta_execution_record.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_set_intent.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_submission_decision.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_submission_record.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_lifecycle_decision.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_state_record.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_lifecycle_run_envelope.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_run_envelope.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_decision.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_record.v1.schema.json',
        'governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_run_envelope.v1.schema.json',
    ]

    for relpath in required_paths:
        assert relpath in manifest_text
        assert relpath in index_text
        assert (SOURCE_ROOT / relpath).exists()
