from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.advisory.execution_package_builder_from_execution_intent_v1 import (
    derive_execution_submission_identity_from_execution_intent_v1,
)
from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    execution_submission_decision_path_v1,
    execution_submission_record_path_v1,
    read_json_obj_v1,
    submission_evidence_dir_for_truth_root_v1,
    write_immutable_json_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_submission_decision.v1.schema.json'
CONTRACT_VERSION = 'execution_submission_decision_contract_v1'
ALLOWED_OUTCOMES = {'submit', 'blocked', 'duplicate', 'no_action'}


def _sorted_unique(items: list[str] | tuple[str, ...]) -> list[str]:
    return sorted(set(str(item) for item in items if str(item).strip()))


@dataclass(frozen=True, slots=True)
class ExecutionSubmissionDecisionV1:
    schema_id: str
    schema_version: str
    record_id: str
    submission_decision_id: str
    execution_intent_id: str
    day_utc: str
    produced_utc: str
    contract_version: str
    outcome: str
    reason_codes: tuple[str, ...]
    predicted_submission_id: str
    predicted_trade_instance_id: str
    duplicate_submission_record_id: str | None
    duplicate_execution_evidence_path: str | None
    input_record_refs: tuple[str, ...]
    canonical_json_hash: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ExecutionSubmissionDecisionV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['submission_decision_id']),
            str(obj['execution_intent_id']),
            str(obj['day_utc']),
            str(obj['produced_utc']),
            str(obj['contract_version']),
            str(obj['outcome']),
            tuple(str(item) for item in obj['reason_codes']),
            str(obj['predicted_submission_id']),
            str(obj['predicted_trade_instance_id']),
            None if obj.get('duplicate_submission_record_id') is None else str(obj['duplicate_submission_record_id']),
            None if obj.get('duplicate_execution_evidence_path') is None else str(obj['duplicate_execution_evidence_path']),
            tuple(str(item) for item in obj['input_record_refs']),
            str(obj['canonical_json_hash']),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'submission_decision_id': self.submission_decision_id,
            'execution_intent_id': self.execution_intent_id,
            'day_utc': self.day_utc,
            'produced_utc': self.produced_utc,
            'contract_version': self.contract_version,
            'outcome': self.outcome,
            'reason_codes': list(self.reason_codes),
            'predicted_submission_id': self.predicted_submission_id,
            'predicted_trade_instance_id': self.predicted_trade_instance_id,
            'duplicate_submission_record_id': self.duplicate_submission_record_id,
            'duplicate_execution_evidence_path': self.duplicate_execution_evidence_path,
            'input_record_refs': list(self.input_record_refs),
            'canonical_json_hash': self.canonical_json_hash,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj


def build_execution_submission_decision_v1(
    *,
    execution_intent: ExecutionIntentV1,
    produced_utc: str,
    truth_root: str | Path | None = None,
) -> ExecutionSubmissionDecisionV1:
    reason_codes: list[str] = []
    duplicate_submission_record_id: str | None = None
    duplicate_execution_evidence_path: str | None = None

    if str(execution_intent.instrument.get('kind') or '').upper() != 'EQUITY':
        outcome = 'blocked'
        reason_codes.append('EXECUTION_INTENT_UNSUPPORTED_INSTRUMENT_KIND')
    elif execution_intent.quantity_shares <= 0:
        outcome = 'blocked'
        reason_codes.append('EXECUTION_INTENT_QUANTITY_INVALID')
    elif execution_intent.side not in {'BUY', 'SELL'}:
        outcome = 'blocked'
        reason_codes.append('EXECUTION_INTENT_SIDE_INVALID')
    elif str(execution_intent.order_terms.get('order_type') or '') not in {'LIMIT', 'MARKET'}:
        outcome = 'blocked'
        reason_codes.append('EXECUTION_INTENT_ORDER_TYPE_INVALID')
    else:
        derived = derive_execution_submission_identity_from_execution_intent_v1(execution_intent=execution_intent)
        submission_record_path = execution_submission_record_path_v1(
            truth_root=truth_root,
            day_utc=execution_intent.day_utc,
            submission_id=derived['submission_id'],
        )
        if submission_record_path.exists():
            duplicate_obj = read_json_obj_v1(submission_record_path)
            duplicate_submission_record_id = str(duplicate_obj.get('submission_record_id') or derived['submission_id'])
            outcome = 'duplicate'
            reason_codes.append('DUPLICATE_SUBMISSION_RECORD_EXISTS')
        else:
            evidence_dir = submission_evidence_dir_for_truth_root_v1(
                truth_root=truth_root,
                day_utc=execution_intent.day_utc,
                submission_id=derived['submission_id'],
            )
            if evidence_dir.exists():
                duplicate_execution_evidence_path = str(evidence_dir)
                outcome = 'duplicate'
                reason_codes.append('DUPLICATE_EXECUTION_EVIDENCE_EXISTS')
            else:
                outcome = 'submit'
                reason_codes.append('SUBMISSION_DECISION_SUBMIT')
    if 'derived' not in locals():
        derived = {
            'submission_id': canonical_hash_for_c2_artifact_v1(
                {
                    'execution_intent_id': execution_intent.execution_intent_id,
                    'day_utc': execution_intent.day_utc,
                    'idempotency_key': execution_intent.idempotency_key,
                }
            ),
            'trade_instance_id': canonical_hash_for_c2_artifact_v1(
                {
                    'execution_intent_id': execution_intent.execution_intent_id,
                    'sleeve_id': execution_intent.sleeve_id,
                    'environment': execution_intent.environment,
                }
            ),
        }

    if outcome not in ALLOWED_OUTCOMES:
        raise ValueError('EXECUTION_SUBMISSION_DECISION_OUTCOME_INVALID')

    obj = {
        'schema_id': 'execution_submission_decision',
        'schema_version': 'v1',
        'record_id': canonical_hash_for_c2_artifact_v1(
            {
                'execution_intent_id': execution_intent.execution_intent_id,
                'predicted_submission_id': derived['submission_id'],
                'outcome': outcome,
                'reason_codes': _sorted_unique(reason_codes),
            }
        ),
        'submission_decision_id': canonical_hash_for_c2_artifact_v1(
            {
                'execution_intent_id': execution_intent.execution_intent_id,
                'predicted_submission_id': derived['submission_id'],
                'outcome': outcome,
            }
        ),
        'execution_intent_id': execution_intent.execution_intent_id,
        'day_utc': execution_intent.day_utc,
        'produced_utc': str(produced_utc),
        'contract_version': CONTRACT_VERSION,
        'outcome': outcome,
        'reason_codes': _sorted_unique(reason_codes),
        'predicted_submission_id': derived['submission_id'],
        'predicted_trade_instance_id': derived['trade_instance_id'],
        'duplicate_submission_record_id': duplicate_submission_record_id,
        'duplicate_execution_evidence_path': duplicate_execution_evidence_path,
        'input_record_refs': _sorted_unique(
            [
                f'execution_intent_id:{execution_intent.execution_intent_id}',
                *execution_intent.parent_lineage_refs,
            ]
        ),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return ExecutionSubmissionDecisionV1.from_dict(obj)


def write_execution_submission_decision_v1(
    *,
    execution_intent: ExecutionIntentV1,
    produced_utc: str,
    truth_root: str | Path | None = None,
) -> tuple[ExecutionSubmissionDecisionV1, str]:
    decision = build_execution_submission_decision_v1(
        execution_intent=execution_intent,
        produced_utc=produced_utc,
        truth_root=truth_root,
    )
    path = execution_submission_decision_path_v1(
        truth_root=truth_root,
        day_utc=decision.day_utc,
        submission_decision_id=decision.submission_decision_id,
    )
    written = write_immutable_json_v1(path, decision.to_dict())
    return decision, str(written.path)
