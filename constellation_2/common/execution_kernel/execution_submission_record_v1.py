from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.advisory.execution_package_builder_from_execution_intent_v1 import (
    build_execution_package_from_execution_intent_v1,
)
from constellation_2.common.execution_kernel.execution_kernel_storage_v1 import (
    execution_submission_record_path_v1,
    read_json_obj_v1,
    write_exclusive_immutable_json_v1,
)
from constellation_2.common.execution_kernel.execution_submission_decision_v1 import ExecutionSubmissionDecisionV1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_submission_record.v1.schema.json'
CONTRACT_VERSION = 'execution_submission_record_contract_v1'
BUILDER_VERSION = 'execution_submission_record_builder_v1'
PACKAGE_BRIDGE_BUILDER_VERSION = 'execution_submission_record_package_bridge_v1'


def _sorted_unique(items: list[str] | tuple[str, ...]) -> list[str]:
    return sorted(set(str(item) for item in items if str(item).strip()))


def _ref_from_obj(*, path: str | Path, sha256: str) -> dict[str, str]:
    return {'path': str(Path(path).expanduser().resolve()), 'sha256': str(sha256)}


def _is_64hex(value: str) -> bool:
    text = str(value or '').strip().lower()
    return len(text) == 64 and all(ch in '0123456789abcdef' for ch in text)


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or '').strip()
        if text:
            return text
    return ''


@dataclass(frozen=True, slots=True)
class ExecutionSubmissionRecordV1:
    schema_id: str
    schema_version: str
    record_id: str
    submission_record_id: str
    execution_intent_id: str
    promotion_record_id: str
    household_id: str
    day_utc: str
    produced_utc: str
    contract_version: str
    builder_version: str
    submission_id: str
    trade_instance_id: str | None
    idempotency_key: str
    status: str
    candidate_ref: dict[str, Any]
    downstream_payload_ref: dict[str, Any]
    execution_build_ref: dict[str, Any]
    execution_package_ref: dict[str, Any]
    input_record_refs: tuple[str, ...]
    parent_lineage_refs: tuple[str, ...]
    source_artifact_refs: tuple[str, ...]
    canonical_json_hash: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ExecutionSubmissionRecordV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['submission_record_id']),
            str(obj['execution_intent_id']),
            str(obj['promotion_record_id']),
            str(obj['household_id']),
            str(obj['day_utc']),
            str(obj['produced_utc']),
            str(obj['contract_version']),
            str(obj['builder_version']),
            str(obj['submission_id']),
            None if obj.get('trade_instance_id') is None else str(obj['trade_instance_id']),
            str(obj['idempotency_key']),
            str(obj['status']),
            dict(obj['candidate_ref']),
            dict(obj['downstream_payload_ref']),
            dict(obj['execution_build_ref']),
            dict(obj['execution_package_ref']),
            tuple(str(item) for item in obj['input_record_refs']),
            tuple(str(item) for item in obj['parent_lineage_refs']),
            tuple(str(item) for item in obj['source_artifact_refs']),
            str(obj['canonical_json_hash']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'ExecutionSubmissionRecordV1':
        return cls.from_dict(read_json_obj_v1(path))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'submission_record_id': self.submission_record_id,
            'execution_intent_id': self.execution_intent_id,
            'promotion_record_id': self.promotion_record_id,
            'household_id': self.household_id,
            'day_utc': self.day_utc,
            'produced_utc': self.produced_utc,
            'contract_version': self.contract_version,
            'builder_version': self.builder_version,
            'submission_id': self.submission_id,
            'trade_instance_id': self.trade_instance_id,
            'idempotency_key': self.idempotency_key,
            'status': self.status,
            'candidate_ref': dict(self.candidate_ref),
            'downstream_payload_ref': dict(self.downstream_payload_ref),
            'execution_build_ref': dict(self.execution_build_ref),
            'execution_package_ref': dict(self.execution_package_ref),
            'input_record_refs': list(self.input_record_refs),
            'parent_lineage_refs': list(self.parent_lineage_refs),
            'source_artifact_refs': list(self.source_artifact_refs),
            'canonical_json_hash': self.canonical_json_hash,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj


def build_execution_submission_record_v1(
    *,
    repo_root: Path,
    execution_intent: ExecutionIntentV1,
    submission_decision: ExecutionSubmissionDecisionV1,
    produced_utc: str,
) -> ExecutionSubmissionRecordV1:
    if submission_decision.outcome != 'submit':
        raise ValueError('SUBMISSION_DECISION_NOT_SUBMIT')

    package_result = build_execution_package_from_execution_intent_v1(
        repo_root=repo_root.resolve(),
        execution_intent=execution_intent,
    )
    package_obj = package_result.get('package_obj')
    build_obj = package_result.get('build_obj')
    package_path = package_result.get('package_path')
    if not isinstance(package_obj, dict) or not isinstance(build_obj, dict) or package_path is None:
        raise ValueError('EXECUTION_PACKAGE_BUILD_INCOMPLETE')
    if str(build_obj.get('closure_status') or '').strip().upper() != 'COMPLETE':
        raise ValueError('EXECUTION_PACKAGE_BUILD_NOT_COMPLETE')

    submission_id = str(package_obj.get('submission_id') or '').strip()
    if submission_id != submission_decision.predicted_submission_id:
        raise ValueError('SUBMISSION_ID_PREDICTION_MISMATCH')

    candidate_ref = package_obj.get('candidate_ref') if isinstance(package_obj.get('candidate_ref'), dict) else {}
    candidate_path = str(candidate_ref.get('phasec_out_dir') or '').strip()
    if not candidate_path:
        raise ValueError('EXECUTION_PACKAGE_CANDIDATE_REF_MISSING')
    selected_order_plan_ref = package_obj.get('selected_order_plan_ref') if isinstance(package_obj.get('selected_order_plan_ref'), dict) else {}
    selected_order_plan_path = str(selected_order_plan_ref.get('path') or '').strip()
    selected_order_plan_sha = str(selected_order_plan_ref.get('sha256') or '').strip()
    if not selected_order_plan_path or not selected_order_plan_sha:
        raise ValueError('EXECUTION_PACKAGE_SELECTED_ORDER_PLAN_REF_MISSING')
    build_ref = build_obj.get('package_ref') if isinstance(build_obj.get('package_ref'), dict) else None
    if build_ref is None:
        build_ref = {
            'path': str(Path(package_path).resolve()),
            'sha256': str(package_obj.get('canonical_json_hash') or ''),
        }

    obj = {
        'schema_id': 'execution_submission_record',
        'schema_version': 'v1',
        'record_id': submission_id,
        'submission_record_id': submission_id,
        'execution_intent_id': execution_intent.execution_intent_id,
        'promotion_record_id': execution_intent.promotion_record_id,
        'household_id': execution_intent.household_id,
        'day_utc': execution_intent.day_utc,
        'produced_utc': str(produced_utc),
        'contract_version': CONTRACT_VERSION,
        'builder_version': BUILDER_VERSION,
        'submission_id': submission_id,
        'trade_instance_id': package_obj.get('trade_instance_id'),
        'idempotency_key': execution_intent.idempotency_key,
        'status': 'READY_TO_SUBMIT',
        'candidate_ref': _ref_from_obj(
            path=candidate_path,
            sha256=str(candidate_ref.get('candidate_sha256') or canonical_hash_for_c2_artifact_v1({'candidate_path': candidate_path}) or ''),
        ),
        'downstream_payload_ref': _ref_from_obj(
            path=selected_order_plan_path,
            sha256=selected_order_plan_sha,
        ),
        'execution_build_ref': _ref_from_obj(
            path=str((package_obj.get('build_ref') or {}).get('path') or Path(package_path).resolve()),
            sha256=str((package_obj.get('build_ref') or {}).get('sha256') or build_obj.get('canonical_json_hash') or ''),
        ),
        'execution_package_ref': _ref_from_obj(
            path=package_path,
            sha256=str(package_obj.get('canonical_json_hash') or ''),
        ),
        'input_record_refs': _sorted_unique(
            [
                f'execution_intent_id:{execution_intent.execution_intent_id}',
                f'submission_decision_id:{submission_decision.submission_decision_id}',
            ]
        ),
        'parent_lineage_refs': _sorted_unique(
            [
                f'execution_intent_id:{execution_intent.execution_intent_id}',
                *execution_intent.parent_lineage_refs,
            ]
        ),
        'source_artifact_refs': _sorted_unique(
            [
                *execution_intent.source_artifact_refs,
                f'execution_package_path:{Path(package_path).resolve()}',
            ]
        ),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return ExecutionSubmissionRecordV1.from_dict(obj)


def write_execution_submission_record_v1(
    *,
    repo_root: Path,
    execution_intent: ExecutionIntentV1,
    submission_decision: ExecutionSubmissionDecisionV1,
    produced_utc: str,
    truth_root: str | Path | None = None,
) -> tuple[ExecutionSubmissionRecordV1, str, str]:
    record = build_execution_submission_record_v1(
        repo_root=repo_root,
        execution_intent=execution_intent,
        submission_decision=submission_decision,
        produced_utc=produced_utc,
    )
    path = execution_submission_record_path_v1(
        truth_root=truth_root,
        day_utc=record.day_utc,
        submission_id=record.submission_id,
    )
    written = write_exclusive_immutable_json_v1(path, record.to_dict())
    return record, str(written.path), str(written.action)


def build_execution_submission_record_from_execution_package_v1(
    *,
    execution_package_obj: dict[str, Any],
    execution_package_path: str | Path,
    day_utc: str,
    produced_utc: str,
    plan_obj: dict[str, Any] | None = None,
    binding_obj: dict[str, Any] | None = None,
) -> ExecutionSubmissionRecordV1:
    package_obj = dict(execution_package_obj)
    plan = {} if plan_obj is None else dict(plan_obj)
    binding = {} if binding_obj is None else dict(binding_obj)

    if str(package_obj.get('schema_id') or '').strip() != 'execution_package':
        raise ValueError('EXECUTION_PACKAGE_SCHEMA_ID_INVALID')
    if str(package_obj.get('schema_version') or '').strip() != 'v1':
        raise ValueError('EXECUTION_PACKAGE_SCHEMA_VERSION_INVALID')

    submission_id = str(package_obj.get('submission_id') or '').strip().lower()
    if not _is_64hex(submission_id):
        raise ValueError('EXECUTION_PACKAGE_SUBMISSION_ID_INVALID')

    package_sha = str(package_obj.get('canonical_json_hash') or '').strip().lower()
    if not _is_64hex(package_sha):
        raise ValueError('EXECUTION_PACKAGE_CANONICAL_JSON_HASH_INVALID')

    selected_plan_ref = package_obj.get('selected_order_plan_ref') if isinstance(package_obj.get('selected_order_plan_ref'), dict) else {}
    selected_plan_path = str(selected_plan_ref.get('path') or '').strip()
    selected_plan_sha = str(selected_plan_ref.get('sha256') or '').strip().lower()
    if not selected_plan_path or not _is_64hex(selected_plan_sha):
        raise ValueError('EXECUTION_PACKAGE_SELECTED_ORDER_PLAN_REF_INVALID')

    candidate_ref = package_obj.get('candidate_ref') if isinstance(package_obj.get('candidate_ref'), dict) else {}
    candidate_path = str(candidate_ref.get('phasec_out_dir') or '').strip()
    if not candidate_path:
        raise ValueError('EXECUTION_PACKAGE_CANDIDATE_PATH_MISSING')
    candidate_sha = str(candidate_ref.get('candidate_sha256') or '').strip().lower()
    if not _is_64hex(candidate_sha):
        candidate_sha = canonical_hash_for_c2_artifact_v1({'candidate_path': str(Path(candidate_path).resolve())})

    advisory_submission = package_obj.get('advisory_submission') if isinstance(package_obj.get('advisory_submission'), dict) else {}
    execution_intent_id = _first_nonempty(
        advisory_submission.get('execution_intent_id'),
        plan.get('source_intent_id'),
        binding.get('intent_id'),
        package_obj.get('intent_id'),
    )
    if not execution_intent_id:
        raise ValueError('EXECUTION_SUBMISSION_RECORD_EXECUTION_INTENT_ID_MISSING')

    promotion_record_id = _first_nonempty(
        advisory_submission.get('promotion_record_id'),
        binding.get('binding_id'),
    )
    if not promotion_record_id:
        promotion_record_id = f'phasec_submission:{submission_id}'

    household_id = _first_nonempty(
        advisory_submission.get('household_id'),
        binding.get('household_id'),
        plan.get('household_id'),
    )
    if not household_id:
        environment = _first_nonempty(candidate_ref.get('environment'), 'UNKNOWN_ENV').upper()
        sleeve_id = _first_nonempty(candidate_ref.get('sleeve_id'), 'UNKNOWN_SLEEVE').upper()
        household_id = f'{environment}:{sleeve_id}'

    idempotency_key = _first_nonempty(
        advisory_submission.get('promotion_idempotency_key'),
        plan.get('intent_sha256'),
        plan.get('intent_hash'),
        binding.get('intent_hash'),
    ).lower()
    if not _is_64hex(idempotency_key):
        idempotency_key = canonical_hash_for_c2_artifact_v1(
            {
                'submission_id': submission_id,
                'execution_intent_id': execution_intent_id,
                'intent_id': _first_nonempty(package_obj.get('intent_id'), plan.get('source_intent_id')),
            }
        )

    build_ref = package_obj.get('build_ref') if isinstance(package_obj.get('build_ref'), dict) else {}
    build_ref_path = _first_nonempty(build_ref.get('path'), str(Path(execution_package_path).expanduser().resolve()))
    build_ref_sha = _first_nonempty(build_ref.get('sha256'), package_sha)

    trade_instance_id_raw = str(package_obj.get('trade_instance_id') or '').strip().lower()
    trade_instance_id = trade_instance_id_raw if _is_64hex(trade_instance_id_raw) else None

    obj = {
        'schema_id': 'execution_submission_record',
        'schema_version': 'v1',
        'record_id': submission_id,
        'submission_record_id': submission_id,
        'execution_intent_id': execution_intent_id,
        'promotion_record_id': promotion_record_id,
        'household_id': household_id,
        'day_utc': str(day_utc),
        'produced_utc': str(produced_utc),
        'contract_version': CONTRACT_VERSION,
        'builder_version': PACKAGE_BRIDGE_BUILDER_VERSION,
        'submission_id': submission_id,
        'trade_instance_id': trade_instance_id,
        'idempotency_key': idempotency_key,
        'status': 'READY_TO_SUBMIT',
        'candidate_ref': _ref_from_obj(path=candidate_path, sha256=candidate_sha),
        'downstream_payload_ref': _ref_from_obj(path=selected_plan_path, sha256=selected_plan_sha),
        'execution_build_ref': _ref_from_obj(path=build_ref_path, sha256=build_ref_sha),
        'execution_package_ref': _ref_from_obj(path=execution_package_path, sha256=package_sha),
        'input_record_refs': _sorted_unique(
            [
                f'execution_intent_id:{execution_intent_id}',
                f'submission_id:{submission_id}',
            ]
        ),
        'parent_lineage_refs': _sorted_unique(
            [
                f'execution_intent_id:{execution_intent_id}',
            ]
        ),
        'source_artifact_refs': _sorted_unique(
            [
                f'execution_package_path:{Path(execution_package_path).expanduser().resolve()}',
                f'order_plan_path:{Path(selected_plan_path).expanduser().resolve()}',
            ]
        ),
        'canonical_json_hash': None,
    }
    obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**obj, 'canonical_json_hash': None})
    return ExecutionSubmissionRecordV1.from_dict(obj)


def write_execution_submission_record_from_execution_package_v1(
    *,
    execution_package_path: str | Path,
    day_utc: str,
    produced_utc: str,
    truth_root: str | Path | None = None,
    plan_obj: dict[str, Any] | None = None,
    binding_obj: dict[str, Any] | None = None,
) -> tuple[ExecutionSubmissionRecordV1, str, str]:
    package_path = Path(execution_package_path).expanduser().resolve()
    package_obj = read_json_obj_v1(package_path)
    record = build_execution_submission_record_from_execution_package_v1(
        execution_package_obj=package_obj,
        execution_package_path=package_path,
        day_utc=day_utc,
        produced_utc=produced_utc,
        plan_obj=plan_obj,
        binding_obj=binding_obj,
    )
    path = execution_submission_record_path_v1(
        truth_root=truth_root,
        day_utc=record.day_utc,
        submission_id=record.submission_id,
    )
    written = write_exclusive_immutable_json_v1(path, record.to_dict())
    return record, str(written.path), str(written.action)
