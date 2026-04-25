from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from constellation_2.common.execution_build_authority_v1 import run_execution_build_authority_v1
from constellation_2.common.execution_identity_authority_v1 import (
    build_execution_identity_record_v1,
    derive_submission_id_v1,
    derive_trade_instance_id_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_truth_sleeves_root
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_account_binding
from constellation_2.phaseC.lib.evidence_writer_v2 import write_phasec_success_outputs_equity_v2
from constellation_2.phaseD.lib.ib_payload_stock_order_v2 import build_binding_digest_for_equity_order_plan_v2
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1


def _canonical_write(path: Path, obj: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b'\n'
    if path.exists():
        current = path.read_bytes()
        if current != payload:
            raise ValueError(f'IMMUTABLE_CONFLICT:{path}')
    else:
        path.write_bytes(payload)
    return canonical_hash_for_c2_artifact_v1(obj)


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT:{path}')
    return obj


def _advisory_submission_obj(execution_intent: ExecutionIntentV1) -> dict[str, str]:
    return {
        'origin': 'advisory_kernel_v1',
        'execution_intent_id': execution_intent.execution_intent_id,
        'execution_intent_canonical_hash': execution_intent.canonical_json_hash,
        'promotion_record_id': execution_intent.promotion_record_id,
        'promotion_idempotency_key': execution_intent.idempotency_key,
    }


def _stamp_advisory_submission_in_package_v1(*, repo_root: Path, package_path: Path, execution_intent: ExecutionIntentV1) -> dict[str, Any]:
    package_obj = _read_json_obj(package_path.resolve())
    if str(package_obj.get('schema_id') or '').strip() != 'execution_package':
        raise ValueError('EXECUTION_PACKAGE_SCHEMA_ID_INVALID')
    if str(package_obj.get('schema_version') or '').strip() != 'v1':
        raise ValueError('EXECUTION_PACKAGE_SCHEMA_VERSION_INVALID')
    if package_obj.get('sealed') is not True:
        raise ValueError('EXECUTION_PACKAGE_NOT_SEALED')

    desired = _advisory_submission_obj(execution_intent)
    current = package_obj.get('advisory_submission')
    if current is not None and current != desired:
        raise ValueError('ADVISORY_SUBMISSION_PROVENANCE_CONFLICT')
    if current == desired:
        return package_obj

    stamped = dict(package_obj)
    stamped['advisory_submission'] = desired
    stamped['canonical_json_hash'] = None
    stamped['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(stamped)
    validate_against_repo_schema_v1(
        stamped,
        repo_root.resolve(),
        'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_package.v1.schema.json',
    )
    payload = canonical_json_bytes_v1(stamped) + b'\n'
    package_path.write_bytes(payload)
    return stamped


def _candidate_path(*, execution_intent: ExecutionIntentV1) -> Path:
    truth_sleeves_root = resolve_truth_sleeves_root().resolve()
    attempt_id = execution_intent.idempotency_key[:12].upper()
    intent_hash = execution_intent.idempotency_key
    return (
        truth_sleeves_root
        / execution_intent.sleeve_id
        / execution_intent.environment
        / 'phaseC_preflight_v1'
        / execution_intent.day_utc
        / f'attempt_{attempt_id}'
        / intent_hash
    ).resolve()


def _equity_order_plan_v2(execution_intent: ExecutionIntentV1) -> dict[str, Any]:
    if str(execution_intent.instrument.get('kind') or '').upper() != 'EQUITY':
        raise ValueError('EXECUTION_INTENT_ONLY_EQUITY_SUPPORTED')
    plan = {
        'schema_id': 'equity_order_plan',
        'schema_version': 'v2',
        'plan_id': execution_intent.execution_intent_id,
        'created_at_utc': execution_intent.created_at_utc,
        'intent_hash': execution_intent.idempotency_key,
        'structure': 'EQUITY_SPOT',
        'symbol': str(execution_intent.instrument['symbol']),
        'currency': str(execution_intent.instrument['currency']),
        'action': execution_intent.side,
        'qty_shares': int(execution_intent.quantity_shares),
        'order_terms': dict(execution_intent.order_terms),
        'engine_id': execution_intent.engine_id,
        'source_intent_id': execution_intent.execution_intent_id,
        'intent_sha256': execution_intent.idempotency_key,
    }
    validate_against_repo_schema_v1(plan, Path(__file__).resolve().parents[3], 'constellation_2/schemas/equity_order_plan.v2.schema.json')
    return plan


def derive_execution_submission_identity_from_execution_intent_v1(*, execution_intent: ExecutionIntentV1) -> dict[str, Any]:
    candidate_path = _candidate_path(execution_intent=execution_intent)
    attempt_id = candidate_path.parent.name.removeprefix('attempt_')
    plan_obj = _equity_order_plan_v2(execution_intent)
    plan_hash = canonical_hash_for_c2_artifact_v1(plan_obj)
    trade_instance_id = derive_trade_instance_id_v1(
        day_utc=execution_intent.day_utc,
        attempt_id=attempt_id,
        sleeve_id=execution_intent.sleeve_id,
        environment=execution_intent.environment,
        intent_id=execution_intent.execution_intent_id,
        intent_hash=execution_intent.idempotency_key,
    )
    submission_id = derive_submission_id_v1(
        intent_id=execution_intent.execution_intent_id,
        plan_hash=plan_hash,
        trade_instance_id=trade_instance_id,
    )
    return {
        'candidate_path': candidate_path,
        'attempt_id': attempt_id,
        'plan_obj': plan_obj,
        'plan_hash': plan_hash,
        'trade_instance_id': trade_instance_id,
        'submission_id': submission_id,
    }


def _mapping_ledger_record_v2(*, execution_intent: ExecutionIntentV1, plan_obj: dict[str, Any], trade_instance_id: str) -> dict[str, Any]:
    plan_hash = canonical_hash_for_c2_artifact_v1(plan_obj)
    record = {
        'schema_id': 'mapping_ledger_record',
        'schema_version': 'v2',
        'record_id': canonical_hash_for_c2_artifact_v1(
            {
                'intent_hash': execution_intent.idempotency_key,
                'plan_hash': plan_hash,
                'mode': 'EQUITY_DIRECT_V1',
                'trade_instance_id': trade_instance_id,
            }
        ),
        'created_at_utc': execution_intent.created_at_utc,
        'intent_hash': execution_intent.idempotency_key,
        'plan_hash': plan_hash,
        'intent_id': execution_intent.execution_intent_id,
        'trade_instance_id': trade_instance_id,
        'mapping_mode': 'EQUITY_DIRECT_V1',
        'options_context': None,
        'equity_context': {
            'symbol': str(execution_intent.instrument['symbol']),
            'currency': str(execution_intent.instrument['currency']),
            'action': execution_intent.side,
            'qty_shares': int(execution_intent.quantity_shares),
        },
        'selection_trace': {
            'policy': 'ADVISORY_EXECUTION_INTENT_V1',
            'tie_breakers': ['PROMOTION_RECORD_APPROVED_DELTA'],
        },
        'canonical_json_hash': None,
    }
    record['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**record, 'canonical_json_hash': None})
    validate_against_repo_schema_v1(record, Path(__file__).resolve().parents[3], 'constellation_2/schemas/mapping_ledger_record.v2.schema.json')
    return record


def _binding_record_v2(*, execution_intent: ExecutionIntentV1, plan_obj: dict[str, Any], mapping_obj: dict[str, Any], trade_instance_id: str, submission_id: str) -> dict[str, Any]:
    _, payload_digest = build_binding_digest_for_equity_order_plan_v2(plan_obj)
    record = {
        'schema_id': 'binding_record',
        'schema_version': 'v2',
        'binding_id': canonical_hash_for_c2_artifact_v1(
            {
                'intent_hash': execution_intent.idempotency_key,
                'plan_hash': canonical_hash_for_c2_artifact_v1(plan_obj),
                'trade_instance_id': trade_instance_id,
                'submission_id': submission_id,
            }
        ),
        'created_at_utc': execution_intent.created_at_utc,
        'intent_id': execution_intent.execution_intent_id,
        'intent_hash': execution_intent.idempotency_key,
        'plan_hash': canonical_hash_for_c2_artifact_v1(plan_obj),
        'mapping_ledger_hash': canonical_hash_for_c2_artifact_v1(mapping_obj),
        'trade_instance_id': trade_instance_id,
        'submission_id': submission_id,
        'freshness_cert_hash': None,
        'broker_payload_digest': {
            'digest_sha256': payload_digest.digest_sha256,
            'format': payload_digest.format,
            'notes': payload_digest.notes,
        },
        'preflight': {
            'validated_schema': True,
            'validated_invariants': True,
            'validated_freshness': True,
            'defined_risk_proven': True,
            'exit_policy_present': True,
        },
        'canonical_json_hash': None,
    }
    record['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1({**record, 'canonical_json_hash': None})
    validate_against_repo_schema_v1(record, Path(__file__).resolve().parents[3], 'constellation_2/schemas/binding_record.v2.schema.json')
    return record


def stage_candidate_from_execution_intent_v1(*, repo_root: Path, execution_intent: ExecutionIntentV1) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    governed_binding = resolve_governed_account_binding(
        repo_root=repo_root,
        environment=execution_intent.environment,
        requested_ib_account=execution_intent.account_id,
        sleeve_id=execution_intent.sleeve_id,
    )
    if governed_binding.ib_account != execution_intent.account_id:
        raise ValueError('EXECUTION_INTENT_ACCOUNT_BINDING_MISMATCH')

    derived = derive_execution_submission_identity_from_execution_intent_v1(execution_intent=execution_intent)
    candidate_path = Path(derived['candidate_path']).resolve()
    plan_obj = dict(derived['plan_obj'])
    attempt_id = str(derived['attempt_id'])
    trade_instance_id = str(derived['trade_instance_id'])
    plan_hash = str(derived['plan_hash'])
    submission_id = str(derived['submission_id'])
    mapping_obj = _mapping_ledger_record_v2(
        execution_intent=execution_intent,
        plan_obj=plan_obj,
        trade_instance_id=trade_instance_id,
    )
    binding_obj = _binding_record_v2(
        execution_intent=execution_intent,
        plan_obj=plan_obj,
        mapping_obj=mapping_obj,
        trade_instance_id=trade_instance_id,
        submission_id=submission_id,
    )
    binding_hash = canonical_hash_for_c2_artifact_v1(binding_obj)
    execution_identity_obj = build_execution_identity_record_v1(
        created_at_utc=execution_intent.created_at_utc,
        day_utc=execution_intent.day_utc,
        attempt_id=attempt_id,
        sleeve_id=execution_intent.sleeve_id,
        environment=execution_intent.environment,
        intent_id=execution_intent.execution_intent_id,
        intent_hash=execution_intent.idempotency_key,
        plan_hash=plan_hash,
        binding_hash=binding_hash,
        trade_instance_id=trade_instance_id,
        submission_id=submission_id,
        duplicate_classification='NEW_INSTANCE_SAME_PLAN',
        source_refs=[
            {'type': 'execution_intent_id', 'path': execution_intent.execution_intent_id},
            {'type': 'execution_intent_canonical_hash', 'path': execution_intent.canonical_json_hash},
            {'type': 'promotion_record_id', 'path': execution_intent.promotion_record_id},
            {'type': 'promotion_idempotency_key', 'path': execution_intent.idempotency_key},
        ],
    )
    submit_preflight_obj = {
        'decision': 'ALLOW',
        'day_utc': execution_intent.day_utc,
        'trade_instance_id': trade_instance_id,
        'submission_id': submission_id,
    }
    attempt_state_obj = {
        'status': 'ACTIVE',
        'day_utc': execution_intent.day_utc,
    }

    required_paths = {
        candidate_path / 'equity_order_plan.v2.json': plan_obj,
        candidate_path / 'mapping_ledger_record.v2.json': mapping_obj,
        candidate_path / 'binding_record.v2.json': binding_obj,
        candidate_path / 'submit_preflight_decision.v1.json': submit_preflight_obj,
        candidate_path / 'execution_identity_record.v1.json': execution_identity_obj,
        candidate_path.parent / 'attempt_state.v1.json': attempt_state_obj,
    }
    if candidate_path.exists():
        for path, obj in required_paths.items():
            payload = canonical_json_bytes_v1(obj) + b'\n'
            if not path.exists() or path.read_bytes() != payload:
                raise ValueError(f'CANDIDATE_IDENTITY_SET_MISMATCH:{path}')
    else:
        write_phasec_success_outputs_equity_v2(
            candidate_path,
            equity_order_plan_v2=plan_obj,
            mapping_ledger_record_v2=mapping_obj,
            binding_record_v2=binding_obj,
            submit_preflight_decision=submit_preflight_obj,
        )
        _canonical_write(candidate_path / 'execution_identity_record.v1.json', execution_identity_obj)
        _canonical_write(candidate_path.parent / 'attempt_state.v1.json', attempt_state_obj)

    return {
        'candidate_path': candidate_path,
        'plan_obj': plan_obj,
        'mapping_obj': mapping_obj,
        'binding_obj': binding_obj,
        'execution_identity_obj': execution_identity_obj,
        'submission_id': submission_id,
        'trade_instance_id': trade_instance_id,
    }


def build_execution_package_from_execution_intent_v1(*, repo_root: Path, execution_intent: ExecutionIntentV1) -> dict[str, Any]:
    staged = stage_candidate_from_execution_intent_v1(repo_root=repo_root, execution_intent=execution_intent)
    result = run_execution_build_authority_v1(
        repo_root=repo_root.resolve(),
        operation_type=execution_intent.operation_type,
        candidate_path=Path(staged['candidate_path']),
        materialize=False,
        emit_package=True,
    )
    if result['build_obj']['closure_status'] != 'COMPLETE' or result['package_obj'] is None or result['package_path'] is None:
        raise ValueError('EXECUTION_PACKAGE_BUILD_NOT_COMPLETE')
    stamped_package_obj = _stamp_advisory_submission_in_package_v1(
        repo_root=repo_root,
        package_path=Path(result['package_path']),
        execution_intent=execution_intent,
    )
    result['package_obj'] = stamped_package_obj
    return result


def handoff_to_existing_paper_trading_v1(
    *,
    repo_root: Path,
    execution_intent: ExecutionIntentV1,
    execution_package_path: Path,
    submission_record_path: Path,
    eval_time_utc: str,
    risk_budget_path: Path,
    ib_host: str,
    ib_port: int,
    ib_client_id: int,
    dry_run: bool,
    submissions_root_override: Path | None = None,
) -> int:
    from constellation_2.phaseD.lib.submit_boundary_paper_v4 import run_submit_boundary_paper_v4

    return run_submit_boundary_paper_v4(
        repo_root=repo_root.resolve(),
        eval_time_utc=eval_time_utc,
        phasec_out_dir=None,
        execution_package_path=execution_package_path.resolve(),
        submission_record_path=submission_record_path.resolve(),
        allow_legacy_raw_candidate=False,
        risk_budget_path=risk_budget_path.resolve(),
        ib_host=ib_host,
        ib_port=ib_port,
        ib_client_id=ib_client_id,
        ib_account=execution_intent.account_id,
        dry_run=dry_run,
        submissions_root_override=None if submissions_root_override is None else submissions_root_override.resolve(),
    )
