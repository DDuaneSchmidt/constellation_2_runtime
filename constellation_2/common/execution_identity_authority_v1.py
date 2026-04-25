from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1

EXECUTION_IDENTITY_AUTHORITY_OWNER = 'execution_identity_authority_v1'
IDENTITY_MODE_LEGACY_BINDING_HASH_V1 = 'LEGACY_BINDING_HASH_V1'
IDENTITY_MODE_TRADE_INSTANCE_V1 = 'TRADE_INSTANCE_V1'

DUPLICATE_CLASSIFICATION_FIRST_INSTANCE = 'FIRST_INSTANCE'
DUPLICATE_CLASSIFICATION_SAME_INSTANCE_REPLAY = 'SAME_INSTANCE_REPLAY'
DUPLICATE_CLASSIFICATION_NEW_INSTANCE_SAME_PLAN = 'NEW_INSTANCE_SAME_PLAN'
DUPLICATE_CLASSIFICATION_NEW_INSTANCE_NEW_PLAN = 'NEW_INSTANCE_NEW_PLAN'


@dataclass(frozen=True)
class ResolvedSubmissionIdentityV1:
    identity_mode: str
    intent_id: Optional[str]
    plan_hash: str
    trade_instance_id: Optional[str]
    submission_id: str


def _require_nonempty_str(name: str, value: Any) -> str:
    text = str(value or '').strip()
    if not text:
        raise ValueError(f'EXECUTION_IDENTITY_{name}_MISSING')
    return text


def _require_hex64(name: str, value: Any) -> str:
    text = _require_nonempty_str(name, value)
    if len(text) != 64 or any(ch not in '0123456789abcdef' for ch in text):
        raise ValueError(f'EXECUTION_IDENTITY_{name}_INVALID')
    return text


def resolve_intent_id_v1(*, intent_obj: Dict[str, Any], plan_obj: Optional[Dict[str, Any]] = None) -> str:
    intent_id = str(intent_obj.get('intent_id') or '').strip()
    if intent_id:
        return intent_id
    if isinstance(plan_obj, dict):
        plan_intent_id = str(plan_obj.get('source_intent_id') or '').strip()
        if plan_intent_id:
            return plan_intent_id
    raise ValueError('EXECUTION_IDENTITY_INTENT_ID_MISSING')


def derive_trade_instance_id_v1(*, day_utc: str, attempt_id: str, sleeve_id: str, environment: str, intent_id: str, intent_hash: str) -> str:
    payload = {
        'authority_owner': EXECUTION_IDENTITY_AUTHORITY_OWNER,
        'identity_type': 'trade_instance_id',
        'day_utc': _require_nonempty_str('DAY_UTC', day_utc),
        'attempt_id': _require_nonempty_str('ATTEMPT_ID', attempt_id),
        'sleeve_id': _require_nonempty_str('SLEEVE_ID', sleeve_id),
        'environment': _require_nonempty_str('ENVIRONMENT', environment).upper(),
        'intent_id': _require_nonempty_str('INTENT_ID', intent_id),
        'intent_hash': _require_hex64('INTENT_HASH', intent_hash),
    }
    return canonical_hash_for_c2_artifact_v1(payload)


def derive_submission_id_v1(*, intent_id: str, plan_hash: str, trade_instance_id: str) -> str:
    payload = {
        'authority_owner': EXECUTION_IDENTITY_AUTHORITY_OWNER,
        'identity_type': 'submission_id',
        'intent_id': _require_nonempty_str('INTENT_ID', intent_id),
        'plan_hash': _require_hex64('PLAN_HASH', plan_hash),
        'trade_instance_id': _require_hex64('TRADE_INSTANCE_ID', trade_instance_id),
    }
    return canonical_hash_for_c2_artifact_v1(payload)


def classify_duplicate_classification_v1(*, prior_trade_instance_id: Optional[str], prior_plan_hash: Optional[str], current_trade_instance_id: str, current_plan_hash: str) -> str:
    trade_instance_id = _require_hex64('TRADE_INSTANCE_ID', current_trade_instance_id)
    plan_hash = _require_hex64('PLAN_HASH', current_plan_hash)
    if not prior_trade_instance_id and not prior_plan_hash:
        return DUPLICATE_CLASSIFICATION_FIRST_INSTANCE
    if prior_trade_instance_id and prior_trade_instance_id == trade_instance_id:
        return DUPLICATE_CLASSIFICATION_SAME_INSTANCE_REPLAY
    if prior_plan_hash and prior_plan_hash == plan_hash:
        return DUPLICATE_CLASSIFICATION_NEW_INSTANCE_SAME_PLAN
    return DUPLICATE_CLASSIFICATION_NEW_INSTANCE_NEW_PLAN


def build_execution_identity_record_v1(*, created_at_utc: str, day_utc: str, attempt_id: str, sleeve_id: str, environment: str, intent_id: str, intent_hash: str, plan_hash: str, binding_hash: str, trade_instance_id: str, submission_id: str, duplicate_classification: str, source_refs: list[Dict[str, str]]) -> Dict[str, Any]:
    record = {
        'schema_id': 'execution_identity_record',
        'schema_version': 'v1',
        'authority_owner': EXECUTION_IDENTITY_AUTHORITY_OWNER,
        'created_at_utc': _require_nonempty_str('CREATED_AT_UTC', created_at_utc),
        'day_utc': _require_nonempty_str('DAY_UTC', day_utc),
        'attempt_id': _require_nonempty_str('ATTEMPT_ID', attempt_id),
        'sleeve_id': _require_nonempty_str('SLEEVE_ID', sleeve_id),
        'environment': _require_nonempty_str('ENVIRONMENT', environment).upper(),
        'intent_id': _require_nonempty_str('INTENT_ID', intent_id),
        'intent_hash': _require_hex64('INTENT_HASH', intent_hash),
        'plan_hash': _require_hex64('PLAN_HASH', plan_hash),
        'binding_hash': _require_hex64('BINDING_HASH', binding_hash),
        'trade_instance_id': _require_hex64('TRADE_INSTANCE_ID', trade_instance_id),
        'submission_id': _require_hex64('SUBMISSION_ID', submission_id),
        'duplicate_classification': _require_nonempty_str('DUPLICATE_CLASSIFICATION', duplicate_classification),
        'source_refs': list(source_refs),
        'canonical_json_hash': None,
    }
    record['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(record)
    return record


def resolve_submission_identity_v1(*, plan_obj: Dict[str, Any], binding_obj: Dict[str, Any], binding_hash: str, execution_identity_obj: Optional[Dict[str, Any]] = None) -> ResolvedSubmissionIdentityV1:
    plan_hash = _require_hex64(
        'PLAN_HASH',
        binding_obj.get('plan_hash') or plan_obj.get('canonical_json_hash') or canonical_hash_for_c2_artifact_v1(plan_obj),
    )

    trade_instance_id = str(binding_obj.get('trade_instance_id') or '').strip()
    submission_id = str(binding_obj.get('submission_id') or '').strip()
    intent_id = str(binding_obj.get('intent_id') or plan_obj.get('source_intent_id') or '').strip() or None

    if trade_instance_id or submission_id:
        resolved_intent_id = _require_nonempty_str('INTENT_ID', intent_id)
        resolved_trade_instance_id = _require_hex64('TRADE_INSTANCE_ID', trade_instance_id)
        resolved_submission_id = _require_hex64('SUBMISSION_ID', submission_id)
        expected_submission_id = derive_submission_id_v1(
            intent_id=resolved_intent_id,
            plan_hash=plan_hash,
            trade_instance_id=resolved_trade_instance_id,
        )
        if resolved_submission_id != expected_submission_id:
            raise ValueError('EXECUTION_IDENTITY_SUBMISSION_ID_MISMATCH')
        if execution_identity_obj is not None:
            if _require_hex64('EXECUTION_IDENTITY_BINDING_HASH', execution_identity_obj.get('binding_hash')) != _require_hex64('BINDING_HASH', binding_hash):
                raise ValueError('EXECUTION_IDENTITY_BINDING_HASH_MISMATCH')
            if _require_hex64('EXECUTION_IDENTITY_PLAN_HASH', execution_identity_obj.get('plan_hash')) != plan_hash:
                raise ValueError('EXECUTION_IDENTITY_PLAN_HASH_MISMATCH')
            if _require_hex64('EXECUTION_IDENTITY_TRADE_INSTANCE_ID', execution_identity_obj.get('trade_instance_id')) != resolved_trade_instance_id:
                raise ValueError('EXECUTION_IDENTITY_TRADE_INSTANCE_ID_MISMATCH')
            if _require_hex64('EXECUTION_IDENTITY_SUBMISSION_ID', execution_identity_obj.get('submission_id')) != resolved_submission_id:
                raise ValueError('EXECUTION_IDENTITY_SUBMISSION_ID_RECORD_MISMATCH')
            record_intent_id = _require_nonempty_str('EXECUTION_IDENTITY_INTENT_ID', execution_identity_obj.get('intent_id'))
            if record_intent_id != resolved_intent_id:
                raise ValueError('EXECUTION_IDENTITY_INTENT_ID_MISMATCH')
        return ResolvedSubmissionIdentityV1(
            identity_mode=IDENTITY_MODE_TRADE_INSTANCE_V1,
            intent_id=resolved_intent_id,
            plan_hash=plan_hash,
            trade_instance_id=resolved_trade_instance_id,
            submission_id=resolved_submission_id,
        )

    legacy_submission_id = _require_hex64('BINDING_HASH', binding_hash)
    return ResolvedSubmissionIdentityV1(
        identity_mode=IDENTITY_MODE_LEGACY_BINDING_HASH_V1,
        intent_id=intent_id,
        plan_hash=plan_hash,
        trade_instance_id=None,
        submission_id=legacy_submission_id,
    )
