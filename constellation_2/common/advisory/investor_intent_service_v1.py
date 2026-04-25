from __future__ import annotations

from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.advisory.advisory_domain_constants_v1 import (
    ADVISORY_VALIDITY_TIERS_V1,
    APPROVAL_PREFERENCES_V1,
    AUTOMATION_PREFERENCES_V1,
    CONCENTRATION_PREFERENCES_V1,
    INVESTOR_INTENT_CONTRACT_VERSION_V1,
    INVESTOR_INTENT_NORMALIZER_VERSION_V1,
    LIQUIDITY_REQUIREMENTS_V1,
    OPERATING_MODES_V1,
    RISK_PREFERENCES_V1,
    TIME_HORIZONS_V1,
)
from constellation_2.common.advisory.advisory_storage_v1 import investor_intent_path_v1, write_immutable_json_v1
from constellation_2.common.advisory.investor_intent_v1 import (
    InvestorIntentApprovalPreferencesV1,
    InvestorIntentConstraintsV1,
    InvestorIntentV1,
)

_ALLOWED_TOP_LEVEL_KEYS = {
    'household_id',
    'intent_version',
    'created_at',
    'effective_at',
    'actor_source',
    'goals',
    'constraints',
    'operating_mode',
    'approval_preferences',
    'compiler_inputs_manifest_refs',
    'lineage_parent_refs',
}

_ALLOWED_CONSTRAINT_KEYS = {
    'time_horizon',
    'risk_preference',
    'liquidity_requirement',
    'concentration_preference',
    'prohibited_exposures',
    'account_role_preferences',
}

_ALLOWED_APPROVAL_KEYS = {
    'automation_preference',
    'approval_preference',
}

_CONTRADICTORY_GOAL_SETS = (
    frozenset({'maximize_growth', 'preserve_capital'}),
    frozenset({'maximize_income', 'minimize_withdrawals'}),
)


def _require_str(obj: dict[str, Any], field: str) -> str:
    value = obj.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field.upper()}_REQUIRED')
    return value.strip()


def _normalize_string_list(value: Any, *, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f'{field.upper()}_NOT_ARRAY')
    items: list[str] = []
    for raw in value:
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError(f'{field.upper()}_ITEM_INVALID')
        items.append(raw.strip().lower())
    return tuple(sorted(set(items)))


def _normalize_optional_enum(value: Any, *, field: str, allowed: set[str]) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field.upper()}_INVALID')
    normalized = value.strip().lower()
    if normalized not in allowed:
        raise ValueError(f'UNSUPPORTED_{field.upper()}')
    return normalized


def _normalize_constraints(obj: Any) -> InvestorIntentConstraintsV1:
    if obj is None:
        obj = {}
    if not isinstance(obj, dict):
        raise ValueError('CONSTRAINTS_NOT_OBJECT')
    extra = sorted(set(obj.keys()) - _ALLOWED_CONSTRAINT_KEYS)
    if extra:
        raise ValueError(f'UNSUPPORTED_CONSTRAINT_FIELDS:{",".join(extra)}')
    prohibited_exposures = _normalize_string_list(obj.get('prohibited_exposures'), field='prohibited_exposures')
    account_role_preferences = _normalize_string_list(obj.get('account_role_preferences'), field='account_role_preferences')
    return InvestorIntentConstraintsV1(
        time_horizon=_normalize_optional_enum(obj.get('time_horizon'), field='time_horizon', allowed=TIME_HORIZONS_V1),
        risk_preference=_normalize_optional_enum(obj.get('risk_preference'), field='risk_preference', allowed=RISK_PREFERENCES_V1),
        liquidity_requirement=_normalize_optional_enum(obj.get('liquidity_requirement'), field='liquidity_requirement', allowed=LIQUIDITY_REQUIREMENTS_V1),
        concentration_preference=_normalize_optional_enum(obj.get('concentration_preference'), field='concentration_preference', allowed=CONCENTRATION_PREFERENCES_V1),
        prohibited_exposures=prohibited_exposures,
        account_role_preferences=account_role_preferences,
    )


def _normalize_approval_preferences(obj: Any) -> InvestorIntentApprovalPreferencesV1:
    if obj is None:
        obj = {}
    if not isinstance(obj, dict):
        raise ValueError('APPROVAL_PREFERENCES_NOT_OBJECT')
    extra = sorted(set(obj.keys()) - _ALLOWED_APPROVAL_KEYS)
    if extra:
        raise ValueError(f'UNSUPPORTED_APPROVAL_PREFERENCE_FIELDS:{",".join(extra)}')
    return InvestorIntentApprovalPreferencesV1(
        automation_preference=_normalize_optional_enum(obj.get('automation_preference'), field='automation_preference', allowed=AUTOMATION_PREFERENCES_V1),
        approval_preference=_normalize_optional_enum(obj.get('approval_preference'), field='approval_preference', allowed=APPROVAL_PREFERENCES_V1),
    )


def _reason_code_for_missing(field: str) -> str:
    return f'MISSING_{field.upper()}'


def build_investor_intent_v1(raw_intake: dict[str, Any]) -> InvestorIntentV1:
    if not isinstance(raw_intake, dict):
        raise ValueError('INVESTOR_INTENT_INPUT_NOT_OBJECT')
    extra = sorted(set(raw_intake.keys()) - _ALLOWED_TOP_LEVEL_KEYS)
    if extra:
        raise ValueError(f'UNSUPPORTED_INVESTOR_INTENT_FIELDS:{",".join(extra)}')

    household_id = _require_str(raw_intake, 'household_id')
    intent_version = _require_str(raw_intake, 'intent_version')
    created_at = _require_str(raw_intake, 'created_at')
    effective_at = _require_str(raw_intake, 'effective_at')
    actor_source = _require_str(raw_intake, 'actor_source')
    goals = _normalize_string_list(raw_intake.get('goals'), field='goals')
    constraints = _normalize_constraints(raw_intake.get('constraints'))
    operating_mode = _normalize_optional_enum(raw_intake.get('operating_mode'), field='operating_mode', allowed=OPERATING_MODES_V1)
    if operating_mode is None:
        raise ValueError('OPERATING_MODE_REQUIRED')
    approval_preferences = _normalize_approval_preferences(raw_intake.get('approval_preferences'))
    compiler_inputs_manifest_refs = _normalize_string_list(raw_intake.get('compiler_inputs_manifest_refs'), field='compiler_inputs_manifest_refs')
    lineage_parent_refs = _normalize_string_list(raw_intake.get('lineage_parent_refs'), field='lineage_parent_refs')

    for contradictory_set in _CONTRADICTORY_GOAL_SETS:
        if contradictory_set.issubset(set(goals)):
            raise ValueError('CONTRADICTORY_GOALS')

    unresolved_fields: list[str] = []
    reason_codes: list[str] = []
    if not goals:
        unresolved_fields.append('goals')
        reason_codes.append(_reason_code_for_missing('goals'))
    if constraints.time_horizon is None:
        unresolved_fields.append('time_horizon')
        reason_codes.append(_reason_code_for_missing('time_horizon'))
    if constraints.risk_preference is None:
        unresolved_fields.append('risk_preference')
        reason_codes.append(_reason_code_for_missing('risk_preference'))
    if constraints.liquidity_requirement is None:
        unresolved_fields.append('liquidity_requirement')
        reason_codes.append(_reason_code_for_missing('liquidity_requirement'))
    if constraints.concentration_preference is None:
        unresolved_fields.append('concentration_preference')
        reason_codes.append(_reason_code_for_missing('concentration_preference'))
    if approval_preferences.automation_preference is None:
        unresolved_fields.append('automation_preference')
        reason_codes.append(_reason_code_for_missing('automation_preference'))
    if approval_preferences.approval_preference is None:
        unresolved_fields.append('approval_preference')
        reason_codes.append(_reason_code_for_missing('approval_preference'))

    completeness_status = 'COMPLETE'
    validity_tier = 'VALID_ADVISORY_ONLY'
    if unresolved_fields:
        completeness_status = 'INCOMPLETE_REMEDIABLE'
        validity_tier = 'INVALID_REMEDIABLE'
    if validity_tier not in ADVISORY_VALIDITY_TIERS_V1:
        raise ValueError('INVALID_VALIDITY_TIER_CONFIGURATION')
    if not reason_codes:
        reason_codes.append('INVESTOR_INTENT_COMPLETE')

    intent_scope = {
        'household_id': household_id,
        'intent_version': intent_version,
        'effective_at': effective_at,
        'goals': list(goals),
        'constraints': constraints.to_dict(),
        'operating_mode': operating_mode,
        'approval_preferences': approval_preferences.to_dict(),
        'compiler_inputs_manifest_refs': list(compiler_inputs_manifest_refs),
        'lineage_parent_refs': list(lineage_parent_refs),
        'contract_version': INVESTOR_INTENT_CONTRACT_VERSION_V1,
        'normalizer_version': INVESTOR_INTENT_NORMALIZER_VERSION_V1,
        'completeness_status': completeness_status,
        'validity_tier': validity_tier,
        'reason_codes': reason_codes,
    }
    intent_id = canonical_sha256_hex_v1(intent_scope)

    obj = {
        'schema_id': 'investor_intent',
        'schema_version': 'v1',
        'record_id': intent_id,
        'intent_id': intent_id,
        'household_id': household_id,
        'intent_version': intent_version,
        'created_at': created_at,
        'effective_at': effective_at,
        'actor_source': actor_source,
        'timestamp_basis': 'created_at_and_effective_at',
        'contract_version': INVESTOR_INTENT_CONTRACT_VERSION_V1,
        'normalizer_version': INVESTOR_INTENT_NORMALIZER_VERSION_V1,
        'compiler_inputs_manifest_refs': list(compiler_inputs_manifest_refs),
        'lineage_parent_refs': list(lineage_parent_refs),
        'goals': list(goals),
        'constraints': constraints.to_dict(),
        'operating_mode': operating_mode,
        'approval_preferences': approval_preferences.to_dict(),
        'unresolved_fields': sorted(set(unresolved_fields)),
        'completeness_status': completeness_status,
        'validity_tier': validity_tier,
        'reason_codes': sorted(set(reason_codes)),
    }
    return InvestorIntentV1.from_dict(obj)


def write_investor_intent_v1(*, raw_intake: dict[str, Any], output_root: str) -> tuple[InvestorIntentV1, str]:
    intent = build_investor_intent_v1(raw_intake)
    path = investor_intent_path_v1(output_root, intent.household_id, intent.intent_id)
    written = write_immutable_json_v1(path, intent.to_dict())
    return intent, str(written)
