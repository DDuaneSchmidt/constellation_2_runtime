from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.advisory.advisory_domain_constants_v1 import (
    POLICY_ALLOCATION_MODES_V1,
    POLICY_COMPILER_ACTOR_PREFIX_V1,
    POLICY_CONTRACT_VERSION_V1,
)
from constellation_2.common.advisory.advisory_storage_v1 import (
    load_assumption_manifest_by_id_v1,
    policy_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.advisory.assumption_manifest_v1 import AssumptionManifestV1
from constellation_2.common.advisory.investor_intent_v1 import InvestorIntentV1
from constellation_2.common.advisory.policy_v1 import PolicyAutomationPermissionsV1, PolicySuitabilityProfileV1, PolicyV1


def _normalize_refs(refs: list[str] | tuple[str, ...], *, field: str) -> tuple[str, ...]:
    if not refs:
        raise ValueError(f'{field.upper()}_REQUIRED')
    normalized = []
    for item in refs:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f'{field.upper()}_ITEM_INVALID')
        normalized.append(item.strip())
    return tuple(sorted(set(normalized)))


def _resolve_assumption_manifests_v1(*, output_root: str, assumption_manifest_refs: tuple[str, ...], compiler_version: str) -> tuple[AssumptionManifestV1, ...]:
    manifests = []
    manifest_type_map: dict[str, AssumptionManifestV1] = {}
    for ref in assumption_manifest_refs:
        try:
            manifest = load_assumption_manifest_by_id_v1(output_root, ref)
        except FileNotFoundError as exc:
            raise ValueError(f'ASSUMPTION_MANIFEST_NOT_FOUND:{ref}') from exc
        if manifest.status != 'ACTIVE':
            raise ValueError(f'ASSUMPTION_MANIFEST_NOT_ACTIVE:{ref}')
        if compiler_version not in manifest.compatible_compiler_versions:
            raise ValueError(f'ASSUMPTION_MANIFEST_INCOMPATIBLE_COMPILER:{ref}')
        if manifest.manifest_type in manifest_type_map:
            raise ValueError(f'DUPLICATE_ASSUMPTION_MANIFEST_TYPE:{manifest.manifest_type}')
        manifest_type_map[manifest.manifest_type] = manifest
        manifests.append(manifest)
    if 'policy_rule_interpretation' not in manifest_type_map:
        raise ValueError('POLICY_RULE_INTERPRETATION_MANIFEST_REQUIRED')
    return tuple(manifests)


def _policy_rule_manifest_v1(manifests: tuple[AssumptionManifestV1, ...]) -> AssumptionManifestV1:
    for manifest in manifests:
        if manifest.manifest_type == 'policy_rule_interpretation':
            return manifest
    raise ValueError('POLICY_RULE_INTERPRETATION_MANIFEST_REQUIRED')


def _normalize_weight_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field.upper()}_REQUIRED')
    try:
        numeric = Decimal(value.strip())
    except InvalidOperation as exc:
        raise ValueError(f'{field.upper()}_INVALID') from exc
    if numeric < Decimal('0') or numeric > Decimal('1'):
        raise ValueError(f'{field.upper()}_OUT_OF_RANGE')
    return str(numeric.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))


def _normalize_allocation_targets(
    allocation_targets: list[dict[str, object]] | tuple[dict[str, object], ...],
) -> tuple[dict[str, object], ...]:
    normalized: list[dict[str, object]] = []
    seen_symbols: set[str] = set()
    for idx, item in enumerate(allocation_targets):
        if not isinstance(item, dict):
            raise ValueError(f'ALLOCATION_TARGET_NOT_OBJECT:{idx}')
        symbol = str(item.get('symbol') or '').strip().upper()
        asset_type = str(item.get('asset_type') or '').strip().upper()
        currency = str(item.get('currency') or '').strip().upper()
        if not symbol:
            raise ValueError(f'ALLOCATION_TARGET_SYMBOL_REQUIRED:{idx}')
        if asset_type not in {'EQUITY', 'CASH'}:
            raise ValueError(f'ALLOCATION_TARGET_ASSET_TYPE_INVALID:{symbol}')
        if not currency:
            raise ValueError(f'ALLOCATION_TARGET_CURRENCY_REQUIRED:{symbol}')
        if asset_type == 'CASH' and symbol != 'CASH_USD':
            raise ValueError(f'ALLOCATION_TARGET_CASH_SYMBOL_INVALID:{symbol}')
        if symbol in seen_symbols:
            raise ValueError(f'DUPLICATE_ALLOCATION_TARGET_SYMBOL:{symbol}')
        seen_symbols.add(symbol)
        normalized.append(
            {
                'symbol': symbol,
                'asset_type': asset_type,
                'currency': currency,
                'target_weight': _normalize_weight_text(item.get('target_weight'), field=f'allocation_targets[{idx}].target_weight'),
            }
        )
    total = sum(Decimal(str(item['target_weight'])) for item in normalized)
    if abs(total - Decimal('1.000000')) > Decimal('0.000001'):
        raise ValueError('ALLOCATION_TARGETS_SUM_INVALID')
    return tuple(sorted(normalized, key=lambda item: (str(item['asset_type']), str(item['symbol']))))


def _normalize_rebalance_threshold(value: object) -> str:
    threshold = _normalize_weight_text(value, field='rebalance_threshold')
    if Decimal(threshold) <= Decimal('0'):
        raise ValueError('REBALANCE_THRESHOLD_INVALID')
    return threshold


def _normalize_minimum_trade_value_cents(value: object) -> int:
    if not isinstance(value, int) or value <= 0:
        raise ValueError('MINIMUM_TRADE_VALUE_CENTS_INVALID')
    return value


def build_policy_v1(
    *,
    investor_intent: InvestorIntentV1,
    compiler_version: str,
    assumption_manifest_refs: list[str] | tuple[str, ...],
    regime_rule_refs: list[str] | tuple[str, ...],
    output_root: str,
    allocation_targets: list[dict[str, object]] | tuple[dict[str, object], ...] = (),
    rebalance_threshold: str | None = None,
    minimum_trade_value_cents: int | None = None,
) -> PolicyV1:
    if investor_intent.completeness_status != 'COMPLETE':
        raise ValueError('PARENT_INTENT_NOT_COMPILABLE')
    if investor_intent.validity_tier != 'VALID_ADVISORY_ONLY':
        raise ValueError('PARENT_INTENT_NOT_VALID')
    if not isinstance(compiler_version, str) or not compiler_version.strip():
        raise ValueError('COMPILER_VERSION_REQUIRED')

    manifest_refs = _normalize_refs(list(assumption_manifest_refs), field='assumption_manifest_refs')
    regime_rules = _normalize_refs(list(regime_rule_refs), field='regime_rule_refs')
    compiler_version_clean = compiler_version.strip()
    manifests = _resolve_assumption_manifests_v1(
        output_root=output_root,
        assumption_manifest_refs=manifest_refs,
        compiler_version=compiler_version_clean,
    )
    rule_manifest = _policy_rule_manifest_v1(manifests)
    rule_payload = rule_manifest.assumption_payload

    automation_preference = investor_intent.approval_preferences.automation_preference
    approval_preference = investor_intent.approval_preferences.approval_preference
    if automation_preference is None or approval_preference is None:
        raise ValueError('PARENT_INTENT_APPROVALS_INCOMPLETE')
    if investor_intent.constraints.risk_preference is None:
        raise ValueError('PARENT_INTENT_RISK_INCOMPLETE')
    if investor_intent.constraints.liquidity_requirement is None:
        raise ValueError('PARENT_INTENT_LIQUIDITY_INCOMPLETE')
    if investor_intent.constraints.time_horizon is None:
        raise ValueError('PARENT_INTENT_TIME_HORIZON_INCOMPLETE')
    if investor_intent.constraints.concentration_preference is None:
        raise ValueError('PARENT_INTENT_CONCENTRATION_INCOMPLETE')
    if investor_intent.operating_mode != 'automation_allowed' and automation_preference == 'auto_with_approval':
        raise ValueError('OPERATING_MODE_AUTOMATION_CONFLICT')

    suitability_profile = PolicySuitabilityProfileV1(
        risk_profile=investor_intent.constraints.risk_preference,
        liquidity_profile=investor_intent.constraints.liquidity_requirement,
        time_horizon=investor_intent.constraints.time_horizon,
        operating_mode=investor_intent.operating_mode,
    )
    automation_permissions = PolicyAutomationPermissionsV1(
        automation_mode=automation_preference,
        requires_explicit_approval=(approval_preference == 'explicit_approval_required'),
    )
    liquidity_floor_rules = tuple(
        str(item) for item in rule_payload['liquidity_floor_rules_by_requirement'][investor_intent.constraints.liquidity_requirement]
    )
    concentration_cap_rules = tuple(
        str(item) for item in rule_payload['concentration_cap_rules_by_preference'][investor_intent.constraints.concentration_preference]
    )
    allowed_exposure_rules = tuple(
        str(item) for item in rule_payload['allowed_exposure_rules_by_risk'][investor_intent.constraints.risk_preference]
    )
    prohibited_exposure_rules = investor_intent.constraints.prohibited_exposures
    account_treatment_rules = tuple(f'account_role_preference:{item}' for item in investor_intent.constraints.account_role_preferences)
    rebalance_philosophy = str(rule_payload['rebalance_philosophy_by_operating_mode'][investor_intent.operating_mode])
    promotion_gate_rules = tuple(
        str(item) for item in rule_payload['promotion_gate_rules_by_operating_mode'][investor_intent.operating_mode]
    )
    approval_requirements = (approval_preference,)

    allocation_targets_norm: tuple[dict[str, object], ...]
    if allocation_targets:
        if rebalance_threshold is None or minimum_trade_value_cents is None:
            raise ValueError('ALLOCATION_POLICY_FIELDS_INCOMPLETE')
        allocation_policy_mode = 'TARGET_WEIGHTS'
        allocation_targets_norm = _normalize_allocation_targets(allocation_targets)
        rebalance_threshold_norm = _normalize_rebalance_threshold(rebalance_threshold)
        minimum_trade_value_cents_norm = _normalize_minimum_trade_value_cents(minimum_trade_value_cents)
    else:
        if rebalance_threshold is not None or minimum_trade_value_cents is not None:
            raise ValueError('ALLOCATION_POLICY_FIELDS_INCOMPLETE')
        allocation_policy_mode = 'EXIT_ONLY'
        allocation_targets_norm = ()
        rebalance_threshold_norm = None
        minimum_trade_value_cents_norm = None
    if allocation_policy_mode not in POLICY_ALLOCATION_MODES_V1:
        raise ValueError('ALLOCATION_POLICY_MODE_INVALID')

    compiled_rule_scope = {
        'suitability_profile': suitability_profile.to_dict(),
        'liquidity_floor_rules': list(liquidity_floor_rules),
        'concentration_cap_rules': list(concentration_cap_rules),
        'allowed_exposure_rules': list(allowed_exposure_rules),
        'prohibited_exposure_rules': list(prohibited_exposure_rules),
        'account_treatment_rules': list(account_treatment_rules),
        'rebalance_philosophy': rebalance_philosophy,
        'promotion_gate_rules': list(promotion_gate_rules),
        'allocation_policy_mode': allocation_policy_mode,
        'allocation_targets': [dict(item) for item in allocation_targets_norm],
        'rebalance_threshold': rebalance_threshold_norm,
        'minimum_trade_value_cents': minimum_trade_value_cents_norm,
        'automation_permissions': automation_permissions.to_dict(),
        'approval_requirements': list(approval_requirements),
    }
    policy_fingerprint = canonical_sha256_hex_v1(compiled_rule_scope)

    policy_scope = {
        'parent_intent_id': investor_intent.intent_id,
        'household_id': investor_intent.household_id,
        'compiler_version': compiler_version_clean,
        'policy_fingerprint': policy_fingerprint,
        'assumption_manifest_refs': list(manifest_refs),
        'regime_rule_refs': list(regime_rules),
        'compiled_rule_scope': compiled_rule_scope,
        'contract_version': POLICY_CONTRACT_VERSION_V1,
    }
    policy_id = canonical_sha256_hex_v1(policy_scope)

    obj = {
        'schema_id': 'policy',
        'schema_version': 'v1',
        'record_id': policy_id,
        'policy_id': policy_id,
        'parent_intent_id': investor_intent.intent_id,
        'household_id': investor_intent.household_id,
        'policy_version': compiler_version_clean,
        'created_at': investor_intent.created_at,
        'effective_at': investor_intent.effective_at,
        'actor_source': f'{POLICY_COMPILER_ACTOR_PREFIX_V1}:{compiler_version_clean}',
        'timestamp_basis': 'parent_investor_intent_timestamps',
        'contract_version': POLICY_CONTRACT_VERSION_V1,
        'compiler_version': compiler_version_clean,
        'policy_fingerprint': policy_fingerprint,
        'assumption_manifest_refs': list(manifest_refs),
        'regime_rule_refs': list(regime_rules),
        'parent_lineage_refs': [f'investor_intent_id:{investor_intent.intent_id}'],
        'suitability_profile': suitability_profile.to_dict(),
        'liquidity_floor_rules': list(liquidity_floor_rules),
        'concentration_cap_rules': list(concentration_cap_rules),
        'allowed_exposure_rules': list(allowed_exposure_rules),
        'prohibited_exposure_rules': list(prohibited_exposure_rules),
        'account_treatment_rules': list(account_treatment_rules),
        'rebalance_philosophy': rebalance_philosophy,
        'promotion_gate_rules': list(promotion_gate_rules),
        'allocation_policy_mode': allocation_policy_mode,
        'allocation_targets': [dict(item) for item in allocation_targets_norm],
        'rebalance_threshold': rebalance_threshold_norm,
        'minimum_trade_value_cents': minimum_trade_value_cents_norm,
        'automation_permissions': automation_permissions.to_dict(),
        'approval_requirements': list(approval_requirements),
        'policy_completeness_status': 'VALID',
        'validity_tier': 'VALID_ADVISORY_ONLY',
        'reason_codes': ['POLICY_COMPILED', 'ASSUMPTION_MANIFESTS_BOUND', 'POLICY_FINGERPRINT_BOUND'],
        'unresolved_fields': [],
    }
    return PolicyV1.from_dict(obj)


def write_policy_v1(
    *,
    investor_intent: InvestorIntentV1,
    compiler_version: str,
    assumption_manifest_refs: list[str] | tuple[str, ...],
    regime_rule_refs: list[str] | tuple[str, ...],
    output_root: str,
    allocation_targets: list[dict[str, object]] | tuple[dict[str, object], ...] = (),
    rebalance_threshold: str | None = None,
    minimum_trade_value_cents: int | None = None,
) -> tuple[PolicyV1, str]:
    policy = build_policy_v1(
        investor_intent=investor_intent,
        compiler_version=compiler_version,
        assumption_manifest_refs=assumption_manifest_refs,
        regime_rule_refs=regime_rule_refs,
        output_root=output_root,
        allocation_targets=allocation_targets,
        rebalance_threshold=rebalance_threshold,
        minimum_trade_value_cents=minimum_trade_value_cents,
    )
    path = policy_path_v1(output_root, policy.household_id, policy.policy_id)
    written = write_immutable_json_v1(path, policy.to_dict())
    return policy, str(written)
