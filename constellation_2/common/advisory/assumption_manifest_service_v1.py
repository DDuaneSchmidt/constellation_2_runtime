from __future__ import annotations

from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.advisory.advisory_domain_constants_v1 import (
    ASSUMPTION_MANIFEST_CONTRACT_VERSION_V1,
    ASSUMPTION_MANIFEST_STATUSES_V1,
    ASSUMPTION_MANIFEST_TYPES_V1,
    CONCENTRATION_PREFERENCES_V1,
    LIQUIDITY_REQUIREMENTS_V1,
    OPERATING_MODES_V1,
    RISK_PREFERENCES_V1,
)
from constellation_2.common.advisory.advisory_storage_v1 import (
    assumption_manifest_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.advisory.assumption_manifest_v1 import AssumptionManifestV1

_ALLOWED_TOP_LEVEL_KEYS = {
    'manifest_version',
    'manifest_type',
    'created_at',
    'effective_at',
    'actor_source',
    'compatible_compiler_versions',
    'assumption_payload',
    'reason_codes',
    'supersedes_manifest_id',
    'status',
}


def _require_str(obj: dict[str, Any], field: str) -> str:
    value = obj.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field.upper()}_REQUIRED')
    return value.strip()


def _normalize_string_list(value: Any, *, field: str, lower: bool = False) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f'{field.upper()}_REQUIRED')
    items: list[str] = []
    for raw in value:
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError(f'{field.upper()}_ITEM_INVALID')
        item = raw.strip()
        items.append(item.lower() if lower else item)
    return tuple(sorted(set(items)))


def _normalize_enum_keyed_rule_map(obj: Any, *, field: str, allowed_keys: set[str]) -> dict[str, list[str]]:
    if not isinstance(obj, dict):
        raise ValueError(f'{field.upper()}_NOT_OBJECT')
    keys = {str(key).strip().lower() for key in obj.keys()}
    if keys != allowed_keys:
        raise ValueError(f'{field.upper()}_KEYS_INVALID')
    normalized: dict[str, list[str]] = {}
    for key in sorted(allowed_keys):
        rules = _normalize_string_list(obj.get(key), field=f'{field}_{key}', lower=True)
        normalized[key] = list(rules)
    return normalized


def _normalize_enum_keyed_scalar_map(obj: Any, *, field: str, allowed_keys: set[str]) -> dict[str, str]:
    if not isinstance(obj, dict):
        raise ValueError(f'{field.upper()}_NOT_OBJECT')
    keys = {str(key).strip().lower() for key in obj.keys()}
    if keys != allowed_keys:
        raise ValueError(f'{field.upper()}_KEYS_INVALID')
    normalized: dict[str, str] = {}
    for key in sorted(allowed_keys):
        value = obj.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f'{field.upper()}_{key.upper()}_INVALID')
        normalized[key] = value.strip().lower()
    return normalized


def _normalize_payload(obj: Any, *, manifest_type: str) -> dict[str, Any]:
    if manifest_type != 'policy_rule_interpretation':
        raise ValueError('UNSUPPORTED_ASSUMPTION_MANIFEST_TYPE')
    if not isinstance(obj, dict):
        raise ValueError('ASSUMPTION_PAYLOAD_NOT_OBJECT')
    allowed = {
        'allowed_exposure_rules_by_risk',
        'concentration_cap_rules_by_preference',
        'liquidity_floor_rules_by_requirement',
        'rebalance_philosophy_by_operating_mode',
        'promotion_gate_rules_by_operating_mode',
    }
    extra = sorted(set(obj.keys()) - allowed)
    if extra:
        raise ValueError(f'UNSUPPORTED_ASSUMPTION_PAYLOAD_FIELDS:{",".join(extra)}')
    return {
        'allowed_exposure_rules_by_risk': _normalize_enum_keyed_rule_map(
            obj.get('allowed_exposure_rules_by_risk'),
            field='allowed_exposure_rules_by_risk',
            allowed_keys=RISK_PREFERENCES_V1,
        ),
        'concentration_cap_rules_by_preference': _normalize_enum_keyed_rule_map(
            obj.get('concentration_cap_rules_by_preference'),
            field='concentration_cap_rules_by_preference',
            allowed_keys=CONCENTRATION_PREFERENCES_V1,
        ),
        'liquidity_floor_rules_by_requirement': _normalize_enum_keyed_rule_map(
            obj.get('liquidity_floor_rules_by_requirement'),
            field='liquidity_floor_rules_by_requirement',
            allowed_keys=LIQUIDITY_REQUIREMENTS_V1,
        ),
        'rebalance_philosophy_by_operating_mode': _normalize_enum_keyed_scalar_map(
            obj.get('rebalance_philosophy_by_operating_mode'),
            field='rebalance_philosophy_by_operating_mode',
            allowed_keys=OPERATING_MODES_V1,
        ),
        'promotion_gate_rules_by_operating_mode': _normalize_enum_keyed_rule_map(
            obj.get('promotion_gate_rules_by_operating_mode'),
            field='promotion_gate_rules_by_operating_mode',
            allowed_keys=OPERATING_MODES_V1,
        ),
    }


def build_assumption_manifest_v1(raw_manifest: dict[str, Any]) -> AssumptionManifestV1:
    if not isinstance(raw_manifest, dict):
        raise ValueError('ASSUMPTION_MANIFEST_INPUT_NOT_OBJECT')
    extra = sorted(set(raw_manifest.keys()) - _ALLOWED_TOP_LEVEL_KEYS)
    if extra:
        raise ValueError(f'UNSUPPORTED_ASSUMPTION_MANIFEST_FIELDS:{",".join(extra)}')

    manifest_version = _require_str(raw_manifest, 'manifest_version')
    manifest_type = _require_str(raw_manifest, 'manifest_type').lower()
    if manifest_type not in ASSUMPTION_MANIFEST_TYPES_V1:
        raise ValueError('UNSUPPORTED_ASSUMPTION_MANIFEST_TYPE')
    created_at = _require_str(raw_manifest, 'created_at')
    effective_at = _require_str(raw_manifest, 'effective_at')
    actor_source = _require_str(raw_manifest, 'actor_source')
    compatible_compiler_versions = _normalize_string_list(
        raw_manifest.get('compatible_compiler_versions'),
        field='compatible_compiler_versions',
    )
    assumption_payload = _normalize_payload(raw_manifest.get('assumption_payload'), manifest_type=manifest_type)
    reason_codes_input = raw_manifest.get('reason_codes')
    if reason_codes_input is None:
        reason_codes_input = ['assumption_manifest_active']
    reason_codes = _normalize_string_list(reason_codes_input, field='reason_codes', lower=True)
    supersedes_manifest_id = raw_manifest.get('supersedes_manifest_id')
    if supersedes_manifest_id is not None:
        if not isinstance(supersedes_manifest_id, str) or not supersedes_manifest_id.strip():
            raise ValueError('SUPERSEDES_MANIFEST_ID_INVALID')
        supersedes_manifest_id = supersedes_manifest_id.strip()
    status = _require_str(raw_manifest, 'status').upper()
    if status not in ASSUMPTION_MANIFEST_STATUSES_V1:
        raise ValueError('UNSUPPORTED_ASSUMPTION_MANIFEST_STATUS')

    manifest_scope = {
        'manifest_version': manifest_version,
        'manifest_type': manifest_type,
        'effective_at': effective_at,
        'contract_version': ASSUMPTION_MANIFEST_CONTRACT_VERSION_V1,
        'compatible_compiler_versions': list(compatible_compiler_versions),
        'assumption_payload': assumption_payload,
        'supersedes_manifest_id': supersedes_manifest_id,
        'status': status,
    }
    manifest_id = canonical_sha256_hex_v1(manifest_scope)
    obj = {
        'schema_id': 'assumption_manifest',
        'schema_version': 'v1',
        'record_id': manifest_id,
        'assumption_manifest_id': manifest_id,
        'manifest_version': manifest_version,
        'manifest_type': manifest_type,
        'created_at': created_at,
        'effective_at': effective_at,
        'actor_source': actor_source,
        'timestamp_basis': 'created_at_and_effective_at',
        'contract_version': ASSUMPTION_MANIFEST_CONTRACT_VERSION_V1,
        'compatible_compiler_versions': list(compatible_compiler_versions),
        'assumption_payload': assumption_payload,
        'reason_codes': list(reason_codes),
        'supersedes_manifest_id': supersedes_manifest_id,
        'status': status,
    }
    return AssumptionManifestV1.from_dict(obj)


def write_assumption_manifest_v1(*, raw_manifest: dict[str, Any], output_root: str) -> tuple[AssumptionManifestV1, str]:
    manifest = build_assumption_manifest_v1(raw_manifest)
    path = assumption_manifest_path_v1(output_root, manifest.assumption_manifest_id)
    written = write_immutable_json_v1(path, manifest.to_dict())
    return manifest, str(written)
