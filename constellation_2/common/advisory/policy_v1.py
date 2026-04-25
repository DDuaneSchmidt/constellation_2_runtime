from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISORY/policy.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class PolicySuitabilityProfileV1:
    risk_profile: str
    liquidity_profile: str
    time_horizon: str
    operating_mode: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'PolicySuitabilityProfileV1':
        return cls(
            str(obj['risk_profile']),
            str(obj['liquidity_profile']),
            str(obj['time_horizon']),
            str(obj['operating_mode']),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            'risk_profile': self.risk_profile,
            'liquidity_profile': self.liquidity_profile,
            'time_horizon': self.time_horizon,
            'operating_mode': self.operating_mode,
        }


@dataclass(frozen=True, slots=True)
class PolicyAutomationPermissionsV1:
    automation_mode: str
    requires_explicit_approval: bool

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'PolicyAutomationPermissionsV1':
        return cls(
            str(obj['automation_mode']),
            bool(obj['requires_explicit_approval']),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            'automation_mode': self.automation_mode,
            'requires_explicit_approval': self.requires_explicit_approval,
        }


@dataclass(frozen=True, slots=True)
class PolicyV1:
    schema_id: str
    schema_version: str
    record_id: str
    policy_id: str
    parent_intent_id: str
    household_id: str
    policy_version: str
    created_at: str
    effective_at: str
    actor_source: str
    timestamp_basis: str
    contract_version: str
    compiler_version: str
    policy_fingerprint: str
    assumption_manifest_refs: tuple[str, ...]
    regime_rule_refs: tuple[str, ...]
    parent_lineage_refs: tuple[str, ...]
    suitability_profile: PolicySuitabilityProfileV1
    liquidity_floor_rules: tuple[str, ...]
    concentration_cap_rules: tuple[str, ...]
    allowed_exposure_rules: tuple[str, ...]
    prohibited_exposure_rules: tuple[str, ...]
    account_treatment_rules: tuple[str, ...]
    rebalance_philosophy: str
    promotion_gate_rules: tuple[str, ...]
    allocation_policy_mode: str
    allocation_targets: tuple[dict[str, Any], ...]
    rebalance_threshold: str | None
    minimum_trade_value_cents: int | None
    automation_permissions: PolicyAutomationPermissionsV1
    approval_requirements: tuple[str, ...]
    policy_completeness_status: str
    validity_tier: str
    reason_codes: tuple[str, ...]
    unresolved_fields: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'PolicyV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['policy_id']),
            str(obj['parent_intent_id']),
            str(obj['household_id']),
            str(obj['policy_version']),
            str(obj['created_at']),
            str(obj['effective_at']),
            str(obj['actor_source']),
            str(obj['timestamp_basis']),
            str(obj['contract_version']),
            str(obj['compiler_version']),
            str(obj['policy_fingerprint']),
            tuple(str(item) for item in obj['assumption_manifest_refs']),
            tuple(str(item) for item in obj['regime_rule_refs']),
            tuple(str(item) for item in obj['parent_lineage_refs']),
            PolicySuitabilityProfileV1.from_dict(obj['suitability_profile']),
            tuple(str(item) for item in obj['liquidity_floor_rules']),
            tuple(str(item) for item in obj['concentration_cap_rules']),
            tuple(str(item) for item in obj['allowed_exposure_rules']),
            tuple(str(item) for item in obj['prohibited_exposure_rules']),
            tuple(str(item) for item in obj['account_treatment_rules']),
            str(obj['rebalance_philosophy']),
            tuple(str(item) for item in obj['promotion_gate_rules']),
            str(obj['allocation_policy_mode']),
            tuple(dict(item) for item in obj['allocation_targets']),
            None if obj.get('rebalance_threshold') is None else str(obj['rebalance_threshold']),
            None if obj.get('minimum_trade_value_cents') is None else int(obj['minimum_trade_value_cents']),
            PolicyAutomationPermissionsV1.from_dict(obj['automation_permissions']),
            tuple(str(item) for item in obj['approval_requirements']),
            str(obj['policy_completeness_status']),
            str(obj['validity_tier']),
            tuple(str(item) for item in obj['reason_codes']),
            tuple(str(item) for item in obj['unresolved_fields']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'PolicyV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'policy_id': self.policy_id,
            'parent_intent_id': self.parent_intent_id,
            'household_id': self.household_id,
            'policy_version': self.policy_version,
            'created_at': self.created_at,
            'effective_at': self.effective_at,
            'actor_source': self.actor_source,
            'timestamp_basis': self.timestamp_basis,
            'contract_version': self.contract_version,
            'compiler_version': self.compiler_version,
            'policy_fingerprint': self.policy_fingerprint,
            'assumption_manifest_refs': list(self.assumption_manifest_refs),
            'regime_rule_refs': list(self.regime_rule_refs),
            'parent_lineage_refs': list(self.parent_lineage_refs),
            'suitability_profile': self.suitability_profile.to_dict(),
            'liquidity_floor_rules': list(self.liquidity_floor_rules),
            'concentration_cap_rules': list(self.concentration_cap_rules),
            'allowed_exposure_rules': list(self.allowed_exposure_rules),
            'prohibited_exposure_rules': list(self.prohibited_exposure_rules),
            'account_treatment_rules': list(self.account_treatment_rules),
            'rebalance_philosophy': self.rebalance_philosophy,
            'promotion_gate_rules': list(self.promotion_gate_rules),
            'allocation_policy_mode': self.allocation_policy_mode,
            'allocation_targets': [dict(item) for item in self.allocation_targets],
            'rebalance_threshold': self.rebalance_threshold,
            'minimum_trade_value_cents': self.minimum_trade_value_cents,
            'automation_permissions': self.automation_permissions.to_dict(),
            'approval_requirements': list(self.approval_requirements),
            'policy_completeness_status': self.policy_completeness_status,
            'validity_tier': self.validity_tier,
            'reason_codes': list(self.reason_codes),
            'unresolved_fields': list(self.unresolved_fields),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
