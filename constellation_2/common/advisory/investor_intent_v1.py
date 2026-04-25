from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISORY/investor_intent.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class InvestorIntentConstraintsV1:
    time_horizon: str | None
    risk_preference: str | None
    liquidity_requirement: str | None
    concentration_preference: str | None
    prohibited_exposures: tuple[str, ...]
    account_role_preferences: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'InvestorIntentConstraintsV1':
        return cls(
            None if obj['time_horizon'] is None else str(obj['time_horizon']),
            None if obj['risk_preference'] is None else str(obj['risk_preference']),
            None if obj['liquidity_requirement'] is None else str(obj['liquidity_requirement']),
            None if obj['concentration_preference'] is None else str(obj['concentration_preference']),
            tuple(str(item) for item in obj['prohibited_exposures']),
            tuple(str(item) for item in obj['account_role_preferences']),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            'time_horizon': self.time_horizon,
            'risk_preference': self.risk_preference,
            'liquidity_requirement': self.liquidity_requirement,
            'concentration_preference': self.concentration_preference,
            'prohibited_exposures': list(self.prohibited_exposures),
            'account_role_preferences': list(self.account_role_preferences),
        }


@dataclass(frozen=True, slots=True)
class InvestorIntentApprovalPreferencesV1:
    automation_preference: str | None
    approval_preference: str | None

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'InvestorIntentApprovalPreferencesV1':
        return cls(
            None if obj['automation_preference'] is None else str(obj['automation_preference']),
            None if obj['approval_preference'] is None else str(obj['approval_preference']),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            'automation_preference': self.automation_preference,
            'approval_preference': self.approval_preference,
        }


@dataclass(frozen=True, slots=True)
class InvestorIntentV1:
    schema_id: str
    schema_version: str
    record_id: str
    intent_id: str
    household_id: str
    intent_version: str
    created_at: str
    effective_at: str
    actor_source: str
    timestamp_basis: str
    contract_version: str
    normalizer_version: str
    compiler_inputs_manifest_refs: tuple[str, ...]
    lineage_parent_refs: tuple[str, ...]
    goals: tuple[str, ...]
    constraints: InvestorIntentConstraintsV1
    operating_mode: str
    approval_preferences: InvestorIntentApprovalPreferencesV1
    unresolved_fields: tuple[str, ...]
    completeness_status: str
    validity_tier: str
    reason_codes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'InvestorIntentV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['intent_id']),
            str(obj['household_id']),
            str(obj['intent_version']),
            str(obj['created_at']),
            str(obj['effective_at']),
            str(obj['actor_source']),
            str(obj['timestamp_basis']),
            str(obj['contract_version']),
            str(obj['normalizer_version']),
            tuple(str(item) for item in obj['compiler_inputs_manifest_refs']),
            tuple(str(item) for item in obj['lineage_parent_refs']),
            tuple(str(item) for item in obj['goals']),
            InvestorIntentConstraintsV1.from_dict(obj['constraints']),
            str(obj['operating_mode']),
            InvestorIntentApprovalPreferencesV1.from_dict(obj['approval_preferences']),
            tuple(str(item) for item in obj['unresolved_fields']),
            str(obj['completeness_status']),
            str(obj['validity_tier']),
            tuple(str(item) for item in obj['reason_codes']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'InvestorIntentV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'intent_id': self.intent_id,
            'household_id': self.household_id,
            'intent_version': self.intent_version,
            'created_at': self.created_at,
            'effective_at': self.effective_at,
            'actor_source': self.actor_source,
            'timestamp_basis': self.timestamp_basis,
            'contract_version': self.contract_version,
            'normalizer_version': self.normalizer_version,
            'compiler_inputs_manifest_refs': list(self.compiler_inputs_manifest_refs),
            'lineage_parent_refs': list(self.lineage_parent_refs),
            'goals': list(self.goals),
            'constraints': self.constraints.to_dict(),
            'operating_mode': self.operating_mode,
            'approval_preferences': self.approval_preferences.to_dict(),
            'unresolved_fields': list(self.unresolved_fields),
            'completeness_status': self.completeness_status,
            'validity_tier': self.validity_tier,
            'reason_codes': list(self.reason_codes),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
