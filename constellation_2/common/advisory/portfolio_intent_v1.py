from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_intent.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class PortfolioIntentV1:
    schema_id: str
    schema_version: str
    record_id: str
    portfolio_intent_id: str
    household_id: str
    parent_policy_id: str
    parent_household_snapshot_id: str
    intent_version: str
    created_at: str
    effective_at: str
    actor_source: str
    timestamp_basis: str
    contract_version: str
    builder_version: str
    parent_lineage_refs: tuple[str, ...]
    classification_refs: tuple[str, ...]
    target_allocation: dict[str, Any]
    current_allocation_summary: dict[str, Any]
    current_value_allocation: dict[str, Any]
    allocation_drift: dict[str, Any]
    required_directional_changes: tuple[dict[str, Any], ...]
    blocked_conditions: tuple[dict[str, Any], ...]
    constrained_deviations: tuple[dict[str, Any], ...]
    action_needed: bool
    execution_eligibility: str
    validity_tier: str
    reason_codes: tuple[str, ...]
    input_record_refs: tuple[str, ...]
    portfolio_intent_fingerprint: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'PortfolioIntentV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['portfolio_intent_id']),
            str(obj['household_id']),
            str(obj['parent_policy_id']),
            str(obj['parent_household_snapshot_id']),
            str(obj['intent_version']),
            str(obj['created_at']),
            str(obj['effective_at']),
            str(obj['actor_source']),
            str(obj['timestamp_basis']),
            str(obj['contract_version']),
            str(obj['builder_version']),
            tuple(str(item) for item in obj['parent_lineage_refs']),
            tuple(str(item) for item in obj['classification_refs']),
            dict(obj['target_allocation']),
            dict(obj['current_allocation_summary']),
            dict(obj['current_value_allocation']),
            dict(obj['allocation_drift']),
            tuple(dict(item) for item in obj['required_directional_changes']),
            tuple(dict(item) for item in obj['blocked_conditions']),
            tuple(dict(item) for item in obj['constrained_deviations']),
            bool(obj['action_needed']),
            str(obj['execution_eligibility']),
            str(obj['validity_tier']),
            tuple(str(item) for item in obj['reason_codes']),
            tuple(str(item) for item in obj['input_record_refs']),
            str(obj['portfolio_intent_fingerprint']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'PortfolioIntentV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'portfolio_intent_id': self.portfolio_intent_id,
            'household_id': self.household_id,
            'parent_policy_id': self.parent_policy_id,
            'parent_household_snapshot_id': self.parent_household_snapshot_id,
            'intent_version': self.intent_version,
            'created_at': self.created_at,
            'effective_at': self.effective_at,
            'actor_source': self.actor_source,
            'timestamp_basis': self.timestamp_basis,
            'contract_version': self.contract_version,
            'builder_version': self.builder_version,
            'parent_lineage_refs': list(self.parent_lineage_refs),
            'classification_refs': list(self.classification_refs),
            'target_allocation': self.target_allocation,
            'current_allocation_summary': self.current_allocation_summary,
            'current_value_allocation': self.current_value_allocation,
            'allocation_drift': self.allocation_drift,
            'required_directional_changes': [dict(item) for item in self.required_directional_changes],
            'blocked_conditions': [dict(item) for item in self.blocked_conditions],
            'constrained_deviations': [dict(item) for item in self.constrained_deviations],
            'action_needed': self.action_needed,
            'execution_eligibility': self.execution_eligibility,
            'validity_tier': self.validity_tier,
            'reason_codes': list(self.reason_codes),
            'input_record_refs': list(self.input_record_refs),
            'portfolio_intent_fingerprint': self.portfolio_intent_fingerprint,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
