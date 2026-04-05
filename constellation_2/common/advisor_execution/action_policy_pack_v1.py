from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_EXECUTION/action_policy_pack.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class ActionSizingRuleV1:
    rule_id: str
    domain_id: str
    action_type: str
    rule_key: str
    annual_amount_mode: str
    periodic_amount_mode: str
    periodicity: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ActionSizingRuleV1':
        return cls(**{k: str(obj[k]) for k in ['rule_id', 'domain_id', 'action_type', 'rule_key', 'annual_amount_mode', 'periodic_amount_mode', 'periodicity']})

    def to_dict(self) -> dict[str, Any]:
        return {
            'rule_id': self.rule_id,
            'domain_id': self.domain_id,
            'action_type': self.action_type,
            'rule_key': self.rule_key,
            'annual_amount_mode': self.annual_amount_mode,
            'periodic_amount_mode': self.periodic_amount_mode,
            'periodicity': self.periodicity,
        }


@dataclass(frozen=True, slots=True)
class ActionConstraintRuleV1:
    rule_id: str
    domain_id: str
    action_type: str
    constraint_code: str
    support_status: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ActionConstraintRuleV1':
        return cls(**{k: str(obj[k]) for k in ['rule_id', 'domain_id', 'action_type', 'constraint_code', 'support_status']})

    def to_dict(self) -> dict[str, Any]:
        return {
            'rule_id': self.rule_id,
            'domain_id': self.domain_id,
            'action_type': self.action_type,
            'constraint_code': self.constraint_code,
            'support_status': self.support_status,
        }


@dataclass(frozen=True, slots=True)
class ReplanTriggerRuleV1:
    trigger_id: str
    trigger_type: str
    description: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ReplanTriggerRuleV1':
        return cls(trigger_id=str(obj['trigger_id']), trigger_type=str(obj['trigger_type']), description=str(obj['description']))

    def to_dict(self) -> dict[str, Any]:
        return {'trigger_id': self.trigger_id, 'trigger_type': self.trigger_type, 'description': self.description}


@dataclass(frozen=True, slots=True)
class DomainActionMaturityV1:
    domain_id: str
    maturity: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'DomainActionMaturityV1':
        return cls(domain_id=str(obj['domain_id']), maturity=str(obj['maturity']))

    def to_dict(self) -> dict[str, Any]:
        return {'domain_id': self.domain_id, 'maturity': self.maturity}


@dataclass(frozen=True, slots=True)
class ActionPolicyPackV1:
    policy_pack_id: str
    created_at: str
    version: str
    action_sizing_rules: tuple[ActionSizingRuleV1, ...]
    action_constraint_rules: tuple[ActionConstraintRuleV1, ...]
    replan_trigger_rules: tuple[ReplanTriggerRuleV1, ...]
    domain_action_maturity: tuple[DomainActionMaturityV1, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ActionPolicyPackV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            policy_pack_id=str(obj['policy_pack_id']),
            created_at=str(obj['created_at']),
            version=str(obj['version']),
            action_sizing_rules=tuple(ActionSizingRuleV1.from_dict(item) for item in obj['action_sizing_rules']),
            action_constraint_rules=tuple(ActionConstraintRuleV1.from_dict(item) for item in obj['action_constraint_rules']),
            replan_trigger_rules=tuple(ReplanTriggerRuleV1.from_dict(item) for item in obj['replan_trigger_rules']),
            domain_action_maturity=tuple(DomainActionMaturityV1.from_dict(item) for item in obj['domain_action_maturity']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'ActionPolicyPackV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': 'action_policy_pack',
            'schema_version': 'v1',
            'policy_pack_id': self.policy_pack_id,
            'created_at': self.created_at,
            'version': self.version,
            'action_sizing_rules': [item.to_dict() for item in self.action_sizing_rules],
            'action_constraint_rules': [item.to_dict() for item in self.action_constraint_rules],
            'replan_trigger_rules': [item.to_dict() for item in self.replan_trigger_rules],
            'domain_action_maturity': [item.to_dict() for item in self.domain_action_maturity],
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
