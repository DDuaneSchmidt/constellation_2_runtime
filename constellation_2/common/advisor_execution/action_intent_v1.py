from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.common.advisor_execution.blocked_action_v1 import BlockedActionV1
from constellation_2.common.advisor_execution.decision_action_v1 import DecisionActionV1
from constellation_2.common.advisor_execution.action_policy_pack_v1 import ReplanTriggerRuleV1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_EXECUTION/action_intent.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class ActionIntentV1:
    action_intent_id: str
    planning_snapshot_id: str
    advisory_packet_id: str
    policy_pack_id: str
    actions: tuple[DecisionActionV1, ...]
    blocked_actions: tuple[BlockedActionV1, ...]
    replan_triggers: tuple[ReplanTriggerRuleV1, ...]
    version: str

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'ActionIntentV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            action_intent_id=str(obj['action_intent_id']),
            planning_snapshot_id=str(obj['planning_snapshot_id']),
            advisory_packet_id=str(obj['advisory_packet_id']),
            policy_pack_id=str(obj['policy_pack_id']),
            actions=tuple(DecisionActionV1.from_dict(item) for item in obj['actions']),
            blocked_actions=tuple(BlockedActionV1.from_dict(item) for item in obj['blocked_actions']),
            replan_triggers=tuple(ReplanTriggerRuleV1.from_dict(item) for item in obj['replan_triggers']),
            version=str(obj['version']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'ActionIntentV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': 'action_intent',
            'schema_version': 'v1',
            'action_intent_id': self.action_intent_id,
            'planning_snapshot_id': self.planning_snapshot_id,
            'advisory_packet_id': self.advisory_packet_id,
            'policy_pack_id': self.policy_pack_id,
            'actions': [item.to_dict() for item in self.actions],
            'blocked_actions': [item.to_dict() for item in self.blocked_actions],
            'replan_triggers': [item.to_dict() for item in self.replan_triggers],
            'version': self.version,
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
