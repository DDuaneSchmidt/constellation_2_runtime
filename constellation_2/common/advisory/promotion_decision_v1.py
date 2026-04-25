from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISORY/promotion_decision.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class PromotionDecisionV1:
    schema_id: str
    schema_version: str
    record_id: str
    promotion_decision_id: str
    household_id: str
    parent_policy_id: str
    parent_portfolio_intent_id: str
    created_at: str
    effective_at: str
    actor_source: str
    contract_version: str
    evaluator_version: str
    parent_lineage_refs: tuple[str, ...]
    approved_change_ids: tuple[str, ...]
    blocked_change_ids: tuple[str, ...]
    outcome: str
    reason_codes: tuple[str, ...]
    input_record_refs: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'PromotionDecisionV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['promotion_decision_id']),
            str(obj['household_id']),
            str(obj['parent_policy_id']),
            str(obj['parent_portfolio_intent_id']),
            str(obj['created_at']),
            str(obj['effective_at']),
            str(obj['actor_source']),
            str(obj['contract_version']),
            str(obj['evaluator_version']),
            tuple(str(item) for item in obj['parent_lineage_refs']),
            tuple(str(item) for item in obj['approved_change_ids']),
            tuple(str(item) for item in obj['blocked_change_ids']),
            str(obj['outcome']),
            tuple(str(item) for item in obj['reason_codes']),
            tuple(str(item) for item in obj['input_record_refs']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'PromotionDecisionV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'promotion_decision_id': self.promotion_decision_id,
            'household_id': self.household_id,
            'parent_policy_id': self.parent_policy_id,
            'parent_portfolio_intent_id': self.parent_portfolio_intent_id,
            'created_at': self.created_at,
            'effective_at': self.effective_at,
            'actor_source': self.actor_source,
            'contract_version': self.contract_version,
            'evaluator_version': self.evaluator_version,
            'parent_lineage_refs': list(self.parent_lineage_refs),
            'approved_change_ids': list(self.approved_change_ids),
            'blocked_change_ids': list(self.blocked_change_ids),
            'outcome': self.outcome,
            'reason_codes': list(self.reason_codes),
            'input_record_refs': list(self.input_record_refs),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
