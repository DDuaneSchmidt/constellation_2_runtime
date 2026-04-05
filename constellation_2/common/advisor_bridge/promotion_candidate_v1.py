from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISOR_BRIDGE/promotion_candidate.v1.schema.json'


@dataclass(frozen=True, slots=True)
class PromotionCandidateV1:
    schema_id: str
    schema_version: str
    produced_utc: str
    run_id: str
    candidate_id: str
    proposal_id: str
    planning_snapshot_id: str
    decision_plan_id: str
    candidate_status: str
    candidate_class: str
    source_account: str
    proposed_amount_cents: int
    periodicity: str
    source_artifact_refs: tuple[str, ...]
    notes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'PromotionCandidateV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(str(obj['schema_id']), str(obj['schema_version']), str(obj['produced_utc']), str(obj['run_id']), str(obj['candidate_id']), str(obj['proposal_id']), str(obj['planning_snapshot_id']), str(obj['decision_plan_id']), str(obj['candidate_status']), str(obj['candidate_class']), str(obj['source_account']), int(obj['proposed_amount_cents']), str(obj['periodicity']), tuple(str(item) for item in obj['source_artifact_refs']), tuple(str(item) for item in obj['notes']))

    def to_dict(self) -> dict[str, Any]:
        obj = {'schema_id': self.schema_id, 'schema_version': self.schema_version, 'produced_utc': self.produced_utc, 'run_id': self.run_id, 'candidate_id': self.candidate_id, 'proposal_id': self.proposal_id, 'planning_snapshot_id': self.planning_snapshot_id, 'decision_plan_id': self.decision_plan_id, 'candidate_status': self.candidate_status, 'candidate_class': self.candidate_class, 'source_account': self.source_account, 'proposed_amount_cents': self.proposed_amount_cents, 'periodicity': self.periodicity, 'source_artifact_refs': list(self.source_artifact_refs), 'notes': list(self.notes)}
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
