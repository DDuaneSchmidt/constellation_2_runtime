from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/REPORTS/decision_chain.v1.schema.json'


@dataclass(frozen=True, slots=True)
class DecisionChainV1:
    schema_id: str
    schema_version: str
    produced_utc: str
    run_id: str
    chain_id: str
    planning_snapshot_id: str
    advisory_packet_id: str
    decision_plan_id: str
    decision_plan_delta_id: str | None
    bridge_translation_id: str | None
    bridge_proposal_id: str | None
    promotion_candidate_id: str | None
    promotion_review_id: str | None
    promotion_manual_review_id: str | None
    promotion_gate_result_id: str | None
    chain_status: str
    source_artifact_refs: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'DecisionChainV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['produced_utc']),
            str(obj['run_id']),
            str(obj['chain_id']),
            str(obj['planning_snapshot_id']),
            str(obj['advisory_packet_id']),
            str(obj['decision_plan_id']),
            obj['decision_plan_delta_id'],
            obj['bridge_translation_id'],
            obj['bridge_proposal_id'],
            obj['promotion_candidate_id'],
            obj['promotion_review_id'],
            obj['promotion_manual_review_id'],
            obj['promotion_gate_result_id'],
            str(obj['chain_status']),
            tuple(str(item) for item in obj['source_artifact_refs']),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'produced_utc': self.produced_utc,
            'run_id': self.run_id,
            'chain_id': self.chain_id,
            'planning_snapshot_id': self.planning_snapshot_id,
            'advisory_packet_id': self.advisory_packet_id,
            'decision_plan_id': self.decision_plan_id,
            'decision_plan_delta_id': self.decision_plan_delta_id,
            'bridge_translation_id': self.bridge_translation_id,
            'bridge_proposal_id': self.bridge_proposal_id,
            'promotion_candidate_id': self.promotion_candidate_id,
            'promotion_review_id': self.promotion_review_id,
            'promotion_manual_review_id': self.promotion_manual_review_id,
            'promotion_gate_result_id': self.promotion_gate_result_id,
            'chain_status': self.chain_status,
            'source_artifact_refs': list(self.source_artifact_refs),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj
