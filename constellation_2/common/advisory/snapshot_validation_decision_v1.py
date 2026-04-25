from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.common.advisory.advisory_domain_constants_v1 import (
    ADVISORY_VALIDITY_TIERS_V1,
    SNAPSHOT_VALIDATION_DECISION_CONTRACT_VERSION_V1,
    SNAPSHOT_VALIDATION_DECISION_VERSION_V1,
    SNAPSHOT_VALIDATION_OUTCOMES_V1,
)
from constellation_2.common.advisory.advisory_storage_v1 import (
    snapshot_validation_decision_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.advisory.household_snapshot_service_v1 import (
    build_household_snapshot_candidate_v1,
    evaluate_household_snapshot_candidate_v1,
)
from constellation_2.common.advisory.policy_v1 import PolicyV1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ADVISORY/snapshot_validation_decision.v1.schema.json'


def _read_json_obj(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


@dataclass(frozen=True, slots=True)
class SnapshotValidationDecisionV1:
    schema_id: str
    schema_version: str
    record_id: str
    snapshot_validation_decision_id: str
    household_id: str
    parent_policy_id: str
    produced_utc: str
    effective_at: str
    input_assembly_id: str
    contract_version: str
    decision_version: str
    outcome: str
    completeness_status: str
    freshness_status: str
    reconciliation_status: str
    validity_tier: str
    input_record_refs: tuple[str, ...]
    reason_codes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'SnapshotValidationDecisionV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['record_id']),
            str(obj['snapshot_validation_decision_id']),
            str(obj['household_id']),
            str(obj['parent_policy_id']),
            str(obj['produced_utc']),
            str(obj['effective_at']),
            str(obj['input_assembly_id']),
            str(obj['contract_version']),
            str(obj['decision_version']),
            str(obj['outcome']),
            str(obj['completeness_status']),
            str(obj['freshness_status']),
            str(obj['reconciliation_status']),
            str(obj['validity_tier']),
            tuple(str(item) for item in obj['input_record_refs']),
            tuple(str(item) for item in obj['reason_codes']),
        )

    @classmethod
    def load_file(cls, path: str | Path) -> 'SnapshotValidationDecisionV1':
        return cls.from_dict(_read_json_obj(Path(path).expanduser().resolve()))

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'record_id': self.record_id,
            'snapshot_validation_decision_id': self.snapshot_validation_decision_id,
            'household_id': self.household_id,
            'parent_policy_id': self.parent_policy_id,
            'produced_utc': self.produced_utc,
            'effective_at': self.effective_at,
            'input_assembly_id': self.input_assembly_id,
            'contract_version': self.contract_version,
            'decision_version': self.decision_version,
            'outcome': self.outcome,
            'completeness_status': self.completeness_status,
            'freshness_status': self.freshness_status,
            'reconciliation_status': self.reconciliation_status,
            'validity_tier': self.validity_tier,
            'input_record_refs': list(self.input_record_refs),
            'reason_codes': list(self.reason_codes),
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj


def build_snapshot_validation_decision_v1(
    *,
    policy: PolicyV1,
    produced_utc: str,
    effective_at: str,
    actor_source: str,
    account_registry_snapshot: dict[str, Any],
    verified_positions_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    verified_cash_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    external_holdings_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    classification_refs: list[str] | tuple[str, ...] = (),
) -> SnapshotValidationDecisionV1:
    del actor_source
    candidate = build_household_snapshot_candidate_v1(
        policy=policy,
        created_at=produced_utc,
        effective_at=effective_at,
        actor_source='snapshot_validation_candidate_builder',
        account_registry_snapshot=account_registry_snapshot,
        verified_positions_inputs=verified_positions_inputs,
        verified_cash_inputs=verified_cash_inputs,
        external_holdings_inputs=external_holdings_inputs,
        classification_refs=classification_refs,
    )
    evaluation = evaluate_household_snapshot_candidate_v1(candidate)
    outcome = str(evaluation['outcome'])
    validity_tier = str(evaluation['validity_tier'])
    if outcome not in SNAPSHOT_VALIDATION_OUTCOMES_V1:
        raise ValueError('SNAPSHOT_VALIDATION_OUTCOME_INVALID')
    if validity_tier not in ADVISORY_VALIDITY_TIERS_V1:
        raise ValueError('SNAPSHOT_VALIDITY_TIER_INVALID')

    obj = {
        'schema_id': 'snapshot_validation_decision',
        'schema_version': 'v1',
        'record_id': canonical_sha256_hex_v1(
            {
                'household_snapshot_id': candidate.household_snapshot_id,
                'outcome': outcome,
                'validity_tier': validity_tier,
                'reason_codes': sorted(set(str(item) for item in evaluation['reason_codes'])),
            }
        ),
        'snapshot_validation_decision_id': canonical_sha256_hex_v1(
            {
                'household_snapshot_id': candidate.household_snapshot_id,
                'outcome': outcome,
                'validity_tier': validity_tier,
            }
        ),
        'household_id': candidate.household_id,
        'parent_policy_id': candidate.parent_policy_id,
        'produced_utc': str(produced_utc),
        'effective_at': candidate.effective_at,
        'input_assembly_id': candidate.household_snapshot_id,
        'contract_version': SNAPSHOT_VALIDATION_DECISION_CONTRACT_VERSION_V1,
        'decision_version': SNAPSHOT_VALIDATION_DECISION_VERSION_V1,
        'outcome': outcome,
        'completeness_status': candidate.completeness_status,
        'freshness_status': candidate.freshness_status,
        'reconciliation_status': candidate.reconciliation_status,
        'validity_tier': validity_tier,
        'input_record_refs': list(candidate.input_record_refs),
        'reason_codes': sorted(set(str(item) for item in evaluation['reason_codes'])),
    }
    return SnapshotValidationDecisionV1.from_dict(obj)


def write_snapshot_validation_decision_v1(
    *,
    policy: PolicyV1,
    produced_utc: str,
    effective_at: str,
    actor_source: str,
    account_registry_snapshot: dict[str, Any],
    verified_positions_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    verified_cash_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    external_holdings_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    classification_refs: list[str] | tuple[str, ...] = (),
    output_root: str = '',
) -> tuple[SnapshotValidationDecisionV1, str]:
    decision = build_snapshot_validation_decision_v1(
        policy=policy,
        produced_utc=produced_utc,
        effective_at=effective_at,
        actor_source=actor_source,
        account_registry_snapshot=account_registry_snapshot,
        verified_positions_inputs=verified_positions_inputs,
        verified_cash_inputs=verified_cash_inputs,
        external_holdings_inputs=external_holdings_inputs,
        classification_refs=classification_refs,
    )
    path = snapshot_validation_decision_path_v1(output_root, decision.household_id, decision.snapshot_validation_decision_id)
    written = write_immutable_json_v1(path, decision.to_dict())
    return decision, str(written)
