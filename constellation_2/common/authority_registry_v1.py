from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.common.metadata_envelope_v1 import MetadataEnvelopeV1, artifact_base_v1

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/REPORTS/authority_registry.v1.schema.json'


@dataclass(frozen=True, slots=True)
class AuthorityRegistryRowV1:
    artifact_family: str
    owner_plane: str
    write_root: str
    authority_class: str
    publication_required: bool
    promotion_required: bool
    replay_expected: bool
    downstream_consumers: tuple[str, ...]
    notes: tuple[str, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'AuthorityRegistryRowV1':
        return cls(
            str(obj['artifact_family']),
            str(obj['owner_plane']),
            str(obj['write_root']),
            str(obj['authority_class']),
            bool(obj['publication_required']),
            bool(obj['promotion_required']),
            bool(obj['replay_expected']),
            tuple(str(item) for item in obj['downstream_consumers']),
            tuple(str(item) for item in obj['notes']),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            'artifact_family': self.artifact_family,
            'owner_plane': self.owner_plane,
            'write_root': self.write_root,
            'authority_class': self.authority_class,
            'publication_required': self.publication_required,
            'promotion_required': self.promotion_required,
            'replay_expected': self.replay_expected,
            'downstream_consumers': list(self.downstream_consumers),
            'notes': list(self.notes),
        }


@dataclass(frozen=True, slots=True)
class AuthorityRegistryV1:
    schema_id: str
    schema_version: str
    produced_utc: str
    run_id: str
    rows: tuple[AuthorityRegistryRowV1, ...]

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> 'AuthorityRegistryV1':
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return cls(
            str(obj['schema_id']),
            str(obj['schema_version']),
            str(obj['produced_utc']),
            str(obj['run_id']),
            tuple(AuthorityRegistryRowV1.from_dict(item) for item in obj['rows']),
        )

    def to_dict(self) -> dict[str, Any]:
        obj = {
            'schema_id': self.schema_id,
            'schema_version': self.schema_version,
            'produced_utc': self.produced_utc,
            'run_id': self.run_id,
            'rows': [item.to_dict() for item in self.rows],
        }
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return obj


def build_authority_registry(*, envelope: MetadataEnvelopeV1) -> AuthorityRegistryV1:
    rows = [
        {'artifact_family': 'action_intent_v1', 'owner_plane': 'decision_plane', 'write_root': 'canonical_execution_outputs', 'authority_class': 'action_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_execution'], 'notes': ['canonical_advisor_execution']},
        {'artifact_family': 'action_policy_pack_v1', 'owner_plane': 'decision_plane', 'write_root': 'canonical_execution_inputs', 'authority_class': 'policy_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_execution'], 'notes': ['canonical_advisor_execution']},
        {'artifact_family': 'advisor_trade_intent_proposal_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'advisory_only', 'publication_required': True, 'promotion_required': True, 'replay_expected': True, 'downstream_consumers': ['promotion_plane'], 'notes': ['not_executable_trading_truth']},
        {'artifact_family': 'advisor_trade_translation_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'review_authority', 'publication_required': True, 'promotion_required': True, 'replay_expected': True, 'downstream_consumers': ['advisor_trade_intent_proposal_v1', 'promotion_plane'], 'notes': ['bridge_review_artifact']},
        {'artifact_family': 'blocked_action_v1', 'owner_plane': 'decision_plane', 'write_root': 'canonical_execution_outputs', 'authority_class': 'action_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_trade_bridge', 'decision_chain'], 'notes': ['canonical_advisor_execution']},
        {'artifact_family': 'capability_schedule_v1', 'owner_plane': 'decision_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'policy_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_kernel'], 'notes': ['kernel_governance_artifact']},
        {'artifact_family': 'decision_action_v1', 'owner_plane': 'decision_plane', 'write_root': 'canonical_execution_outputs', 'authority_class': 'action_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_trade_bridge', 'decision_chain'], 'notes': ['canonical_advisor_execution']},
        {'artifact_family': 'decision_chain_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'report_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['operators', 'replay'], 'notes': ['cross_layer_chain_of_custody']},
        {'artifact_family': 'decision_plan_delta_v1', 'owner_plane': 'decision_plane', 'write_root': 'canonical_execution_outputs', 'authority_class': 'action_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['decision_chain'], 'notes': ['canonical_advisor_execution']},
        {'artifact_family': 'decision_plan_v1', 'owner_plane': 'decision_plane', 'write_root': 'canonical_execution_outputs', 'authority_class': 'action_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_trade_bridge', 'decision_chain'], 'notes': ['canonical_advisor_execution']},
        {'artifact_family': 'official_recommendation_set_v1', 'owner_plane': 'truth_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'recommendation_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_execution', 'decision_chain'], 'notes': ['advisor_input_truth']},
        {'artifact_family': 'planning_snapshot_v1', 'owner_plane': 'truth_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'fact_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_kernel', 'advisor_execution', 'advisor_trade_bridge', 'decision_chain'], 'notes': ['advisor_input_truth']},
        {'artifact_family': 'promotion_candidate_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'promotion_candidate_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['promotion_review_v1', 'decision_chain'], 'notes': ['never_executable_in_this_phase']},
        {'artifact_family': 'promotion_manual_review_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/<MODE>/promotion_manual_review_v1/<DAY>/', 'authority_class': 'recommendation_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['future promotion gate only'], 'notes': ['reviewed but non-executable promotion artifact']},
        {'artifact_family': 'promotion_gate_result_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/<MODE>/promotion_gate_result_v1/<DAY>/', 'authority_class': 'recommendation_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['future trading-intent promotion only'], 'notes': ['promotion-ready governance artifact, still non-executable']},
        {'artifact_family': 'promotion_review_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'promotion_review_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['decision_chain', 'operators'], 'notes': ['never_approved_in_this_phase']},
        {'artifact_family': 'publication_gate_result_v1', 'owner_plane': 'decision_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'policy_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_kernel'], 'notes': ['kernel_governance_artifact']},
        {'artifact_family': 'replay_manifest_v1', 'owner_plane': 'truth_plane', 'write_root': '/tmp/constellation_2_foundation/reports', 'authority_class': 'report_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['replay'], 'notes': ['traceability_artifact']},
        {'artifact_family': 'runtime_trace_bundle_v1', 'owner_plane': 'truth_plane', 'write_root': '/tmp/constellation_2_foundation/reports', 'authority_class': 'report_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['operators', 'replay'], 'notes': ['traceability_artifact']},
        {'artifact_family': 'semantic_reconciliation_report_v1', 'owner_plane': 'decision_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'policy_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_kernel', 'decision_chain'], 'notes': ['kernel_governance_artifact']},
    ]
    rows_sorted = sorted(rows, key=lambda item: item['artifact_family'])
    obj = artifact_base_v1(schema_id='authority_registry', envelope=envelope)
    obj['rows'] = rows_sorted
    return AuthorityRegistryV1.from_dict(obj)
