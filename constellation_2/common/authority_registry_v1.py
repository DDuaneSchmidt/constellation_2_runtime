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
        {'artifact_family': 'assumption_manifest_v1', 'owner_plane': 'advisory_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/authorities/assumption_manifest_v1/manifests/', 'authority_class': 'advisory_manifest_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['policy_v1'], 'notes': ['governed_assumption_authority', 'compiler_compatibility_explicit']},
        {'artifact_family': 'approved_change_set_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER/execution_kernel_v1/approved_change_sets/', 'authority_class': 'execution_set_input_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['multi_delta_execution_decision_v1', 'multi_delta_execution_record_v1'], 'notes': ['sole_governed_multi_change_input', 'promotion_lineage_and_value_basis_frozen']},
        {'artifact_family': 'execution_intent_v1', 'owner_plane': 'advisory_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/artifacts/execution_intent_v1/households/', 'authority_class': 'execution_bridge_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['execution_build_authority_v1', 'submit_boundary_paper_v4'], 'notes': ['only_advisory_execution_artifact', 'pure_promotion_record_delta_transform']},
        {'artifact_family': 'execution_set_intent_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER/execution_kernel_v1/execution_set_intents/', 'authority_class': 'execution_bridge_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['execution_intent_v1', 'execution_submission_decision_v1'], 'notes': ['sole_multi_delta_handoff_artifact', 'ordered_member_execution_intents_only']},
        {'artifact_family': 'execution_submission_decision_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER/execution_kernel_v1/submission_decisions/', 'authority_class': 'gate_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['execution_submission_record_v1', 'execution_run_envelope_v1'], 'notes': ['pure_execution_submission_gate', 'duplicate_detection_is_durable']},
        {'artifact_family': 'execution_submission_record_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER/execution_kernel_v1/submission_records/', 'authority_class': 'execution_submission_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['submit_boundary_paper_v4', 'execution_lifecycle_decision_v1', 'execution_state_record_v1', 'execution_run_envelope_v1'], 'notes': ['sole_pre_submit_authority', 'frozen_execution_package_required']},
        {'artifact_family': 'multi_delta_execution_decision_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER/execution_kernel_v1/multi_delta_execution_decisions/', 'authority_class': 'gate_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['multi_delta_execution_record_v1', 'execution_set_intent_v1'], 'notes': ['pure_set_level_execution_gate', 'duplicate_detection_is_durable']},
        {'artifact_family': 'multi_delta_execution_record_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER/execution_kernel_v1/multi_delta_execution_records/', 'authority_class': 'execution_set_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['execution_set_intent_v1', 'operators', 'replay'], 'notes': ['sole_multi_trade_execution_authority', 'membership_root_for_set_level_truth']},
        {'artifact_family': 'execution_lifecycle_decision_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER/execution_kernel_v1/lifecycle_decisions/', 'authority_class': 'gate_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['execution_state_record_v1', 'execution_lifecycle_run_envelope_v1'], 'notes': ['pure_lifecycle_transition_gate', 'downstream_evidence_only']},
        {'artifact_family': 'execution_state_record_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER/execution_kernel_v1/execution_state_records/', 'authority_class': 'execution_state_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['operators', 'replay', 'reconciled_trade_state_v1'], 'notes': ['sole_lifecycle_truth_surface', 'derived_from_authoritative_execution_evidence']},
        {'artifact_family': 'execution_lifecycle_run_envelope_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER/execution_kernel_v1/lifecycle_run_envelopes/', 'authority_class': 'audit_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['operators', 'replay'], 'notes': ['mandatory_for_every_lifecycle_processing_run', 'links_lifecycle_decision_and_state_truth']},
        {'artifact_family': 'execution_run_envelope_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER/execution_kernel_v1/run_envelopes/', 'authority_class': 'audit_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['operators', 'replay'], 'notes': ['mandatory_for_every_execution_kernel_run', 'submission_and_downstream_evidence_linked']},
        {'artifact_family': 'runtime_control_decision_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth/runtime_control_kernel_v1/decisions/', 'authority_class': 'gate_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['runtime_control_record_v1', 'runtime_control_run_envelope_v1'], 'notes': ['pure_runtime_control_gate', 'kill_switch_and_readiness_only']},
        {'artifact_family': 'runtime_control_record_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth/runtime_control_kernel_v1/records/', 'authority_class': 'runtime_control_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['submit_boundary_paper_v4', 'run_submit_boundary_status_v1'], 'notes': ['sole_runtime_allow_block_truth', 'kill_switch_and_readiness_evidence_only']},
        {'artifact_family': 'runtime_control_run_envelope_v1', 'owner_plane': 'execution_plane', 'write_root': '/home/node/constellation_runtime_data/truth/runtime_control_kernel_v1/run_envelopes/', 'authority_class': 'audit_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['operators', 'replay'], 'notes': ['mandatory_for_every_runtime_control_evaluation', 'links_decision_and_control_record']},
        {'artifact_family': 'household_snapshot_v1', 'owner_plane': 'advisory_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/authorities/household_snapshot_v1/households/', 'authority_class': 'advisory_household_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['portfolio_intent_v1'], 'notes': ['frozen_household_authority', 'verified_unverified_separation_required']},
        {'artifact_family': 'investor_intent_v1', 'owner_plane': 'advisory_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/authorities/investor_intent_v1/households/', 'authority_class': 'advisory_top_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['policy_v1', 'future household snapshot compiler'], 'notes': ['top_of_stack_advisory_authority', 'no_recommendation_dependency']},
        {'artifact_family': 'kernel_run_envelope_v1', 'owner_plane': 'advisory_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/artifacts/kernel_run_envelope_v1/households/', 'authority_class': 'audit_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['operators', 'replay'], 'notes': ['mandatory_for_every_kernel_run', 'links_execution_package_and_handoff']},
        {'artifact_family': 'snapshot_run_envelope_v1', 'owner_plane': 'advisory_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/artifacts/snapshot_run_envelope_v1/households/', 'authority_class': 'audit_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['operators', 'replay'], 'notes': ['mandatory_for_every_snapshot_kernel_run', 'links_validation_and_household_snapshot']},
        {'artifact_family': 'snapshot_validation_decision_v1', 'owner_plane': 'advisory_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/artifacts/snapshot_validation_decision_v1/households/', 'authority_class': 'gate_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['household_snapshot_v1', 'snapshot_run_envelope_v1'], 'notes': ['pure_snapshot_gate_only', 'verified_core_completeness_and_freshness_only']},
        {'artifact_family': 'official_recommendation_set_v1', 'owner_plane': 'truth_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'recommendation_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_execution', 'decision_chain'], 'notes': ['advisor_input_truth']},
        {'artifact_family': 'policy_v1', 'owner_plane': 'advisory_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/authorities/policy_v1/households/', 'authority_class': 'advisory_policy_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['household_snapshot_v1', 'portfolio_intent_v1'], 'notes': ['compiled_advisory_policy_authority', 'no_household_or_market_dependency']},
        {'artifact_family': 'portfolio_intent_v1', 'owner_plane': 'advisory_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/authorities/portfolio_intent_v1/households/', 'authority_class': 'advisory_portfolio_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['promotion_decision_v1', 'promotion_record_v2'], 'notes': ['narrow_target_state_authority', 'no_recommendation_or_broker_dependency']},
        {'artifact_family': 'planning_snapshot_v1', 'owner_plane': 'truth_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'fact_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_kernel', 'advisor_execution', 'advisor_trade_bridge', 'decision_chain'], 'notes': ['advisor_input_truth']},
        {'artifact_family': 'promotion_candidate_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'intermediate_only', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['promotion_review_v1', 'promotion_record_v1', 'decision_chain'], 'notes': ['promotion_input_only', 'never_executable_authority']},
        {'artifact_family': 'promotion_decision_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/artifacts/promotion_decision_v1/households/', 'authority_class': 'gate_artifact', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['promotion_record_v2', 'kernel_run_envelope_v1'], 'notes': ['pure_gate_result_only', 'policy_thresholds_only']},
        {'artifact_family': 'promotion_manual_review_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/<MODE>/promotion_manual_review_v1/<DAY>/', 'authority_class': 'intermediate_only', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['promotion_gate_result_v1', 'promotion_record_v1'], 'notes': ['manual_approval_input_only', 'never_promotion_authority']},
        {'artifact_family': 'promotion_gate_result_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/<MODE>/promotion_gate_result_v1/<DAY>/', 'authority_class': 'derived_only', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['decision_chain', 'operators'], 'notes': ['compatibility_output_only', 'not_authoritative_for_promotion']},
        {'artifact_family': 'promotion_record_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/<MODE>/promotion_record_v1/<DAY>/', 'authority_class': 'historical_compatibility_only', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['decision_chain', 'operators'], 'notes': ['legacy_recommendation_lineage_only', 'not_authoritative_for_kernel_execution']},
        {'artifact_family': 'promotion_record_v2', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime/authorities/promotion_record_v2/households/', 'authority_class': 'promotion_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['execution_intent_v1', 'kernel_run_envelope_v1'], 'notes': ['sole_promotion_authority', 'approved_delta_canonicalized']},
        {'artifact_family': 'promotion_review_v1', 'owner_plane': 'promotion_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'intermediate_only', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['promotion_manual_review_v1', 'promotion_record_v1', 'decision_chain'], 'notes': ['promotion_review_input_only', 'never_promotion_authority']},
        {'artifact_family': 'publication_gate_result_v1', 'owner_plane': 'decision_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'policy_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_kernel'], 'notes': ['kernel_governance_artifact']},
        {'artifact_family': 'replay_manifest_v1', 'owner_plane': 'truth_plane', 'write_root': '/tmp/constellation_2_foundation/reports', 'authority_class': 'report_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['replay'], 'notes': ['traceability_artifact']},
        {'artifact_family': 'runtime_trace_bundle_v1', 'owner_plane': 'truth_plane', 'write_root': '/tmp/constellation_2_foundation/reports', 'authority_class': 'report_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['operators', 'replay'], 'notes': ['traceability_artifact']},
        {'artifact_family': 'semantic_reconciliation_report_v1', 'owner_plane': 'decision_plane', 'write_root': '/tmp/constellation_2_foundation/advisor_runtime', 'authority_class': 'policy_authority', 'publication_required': True, 'promotion_required': False, 'replay_expected': True, 'downstream_consumers': ['advisor_kernel', 'decision_chain'], 'notes': ['kernel_governance_artifact']},
    ]
    rows_sorted = sorted(rows, key=lambda item: item['artifact_family'])
    obj = artifact_base_v1(schema_id='authority_registry', envelope=envelope)
    obj['rows'] = rows_sorted
    return AuthorityRegistryV1.from_dict(obj)
