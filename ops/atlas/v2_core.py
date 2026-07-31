from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


OBJECT_ID_FIELDS = {
    "AttentionDecision": "decision_id",
    "Prediction": "prediction_id",
    "Outcome": "outcome_id",
    "Regret": "regret_id",
    "CalibrationRecord": "calibration_id",
    "BehaviorChange": "behavior_change_id",
    "BeliefUpdate": "belief_update_id",
    "ExperienceEvent": "experience_id",
    "HistoricalExperienceRecord": "record_id",
    "CandidateWisdom": "wisdom_id",
    "WisdomValidation": "validation_id",
    "WisdomEvent": "event_id",
    "WisdomRetirement": "retirement_id",
    "CheapExperiment": "experiment_id",
    "LearningVelocityMetric": "metric_id",
    "ExperimentTierSummary": "summary_id",
    "PromotionGateDecision": "gate_id",
    "CheapExperimentBatch": "batch_id",
    "RunnerBudget": "runner_id",
    "LearningEstimate": "estimate_id",
    "LearningEstimateEvaluation": "evaluation_id",
    "AttentionSignal": "signal_id",
    "EstimatorPerformanceReport": "report_id",
    "ThresholdExperimentReport": "report_id",
    "ExperimentalRoutingDecision": "routing_id",
    "LabelIntegrityReport": "report_id",
    "ExternalStrategySource": "source_id",
    "ExternalStrategyClaim": "claim_id",
    "ExternalStrategyMechanism": "mechanism_id",
    "ExternalStrategyContrarianTheory": "contrarian_id",
    "ExternalStrategyDeduplicationResult": "dedupe_id",
    "ExternalStrategyCheapExperimentHandoff": "handoff_id",
    "TranscriptIntakeRequest": "request_id",
    "TranscriptSegment": "segment_id",
    "TranscriptClaimCandidate": "candidate_id",
    "ClaimBatch": "batch_id",
    "ClaimBatchRun": "run_id",
    "ClaimBatchMetrics": "metrics_id",
    "MechanismRegistry": "mechanism_id",
    "MechanismRegistryEntry": "mechanism_id",
    "MechanismCluster": "cluster_id",
    "MechanismLineage": "lineage_id",
    "MechanismMetrics": "metrics_id",
    "ClaimClusterSummary": "summary_id",
    "ResearchHypothesis": "hypothesis_id",
    "HypothesisFalsificationPlan": "plan_id",
    "HypothesisGenerationRun": "run_id",
    "CheapExperimentSpec": "experiment_spec_id",
    "ExperimentDataRequirement": "data_requirement_id",
    "ExperimentEvaluationPlan": "evaluation_plan_id",
    "ExperimentGenerationRun": "generation_run_id",
    "AutonomousResearchLoopRun": "loop_run_id",
    "AutonomousResearchLoopSummary": "summary_id",
    "ExperimentResult": "result_id",
    "ExperimentOutcomeSummary": "summary_id",
    "ExperimentExecutionRun": "execution_run_id",
    "AutonomousResearchLoopRun": "loop_run_id",
    "AutonomousResearchLoopSummary": "summary_id",
    "GeneratedResearchClaim": "generated_claim_id",
    "ClaimGenerationRun": "run_id",
    "ClaimGenerationSourceSummary": "summary_id",
}

REQUIRED_FIELDS = {
    "AttentionDecision": (
        "decision_id",
        "created_at",
        "decision_type",
        "uncertainty_target",
        "importance_score",
        "expected_learning_value",
        "expected_regret_if_ignored",
        "attention_cost",
        "selected_action",
        "rejected_alternatives",
        "decision_reason",
        "status",
        "transition_history",
    ),
    "Prediction": (
        "prediction_id",
        "decision_id",
        "created_at",
        "prediction_statement",
        "confidence",
        "expected_outcome",
        "evaluation_date",
        "status",
        "transition_history",
    ),
    "Outcome": (
        "outcome_id",
        "prediction_id",
        "created_at",
        "observed_outcome",
        "outcome_date",
        "matched_expected_outcome",
        "outcome_confidence",
        "evidence_reference",
        "transition_history",
    ),
    "Regret": (
        "regret_id",
        "decision_id",
        "outcome_id",
        "created_at",
        "regret_score",
        "missed_alternative",
        "regret_reason",
        "importance_weighted_regret",
        "transition_history",
    ),
    "CalibrationRecord": (
        "calibration_id",
        "prediction_id",
        "created_at",
        "confidence",
        "actual_result",
        "calibration_error",
        "calibration_bucket",
        "transition_history",
    ),
    "BehaviorChange": (
        "behavior_change_id",
        "created_at",
        "previous_behavior",
        "new_behavior",
        "change_reason",
        "expected_future_impact",
        "status",
        "transition_history",
    ),
    "BeliefUpdate": (
        "belief_update_id",
        "created_at",
        "triggering_experience_id",
        "previous_belief",
        "new_belief",
        "update_reason",
        "status",
        "transition_history",
    ),
    "ExperienceEvent": (
        "experience_id",
        "created_at",
        "source_decision_id",
        "prediction_id",
        "outcome_id",
        "lesson",
        "experience_quality_score",
        "transition_history",
    ),
    "HistoricalExperienceRecord": (
        "record_id",
        "created_at",
        "source_artifact",
        "source_type",
        "historical_date",
        "decision_summary",
        "expected_outcome",
        "actual_outcome",
        "confidence",
        "regret_score",
        "calibration_error",
        "experience_quality_score",
        "expected_learning_value_pre_outcome",
        "actual_learning_value_post_outcome",
        "expected_learning_source_fields",
        "actual_learning_source_fields",
        "conversion_reason",
        "provenance_reference",
        "converted_experience_event_id",
        "status",
        "transition_history",
    ),
    "CandidateWisdom": (
        "wisdom_id",
        "created_at",
        "statement",
        "originating_experience_ids",
        "supporting_outcome_ids",
        "supporting_count",
        "contradicting_count",
        "confidence",
        "status",
        "wisdom_score",
        "transition_history",
    ),
    "WisdomValidation": (
        "validation_id",
        "created_at",
        "wisdom_id",
        "prediction_affected",
        "decision_affected",
        "behavior_changed",
        "validation_result",
        "transition_history",
    ),
    "WisdomEvent": (
        "event_id",
        "created_at",
        "wisdom_id",
        "triggering_experience",
        "previous_behavior",
        "new_behavior",
        "expected_impact",
        "actual_impact",
        "transition_history",
    ),
    "WisdomRetirement": (
        "retirement_id",
        "created_at",
        "wisdom_id",
        "reason",
        "contradicting_evidence",
        "transition_history",
    ),
    "CheapExperiment": (
        "experiment_id",
        "created_at",
        "originating_object_id",
        "originating_object_type",
        "tier",
        "prediction_statement",
        "expected_learning_value",
        "attention_cost_estimate",
        "data_scope",
        "method_summary",
        "outcome_summary",
        "actual_learning_value",
        "status",
        "transition_history",
    ),
    "LearningVelocityMetric": (
        "metric_id",
        "created_at",
        "period_start",
        "period_end",
        "prediction_outcome_cycles",
        "cheap_experiments_completed",
        "actual_learning_total",
        "average_learning_per_cycle",
        "rejection_count",
        "promotion_count",
        "promotion_rate",
        "importance_weighted_regret_total",
        "transition_history",
    ),
    "ExperimentTierSummary": (
        "summary_id",
        "created_at",
        "period_start",
        "period_end",
        "tier",
        "experiments_run",
        "experiments_rejected",
        "experiments_promoted",
        "actual_learning_total",
        "average_cost_estimate",
        "average_learning_value",
        "transition_history",
    ),
    "PromotionGateDecision": (
        "gate_id",
        "created_at",
        "experiment_id",
        "from_tier",
        "to_tier",
        "decision",
        "reason",
        "expected_incremental_learning",
        "required_evidence",
        "forbidden_authority_acknowledged",
        "transition_history",
    ),
    "CheapExperimentBatch": (
        "batch_id",
        "created_at",
        "batch_type",
        "data_mode",
        "fixture_count",
        "accepted_count",
        "rejected_count",
        "emitted_record_counts",
        "allowed_tiers",
        "forbidden_authority_acknowledged",
        "status",
        "transition_history",
    ),
    "RunnerBudget": (
        "runner_id",
        "created_at",
        "max_experiments_per_run",
        "max_experiments_per_day",
        "allowed_tiers",
        "max_tier_2_per_day",
        "tier_3_enabled",
        "tier_4_enabled",
        "stop_on_error_count",
        "dry_run",
        "transition_history",
    ),
    "LearningEstimate": (
        "estimate_id",
        "created_at",
        "estimator_run_id",
        "source_object_id",
        "source_object_type",
        "expected_learning_value",
        "importance_score",
        "expected_regret_if_ignored",
        "attention_cost_estimate",
        "estimated_attention_priority",
        "estimator_version",
        "status",
        "transition_history",
    ),
    "LearningEstimateEvaluation": (
        "evaluation_id",
        "estimator_run_id",
        "estimate_id",
        "actual_learning_value",
        "actual_regret",
        "behavior_change_observed",
        "calibration_error",
        "learning_prediction_error",
        "importance_weighted_learning_error",
        "evaluation_reason",
        "evaluated_at",
        "transition_history",
    ),
    "AttentionSignal": (
        "signal_id",
        "estimator_run_id",
        "source_object_id",
        "source_object_type",
        "expected_learning_value",
        "importance_score",
        "attention_priority_score",
        "recommended_attention_action",
        "transition_history",
    ),
    "EstimatorPerformanceReport": (
        "report_id",
        "estimator_run_id",
        "period_start",
        "period_end",
        "source_ledger_root",
        "source_record_counts",
        "input_fingerprint",
        "estimator_version",
        "estimate_count",
        "evaluated_count",
        "mean_learning_prediction_error",
        "mean_importance_weighted_error",
        "behavior_change_rate",
        "regret_reduction_signal",
        "summary",
        "transition_history",
    ),
    "ThresholdExperimentReport": (
        "report_id",
        "created_at",
        "source_ledger_root",
        "threshold_profile_name",
        "production_thresholds",
        "experimental_thresholds",
        "score_distribution",
        "production_action_distribution",
        "experimental_action_distribution",
        "action_distribution_delta",
        "estimated_top_tail_count",
        "promote_to_tier_2_experiment_count",
        "requires_gate_review_experiment_count",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "ExperimentalRoutingDecision": (
        "routing_id",
        "created_at",
        "source_threshold_report_id",
        "source_attention_signal_id",
        "source_object_id",
        "source_object_type",
        "threshold_profile_name",
        "experimental_action",
        "routed_to_tier",
        "experiment_only",
        "reason",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "LabelIntegrityReport": (
        "report_id",
        "created_at",
        "dataset_name",
        "source_ledger_root",
        "record_count",
        "expected_label_source_fields",
        "actual_label_source_fields",
        "shared_source_fields",
        "circularity_score",
        "correlation_expected_actual",
        "label_independence_status",
        "warnings",
        "recommendations",
        "expected_distinct_values",
        "actual_distinct_values",
        "expected_entropy",
        "actual_entropy",
        "label_overlap_ratio",
        "shared_provenance_ratio",
        "authority_boundary_acknowledged",
        "transition_history",
    ),
    "ExternalStrategySource": (
        "source_id",
        "created_at",
        "source_type",
        "source_title",
        "input_text",
        "input_text_hash",
        "transcript_available",
        "status",
        "transition_history",
    ),
    "ExternalStrategyClaim": (
        "claim_id",
        "source_id",
        "claim_text",
        "confidence",
        "extraction_status",
        "transition_history",
    ),
    "ExternalStrategyMechanism": (
        "mechanism_id",
        "claim_id",
        "mechanism_family",
        "mechanism_description",
        "classification_confidence",
        "transition_history",
    ),
    "ExternalStrategyContrarianTheory": (
        "contrarian_id",
        "created_at",
        "claim_id",
        "mechanism_id",
        "primary_failure_modes",
        "opposite_hypothesis",
        "fragility_conditions",
        "required_falsification_tests",
        "prior_failure_matches",
        "contrarian_confidence",
        "status",
        "transition_history",
    ),
    "ExternalStrategyDeduplicationResult": (
        "dedupe_id",
        "claim_id",
        "claim_fingerprint",
        "mechanism_fingerprint",
        "is_duplicate",
        "status",
        "transition_history",
    ),
    "ExternalStrategyCheapExperimentHandoff": (
        "handoff_id",
        "claim_id",
        "mechanism_id",
        "dedupe_id",
        "eligible_for_cheap_experiment",
        "recommended_tier",
        "reason",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "TranscriptIntakeRequest": (
        "request_id",
        "created_at",
        "source_title",
        "transcript_text",
        "transcript_hash",
        "status",
        "transition_history",
    ),
    "TranscriptSegment": (
        "segment_id",
        "request_id",
        "start_offset",
        "end_offset",
        "segment_text",
        "segment_type",
        "transition_history",
    ),
    "TranscriptClaimCandidate": (
        "candidate_id",
        "request_id",
        "segment_ids",
        "candidate_text",
        "confidence",
        "status",
        "transition_history",
    ),
    "ClaimBatch": (
        "batch_id",
        "created_at",
        "pipeline_version",
        "claim_count",
        "batch_size_tier",
        "allowed_source_types",
        "source_types_present",
        "claim_ids",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "ClaimBatchRun": (
        "run_id",
        "batch_id",
        "created_at",
        "pipeline_version",
        "pipeline_steps",
        "input_claim_count",
        "processed_claim_count",
        "emitted_record_counts",
        "mechanism_record_ids",
        "dedupe_record_ids",
        "contrarian_record_ids",
        "handoff_record_ids",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "ClaimBatchMetrics": (
        "metrics_id",
        "batch_id",
        "created_at",
        "total_claims",
        "unique_claims",
        "unique_mechanisms",
        "duplicate_ratio",
        "mechanism_distribution",
        "cheap_experiment_candidates",
        "claims_ingested",
        "claims_deduped",
        "mechanisms_discovered",
        "mechanisms_reused",
        "contrarian_coverage",
        "cheap_experiment_coverage",
        "authority_boundary_acknowledged",
        "transition_history",
    ),
    "MechanismRegistry": (
        "mechanism_id",
        "created_at",
        "mechanism_family",
        "claim_count",
        "source_count",
        "contrarian_count",
        "cheap_experiment_count",
        "last_updated",
        "transition_history",
    ),
    "MechanismRegistryEntry": (
        "mechanism_id",
        "mechanism_family",
        "created_at",
        "updated_at",
        "claim_count",
        "source_count",
        "contrarian_count",
        "cheap_experiment_eligibility_count",
        "learning_event_count",
        "status",
        "transition_history",
    ),
    "MechanismCluster": (
        "cluster_id",
        "created_at",
        "mechanism_family",
        "claim_ids",
        "claim_count",
        "source_diversity",
        "confidence",
        "status",
        "transition_history",
    ),
    "MechanismLineage": (
        "lineage_id",
        "mechanism_id",
        "variant_description",
        "lineage_reason",
        "status",
        "transition_history",
    ),
    "MechanismMetrics": (
        "metrics_id",
        "mechanism_id",
        "claim_count",
        "duplicate_ratio",
        "contrarian_coverage",
        "cheap_experiment_coverage",
        "learning_event_count",
        "last_updated",
        "transition_history",
    ),
    "ClaimClusterSummary": (
        "summary_id",
        "batch_id",
        "created_at",
        "cluster_count",
        "clusters",
        "dedupe_strategy",
        "authority_boundary_acknowledged",
        "transition_history",
    ),
    "ResearchHypothesis": (
        "hypothesis_id",
        "created_at",
        "source_claim_id",
        "mechanism_id",
        "mechanism_family",
        "hypothesis_text",
        "testable_condition",
        "expected_direction",
        "baseline_comparison",
        "required_data",
        "falsification_criteria",
        "contrarian_inputs",
        "confidence",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "HypothesisFalsificationPlan": (
        "plan_id",
        "created_at",
        "hypothesis_id",
        "failure_modes",
        "required_tests",
        "minimum_sample_requirement",
        "baseline_requirement",
        "falsification_threshold",
        "status",
        "transition_history",
    ),
    "HypothesisGenerationRun": (
        "run_id",
        "created_at",
        "claims_processed",
        "hypotheses_created",
        "insufficient_detail_count",
        "duplicate_hypothesis_count",
        "status",
        "transition_history",
    ),
    "CheapExperimentSpec": (
        "experiment_spec_id",
        "hypothesis_id",
        "mechanism_id",
        "tier",
        "entry_condition",
        "exit_condition",
        "stop_condition",
        "target_condition",
        "baseline_condition",
        "required_data",
        "evaluation_metric",
        "falsification_threshold",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "ExperimentDataRequirement": (
        "data_requirement_id",
        "experiment_spec_id",
        "hypothesis_id",
        "data_name",
        "data_purpose",
        "minimum_observation_count",
        "source_mode",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "ExperimentEvaluationPlan": (
        "evaluation_plan_id",
        "experiment_spec_id",
        "hypothesis_id",
        "baseline_condition",
        "evaluation_metric",
        "falsification_threshold",
        "comparison_method",
        "result_recording_allowed",
        "execution_allowed",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "ExperimentGenerationRun": (
        "generation_run_id",
        "created_at",
        "generator_version",
        "input_hypothesis_count",
        "generated_spec_count",
        "experiment_spec_ids",
        "data_requirement_ids",
        "evaluation_plan_ids",
        "allowed_tiers",
        "forbidden_tiers",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "AutonomousResearchLoopRun": (
        "loop_run_id",
        "created_at",
        "runner_version",
        "mode",
        "max_loop_count",
        "claims_seen",
        "hypotheses_created",
        "specs_created",
        "results_created",
        "experience_events_created",
        "learning_estimates_created",
        "label_integrity_report_id",
        "authority_boundary_acknowledged",
        "stopped_on_authority_violation",
        "status",
        "transition_history",
    ),
    "AutonomousResearchLoopSummary": (
        "summary_id",
        "loop_run_id",
        "created_at",
        "mode",
        "claims_seen",
        "mechanisms_seen",
        "hypotheses_created",
        "specs_created",
        "results_created",
        "experience_events_created",
        "learning_estimates_created",
        "learning_evaluations_created",
        "attention_signals_created",
        "label_integrity_report_id",
        "label_independence_status",
        "authority_boundary_acknowledged",
        "forbidden_authority_terms_absent",
        "status",
        "transition_history",
    ),
    "ExperimentResult": (
        "result_id",
        "execution_run_id",
        "experiment_spec_id",
        "hypothesis_id",
        "mechanism_id",
        "tier",
        "data_mode",
        "baseline_condition",
        "baseline_observation_count",
        "experiment_observation_count",
        "baseline_metric_value",
        "experiment_metric_value",
        "baseline_comparison",
        "evaluation_metric",
        "falsification_threshold",
        "falsification_result",
        "outcome",
        "result_summary",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "ExperimentOutcomeSummary": (
        "summary_id",
        "execution_run_id",
        "created_at",
        "result_count",
        "pass_count",
        "fail_count",
        "inconclusive_count",
        "falsified_count",
        "not_falsified_count",
        "baseline_comparison_recorded_count",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "ExperimentExecutionRun": (
        "execution_run_id",
        "created_at",
        "executor_version",
        "input_spec_count",
        "result_count",
        "experiment_spec_ids",
        "result_ids",
        "outcome_summary_id",
        "allowed_data_modes",
        "forbidden_authority_acknowledged",
        "status",
        "transition_history",
    ),
    "AutonomousResearchLoopRun": (
        "loop_run_id",
        "created_at",
        "runner_version",
        "mode",
        "max_loop_count",
        "claims_seen",
        "hypotheses_created",
        "specs_created",
        "results_created",
        "experience_events_created",
        "learning_estimates_created",
        "label_integrity_report_id",
        "authority_boundary_acknowledged",
        "stopped_on_authority_violation",
        "status",
        "transition_history",
    ),
    "AutonomousResearchLoopSummary": (
        "summary_id",
        "loop_run_id",
        "created_at",
        "mode",
        "claims_seen",
        "mechanisms_seen",
        "hypotheses_created",
        "specs_created",
        "results_created",
        "experience_events_created",
        "learning_estimates_created",
        "learning_evaluations_created",
        "attention_signals_created",
        "label_integrity_report_id",
        "label_independence_status",
        "authority_boundary_acknowledged",
        "forbidden_authority_terms_absent",
        "status",
        "transition_history",
    ),
    "GeneratedResearchClaim": (
        "generated_claim_id",
        "created_at",
        "source_event_ids",
        "source_mechanism_ids",
        "source_type",
        "mechanism_family",
        "claim_text",
        "rationale",
        "novelty_basis",
        "expected_learning_value",
        "uncertainty_score",
        "contrarian_prompt",
        "intended_testability",
        "authority_boundary_acknowledged",
        "status",
        "transition_history",
    ),
    "ClaimGenerationRun": (
        "run_id",
        "created_at",
        "max_claims",
        "claims_generated",
        "duplicates_detected",
        "insufficient_basis_count",
        "mechanism_distribution",
        "status",
        "transition_history",
    ),
    "ClaimGenerationSourceSummary": (
        "summary_id",
        "run_id",
        "source_type",
        "source_count",
        "generated_count",
        "duplicate_count",
        "rejected_count",
        "transition_history",
    ),
}

FORBIDDEN_ARTIFACT_TERMS = (
    "discovery",
    "generation",
    "trading",
    "trade",
    "sleeve",
    "candidate",
    "paper_position",
    "paper-position",
    "capital_allocation",
    "allocation",
)

UNKNOWN_STATUSES = {"unknown", "outcome_unknown", "pending_outcome"}
FAILED_STATUSES = {"failed", "failure"}
WISDOM_STATUSES = {"EMERGING", "SUPPORTED", "STRONG", "CONTESTED", "RETIRED"}
CHEAP_EXPERIMENT_TIERS = {
    "TIER_0_DEDUPE",
    "TIER_1_SANITY",
    "TIER_2_LIGHTWEIGHT_VALIDATION",
    "TIER_3_ROBUST_VALIDATION",
    "TIER_4_MATURITY_TRACKING",
}
CHEAP_EXPERIMENT_SPEC_TIERS = {
    "TIER_0_DEDUPE",
    "TIER_1_SANITY",
    "TIER_2_LIGHTWEIGHT_VALIDATION",
}
CHEAP_EXPERIMENT_EXECUTOR_DATA_MODES = {
    "FIXTURE",
    "MOCK_HISTORICAL",
    "HISTORICAL_READONLY",
}
EXPERIMENT_OUTCOMES = {"PASS", "FAIL", "INCONCLUSIVE"}
EXPERIMENT_FALSIFICATION_RESULTS = {"FALSIFIED", "NOT_FALSIFIED", "INCONCLUSIVE"}
AUTONOMOUS_RESEARCH_LOOP_MODES = {"mock", "historical"}
AUTONOMOUS_RESEARCH_LOOP_RUN_STATUSES = {"COMPLETED_SAFE_RESEARCH_LOOP", "STOPPED_AUTHORITY_VIOLATION", "NO_CLAIMS"}
AUTONOMOUS_RESEARCH_LOOP_SUMMARY_STATUSES = {"SAFE_RESEARCH_LOOP_COMPLETED", "STOPPED_AUTHORITY_VIOLATION", "NO_CLAIMS"}
CLAIM_IDEA_SOURCE_TYPES = {
    "HISTORICAL_FAILURE",
    "HISTORICAL_CONTRADICTION",
    "HISTORICAL_DECISION",
    "HISTORICAL_RESEARCH_OUTCOME",
    "MECHANISM_REGISTRY",
    "MECHANISM_GAP",
    "CONTRARIAN_GAP",
    "TRANSCRIPT_CLAIM",
    "PRIOR_EXPERIENCE_EVENT",
    "REGRET_SIGNAL",
    "CALIBRATION_ERROR",
}
GENERATED_RESEARCH_CLAIM_STATUSES = {
    "GENERATED",
    "DUPLICATE",
    "INSUFFICIENT_BASIS",
    "UNSUPPORTED_MECHANISM",
    "REJECTED_LOW_TESTABILITY",
}
CLAIM_GENERATION_RUN_STATUSES = {
    "COMPLETED",
    "COMPLETED_WITH_REJECTIONS",
    "NO_ELIGIBLE_SOURCES",
}
LOW_VOLUME_GATE_TIERS = {"TIER_3_ROBUST_VALIDATION", "TIER_4_MATURITY_TRACKING"}
GATED_EXPERIMENT_TIERS = LOW_VOLUME_GATE_TIERS
HIGH_VOLUME_EXPERIMENT_TIERS = {"TIER_0_DEDUPE", "TIER_1_SANITY"}
MODERATE_VOLUME_EXPERIMENT_TIERS = {"TIER_2_LIGHTWEIGHT_VALIDATION"}
PROMOTION_DECISIONS = {"APPROVED", "REJECTED", "DEFERRED"}
LEARNING_ESTIMATOR_SOURCE_TYPES = {
    "CheapExperiment",
    "ExperienceEvent",
    "AttentionDecision",
    "LearningVelocityMetric",
}
ALLOWED_ATTENTION_ACTIONS = {
    "IGNORE",
    "REJECT",
    "WATCH",
    "CHEAP_TEST",
    "PROMOTE_TO_TIER_2",
    "REQUIRES_GATE_REVIEW",
}
EXPERIMENTAL_ROUTABLE_ACTIONS = {
    "CHEAP_TEST",
    "PROMOTE_TO_TIER_2",
}
EXPERIMENTAL_ROUTED_TIERS = {
    "TIER_1_SANITY",
    "TIER_2_LIGHTWEIGHT_VALIDATION",
}
FORBIDDEN_ATTENTION_ACTIONS = {
    "CREATE_CANDIDATE",
    "CREATE_SLEEVE",
    "CREATE_PAPER_POSITION",
    "TRADE",
    "ALLOCATE_CAPITAL",
    "VALIDATE",
}
HISTORICAL_EXPERIENCE_SOURCE_TYPES = {
    "FAILURE",
    "OBSERVATION",
    "KNOWLEDGE",
    "DECISION",
    "CONTRADICTION",
    "RESEARCH_OUTCOME",
}
HISTORICAL_EXPERIENCE_STATUSES = {
    "CONVERTED",
    "INCOMPLETE_PROVENANCE",
    "UNSUPPORTED_SOURCE",
    "DUPLICATE_SOURCE",
    "REJECTED_LOW_QUALITY",
}
EXTERNAL_STRATEGY_SOURCE_TYPES = {
    "YOUTUBE_TRANSCRIPT",
    "YOUTUBE_MANUAL_NOTES",
    "BLOG",
    "PAPER",
    "OTHER",
}
EXTERNAL_STRATEGY_EXTRACTION_STATUSES = {
    "EXTRACTED",
    "INSUFFICIENT_RULE_DETAIL",
    "TRANSCRIPT_REQUIRED",
    "UNSUPPORTED_SOURCE",
    "DUPLICATE_CLAIM",
}
EXTERNAL_STRATEGY_MECHANISM_FAMILIES = {
    "OPENING_RANGE",
    "BREAKOUT",
    "MEAN_REVERSION",
    "TREND_CONTINUATION",
    "LIQUIDITY_SWEEP",
    "VOLATILITY_EXPANSION",
    "PULLBACK",
    "MOMENTUM",
    "VWAP_OR_AVERAGE_RECLAIM",
    "ATR_FILTER",
    "SESSION_TIMING",
    "UNKNOWN",
}
EXTERNAL_STRATEGY_PRIMARY_FAILURE_MODES = {
    "OVERFITTING",
    "CROWDING",
    "SLIPPAGE",
    "REGIME_DEPENDENCE",
    "VAGUE_RULES",
    "LOOKAHEAD_BIAS",
    "SURVIVORSHIP_BIAS",
    "COMMISSION_DRAG",
    "CHERRY_PICKING",
    "DATA_MINING",
    "LOW_SAMPLE_SIZE",
    "UNKNOWN",
}
EXTERNAL_STRATEGY_CONTRARIAN_STATUSES = {
    "GENERATED",
    "UNKNOWN",
    "INSUFFICIENT_DETAIL",
    "DUPLICATE_CONTRARIAN",
    "REQUIRES_CLARIFICATION",
}
EXTERNAL_STRATEGY_HANDOFF_TIERS = {
    "TIER_0_DEDUPE",
    "TIER_1_SANITY",
    "TIER_2_LIGHTWEIGHT_VALIDATION",
}
HIGH_VOLUME_CLAIM_SOURCE_TYPES = {
    "TRANSCRIPT_INTAKE",
    "MANUAL_CLAIM_ENTRY",
    "HISTORICAL_RESEARCH_ARTIFACTS",
    "EXTERNAL_STRATEGY_CLAIMS",
}
HIGH_VOLUME_CLAIM_BATCH_TIERS = {
    "UP_TO_1000",
    "UP_TO_5000",
    "UP_TO_10000",
}
TRANSCRIPT_INTAKE_STATUSES = {
    "RECEIVED",
    "PARSED",
    "FAILED",
}
TRANSCRIPT_SEGMENT_TYPES = {
    "ENTRY_RULE",
    "EXIT_RULE",
    "FILTER_RULE",
    "RISK_RULE",
    "MARKET_CONTEXT",
    "PERFORMANCE_CLAIM",
    "OTHER",
}
TRANSCRIPT_CLAIM_CANDIDATE_STATUSES = {
    "READY_FOR_EXTRACTION",
    "INSUFFICIENT_DETAIL",
    "DUPLICATE_SEGMENT",
}
RESEARCH_HYPOTHESIS_STATUSES = {
    "GENERATED",
    "TESTABLE",
    "INSUFFICIENT_DETAIL",
    "DUPLICATE_HYPOTHESIS",
    "UNSUPPORTED_MECHANISM",
    "REJECTED_LOW_SPECIFICITY",
}
HYPOTHESIS_FALSIFICATION_PLAN_STATUSES = {
    "CREATED",
    "PLANNED",
    "INSUFFICIENT_DETAIL",
    "REQUIRES_BASELINE",
    "REQUIRES_DATA",
}
HYPOTHESIS_GENERATION_RUN_STATUSES = {
    "COMPLETED",
    "COMPLETED_WITH_INSUFFICIENT_DETAIL",
    "NO_TESTABLE_HYPOTHESES",
}
FORBIDDEN_AUTHORITY_KEYS = {
    "trading_authority",
    "trade_authority",
    "trade_advice_allowed",
    "trade_recommendation",
    "live_recommendation",
    "broker_execution",
    "broker_execution_allowed",
    "autonomous_execution",
    "autonomous_execution_allowed",
    "sleeve_creation",
    "sleeve_id",
    "created_sleeve_id",
    "candidate_creation",
    "candidate_id",
    "created_candidate_id",
    "paper_position_creation",
    "paper_position_id",
    "created_paper_position_id",
    "capital_allocation",
    "allocation_id",
    "validation_authority",
    "validation_authority_allowed",
    "recommendation",
    "recommendation_id",
    "trading_recommendation",
    "broker_order",
    "order_id",
}
PROHIBITED_AUTHORITY_FIELDS = tuple(sorted(FORBIDDEN_AUTHORITY_KEYS))
WISDOM_SCORE_FIELDS = (
    "importance",
    "behavior_change_frequency",
    "future_decision_impact",
    "calibration_impact",
    "adaptation_impact",
)

class AtlasV2ValidationError(ValueError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_missing(value: Any) -> bool:
    return value is None or value == ""


def initial_transition(*, timestamp: str, status: str, reason: str, triggering_object: str) -> dict[str, str]:
    return {
        "transitioned_at": timestamp,
        "from_status": "none",
        "to_status": status,
        "reason": reason,
        "triggering_object": triggering_object,
    }


def validate_transition(transition: dict[str, Any]) -> None:
    for field in ("transitioned_at", "from_status", "to_status", "reason", "triggering_object"):
        if _is_missing(transition.get(field)):
            raise AtlasV2ValidationError(f"transition missing required field: {field}")
    if str(transition["from_status"]).lower() in UNKNOWN_STATUSES and str(transition["to_status"]).lower() in FAILED_STATUSES:
        raise AtlasV2ValidationError("unknown status cannot silently transition to failed")


def validate_object(record: dict[str, Any]) -> None:
    object_type = str(record.get("object_type") or "")
    if object_type not in REQUIRED_FIELDS:
        raise AtlasV2ValidationError(f"unknown Atlas V2 object_type: {object_type}")
    for field in REQUIRED_FIELDS[object_type]:
        if field not in record:
            raise AtlasV2ValidationError(f"{object_type} missing required field: {field}")
        if _is_missing(record.get(field)) and not (object_type == "ExternalStrategySource" and field == "input_text"):
            raise AtlasV2ValidationError(f"{object_type} missing required field: {field}")
    history = record.get("transition_history")
    if not isinstance(history, list) or not history:
        raise AtlasV2ValidationError(f"{object_type} requires non-empty transition_history")
    for transition in history:
        if not isinstance(transition, dict):
            raise AtlasV2ValidationError(f"{object_type} transition_history contains non-object transition")
        validate_transition(transition)
    if object_type == "BehaviorChange" and not (record.get("triggering_regret_id") or record.get("triggering_calibration_id")):
        raise AtlasV2ValidationError("BehaviorChange requires triggering_regret_id or triggering_calibration_id")
    if object_type == "CandidateWisdom":
        validate_candidate_wisdom(record)
    if object_type == "WisdomValidation":
        validate_wisdom_validation(record)
    if object_type == "WisdomEvent":
        validate_wisdom_event(record)
    if object_type == "WisdomRetirement":
        validate_wisdom_retirement(record)
    if object_type == "CheapExperiment":
        validate_cheap_experiment(record)
    if object_type == "LearningVelocityMetric":
        validate_learning_velocity_metric(record)
    if object_type == "ExperimentTierSummary":
        validate_experiment_tier_summary(record)
    if object_type == "PromotionGateDecision":
        validate_promotion_gate_decision(record)
    if object_type == "CheapExperimentBatch":
        validate_cheap_experiment_batch(record)
    if object_type == "RunnerBudget":
        validate_runner_budget(record)
    if object_type == "LearningEstimate":
        validate_learning_estimate(record)
    if object_type == "LearningEstimateEvaluation":
        validate_learning_estimate_evaluation(record)
    if object_type == "AttentionSignal":
        validate_attention_signal(record)
    if object_type == "EstimatorPerformanceReport":
        validate_estimator_performance_report(record)
    if object_type == "ThresholdExperimentReport":
        validate_threshold_experiment_report(record)
    if object_type == "ExperimentalRoutingDecision":
        validate_experimental_routing_decision(record)
    if object_type == "LabelIntegrityReport":
        validate_label_integrity_report(record)
    if object_type == "HistoricalExperienceRecord":
        validate_historical_experience_record(record)
    if object_type == "ExternalStrategySource":
        validate_external_strategy_source(record)
    if object_type == "ExternalStrategyClaim":
        validate_external_strategy_claim(record)
    if object_type == "ExternalStrategyMechanism":
        validate_external_strategy_mechanism(record)
    if object_type == "ExternalStrategyContrarianTheory":
        validate_external_strategy_contrarian_theory(record)
    if object_type == "ExternalStrategyDeduplicationResult":
        validate_external_strategy_deduplication_result(record)
    if object_type == "ExternalStrategyCheapExperimentHandoff":
        validate_external_strategy_cheap_experiment_handoff(record)
    if object_type == "TranscriptIntakeRequest":
        validate_transcript_intake_request(record)
    if object_type == "TranscriptSegment":
        validate_transcript_segment(record)
    if object_type == "TranscriptClaimCandidate":
        validate_transcript_claim_candidate(record)
    if object_type == "ClaimBatch":
        validate_claim_batch(record)
    if object_type == "ClaimBatchRun":
        validate_claim_batch_run(record)
    if object_type == "ClaimBatchMetrics":
        validate_claim_batch_metrics(record)
    if object_type == "MechanismRegistry":
        validate_mechanism_registry(record)
    if object_type == "MechanismRegistryEntry":
        validate_mechanism_registry_entry(record)
    if object_type == "MechanismCluster":
        validate_mechanism_cluster(record)
    if object_type == "MechanismLineage":
        validate_mechanism_lineage(record)
    if object_type == "MechanismMetrics":
        validate_mechanism_metrics(record)
    if object_type == "ClaimClusterSummary":
        validate_claim_cluster_summary(record)
    if object_type == "GeneratedResearchClaim":
        validate_generated_research_claim(record)
    if object_type == "ClaimGenerationRun":
        validate_claim_generation_run(record)
    if object_type == "ClaimGenerationSourceSummary":
        validate_claim_generation_source_summary(record)
    if object_type == "ResearchHypothesis":
        validate_research_hypothesis(record)
    if object_type == "HypothesisFalsificationPlan":
        validate_hypothesis_falsification_plan(record)
    if object_type == "HypothesisGenerationRun":
        validate_hypothesis_generation_run(record)
    if object_type == "CheapExperimentSpec":
        validate_cheap_experiment_spec(record)
    if object_type == "ExperimentDataRequirement":
        validate_experiment_data_requirement(record)
    if object_type == "ExperimentEvaluationPlan":
        validate_experiment_evaluation_plan(record)
    if object_type == "ExperimentGenerationRun":
        validate_experiment_generation_run(record)
    if object_type == "AutonomousResearchLoopRun":
        validate_autonomous_research_loop_run(record)
    if object_type == "AutonomousResearchLoopSummary":
        validate_autonomous_research_loop_summary(record)
    if object_type == "AutonomousResearchLoopRun":
        validate_autonomous_research_loop_run(record)
    if object_type == "AutonomousResearchLoopSummary":
        validate_autonomous_research_loop_summary(record)
    if object_type == "ExperimentResult":
        validate_experiment_result(record)
    if object_type == "ExperimentOutcomeSummary":
        validate_experiment_outcome_summary(record)
    if object_type == "ExperimentExecutionRun":
        validate_experiment_execution_run(record)
    if object_type == "AutonomousResearchLoopRun":
        validate_autonomous_research_loop_run(record)
    if object_type == "AutonomousResearchLoopSummary":
        validate_autonomous_research_loop_summary(record)


def _require_non_empty_list(record: dict[str, Any], field: str, object_type: str) -> None:
    value = record.get(field)
    if not isinstance(value, list) or not value or any(_is_missing(item) for item in value):
        raise AtlasV2ValidationError(f"{object_type} requires non-empty {field}")


def wisdom_score(
    *,
    importance: float,
    behavior_change_frequency: float,
    future_decision_impact: float,
    calibration_impact: float,
    adaptation_impact: float,
) -> dict[str, float]:
    components = {
        "importance": importance,
        "behavior_change_frequency": behavior_change_frequency,
        "future_decision_impact": future_decision_impact,
        "calibration_impact": calibration_impact,
        "adaptation_impact": adaptation_impact,
    }
    for field, value in components.items():
        if not isinstance(value, (int, float)) or value < 0 or value > 1:
            raise AtlasV2ValidationError(f"wisdom_score {field} must be a number between 0 and 1")
    total = round(
        (
            importance * 0.25
            + behavior_change_frequency * 0.25
            + future_decision_impact * 0.2
            + calibration_impact * 0.15
            + adaptation_impact * 0.15
        ),
        4,
    )
    return {**components, "total": total}


def validate_wisdom_score(score: dict[str, Any]) -> None:
    if not isinstance(score, dict):
        raise AtlasV2ValidationError("CandidateWisdom wisdom_score must be an object")
    for field in WISDOM_SCORE_FIELDS:
        value = score.get(field)
        if not isinstance(value, (int, float)) or value < 0 or value > 1:
            raise AtlasV2ValidationError(f"CandidateWisdom wisdom_score missing valid {field}")
    total = score.get("total")
    if total is not None and (not isinstance(total, (int, float)) or total < 0 or total > 1):
        raise AtlasV2ValidationError("CandidateWisdom wisdom_score total must be between 0 and 1")


def validate_candidate_wisdom(record: dict[str, Any]) -> None:
    status = str(record.get("status") or "")
    if status not in WISDOM_STATUSES:
        raise AtlasV2ValidationError(f"CandidateWisdom invalid status: {status}")
    _require_non_empty_list(record, "originating_experience_ids", "CandidateWisdom")
    _require_non_empty_list(record, "supporting_outcome_ids", "CandidateWisdom")
    if int(record.get("supporting_count", 0)) < len(record["originating_experience_ids"]):
        raise AtlasV2ValidationError("CandidateWisdom supporting_count must cover originating experiences")
    if not isinstance(record.get("contradicting_count"), int) or record["contradicting_count"] < 0:
        raise AtlasV2ValidationError("CandidateWisdom contradicting_count must be a non-negative integer")
    confidence = record.get("confidence")
    if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 1:
        raise AtlasV2ValidationError("CandidateWisdom confidence must be between 0 and 1")
    validate_wisdom_score(record["wisdom_score"])


def validate_wisdom_validation(record: dict[str, Any]) -> None:
    if not record.get("behavior_changed"):
        raise AtlasV2ValidationError("WisdomValidation requires behavior_changed=true; otherwise it is an observation")
    if str(record.get("validation_result") or "").upper() in {"PASSED", "VALIDATED", "SUPPORTED"}:
        if _is_missing(record.get("prediction_affected")) or _is_missing(record.get("decision_affected")):
            raise AtlasV2ValidationError("WisdomValidation requires affected prediction and decision")


def validate_wisdom_event(record: dict[str, Any]) -> None:
    if record.get("previous_behavior") == record.get("new_behavior"):
        raise AtlasV2ValidationError("WisdomEvent requires changed behavior")
    if _is_missing(record.get("actual_impact")):
        raise AtlasV2ValidationError("WisdomEvent requires actual_impact")


def validate_wisdom_retirement(record: dict[str, Any]) -> None:
    _require_non_empty_list(record, "contradicting_evidence", "WisdomRetirement")


def _same_number(left: float, right: float) -> bool:
    return abs(left - right) <= 1e-9


def _require_number(record: dict[str, Any], field: str, object_type: str) -> float:
    value = record.get(field)
    if not isinstance(value, (int, float)):
        raise AtlasV2ValidationError(f"{object_type} {field} must be a number")
    return float(value)


def _assert_no_forbidden_authority_fields(record: dict[str, Any], object_type: str) -> None:
    for key, value in record.items():
        normalized = str(key).lower()
        if normalized in FORBIDDEN_AUTHORITY_KEYS and value not in (None, "", False, [], {}):
            raise AtlasV2ValidationError(f"{object_type} cannot set prohibited authority field: {key}")


def validate_cheap_experiment(record: dict[str, Any]) -> None:
    tier = str(record.get("tier") or "")
    if tier not in CHEAP_EXPERIMENT_TIERS:
        raise AtlasV2ValidationError(f"CheapExperiment invalid tier: {tier}")
    _require_number(record, "expected_learning_value", "CheapExperiment")
    _require_number(record, "attention_cost_estimate", "CheapExperiment")
    _require_number(record, "actual_learning_value", "CheapExperiment")
    if tier in LOW_VOLUME_GATE_TIERS and _is_missing(record.get("promotion_gate_id")):
        raise AtlasV2ValidationError(f"CheapExperiment {tier} requires promotion_gate_id")
    if tier == "TIER_4_MATURITY_TRACKING" and _is_missing(record.get("evidence_maturity_rationale")):
        raise AtlasV2ValidationError("CheapExperiment TIER_4_MATURITY_TRACKING requires evidence_maturity_rationale")
    _assert_no_forbidden_authority_fields(record, "CheapExperiment")




def validate_cheap_experiment_spec(record: dict[str, Any]) -> None:
    tier = str(record.get("tier") or "")
    if tier not in CHEAP_EXPERIMENT_SPEC_TIERS:
        raise AtlasV2ValidationError(f"CheapExperimentSpec invalid tier: {tier}")
    for field in (
        "entry_condition",
        "exit_condition",
        "stop_condition",
        "target_condition",
        "baseline_condition",
        "evaluation_metric",
        "falsification_threshold",
    ):
        if _is_missing(record.get(field)):
            raise AtlasV2ValidationError(f"CheapExperimentSpec requires {field}")
    required_data = record.get("required_data")
    if not isinstance(required_data, list) or not required_data or any(_is_missing(item) for item in required_data):
        raise AtlasV2ValidationError("CheapExperimentSpec requires non-empty required_data")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("CheapExperimentSpec requires authority_boundary_acknowledged=true")
    if str(record.get("status") or "") not in {"GENERATED_SPEC", "SPEC_ONLY"}:
        raise AtlasV2ValidationError("CheapExperimentSpec status must be GENERATED_SPEC or SPEC_ONLY")
    _assert_no_forbidden_authority_fields(record, "CheapExperimentSpec")


def validate_experiment_data_requirement(record: dict[str, Any]) -> None:
    value = _require_number(record, "minimum_observation_count", "ExperimentDataRequirement")
    if value < 1 or int(value) != value:
        raise AtlasV2ValidationError("ExperimentDataRequirement minimum_observation_count must be a positive integer")
    if str(record.get("source_mode") or "") not in {"EXISTING_LEDGER", "EXISTING_HISTORICAL_FIXTURE", "OPERATOR_SUPPLIED_DATA"}:
        raise AtlasV2ValidationError("ExperimentDataRequirement source_mode is outside cheap experiment generator boundary")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ExperimentDataRequirement requires authority_boundary_acknowledged=true")
    if str(record.get("status") or "") not in {"REQUIRED", "SPEC_ONLY"}:
        raise AtlasV2ValidationError("ExperimentDataRequirement status must be REQUIRED or SPEC_ONLY")
    _assert_no_forbidden_authority_fields(record, "ExperimentDataRequirement")


def validate_experiment_evaluation_plan(record: dict[str, Any]) -> None:
    for field in ("baseline_condition", "evaluation_metric", "falsification_threshold", "comparison_method"):
        if _is_missing(record.get(field)):
            raise AtlasV2ValidationError(f"ExperimentEvaluationPlan requires {field}")
    if record.get("result_recording_allowed") is not False:
        raise AtlasV2ValidationError("ExperimentEvaluationPlan result_recording_allowed must be false for spec generation")
    if record.get("execution_allowed") is not False:
        raise AtlasV2ValidationError("ExperimentEvaluationPlan execution_allowed must be false")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ExperimentEvaluationPlan requires authority_boundary_acknowledged=true")
    if str(record.get("status") or "") not in {"PLANNED_SPEC_ONLY", "SPEC_ONLY"}:
        raise AtlasV2ValidationError("ExperimentEvaluationPlan status must be PLANNED_SPEC_ONLY or SPEC_ONLY")
    _assert_no_forbidden_authority_fields(record, "ExperimentEvaluationPlan")


def validate_experiment_generation_run(record: dict[str, Any]) -> None:
    for field in ("input_hypothesis_count", "generated_spec_count"):
        value = _require_number(record, field, "ExperimentGenerationRun")
        if value < 0 or int(value) != value:
            raise AtlasV2ValidationError(f"ExperimentGenerationRun {field} must be a non-negative integer")
    for field in ("experiment_spec_ids", "data_requirement_ids", "evaluation_plan_ids", "allowed_tiers", "forbidden_tiers"):
        value = record.get(field)
        if not isinstance(value, list):
            raise AtlasV2ValidationError(f"ExperimentGenerationRun {field} must be a list")
    for tier in record.get("allowed_tiers", []):
        if tier not in CHEAP_EXPERIMENT_SPEC_TIERS:
            raise AtlasV2ValidationError(f"ExperimentGenerationRun invalid allowed tier: {tier}")
    for tier in ("TIER_3_ROBUST_VALIDATION", "TIER_4_MATURITY_TRACKING"):
        if tier not in record.get("forbidden_tiers", []):
            raise AtlasV2ValidationError("ExperimentGenerationRun must list TIER_3/TIER_4 as forbidden")
    if record.get("generated_spec_count") != len(record.get("experiment_spec_ids", [])):
        raise AtlasV2ValidationError("ExperimentGenerationRun generated_spec_count must equal experiment_spec_ids count")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ExperimentGenerationRun requires authority_boundary_acknowledged=true")
    if str(record.get("status") or "") != "COMPLETED_SPEC_GENERATION":
        raise AtlasV2ValidationError("ExperimentGenerationRun status must be COMPLETED_SPEC_GENERATION")
    _assert_no_forbidden_authority_fields(record, "ExperimentGenerationRun")


def validate_autonomous_research_loop_run(record: dict[str, Any]) -> None:
    if str(record.get("runner_version") or "") != "atlas_v2_autonomous_research_loop_runner_v1":
        raise AtlasV2ValidationError("AutonomousResearchLoopRun runner_version must be atlas_v2_autonomous_research_loop_runner_v1")
    if str(record.get("mode") or "") not in {"mock", "historical"}:
        raise AtlasV2ValidationError("AutonomousResearchLoopRun requires mock or historical mode")
    for field in (
        "max_loop_count",
        "claims_seen",
        "hypotheses_created",
        "specs_created",
        "results_created",
        "experience_events_created",
        "learning_estimates_created",
    ):
        value = _require_number(record, field, "AutonomousResearchLoopRun")
        if value < 0 or int(value) != value:
            raise AtlasV2ValidationError(f"AutonomousResearchLoopRun {field} must be a non-negative integer")
    if int(record["max_loop_count"]) > 10:
        raise AtlasV2ValidationError("AutonomousResearchLoopRun max_loop_count cannot exceed 10")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("AutonomousResearchLoopRun requires authority_boundary_acknowledged=true")
    if record.get("stopped_on_authority_violation") not in (True, False):
        raise AtlasV2ValidationError("AutonomousResearchLoopRun stopped_on_authority_violation must be boolean")
    if str(record.get("status") or "") not in {"COMPLETED_SAFE_RESEARCH_LOOP", "STOPPED_AUTHORITY_VIOLATION", "NO_CLAIMS"}:
        raise AtlasV2ValidationError("AutonomousResearchLoopRun status is outside safe loop boundary")
    _assert_no_forbidden_authority_fields(record, "AutonomousResearchLoopRun")


def validate_autonomous_research_loop_summary(record: dict[str, Any]) -> None:
    if str(record.get("mode") or "") not in {"mock", "historical"}:
        raise AtlasV2ValidationError("AutonomousResearchLoopSummary requires mock or historical mode")
    for field in (
        "claims_seen",
        "mechanisms_seen",
        "hypotheses_created",
        "specs_created",
        "results_created",
        "experience_events_created",
        "learning_estimates_created",
        "learning_evaluations_created",
        "attention_signals_created",
    ):
        value = _require_number(record, field, "AutonomousResearchLoopSummary")
        if value < 0 or int(value) != value:
            raise AtlasV2ValidationError(f"AutonomousResearchLoopSummary {field} must be a non-negative integer")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("AutonomousResearchLoopSummary requires authority_boundary_acknowledged=true")
    if record.get("forbidden_authority_terms_absent") not in (True, False):
        raise AtlasV2ValidationError("AutonomousResearchLoopSummary forbidden_authority_terms_absent must be boolean")
    if str(record.get("label_independence_status") or "") not in {"INDEPENDENT", "PARTIALLY_SHARED", "HIGHLY_SHARED", "CIRCULAR", "NOT_RUN"}:
        raise AtlasV2ValidationError("AutonomousResearchLoopSummary invalid label_independence_status")
    if str(record.get("status") or "") not in {"SAFE_RESEARCH_LOOP_COMPLETED", "STOPPED_AUTHORITY_VIOLATION", "NO_CLAIMS"}:
        raise AtlasV2ValidationError("AutonomousResearchLoopSummary status is outside safe loop boundary")
    _assert_no_forbidden_authority_fields(record, "AutonomousResearchLoopSummary")


def validate_experiment_result(record: dict[str, Any]) -> None:
    tier = str(record.get("tier") or "")
    if tier not in CHEAP_EXPERIMENT_SPEC_TIERS:
        raise AtlasV2ValidationError(f"ExperimentResult invalid tier: {tier}")
    data_mode = str(record.get("data_mode") or "")
    if data_mode not in CHEAP_EXPERIMENT_EXECUTOR_DATA_MODES:
        raise AtlasV2ValidationError(f"ExperimentResult data_mode is not fixture/mock/historical-readonly: {data_mode}")
    for field in ("baseline_observation_count", "experiment_observation_count"):
        value = _require_number(record, field, "ExperimentResult")
        if value < 0 or int(value) != value:
            raise AtlasV2ValidationError(f"ExperimentResult {field} must be a non-negative integer")
    _require_number(record, "baseline_metric_value", "ExperimentResult")
    _require_number(record, "experiment_metric_value", "ExperimentResult")
    for field in ("baseline_condition", "baseline_comparison", "evaluation_metric", "falsification_threshold", "result_summary"):
        if _is_missing(record.get(field)):
            raise AtlasV2ValidationError(f"ExperimentResult requires {field}")
    if str(record.get("outcome") or "") not in EXPERIMENT_OUTCOMES:
        raise AtlasV2ValidationError("ExperimentResult outcome must be PASS, FAIL, or INCONCLUSIVE")
    if str(record.get("falsification_result") or "") not in EXPERIMENT_FALSIFICATION_RESULTS:
        raise AtlasV2ValidationError("ExperimentResult falsification_result must be FALSIFIED, NOT_FALSIFIED, or INCONCLUSIVE")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ExperimentResult requires authority_boundary_acknowledged=true")
    if str(record.get("status") or "") != "RECORDED_READ_ONLY_RESULT":
        raise AtlasV2ValidationError("ExperimentResult status must be RECORDED_READ_ONLY_RESULT")
    _assert_no_forbidden_authority_fields(record, "ExperimentResult")


def validate_experiment_outcome_summary(record: dict[str, Any]) -> None:
    count_fields = (
        "result_count",
        "pass_count",
        "fail_count",
        "inconclusive_count",
        "falsified_count",
        "not_falsified_count",
        "baseline_comparison_recorded_count",
    )
    for field in count_fields:
        value = _require_number(record, field, "ExperimentOutcomeSummary")
        if value < 0 or int(value) != value:
            raise AtlasV2ValidationError(f"ExperimentOutcomeSummary {field} must be a non-negative integer")
    if record["result_count"] != record["pass_count"] + record["fail_count"] + record["inconclusive_count"]:
        raise AtlasV2ValidationError("ExperimentOutcomeSummary outcome counts must equal result_count")
    if record["falsified_count"] + record["not_falsified_count"] > record["result_count"]:
        raise AtlasV2ValidationError("ExperimentOutcomeSummary falsification counts cannot exceed result_count")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ExperimentOutcomeSummary requires authority_boundary_acknowledged=true")
    if str(record.get("status") or "") != "SUMMARIZED_READ_ONLY_RESULTS":
        raise AtlasV2ValidationError("ExperimentOutcomeSummary status must be SUMMARIZED_READ_ONLY_RESULTS")
    _assert_no_forbidden_authority_fields(record, "ExperimentOutcomeSummary")


def validate_experiment_execution_run(record: dict[str, Any]) -> None:
    for field in ("input_spec_count", "result_count"):
        value = _require_number(record, field, "ExperimentExecutionRun")
        if value < 0 or int(value) != value:
            raise AtlasV2ValidationError(f"ExperimentExecutionRun {field} must be a non-negative integer")
    for field in ("experiment_spec_ids", "result_ids", "allowed_data_modes"):
        if not isinstance(record.get(field), list):
            raise AtlasV2ValidationError(f"ExperimentExecutionRun {field} must be a list")
    for mode in record.get("allowed_data_modes", []):
        if mode not in CHEAP_EXPERIMENT_EXECUTOR_DATA_MODES:
            raise AtlasV2ValidationError(f"ExperimentExecutionRun invalid allowed data mode: {mode}")
    if record.get("input_spec_count") != len(record.get("experiment_spec_ids", [])):
        raise AtlasV2ValidationError("ExperimentExecutionRun input_spec_count must equal experiment_spec_ids count")
    if record.get("result_count") != len(record.get("result_ids", [])):
        raise AtlasV2ValidationError("ExperimentExecutionRun result_count must equal result_ids count")
    if record.get("forbidden_authority_acknowledged") is not True:
        raise AtlasV2ValidationError("ExperimentExecutionRun requires forbidden_authority_acknowledged=true")
    if str(record.get("status") or "") != "COMPLETED_READ_ONLY_EXECUTION":
        raise AtlasV2ValidationError("ExperimentExecutionRun status must be COMPLETED_READ_ONLY_EXECUTION")
    _assert_no_forbidden_authority_fields(record, "ExperimentExecutionRun")


def _require_non_negative_integer(record: dict[str, Any], field: str, object_type: str) -> int:
    value = _require_number(record, field, object_type)
    if value < 0 or int(value) != value:
        raise AtlasV2ValidationError(f"{object_type} {field} must be a non-negative integer")
    return int(value)


def validate_autonomous_research_loop_run(record: dict[str, Any]) -> None:
    if str(record.get("runner_version") or "") != "atlas_v2_autonomous_research_loop_runner_v1":
        raise AtlasV2ValidationError("AutonomousResearchLoopRun invalid runner_version")
    if str(record.get("mode") or "") not in AUTONOMOUS_RESEARCH_LOOP_MODES:
        raise AtlasV2ValidationError("AutonomousResearchLoopRun mode must be mock or historical")
    max_loop_count = _require_non_negative_integer(record, "max_loop_count", "AutonomousResearchLoopRun")
    if max_loop_count > 10:
        raise AtlasV2ValidationError("AutonomousResearchLoopRun max_loop_count must be at most 10")
    for field in ("claims_seen", "hypotheses_created", "specs_created", "results_created", "experience_events_created", "learning_estimates_created"):
        _require_non_negative_integer(record, field, "AutonomousResearchLoopRun")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("AutonomousResearchLoopRun requires authority_boundary_acknowledged=true")
    if not isinstance(record.get("stopped_on_authority_violation"), bool):
        raise AtlasV2ValidationError("AutonomousResearchLoopRun stopped_on_authority_violation must be boolean")
    if str(record.get("status") or "") not in AUTONOMOUS_RESEARCH_LOOP_RUN_STATUSES:
        raise AtlasV2ValidationError("AutonomousResearchLoopRun invalid status")
    _assert_no_forbidden_authority_fields(record, "AutonomousResearchLoopRun")


def validate_autonomous_research_loop_summary(record: dict[str, Any]) -> None:
    if str(record.get("mode") or "") not in AUTONOMOUS_RESEARCH_LOOP_MODES:
        raise AtlasV2ValidationError("AutonomousResearchLoopSummary mode must be mock or historical")
    for field in (
        "claims_seen",
        "mechanisms_seen",
        "hypotheses_created",
        "specs_created",
        "results_created",
        "experience_events_created",
        "learning_estimates_created",
        "learning_evaluations_created",
        "attention_signals_created",
    ):
        _require_non_negative_integer(record, field, "AutonomousResearchLoopSummary")
    if record.get("label_independence_status") not in {"INDEPENDENT", "PARTIALLY_SHARED", "HIGHLY_SHARED", "CIRCULAR", "NOT_RUN"}:
        raise AtlasV2ValidationError("AutonomousResearchLoopSummary invalid label_independence_status")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("AutonomousResearchLoopSummary requires authority_boundary_acknowledged=true")
    if not isinstance(record.get("forbidden_authority_terms_absent"), bool):
        raise AtlasV2ValidationError("AutonomousResearchLoopSummary forbidden_authority_terms_absent must be boolean")
    if str(record.get("status") or "") not in AUTONOMOUS_RESEARCH_LOOP_SUMMARY_STATUSES:
        raise AtlasV2ValidationError("AutonomousResearchLoopSummary invalid status")
    _assert_no_forbidden_authority_fields(record, "AutonomousResearchLoopSummary")

def validate_learning_velocity_metric(record: dict[str, Any]) -> None:
    cycles = _require_number(record, "prediction_outcome_cycles", "LearningVelocityMetric")
    completed = _require_number(record, "cheap_experiments_completed", "LearningVelocityMetric")
    total = _require_number(record, "actual_learning_total", "LearningVelocityMetric")
    average = _require_number(record, "average_learning_per_cycle", "LearningVelocityMetric")
    rejections = _require_number(record, "rejection_count", "LearningVelocityMetric")
    promotion_count = _require_number(record, "promotion_count", "LearningVelocityMetric")
    promotion_rate = _require_number(record, "promotion_rate", "LearningVelocityMetric")
    regret = _require_number(record, "importance_weighted_regret_total", "LearningVelocityMetric")
    if min(cycles, completed, total, average, rejections, promotion_count, regret) < 0:
        raise AtlasV2ValidationError("LearningVelocityMetric values must be non-negative")
    if promotion_rate < 0 or promotion_rate > 1:
        raise AtlasV2ValidationError("LearningVelocityMetric promotion_rate must be between 0 and 1")


def validate_experiment_tier_summary(record: dict[str, Any]) -> None:
    tier = str(record.get("tier") or "")
    if tier not in CHEAP_EXPERIMENT_TIERS:
        raise AtlasV2ValidationError(f"ExperimentTierSummary invalid tier: {tier}")
    for field in (
        "experiments_run",
        "experiments_rejected",
        "experiments_promoted",
        "actual_learning_total",
        "average_cost_estimate",
        "average_learning_value",
    ):
        value = _require_number(record, field, "ExperimentTierSummary")
        if value < 0:
            raise AtlasV2ValidationError(f"ExperimentTierSummary {field} must be non-negative")


def validate_promotion_gate_decision(record: dict[str, Any]) -> None:
    from_tier = str(record.get("from_tier") or "")
    to_tier = str(record.get("to_tier") or "")
    if from_tier not in CHEAP_EXPERIMENT_TIERS or to_tier not in CHEAP_EXPERIMENT_TIERS:
        raise AtlasV2ValidationError("PromotionGateDecision requires valid from_tier and to_tier")
    if to_tier not in LOW_VOLUME_GATE_TIERS:
        raise AtlasV2ValidationError("PromotionGateDecision only gates TIER_3 or TIER_4 promotion")
    if str(record.get("decision") or "") not in PROMOTION_DECISIONS:
        raise AtlasV2ValidationError("PromotionGateDecision decision must be APPROVED, REJECTED, or DEFERRED")
    _require_number(record, "expected_incremental_learning", "PromotionGateDecision")
    _require_non_empty_list(record, "required_evidence", "PromotionGateDecision")
    if record.get("forbidden_authority_acknowledged") is not True:
        raise AtlasV2ValidationError("PromotionGateDecision requires forbidden_authority_acknowledged=true")
    _assert_no_forbidden_authority_fields(record, "PromotionGateDecision")


def validate_cheap_experiment_batch(record: dict[str, Any]) -> None:
    if str(record.get("batch_type") or "") != "MOCK_HISTORICAL_CHEAP_EXPERIMENT_BATCH":
        raise AtlasV2ValidationError("CheapExperimentBatch batch_type must be MOCK_HISTORICAL_CHEAP_EXPERIMENT_BATCH")
    if str(record.get("data_mode") or "") not in {"fixture", "mock_historical"}:
        raise AtlasV2ValidationError("CheapExperimentBatch data_mode must be fixture or mock_historical")
    for field in ("fixture_count", "accepted_count", "rejected_count"):
        value = _require_number(record, field, "CheapExperimentBatch")
        if value < 0:
            raise AtlasV2ValidationError(f"CheapExperimentBatch {field} must be non-negative")
    counts = record.get("emitted_record_counts")
    if not isinstance(counts, dict):
        raise AtlasV2ValidationError("CheapExperimentBatch emitted_record_counts must be an object")
    allowed_tiers = record.get("allowed_tiers")
    if not isinstance(allowed_tiers, list) or not allowed_tiers:
        raise AtlasV2ValidationError("CheapExperimentBatch requires allowed_tiers")
    for tier in allowed_tiers:
        if tier not in CHEAP_EXPERIMENT_TIERS:
            raise AtlasV2ValidationError(f"CheapExperimentBatch invalid allowed tier: {tier}")
    if record.get("forbidden_authority_acknowledged") is not True:
        raise AtlasV2ValidationError("CheapExperimentBatch requires forbidden_authority_acknowledged=true")
    _assert_no_forbidden_authority_fields(record, "CheapExperimentBatch")


def validate_runner_budget(record: dict[str, Any]) -> None:
    for field in (
        "max_experiments_per_run",
        "max_experiments_per_day",
        "max_tier_2_per_day",
        "stop_on_error_count",
    ):
        value = _require_number(record, field, "RunnerBudget")
        if value < 0:
            raise AtlasV2ValidationError(f"RunnerBudget {field} must be non-negative")
    allowed_tiers = record.get("allowed_tiers")
    if not isinstance(allowed_tiers, list) or not allowed_tiers:
        raise AtlasV2ValidationError("RunnerBudget requires non-empty allowed_tiers")
    for tier in allowed_tiers:
        if tier not in CHEAP_EXPERIMENT_TIERS:
            raise AtlasV2ValidationError(f"RunnerBudget invalid allowed tier: {tier}")
    if record.get("tier_3_enabled") not in (True, False):
        raise AtlasV2ValidationError("RunnerBudget tier_3_enabled must be boolean")
    if record.get("tier_4_enabled") not in (True, False):
        raise AtlasV2ValidationError("RunnerBudget tier_4_enabled must be boolean")
    if record.get("dry_run") not in (True, False):
        raise AtlasV2ValidationError("RunnerBudget dry_run must be boolean")
    if "TIER_3_ROBUST_VALIDATION" in allowed_tiers and record.get("tier_3_enabled") is not True:
        raise AtlasV2ValidationError("RunnerBudget cannot allow TIER_3 unless tier_3_enabled=true")
    if "TIER_4_MATURITY_TRACKING" in allowed_tiers and record.get("tier_4_enabled") is not True:
        raise AtlasV2ValidationError("RunnerBudget cannot allow TIER_4 unless tier_4_enabled=true")
    _assert_no_forbidden_authority_fields(record, "RunnerBudget")


def _require_estimator_source(record: dict[str, Any], object_type: str) -> None:
    source_type = str(record.get("source_object_type") or "")
    if source_type not in LEARNING_ESTIMATOR_SOURCE_TYPES:
        raise AtlasV2ValidationError(f"{object_type} invalid source_object_type: {source_type}")


def _require_estimator_run_id(record: dict[str, Any], object_type: str) -> None:
    if _is_missing(record.get("estimator_run_id")):
        raise AtlasV2ValidationError(f"{object_type} requires estimator_run_id")
    if "duplicate_of_run_id" in record and _is_missing(record.get("duplicate_of_run_id")):
        raise AtlasV2ValidationError(f"{object_type} duplicate_of_run_id must be non-empty when present")


def _require_zero_to_one(record: dict[str, Any], field: str, object_type: str) -> float:
    value = _require_number(record, field, object_type)
    if value < 0 or value > 1:
        raise AtlasV2ValidationError(f"{object_type} {field} must be between 0 and 1")
    return value


def _bounded_probability(value: Any, label: str) -> float:
    result = float(value)
    if result < 0 or result > 1:
        raise AtlasV2ValidationError(f"{label} must be between 0 and 1")
    return result


def _calibration_bucket(confidence: float) -> str:
    return f"{int(confidence * 10) / 10:.1f}"


def validate_learning_estimate(record: dict[str, Any]) -> None:
    _require_estimator_run_id(record, "LearningEstimate")
    _require_estimator_source(record, "LearningEstimate")
    for field in (
        "expected_learning_value",
        "importance_score",
        "expected_regret_if_ignored",
        "attention_cost_estimate",
        "estimated_attention_priority",
    ):
        _require_zero_to_one(record, field, "LearningEstimate")
    if str(record.get("estimator_version") or "") != "atlas_v2_learning_value_estimator_v1":
        raise AtlasV2ValidationError("LearningEstimate estimator_version must be atlas_v2_learning_value_estimator_v1")
    _assert_no_forbidden_authority_fields(record, "LearningEstimate")


def validate_learning_estimate_evaluation(record: dict[str, Any]) -> None:
    _require_estimator_run_id(record, "LearningEstimateEvaluation")
    actual = _require_number(record, "actual_learning_value", "LearningEstimateEvaluation")
    _require_number(record, "actual_regret", "LearningEstimateEvaluation")
    _require_number(record, "calibration_error", "LearningEstimateEvaluation")
    prediction_error = _require_number(record, "learning_prediction_error", "LearningEstimateEvaluation")
    weighted_error = _require_number(record, "importance_weighted_learning_error", "LearningEstimateEvaluation")
    if record.get("behavior_change_observed") not in (True, False):
        raise AtlasV2ValidationError("LearningEstimateEvaluation behavior_change_observed must be boolean")
    if "expected_learning_value" in record:
        expected = _require_number(record, "expected_learning_value", "LearningEstimateEvaluation")
        if abs((actual - expected) - prediction_error) > 1e-6:
            raise AtlasV2ValidationError("LearningEstimateEvaluation learning_prediction_error must equal actual minus expected")
    if abs(weighted_error) > 1:
        raise AtlasV2ValidationError("LearningEstimateEvaluation importance_weighted_learning_error must be bounded")
    _assert_no_forbidden_authority_fields(record, "LearningEstimateEvaluation")


def validate_attention_signal(record: dict[str, Any]) -> None:
    _require_estimator_run_id(record, "AttentionSignal")
    _require_estimator_source(record, "AttentionSignal")
    for field in ("expected_learning_value", "importance_score", "attention_priority_score"):
        _require_zero_to_one(record, field, "AttentionSignal")
    if "actual_learning_value" in record:
        _require_number(record, "actual_learning_value", "AttentionSignal")
    if "regret_score" in record:
        _require_zero_to_one(record, "regret_score", "AttentionSignal")
    if "behavior_change_observed" in record and record.get("behavior_change_observed") not in (True, False):
        raise AtlasV2ValidationError("AttentionSignal behavior_change_observed must be boolean")
    action = str(record.get("recommended_attention_action") or "")
    if action in FORBIDDEN_ATTENTION_ACTIONS:
        raise AtlasV2ValidationError(f"AttentionSignal cannot recommend forbidden action: {action}")
    if action not in ALLOWED_ATTENTION_ACTIONS:
        raise AtlasV2ValidationError(f"AttentionSignal invalid recommended_attention_action: {action}")
    _assert_no_forbidden_authority_fields(record, "AttentionSignal")


def validate_estimator_performance_report(record: dict[str, Any]) -> None:
    _require_estimator_run_id(record, "EstimatorPerformanceReport")
    if _is_missing(record.get("source_ledger_root")):
        raise AtlasV2ValidationError("EstimatorPerformanceReport requires source_ledger_root")
    if _is_missing(record.get("input_fingerprint")):
        raise AtlasV2ValidationError("EstimatorPerformanceReport requires input_fingerprint")
    source_counts = record.get("source_record_counts")
    if not isinstance(source_counts, dict) or not source_counts:
        raise AtlasV2ValidationError("EstimatorPerformanceReport requires non-empty source_record_counts")
    for key, value in source_counts.items():
        if _is_missing(key) or not isinstance(value, (int, float)) or value < 0:
            raise AtlasV2ValidationError("EstimatorPerformanceReport source_record_counts values must be non-negative numbers")
    for field in (
        "estimate_count",
        "evaluated_count",
        "mean_learning_prediction_error",
        "mean_importance_weighted_error",
        "behavior_change_rate",
        "regret_reduction_signal",
    ):
        _require_number(record, field, "EstimatorPerformanceReport")
    if float(record["estimate_count"]) < 0 or float(record["evaluated_count"]) < 0:
        raise AtlasV2ValidationError("EstimatorPerformanceReport counts must be non-negative")
    if float(record["evaluated_count"]) > float(record["estimate_count"]):
        raise AtlasV2ValidationError("EstimatorPerformanceReport evaluated_count cannot exceed estimate_count")
    if float(record["behavior_change_rate"]) < 0 or float(record["behavior_change_rate"]) > 1:
        raise AtlasV2ValidationError("EstimatorPerformanceReport behavior_change_rate must be between 0 and 1")
    if "correlation_expected_actual" in record:
        value = _require_number(record, "correlation_expected_actual", "EstimatorPerformanceReport")
        if value < -1 or value > 1:
            raise AtlasV2ValidationError("EstimatorPerformanceReport correlation_expected_actual must be between -1 and 1")
    _assert_no_forbidden_authority_fields(record, "EstimatorPerformanceReport")



def validate_threshold_experiment_report(record: dict[str, Any]) -> None:
    if str(record.get("threshold_profile_name") or "") != "quantile_fixture_v1":
        raise AtlasV2ValidationError("ThresholdExperimentReport threshold_profile_name must be quantile_fixture_v1")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ThresholdExperimentReport must acknowledge authority boundaries")
    for field in (
        "production_thresholds",
        "experimental_thresholds",
        "score_distribution",
        "production_action_distribution",
        "experimental_action_distribution",
        "action_distribution_delta",
    ):
        if not isinstance(record.get(field), dict):
            raise AtlasV2ValidationError(f"ThresholdExperimentReport {field} must be an object")
    for field in (
        "estimated_top_tail_count",
        "promote_to_tier_2_experiment_count",
        "requires_gate_review_experiment_count",
    ):
        value = _require_number(record, field, "ThresholdExperimentReport")
        if value < 0:
            raise AtlasV2ValidationError(f"ThresholdExperimentReport {field} must be non-negative")
    for distribution_field in ("production_action_distribution", "experimental_action_distribution"):
        for action, count in record[distribution_field].items():
            if action in FORBIDDEN_ATTENTION_ACTIONS:
                raise AtlasV2ValidationError(f"ThresholdExperimentReport cannot include forbidden action {action}")
            if action not in ALLOWED_ATTENTION_ACTIONS:
                raise AtlasV2ValidationError(f"ThresholdExperimentReport invalid action {action}")
            if not isinstance(count, int) or count < 0:
                raise AtlasV2ValidationError(f"ThresholdExperimentReport {distribution_field} counts must be non-negative integers")
    for action, delta in record["action_distribution_delta"].items():
        if action not in ALLOWED_ATTENTION_ACTIONS:
            raise AtlasV2ValidationError(f"ThresholdExperimentReport invalid delta action {action}")
        if not isinstance(delta, int):
            raise AtlasV2ValidationError("ThresholdExperimentReport action_distribution_delta values must be integers")
    _assert_no_forbidden_authority_fields(record, "ThresholdExperimentReport")


def validate_experimental_routing_decision(record: dict[str, Any]) -> None:
    if str(record.get("threshold_profile_name") or "") != "quantile_fixture_v1":
        raise AtlasV2ValidationError("ExperimentalRoutingDecision threshold_profile_name must be quantile_fixture_v1")
    _require_estimator_source(record, "ExperimentalRoutingDecision")
    action = str(record.get("experimental_action") or "")
    if action in FORBIDDEN_ATTENTION_ACTIONS:
        raise AtlasV2ValidationError(f"ExperimentalRoutingDecision cannot route forbidden action: {action}")
    if action == "REQUIRES_GATE_REVIEW":
        raise AtlasV2ValidationError("ExperimentalRoutingDecision cannot route REQUIRES_GATE_REVIEW")
    if action not in EXPERIMENTAL_ROUTABLE_ACTIONS:
        raise AtlasV2ValidationError(f"ExperimentalRoutingDecision invalid experimental_action: {action}")
    tier = str(record.get("routed_to_tier") or "")
    if tier not in EXPERIMENTAL_ROUTED_TIERS:
        raise AtlasV2ValidationError(f"ExperimentalRoutingDecision invalid routed_to_tier: {tier}")
    if action == "CHEAP_TEST" and tier != "TIER_1_SANITY":
        raise AtlasV2ValidationError("ExperimentalRoutingDecision CHEAP_TEST routes only to TIER_1_SANITY")
    if action == "PROMOTE_TO_TIER_2" and tier != "TIER_2_LIGHTWEIGHT_VALIDATION":
        raise AtlasV2ValidationError(
            "ExperimentalRoutingDecision PROMOTE_TO_TIER_2 routes only to TIER_2_LIGHTWEIGHT_VALIDATION"
        )
    if tier in GATED_EXPERIMENT_TIERS and _is_missing(record.get("promotion_gate_id")):
        raise AtlasV2ValidationError(f"ExperimentalRoutingDecision {tier} requires PromotionGateDecision")
    if record.get("experiment_only") is not True:
        raise AtlasV2ValidationError("ExperimentalRoutingDecision requires experiment_only=true")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ExperimentalRoutingDecision requires authority_boundary_acknowledged=true")
    if str(record.get("status") or "") != "routed_experiment_only":
        raise AtlasV2ValidationError("ExperimentalRoutingDecision status must be routed_experiment_only")
    _assert_no_forbidden_authority_fields(record, "ExperimentalRoutingDecision")


def validate_label_integrity_report(record: dict[str, Any]) -> None:
    status = str(record.get("label_independence_status") or "")
    if status not in {"INDEPENDENT", "PARTIALLY_SHARED", "HIGHLY_SHARED", "CIRCULAR"}:
        raise AtlasV2ValidationError(f"LabelIntegrityReport invalid label_independence_status: {status}")
    if _is_missing(record.get("source_ledger_root")):
        raise AtlasV2ValidationError("LabelIntegrityReport requires source_ledger_root")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("LabelIntegrityReport requires authority_boundary_acknowledged=true")
    for field in ("expected_label_source_fields", "actual_label_source_fields", "shared_source_fields", "warnings", "recommendations"):
        value = record.get(field)
        if not isinstance(value, list):
            raise AtlasV2ValidationError(f"LabelIntegrityReport {field} must be a list")
        if any(not isinstance(item, str) or _is_missing(item) for item in value):
            raise AtlasV2ValidationError(f"LabelIntegrityReport {field} must contain non-empty strings")
    for field in ("record_count", "expected_distinct_values", "actual_distinct_values"):
        value = _require_number(record, field, "LabelIntegrityReport")
        if value < 0 or int(value) != value:
            raise AtlasV2ValidationError(f"LabelIntegrityReport {field} must be a non-negative integer")
    for field in ("circularity_score", "label_overlap_ratio", "shared_provenance_ratio"):
        value = _require_number(record, field, "LabelIntegrityReport")
        if value < 0 or value > 1:
            raise AtlasV2ValidationError(f"LabelIntegrityReport {field} must be between 0 and 1")
    for field in ("expected_entropy", "actual_entropy"):
        value = _require_number(record, field, "LabelIntegrityReport")
        if value < 0:
            raise AtlasV2ValidationError(f"LabelIntegrityReport {field} must be non-negative")
    value = _require_number(record, "correlation_expected_actual", "LabelIntegrityReport")
    if value < -1 or value > 1:
        raise AtlasV2ValidationError("LabelIntegrityReport correlation_expected_actual must be between -1 and 1")
    _assert_no_forbidden_authority_fields(record, "LabelIntegrityReport")

def validate_historical_experience_record(record: dict[str, Any]) -> None:
    source_type = str(record.get("source_type") or "")
    if source_type not in HISTORICAL_EXPERIENCE_SOURCE_TYPES:
        raise AtlasV2ValidationError(f"HistoricalExperienceRecord invalid source_type: {source_type}")
    status = str(record.get("status") or "")
    if status not in HISTORICAL_EXPERIENCE_STATUSES:
        raise AtlasV2ValidationError(f"HistoricalExperienceRecord invalid status: {status}")
    for field in (
        "confidence",
        "regret_score",
        "calibration_error",
        "experience_quality_score",
        "expected_learning_value_pre_outcome",
        "actual_learning_value_post_outcome",
    ):
        _require_zero_to_one(record, field, "HistoricalExperienceRecord")
    for field in ("expected_learning_source_fields", "actual_learning_source_fields"):
        value = record.get(field)
        if not isinstance(value, list) or not value:
            raise AtlasV2ValidationError(f"HistoricalExperienceRecord {field} must be a non-empty list")
        if any(not isinstance(item, str) or _is_missing(item) for item in value):
            raise AtlasV2ValidationError(f"HistoricalExperienceRecord {field} must contain non-empty strings")
    expected = str(record.get("expected_outcome") or "").strip().lower()
    actual = str(record.get("actual_outcome") or "").strip().lower()
    if actual in FAILED_STATUSES and expected in UNKNOWN_STATUSES:
        raise AtlasV2ValidationError("HistoricalExperienceRecord cannot collapse unknown into failed")
    if status == "CONVERTED" and _is_missing(record.get("converted_experience_event_id")):
        raise AtlasV2ValidationError("HistoricalExperienceRecord CONVERTED requires converted_experience_event_id")
    if status != "CONVERTED" and record.get("converted_experience_event_id") not in ("NONE", "", None):
        raise AtlasV2ValidationError("HistoricalExperienceRecord non-converted status cannot reference ExperienceEvent")
    if status == "CONVERTED" and _same_number(
        float(record["expected_learning_value_pre_outcome"]),
        float(record["actual_learning_value_post_outcome"]),
    ) and _same_number(float(record["expected_learning_value_pre_outcome"]), float(record["experience_quality_score"])):
        raise AtlasV2ValidationError("HistoricalExperienceRecord cannot use experience_quality_score as both expected and actual learning")
    _assert_no_forbidden_authority_fields(record, "HistoricalExperienceRecord")


def validate_external_strategy_source(record: dict[str, Any]) -> None:
    source_type = str(record.get("source_type") or "")
    if source_type not in EXTERNAL_STRATEGY_SOURCE_TYPES:
        raise AtlasV2ValidationError(f"ExternalStrategySource invalid source_type: {source_type}")
    if record.get("transcript_available") not in (True, False):
        raise AtlasV2ValidationError("ExternalStrategySource transcript_available must be boolean")
    if _is_missing(record.get("input_text_hash")):
        raise AtlasV2ValidationError("ExternalStrategySource requires input_text_hash")
    if _is_missing(record.get("source_title")):
        raise AtlasV2ValidationError("ExternalStrategySource requires source_title")
    _assert_no_forbidden_authority_fields(record, "ExternalStrategySource")


def validate_external_strategy_claim(record: dict[str, Any]) -> None:
    status = str(record.get("extraction_status") or "")
    if status not in EXTERNAL_STRATEGY_EXTRACTION_STATUSES:
        raise AtlasV2ValidationError(f"ExternalStrategyClaim invalid extraction_status: {status}")
    _require_zero_to_one(record, "confidence", "ExternalStrategyClaim")
    truth_terms = ("profitable", "validated", "proven", "guaranteed", "trade advice", "recommendation")
    claim_text = str(record.get("claim_text") or "").lower()
    if any(term in claim_text for term in truth_terms):
        raise AtlasV2ValidationError("ExternalStrategyClaim cannot assert truth, profitability, validation, or advice")
    _assert_no_forbidden_authority_fields(record, "ExternalStrategyClaim")


def validate_external_strategy_mechanism(record: dict[str, Any]) -> None:
    family = str(record.get("mechanism_family") or "")
    if family not in EXTERNAL_STRATEGY_MECHANISM_FAMILIES:
        raise AtlasV2ValidationError(f"ExternalStrategyMechanism invalid mechanism_family: {family}")
    _require_zero_to_one(record, "classification_confidence", "ExternalStrategyMechanism")
    _assert_no_forbidden_authority_fields(record, "ExternalStrategyMechanism")



def validate_external_strategy_contrarian_theory(record: dict[str, Any]) -> None:
    failure_modes = record.get("primary_failure_modes")
    if not isinstance(failure_modes, list) or not failure_modes:
        raise AtlasV2ValidationError("ExternalStrategyContrarianTheory requires non-empty primary_failure_modes")
    invalid_modes = [mode for mode in failure_modes if str(mode) not in EXTERNAL_STRATEGY_PRIMARY_FAILURE_MODES]
    if invalid_modes:
        raise AtlasV2ValidationError(f"ExternalStrategyContrarianTheory invalid primary_failure_modes: {invalid_modes}")
    status = str(record.get("status") or "")
    if status not in EXTERNAL_STRATEGY_CONTRARIAN_STATUSES:
        raise AtlasV2ValidationError(f"ExternalStrategyContrarianTheory invalid status: {status}")
    if "VAGUE_RULES" in failure_modes and status != "REQUIRES_CLARIFICATION":
        raise AtlasV2ValidationError("ExternalStrategyContrarianTheory VAGUE_RULES requires REQUIRES_CLARIFICATION status")
    if "UNKNOWN" in failure_modes and status not in {"UNKNOWN", "REQUIRES_CLARIFICATION"}:
        raise AtlasV2ValidationError("ExternalStrategyContrarianTheory UNKNOWN failure mode requires UNKNOWN status")
    for field in ("fragility_conditions", "required_falsification_tests"):
        _require_non_empty_list(record, field, "ExternalStrategyContrarianTheory")
    matches = record.get("prior_failure_matches")
    if not isinstance(matches, list):
        raise AtlasV2ValidationError("ExternalStrategyContrarianTheory prior_failure_matches must be a list")
    for match in matches:
        if not isinstance(match, dict):
            raise AtlasV2ValidationError("ExternalStrategyContrarianTheory prior_failure_matches entries must be objects")
        if _is_missing(match.get("provenance_reference")):
            raise AtlasV2ValidationError("ExternalStrategyContrarianTheory prior_failure_matches require provenance_reference")
    _require_zero_to_one(record, "contrarian_confidence", "ExternalStrategyContrarianTheory")
    _assert_no_forbidden_authority_fields(record, "ExternalStrategyContrarianTheory")

def validate_external_strategy_deduplication_result(record: dict[str, Any]) -> None:
    if record.get("is_duplicate") not in (True, False):
        raise AtlasV2ValidationError("ExternalStrategyDeduplicationResult is_duplicate must be boolean")
    if record.get("is_duplicate") is True and _is_missing(record.get("duplicate_reason")):
        raise AtlasV2ValidationError("ExternalStrategyDeduplicationResult duplicate requires duplicate_reason")
    if record.get("is_duplicate") is False and not _is_missing(record.get("duplicate_of_claim_id")):
        raise AtlasV2ValidationError("ExternalStrategyDeduplicationResult non-duplicate cannot set duplicate_of_claim_id")
    _assert_no_forbidden_authority_fields(record, "ExternalStrategyDeduplicationResult")


def validate_external_strategy_cheap_experiment_handoff(record: dict[str, Any]) -> None:
    tier = str(record.get("recommended_tier") or "")
    if tier not in EXTERNAL_STRATEGY_HANDOFF_TIERS:
        raise AtlasV2ValidationError(f"ExternalStrategyCheapExperimentHandoff invalid recommended_tier: {tier}")
    if tier in {"TIER_1_SANITY", "TIER_2_LIGHTWEIGHT_VALIDATION"} and _is_missing(record.get("contrarian_id")):
        raise AtlasV2ValidationError("ExternalStrategyCheapExperimentHandoff TIER_1/TIER_2 requires contrarian theory")
    if record.get("eligible_for_cheap_experiment") not in (True, False):
        raise AtlasV2ValidationError("ExternalStrategyCheapExperimentHandoff eligible_for_cheap_experiment must be boolean")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ExternalStrategyCheapExperimentHandoff requires authority_boundary_acknowledged=true")
    _assert_no_forbidden_authority_fields(record, "ExternalStrategyCheapExperimentHandoff")


def validate_transcript_intake_request(record: dict[str, Any]) -> None:
    status = str(record.get("status") or "")
    if status not in TRANSCRIPT_INTAKE_STATUSES:
        raise AtlasV2ValidationError(f"TranscriptIntakeRequest invalid status: {status}")
    if _is_missing(record.get("source_title")):
        raise AtlasV2ValidationError("TranscriptIntakeRequest requires source_title")
    if _is_missing(record.get("transcript_text")):
        raise AtlasV2ValidationError("TranscriptIntakeRequest requires transcript_text")
    transcript_hash = str(record.get("transcript_hash") or "")
    if len(transcript_hash) != 64 or any(ch not in "0123456789abcdef" for ch in transcript_hash):
        raise AtlasV2ValidationError("TranscriptIntakeRequest requires sha256 transcript_hash")
    _assert_no_forbidden_authority_fields(record, "TranscriptIntakeRequest")


def validate_transcript_segment(record: dict[str, Any]) -> None:
    segment_type = str(record.get("segment_type") or "")
    if segment_type not in TRANSCRIPT_SEGMENT_TYPES:
        raise AtlasV2ValidationError(f"TranscriptSegment invalid segment_type: {segment_type}")
    start_offset = record.get("start_offset")
    end_offset = record.get("end_offset")
    if not isinstance(start_offset, int) or not isinstance(end_offset, int) or start_offset < 0 or end_offset <= start_offset:
        raise AtlasV2ValidationError("TranscriptSegment requires valid start_offset/end_offset")
    if _is_missing(record.get("segment_text")):
        raise AtlasV2ValidationError("TranscriptSegment requires segment_text")
    _assert_no_forbidden_authority_fields(record, "TranscriptSegment")


def validate_transcript_claim_candidate(record: dict[str, Any]) -> None:
    status = str(record.get("status") or "")
    if status not in TRANSCRIPT_CLAIM_CANDIDATE_STATUSES:
        raise AtlasV2ValidationError(f"TranscriptClaimCandidate invalid status: {status}")
    segment_ids = record.get("segment_ids")
    if not isinstance(segment_ids, list) or not segment_ids or any(_is_missing(item) for item in segment_ids):
        raise AtlasV2ValidationError("TranscriptClaimCandidate requires non-empty segment_ids")
    _require_zero_to_one(record, "confidence", "TranscriptClaimCandidate")
    text = str(record.get("candidate_text") or "").lower()
    forbidden_implications = ("profitable", "validated", "proven", "guaranteed", "trade advice", "recommendation")
    if any(term in text for term in forbidden_implications):
        raise AtlasV2ValidationError("TranscriptClaimCandidate cannot assert truth, profitability, validation, or advice")
    for key, value in record.items():
        normalized = str(key).lower()
        if normalized == "candidate_id":
            continue
        if normalized in FORBIDDEN_AUTHORITY_KEYS and value not in (None, "", False, [], {}):
            raise AtlasV2ValidationError(f"TranscriptClaimCandidate cannot set prohibited authority field: {key}")



def validate_claim_batch(record: dict[str, Any]) -> None:
    count = _require_number(record, "claim_count", "ClaimBatch")
    if count < 0 or count > 10000:
        raise AtlasV2ValidationError("ClaimBatch claim_count must be between 0 and 10000")
    tier = str(record.get("batch_size_tier") or "")
    if tier not in HIGH_VOLUME_CLAIM_BATCH_TIERS:
        raise AtlasV2ValidationError(f"ClaimBatch invalid batch_size_tier: {tier}")
    allowed = record.get("allowed_source_types")
    present = record.get("source_types_present")
    claim_ids = record.get("claim_ids")
    if not isinstance(allowed, list) or set(allowed) != HIGH_VOLUME_CLAIM_SOURCE_TYPES:
        raise AtlasV2ValidationError("ClaimBatch allowed_source_types must match high-volume claim source boundary")
    if not isinstance(present, list) or any(source not in HIGH_VOLUME_CLAIM_SOURCE_TYPES for source in present):
        raise AtlasV2ValidationError("ClaimBatch source_types_present contains unsupported source type")
    if not isinstance(claim_ids, list) or len(claim_ids) != int(count) or any(_is_missing(item) for item in claim_ids):
        raise AtlasV2ValidationError("ClaimBatch claim_ids must contain one id per ingested claim")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ClaimBatch requires authority_boundary_acknowledged=true")
    _assert_no_forbidden_authority_fields(record, "ClaimBatch")


def validate_claim_batch_run(record: dict[str, Any]) -> None:
    input_count = _require_number(record, "input_claim_count", "ClaimBatchRun")
    processed_count = _require_number(record, "processed_claim_count", "ClaimBatchRun")
    if input_count < 0 or input_count > 10000 or processed_count < 0 or processed_count > input_count:
        raise AtlasV2ValidationError("ClaimBatchRun claim counts must be non-negative, bounded at 10000, and processed<=input")
    steps = record.get("pipeline_steps")
    expected_steps = [
        "CLAIMS",
        "DEDUPE",
        "MECHANISM_CLASSIFICATION",
        "CONTRARIAN_THEORY",
        "MECHANISM_REGISTRY",
        "CHEAP_EXPERIMENT_ELIGIBILITY",
    ]
    if steps != expected_steps:
        raise AtlasV2ValidationError("ClaimBatchRun pipeline_steps must match Phase 1 pipeline")
    counts = record.get("emitted_record_counts")
    if not isinstance(counts, dict):
        raise AtlasV2ValidationError("ClaimBatchRun emitted_record_counts must be an object")
    for field in ("mechanism_record_ids", "dedupe_record_ids", "contrarian_record_ids", "handoff_record_ids"):
        value = record.get(field)
        if not isinstance(value, list) or len(value) > processed_count or any(_is_missing(item) for item in value):
            raise AtlasV2ValidationError(f"ClaimBatchRun {field} must be a bounded list")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ClaimBatchRun requires authority_boundary_acknowledged=true")
    _assert_no_forbidden_authority_fields(record, "ClaimBatchRun")


def validate_claim_batch_metrics(record: dict[str, Any]) -> None:
    total = _require_number(record, "total_claims", "ClaimBatchMetrics")
    unique_claims = _require_number(record, "unique_claims", "ClaimBatchMetrics")
    unique_mechanisms = _require_number(record, "unique_mechanisms", "ClaimBatchMetrics")
    duplicate_ratio = _require_number(record, "duplicate_ratio", "ClaimBatchMetrics")
    claims_deduped = _require_number(record, "claims_deduped", "ClaimBatchMetrics")
    mechanisms_reused = _require_number(record, "mechanisms_reused", "ClaimBatchMetrics")
    contrarian_coverage = _require_number(record, "contrarian_coverage", "ClaimBatchMetrics")
    cheap_coverage = _require_number(record, "cheap_experiment_coverage", "ClaimBatchMetrics")
    if total < 0 or total > 10000 or unique_claims < 0 or unique_mechanisms < 0:
        raise AtlasV2ValidationError("ClaimBatchMetrics claim counts must be non-negative and bounded at 10000")
    if unique_claims > total:
        raise AtlasV2ValidationError("ClaimBatchMetrics unique_claims cannot exceed total_claims")
    for field_name, value in (("duplicate_ratio", duplicate_ratio), ("contrarian_coverage", contrarian_coverage), ("cheap_experiment_coverage", cheap_coverage)):
        if value < 0 or value > 1:
            raise AtlasV2ValidationError(f"ClaimBatchMetrics {field_name} must be between 0 and 1")
    for field_name in ("claims_ingested", "claims_deduped", "mechanisms_discovered", "mechanisms_reused"):
        value = record.get(field_name)
        if not isinstance(value, int) or value < 0:
            raise AtlasV2ValidationError(f"ClaimBatchMetrics {field_name} must be a non-negative integer")
    if not isinstance(record.get("mechanism_distribution"), dict):
        raise AtlasV2ValidationError("ClaimBatchMetrics mechanism_distribution must be an object")
    cheap = record.get("cheap_experiment_candidates")
    if not isinstance(cheap, list):
        raise AtlasV2ValidationError("ClaimBatchMetrics cheap_experiment_candidates must be a list")
    if len(cheap) > unique_mechanisms:
        raise AtlasV2ValidationError("ClaimBatchMetrics cheap experiment routing must remain bounded by unique mechanisms")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ClaimBatchMetrics requires authority_boundary_acknowledged=true")
    _assert_no_forbidden_authority_fields(record, "ClaimBatchMetrics")


def validate_mechanism_registry(record: dict[str, Any]) -> None:
    family = str(record.get("mechanism_family") or "")
    if family not in EXTERNAL_STRATEGY_MECHANISM_FAMILIES:
        raise AtlasV2ValidationError(f"MechanismRegistry invalid mechanism_family: {family}")
    for field in ("claim_count", "source_count", "contrarian_count", "cheap_experiment_count"):
        value = record.get(field)
        if not isinstance(value, int) or value < 0:
            raise AtlasV2ValidationError(f"MechanismRegistry {field} must be a non-negative integer")
    _assert_no_forbidden_authority_fields(record, "MechanismRegistry")


def validate_mechanism_registry_entry(record: dict[str, Any]) -> None:
    family = str(record.get("mechanism_family") or "")
    if family not in EXTERNAL_STRATEGY_MECHANISM_FAMILIES:
        raise AtlasV2ValidationError(f"MechanismRegistryEntry invalid mechanism_family: {family}")
    for field in ("claim_count", "source_count", "contrarian_count", "cheap_experiment_eligibility_count", "learning_event_count"):
        value = record.get(field)
        if not isinstance(value, int) or value < 0:
            raise AtlasV2ValidationError(f"MechanismRegistryEntry {field} must be a non-negative integer")
    status = str(record.get("status") or "")
    if family == "UNKNOWN" and status not in {"RETAINED_NOT_PROMOTED", "UNKNOWN_RETAINED"}:
        raise AtlasV2ValidationError("MechanismRegistryEntry UNKNOWN mechanisms must be retained but not promoted")
    if status in {"PROMOTED", "VALIDATED", "RECOMMENDED"}:
        raise AtlasV2ValidationError("MechanismRegistryEntry cannot promote, validate, or recommend mechanisms")
    _assert_no_forbidden_authority_fields(record, "MechanismRegistryEntry")


def validate_mechanism_cluster(record: dict[str, Any]) -> None:
    family = str(record.get("mechanism_family") or "")
    if family not in EXTERNAL_STRATEGY_MECHANISM_FAMILIES:
        raise AtlasV2ValidationError(f"MechanismCluster invalid mechanism_family: {family}")
    claim_ids = record.get("claim_ids")
    if not isinstance(claim_ids, list) or not claim_ids or any(_is_missing(item) for item in claim_ids):
        raise AtlasV2ValidationError("MechanismCluster requires non-empty claim_ids")
    if int(record.get("claim_count", -1)) < len(claim_ids):
        raise AtlasV2ValidationError("MechanismCluster claim_count must cover representative claim_ids")
    if not isinstance(record.get("source_diversity"), int) or record["source_diversity"] < 0:
        raise AtlasV2ValidationError("MechanismCluster source_diversity must be a non-negative integer")
    _require_zero_to_one(record, "confidence", "MechanismCluster")
    status = str(record.get("status") or "")
    if status in {"PROMOTED", "VALIDATED", "RECOMMENDED"}:
        raise AtlasV2ValidationError("MechanismCluster cannot promote, validate, or recommend mechanisms")
    _assert_no_forbidden_authority_fields(record, "MechanismCluster")


def validate_mechanism_lineage(record: dict[str, Any]) -> None:
    if _is_missing(record.get("mechanism_id")):
        raise AtlasV2ValidationError("MechanismLineage requires mechanism_id")
    if "parent_mechanism_id" in record and _is_missing(record.get("parent_mechanism_id")):
        raise AtlasV2ValidationError("MechanismLineage parent_mechanism_id must be non-empty when present")
    for field in ("variant_description", "lineage_reason", "status"):
        if _is_missing(record.get(field)):
            raise AtlasV2ValidationError(f"MechanismLineage requires {field}")
    if str(record.get("status") or "") in {"PROMOTED", "VALIDATED", "RECOMMENDED"}:
        raise AtlasV2ValidationError("MechanismLineage cannot promote, validate, or recommend mechanisms")
    _assert_no_forbidden_authority_fields(record, "MechanismLineage")


def validate_mechanism_metrics(record: dict[str, Any]) -> None:
    claim_count = _require_number(record, "claim_count", "MechanismMetrics")
    duplicate_ratio = _require_number(record, "duplicate_ratio", "MechanismMetrics")
    contrarian_coverage = _require_number(record, "contrarian_coverage", "MechanismMetrics")
    cheap_coverage = _require_number(record, "cheap_experiment_coverage", "MechanismMetrics")
    learning_event_count = _require_number(record, "learning_event_count", "MechanismMetrics")
    if claim_count < 0 or learning_event_count < 0:
        raise AtlasV2ValidationError("MechanismMetrics counts must be non-negative")
    for field_name, value in (("duplicate_ratio", duplicate_ratio), ("contrarian_coverage", contrarian_coverage), ("cheap_experiment_coverage", cheap_coverage)):
        if value < 0 or value > 1:
            raise AtlasV2ValidationError(f"MechanismMetrics {field_name} must be between 0 and 1")
    _assert_no_forbidden_authority_fields(record, "MechanismMetrics")


def validate_claim_cluster_summary(record: dict[str, Any]) -> None:
    clusters = record.get("clusters")
    if not isinstance(clusters, list):
        raise AtlasV2ValidationError("ClaimClusterSummary clusters must be a list")
    if int(record.get("cluster_count", -1)) != len(clusters):
        raise AtlasV2ValidationError("ClaimClusterSummary cluster_count must match clusters")
    if _is_missing(record.get("dedupe_strategy")):
        raise AtlasV2ValidationError("ClaimClusterSummary requires dedupe_strategy")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ClaimClusterSummary requires authority_boundary_acknowledged=true")
    _assert_no_forbidden_authority_fields(record, "ClaimClusterSummary")



def _assert_no_authority_text(record: dict[str, Any], object_type: str, fields: tuple[str, ...]) -> None:
    forbidden_terms = (
        "profitable",
        "profitability",
        "trade advice",
        "recommendation",
        "recommended",
        "validated",
        "proven",
        "guaranteed",
        "broker",
        "allocation",
        "paper position",
        "candidate",
        "sleeve",
    )
    for field in fields:
        lowered = str(record.get(field) or "").lower()
        if any(term in lowered for term in forbidden_terms):
            raise AtlasV2ValidationError(f"{object_type} cannot imply profitability, advice, validation, execution, or allocation authority")




def validate_generated_research_claim(record: dict[str, Any]) -> None:
    source_type = str(record.get("source_type") or "")
    if source_type not in CLAIM_IDEA_SOURCE_TYPES:
        raise AtlasV2ValidationError(f"GeneratedResearchClaim invalid source_type: {source_type}")
    family = str(record.get("mechanism_family") or "")
    if family not in EXTERNAL_STRATEGY_MECHANISM_FAMILIES:
        raise AtlasV2ValidationError(f"GeneratedResearchClaim invalid mechanism_family: {family}")
    status = str(record.get("status") or "")
    if status not in GENERATED_RESEARCH_CLAIM_STATUSES:
        raise AtlasV2ValidationError(f"GeneratedResearchClaim invalid status: {status}")
    for field in ("expected_learning_value", "uncertainty_score"):
        _require_zero_to_one(record, field, "GeneratedResearchClaim")
    for field in ("source_event_ids", "source_mechanism_ids"):
        value = record.get(field)
        if not isinstance(value, list) or any(_is_missing(item) for item in value):
            raise AtlasV2ValidationError(f"GeneratedResearchClaim {field} must be a list of ids")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("GeneratedResearchClaim requires authority_boundary_acknowledged=true")
    for field in ("claim_text", "rationale", "novelty_basis", "contrarian_prompt", "intended_testability"):
        if _is_missing(record.get(field)):
            raise AtlasV2ValidationError(f"GeneratedResearchClaim requires {field}")
    if status == "GENERATED":
        if family == "UNKNOWN":
            raise AtlasV2ValidationError("GeneratedResearchClaim GENERATED records require supported mechanism_family")
        testability_text = str(record.get("intended_testability") or "").lower()
        required_markers = ("baseline", "falsif", "compare", "observation", "threshold", "rule")
        if not any(marker in testability_text for marker in required_markers):
            raise AtlasV2ValidationError("GeneratedResearchClaim GENERATED records require explicit intended_testability")
    if status in {"DUPLICATE", "INSUFFICIENT_BASIS", "UNSUPPORTED_MECHANISM", "REJECTED_LOW_TESTABILITY"} and float(record.get("expected_learning_value", 0)) > 0.35:
        raise AtlasV2ValidationError("GeneratedResearchClaim rejected records must keep expected_learning_value low")
    _assert_no_authority_text(
        record,
        "GeneratedResearchClaim",
        ("claim_text", "rationale", "novelty_basis", "contrarian_prompt", "intended_testability"),
    )
    _assert_no_forbidden_authority_fields(record, "GeneratedResearchClaim")


def validate_claim_generation_run(record: dict[str, Any]) -> None:
    status = str(record.get("status") or "")
    if status not in CLAIM_GENERATION_RUN_STATUSES:
        raise AtlasV2ValidationError(f"ClaimGenerationRun invalid status: {status}")
    source_inputs = record.get("source_inputs", [])
    if not isinstance(source_inputs, list):
        raise AtlasV2ValidationError("ClaimGenerationRun source_inputs must be a list")
    max_claims = _require_number(record, "max_claims", "ClaimGenerationRun")
    claims_generated = _require_number(record, "claims_generated", "ClaimGenerationRun")
    duplicates = _require_number(record, "duplicates_detected", "ClaimGenerationRun")
    insufficient = _require_number(record, "insufficient_basis_count", "ClaimGenerationRun")
    for field_name, value in (
        ("max_claims", max_claims),
        ("claims_generated", claims_generated),
        ("duplicates_detected", duplicates),
        ("insufficient_basis_count", insufficient),
    ):
        if value < 0 or int(value) != value:
            raise AtlasV2ValidationError(f"ClaimGenerationRun {field_name} must be a non-negative integer")
    if max_claims > 100:
        raise AtlasV2ValidationError("ClaimGenerationRun max_claims must be at most 100")
    if claims_generated > max_claims:
        raise AtlasV2ValidationError("ClaimGenerationRun claims_generated cannot exceed max_claims")
    distribution = record.get("mechanism_distribution")
    if not isinstance(distribution, dict):
        raise AtlasV2ValidationError("ClaimGenerationRun mechanism_distribution must be an object")
    for family, count in distribution.items():
        if str(family) not in EXTERNAL_STRATEGY_MECHANISM_FAMILIES:
            raise AtlasV2ValidationError(f"ClaimGenerationRun invalid mechanism_distribution family: {family}")
        if not isinstance(count, int) or count < 0:
            raise AtlasV2ValidationError("ClaimGenerationRun mechanism_distribution counts must be non-negative integers")
    _assert_no_forbidden_authority_fields(record, "ClaimGenerationRun")


def validate_claim_generation_source_summary(record: dict[str, Any]) -> None:
    source_type = str(record.get("source_type") or "")
    if source_type not in CLAIM_IDEA_SOURCE_TYPES:
        raise AtlasV2ValidationError(f"ClaimGenerationSourceSummary invalid source_type: {source_type}")
    for field in ("source_count", "generated_count", "duplicate_count", "rejected_count"):
        value = _require_number(record, field, "ClaimGenerationSourceSummary")
        if value < 0 or int(value) != value:
            raise AtlasV2ValidationError(f"ClaimGenerationSourceSummary {field} must be a non-negative integer")
    if int(record["generated_count"]) + int(record["duplicate_count"]) + int(record["rejected_count"]) > int(record["source_count"]):
        raise AtlasV2ValidationError("ClaimGenerationSourceSummary emitted counts cannot exceed source_count")
    _assert_no_forbidden_authority_fields(record, "ClaimGenerationSourceSummary")

def validate_research_hypothesis(record: dict[str, Any]) -> None:
    status = str(record.get("status") or "")
    if status not in RESEARCH_HYPOTHESIS_STATUSES:
        raise AtlasV2ValidationError(f"ResearchHypothesis invalid status: {status}")
    _require_zero_to_one(record, "confidence", "ResearchHypothesis")
    for field in ("required_data", "falsification_criteria", "contrarian_inputs"):
        _require_non_empty_list(record, field, "ResearchHypothesis")
    for field in ("hypothesis_text", "testable_condition", "expected_direction", "baseline_comparison", "mechanism_family"):
        if _is_missing(record.get(field)):
            raise AtlasV2ValidationError(f"ResearchHypothesis requires {field}")
    if record.get("authority_boundary_acknowledged") is not True:
        raise AtlasV2ValidationError("ResearchHypothesis requires authority_boundary_acknowledged=true")
    family = str(record.get("mechanism_family") or "")
    if family == "UNKNOWN" and status not in {"UNSUPPORTED_MECHANISM", "INSUFFICIENT_DETAIL"}:
        raise AtlasV2ValidationError("ResearchHypothesis UNKNOWN mechanisms cannot be promoted to generated hypotheses")
    if status in {"GENERATED", "TESTABLE"}:
        vague_markers = ("unclear", "vague", "insufficient", "not enough detail", "more detail")
        combined = " ".join(str(record.get(field) or "") for field in ("hypothesis_text", "testable_condition", "baseline_comparison")).lower()
        if any(marker in combined for marker in vague_markers):
            raise AtlasV2ValidationError("ResearchHypothesis generated records cannot be vague")
    if status in {"INSUFFICIENT_DETAIL", "UNSUPPORTED_MECHANISM", "REJECTED_LOW_SPECIFICITY"} and float(record.get("confidence", 0)) > 0.35:
        raise AtlasV2ValidationError("ResearchHypothesis non-generated confidence must remain low")
    _assert_no_authority_text(
        record,
        "ResearchHypothesis",
        ("hypothesis_text", "testable_condition", "expected_direction", "baseline_comparison"),
    )
    _assert_no_forbidden_authority_fields(record, "ResearchHypothesis")


def validate_hypothesis_falsification_plan(record: dict[str, Any]) -> None:
    status = str(record.get("status") or "")
    if status not in HYPOTHESIS_FALSIFICATION_PLAN_STATUSES:
        raise AtlasV2ValidationError(f"HypothesisFalsificationPlan invalid status: {status}")
    for field in ("failure_modes", "required_tests"):
        _require_non_empty_list(record, field, "HypothesisFalsificationPlan")
    invalid_modes = [mode for mode in record.get("failure_modes", []) if str(mode) not in EXTERNAL_STRATEGY_PRIMARY_FAILURE_MODES]
    if invalid_modes:
        raise AtlasV2ValidationError(f"HypothesisFalsificationPlan invalid failure_modes: {invalid_modes}")
    count = record.get("minimum_sample_requirement")
    if not isinstance(count, int) or count < 0:
        raise AtlasV2ValidationError("HypothesisFalsificationPlan minimum_sample_requirement must be a non-negative integer")
    for field in ("baseline_requirement", "falsification_threshold"):
        if _is_missing(record.get(field)):
            raise AtlasV2ValidationError(f"HypothesisFalsificationPlan requires {field}")
    _assert_no_authority_text(record, "HypothesisFalsificationPlan", ())
    _assert_no_forbidden_authority_fields(record, "HypothesisFalsificationPlan")


def validate_hypothesis_generation_run(record: dict[str, Any]) -> None:
    status = str(record.get("status") or "")
    if status not in HYPOTHESIS_GENERATION_RUN_STATUSES:
        raise AtlasV2ValidationError(f"HypothesisGenerationRun invalid status: {status}")
    claims_processed = _require_number(record, "claims_processed", "HypothesisGenerationRun")
    hypotheses_created = _require_number(record, "hypotheses_created", "HypothesisGenerationRun")
    insufficient_detail = _require_number(record, "insufficient_detail_count", "HypothesisGenerationRun")
    duplicate_count = _require_number(record, "duplicate_hypothesis_count", "HypothesisGenerationRun")
    for field_name, value in (
        ("claims_processed", claims_processed),
        ("hypotheses_created", hypotheses_created),
        ("insufficient_detail_count", insufficient_detail),
        ("duplicate_hypothesis_count", duplicate_count),
    ):
        if value < 0 or int(value) != value:
            raise AtlasV2ValidationError(f"HypothesisGenerationRun {field_name} must be a non-negative integer")
    if hypotheses_created > claims_processed:
        raise AtlasV2ValidationError("HypothesisGenerationRun hypotheses_created cannot exceed claims_processed")
    if insufficient_detail + duplicate_count > claims_processed:
        raise AtlasV2ValidationError("HypothesisGenerationRun detail and duplicate counts cannot exceed claims_processed")
    _assert_no_forbidden_authority_fields(record, "HypothesisGenerationRun")

def object_key(record: dict[str, Any]) -> str:
    object_type = str(record.get("object_type") or "")
    id_field = OBJECT_ID_FIELDS.get(object_type)
    if not id_field:
        raise AtlasV2ValidationError(f"unknown Atlas V2 object_type: {object_type}")
    object_id = record.get(id_field)
    if _is_missing(object_id):
        raise AtlasV2ValidationError(f"{object_type} missing id field: {id_field}")
    return str(object_id)


@dataclass(frozen=True)
class AtlasV2AuditResult:
    ok: bool
    failures: tuple[str, ...]


class AtlasV2Ledger:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def ledger_path(self, object_type: str) -> Path:
        if object_type not in OBJECT_ID_FIELDS:
            raise AtlasV2ValidationError(f"unknown Atlas V2 object_type: {object_type}")
        return self.root / f"{object_type}.jsonl"

    def append(self, record: dict[str, Any]) -> dict[str, Any]:
        validate_object(record)
        path = self.ledger_path(str(record["object_type"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(path.read_text(encoding="utf-8") if path.exists() else "", encoding="utf-8")
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
        return record

    def records(self, object_type: str) -> list[dict[str, Any]]:
        path = self.ledger_path(object_type)
        if not path.exists():
            return []
        records: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                payload = json.loads(line)
                if isinstance(payload, dict):
                    records.append(payload)
        return records

    def latest_by_id(self, object_type: str) -> dict[str, dict[str, Any]]:
        latest: dict[str, dict[str, Any]] = {}
        for record in self.records(object_type):
            latest[object_key(record)] = record
        return latest

    def latest(self, object_type: str, object_id: str) -> dict[str, Any]:
        latest = self.latest_by_id(object_type).get(object_id)
        if latest is None:
            raise AtlasV2ValidationError(f"{object_type} not found: {object_id}")
        return latest

    def create_record(self, object_type: str, payload: dict[str, Any], *, reason: str, triggering_object: str) -> dict[str, Any]:
        created_at = str(payload.get("created_at") or utc_now_iso())
        status = str(payload.get("status") or "recorded")
        record = {
            "object_type": object_type,
            "created_at": created_at,
            **payload,
            "transition_history": [
                initial_transition(
                    timestamp=created_at,
                    status=status,
                    reason=reason,
                    triggering_object=triggering_object,
                )
            ],
        }
        return self.append(record)

    def transition_object(
        self,
        object_type: str,
        object_id: str,
        *,
        to_status: str,
        reason: str,
        triggering_object: str,
        transitioned_at: str | None = None,
    ) -> dict[str, Any]:
        previous = self.latest(object_type, object_id)
        current_status = str(previous.get("status") or previous["transition_history"][-1]["to_status"])
        transition = {
            "transitioned_at": transitioned_at or utc_now_iso(),
            "from_status": current_status,
            "to_status": to_status,
            "reason": reason,
            "triggering_object": triggering_object,
        }
        validate_transition(transition)
        next_record = deepcopy(previous)
        next_record["status"] = to_status
        next_record["transition_history"] = list(previous["transition_history"]) + [transition]
        return self.append(next_record)

    def promote_cheap_experiment(
        self,
        experiment_id: str,
        *,
        to_tier: str,
        gate_id: str | None = None,
        evidence_maturity_rationale: str | None = None,
        transitioned_at: str | None = None,
    ) -> dict[str, Any]:
        previous = self.latest("CheapExperiment", experiment_id)
        from_tier = str(previous["tier"])
        if to_tier not in CHEAP_EXPERIMENT_TIERS:
            raise AtlasV2ValidationError(f"CheapExperiment invalid promotion tier: {to_tier}")
        if to_tier in LOW_VOLUME_GATE_TIERS:
            if not gate_id:
                raise AtlasV2ValidationError(f"CheapExperiment promotion to {to_tier} requires PromotionGateDecision")
            gate = self.latest("PromotionGateDecision", gate_id)
            if gate["experiment_id"] != experiment_id or gate["from_tier"] != from_tier or gate["to_tier"] != to_tier:
                raise AtlasV2ValidationError("PromotionGateDecision does not match cheap experiment tier transition")
            if gate["decision"] != "APPROVED":
                raise AtlasV2ValidationError("PromotionGateDecision must be APPROVED before promotion")
        if to_tier == "TIER_4_MATURITY_TRACKING" and _is_missing(evidence_maturity_rationale or previous.get("evidence_maturity_rationale")):
            raise AtlasV2ValidationError("CheapExperiment promotion to TIER_4_MATURITY_TRACKING requires evidence_maturity_rationale")
        transition = {
            "transitioned_at": transitioned_at or utc_now_iso(),
            "from_status": from_tier,
            "to_status": to_tier,
            "reason": "cheap experiment tier promotion",
            "triggering_object": f"PromotionGateDecision:{gate_id}" if gate_id else f"CheapExperiment:{experiment_id}",
        }
        validate_transition(transition)
        next_record = deepcopy(previous)
        next_record["tier"] = to_tier
        next_record["status"] = "promoted"
        if gate_id:
            next_record["promotion_gate_id"] = gate_id
        if evidence_maturity_rationale:
            next_record["evidence_maturity_rationale"] = evidence_maturity_rationale
        next_record["transition_history"] = list(previous["transition_history"]) + [transition]
        return self.append(next_record)

    def build_learning_velocity_metric(self, *, metric_id: str, period_start: str, period_end: str) -> dict[str, Any]:
        experiments = list(self.latest_by_id("CheapExperiment").values())
        completed = [record for record in experiments if str(record.get("status")) == "completed" and not _is_missing(record.get("outcome_summary"))]
        cycles = len(completed)
        actual_learning_total = round(sum(float(record.get("actual_learning_value", 0)) for record in completed), 4)
        promotions = [record for record in experiments if str(record.get("status")) == "promoted"]
        rejections = [record for record in experiments if str(record.get("status")) == "rejected"]
        promotion_count = len(promotions)
        decision_count = promotion_count + len(rejections)
        promotion_rate = round(promotion_count / decision_count, 4) if decision_count else 0.0
        regrets = self.records("Regret")
        importance_weighted_regret_total = round(sum(float(record.get("importance_weighted_regret", 0)) for record in regrets), 4)
        return self.create_record(
            "LearningVelocityMetric",
            {
                "metric_id": metric_id,
                "period_start": period_start,
                "period_end": period_end,
                "prediction_outcome_cycles": cycles,
                "cheap_experiments_completed": cycles,
                "actual_learning_total": actual_learning_total,
                "average_learning_per_cycle": round(actual_learning_total / cycles, 4) if cycles else 0.0,
                "rejection_count": len(rejections),
                "promotion_count": promotion_count,
                "promotion_rate": promotion_rate,
                "importance_weighted_regret_total": importance_weighted_regret_total,
            },
            reason="learning velocity computed from append-only cheap experiment ledger",
            triggering_object="CheapExperiment:*",
        )

    def build_experiment_tier_summary(self, *, summary_id: str, period_start: str, period_end: str, tier: str) -> dict[str, Any]:
        if tier not in CHEAP_EXPERIMENT_TIERS:
            raise AtlasV2ValidationError(f"ExperimentTierSummary invalid tier: {tier}")
        experiments = [record for record in self.latest_by_id("CheapExperiment").values() if record["tier"] == tier]
        run_count = len(experiments)
        actual_learning_total = round(sum(float(record.get("actual_learning_value", 0)) for record in experiments), 4)
        cost_total = round(sum(float(record.get("attention_cost_estimate", 0)) for record in experiments), 4)
        return self.create_record(
            "ExperimentTierSummary",
            {
                "summary_id": summary_id,
                "period_start": period_start,
                "period_end": period_end,
                "tier": tier,
                "experiments_run": run_count,
                "experiments_rejected": sum(1 for record in experiments if str(record.get("status")) == "rejected"),
                "experiments_promoted": sum(1 for record in experiments if str(record.get("status")) == "promoted"),
                "actual_learning_total": actual_learning_total,
                "average_cost_estimate": round(cost_total / run_count, 4) if run_count else 0.0,
                "average_learning_value": round(actual_learning_total / run_count, 4) if run_count else 0.0,
            },
            reason="tier summary computed from append-only cheap experiment ledger",
            triggering_object="CheapExperiment:*",
        )


    def run_cheap_experiment_batch(
        self,
        *,
        batch_id: str,
        fixtures: list[dict[str, Any]],
        period_start: str,
        period_end: str,
        created_at: str | None = None,
    ) -> dict[str, Any]:
        timestamp = created_at or utc_now_iso()
        accepted_experiments: list[dict[str, Any]] = []
        rejected_fixtures: list[dict[str, Any]] = []
        regrets: list[dict[str, Any]] = []
        calibrations: list[dict[str, Any]] = []
        experience_events: list[dict[str, Any]] = []
        emitted_counts: dict[str, int] = {
            "AttentionDecision": 0,
            "Prediction": 0,
            "Outcome": 0,
            "Regret": 0,
            "CalibrationRecord": 0,
            "ExperienceEvent": 0,
            "CheapExperiment": 0,
        }

        for index, fixture in enumerate(fixtures, start=1):
            tier = str(fixture.get("tier") or "")
            fixture_id = str(fixture.get("fixture_id") or f"fixture-{index:04d}")
            if tier in GATED_EXPERIMENT_TIERS and _is_missing(fixture.get("promotion_gate_id")):
                rejected_fixtures.append(
                    {
                        "fixture_id": fixture_id,
                        "tier": tier,
                        "reason": f"{tier} requires PromotionGateDecision",
                    }
                )
                continue
            if tier not in HIGH_VOLUME_EXPERIMENT_TIERS:
                rejected_fixtures.append(
                    {
                        "fixture_id": fixture_id,
                        "tier": tier,
                        "reason": "batch runner only executes TIER_0_DEDUPE and TIER_1_SANITY fixtures",
                    }
                )
                continue

            experiment_id = str(fixture.get("experiment_id") or f"{batch_id}-cheap-{index:04d}")
            decision_id = str(fixture.get("decision_id") or f"{experiment_id}-decision")
            prediction_id = str(fixture.get("prediction_id") or f"{experiment_id}-prediction")
            outcome_id = str(fixture.get("outcome_id") or f"{experiment_id}-outcome")
            regret_id = str(fixture.get("regret_id") or f"{experiment_id}-regret")
            calibration_id = str(fixture.get("calibration_id") or f"{experiment_id}-calibration")
            prediction_statement = str(fixture.get("prediction_statement") or "Fixture cheap experiment will produce bounded learning evidence.")
            outcome_summary = str(fixture.get("outcome_summary") or "Fixture outcome observed for cheap experiment.")
            expected_learning = float(fixture.get("expected_learning_value", 0.1))
            attention_cost = float(fixture.get("attention_cost_estimate", 0.01))
            actual_learning = float(fixture.get("actual_learning_value", expected_learning))
            importance_score = _bounded_probability(fixture.get("importance_score", 0.1), "CheapExperimentBatch importance_score")
            confidence = _bounded_probability(fixture.get("confidence", 0.5), "CheapExperimentBatch confidence")
            expected_result = bool(fixture.get("expected_result", True))
            actual_result = bool(fixture.get("actual_result", expected_result))
            matched_expected_outcome = (
                bool(fixture["matched_expected_outcome"])
                if "matched_expected_outcome" in fixture
                else actual_result == expected_result
            )
            calibration_error = round(abs((1.0 if actual_result else 0.0) - confidence), 6)
            regret_score = round(
                _bounded_probability(
                    fixture.get("regret_score", 0.05 if matched_expected_outcome else 0.45),
                    "CheapExperimentBatch regret_score",
                ),
                6,
            )

            self.create_record(
                "AttentionDecision",
                {
                    "decision_id": decision_id,
                    "created_at": timestamp,
                    "decision_type": "cheap_experiment_fixture_batch",
                    "uncertainty_target": prediction_statement,
                    "importance_score": importance_score,
                    "expected_learning_value": expected_learning,
                    "expected_regret_if_ignored": float(fixture.get("expected_regret_if_ignored", 0.0)),
                    "attention_cost": attention_cost,
                    "selected_action": "Run read-only cheap experiment fixture",
                    "rejected_alternatives": ["Create candidate", "Create sleeve", "Create paper position"],
                    "decision_reason": "Fixture batch supports high-volume prediction-outcome learning without authority expansion.",
                    "status": "recorded",
                },
                reason="cheap experiment batch decision recorded",
                triggering_object=f"CheapExperimentBatch:{batch_id}",
            )
            emitted_counts["AttentionDecision"] += 1
            self.create_record(
                "Prediction",
                {
                    "prediction_id": prediction_id,
                    "decision_id": decision_id,
                    "created_at": timestamp,
                    "prediction_statement": prediction_statement,
                    "confidence": confidence,
                    "expected_outcome": str(fixture.get("expected_outcome", outcome_summary)),
                    "evaluation_date": period_end,
                    "status": "evaluated",
                },
                reason="cheap experiment batch prediction recorded",
                triggering_object=f"AttentionDecision:{decision_id}",
            )
            emitted_counts["Prediction"] += 1
            self.create_record(
                "Outcome",
                {
                    "outcome_id": outcome_id,
                    "prediction_id": prediction_id,
                    "created_at": timestamp,
                    "observed_outcome": outcome_summary,
                    "outcome_date": period_end,
                    "matched_expected_outcome": matched_expected_outcome,
                    "outcome_confidence": float(fixture.get("outcome_confidence", 0.5)),
                    "evidence_reference": str(fixture.get("evidence_reference", f"fixtures/atlas_v2/{batch_id}/{fixture_id}")),
                },
                reason="cheap experiment batch outcome recorded",
                triggering_object=f"Prediction:{prediction_id}",
            )
            emitted_counts["Outcome"] += 1
            regret = self.create_record(
                "Regret",
                {
                    "regret_id": regret_id,
                    "decision_id": decision_id,
                    "outcome_id": outcome_id,
                    "created_at": timestamp,
                    "regret_score": regret_score,
                    "missed_alternative": str(fixture.get("missed_alternative", "No higher-authority action was needed.")),
                    "regret_reason": str(fixture.get("regret_reason", "Regret reflects residual mismatch or delayed cheap learning.")),
                    "importance_weighted_regret": round(importance_score * regret_score, 6),
                },
                reason="cheap experiment batch regret scored",
                triggering_object=f"Outcome:{outcome_id}",
            )
            regrets.append(regret)
            emitted_counts["Regret"] += 1
            calibration = self.create_record(
                "CalibrationRecord",
                {
                    "calibration_id": calibration_id,
                    "prediction_id": prediction_id,
                    "created_at": timestamp,
                    "confidence": confidence,
                    "actual_result": actual_result,
                    "calibration_error": calibration_error,
                    "calibration_bucket": _calibration_bucket(confidence),
                },
                reason="cheap experiment batch calibration scored",
                triggering_object=f"Outcome:{outcome_id}",
            )
            calibrations.append(calibration)
            emitted_counts["CalibrationRecord"] += 1
            experiment = self.create_record(
                "CheapExperiment",
                {
                    "experiment_id": experiment_id,
                    "created_at": timestamp,
                    "originating_object_id": prediction_id,
                    "originating_object_type": "Prediction",
                    "tier": tier,
                    "prediction_statement": prediction_statement,
                    "expected_learning_value": expected_learning,
                    "attention_cost_estimate": attention_cost,
                    "data_scope": str(fixture.get("data_scope", "fixed fixture mock historical record")),
                    "method_summary": str(fixture.get("method_summary", "Read-only fixture comparison of prediction statement to outcome summary.")),
                    "outcome_summary": outcome_summary,
                    "actual_learning_value": actual_learning,
                    "status": str(fixture.get("status", "completed")),
                    "batch_id": batch_id,
                },
                reason="cheap experiment batch record emitted",
                triggering_object=f"Prediction:{prediction_id}",
            )
            accepted_experiments.append(experiment)
            emitted_counts["CheapExperiment"] += 1
            if fixture.get("link_experience_event", True):
                experience_id = str(fixture.get("experience_id") or f"{experiment_id}-experience")
                event = self.create_record(
                    "ExperienceEvent",
                    {
                        "experience_id": experience_id,
                        "created_at": timestamp,
                        "source_decision_id": decision_id,
                        "prediction_id": prediction_id,
                        "outcome_id": outcome_id,
                        "regret_id": regret_id,
                        "calibration_id": calibration_id,
                        "lesson": str(fixture.get("lesson", "Cheap fixture prediction-outcome cycle produced bounded learning evidence.")),
                        "experience_quality_score": float(fixture.get("experience_quality_score", 0.5)),
                        "cheap_experiment_id": experiment_id,
                        "batch_id": batch_id,
                    },
                    reason="cheap experiment batch linked experience event",
                    triggering_object=f"CheapExperiment:{experiment_id}",
                )
                experience_events.append(event)
                emitted_counts["ExperienceEvent"] += 1

        batch = self.create_record(
            "CheapExperimentBatch",
            {
                "batch_id": batch_id,
                "created_at": timestamp,
                "batch_type": "MOCK_HISTORICAL_CHEAP_EXPERIMENT_BATCH",
                "data_mode": "mock_historical",
                "fixture_count": len(fixtures),
                "accepted_count": len(accepted_experiments),
                "rejected_count": len(rejected_fixtures),
                "emitted_record_counts": emitted_counts,
                "allowed_tiers": sorted(HIGH_VOLUME_EXPERIMENT_TIERS),
                "forbidden_authority_acknowledged": True,
                "status": "completed" if accepted_experiments else "rejected",
            },
            reason="cheap experiment fixture batch summarized",
            triggering_object="fixture_list:fixed",
        )
        metric = self.compute_learning_velocity_metric(
            metric_id=f"{batch_id}-learning-velocity",
            period_start=period_start,
            period_end=period_end,
            created_at=timestamp,
        )
        tier_summaries = [
            self.build_experiment_tier_summary(
                summary_id=f"{batch_id}-{tier.lower()}-summary",
                period_start=period_start,
                period_end=period_end,
                tier=tier,
            )
            for tier in sorted(HIGH_VOLUME_EXPERIMENT_TIERS)
        ]
        return {
            "batch": batch,
            "experiments": accepted_experiments,
            "regrets": regrets,
            "calibrations": calibrations,
            "experience_events": experience_events,
            "learning_velocity_metric": metric,
            "tier_summaries": tier_summaries,
            "rejected_fixtures": rejected_fixtures,
        }

    def audit_complete_experience_links(self) -> AtlasV2AuditResult:
        decisions = self.latest_by_id("AttentionDecision")
        predictions = self.latest_by_id("Prediction")
        outcomes = self.latest_by_id("Outcome")
        regrets = self.latest_by_id("Regret")
        calibrations = self.latest_by_id("CalibrationRecord")
        failures: list[str] = []
        for event in self.records("ExperienceEvent"):
            if event["source_decision_id"] not in decisions:
                failures.append(f"ExperienceEvent {event[experience_id]} missing decision {event[source_decision_id]}")
            if event["prediction_id"] not in predictions:
                failures.append(f"ExperienceEvent {event[experience_id]} missing prediction {event[prediction_id]}")
            if event["outcome_id"] not in outcomes:
                failures.append(f"ExperienceEvent {event[experience_id]} missing outcome {event[outcome_id]}")
            if event.get("regret_id") and event["regret_id"] not in regrets:
                failures.append(f"ExperienceEvent {event[experience_id]} missing regret {event[regret_id]}")
            if event.get("calibration_id") and event["calibration_id"] not in calibrations:
                failures.append(f"ExperienceEvent {event[experience_id]} missing calibration {event[calibration_id]}")
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))

    def audit_behavior_changes(self) -> AtlasV2AuditResult:
        failures: list[str] = []
        for change in self.records("BehaviorChange"):
            if not (change.get("triggering_regret_id") or change.get("triggering_calibration_id")):
                failures.append(f"BehaviorChange {change['behavior_change_id']} has no trigger")
            if _is_missing(change.get("change_reason")):
                failures.append(f"BehaviorChange {change['behavior_change_id']} has no change_reason")
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))

    def audit_wisdom_links(self) -> AtlasV2AuditResult:
        experiences = self.latest_by_id("ExperienceEvent")
        outcomes = self.latest_by_id("Outcome")
        wisdom = self.latest_by_id("CandidateWisdom")
        failures: list[str] = []
        for candidate in self.records("CandidateWisdom"):
            for experience_id in candidate["originating_experience_ids"]:
                if experience_id not in experiences:
                    failures.append(f"CandidateWisdom {candidate['wisdom_id']} missing experience {experience_id}")
            for outcome_id in candidate["supporting_outcome_ids"]:
                if outcome_id not in outcomes:
                    failures.append(f"CandidateWisdom {candidate['wisdom_id']} missing outcome {outcome_id}")
        for validation in self.records("WisdomValidation"):
            if validation["wisdom_id"] not in wisdom:
                failures.append(f"WisdomValidation {validation['validation_id']} missing wisdom {validation['wisdom_id']}")
        for event in self.records("WisdomEvent"):
            if event["wisdom_id"] not in wisdom:
                failures.append(f"WisdomEvent {event['event_id']} missing wisdom {event['wisdom_id']}")
            if event["triggering_experience"] not in experiences:
                failures.append(f"WisdomEvent {event['event_id']} missing experience {event['triggering_experience']}")
        for retirement in self.records("WisdomRetirement"):
            if retirement["wisdom_id"] not in wisdom:
                failures.append(f"WisdomRetirement {retirement['retirement_id']} missing wisdom {retirement['wisdom_id']}")
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))

    def audit_retired_wisdom_history(self) -> AtlasV2AuditResult:
        failures: list[str] = []
        retirements_by_wisdom = {record["wisdom_id"] for record in self.records("WisdomRetirement")}
        for candidate in self.records("CandidateWisdom"):
            if candidate["status"] == "RETIRED" and candidate["wisdom_id"] not in retirements_by_wisdom:
                failures.append(f"CandidateWisdom {candidate['wisdom_id']} retired without WisdomRetirement history")
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))

    def audit_contested_wisdom_challengeable(self) -> AtlasV2AuditResult:
        failures: list[str] = []
        for candidate in self.records("CandidateWisdom"):
            if candidate["status"] == "CONTESTED" and candidate.get("contradicting_count", 0) < 1:
                failures.append(f"CandidateWisdom {candidate['wisdom_id']} contested without contradicting evidence")
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))

    def audit_forbidden_artifacts(self) -> AtlasV2AuditResult:
        failures: list[str] = []
        allowed = {f"{object_type}.jsonl" for object_type in OBJECT_ID_FIELDS}
        if not self.root.exists():
            return AtlasV2AuditResult(ok=True, failures=())
        for path in self.root.rglob("*"):
            if not path.is_file():
                continue
            name = path.name.lower()
            if path.name not in allowed:
                failures.append(f"unexpected Atlas V2 artifact: {path.relative_to(self.root).as_posix()}")
                for term in FORBIDDEN_ARTIFACT_TERMS:
                    if term in name:
                        failures.append(f"forbidden artifact term {term}: {path.relative_to(self.root).as_posix()}")
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))


    def audit_cheap_experiment_gates(self) -> AtlasV2AuditResult:
        gates = self.latest_by_id("PromotionGateDecision")
        failures: list[str] = []
        for experiment in self.records("CheapExperiment"):
            tier = str(experiment.get("tier") or "")
            if tier not in GATED_EXPERIMENT_TIERS:
                continue
            gate_id = experiment.get("promotion_gate_id")
            if not gate_id or gate_id not in gates:
                failures.append(f"CheapExperiment {experiment['experiment_id']} missing PromotionGateDecision for {tier}")
                continue
            gate = gates[str(gate_id)]
            if gate.get("experiment_id") != experiment["experiment_id"]:
                failures.append(f"PromotionGateDecision {gate_id} does not link to CheapExperiment {experiment['experiment_id']}")
            if gate.get("to_tier") != tier:
                failures.append(f"PromotionGateDecision {gate_id} does not promote to {tier}")
            if str(gate.get("decision") or "").upper() != "APPROVED":
                failures.append(f"PromotionGateDecision {gate_id} is not approved for {tier}")
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))

    def audit_cheap_experiment_authority_boundaries(self) -> AtlasV2AuditResult:
        failures: list[str] = []
        for object_type in (
            "CheapExperiment",
            "LearningVelocityMetric",
            "ExperimentTierSummary",
            "PromotionGateDecision",
            "CheapExperimentBatch",
            "LearningEstimate",
            "LearningEstimateEvaluation",
            "AttentionSignal",
            "EstimatorPerformanceReport",
            "ThresholdExperimentReport",
            "ExperimentalRoutingDecision",
            "LabelIntegrityReport",
            "HistoricalExperienceRecord",
            "CheapExperimentSpec",
            "ExperimentDataRequirement",
            "ExperimentEvaluationPlan",
            "ExperimentGenerationRun",
            "ExperimentResult",
            "ExperimentOutcomeSummary",
            "ExperimentExecutionRun",
            "AutonomousResearchLoopRun",
            "AutonomousResearchLoopSummary",
            "GeneratedResearchClaim",
            "ClaimGenerationRun",
            "ClaimGenerationSourceSummary",
        ):
            for record in self.records(object_type):
                try:
                    _assert_no_forbidden_authority_fields(record, object_type)
                except AtlasV2ValidationError as exc:
                    failures.append(str(exc))
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))


    def audit_experiment_executor_links(self) -> AtlasV2AuditResult:
        specs = self.latest_by_id("CheapExperimentSpec")
        runs = self.latest_by_id("ExperimentExecutionRun")
        summaries = self.latest_by_id("ExperimentOutcomeSummary")
        failures: list[str] = []
        for result in self.records("ExperimentResult"):
            spec_id = result["experiment_spec_id"]
            if spec_id not in specs:
                failures.append(f"ExperimentResult {result['result_id']} missing CheapExperimentSpec {spec_id}")
                continue
            spec = specs[spec_id]
            for field in ("hypothesis_id", "mechanism_id", "tier", "baseline_condition", "evaluation_metric", "falsification_threshold"):
                if result.get(field) != spec.get(field):
                    failures.append(f"ExperimentResult {result['result_id']} does not match CheapExperimentSpec {spec_id} field {field}")
            if result["execution_run_id"] not in runs:
                failures.append(f"ExperimentResult {result['result_id']} missing ExperimentExecutionRun {result['execution_run_id']}")
        for run in self.records("ExperimentExecutionRun"):
            summary_id = run["outcome_summary_id"]
            if summary_id not in summaries:
                failures.append(f"ExperimentExecutionRun {run['execution_run_id']} missing ExperimentOutcomeSummary {summary_id}")
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))


    def audit_historical_experience_authority_boundaries(self) -> AtlasV2AuditResult:
        failures: list[str] = []
        for record in self.records("HistoricalExperienceRecord"):
            try:
                _assert_no_forbidden_authority_fields(record, "HistoricalExperienceRecord")
            except AtlasV2ValidationError as exc:
                failures.append(str(exc))
        for event in self.records("ExperienceEvent"):
            if event.get("historical_record_id") and _is_missing(event.get("source_artifact")):
                failures.append(f"ExperienceEvent {event['experience_id']} missing historical source_artifact")
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))

    def audit_learning_estimator_authority_boundaries(self) -> AtlasV2AuditResult:
        failures: list[str] = []
        for signal in self.records("AttentionSignal"):
            action = str(signal.get("recommended_attention_action") or "")
            if action in FORBIDDEN_ATTENTION_ACTIONS:
                failures.append(f"AttentionSignal {signal['signal_id']} recommends forbidden action {action}")
        for decision in self.records("ExperimentalRoutingDecision"):
            if decision.get("experiment_only") is not True:
                failures.append(f"ExperimentalRoutingDecision {decision['routing_id']} is not experiment_only")
            if str(decision.get("experimental_action") or "") == "REQUIRES_GATE_REVIEW":
                failures.append(f"ExperimentalRoutingDecision {decision['routing_id']} routes gate-review action")
            if str(decision.get("routed_to_tier") or "") in GATED_EXPERIMENT_TIERS and _is_missing(decision.get("promotion_gate_id")):
                failures.append(f"ExperimentalRoutingDecision {decision['routing_id']} routes gated tier without PromotionGateDecision")
        for object_type in (
            "LearningEstimate",
            "LearningEstimateEvaluation",
            "AttentionSignal",
            "EstimatorPerformanceReport",
            "ThresholdExperimentReport",
            "ExperimentalRoutingDecision",
            "LabelIntegrityReport",
        ):
            for record in self.records(object_type):
                try:
                    _assert_no_forbidden_authority_fields(record, object_type)
                except AtlasV2ValidationError as exc:
                    failures.append(str(exc))
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))

    def compute_learning_velocity_metric(
        self,
        *,
        metric_id: str,
        period_start: str,
        period_end: str,
        created_at: str | None = None,
    ) -> dict[str, Any]:
        predictions = self.latest_by_id("Prediction")
        outcomes = self.records("Outcome")
        prediction_outcome_cycles = sum(1 for outcome in outcomes if outcome.get("prediction_id") in predictions)
        experiments = self.records("CheapExperiment")
        completed = [record for record in experiments if str(record.get("status") or "").lower() == "completed"]
        rejected = [record for record in experiments if str(record.get("status") or "").lower() == "rejected"]
        approved_gates = [
            record
            for record in self.records("PromotionGateDecision")
            if str(record.get("decision") or "").upper() == "APPROVED"
        ]
        actual_learning_total = round(sum(float(record.get("actual_learning_value", 0)) for record in completed), 6)
        regret_total = round(sum(float(record.get("importance_weighted_regret", 0)) for record in self.records("Regret")), 6)
        cycles = prediction_outcome_cycles
        completed_count = len(completed)
        payload = {
            "metric_id": metric_id,
            "created_at": created_at or utc_now_iso(),
            "period_start": period_start,
            "period_end": period_end,
            "prediction_outcome_cycles": cycles,
            "cheap_experiments_completed": completed_count,
            "actual_learning_total": actual_learning_total,
            "average_learning_per_cycle": 0.0 if cycles == 0 else round(actual_learning_total / cycles, 6),
            "rejection_count": len(rejected),
            "promotion_count": len(approved_gates),
            "promotion_rate": 0.0 if completed_count == 0 else round(len(approved_gates) / completed_count, 6),
            "importance_weighted_regret_total": regret_total,
        }
        return self.create_record(
            "LearningVelocityMetric",
            payload,
            reason="learning velocity computed from outcome-linked cheap experiments",
            triggering_object="AtlasV2Ledger:compute_learning_velocity_metric",
        )

    def audit_all(self) -> AtlasV2AuditResult:
        failures: list[str] = []
        for object_type in OBJECT_ID_FIELDS:
            for record in self.records(object_type):
                try:
                    validate_object(record)
                except AtlasV2ValidationError as exc:
                    failures.append(str(exc))
        for audit in (
            self.audit_complete_experience_links(),
            self.audit_behavior_changes(),
            self.audit_wisdom_links(),
            self.audit_retired_wisdom_history(),
            self.audit_contested_wisdom_challengeable(),
            self.audit_cheap_experiment_gates(),
            self.audit_cheap_experiment_authority_boundaries(),
            self.audit_experiment_executor_links(),
            self.audit_learning_estimator_authority_boundaries(),
            self.audit_historical_experience_authority_boundaries(),
            self.audit_forbidden_artifacts(),
        ):
            failures.extend(audit.failures)
        return AtlasV2AuditResult(ok=not failures, failures=tuple(failures))

