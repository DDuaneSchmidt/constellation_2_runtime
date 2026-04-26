---
id: GOVERNANCE_INDEX_C2_V1
title: "Constellation 2.0 Governance Index"
status: ACTIVE
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Constellation 2.0 Governance Index

This repository is the constitutional root of **Constellation 2.0**.

## Authority model

- **Governance** (this folder) defines contracts, registries, and change control.
- **Git** defines exact versioned behavior of runtime code and governance.
- **Runtime truth** is authoritative by subsystem: control-plane truth remains under `/home/node/constellation_runtime_data/truth/`, while execution-root authority for active PAPER execution is sleeve-scoped under `/home/node/constellation_runtime_data/truth_sleeves/<sleeve_id>/<mode>/`.

## Canonical runtime truth root

- `/home/node/constellation_runtime_data/truth`
- `/home/node/constellation_runtime_data/truth_sleeves/<sleeve_id>/<mode>` for active execution-root families

## Truth-Root Lifecycle Orchestration

- `C2_TRUTH_LIFECYCLE_PHASE_GRAPH_V1` is the canonical phase-order registry for truth-root lifecycle execution.
- `truth_lifecycle_phase_result_v1` is the uniform per-phase blocker/result contract.
- `truth_day_run_ledger_v1` is the authoritative day-run lifecycle ledger with first-blocker and bootstrap reporting.
- `run_gate_authority_plane_v1.py` is the canonical truth-root lifecycle execution entrypoint for gate/authorization lifecycle phases and must execute through the governed phase graph.

## Day Activation Layer

- Day Activation Authority seals target-day admission, canonical authority head, and authorization gate verdict into day_activation_package_v1 before Global Context runs.
- Global Context primary flow now consumes day_activation_package_v1 instead of reopening raw control-plane nodes.

## Canonical PAPER Startup Boundary

- `pre_open_bundle_v1` is the canonical startup prerequisite truth.
- `session_promotion_decision_v1` is the explicit promotion boundary for startup current-state advancement.
- `active_session_v1/current.json` is the only startup current-state artifact promoted in this pass.
- producer-local current surfaces such as `ib_api_handshake/latest_pointer.v1.json` and PRIMARY/PAPER `run_pointer_v2/canonical_authority_head.v1.json` remain pre-open inputs, not startup promotion outputs.

## Bundle 4 Control-Plane Admission

- `live_control_plane_authority_inventory_v1` is the canonical inventory for authoritative, derived, and forbidden control-plane surfaces.
- `control_stage_day_admitted_v1`, `control_stage_context_admitted_v1`, `control_stage_session_admitted_v1`, and `control_stage_execution_build_admitted_v1` are the ordered stage-admission artifacts for the canonical startup chain.
- `startup_chain_certification_v1` is the durable certification artifact for boundary validation, family validation, and stage coherence across the canonical startup chain.

## Bundle 5 Certified Stage Machine

- `control_plane_transition_engine_v1` is the only legal evaluation path for control-plane stage transitions.
- `control_stage_transition_record_v1` is the durable audit record for blocked, admitted, certified, recomputed, and superseded stage transitions.
- `run_control_plane_transition_v1.py` is the thin stage-transition orchestration entrypoint.

## Bundle 6 Certified Trust Plane

- `control_plane_trust_projection_v1` ratifies one trust-plane projection kernel as the only semantic source for operator, advisory, and reporting surfaces over certified control-plane truth.
- `control_plane_operator_status_v1`, `control_plane_blocked_transition_view_v1`, `advisory_truth_binding_status_v1`, and `transition_timeline_projection_v1` are the governed trust-plane read-model artifacts for Bundle 6.
- `run_control_plane_operator_status_v1.py`, `run_control_plane_blocked_transition_view_v1.py`, `run_transition_timeline_projection_v1.py`, and `run_advisory_truth_binding_status_v1.py` are the thin Bundle 6 projection CLIs.

## Configuration Activation Layer

- `configuration_activation_authority_v1` is the governance-first runtime configuration activation family.
- `configuration_state_v1/current.json` is the only published current-state artifact in that family.
- runtime consumers must read only the `compiled_active_config_v1` referenced by `configuration_state_v1/current.json`.
- raw `configuration_policy_snapshot_v1` is audit/validation input only and must not become a direct runtime-consumption surface.

## Bundle 7 Release Baseline Closure

- `release_baseline_readiness_v1` defines the fail-closed baseline-ready requirements for release and operator start.
- `runtime_authority_closure_v1` defines the active-path forbidden-root closure rules for Bundle 7.
- `critical_validator_coverage_v1` defines the minimum independent validator set required for release safety.
- `certified_operational_readiness_v1` defines the minimal current-state read model for release and incident triage.
- `bundle7_semantic_preservation_v1` ratifies that Bundle 7 preserves Bundle 3-6 semantics and only closes baseline/authority gaps.

## Bundle 8 Certified Decision Plane

- `advisory_decision_state_kernel_v1` defines the only legal semantic source for advisory decision safety on the active C2 path.
- `advisory_decision_state_v1` is the durable canonical advisory decision artifact family.
- `advisory_decision_state_precedence_v1` defines the deterministic invalidation and downgrade matrix.
- `advisory_decision_explanation_mapping_v1` defines the deterministic explanation path for advisory decisions.
- `bundle8_semantic_preservation_v1` ratifies that Bundle 8 preserves Bundles 3-7 semantics and does not rewrite control-plane or release semantics.

## Bundle 9 Bounded Obligation Pipelines

- `critical_path_obligation_pipeline_v1` defines the only legal five-phase shape for Bundle 9 critical paths.
- `bundle9_decomposition_map_v1` defines the first-wave critical-path decomposition map and explicit deferred scope.
- `bundle9_performance_envelope_v1` defines Bundle 9 phase-boundary timing budgets and validation profiles.
- `bundle9_replay_forensic_escalation_v1` defines the legal replay/recompute escalation modes for Bundle 9.
- `bundle9_semantic_preservation_v1` ratifies that Bundle 9 preserves Bundles 3-8 semantics and does not rewrite control-plane, trust-plane, or advisory semantics.

## Bundle 10 Certified Tax State Plane

- `tax_state_kernel_v1` defines the only legal semantic source for live tax decision support on the active C2 path.
- `tax_state_v1` is the durable canonical tax state artifact family.
- `tax_state_precedence_v1` defines the deterministic blocker and opportunity precedence matrix.
- `tax_state_explanation_mapping_v1` defines the deterministic explanation path for tax state.
- `tax_aware_advisory_binding_v1` defines the single legal path by which tax state may influence advisory.
- `bundle10_semantic_preservation_v1` ratifies that Bundle 10 preserves Bundles 3-9 semantics and does not rewrite control-plane, trust-plane, or advisory semantics outside the governed tax-binding seam.

## Bundle 11 Certified Opportunity Plane

- `opportunity_state_kernel_v1` defines the only legal semantic source for proactive opportunity discovery and review prioritization on the active C2 path.
- `opportunity_state_v1` is the durable canonical opportunity artifact family.
- `opportunity_review_snapshot_v1` is the governed review snapshot and delta artifact family for Bundle 11.
- `opportunity_state_precedence_v1` defines the deterministic blocker and review-priority precedence matrix.
- `opportunity_scenario_significance_v1` defines the bounded scenario-significance binding path.
- `opportunity_explanation_mapping_v1` defines the deterministic explanation path for opportunity state.
- `bundle11_semantic_preservation_v1` ratifies that Bundle 11 preserves Bundles 3-10 semantics and does not rewrite control-plane, trust-plane, advisory, tax, or release semantics.

## Bundle 12 Certified Product Summary Plane

- `product_summary_kernel_v1` defines the only legal semantic source for product-level top-of-screen selection on the active C2 path.
- `product_summary_v1` is the durable canonical product summary artifact family.
- `product_snapshot_v1` is the governed product snapshot and provenance artifact family for Bundle 12.
- `product_summary_precedence_v1` defines the deterministic summary-selection precedence model.
- `bounded_product_ai_condensation_v1` defines the bounded AI condensation path and deterministic fallback law.
- `product_shell_professionalism_v1` defines the operational shell behavior requirements where governed product truth now exists.
- `bundle12_semantic_preservation_v1` ratifies that Bundle 12 preserves Bundles 3-11 semantics and does not rewrite lower-level domain semantics.

## Bundle 13 Certified Value Plane

- `value_state_kernel_v1` defines the only legal semantic source for realized value proof and allowed value claims on the active C2 path.
- `value_state_v1` is the durable canonical value artifact family.
- `value_claim_strength_v1` defines the explicit claim-strength ladder for Bundle 13.
- `value_effectiveness_attribution_v1` defines the conservative effectiveness and attribution model.
- `value_sleeve_linkage_v1` defines the bounded sleeve-linkage model and claim-weakening rules.
- `value_comparison_v1` defines the small legal comparison set and explicit rejection law for unsupported comparisons.
- `value_explanation_mapping_v1` defines the deterministic explanation path for value proof.
- `bundle13_semantic_preservation_v1` ratifies that Bundle 13 preserves Bundles 3-12 semantics and does not rewrite lower-level domain semantics.

## Bundle 14 Certified Refinement Plane

- `refinement_state_kernel_v1` defines the only legal semantic source for governed simplification, demotion, compression, and prominence decisions.
- `refinement_state_v1` is the durable canonical refinement artifact family.
- `refinement_threshold_v1` defines the deterministic evidence thresholds for refinement actions.
- `refinement_action_model_v1` defines the small legal action set and reversibility law.
- `refinement_product_safety_preservation_v1` bans optimization away from trust-preserving visibility.
- `refinement_provenance_v1` defines the durable before/after and evidence-basis reconstruction path.
- `bundle14_semantic_preservation_v1` ratifies that Bundle 14 preserves Bundles 3-13 semantics and does not rewrite lower-level domain semantics.

## Bundle 15 Certified Policy Evolution Plane

- `policy_evolution_state_kernel_v1` defines the only legal semantic source for governed temporal policy evolution over refinement and product emphasis.
- `policy_evolution_state_v1` is the durable canonical policy-evolution artifact family.
- `policy_evidence_window_threshold_v1` defines deterministic historical-evidence thresholds.
- `policy_evolution_action_model_v1` defines the propose/preserve/withhold/expire/rollback action set.
- `policy_trust_preserving_override_v1` bans policy evolution from weakening trust-preserving visibility protections.
- `policy_evolution_provenance_v1` defines the durable before/after and evidence-window reconstruction path.
- `bundle15_semantic_preservation_v1` ratifies that Bundle 15 preserves Bundles 3-14 semantics and does not rewrite lower-level domain semantics.

## Document classes

### Weekly research protocols
- `governance/weekly_engine_diagnostic_review_protocol_v1.md`

### AI governance contracts
- `governance/contracts/constellation_ai_reasoning_contract.v1.md`
- `governance/contracts/constellation_system_invariants.v1.md`
- `governance/meta_governance/00_OVERVIEW.md`
- `governance/meta_governance/01_TIERS_AND_INVARIANTS.md`
- `governance/meta_governance/02_MUTATION_PROTOCOLS.md`
- `governance/meta_governance/03_ACTIVATION_SNAPSHOTS.md`
- `governance/meta_governance/04_AUDIT_AND_LINEAGE.md`
- `governance/meta_governance/05_META_GOVERNANCE_CONTRACT.md`
- `governance/contracts/INVESTOR_INTENT_CONTRACT.md`
- `governance/contracts/POLICY_CONTRACT.md`
- `governance/contracts/HOUSEHOLD_SNAPSHOT_CONTRACT.md`
- `governance/contracts/ECONOMIC_STATE_KERNEL_BOUNDARY_CONTRACT.md`
- `governance/contracts/ADVISORY_VALUE_BASIS_AUTHORITY_CONTRACT.md`
- `governance/contracts/PORTFOLIO_INTENT_CONTRACT.md`
- `governance/contracts/ALLOCATION_ADVISORY_CONTRACT.md`
- `governance/contracts/ADVISORY_ACTIONABILITY_GATE_CONTRACT.md`
- `governance/contracts/PROMOTION_RECORD_CONTRACT.md`
- `governance/contracts/ALLOCATION_EXECUTION_SHAPING_CONTRACT.md`
- `governance/contracts/MULTI_DELTA_EXECUTION_KERNEL_BOUNDARY_CONTRACT.md`
- `governance/contracts/APPROVED_CHANGE_SET_CONTRACT.md`
- `governance/contracts/MULTI_DELTA_EXECUTION_DECISION_CONTRACT.md`
- `governance/contracts/MULTI_DELTA_EXECUTION_RECORD_CONTRACT.md`
- `governance/contracts/EXECUTION_SET_INTENT_CONTRACT.md`
- `governance/contracts/ADVISORY_DOMAIN_LINEAGE_CONTRACT.md`
- `governance/contracts/ADVISORY_VALIDITY_TIERS_CONTRACT.md`
- `governance/contracts/ASSUMPTION_MANIFEST_CONTRACT.md`
- `governance/contracts/CLASSIFICATION_MANIFEST_USAGE_CONTRACT.md`
- `governance/contracts/ADVISORY_TO_TRADING_PROMOTION_BOUNDARY.md`
- `governance/contracts/ADVISORY_KERNEL_BOUNDARY_CONTRACT.md`
- `governance/contracts/ADVISORY_KERNEL_EXECUTION_CONTRACT.md`
- `governance/contracts/EXECUTION_KERNEL_BOUNDARY_CONTRACT.md`
- `governance/contracts/EXECUTION_SUBMISSION_CONTRACT.md`
- `governance/contracts/EXECUTION_STATE_KERNEL_BOUNDARY_CONTRACT.md`
- `governance/contracts/EXECUTION_STATE_AUTHORITY_CONTRACT.md`
- `governance/contracts/OPERATOR_RUNTIME_CONTROL_KERNEL_BOUNDARY_CONTRACT.md`
- `governance/contracts/RUNTIME_CONTROL_AUTHORITY_CONTRACT.md`
- `governance/contracts/UI_OPERATOR_SHELL_BOUNDARY_CONTRACT.md`
- `governance/contracts/UI_AUTHORITY_RENDERING_CONTRACT.md`

### Advisory authority rewrite contracts
- The advisory authority rewrite is governance-first in this repo.
- `official_recommendation_set_v1` and the legacy fragmented `promotion_*` family are not future advisory authorities under the new contract set.
- `PromotionRecord` is the sole future authority surface for advisory-to-trading promotion.
- `planning_snapshot_v1` is a legacy mixed migration surface, not the future canonical household authority.
- `ExecutionIntent` is the only future advisory-produced execution artifact allowed to reach existing paper trading.
- value-based allocation advisory expands only in this order: `Value Basis Authority -> Allocation Advisory -> Actionability Gate -> Execution Shaping`.
- value basis must be governed before value-based allocation advice is emitted.
- every valid kernel run must emit `KernelRunEnvelope`, including blocked and no-action outcomes.
- `governance/04_DATA/SCHEMAS/C2/ADVISORY/investor_intent.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/assumption_manifest.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/policy.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/household_snapshot.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/snapshot_validation_decision.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/snapshot_run_envelope.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_intent.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/promotion_decision.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/execution_intent.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/kernel_run_envelope.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/policy_snapshot.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/household_state_snapshot.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/compiled_constraints.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/allocation_plan.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/risk_envelope.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/rebalance_candidates.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/tax_adjudicated_rebalance.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_authorization.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_decision_record.v1.schema.json`, and `governance/04_DATA/SCHEMAS/C2/ADVISOR_BRIDGE/promotion_record.v2.schema.json` are the active advisory kernel schemas.
- `governance/04_DATA/SCHEMAS/C2/EXECUTION/approved_change_set.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/EXECUTION/multi_delta_execution_decision.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/EXECUTION/multi_delta_execution_record.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_set_intent.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_submission_decision.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_submission_record.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_lifecycle_decision.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_state_record.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_lifecycle_run_envelope.v1.schema.json`, and `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_run_envelope.v1.schema.json` are the active execution-kernel schemas.
- `governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_decision.v1.schema.json`, `governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_record.v1.schema.json`, and `governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_control_run_envelope.v1.schema.json` are the active runtime-control kernel schemas.
- `governance/contracts/UI_OPERATOR_SHELL_BOUNDARY_CONTRACT.md` and `governance/contracts/UI_AUTHORITY_RENDERING_CONTRACT.md` govern the kernel-aligned operator shell and prohibit UI-side authority drift.
- Legacy placeholder advisory schema registrations for `advisory_input_artifact`, `advisory_decision_artifact`, and `advisory_comparison_artifact` are retired from the active governance manifest because those files do not exist on disk.


### A) C2 Bundle Contracts (Design Authority)
- `governance/01_CONTRACTS/C2/`

### B) C2 Canonical Contracts (Runtime + Audit Authority)
- `governance/05_CONTRACTS/C2/`

### C) Registries (Enumerations / reason codes / constants)
- `governance/02_REGISTRIES/`

Additional registries introduced:

- `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`
- `governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json`
- `governance/02_REGISTRIES/C2_LIQUIDITY_SLIPPAGE_POLICY_V1.json`
- `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`
- `governance/02_REGISTRIES/C2_DIAGNOSTICS_FRESHNESS_POLICY_V1.json`
- `governance/02_REGISTRIES/C2_SLEEVE_LIVE_READINESS_POLICY_V1.json`
- `governance/02_REGISTRIES/C2_HOUSEHOLD_PORTFOLIO_REASON_CODE_REGISTRY_V1.json`
- `governance/02_REGISTRIES/C2_SLEEVE_EDGE_QUALIFICATION_POLICY_V1.json`
  This v1 registry governs unknown-attribution tolerance and allocator calculation-version compatibility for sleeve-edge control-path consumption.
- `governance/02_REGISTRIES/C2_GOVERNED_EVALUATION_POLICY_V1.json`
  This v1 registry governs benchmark policy by sleeve role, validity thresholds, evaluation thresholds, and action sequencing for governed weekly sleeve and portfolio evaluation.
- `governance/02_REGISTRIES/C2_PLATFORM_READINESS_POLICY_V1.json`
- `governance/02_REGISTRIES/C2_LIFECYCLE_DEPENDENCY_CLASSIFICATION_V1.json`
- `governance/02_REGISTRIES/C2_SESSION_AUTHORITY_BLOCKER_REASON_REGISTRY_V1.json`
- `governance/05_CONTRACTS/C2/market_calendar_coverage_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/market_calendar_coverage_status_v1.contract.md`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/market_calendar_coverage_status.v1.schema.json`
- `governance/05_CONTRACTS/C2/session_authority_status_v1.contract.md`
- `governance/05_CONTRACTS/C2/session_authority_alert_v1.contract.md`
- `governance/05_CONTRACTS/C2/market_calendar_coverage_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/market_calendar_coverage_status_v1.contract.md`
- `governance/05_CONTRACTS/C2/multi_sleeve_rollup_pointer_index_v1.contract.md`
- `governance/05_CONTRACTS/C2/sleeve_rollup_v1.contract.md`
- `governance/05_CONTRACTS/C2/paper_session_ledger_v1.contract.md`
- `governance/05_CONTRACTS/C2/startup_proof_validation_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_summary_v1.contract.md`
- `governance/05_CONTRACTS/C2/intents_day_completeness_v1.contract.md`
- `governance/05_CONTRACTS/C2/no_intents_day_v1.contract.md`
- `governance/05_CONTRACTS/C2/trading_day_intent_generation_v1.contract.md`
- `governance/05_CONTRACTS/C2/startup_materialization_inputs_prep_v1.contract.md`
- `governance/05_CONTRACTS/C2/phasec_risk_inputs_prep_v1.contract.md`
- `governance/05_CONTRACTS/C2/paper_day_control_plane_v1.contract.md`
- `governance/05_CONTRACTS/C2/trading_day_control_plane_v1.contract.md`
- `governance/05_CONTRACTS/C2/trading_day_execution_control_plane_v1.contract.md`
- `governance/05_CONTRACTS/C2/trading_day_state_machine_v1.contract.md`
- `governance/05_CONTRACTS/C2/deployment_state_machine_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_journal_v1.contract.md`
- `governance/05_CONTRACTS/C2/trading_symbol_policy_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_journal_event_type_registry_v1.contract.md`
- `governance/05_CONTRACTS/C2/current_system_projection_v1.contract.md`
- `governance/05_CONTRACTS/C2/alerts_projection_v1.contract.md`
- `governance/05_CONTRACTS/C2/performance_projection_v1.contract.md`
- `governance/05_CONTRACTS/C2/recurrence_fingerprint_v1.contract.md`
- `governance/05_CONTRACTS/C2/recurrence_registry_v1.contract.md`
- `governance/05_CONTRACTS/C2/recurrence_kill_gate_v1.contract.md`
- `governance/05_CONTRACTS/C2/release_root_activation_v1.contract.md`
- `governance/05_CONTRACTS/C2/active_runtime_contract_v1.contract.md`
- `governance/05_CONTRACTS/C2/runtime_startup_identity_v1.contract.md`
- `governance/05_CONTRACTS/C2/runtime_lifecycle_v1.contract.md`
- `governance/05_CONTRACTS/C2/single_node_hosted_deployment_foundation_v1.contract.md`
- `governance/05_CONTRACTS/C2/paper_session_evidence_manifest_v1.contract.md` (superseded by `paper_session_ledger_v1`)
- `governance/05_CONTRACTS/C2/paper_session_kernel_v1.contract.md` (superseded by `paper_session_ledger_v1`)
- `governance/05_CONTRACTS/C2/preopen_requires_multi_sleeve_rollup_verification_v1.contract.md`
- `governance/05_CONTRACTS/C2/v2_readiness_dependency_contract_v1.contract.md`
- `governance/05_CONTRACTS/C2/capital_monitoring_attestation_v1.contract.md`

### Registries (governed)
- `governance/02_REGISTRIES/GATE_HIERARCHY_V1.json`
- `governance/02_REGISTRIES/C2_EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_V1.json`

### D) Operator docs and runbooks (non-authoritative guidance)
- `docs/` (informational; not a contract unless also registered in the manifest)

## Bundle F + G (Accounting + Allocation)

The following C2 Bundle F/G documents are introduced under `docs/c2/` and are tracked in the governance manifest for traceability:

- `docs/c2/F_ACCOUNTING_SPINE_V1.md`
- `docs/c2/F_ACCOUNTING_SCHEMA_V1.md`
- `docs/c2/F_ACCOUNTING_RECONSTRUCTION_GUARANTEE_V1.md`
- `docs/c2/F_MARKING_POLICY_CONSERVATIVE_V1.md`
- `docs/c2/G_ALLOCATION_SPINE_V1.md`
- `docs/c2/G_THROTTLE_RULES_V1.md`
- `docs/c2/G_RISK_BUDGET_CONTRACT_V1.md`
- `docs/c2/G_REASON_CODES_V1.md`
- `docs/c2/RUNBOOK_FG_PHASE_V1.md`

NOTE: In this repo, docs under `docs/` are treated as governed artifacts **only when explicitly listed** in `governance/00_MANIFEST.yaml`.

## Governed canonical contracts

These canonical contracts are governance-controlled and must be explicitly listed in `governance/00_MANIFEST.yaml` to be treated as governed artifacts:

- `governance/05_CONTRACTS/C2/drawdown_convention_v1.contract.md`
- `governance/05_CONTRACTS/C2/capital_risk_envelope_v1.contract.md`
- `governance/05_CONTRACTS/C2/capital_risk_envelope_v2.contract.md`
- `governance/05_CONTRACTS/C2/systemic_risk_gate_v3.contract.md`
- `governance/05_CONTRACTS/C2/liquidity_slippage_gate_v1.contract.md`
- `governance/05_CONTRACTS/C2/bundle_a_paper_trading_readiness_audit_proof_v1.contract.md`
- `governance/05_CONTRACTS/C2/bundle_b_risk_blindspot_elimination_v1.contract.md`
- `governance/05_CONTRACTS/C2/bundled_c_exposure_convergence_lifecycle_v1.contract.md`
- `governance/05_CONTRACTS/C2/economic_nav_drawdown_truth_spine_bundle_v1.contract.md`
- `governance/05_CONTRACTS/C2/nav_snapshot_truth_v1.contract.md`
- `governance/05_CONTRACTS/C2/nav_history_ledger_v1.contract.md`
- `governance/05_CONTRACTS/C2/drawdown_window_pack_v1.contract.md`
- `governance/05_CONTRACTS/C2/economic_truth_availability_certificate_v1.contract.md`
- `governance/05_CONTRACTS/C2/eod_observability_bundle_v1.contract.md`
- `governance/05_CONTRACTS/C2/regime_classification_spine_v1.contract.md`
- `governance/05_CONTRACTS/C2/regime_classification_spine_v1.deprecation_notice.md`
- `governance/05_CONTRACTS/C2/regime_classification_spine_v2.contract.md`

- `governance/05_CONTRACTS/C2/gate_hierarchy_v1.contract.md`
- `governance/05_CONTRACTS/C2/truth_surface_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/failure_injection_harness_v1.contract.md`

- `governance/05_CONTRACTS/C2/single_final_verdict_consumption_v1.contract.md`
- `governance/05_CONTRACTS/C2/intent_simulator_v1.contract.md`
- `governance/05_CONTRACTS/C2/ib_historical_market_data_snapshot_downloader_v1.contract.md`
- `governance/05_CONTRACTS/C2/defensive_tail_required_inputs_bridge_v1.contract.md`
- `governance/05_CONTRACTS/C2/cross_asset_trend_v1.contract.md`
- `governance/05_CONTRACTS/C2/market_neutral_spread_v1.contract.md`
- `governance/05_CONTRACTS/C2/C2_ATTEMPT_SCOPED_WRITE_POLICY_WITH_CANONICAL_POINTERS_V3.contract.md`
- `governance/05_CONTRACTS/C2/ib_account_registry_v1.contract.md`
- `governance/05_CONTRACTS/C2/trade_submit_readiness_c2_v1.contract.md`
- `governance/05_CONTRACTS/C2/constitutional_runtime_architecture_v1.contract.md`
- `governance/05_CONTRACTS/C2/constitutional_artifact_taxonomy_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_root_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_root_isolation_policy_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_profile_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_identity_authority_v1.contract.md`
- `governance/portfolio/household_policy_compiler_contract.md`
- `governance/portfolio/portfolio_authorization_contract.md`
- `governance/05_CONTRACTS/C2/trade_instance_identity_v1.contract.md`
- `governance/05_CONTRACTS/C2/submission_identity_v1.contract.md`
- `governance/05_CONTRACTS/C2/kill_switch_entry_policy_v1.contract.md`
- `governance/05_CONTRACTS/C2/single_ib_account_mode_v1.contract.md`
- `governance/05_CONTRACTS/C2/multi_account_topology_v1.contract.md`
- `governance/05_CONTRACTS/C2/sleeve_registry_v1.contract.md`
- `governance/05_CONTRACTS/C2/truth_partitioning_by_sleeve_v1.contract.md`
- `governance/05_CONTRACTS/C2/per_sleeve_readiness_v1.contract.md`
- `governance/05_CONTRACTS/C2/per_sleeve_orchestrator_v1.contract.md`
- `governance/05_CONTRACTS/C2/diagnostics_scope_health_v1.contract.md`
- `governance/05_CONTRACTS/C2/sleeve_live_readiness_v1.contract.md`
  Advisory only; not canonical sleeve-edge control truth.
- `governance/05_CONTRACTS/C2/sleeve_edge_fact_ledger_v1.contract.md`
- `governance/05_CONTRACTS/C2/sleeve_edge_metric_v1.contract.md`
- `governance/05_CONTRACTS/C2/sleeve_edge_qualification_v1.contract.md`
- `governance/04_DATA/SCHEMAS/C2/RUNTIME/constitutional_artifact_authority_registry.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RUNTIME/governed_artifact_lineage.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RUNTIME/frozen_decision_input_bundle.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RUNTIME/artifact_dependency_declaration.v1.schema.json`
- `governance/02_REGISTRIES/C2_ARTIFACT_AUTHORITY_REGISTRY_V1.json` is the active constitutional artifact contract registry for the paper-day route.
- `governance/05_CONTRACTS/C2/sleeve_edge_snapshot_v1.contract.md`
- `governance/05_CONTRACTS/C2/sleeve_edge_allocator_integration_v1.contract.md`
- `governance/05_CONTRACTS/C2/governed_evaluation_architecture_v1.contract.md`
- `governance/05_CONTRACTS/C2/evaluation_authority_foundation_v1.contract.md`
- `governance/05_CONTRACTS/C2/weekly_scorecard_view_v1.contract.md`

Governed evaluation v1 is additive and layered:

- measurement truth remains separate from validity
- validity remains separate from evaluation
- evaluation remains separate from governance action
- weekly scorecards remain derived-only read models

Governed evaluation schema families:

- `governance/04_DATA/SCHEMAS/C2/EVALUATION/evaluation_policy_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/evaluation_input_manifest.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/outcome_attribution_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_edge_measurement_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/allocation_governance_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_operative_control_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_performance_truth.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_validity_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_evaluation_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/portfolio_performance_truth.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/portfolio_validity_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/portfolio_evaluation_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_governance_action_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/portfolio_governance_action_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/weekly_scorecard_view.v1.schema.json`

Sleeve edge v1 remains governed in place. Hardening changes are additive in v1: revision lineage, `UNKNOWN_ATTRIBUTION` exclusion semantics, allocator read-only consumption, policy-version locking, snapshot integrity validation, and audit-only `unavailable_metrics[]`.

Canonical sleeve-edge control truth:
- `sleeve_edge_snapshot_v1` is the canonical frozen control artifact
- `ops/tools/run_capital_authority_allocation_day_v1.py` is the canonical consumer
- `ops/tools/run_c2_paper_day_orchestrator_v2.py` is the canonical sequencing owner

Non-canonical sleeve-edge-related advisory surfaces:
- `sleeve_live_readiness_v1`
- `allocation_summary_v1`
- `constellation_2/phaseG/allocation/run/run_allocation_day_v2.py`

These advisory surfaces must not be used for control decisions.
- `governance/05_CONTRACTS/C2/bug_metrics_v1.contract.md`
- `governance/05_CONTRACTS/C2/platform_readiness_v1.contract.md`
- `governance/05_CONTRACTS/C2/capital_monitoring_attestation_v1.contract.md`
- `governance/05_CONTRACTS/C2/reconciled_trade_state_v1.contract.md`
- `governance/05_CONTRACTS/C2/trade_identity_resolution_v1.contract.md`
- `governance/05_CONTRACTS/C2/reconciliation_health_v1.contract.md`
- `governance/05_CONTRACTS/C2/reconciled_trade_description_v1.contract.md`
- `governance/05_CONTRACTS/C2/reconciliation_provenance_v1.contract.md`
- `governance/05_CONTRACTS/ORCHESTRATION/C2_ORCHESTRATOR_V2_CONTRACT.md`
- `governance/05_CONTRACTS/C2/auto_repair_controller_v1.contract.md`


## Governed data schemas

## Tax Engine Foundation

The tax engine foundation introduced in this branch is governance-first and remains execution
separated in this pass.

Governed tax contracts:

- `governance/tax/01_TAX_SCOPE_AND_TRUTH_MODEL.md`
- `governance/tax/02_TAX_STATE_AND_SNAPSHOT_CONTRACT.md`
- `governance/tax/03_TAX_POLICY_AND_DECISION_CONTRACT.md`
- `governance/tax/04_TAX_EXECUTION_GATE_AND_REPLAY_CONTRACT.md`
- `governance/tax/05_TAX_ROUTING_HARVEST_AND_RECONCILIATION_CONTRACT.md`
- `governance/tax/06_TAX_CORPORATE_ACTION_PROVISIONAL_FINALIZED_CONTRACT.md`

Governed tax registry:

- `governance/02_REGISTRIES/C2_TAX_REASON_CODE_REGISTRY_V1.json`

Governed tax schemas:

- `governance/04_DATA/SCHEMAS/C2/TAX/tax_scope_registry.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_scope_membership.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/wash_enforcement_scope.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_observed_event.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/corporate_action_observed_event.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_fact_candidate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_fact_acceptance_decision.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/accepted_tax_fact.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_fact_correction.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/accepted_tax_fact_journal.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_lot_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_wash_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_data_completeness_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_snapshot_build_manifest.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_position_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_decision_ranking_policy.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_rounding_policy.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_policy_registry.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/resolved_tax_policy_set.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_decision_dependency_fingerprint.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_execution_gate_result.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_correction_impact_index.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_decision_time_truth_view.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_current_corrected_truth_view.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/sell_tax_decision.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/buy_tax_decision.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/account_routing_tax_decision.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/harvest_candidate_decision.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_decision_journal.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/realized_tax_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_advisory_explanation.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/decision_replay_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/lot_reconciliation_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/broker_tax_reconciliation_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_corporate_action_candidate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_corporate_action_acceptance_decision.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TAX/tax_corporate_action_state.v1.schema.json`

Tax foundation invariants:

- observed tax events remain distinct from accepted tax facts
- accepted facts remain append-only with additive correction lineage
- snapshots are derived only from accepted facts
- policy is versioned and resolved separately from state
- decisions remain durable artifacts with dependency fingerprints
- execution consumes validation artifacts and does not recompute tax logic

These JSON schemas are governance-controlled and must be explicitly listed in `governance/00_MANIFEST.yaml` to be treated as governed artifacts:

### Existing governed schemas
- `governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v4.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MARKET_DATA/market_data_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MARKET_DATA/market_calendar.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ADVISORY/policy_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ADVISORY/household_state_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ADVISORY/compiled_constraints.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ADVISORY/allocation_plan.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ADVISORY/risk_envelope.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ADVISORY/rebalance_candidates.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ADVISORY/tax_adjudicated_rebalance.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_authorization.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ADVISORY/portfolio_decision_record.v1.schema.json`

### READINESS schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.latest_pointer.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/READINESS/sleeve_live_readiness.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_edge_fact_ledger.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_edge_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/READINESS/bug_metrics.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/READINESS/platform_readiness.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REGISTRIES/capability_policy_registry.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/capability_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_policy_verdict.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/production_policy_verdict.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/policy_diff.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/execution_outcome.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/next_day_readiness_probe.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/fresh_day_admission.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_build.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_startup_intent_input_convergence.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_startup_authorization_convergence.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization_input_convergence.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_admission.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/active_session.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/session_authority_status.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/session_authority_alert.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/market_calendar_coverage_status.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/trade_identity.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/incorporated_broker_trade_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciled_trade_description.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_health.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_provenance.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/reconciled_trade_state_summary.v1.schema.json`

### READINESS tools (governed writers)
- `ops/tools/run_trade_submit_readiness_c2_v1.py`
- `ops/tools/run_capability_state_v1.py`
- `ops/tools/run_paper_policy_verdict_v1.py`
- `ops/tools/run_production_policy_verdict_v1.py`
- `ops/tools/run_policy_diff_v1.py`
- `ops/tools/run_execution_outcome_v1.py`
- `ops/tools/run_next_day_readiness_probe_v1.py`
- `ops/tools/run_tomorrow_paper_startup_prep_v1.py`
- `ops/tools/run_fresh_day_admission_v1.py`
- `ops/tools/run_session_authority_v1.py`
- `ops/tools/run_day_authority_decision_v1.py`
- `ops/tools/run_paper_startup_intent_input_convergence_v1.py`
- `ops/tools/run_paper_startup_authorization_convergence_v1.py`
- `ops/tools/run_startup_materialization_input_convergence_v1.py`
- `ops/tools/run_session_authority_status_v1.py`
- `ops/tools/run_session_authority_alert_v1.py`
- `ops/tools/run_bod_execution_environment_proof_v1.py`
- `ops/tools/run_operator_day_authority_summary_v1.py`
- `ops/tools/run_day_failure_causality_v1.py`
- `ops/tools/run_day_open_trigger_v1.py`
- `ops/tools/run_day_open_attempt_v1.py`
- `ops/tools/run_reconciled_trade_state_v1.py`
- `ops/tools/run_session_authority_production_validation_v1.py`
- `ops/tools/run_market_calendar_coverage_authority_v1.py`
- `ops/tools/run_market_calendar_source_coverage_check_v1.py`
- `ops/tools/run_market_calendar_coverage_status_v1.py`
- `ops/governance/RUNBOOK_PAPER_EXECUTION_ROOT_AUTHORITY_V1.md`
- `ops/governance/RUNBOOK_PAPER_EXECUTION_PROFILE_AUTHORITY_V1.md`
- `ops/governance/RUNBOOK_PAPER_EXECUTION_ROOT_ISOLATION_V1.md`
- `ops/governance/RUNBOOK_EXECUTION_IDENTITY_AUTHORITY_V1.md`
- `ops/governance/IMPLEMENTATION_FIX_PAPER_EXECUTION_ROOT_AND_PROFILE_AUTHORITY_V1.md`
- `ops/governance/IMPLEMENTATION_EXECUTION_IDENTITY_AUTHORITY_V1.md`
- `ops/governance/RUNBOOK_BROKER_FACT_SPINE_V1.md`
- `ops/governance/IMPLEMENTATION_CORE1_BROKER_FACT_SPINE_V1.md`
- `ops/tools/run_broker_fact_spine_v1.py`
- `ops/tools/run_market_calendar_coverage_refresh_v1.py`
- `ops/tools/run_sleeve_live_readiness_v1.py`
  Advisory only; emitted output must carry an explicit non-canonical control warning.
- `ops/tools/run_constellation_bug_metrics_v1.py`
- `ops/tools/run_constellation_platform_readiness_v1.py`
- `ops/tools/run_capital_monitoring_attestation_v1.py`
- `ops/tools/run_startup_materialization_v1.py`
- `ops/tools/run_paper_trading_posture_v1.py`
- `ops/tools/run_submit_boundary_status_v1.py`
- `ops/tools/run_startup_proof_validation_v1.py`
- `ops/tools/run_operator_summary_v1.py`
- `ops/tools/run_intents_day_completeness_v1.py`
- `ops/tools/run_trading_day_intent_generation_v1.py`
- `ops/tools/run_startup_materialization_inputs_prep_v1.py`
- `ops/tools/run_phasec_risk_inputs_prep_v1.py`
- `ops/tools/run_paper_day_control_plane_v1.py`
- `ops/tools/run_trading_day_control_plane_v1.py`
- `ops/tools/run_trading_day_execution_control_plane_v1.py`
- `ops/tools/run_trading_day_state_machine_v1.py`
- `ops/tools/run_deployment_state_machine_v1.py`
- `ops/tools/run_execution_journal_v1.py`
- `ops/tools/run_current_system_projection_v1.py`
- `ops/tools/run_alerts_projection_v1.py`
- `ops/tools/run_performance_projection_v1.py`
- `ops/tools/run_recurrence_kill_gate_v1.py`
- `governance/05_CONTRACTS/C2/runtime_path_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/runtime_environment_boundary_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_alert_decision_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_outcome_v1.contract.md`
- `governance/05_CONTRACTS/C2/next_day_readiness_probe_v1.contract.md`
- `governance/05_CONTRACTS/C2/day_readiness_automation_v1.contract.md`
- `governance/05_CONTRACTS/C2/fresh_day_admission_v1.contract.md`
- `governance/05_CONTRACTS/C2/session_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/pre_open_materializer_v1.contract.md`
- `governance/05_CONTRACTS/C2/pre_open_bundle_v1.contract.md`
- `governance/05_CONTRACTS/C2/paper_startup_intent_input_convergence_v1.contract.md`
- `governance/05_CONTRACTS/C2/paper_startup_authorization_convergence_v1.contract.md`
- `governance/05_CONTRACTS/C2/paper_startup_admission_authorization_binding_v1.contract.md`
- `governance/05_CONTRACTS/C2/startup_materialization_input_convergence_v1.contract.md`
- `governance/05_CONTRACTS/C2/bod_execution_environment_proof_v1.contract.md`
- `governance/05_CONTRACTS/C2/session_readiness_refresh_v1.contract.md`
- `governance/05_CONTRACTS/C2/target_day_build_v1.contract.md`
- `governance/05_CONTRACTS/C2/target_day_admission_v1.contract.md`
- `governance/05_CONTRACTS/C2/session_promotion_decision_v1.contract.md`
- `governance/05_CONTRACTS/C2/active_session_v1.contract.md`
- `governance/05_CONTRACTS/C2/session_authority_status_v1.contract.md`
- `governance/05_CONTRACTS/C2/session_authority_alert_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_day_authority_summary_v1.contract.md`
- `governance/05_CONTRACTS/C2/day_failure_causality_v1.contract.md`
- `governance/05_CONTRACTS/C2/day_open_trigger_v1.contract.md`
- `governance/05_CONTRACTS/C2/day_open_attempt_v1.contract.md`
- `governance/05_CONTRACTS/C2/day_open_policy_v1.contract.md`

Canonical startup contract note:
- `pre_open_bundle_v1` is the canonical startup prerequisite closure surface for Session Authority PAPER day admission.
- `startup_materialization_v1` remains upstream fact-only and must not replace `pre_open_bundle_v1` for admission binding.
- `run_tomorrow_paper_startup_prep_v1.py` is the canonical Day Readiness Automation owner; it may refresh known refreshable prerequisites through canonical owners, rerun bootstrap, and write `day_readiness_automation_v1` as a subordinate operator projection only.
- `day_readiness_automation_v1` may classify real time/data-bound blockers as `WAITING_FOR_MARKET_DATA`, but it must never replace bootstrap, `pre_open_bundle_v1`, or `session_promotion_decision_v1` as startup truth.
- for startup reference-price readiness, `WAITING_FOR_MARKET_DATA` is expected before `09:30 America/New_York` unless a governed same-day positive core-session price already exists; at or after `09:30 America/New_York`, the first governed positive same-day `market_data_snapshot_v1` record is acceptable, while stale prior-day or liquidity-gate fallback prices remain invalid.
- canonical PAPER startup route: `ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh` -> `ops/tools/run_paper_session_bootstrap_v1.py` -> `ops/tools/run_pre_open_materializer_v1.py` -> `ops/tools/run_session_authority_v1.py` -> `ops/tools/run_day_open_attempt_v1.py` -> `ops/tools/run_c2_multi_sleeve_orchestrator_v1.py` -> `ops/tools/run_c2_paper_day_orchestrator_v2.py`
- `run_paper_session_bootstrap_v1.py` is the canonical operator-facing morning startup surface under the systemd entrypoint; it must stop before day-open when pre-open or Session Authority blocks the day.
- the canonical bootstrap report now includes `runtime_prerequisite_verification`, which is an operator-facing ordered owner map over startup prerequisites; it is a projection only and does not replace `pre_open_bundle_v1` or `session_promotion_decision_v1` as control truth.
- blocked morning output must identify the earliest failing owned prerequisite, its owner tool, and its artifact path, then instruct operators to fix that prerequisite and rerun the canonical entrypoint.
- when pre-open is complete but promotion is not `PROMOTED`, the canonical morning stop surface is `session_promotion_decision_v1`, and the morning wrapper must print that artifact path directly.
- delegated pre-open producer outcomes are now classified explicitly in `pre_open_bundle_v1.producer_results[]` as `PASS`, `FAIL`, `STALE`, `MISMATCH`, `BLOCKED`, or `UNAVAILABLE`.
- the upstream handshake latest-pointer artifact now carries explicit `status` and `reason_codes` so pre-open consumers do not need to infer all producer failure meaning from file presence alone.
- delegated startup producers may perform one immediate post-write read-back verification of the canonical artifact they just wrote; retry loops and background recovery are still prohibited.
- the handshake producer now publishes same-day handshake identity fields and can remain `UNAVAILABLE` from a written same-day fail artifact when broker/session evidence proves external unavailability rather than an intentional control-path block.

Registered day-open governance note:
- the existing day-open contract set above now carries a PAPER-only readiness-governed same-day-open override; LIVE / non-paper behavior remains strict under the same registered paths
- the runtime entry chain delegates submit sequencing to `run_c2_paper_day_orchestrator_v2.py` through the day-open and multi-sleeve wrappers

### Session Authority operator docs (governed via manifest)
- `ops/governance/RUNBOOK_SESSION_AUTHORITY_WITHHELD_ROLLOVER_V1.md`
- `ops/governance/RUNBOOK_SESSION_AUTHORITY_SOURCE_NOT_EXTENDED_V1.md`
- `ops/governance/RUNBOOK_SESSION_AUTHORITY_STALE_ARTIFACT_V1.md`
- `ops/governance/RUNBOOK_SESSION_AUTHORITY_HIDDEN_DEPENDENCY_V1.md`
- `ops/governance/RUNBOOK_SESSION_AUTHORITY_TRACEABILITY_FAILURE_V1.md`
- `ops/governance/PRODUCTION_READINESS_SESSION_AUTHORITY_V1.md`
- `ops/governance/IMPLEMENTATION_SESSION_AUTHORITY_PRODUCTION_READINESS_MONITORING_ALERTING_V1.md`
- `ops/governance/RUNBOOK_MARKET_CALENDAR_COVERAGE_AUTHORITY_V1.md`
- `ops/governance/RUNBOOK_MARKET_CALENDAR_SOURCE_MAINTENANCE_V1.md`
- `ops/governance/RUNBOOK_DAY_AUTHORITY_DECISION_MAINTENANCE_V1.md`
- `ops/governance/RUNBOOK_MARKET_CALENDAR_NEXT_DAY_COVERAGE_V1.md`
- `ops/governance/RUNBOOK_MARKET_CALENDAR_RUNTIME_REFRESH_V1.md`
- `ops/governance/RUNBOOK_SOURCE_NOT_EXTENDED_PREVENTION_V1.md`
- `ops/governance/IMPLEMENTATION_MARKET_CALENDAR_SOURCE_NOT_EXTENDED_PREVENTION_V1.md`
- `ops/governance/IMPLEMENTATION_PERMANENTIZE_FUTURE_DAY_MARKET_CALENDAR_COVERAGE_V1.md`
- `ops/governance/IMPLEMENTATION_PERMANENTIZE_DAY_AUTHORITY_DECISION_V1.md`
- `ops/governance/IMPLEMENTATION_EXTEND_GOVERNED_MARKET_CALENDAR_SOURCE_THROUGH_YEAR_END_V1.md`
- `ops/governance/RUNBOOK_TRADING_SYMBOL_POLICY_AUTHORITY_V1.md`
- `ops/governance/RUNBOOK_PAPER_ENTRY_POLICY_V1.md`
- `ops/governance/IMPLEMENTATION_RESOLVE_PAPER_ENTRY_AND_EXECUTION_ROOT_POLICY_V1.md`
- `ops/governance/IMPLEMENTATION_ENABLE_GOVERNED_PAPER_ENTRY_POLICY_V1.md`
- `ops/governance/IMPLEMENTATION_FIX_TRADING_SYMBOL_POLICY_AUTHORITY_V1.md`
- `ops/governance/paper_trading_today_operator_playbook_v1.md`
- `ops/tools/run_paper_session_ledger_v1.py`
- `ops/tools/run_paper_session_evidence_manifest_v1.py` (superseded by `paper_session_ledger_v1`)
- `ops/tools/run_paper_session_kernel_v1.py` (superseded by `paper_session_ledger_v1`)

### Engine activity schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/oms_decision.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/intents_day_rollup.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/heartbeat_gate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/no_intents_day.v1.schema.json`

### Phase J — Monitoring schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/MONITORING/portfolio_nav_series.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/engine_metrics.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/engine_daily_returns.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/engine_correlation_matrix.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/stress_replay_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/degradation_sentinel.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/capital_authority_monitoring_attestation.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/capital_efficiency.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/nav_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/nav_history_ledger.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/drawdown_window_pack.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/economic_truth_availability_certificate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/eod_slo_sentinel.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/regime_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/regime_snapshot.v2.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/engine_heartbeat.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/incident_event.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/stress_scenario_result.v1.schema.json`
- [Replay Integrity Schema JSON (V1)](04_DATA/SCHEMAS/C2/REPORTS/replay_integrity.v1.schema.json) 
— `governance/04_DATA/SCHEMAS/C2/REPORTS/replay_integrity.v1.schema.json`

### Reports schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_daily_gate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_gate_verdict.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/reconciliation_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/reconciliation_report.v2.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/pipeline_manifest.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/day_readiness_automation.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/eod_run_certificate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/liquidity_slippage_gate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RISK/liquidity_slippage_policy.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/exposure_reconciliation_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/delta_order_plan.v1.schema.json`
- [Broker Reconciliation Schema JSON (V1)](04_DATA/SCHEMAS/C2/REPORTS/broker_reconciliation.v1.schema.json) — id: C2_REPORTS_BROKER_RECONCILIATION_SCHEMA_V1 — status: DRAFT — `governance/04_DATA/SCHEMAS/C2/REPORTS/broker_reconciliation.v1.schema.json`
- [Engine Correlation Gate Schema JSON (V1)](04_DATA/SCHEMAS/C2/REPORTS/engine_correlation_gate.v1.schema.json) — id: C2_REPORTS_ENGINE_CORRELATION_GATE_SCHEMA_V1 — status: DRAFT — `governance/04_DATA/SCHEMAS/C2/REPORTS/engine_correlation_gate.v1.schema.json`
- [Broker Reconciliation Schema JSON (V2)](04_DATA/SCHEMAS/C2/REPORTS/broker_reconciliation.v2.schema.json) — id: C2_REPORTS_BROKER_RECONCILIATION_SCHEMA_V2 — status: DRAFT — `governance/04_DATA/SCHEMAS/C2/REPORTS/broker_reconciliation.v2.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/auto_repair_health_supervisor.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/auto_repair_trigger_decision.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/auto_repair_controller_state.v1.schema.json`

- `governance/04_DATA/SCHEMAS/C2/REPORTS/gate_stack_verdict.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/truth_surface_authority_gate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/failure_injection_harness.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_trading_posture.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_status.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/startup_proof_validation.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_summary.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_rollup.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_ledger.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/recurrence_registry.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/recurrence_kill_gate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/intents_day_completeness.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_intent_generation.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization_inputs_prep.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/phasec_risk_inputs_prep.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_day_control_plane.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_control_plane.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_execution_control_plane.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_state_machine.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/day_open_trigger.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/day_open_attempt.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/deployment_state_machine.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_evidence_manifest.v1.schema.json` (superseded by `paper_session_ledger_v1`)
- `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_kernel.v1.schema.json` (superseded by `paper_session_ledger_v1`)
- `governance/04_DATA/SCHEMAS/C2/REPORTS/execution_journal.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/current_system_projection.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/alerts_projection.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/performance_projection.v1.schema.json`

### Execution evidence schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_submission_decision.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_submission_record.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_lifecycle_decision.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_state_record.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_lifecycle_run_envelope.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_run_envelope.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/submission_index.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_event_raw.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_raw_evidence_envelope.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_event_day_manifest.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_event_stream_record.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/fill_ledger.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/FACTS/observation_session_fact.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/FACTS/observed_order_fact.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/FACTS/observed_order_status_fact.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/FACTS/observed_fill_fact.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/FACTS/observed_position_fact.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/broker_observation_health.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/broker_fact_spine_audit.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/execution_reconciliation.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/order_lifecycle_event.v1.schema.json`
- [Broker Statement Normalized Schema JSON (V1)](04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_statement_normalized.v1.schema.json) — id: C2_EXECUTION_EVIDENCE_BROKER_STATEMENT_NORMALIZED_SCHEMA_V1 — status: DRAFT — `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_statement_normalized.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_quarantine_tombstone.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_no_execution_event.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_evidence_submission_manifest.v3.schema.json`


### Risk schemas (governed outputs)

- `governance/04_DATA/SCHEMAS/C2/RISK/engine_model_registry.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RISK/engine_risk_budget_ledger.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RISK/global_kill_switch_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RISK/systemic_risk_gate.v3.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RISK/exposure_net.v1.schema.json`

### Positions schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/POSITIONS/position_lifecycle_ledger.v1.schema.json`

- governance/03_CONTRACTS/C2_TRUE_EVIDENCE_SPINE_V2.md — True Evidence Spine v2 (broker-truth contract)
- governance/03_CONTRACTS/C2_PHASED_SUBMISSION_RUNNER_V1.md — Phase D Submission Runner v1 (audit-grade submission entrypoint)
- governance/03_CONTRACTS/C2_LATEST_POINTER_MUTABILITY_CONTRACT_V1.md — Latest Pointer Mutability Contract v1 (atomic mutable latest pointers; day-keyed truth remains immutable)
- `governance/03_CONTRACTS/C2/EXECUTION_EVIDENCE/execution_observer_spine_v1.md` — Execution Observer Spine v1 (pull-based broker snapshot)
- `governance/03_CONTRACTS/C2/EXECUTION_EVIDENCE/fill_ledger_spine_v1.md` — Fill Ledger Spine v1 (deterministic aggregation)
- `governance/03_CONTRACTS/C2/EXECUTION_EVIDENCE/execution_reconciliation_spine_v1.md` — Execution Reconciliation Spine v1 (broker vs truth)

- [Accounting NAV Schema JSON (V2)](04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v2.schema.json) — id: C2_ACCOUNTING_NAV_SCHEMA_V2 — status: DRAFT — `governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v2.schema.json`
- [Engine Linkage Snapshot Schema JSON (V1)](04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/engine_linkage.v1.schema.json) — id: C2_ENGINE_LINKAGE_SCHEMA_V1 — status: DRAFT — `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/engine_linkage.v1.schema.json`
- [Engine Attribution Schema JSON (V2)](04_DATA/SCHEMAS/C2/ACCOUNTING/engine_attribution.v2.schema.json) — id: C2_ACCOUNTING_ENGINE_ATTRIBUTION_SCHEMA_V2 — status: DRAFT — `governance/04_DATA/SCHEMAS/C2/ACCOUNTING/engine_attribution.v2.schema.json`
- [Engine Daily Returns Schema JSON (V1)](04_DATA/SCHEMAS/C2/MONITORING/engine_daily_returns.v1.schema.json) — id: C2_MONITORING_ENGINE_DAILY_RETURNS_SCHEMA_V1 — status: DRAFT — `governance/04_DATA/SCHEMAS/C2/MONITORING/engine_daily_returns.v1.schema.json`
- [Correlation Preconditions Gate Schema JSON (V2)](04_DATA/SCHEMAS/C2/REPORTS/correlation_preconditions_gate.v2.schema.json) — id: C2_REPORTS_CORRELATION_PRECONDITIONS_GATE_SCHEMA_V2 — status: DRAFT — `governance/04_DATA/SCHEMAS/C2/REPORTS/correlation_preconditions_gate.v2.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RISK/systemic_risk_gate.v2.schema.json`
- [Replay Integrity Schema JSON (V2)](04_DATA/SCHEMAS/C2/REPORTS/replay_integrity.v2.schema.json) — id: C2_REPORTS_REPLAY_INTEGRITY_SCHEMA_V2 — status: DRAFT — `governance/04_DATA/SCHEMAS/C2/REPORTS/replay_integrity.v2.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/stress_drift_sentinel.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_stress_override.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RISK/systemic_risk_gate.v3.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/day_binding_gate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/state_surface.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/capital_seed_gate.v1.schema.json`
- `governance/05_CONTRACTS/C2/latest_pointer_fanout_elimination_v1.contract.md`
- `governance/05_CONTRACTS/C2/spine_exclusivity_v1.contract.md`
- `governance/05_CONTRACTS/C3/c3_ui_status_contract_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_future_day_override_v1.contract.md`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_future_day_override.v1.schema.json`
- `governance/05_CONTRACTS/C2/economic_truth_spine_v1.contract.md`
- `governance/05_CONTRACTS/C2/nav_v2_conditional_inputs_v1.contract.md`
- `governance/05_CONTRACTS/C2/orchestrator_abort_policy_v1.contract.md`
- `governance/05_CONTRACTS/C2/baseline_readiness_v1.contract.md`

### Subsystem Authority Architecture (governed)
- `ops/tools/run_subsystem_authority_v1.py`
- `governance/05_CONTRACTS/C2/subsystem_authority_manifest_model_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_authority_manifest_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_profile_authority_manifest_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_identity_binding_v1.contract.md`
- `governance/05_CONTRACTS/C2/broker_fact_spine_v1.contract.md`
- `governance/05_CONTRACTS/C2/broker_observation_health_v1.contract.md`
- `governance/05_CONTRACTS/C2/broker_fact_identity_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_identity_authority_manifest_v1.contract.md`
- `governance/05_CONTRACTS/C2/account_trading_policy_authority_manifest_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_summary_authority_manifest_v1.contract.md`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/subsystem_authority_manifest.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/execution_dossier.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/execution_profile_dossier.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/execution_identity_dossier.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/account_trading_policy_dossier.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_summary_dossier.v1.schema.json`
- `ops/governance/RUNBOOK_SUBSYSTEM_AUTHORITY_PRECEDENCE_V1.md`
- `ops/governance/IMPLEMENTATION_SUBSYSTEM_AUTHORITY_ARCHITECTURE_V1.md`

### Core 3 Action Authority (governed)
- `governance/05_CONTRACTS/C2/global_actionability_gate_v1.contract.md`
- `governance/05_CONTRACTS/C2/post_entry_action_policy_v1.contract.md`
- `governance/05_CONTRACTS/C2/action_conflict_resolution_v1.contract.md`
- `governance/05_CONTRACTS/C2/action_decision_provenance_v1.contract.md`
- `governance/05_CONTRACTS/C2/lifecycle_action_authority_v1.contract.md`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/global_actionability_gate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/candidate_action_set.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/lifecycle_action_authority.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/action_decision_provenance.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/lifecycle_action_operator_surface.v1.schema.json`
- `ops/tools/run_lifecycle_action_authority_v1.py`

### Core 1 hardening artifacts
- `governance/05_CONTRACTS/C2/broker_observation_trust_dependency_v1.contract.md`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/broker_observation_trust_dependency.v1.schema.json`
- `ops/fixtures/broker_fact_spine_reconnect_gap_sample_v1.jsonl`
- `governance/05_CONTRACTS/C2/core2_core1_dependency_gate_v1.contract.md`
- `governance/05_CONTRACTS/C2/core1_legacy_runtime_boundary_v1.contract.md`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/core1_pre_core2_readiness.v1.schema.json`
- `ops/tools/run_core1_pre_core2_readiness_v1.py`

### Core 4 Post-Entry Submit Boundary (governed)
- `governance/05_CONTRACTS/C2/post_entry_submit_boundary_v1.contract.md`
- `governance/05_CONTRACTS/C2/post_entry_action_request_v1.contract.md`
- `governance/05_CONTRACTS/C2/post_entry_boundary_snapshot_binding_v1.contract.md`
- `governance/05_CONTRACTS/C2/authorized_post_entry_payload_v1.contract.md`
- `governance/05_CONTRACTS/C2/post_entry_boundary_provenance_v1.contract.md`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/post_entry_action_request.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/post_entry_boundary_snapshot_binding.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/post_entry_submit_boundary.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/authorized_post_entry_payload.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/post_entry_boundary_provenance.v1.schema.json`
- `ops/tools/run_post_entry_submit_boundary_v1.py`
### Core 5 Operator Health (governed)
- `governance/05_CONTRACTS/C2/operator_trade_health_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_snapshot_binding_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_rollup_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_summary_provenance_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_narrative_rendering_v1.contract.md`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_snapshot_binding.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_trade_health.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_summary_provenance.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_timeline_view.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_trade_health_queues.v1.schema.json`
- `ops/tools/run_operator_trade_health_v1.py`
- `ops/tools/run_operator_trade_health_queues_v1.py`


### Upper-Layer Governance Planes (governed)
- `governance/05_CONTRACTS/C2/strategy_policy_projection_v1.contract.md`
- `governance/05_CONTRACTS/C2/orchestration_state_v1.contract.md`
- `governance/05_CONTRACTS/C2/orchestration_trigger_v1.contract.md`
- `governance/05_CONTRACTS/C2/exception_state_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_intervention_state_v1.contract.md`
- `governance/05_CONTRACTS/C2/upper_layer_provenance_spine_v1.contract.md`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/strategy_policy_projection.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/orchestration_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/orchestration_trigger.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/exception_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/operator_intervention_state.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/TRADE_STATE/upper_layer_provenance_spine.v1.schema.json`
- `ops/tools/run_strategy_policy_projection_v1.py`
- `ops/tools/run_exception_state_v1.py`
- `ops/tools/run_operator_intervention_state_v1.py`
- `ops/tools/run_orchestration_plane_v1.py`

### Execution Build Authority (governed)
- `governance/02_REGISTRIES/C2_EXECUTION_BUILD_MANIFESTS_V1.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/execution_dependency_manifest.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/execution_build.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_package.v1.schema.json`
- `governance/05_CONTRACTS/C2/execution_build_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_package_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_dependency_manifest_fresh_paper_entry_v1.contract.md`
- `ops/tools/run_execution_build_authority_v1.py`
- `ops/governance/RUNBOOK_EXECUTION_BUILD_AUTHORITY_V1.md`
- `ops/governance/IMPLEMENTATION_EXECUTION_BUILD_AUTHORITY_V1.md`


### Economic State Authority (governed)
- `governance/02_REGISTRIES/C2_ECONOMIC_STATE_MANIFESTS_V1.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/economic_dependency_manifest.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/economic_state_build.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ECONOMIC/economic_state_package.v1.schema.json`
- `governance/05_CONTRACTS/C2/economic_state_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/economic_state_package_v1.contract.md`
- `governance/05_CONTRACTS/C2/economic_dependency_manifest_fresh_paper_entry_v1.contract.md`
- `ops/tools/run_economic_state_authority_v1.py`
- `ops/governance/RUNBOOK_ECONOMIC_STATE_AUTHORITY_V1.md`
- `ops/governance/IMPLEMENTATION_ECONOMIC_STATE_AUTHORITY_V1.md`

Economic-state hardening note:
- `allocation_summary_v1` may remain materialized as a legacy advisory summary, but it is not canonical control truth after `capital_authority_allocation_v1` and sleeve-edge qualification cutover


### Global Context Authority (governed)
- `governance/02_REGISTRIES/C2_GLOBAL_CONTEXT_MANIFESTS_V1.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/global_context_dependency_manifest.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/global_context_build.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/CONTEXT/global_context_package.v1.schema.json`
- `governance/05_CONTRACTS/C2/global_context_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/global_context_package_v1.contract.md`
- `governance/05_CONTRACTS/C2/global_context_dependency_manifest_fresh_paper_entry_v1.contract.md`
- `ops/tools/run_global_context_authority_v1.py`
- `ops/governance/RUNBOOK_GLOBAL_CONTEXT_AUTHORITY_V1.md`
- `ops/governance/IMPLEMENTATION_GLOBAL_CONTEXT_AUTHORITY_V1.md`


### Control Plane Read Dominance (pre-migration)
- `governance/05_CONTRACTS/C2/control_plane_read_gateway_v1.contract.md`
- `governance/05_CONTRACTS/C2/control_plane_semantic_surface_v1.contract.md`
- `constellation_2/common/control_plane_read_gateway_v1.py`
- `ops/tools/read_control_plane_surface_v1.py`
- `constellation_2/common/control_plane_read_boundary_v1.py`

### Canonical Repo Protection + Patch Intake (governed)
- `governance/03_RUNTIME/CANONICAL_REPO_PROTECTION_AND_PATCH_INTAKE_V1.md`
- `governance/03_RUNTIME/PATCH_BASE_COMMIT_AND_READINESS_FREEZE_CONTRACT_V1.md`
- `ops/tools/require_canonical_repo_clean_v1.py`
- `ops/tools/create_codex_agent_workspace_v1.py`
- `ops/tools/verify_codex_agent_workspace_v1.py`
- `ops/tools/apply_codex_patch_bundle_v1.py`
- `ops/tools/verify_patch_bundle_base_commit_v1.py`
- `ops/tools/run_readiness_freeze_preflight_v1.py`
- `ops/tools/protect_canonical_repo_v1.py`
- `ops/tools/unprotect_canonical_repo_for_intake_v1.py`

### IB Reconciliation Loop v1 (governed)
- `governance/03_RUNTIME/IB_RECONCILIATION_LOOP_CONTRACT_V1.md`
- `ops/tools/run_ib_flex_normalize_v1.py`
- `ops/tools/run_aegis_expected_activity_extract_v1.py`
- `ops/tools/run_ib_reconciliation_v1.py`
- `ops/tools/run_ib_reconciliation_ai_packet_v1.py`
- `ops/tools/run_ib_reconciliation_daily_loop_v1.py`
