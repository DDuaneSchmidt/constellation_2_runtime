# Aegis Hypothesis-Centric Research Requirements v1

## Purpose

Move Aegis from sleeve-centered research management to hypothesis-centered research portfolio management. Sleeves remain implementation vehicles. Hypotheses become the primary unit of truth, validation, learning, and research allocation.

## Non-Goals

This work does not implement autonomous broker execution, tax harvesting, Oak Harvest replacement, black-box allocation, or replacement sleeve scorecards.

## Required Entities

### Research Thesis

Required fields: `thesis_id`, `name`, `description`, `rationale`, `source`, `created_at`, `status`, `owner_type`, `related_hypotheses`, `evidence_summary`, `allocation_score`.

Allowed states: `PROPOSED`, `ACTIVE`, `EXPANDING`, `DEGRADED`, `RETIRED`.

### Hypothesis

Required fields: `hypothesis_id`, `thesis_id`, `name`, `formal_claim`, `market_universe`, `signal_definition`, `expected_behavior`, `invalidation_criteria`, `status`, `validation_state`, `evidence_score`, `confidence_score`, `linked_sleeves`, `linked_candidates`, `linked_paper_positions`, `linked_outcomes`.

Allowed states: `PROPOSED`, `INVESTIGATING`, `ACCUMULATING_EVIDENCE`, `VALIDATION_READY`, `VALIDATED`, `PROMOTED_TO_SLEEVE`, `SCALING`, `DEGRADED`, `DISPROVEN`, `RETIRED`.

### Sleeve Implementation

Required fields: `sleeve_id`, `hypothesis_id`, `thesis_id`, `implementation_version`, `candidate_generation_rules`, `validation_rules`, `paper_trade_rules`, `relationship_state`, `mapping_confidence`.

Allowed relationship states: `CANDIDATE_IMPLEMENTATION`, `PAPER_TESTING`, `ACTIVE_VALIDATION`, `SCALE_CANDIDATE`, `WATCH`, `RETIRE_CANDIDATE`, `RETIRED`.

### Evidence Object

Required fields: `evidence_id`, `hypothesis_id`, `sleeve_id`, `candidate_id`, `position_id`, `evidence_type`, `source_artifact`, `source_hash`, `day_utc`, `generated_at`, `validity_status`, `notes`.

### Research Portfolio

Required outputs: active theses, active hypotheses, validated hypotheses, degraded hypotheses, retired hypotheses, hypothesis-to-sleeve map, thesis-to-sleeve map, and deterministic research allocation recommendations.

## Audit Requirements

Daily self-check must fail explicitly when any active sleeve, candidate, paper position, validation sample, or allocation recommendation is missing deterministic hypothesis/thesis linkage. Missing mappings may only be repaired by a deterministic legacy rule or by source-declared fields in producer artifacts.

## Success Criteria

A daily portfolio artifact answers: what ideas Aegis is testing, which are gathering evidence, which are validation-ready, which are validated/disproven, which sleeves implement each idea, and where research effort should go next.
