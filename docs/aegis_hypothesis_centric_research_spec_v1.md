# Aegis Hypothesis-Centric Research Spec v1

## Artifacts

- `reports/aegis_research_thesis_registry_v1/<day_utc>/thesis_registry.v1.json`
- `reports/aegis_hypothesis_registry_v1/<day_utc>/hypothesis_registry.v1.json`
- `reports/aegis_hypothesis_state_v1/<day_utc>/hypothesis_states.v1.json`
- `reports/aegis_research_portfolio_v1/<day_utc>/research_portfolio.v1.json`
- `reports/aegis_research_allocation_v1/<day_utc>/research_allocation.v1.json`
- `reports/aegis_research_portfolio_self_check_v1/<day_utc>/self_check.v1.json`

## Evidence Contracts

Candidate contracts should carry `hypothesis_id`, `thesis_id`, and `hypothesis_mapping_confidence` when generated after this upgrade. Historical candidates are linked in the portfolio artifact by deterministic sleeve-based migration rules.

Paper positions retain candidate/sleeve lineage and inherit `hypothesis_id`, `thesis_id`, and `hypothesis_mapping_confidence` from candidate lineage or deterministic sleeve migration.

Validation samples/results are grouped first by `hypothesis_id`, then by sleeve. If a sample lacks source-declared hypothesis linkage, it may only be linked through a deterministic legacy mapping from candidate, position, or sleeve.

## State Transitions

Transitions are rule-based. Every transition emits `prior_state`, `new_state`, `transition_reason`, `required_evidence`, `evidence_artifact_paths`, `timestamp`, and `source_hashes`.

Hypothesis transition rules:
- no candidates and no samples: `INVESTIGATING`
- candidates but no paper positions: `ACCUMULATING_EVIDENCE`
- paper positions or validation samples below validation threshold: `ACCUMULATING_EVIDENCE`
- sample count at threshold but no validation failure: `VALIDATION_READY`
- validation result supported: `VALIDATED`
- validation failure: `DISPROVEN`
- negative expectancy with open evidence but insufficient validation: `DEGRADED`

Sleeve relationship transition rules:
- no paper positions: `CANDIDATE_IMPLEMENTATION`
- paper positions but insufficient validation: `PAPER_TESTING`
- validation samples/results present: `ACTIVE_VALIDATION`
- validated hypothesis and positive expectancy: `SCALE_CANDIDATE`
- degraded/disproven hypothesis: `WATCH` or `RETIRE_CANDIDATE`

## Research Allocation Score

The allocation score is deterministic and explainable. Inputs: evidence quality, sample count, sample independence, regime coverage, candidate yield, validation progress, performance expectancy, uncertainty, time to decision, strategic diversification value, resource cost.

Allowed recommendations: `INCREASE`, `MAINTAIN`, `REDUCE`, `PAUSE`, `RETIRE`, `INVESTIGATE_MORE`.

Every recommendation must include reason codes.
