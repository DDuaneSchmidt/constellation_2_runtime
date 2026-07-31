# Aegis Sleeve Throughput Diagnostics Spec V1

## Artifact

Family: `aegis_sleeve_throughput_diagnostics_v1`

File: `sleeve_throughput_diagnostics.v1.json`

## Inputs

- `aegis_research_portfolio_v1`
- `aegis_hypothesis_workflow_state_v1`
- `aegis_generated_hypothesis_throughput_v1`
- `aegis_signal_evidence_graph_v1`
- `aegis_candidate_contracts_v1`
- `aegis_candidate_to_paper_lifecycle_v1`
- `aegis_paper_position_ledger_v1`
- `aegis_outcome_registry_v1`
- `aegis_validation_samples_v1`
- `aegis_research_quality_engine_v1`
- `aegis_macro_calendar_data_readiness_v1`
- `aegis_market_data_universe_consistency_v1`
- `aegis_operator_action_queue_v1`

## Sleeve Row Fields

Each row includes sleeve identity, stage counts, most recent activity timestamp, throughput status, furthest stage, blocker details, owner, David-action flag, expected/abnormal classification, source artifact paths, and source hashes.

## Status Rules

- `FLOWING`: evidence reached candidate, contract, paper observation, and outcome or validation.
- `UNDERPRODUCING`: paper observation exists without outcomes, or outcomes exist without validation samples.
- `BLOCKED`: evidence is present at one stage but cannot advance to the next, or workflow/action evidence declares a data/action blocker.
- `DORMANT`: no signal or downstream evidence exists for the target day.
