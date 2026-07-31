# Cheap Experiment Batch Runner

The Atlas V2 cheap experiment batch runner exists to record many low-cost prediction-to-outcome cycles from fixed fixtures or mock historical data. It is learning evidence only.

## Object

`CheapExperimentBatch` summarizes one append-only batch run. It records fixture count, accepted count, rejected count, emitted record counts, allowed tiers, and explicit forbidden-authority acknowledgement.

Allowed batch modes:

- `fixture`
- `mock_historical`

Allowed runnable tiers:

- `TIER_0_DEDUPE`
- `TIER_1_SANITY`

`TIER_3_ROBUST_VALIDATION` and `TIER_4_MATURITY_TRACKING` are not executable by the high-volume batch path unless a separate `PromotionGateDecision` exists. The batch runner rejects ungated TIER_3/TIER_4 fixtures instead of downgrading or inventing authority.

## Emitted Records

For each accepted fixture, the runner may emit:

- `AttentionDecision`
- `Prediction`
- `Outcome`
- `CheapExperiment`
- `ExperienceEvent` when outcome linkage is supported

For the batch, the runner emits:

- `CheapExperimentBatch`
- `LearningVelocityMetric`
- `ExperimentTierSummary` for each high-volume tier

All outputs are append-only Atlas V2 JSONL records.

## Authority Boundary

The batch runner is record/read/audit only. It must not create candidates, sleeves, paper positions, trades, allocations, recommendations, broker actions, autonomous actions, validation authority, discovery, generation, or YouTube ingestion.
