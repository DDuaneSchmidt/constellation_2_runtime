# First Batch Protocol

The first Atlas V2 cheap experiment batch should use a fixed fixture list of at least 100 `TIER_0_DEDUPE` and `TIER_1_SANITY` experiments.

## Protocol

1. Build a fixed fixture list. Do not source from discovery, crawling, recommendation systems, trading systems, or candidate systems.
2. Each fixture must include a bounded `prediction_statement`, tier, expected learning value, attention cost estimate, outcome summary, and actual learning value.
3. Run the batch through the Atlas V2 ledger batch runner.
4. Confirm `CheapExperiment` records were emitted for accepted fixtures.
5. Confirm outcome-linked `ExperienceEvent` records exist where supported.
6. Confirm `LearningVelocityMetric` and per-tier `ExperimentTierSummary` records were emitted.
7. Confirm authority audits pass.
8. Reject any `TIER_3` or `TIER_4` fixture that lacks a `PromotionGateDecision`.

## Required Proof

A valid first batch demonstrates only this chain:

`fixed fixture -> prediction -> outcome -> CheapExperiment -> optional ExperienceEvent -> LearningVelocityMetric`

It does not demonstrate validation authority, trade readiness, candidate readiness, sleeve readiness, allocation readiness, or recommendation quality.
