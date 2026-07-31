# Cheap Experiment Generator Principles

Atlas V2 Cheap Experiment Generator V1 converts existing `ResearchHypothesis` records into `CheapExperimentSpec` records.

It generates specifications only. It does not execute experiments, calculate outcomes, record predictions, record outcomes, create candidates, create sleeves, create paper positions, route orders, allocate capital, recommend actions, or validate claims.

Allowed tiers are `TIER_0_DEDUPE`, `TIER_1_SANITY`, and `TIER_2_LIGHTWEIGHT_VALIDATION`. `TIER_3_ROBUST_VALIDATION` and `TIER_4_MATURITY_TRACKING` are forbidden until a later explicit gate exists.
