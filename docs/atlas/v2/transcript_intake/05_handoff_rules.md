# Handoff Rules

Cheap experiment handoff uses the existing `ExternalStrategyCheapExperimentHandoff` object.

Allowed handoff tiers are limited to:

- `TIER_0_DEDUPE`
- `TIER_1_SANITY`
- `TIER_2_LIGHTWEIGHT_VALIDATION`

The handoff may identify a cheap test boundary only. It cannot authorize trading, broker execution, autonomous execution, sleeves, candidates, paper positions, capital allocation, recommendations, validation authority, or allocation surfaces.
