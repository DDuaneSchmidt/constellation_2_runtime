# Aegis Oil Shock Candidate Construction Requirements v1

## Intent

Complete the deterministic Oil Shock candidate-construction audit path without forcing candidates or bypassing Aegis gates.

## Requirements

- Produce `aegis_oil_shock_candidate_construction_v1` for each target day.
- Use governed Oil Shock artifacts as authority: proposal, evidence packet, shadow trial, promotion packet, approval queue/event, paper setup, market-data universe consistency, and candidate-flow enablement.
- Report construction fields: hypothesis id, sleeve/generated sleeve id, signal id, symbol or basket, direction, instrument type, entry logic, exit logic, risk policy id, exit policy id, expected holding period, required evidence fields, governance status, construction status, and reason codes.
- Do not emit raw signals when governed approval/setup/policy fields are missing.
- Classify missing governed construction policy as `POLICY_INCOMPLETE`, not market-data failure.
- Classify absent market setup as `NO_MARKET_SETUP` only after construction policy is ready and market evidence is complete.
- Preserve candidate contracts, entry reference certification, paper lifecycle, outcome registry, and validation eligibility as authoritative downstream gates.
- Do not enable broker execution, live trading, trade advice, real capital allocation, autonomous execution, order management, or forced candidates.
