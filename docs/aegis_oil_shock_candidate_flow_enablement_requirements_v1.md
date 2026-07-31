# Aegis Oil Shock Candidate Flow Enablement Requirements v1

## Intent
Move Oil Shock from paper-tracking readiness into deterministic candidate-flow evaluation, or prove exactly why candidates cannot be produced yet.

## Scope
This package is research-only. It may inspect producer, market-data, candidate diagnostics, candidate contracts, paper lifecycle, outcome, and validation artifacts. It must not create broker orders, trading advice, live positions, real-capital allocation, autonomous execution, paper observations, or forced candidates.

## Required Classification
The enablement artifact must classify Oil Shock as exactly one of:

- `WAITING_FOR_MARKET_CONDITIONS`
- `SYSTEM_DATA_PIPELINE_REQUIRED`
- `PRODUCER_MISSING`
- `POLICY_INCOMPLETE`
- `CANDIDATE_CONSTRUCTION_INCOMPLETE`
- `NO_MARKET_SETUP`
- `CANDIDATE_FLOW_STARTED`

`NO_MARKET_SETUP` means the producer ran and all required system evidence exists, but the deterministic market setup did not qualify. David action remains false.

`SYSTEM_DATA_PIPELINE_REQUIRED` means system evidence, such as required market-data symbols, is missing or stale. David action remains false unless Aegis creates an explicit operator-provided source action.

## Required Fields
The artifact must emit hypothesis id/name, producer status, producer registration/run status, candidate-flow status, candidate count, raw signal count, required/available/missing evidence fields, blocker code, blocker owner, David action flag, next expected step, source paths, source hashes, and computation time.
