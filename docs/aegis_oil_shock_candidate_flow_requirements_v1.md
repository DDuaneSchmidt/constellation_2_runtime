# Aegis Oil Shock Candidate Flow Requirements v1

## Purpose
Prove whether the approved Oil Shock hypothesis can move from `PAPER_TRACKING_READY` into deterministic candidate generation and paper-validation flow.

## Scope
Research-only candidate-flow proof for Oil Shock reversals across energy ETFs. The proof diagnoses candidate readiness and blockers. It must not force candidates, bypass existing gates, create broker actions, provide trade advice, mutate allocations, or manage real-world positions.

## Required Artifact
`aegis_oil_shock_candidate_flow_v1` must report hypothesis id, paper setup status, instrument universe, required and available evidence fields, missing fields, candidate-generation rules, construction readiness, entry-price certification path, exit-policy path, reason no candidates have generated, blocker classification, candidate-flow status, and safety flags.

## Candidate Behavior
If no valid candidate exists, emit deterministic reason codes among `NO_MARKET_SETUP`, `MISSING_DATA`, `PRODUCER_MISSING`, `CANDIDATE_CONSTRUCTION_INCOMPLETE`, `POLICY_INCOMPLETE`, `WAIT_FOR_MARKET_CONDITIONS`, and `IMPLEMENTATION_DEFECT`.

If a valid candidate already exists in existing deterministic candidate artifacts, the proof must report that it has entered the existing candidate -> contract -> paper construction -> paper tracking -> outcome/validation path. It must not synthesize candidates.

## AI Boundary
AI Research Intelligence may be used only for advisory root-cause text. AI must not create, approve, reject, or route candidates.

## UI Requirement
The Research page must show Oil Shock candidate-flow status, current state, reason no candidates exist if applicable, next expected step, and David action requirement. If no candidate is ready, the UI language must include: `No David action required. Aegis is waiting for qualifying Oil Shock candidate conditions.`
