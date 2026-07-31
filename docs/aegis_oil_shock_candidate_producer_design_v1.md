# Aegis Oil Shock Candidate Producer Design v1

## Architecture
The producer is a narrow research-only sleeve integrated into the existing Aegis candidate-generation architecture. It writes a standard sleeve evaluation artifact so existing consumers can discover its status without special UI inference.

## Registration
`C2_OIL_SHOCK_REVERSAL_V1` is registered in the engine model registry and supporting research policy registries. The runner path points to the Oil Shock exposure-intent producer, while the package command writes the producer proof artifact and sleeve evaluation row.

## Determinism
All timestamps inside deterministic content are TARGET_DAY anchored where possible. Content hashes exclude generated wall-clock time. Same TARGET_DAY and same inputs produce the same status, reason codes, output intent signature, and candidate-contract behavior.

## Failure Model
- Missing approved artifacts or required universe data: `MISSING_DATA`.
- Missing proposal event thresholds or candidate policy details: `CANDIDATE_CONSTRUCTION_INCOMPLETE`.
- Missing entry/exit/risk policy path: `POLICY_INCOMPLETE`.
- Complete inputs but no qualifying event condition: `NO_MARKET_SETUP`.
- Qualifying event condition and emitted raw signal: `VALID_CANDIDATE_SIGNAL`.

## UI Model
The Research page renders backend fields only: producer status, last evaluation status, reason codes, candidate count, and David action flag. It does not infer whether Oil Shock is ready.

## Safety
The producer never mutates allocation, creates paper observations, creates orders, calls broker APIs, enables trade advice, or approves candidates.
