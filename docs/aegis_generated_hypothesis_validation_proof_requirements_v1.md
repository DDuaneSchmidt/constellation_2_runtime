# Aegis Generated Hypothesis Validation Proof Requirements v1

## Intent

Prove whether generated hypotheses move from proposal and approval into real validation evidence, or identify the exact stage where each hypothesis stops.

## Scope

This is research-only tracking and proof. It must not force candidates, fabricate market setups, alter hypothesis logic, bypass gates, create paper observations, change validation rules, or affect trading and broker behavior.

## Required Stages

The proof tracks:

Proposal -> Shadow Validation -> Approval -> Paper Tracking Ready -> Candidate Flow -> Paper Observation Flow -> Outcome Flow -> Validation Sample Flow.

Each stage state must be one of:

- `NOT_STARTED`
- `READY`
- `FLOWING`
- `BLOCKED`
- `NEEDS_DATA`
- `WAITING_FOR_MARKET_CONDITIONS`
- `COMPLETE`
- `NOT_APPLICABLE`

## Required Current Results

Oil Shock must show paper tracking ready, stopped at candidate flow, no validation samples, `blocker_owner=MARKET_CONDITIONS`, and no David action.

Macro Calendar must stop at data readiness / shadow validation with `blocker_owner=DAVID` and David action required.

## Safety

The proof is read-only and must preserve disabled broker execution, live trading, trade advice, real-capital allocation, autonomous execution, order management, candidate mutation, paper lifecycle mutation, outcome mutation, and validation mutation.
