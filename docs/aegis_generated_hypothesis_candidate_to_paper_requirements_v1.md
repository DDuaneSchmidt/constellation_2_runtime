# Aegis Generated Hypothesis Candidate-to-Paper Requirements v1

## Scope

`aegis_generated_hypothesis_candidate_to_paper_v1` proves whether the Oil Shock generated-hypothesis candidate reaches paper observation flow through normal Aegis gates, or stops with a deterministic blocker.

## Required Trace

The artifact traces:

1. Oil Shock raw signal and candidate contract.
2. Entry reference price certification.
3. Paper trade construction.
4. Candidate-to-paper lifecycle auto-promotion.
5. Paper position ledger.
6. Outcome registry and validation-sample pending state.

## Non-Mutation Requirements

The proof must not create raw signals, candidate contracts, paper observations, paper positions, outcome rows, validation samples, allocation changes, trade advice, broker actions, live trading state, or safety-gate changes.

## Required Stop Behavior

Candidate-contract rejection, entry certification failure, paper construction failure, and auto-promotion blocks must be preserved exactly from their upstream artifacts. The proof reports those blockers and does not override them.
