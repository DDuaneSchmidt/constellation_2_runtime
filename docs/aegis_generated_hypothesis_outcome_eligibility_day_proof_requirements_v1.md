# AEGIS Generated Hypothesis Outcome Eligibility Day Proof Requirements v1

## Purpose

Package 020 proves the first eligible outcome-day state for the Oil Shock generated-hypothesis paper observation on `TARGET_DAY=2026-06-03`. It answers whether deterministic outcome creation produced an authoritative outcome row, or why the paper observation remains not ready.

## Required Inputs

- `aegis_paper_position_ledger_v1` for the authoritative paper observation and lineage.
- `aegis_paper_outcome_auto_closure_v1` for deterministic close evaluation state.
- `aegis_exit_recommendations_v1` for close-rule and exit recommendation evidence.
- `aegis_outcome_registry_v1` for authoritative outcome row presence.
- Package 018 and Package 019 artifacts as prior lifecycle evidence.
- Generated hypothesis validation proof as a read-only advancement reference.

## Output

The artifact is written to:

`truth/reports/aegis_generated_hypothesis_outcome_eligibility_day_proof_v1/<TARGET_DAY>/generated_hypothesis_outcome_eligibility_day_proof.v1.json`

It must report the generated hypothesis id, sleeve id, candidate contract id, paper observation id, paper position id, original entry date, target day, age in calendar and trading days, minimum holding period, holding status, mark status, exit-price status, close-rule status, close-condition status, outcome status, blocker fields, owner, and David action requirement.

## Invariants

- The proof is read-only.
- It does not create outcomes, validation samples, research quality rows, candidates, paper observations, trades, broker actions, allocations, close-rule changes, holding-period changes, or safety-gate changes.
- Validation proof may advance only when an authoritative outcome row already exists in the deterministic outcome registry.
- Not-ready states must use deterministic blocker codes, never generic errors.
