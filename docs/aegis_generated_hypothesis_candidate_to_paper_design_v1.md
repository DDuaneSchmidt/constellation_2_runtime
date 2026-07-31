# Aegis Generated Hypothesis Candidate-to-Paper Design v1

## Design

The builder reads existing truth artifacts and performs an ordered lookup for Oil Shock lineage. It identifies the candidate from the signal-to-candidate proof or candidate-contract artifact, then joins entry certification, paper construction, lifecycle, paper ledger, outcome registry, and validation samples by candidate id, raw signal id, paper position id, or outcome id.

## Gate Classification

The first failed gate determines `current_stop_stage` and `current_stop_reason`:

- Candidate-contract rejection stops at candidate contract.
- Non-certified entry price stops at entry reference price.
- Incomplete paper construction stops at paper construction and includes missing fields.
- Blocked auto-promotion stops at auto-promotion and preserves lifecycle reason codes.
- Successful auto-promotion plus ledger evidence marks paper observation created.

## Safety

The artifact is read-only. It does not call candidate creation, paper event materialization, outcome closure, validation sampling, broker, allocation, or order-management code.
