# Aegis Generated Hypothesis Signal-to-Candidate Requirements v1

## Intent

Trace approved generated-hypothesis raw signals, starting with Oil Shock, through the existing signal evidence graph and candidate-contract pipeline without bypassing any gate.

## Requirements

- Emit `aegis_generated_hypothesis_signal_to_candidate_v1` for each target day.
- Report the Oil Shock raw signal ID, schema status, direction, instrument type, symbol, governance status, construction policy, risk policy, exit policy, entry reference price status, candidate contract status, rejection reasons, and missing fields.
- Classify failures as `RAW_SIGNAL_MISSING`, `SIGNAL_SCHEMA_INCOMPLETE`, `SIGNAL_EVIDENCE_GRAPH_MISSING`, `CANDIDATE_CONSTRUCTION_POLICY_MISSING`, `CANDIDATE_CONTRACT_REJECTED`, `ENTRY_PRICE_CERTIFICATION_FAILED`, `GOVERNANCE_POLICY_MISSING`, `NO_MARKET_SETUP`, or `CANDIDATE_CONTRACT_CREATED`.
- Preserve candidate-contract rejection reason codes. The proof may not override rejections.
- If a valid Oil Shock signal exists but candidate contracts are stale or absent, report `STALE_CANDIDATE_CONTRACTS` with `CANDIDATE_CONTRACT_ABSENT_AFTER_VALID_SIGNAL`.
- The audit chain must regenerate signal evidence, candidate contracts, and candidate-to-paper lifecycle after generated-hypothesis producers emit raw signals.
- The proof is read-only and must not create raw signals, candidate contracts, paper observations, outcomes, trades, allocations, broker actions, or safety-gate changes.
