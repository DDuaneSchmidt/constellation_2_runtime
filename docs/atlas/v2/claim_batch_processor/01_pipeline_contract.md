# Pipeline Contract

The Phase 1 pipeline is fixed:

1. Claims
2. Dedupe
3. Mechanism Classification
4. Contrarian Theory
5. Mechanism Registry
6. Cheap Experiment Eligibility

Raw claim IDs are stored on ClaimBatch. Batch runs process up to 10,000 claims in memory and emit run-level evidence. Mechanism, contrarian, registry, and cheap eligibility records are emitted once per compressed mechanism fingerprint.

Batch size tiers are:

- UP_TO_1000
- UP_TO_5000
- UP_TO_10000

The success metric is satisfied when 10,000 input claims compress to fewer than 500 unique mechanisms.
