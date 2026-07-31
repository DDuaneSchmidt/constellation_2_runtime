# Batch Ingestion

Supported batch sizes:

- Up to 1,000 claims: `UP_TO_1000`
- Up to 5,000 claims: `UP_TO_5000`
- Up to 10,000 claims: `UP_TO_10000`

Allowed claim origins:

- `TRANSCRIPT_INTAKE`
- `MANUAL_CLAIM_ENTRY`
- `HISTORICAL_RESEARCH_ARTIFACTS`
- `EXTERNAL_STRATEGY_CLAIMS`

Each claim must provide claim text. Optional source and claim identifiers are preserved when supplied. Missing identifiers are deterministic local intake identifiers, not downstream authority objects.

The batch emits `ClaimBatch`, `ClaimBatchMetrics`, `MechanismRegistry`, `MechanismCluster`, and `ClaimClusterSummary` records when a ledger is provided.
