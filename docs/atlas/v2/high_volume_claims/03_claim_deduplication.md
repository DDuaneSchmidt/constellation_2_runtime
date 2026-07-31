# Claim Deduplication

Atlas must not treat every claim as unique.

Deduplication uses:

- normalized claim fingerprints
- mechanism fingerprints
- mechanism-family clustering
- source diversity counts

`MechanismCluster` fields:

- `cluster_id`
- `mechanism_family`
- `claim_ids`
- `claim_count`
- `source_diversity`
- `confidence`

`ClaimClusterSummary` records cluster counts and the dedupe strategy. Duplicate claims may increase confidence in intake coverage, but they do not create one experiment per claim.
