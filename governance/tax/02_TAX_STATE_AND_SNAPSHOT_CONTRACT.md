# Tax State And Snapshot Contract

Contract ID: `C2_TAX_STATE_AND_SNAPSHOT_CONTRACT_V1`

## Purpose

Define deterministic tax state derivation from accepted facts only, together with the build
manifest required for reproducible tax snapshots.

## Snapshot Ownership

Tax snapshots are derived state only.

- accepted tax facts are the only authoritative input truth for tax snapshots
- observed events, candidates, and acceptance decisions are supporting evidence and must not be
  treated as current tax state
- reporting and advisory readers are read-only consumers

## Build Manifest

Every tax snapshot must bind an explicit build manifest containing at minimum:

- `snapshot_id`
- `scope_id`
- `as_of_effective_at`
- `facts_included_through_recorded_at`
- `accepted_fact_set_hash`
- `build_ruleset_id`
- `policy_dependency_version_set`
- `algorithm_version_set`

## Deterministic Ordering

Snapshot construction must use deterministic canonical ordering for:

- accepted fact replay
- correction application
- lot ordering
- scope ordering
- JSON serialization

## Restriction Propagation

Unknown or incomplete tax truth must flow through the derived state as explicit restrictions.

- unknown basis must emit `TAX_BASIS_UNKNOWN`
- unknown holding period must emit `TAX_HOLDING_PERIOD_UNKNOWN`
- imported-position restrictions must remain visible at lot state and snapshot level
- unresolved corporate action impact must emit `TAX_CORP_ACTION_UNRESOLVED`

## Decision-Time Truth Versus Current Corrected Truth

The system must preserve both:

- decision-time truth: the accepted fact set and corrections visible at the time a snapshot or
  decision was built
- current corrected truth: the latest additive correction state after later corrections

The build manifest is the boundary that makes decision-time truth reproducible.

## Snapshot Outputs

Every governed tax snapshot must expose at minimum:

- account tax regime
- lot basis
- holding period state
- realized and unrealized tax classification
- wash-sale state
- confidence flags
- restrictions
- data quality flags
- machine-readable reason codes

