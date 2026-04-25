# Classification Manifest Usage Contract

Contract ID: `ADVISORY_CLASSIFICATION_MANIFEST_USAGE_CONTRACT_V1`

## Purpose

Define how classification manifests may be used by `HouseholdSnapshot` and `PortfolioIntent`.

## Scope

This contract governs usage of classification truth in the advisory domain. It does not itself define a new classification data source.

## Ownership

Governance owns classification-manifest usage rules. Advisory authorities consume classification sources only through explicit governed references.

## Canonical Inputs

- classification manifest references
- asset identifiers
- account classification references where available
- holdings or balance records being classified

## Canonical Outputs

Classified advisory outputs must include:

- classification manifest references used
- explicit separation of classified versus unclassified components
- explicit verified versus unverified separation when classification certainty differs

## Allowed Writers

- `HouseholdSnapshot` and `PortfolioIntent` writers may consume governed classification references

## Forbidden Writers

- UI/read-model layers asserting new classifications
- portfolio-intent writers silently filling unknown classifications
- recommendation producers inventing classification assumptions

## Invariants

- classification gaps must remain explicit
- verified and unverified components must remain separated
- classification provenance must be reproducible
- lack of classification support must reduce authority rather than be silently approximated

## Failure States

- unknown classification
- classification manifest missing
- duplicate or conflicting classification for the same asset

Failure handling:

- degrade validity tier or hard stop depending on scope and downstream requirement

## Validity Model

- validity is inherited into `HouseholdSnapshot` and `PortfolioIntent` using the governed four-tier vocabulary

## Lineage Requirements

- authoritative records using classification must store manifest refs and the affected scope

## Replay Expectations

- replay must reuse the same classification manifests and reproduce the same classified or unclassified split

## Projection Relationship

- UI may summarize classification completeness
- it must not become the authority for class membership

## Operator Intervention Rules

- classification overrides must be explicit override records with lineage; no silent edits to past classified records

