# Tax Policy And Decision Contract

Contract ID: `C2_TAX_POLICY_AND_DECISION_CONTRACT_V1`

## Purpose

Define the governed tax policy families, resolved policy sets, durable decision artifacts, and
dependency-fingerprint rules for tax decisions.

## Policy Families

Material decision behavior must be governed through explicit policy artifacts.

At minimum the tax domain must expose:

- account tax regime policy
- imported position restriction policy
- lot selection policy
- wash-sale enforcement policy
- gain and loss budget policy scaffold
- safe degraded mode policy
- ranking and tie-break policy
- rounding and precision policy

No material tax decision rule may exist only as hidden code.

## Resolved Policy Set

Tax decisions must consume a resolved policy set that is versioned separately from tax state.

The resolved policy set must bind:

- the contributing policy registry identity
- the effective scope
- the active account tax regime treatment
- ranking policy identity
- rounding policy identity

## Decision Artifact Requirements

Every tax decision must be durable and replayable. The decision artifact must bind:

- accepted fact set hash
- snapshot id
- resolved policy set id
- decision algorithm version
- dependency fingerprint
- machine-readable reason codes
- explicit degraded mode or block outcome when truth is incomplete

Execution-facing consumers must never recalculate tax logic ad hoc.

## Dependency Fingerprints

Every decision fingerprint must cover at minimum:

- relevant accepted fact set hash
- relevant lot state hashes
- relevant wash state hash
- account tax regime hash
- resolved policy set hash
- ranking policy hash
- rounding policy hash
- algorithm version

Consumers must be able to reject stale decisions deterministically.

## Safe Degraded Modes

The tax domain recognizes these safe degraded mode classes:

- `FULL_OPTIMIZATION_ALLOWED`
- `SAFE_EXECUTION_ALLOWED`
- `PREVIEW_ONLY`
- `REQUIRES_OPERATOR_REVIEW`
- `HARD_BLOCKED`

## Tie-Break Determinism

When multiple sell routes have equivalent primary rank, the final stable tie-break order is:

1. fewest policy warnings
2. lowest current-year tax cost
3. lowest wash-risk exposure
4. lowest lot-count fragmentation
5. lexical order by canonical lot ids

This ordering is governed policy, not an implicit implementation detail.

## No Silent Fallback Rule

The tax domain must never silently change lot-selection method or degrade from governed policy to
FIFO, best-effort, or implicit behavior.

If policy does not explicitly permit the method required to produce a decision, the decision must
degrade safely or hard-block with explicit reason codes.

