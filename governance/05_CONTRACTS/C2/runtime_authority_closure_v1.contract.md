---
id: C2_RUNTIME_AUTHORITY_CLOSURE_V1
title: "C2 Runtime Authority Closure Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_runtime_authority_closure
---

# C2 Runtime Authority Closure Contract v1

## Purpose

Define the final Bundle 7 closure rules for remaining active repo-local and runtime-copy truth references.

## Active-path definition

For Bundle 7, `active path` means:

- release and deployment control surfaces
- runtime-state, diagnostics, bug-metrics, and platform-readiness surfaces
- trust-plane operator/timeline projection surfaces
- operator/reporting surfaces that consume those readiness or trust artifacts
- validators and gates that govern those same surfaces

Historical docs, quarantined scripts, and explicitly legacy surfaces are out of scope unless separately ratified back onto the live path.

## Forbidden roots

The following are forbidden on active paths:

- `constellation_2/runtime/truth`
- `constellation_2/runtime/truth_sleeves`
- `/home/node/constellation/constellation_2/runtime/truth`
- `/home/node/constellation/constellation_2/runtime/truth_sleeves`
- `/home/node/constellation_2_runtime/constellation_2/runtime/truth`
- `/home/node/constellation_2_runtime/constellation_2/runtime/truth_sleeves`
- direct execution-root assumptions rooted at `/home/node/constellation_2_runtime` except where governance already marks that path diagnostic-only

## Allowed exceptions

Allowed only when explicitly labeled and non-authoritative:

- deployment drift detection over `/home/node/constellation_2_runtime`
- quarantine or historical references outside the active path
- tests whose purpose is to prove forbidden-root rejection

## Proof law

Contracts, tools, and tests on active paths MUST prove canonical-root compliance by one of:

- resolving through canonical runtime-contract helpers
- validating emitted artifact refs that already lie under canonical truth roots
- failing closed when a forbidden root is detected

## Fail-closed behavior

If an active-path tool, validator, or gate detects a forbidden root:

- it MUST report the hit explicitly
- it MUST NOT silently fall back to repo-local or runtime-copy truth
- any release/readiness decision depending on that surface MUST fail closed
