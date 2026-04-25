---
id: C2_EXECUTION_PACKAGE_V1
title: "C2 Sealed Execution Package Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_pre_submit_execution_package
---

# Purpose

This contract defines the sealed execution package consumed by Submit Boundary.

A sealed execution package is the first-class artifact meaning "this governed candidate is executable now under the current closure basis".

# Required fields

- candidate ref
- attempt id
- intent id
- trade instance id
- submission id
- selected order plan ref
- dependency manifest ref
- build ref
- dependency refs with hashes
- `sealed=true`
- `sealed_utc`
- `seal_basis=full closure achieved`

# Trust basis

Submit Boundary must trust the package only when:

- the package schema is valid
- the package is sealed
- the referenced build artifact exists and has `closure_status=COMPLETE`
- dependency hashes match referenced artifacts
- fast-moving live invariants still pass at submit time

# Non-authoritative artifacts

A raw Phase C directory, readiness status, or operator summary alone is not a sealed package and must not be treated as executable authority.
