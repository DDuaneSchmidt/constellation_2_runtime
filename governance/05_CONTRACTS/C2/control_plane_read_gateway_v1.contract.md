# Control Plane Read Gateway V1

Status: DRAFT
Contract ID: `control_plane_read_gateway_v1`
Contract Class: `read_boundary`

## Purpose

This contract defines the only approved read boundary for governed control-plane truth during the pre-migration read-dominance phase.

## Scope

The gateway governs reads for control-plane truth surfaces in runtime, UI, and control-plane codepaths.

In scope:
- release runtime contract and active release manifest
- configuration activation current state and compiled active config
- session current/status/alert/build/admission and market-calendar day or coverage surfaces
- lifecycle startup, convergence, readiness-bridge, and deployment-input surfaces
- execution boundary, ledger, control-plane, replay, startup, runtime-control, gate verdict, reconciliation, trade-submit readiness, and economic-state surfaces
- operator projections that are read as control-plane truth inputs, including platform readiness and kill-switch projection
- platform deployment and governed diagnostics surfaces used as control-plane inputs

Out of scope:
- new canonical currents
- ATA or owner routing
- ownership flips
- reducer computation
- registry canonization changes beyond this read boundary

## Invariants

1. Governed control-plane truth reads in the active read-dominance path must go through `constellation_2.common.control_plane_read_gateway_v1`.
2. The gateway must require explicit `domain` and `surface`.
3. The gateway must only resolve and read already-governed artifacts.
4. The gateway must not compute new business authority or mutate truth.
5. The gateway must not perform owner routing, OLD/NEW routing, or ATA behavior.
6. Unknown or ambiguous read requests must fail closed.
7. Direct filesystem reads of governed control-plane truth are prohibited in the active read-dominance path.
8. Approved semantic surfaces may perform narrow governed composition only when
   the composition rule is explicitly defined by current repo contracts/code.

## Approved Readers

Approved production readers in this phase are limited to:
- `constellation_2.common.control_plane_read_gateway_v1`
- `ops/tools/read_control_plane_surface_v1.py`

Other runtime, UI, and control-plane modules may only consume governed
control-plane truth through those boundaries or helper wrappers that
immediately delegate to them.

Approved semantic surfaces are additionally governed by:
- `governance/05_CONTRACTS/C2/control_plane_semantic_surface_v1.contract.md`

## Enforcement

The repository must maintain a static direct-read guard over the active read-dominance path.

The guard must fail when an active-path file:
- references governed control-plane surface literals, and
- performs direct file or JSON reads instead of the gateway

Temporary exemptions:
- tests may inspect files directly
- path resolver modules may define paths but may not read control-plane truth directly

## Fail-Closed Behavior

The gateway must fail closed when:
- the requested domain is unknown
- the requested surface is unknown
- the requested semantic surface is unknown
- a required day or account selector is missing
- a required sleeve binding or truth-sleeves selector is missing
- a required scope, context-hash, submission-id, or release-root selector is missing
- the artifact is missing or invalid
- a latest-canonical selection is ambiguous
- the underlying governed schema validation fails

## Non-Authority Rule

The gateway is a read boundary only.

It is not:
- an authority surface
- a projection layer
- a migration router
- an ownership registry

## Migration Rule

This contract exists to establish read dominance before ATA or truth-kernel migration.

No later migration phase may introduce new canonical authority currents until direct control-plane reads are routed or explicitly blocked by this boundary.
