# Economic State Kernel Boundary Contract

Contract ID: `ADVISORY_ECONOMIC_STATE_KERNEL_BOUNDARY_CONTRACT_V1`

## Purpose

Define the smallest governed economic-state kernel that may produce advisory current-state authority.

## Canonical Kernel Shape

The economic-state kernel is limited to:

- explicit snapshot input assembly
- pure snapshot validation decision
- immutable `HouseholdSnapshot`
- immutable snapshot run envelope

## Sole Downstream Current-State Artifact

`HouseholdSnapshot` is the only current-state artifact downstream advisory may consume.

Downstream advisory must not reopen:

- raw positions snapshots
- raw cash-ledger snapshots
- market-data snapshots
- reconciliation reports
- dashboard/read-model projections

## Required Inputs

- explicit account scope
- explicit verified positions inputs for in-scope accounts
- explicit verified cash inputs for in-scope accounts
- optional external holdings inputs only when explicitly modeled as unverified components

## Marks / Prices

Marks and prices are out of scope for this kernel version.

They may enter the snapshot kernel only after:

- a governed advisory consumer proves they are required
- a governed snapshot contract defines exact mark scope and freshness semantics

No current writer may silently read or infer live marks for advisory use.

If such an extension is introduced, it must remain:

- a governed extension of `HouseholdSnapshot`
- fail-closed on incomplete valuation basis
- lineage-bearing and deterministic
- narrower than a generalized pricing platform

## Validation Boundary

Snapshot validation may evaluate only:

- required input presence
- explicit account-scope coverage
- verified-core completeness
- verified-core freshness
- explicit reconciliation status when such evidence is actually provided

Validation must fail closed on missing required verified-core inputs.

## Frozen Artifact Rule

After `HouseholdSnapshot` is created, downstream advisory must rely only on that frozen artifact.

No post-snapshot live reread of current-state truth is permitted in the advisory kernel path.

## Run Evidence Rule

Every snapshot-kernel run must emit a run envelope, including blocked runs.

If mandatory envelope persistence fails, the run must fail closed.

## Forbidden Expansion

This kernel must not become:

- a reporting platform
- a market analytics platform
- a generalized economic orchestration layer
- a pricing infrastructure redesign
- a second advisory authority stack
