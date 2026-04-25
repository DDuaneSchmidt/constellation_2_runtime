# Tax Corporate Action Provisional Finalized Contract

Contract ID: `C2_TAX_CORPORATE_ACTION_PROVISIONAL_FINALIZED_CONTRACT_V1`

## Purpose

Define the bounded corporate-action workflow for tax truth without building a full generic
corporate-action engine.

## Governed Workflow

The governed state flow is:

- observed corporate action
- normalized corporate-action candidate
- provisional accepted fact
- finalized accepted fact
- snapshot impact applied

Truth conversion remains additive. Finalization must not mutate prior facts in place.

## Supported Action Classes

Supported in this bounded pass:

- `stock_split`
- `reverse_split`
- `return_of_capital`

Deferred with explicit unsupported reason handling:

- `spinoff`
- `merger`
- `symbol_change`

## Provisional And Finalized Truth

Provisional corporate actions:

- may restrict optimization
- must propagate unresolved corporate-action reason codes into lot state and downstream decisions
- must not silently alter basis or holding period

Finalized corporate actions:

- may update accepted tax facts only through additive accepted-fact flow
- may clear unresolved restriction only when required finalization payload is explicit
- must preserve corporate-action provenance on affected lots

## Restriction Propagation

If corporate-action consequences remain unresolved for basis or holding period, sell and harvest
decisions must block or degrade safely with machine-readable reason codes.
