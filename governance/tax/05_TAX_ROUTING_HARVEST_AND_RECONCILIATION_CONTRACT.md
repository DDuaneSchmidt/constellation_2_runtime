# Tax Routing Harvest And Reconciliation Contract

Contract ID: `C2_TAX_ROUTING_HARVEST_AND_RECONCILIATION_CONTRACT_V1`

## Purpose

Define the governed contracts for account-routing decisions, harvest-candidate preview decisions,
and read-only reconciliation reports.

## Account Routing Rules

Account-routing decisions must remain tax-domain artifacts and must not mutate execution logic.

At minimum routing must consider:

- explicit account tax regime
- governed account-location policy
- sleeve or strategy id when supplied
- imported-position restrictions
- asset tax classification when supplied
- wash-risk implications when supplied
- turnover sensitivity policy

Required baseline rules:

- taxable accounts are preferred when harvesting eligibility is required
- sheltered accounts are preferred for turnover-heavy exposures when policy says so
- sheltered accounts are preferred for ordinary-income-heavy exposures when policy says so
- missing account regime blocks optimization
- missing asset tax classification degrades safely or requires review
- imported-position restrictions are never bypassed silently

## Harvest Candidate Rules

Harvest candidate decisions are preview artifacts only. They are not an auto-trading surface.

At minimum harvest preview must enforce:

- deterministic lot ordering
- minimum-loss threshold policy
- exclusion for unknown basis
- exclusion for unknown holding period
- warning or block for active wash-risk windows
- exclusion when unresolved corporate-action impact affects basis truth

## Reconciliation Boundary

Reconciliation artifacts are read-only consumers of accepted facts, snapshots, and broker-view
inputs. Reconciliation must not mutate tax truth.

Minimum governed mismatch taxonomy:

- quantity mismatch
- basis mismatch
- acquisition-date mismatch
- holding-period mismatch
- realized-proceeds mismatch
- realized-gain-loss mismatch
- corporate-action mismatch
- missing in broker
- missing in constellation

Mismatch ordering and serialization must be deterministic.
