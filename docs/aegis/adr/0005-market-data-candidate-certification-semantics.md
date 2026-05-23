# ADR 0005: Market Data And Candidate Certification Semantics

## Status
Accepted

## Context
Aegis previously blended candidate visibility, market-data validation, candidate certification, and execution eligibility into shared operator status fields. This made states such as validated current-day market data, certification-pending candidates, non-certified candidate rows, and zero execution-eligible rows appear contradictory.

The failure mode was semantic, not a trading permission change: operators needed to see provisional candidates and understand market-data freshness, while execution-facing workflows still had to reject anything that was not certified.

## Problem
The following concerns were represented through overlapping fields:
- Candidate visibility: whether operators may inspect current provisional candidate rows.
- Market-data validation: whether current market data is validated, pending, partial, or invalid.
- Candidate certification: whether candidate snapshots are provisional, certification-pending, certified, or failed.
- Execution eligibility: whether execution-facing workflows may consume candidate snapshots.

This blending allowed UI and projection code to overinterpret compatibility fields such as `current_truth_status`, `runtime_mode`, and `current_day_run_status`.

## Decision
Aegis operator projections split primary operator semantics into three explicit fields:
- `market_data_state`
- `candidate_certification_state`
- `execution_eligibility_state`

`market_data_state` describes market-data validation only. Supported operator values are:
- `MARKET_DATA_VALIDATED`
- `MARKET_DATA_PENDING`
- `MARKET_DATA_PARTIAL`
- `MARKET_DATA_INVALID`

`candidate_certification_state` describes candidate snapshot certification only. Supported operator values are:
- `CANDIDATES_PROVISIONAL`
- `CANDIDATES_CERTIFICATION_PENDING`
- `CANDIDATES_CERTIFIED`
- `CANDIDATES_CERTIFICATION_FAILED`

`execution_eligibility_state` describes execution-facing admissibility only. Supported operator values are:
- `EXECUTION_LOCKED_NON_CERTIFIED`
- `EXECUTION_ELIGIBLE_CERTIFIED_ONLY`

Dashboard, operator projections, and UI copy must use these fields as the primary operator semantics.

## Invariants
- Provisional candidates may be visible to operators.
- Provisional candidates are read-only and must be labeled non-certified.
- Only certified candidates may become execution eligible.
- Candidate generation and display do not imply execution eligibility.
- Certified promotion creates separate certified candidate snapshots; it must not mutate provisional snapshots.
- Execution-facing workflows must reject provisional, partial, stale, invalid, or otherwise non-certified candidate snapshots.

## Legacy Compatibility Fields
The following fields remain compatibility fields only:
- `current_truth_status`
- `runtime_mode`
- `current_day_run_status`

They may remain in APIs and artifacts for backwards compatibility, diagnostics, or migration support, but they must not drive primary operator semantics in UI, operator copy, candidate gating, certification, or execution eligibility.

## Regression Risks
The main regressions this ADR is intended to prevent are:
- Treating vendor lag as a system failure.
- Hiding provisional candidates while current-day certification is pending.
- Allowing non-certified rows into scoring, ranking finalization, execution, submit, transmit, or order paths.
- Using blended status fields in UI as if they were the authoritative market-data, candidate-certification, or execution-eligibility state.
- Overwriting certified snapshots with provisional data.

## Required Tests
The architecture requires regression coverage for:
- Market data validated plus candidates pending does not display as fully certified.
- Vendor lag plus provisional visibility keeps the workspace usable and does not render as a hard failure.
- Execution remains locked for `NON_CERTIFIED` candidates.
- Certified promotion creates a separate candidate snapshot instead of mutating a provisional snapshot.
- The execution firewall rejects non-certified candidate snapshots.

## Consequences
Operators can inspect current provisional candidate information before final certification without weakening execution safeguards. UI and API consumers get a clearer state model: market data can be validated while candidates remain certification-pending and execution remains locked.

The system remains read-only for non-certified candidates. This ADR does not authorize broker submit/transmit, live trading, autonomous execution, trade advice, automatic promotion, or any weakening of execution firewalls.

## Safety Constraints
No broker submit/transmit, autonomous execution, live trading, automatic capital allocation, trade advice, or order routing is enabled by this architecture. Execution eligibility remains certified-only.
