# Aegis Construction Input Boundary Requirements

## Core Principle

A candidate must never silently disappear between:

1. capture
2. lifecycle projection
3. candidate contract generation
4. market-data binding
5. paper trade construction
6. operator readiness

Every current-session candidate must end the construction pipeline in exactly one explicit result:

- `CONSTRUCTED`
- `REJECTED_BY_CONSTRUCTION`
- `CONSTRUCTION_BLOCKED`
- `NOT_ELIGIBLE_FOR_CONSTRUCTION`

Silent omission is forbidden.

## Canonical Candidate Population

The canonical construction input population is:

```text
candidate_lifecycle_projection_v1.current_session_candidates
```

The construction boundary must evaluate every row in that population.

A narrower source, such as `candidate_review_packet_v1`, may be used only if all of the following are true:

- the narrowing rule is documented
- every excluded current-session candidate receives an explicit exclusion reason
- a boundary report is emitted
- the boundary report proves `silent_omission_count == 0`

No current-session candidate may be omitted simply because it is absent from `candidate_review_packet_v1`.

## Required Boundary Report

Create or document:

```text
truth/reports/aegis_construction_input_boundary_v1/<day>/construction_input_boundary.v1.json
```

It must include:

- `paper_session_id`
- `current_session_candidate_count`
- `candidate_review_packet_count`
- `candidate_contract_count`
- `market_data_bound_count`
- `market_data_valid_count`
- `construction_attempted_count`
- `constructed_count`
- `rejected_count`
- `blocked_count`
- `not_eligible_count`
- `silent_omission_count`

For every current-session candidate, include:

- `symbol`
- `candidate_id`
- `paper_session_id`
- `in_current_session_candidates`
- `in_candidate_review_packet`
- `candidate_contract_present`
- `market_data_bound`
- `market_data_valid`
- `construction_attempted`
- `construction_result`
- `boundary_status`
- `boundary_reason`

## Boundary Statuses

Supported statuses:

- `READY_FOR_CONSTRUCTION`
- `CONSTRUCTED`
- `REJECTED_BY_CONSTRUCTION`
- `CONSTRUCTION_BLOCKED_MARKET_DATA`
- `CONSTRUCTION_BLOCKED_MISSING_CONTRACT`
- `CONSTRUCTION_BLOCKED_MISSING_REQUIRED_FIELD`
- `NOT_ELIGIBLE_FOR_CONSTRUCTION`
- `BOUNDARY_VIOLATION_SILENT_OMISSION`

## Boundary Violation Rule

If:

```text
current_session_candidate_count > construction_attempted_count + rejected_count + blocked_count + not_eligible_count
```

then emit:

```text
BOUNDARY_VIOLATION_SILENT_OMISSION
```

and fail the construction boundary check.

No current-session candidate may be omitted simply because it is absent from `candidate_review_packet_v1`.

## Required Command

Create or document:

```bash
npm run aegis:construction-boundary-status
```

It should print:

```text
Construction input boundary for PAPER-YYYY-MM-DD-0950:
- current-session candidates: 25
- candidate review packet: 2
- candidate contracts: 2
- market-data valid: 8
- construction attempted: 2
- constructed: 2
- blocked missing contract: 6
- blocked market data: 17
- silent omissions: 0
```

If silent omissions exist, it must print:

```text
BOUNDARY VIOLATION: <N> current-session candidates were omitted without explicit construction result.
```

## Relationship to Existing Documents

This document is subordinate to:

- `AEGIS_OPERATOR_WORKFLOW.md`
- `AEGIS_POSITIONS_UI_REQUIREMENTS.md`
- `AEGIS_CANDIDATE_READINESS_REQUIREMENTS.md`
- `AEGIS_CANDIDATE_CONSTRUCTION_REQUIREMENTS.md`

It defines the boundary between candidate readiness and construction implementation.

If construction implementation conflicts with this boundary, the boundary requirement wins unless a higher-level requirements document is changed first.

## Required Tests

Tests must prove:

- all current-session candidates appear in the construction boundary report
- candidates absent from `candidate_review_packet_v1` are not silently omitted
- candidates with valid market data but missing contracts become `CONSTRUCTION_BLOCKED_MISSING_CONTRACT`
- candidates with missing or stale market data become `CONSTRUCTION_BLOCKED_MARKET_DATA`
- constructed candidates are marked `CONSTRUCTED`
- silent omission count is zero
- boundary violation is emitted if a current-session candidate has no explicit result

## Current Runtime Finding: 2026-05-28

For `PAPER-2026-05-28-0950`, runtime investigation found:

- `candidate_lifecycle_projection_v1.current_session_candidates`: 25 candidates
- `market_data_inputs_v1`: 8 valid current-session symbols
- `candidate_review_packet_v1`: 2 symbols, `QQQ` and `SPY`
- `aegis_candidate_contracts_v1`: 2 contracts, `QQQ` and `SPY`
- `paper_trade_construction_v1`: candidate source `aegis_candidate_review_packet_v1`
- `paper_trade_construction_v1`: constructed only `QQQ` and `SPY`

Six symbols had valid market data but were not attempted by construction because they were absent from both `candidate_review_packet_v1` and `aegis_candidate_contracts_v1`:

- `AAL`
- `AMD`
- `AMDL`
- `AMT`
- `ARM`
- `CIFR`

This is a construction input-boundary defect. It is not a Positions UI defect.

## Repair Plan

### Why does `candidate_review_packet_v1` contain only `QQQ` and `SPY`?

`candidate_review_packet_v1` currently appears to represent the narrow already-contracted or active-review population, not the full current-session capture population.

That is valid only if the excluded current-session candidates are reported elsewhere with explicit construction boundary results. Today they are not; they disappear before construction attempts are recorded.

### Why does `aegis_candidate_contracts_v1` contain only `QQQ` and `SPY`?

`aegis_candidate_contracts_v1` currently contains only candidates that passed candidate-contract validation. The six market-data-valid but unconstructed symbols do not have candidate contracts, so construction cannot prove required fields such as contract identity, direction, entry, stop, sizing, and evidence lineage.

The contract generator should either:

- create valid candidate contracts for all current-session candidates that satisfy contract requirements, or
- emit explicit rejected/blocked contract rows for every current-session candidate it declines to contract.

### Why are six market-data-valid candidates not contracted or attempted?

They are current-session lifecycle candidates and have valid market data, but they are missing from the two artifacts construction currently uses as its effective candidate boundary:

- `candidate_review_packet_v1`
- `aegis_candidate_contracts_v1`

Because `paper_trade_construction_v1` prefers `candidate_review_packet_v1` and only falls back to the paper review queue when the packet is empty, those six candidates are never iterated. The absence is silent from construction's perspective.

### Should candidate contracts be generated for all current-session candidates?

Candidate contracts should be evaluated for all current-session candidates.

Not every current-session candidate must receive a valid contract. However, every current-session candidate must receive one of:

- valid candidate contract
- rejected candidate contract with reason
- blocked candidate contract with missing-field/upstream reason
- not-eligible reason

The contract artifact must not contain only successful contracts without accounting for excluded current-session candidates.

### Should construction consume lifecycle candidates directly?

Construction boundary evaluation should consume `candidate_lifecycle_projection_v1.current_session_candidates` directly, or consume an artifact that is proven to be a complete one-row-per-current-session-candidate boundary report.

The construction implementation may still use `candidate_review_packet_v1` or `aegis_candidate_contracts_v1` for detailed fields, but those artifacts cannot define the full population unless they also account for all current-session candidates.

### Should `candidate_review_packet_v1` be redefined as only ready-for-review candidates?

Yes, if that matches product intent.

If `candidate_review_packet_v1` is only a ready-for-review or active-review packet, it must not be treated as the construction input population. It should be labeled and documented as a narrowed review artifact, and boundary reporting must explain why excluded lifecycle candidates are not ready for review.

### Where should construction eligibility be decided?

Construction eligibility should be decided at the construction input boundary, using evidence from:

- `candidate_lifecycle_projection_v1.current_session_candidates`
- `aegis_candidate_contracts_v1`
- `market_data_inputs_v1`
- candidate construction/readiness requirements
- construction policy/risk policy artifacts

Eligibility is not a UI decision. The UI should display the resulting lifecycle/readiness status only.

### What artifact owns the canonical not-eligible reason?

The canonical not-eligible reason should be owned by:

```text
aegis_construction_input_boundary_v1
```

Downstream construction and lifecycle projections may copy or summarize that reason, but they must not invent a different one.

## Safety Boundary

This boundary does not authorize:

- trade advice
- live broker submit/transmit
- autonomous execution
- automatic paper trade execution

It only defines evidence accounting for manual paper candidate construction and readiness.
