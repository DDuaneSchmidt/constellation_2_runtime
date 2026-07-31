# Aegis Contract Generation Requirements

## Core Principle

Every current-session candidate that reaches the contract generation boundary must receive an explicit contract outcome.

Silent omission is forbidden.

## Canonical Contract Input Population

The contract generator must evaluate either:

1. all `candidate_lifecycle_projection_v1.current_session_candidates`, or
2. a narrower documented population with explicit exclusion reasons for every omitted current-session candidate.

If `signal_evidence_graph_v1` remains the input source, then every current-session candidate absent from that graph must receive a contract-boundary result explaining why.

## Contract Outcomes

Every current-session candidate must receive one of:

- `VALID_CONTRACT`
- `CONTRACT_BLOCKED`
- `CONTRACT_REJECTED`
- `NOT_ELIGIBLE_FOR_CONTRACT`
- `CONTRACT_INPUT_BOUNDARY_BLOCKED`

Forbidden outcomes:

- not attempted
- silently omitted
- absent from signal graph without explanation
- excluded by packet narrowing without reason

## Required Contract Fields

A valid candidate contract must include:

- `candidate_id`
- `candidate_contract_id`
- `paper_session_id`
- `symbol`
- `direction`
- `source_candidate_path`
- `source_signal_path` if applicable
- `eligibility_status`
- `contract_status`
- `created_at`
- `contract_version`

Recommended fields:

- sleeve
- strategy
- entry source
- stop source
- quantity source
- market data binding reference
- risk model reference

## Required Contract Boundary Report

Create or document:

```text
truth/reports/aegis_contract_generation_boundary_v1/<day>/contract_generation_boundary.v1.json
```

It must include:

- `paper_session_id`
- `current_session_candidate_count`
- `market_data_valid_count`
- `signal_evidence_graph_signal_count`
- `contract_generation_attempted_count`
- `valid_contract_count`
- `blocked_count`
- `rejected_count`
- `not_eligible_count`
- `silent_omission_count`

For every current-session candidate, include:

- `symbol`
- `candidate_id`
- `paper_session_id`
- `in_current_session_candidates`
- `market_data_valid`
- `in_signal_evidence_graph`
- `contract_generation_attempted`
- `contract_present`
- `contract_outcome`
- `boundary_status`
- `boundary_reason`

## Boundary Statuses

Supported statuses:

- `READY_FOR_CONTRACT_GENERATION`
- `VALID_CONTRACT`
- `CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE`
- `CONTRACT_BLOCKED_MISSING_MARKET_DATA`
- `CONTRACT_REJECTED_BY_POLICY`
- `NOT_ELIGIBLE_FOR_CONTRACT`
- `CONTRACT_INPUT_BOUNDARY_VIOLATION`

## Boundary Violation Rule

If a current-session candidate has valid market data but is not attempted by contract generation, the contract boundary must emit:

```text
CONTRACT_INPUT_BOUNDARY_VIOLATION
```

or a more specific status such as:

```text
CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE
```

No valid current-session candidate may be absent from contract generation without an explicit result.

## Required Command

Create or document:

```bash
npm run aegis:contract-generation-status
```

It should print:

```text
Contract generation for PAPER-YYYY-MM-DD-0950:
- current-session candidates: 25
- market-data valid: 8
- signal evidence graph signals: 2
- contract generation attempted: 2
- valid contracts: 2
- blocked missing signal evidence: 6
- blocked missing market data: 17
- silent omissions: 0
```

## Relationship to Other Documents

This document is subordinate to:

- `AEGIS_OPERATOR_WORKFLOW.md`
- `AEGIS_POSITIONS_UI_REQUIREMENTS.md`
- `AEGIS_CANDIDATE_READINESS_REQUIREMENTS.md`
- `AEGIS_CANDIDATE_CONSTRUCTION_REQUIREMENTS.md`
- `AEGIS_CONSTRUCTION_INPUT_BOUNDARY_REQUIREMENTS.md`

It defines the boundary between current-session candidate population and candidate contract creation.

If contract generation implementation conflicts with this document, this document wins unless a higher-level requirements document is changed first.

## Acceptance Tests

Tests must prove:

- every current-session candidate appears in the contract generation boundary report
- market-data-valid candidates missing from `signal_evidence_graph_v1` receive `CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE`
- `QQQ` and `SPY` receive `VALID_CONTRACT`
- no current-session candidate is silently omitted
- `silent_omission_count` is zero
- contract generation status command prints market-data-valid but uncontracted symbols
- construction boundary can consume contract boundary outcomes

## Current Runtime Finding: 2026-05-28

Runtime investigation found:

- `candidate_lifecycle_projection_v1.current_session_candidates`: 25 candidates
- `market_data_inputs_v1`: 8 valid current-session symbols
- `signal_evidence_graph_v1.signals`: only `QQQ` and `SPY`
- `aegis_candidate_contracts_v1`: only `QQQ` and `SPY`
- `candidate_review_packet_v1`: only `QQQ` and `SPY`
- `paper_trade_construction_v1`: only `QQQ` and `SPY`

Six symbols were current-session candidates with valid market data but were never attempted by contract generation:

- `AAL`
- `AMD`
- `AMDL`
- `AMT`
- `ARM`
- `CIFR`

Root cause:

`aegis_candidate_contracts_v1` consumes `signal_evidence_graph_v1.signals`, which is narrower than the current-session candidate population. Omitted symbols receive no contract-level outcome.

This is a contract generation input-boundary defect. It is not a Positions UI defect.

## Investigation Requirement

After this document is created, investigate:

- Why does `signal_evidence_graph_v1` contain only `QQQ` and `SPY`?
- Is `signal_evidence_graph_v1` supposed to represent all current-session candidates or only a subset?
- What producer creates the six current-session candidates that are missing from signal evidence?
- Should candidate contracts be generated from lifecycle candidates directly?
- Or should signal evidence graph be expanded to include all current-session candidates?
- Where should contract eligibility rules live?

Do not change contract-generation behavior until this requirements document is created and referenced by the relevant manifest.

## Governance Rule

No future Aegis contract generation pipeline may omit a current-session candidate without a contract boundary result.

Contract generation may reject, block, or mark a candidate not eligible, but it must not leave the candidate invisible to downstream construction and readiness reporting.

Candidate lifecycle, signal evidence, contract generation, construction, and readiness must remain separate concepts with explicit boundary reports between them.
