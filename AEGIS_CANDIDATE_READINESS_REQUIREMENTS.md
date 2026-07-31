# Aegis Candidate Readiness Requirements

## Core Principle

A captured candidate is not automatically reviewable.

A candidate is operator-reviewable only when it is a final output candidate that passed the signal evidence boundary and has all required paper-trade construction fields.

Candidate generation, candidate capture, candidate construction, and candidate readiness are separate concepts. Aegis must not treat raw or partially constructed signals as actionable paper-trade candidates.

## Current Runtime Context

Current runtime showed:

- 25 current-session lineage rows
- 2 final output candidates captured
- 2 ready for operator review
- 23 rejected intents excluded from Today’s Candidates

This means Aegis must distinguish final output candidates from rejected-intent lineage before applying readiness gates.

## Required Fields for Operator Review

A candidate must have valid:

- `symbol`
- `direction`
- `planned_entry`
- `planned_stop`
- `quantity`
- `paper_session_id`
- `candidate_id`
- `candidate_contract_id`

Recommended but optional:

- `risk_amount`
- `risk_percent`
- `reward_risk_ratio`
- `sleeve` / `strategy`
- `market_data_timestamp`

## Candidate Readiness States

### CAPTURED_RAW

Candidate exists but has not passed construction validation.

### READY_FOR_OPERATOR_REVIEW

Candidate has all required fields.

### INCOMPLETE_CANDIDATE

Candidate is missing required fields.

### REJECTED_BY_CONSTRUCTION

Candidate failed construction rules.

### STALE_CANDIDATE

Candidate was built from stale market/session data.

## Readiness Rule

A candidate may not expose operator disposition actions unless both conditions are true:

- signal evidence boundary status is `SIGNAL_EVIDENCE_PRESENT`
- readiness state is `READY_FOR_OPERATOR_REVIEW`

Allowed actions for `READY_FOR_OPERATOR_REVIEW`:

- Confirm Captured
- Mark Not Captured
- Defer
- Details

Allowed actions for `INCOMPLETE_CANDIDATE` output candidates:

- Details only

`INCOMPLETE_CANDIDATE` rows must not expose enabled Confirm Captured, Mark Not Captured, or Defer controls.

Rejected intents are not `INCOMPLETE_CANDIDATE` rows on the main Positions page. Rejected intents are excluded from Today’s Candidates and must appear only in diagnostics/history. Diagnostics should display the 23 rejected-intent lineage rows for the current session.

## Required UI Behavior

The Positions page must show:

- Output candidates captured count
- Rejected intents excluded count
- Ready for operator review count
- Incomplete output candidate count
- Rejected by construction count
- Stale count

Example:

```text
2 output candidates captured for PAPER-2026-05-28-0950. 2 ready for review, 23 rejected intents excluded.
```

Each incomplete candidate must show:

- Missing fields
- Plain-English reason
- Source stage that failed
- Repair action if available

## Required Diagnostics

For incomplete candidates, diagnostics must answer:

- Which field is missing?
- Which upstream artifact should have produced it?
- Which sleeve/strategy generated the candidate?
- Was market data missing?
- Was stop construction skipped?
- Was quantity sizing skipped?
- Was the candidate created from raw signal output instead of constructed trade output?

## Required Investigation Command

Aegis must provide or document this command:

```bash
npm run aegis:candidate-readiness
```

It should print:

- Total candidates
- Ready count
- Incomplete count
- Incomplete by missing field
- Incomplete by sleeve/strategy
- Incomplete by source artifact
- Top repair action

Example output:

```text
Candidate readiness for PAPER-2026-05-28-0950:

- current_session_lineage_rows: 25
- output_candidates_captured: 2
- ready_for_operator_review: 2
- excluded_rejected_intents: 23
- incomplete_output_candidates: 0
- affected sleeves: ...
- repair: inspect diagnostics/history for rejected-intent lineage; do not make rejected intents operator-actionable
```

Until the command exists, the Candidate Readiness report may be generated from `aegis_candidate_lifecycle_projection_v1`, `paper_trade_construction_v1`, `aegis_candidate_review_packet_v1`, and `aegis_paper_review_queue_v1`.

## Acceptance Tests

Tests must prove:

- Final output candidate with entry, stop, quantity is `READY_FOR_OPERATOR_REVIEW`.
- Final output candidate missing stop is `INCOMPLETE_CANDIDATE`.
- Candidate missing quantity is `INCOMPLETE_CANDIDATE`.
- Incomplete candidate exposes Details only.
- Incomplete candidate cannot Confirm Captured.
- Readiness panel shows captured / ready / incomplete counts.
- Readiness diagnostics list missing fields.
- Output candidates remain visible even when incomplete.
- Rejected intents are excluded from Today’s Candidates and do not expose Confirm Captured, Mark Not Captured, or Defer.

## Governance Rule

No future pipeline may label raw signal output, rejected intents, or arbitration-only lineage as operator-reviewable candidates.

Candidate generation and candidate readiness are separate concepts.

Aegis must distinguish:

- Candidate captured
- Candidate constructed
- Candidate ready for operator review

If implementation behavior conflicts with this document, this document wins and the implementation must be corrected.

## Next Investigation Requirement

Before changing candidate construction behavior, Aegis must investigate candidate lineage and answer:

- Which rows are final output candidates?
- Which rows are rejected intents or arbitration-only lineage?
- Are any final output candidates missing stop or quantity?
- Are missing fields caused by market data gaps or construction defects?
- Which source artifact is responsible?
- What code path should repair output-candidate construction without making rejected intents operator-actionable?

No candidate construction behavior may be changed until this requirements document is created and referenced by the relevant module manifest.
