# Aegis Signal Evidence Boundary Requirements

## Core Principle

Every current-session lineage row must receive an explicit signal evidence boundary outcome.

Silent omission is forbidden. Rejected intents are lineage, not operator-reviewable Today’s Candidates.

## Canonical Population

The signal evidence boundary must evaluate:

```text
candidate_lifecycle_projection_v1.current_session_candidates
```

against:

- `signal_evidence_graph_v1.signals`
- sleeve `output_intents`
- sleeve `rejected_intents`
- `intent_arbitration_v1.portfolio_ranking`
- `raw_candidate_intents`
- `aegis_paper_review_queue_v1.rows`

`signal_evidence_graph_v1` may remain a narrow graph over final sleeve `output_intents`, but the boundary must account for every current-session lineage row that is absent from that graph. Only rows with `SIGNAL_EVIDENCE_PRESENT` may flow into Today’s Candidates as output candidates.

## Required Outcomes

Each current-session lineage row must receive one of:

- `SIGNAL_EVIDENCE_PRESENT`
- `SIGNAL_EVIDENCE_REJECTED_INTENT`
- `SIGNAL_EVIDENCE_ARBITRATION_ONLY`
- `SIGNAL_EVIDENCE_REVIEW_QUEUE_ONLY`
- `SIGNAL_EVIDENCE_BLOCKED_MISSING_OUTPUT_INTENT`
- `NOT_SIGNAL_ELIGIBLE`
- `SIGNAL_BOUNDARY_VIOLATION`

Forbidden outcomes:

- silently omitted
- absent from `output_intents` without explanation
- present in review queue but missing from signal evidence without boundary status
- present in arbitration or rejected-intent lineage but invisible to contract generation

`SIGNAL_EVIDENCE_REJECTED_INTENT`, `SIGNAL_EVIDENCE_ARBITRATION_ONLY`, and `SIGNAL_EVIDENCE_REVIEW_QUEUE_ONLY` are exclusion outcomes for the main Positions page. They must not be shown as operator-reviewable Today’s Candidates.

## Required Report

Create or document:

```text
truth/reports/aegis_signal_evidence_boundary_v1/<day>/signal_evidence_boundary.v1.json
```

It must include:

- `paper_session_id`
- `current_session_candidate_count`
- `signal_evidence_graph_signal_count`
- `output_intent_count`
- `rejected_intent_count`
- `arbitration_only_count`
- `review_queue_only_count`
- `signal_evidence_present_count`
- `blocked_missing_output_intent_count`
- `silent_omission_count`

Each row must include:

- `symbol`
- `candidate_id`
- `paper_session_id`
- `in_current_session_candidates`
- `in_signal_evidence_graph`
- `in_output_intents`
- `in_rejected_intents`
- `in_arbitration_ranking`
- `in_paper_review_queue`
- `boundary_status`
- `boundary_reason`

## Required Command

Add or document:

```bash
npm run aegis:signal-evidence-boundary-status
```

Expected output example:

```text
Signal evidence boundary for PAPER-2026-05-28-0950:
- current-session lineage rows: 25
- signal evidence graph signals: 2
- output intents: 2
- rejected-intent lineage: 23
- review/arbitration-only: 0
- signal evidence present: 2
- Today’s Candidates: 2
- excluded rejected intents: 23
- silent omissions: 0
```

## Acceptance Tests

Tests must prove:

- all current-session lineage rows appear in the boundary report
- `QQQ` and `SPY` are `SIGNAL_EVIDENCE_PRESENT`
- `AAL`, `AMD`, `AMDL`, `AMT`, `ARM`, and `CIFR` receive explicit rejected/arbitration lineage outcomes
- rejected-intent lineage outcomes are excluded from Today’s Candidates
- rejected-intent lineage rows do not expose Confirm Captured, Mark Not Captured, or Defer
- no current-session lineage row is silently omitted
- contract generation boundary can consume signal boundary reasons
- `silent_omission_count` is zero

## Current Runtime Finding: 2026-05-28

Runtime investigation found:

- `signal_evidence_graph_v1` only consumes sleeve `output_intents`
- only `QQQ` and `SPY` were present in final sleeve `output_intents`
- `AAL`, `AMD`, `AMDL`, `AMT`, `ARM`, and `CIFR` were current-session lineage rows with valid market data
- those six symbols existed in rejected-intent, arbitration, review-queue, or candidate-state lineage instead of final `output_intents`
- therefore they were absent from signal evidence, absent from candidate contracts, and absent from paper trade construction
- rejected-intent lineage must be excluded from Today’s Candidates and shown only in diagnostics/history

This is a signal evidence input-boundary defect. It is not a Positions UI defect and must not be repaired by weakening readiness gates.


## UI Relationship

The main Positions page must use the signal evidence boundary to separate output candidates from excluded lineage:

- Today’s Candidates includes only rows with `SIGNAL_EVIDENCE_PRESENT`.
- Ready for operator review is computed only from Today’s Candidates/output candidates.
- Rejected intents are counted as excluded rejected intents.
- Rejected intents must appear only on diagnostics/history surfaces.
- Diagnostics should display the 23 rejected-intent lineage rows for the current session.
- Rejected intents must not show Confirm Captured, Mark Not Captured, or Defer.

For `PAPER-2026-05-28-0950`, the expected operator-facing counts are:

- Today’s Candidates: 2
- Ready for review: 2
- Excluded rejected intents: 23

## Governance Rule

`signal_evidence_graph_v1` may remain narrow, but its exclusions must be explicit.

No candidate may be excluded from contract generation solely because it is absent from `output_intents` without a recorded signal-boundary outcome.

No rejected intent may be promoted to an operator-reviewable Today’s Candidate merely because it exists in lifecycle, review queue, arbitration, or rejected-intent lineage.

Signal generation behavior, operator UI behavior, and readiness gates must not change until this boundary is implemented and tested.

## Relationship to Other Documents

This document is subordinate to:

- `AEGIS_OPERATOR_WORKFLOW.md`
- `AEGIS_POSITIONS_UI_REQUIREMENTS.md`
- `AEGIS_CANDIDATE_READINESS_REQUIREMENTS.md`
- `AEGIS_CANDIDATE_CONSTRUCTION_REQUIREMENTS.md`
- `AEGIS_CONSTRUCTION_INPUT_BOUNDARY_REQUIREMENTS.md`
- `AEGIS_CONTRACT_GENERATION_REQUIREMENTS.md`

It defines the boundary between current-session candidate population and signal evidence graph coverage.
