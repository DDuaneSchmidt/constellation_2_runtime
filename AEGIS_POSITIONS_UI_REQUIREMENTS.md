# Aegis Positions UI Requirements

## Purpose

This document defines the stable product requirements for the Aegis Positions page and manual paper workflow. Future changes must preserve these requirements so the UI does not reintroduce confusing candidate/position behavior, hidden actions, debug clutter, or ambiguous "Paper Trade" semantics.

This document implements the business/operator workflow authority defined in `AEGIS_OPERATOR_WORKFLOW.md`. If these UI requirements conflict with that workflow, the workflow document wins.

## 1. Product Principle

Aegis does not place trades.

Aegis records operator decisions, manual paper entries, manual paper exits, receipts, and resulting position state.

The Positions workflow is manual paper tracking only. It must never imply that Aegis submitted, routed, transmitted, or executed a trade.

## 2. Main Positions Page Sections

The main Positions page must show only these sections:

1. Action Required
2. Open Positions
3. Today's Candidates
4. Closed Positions

Diagnostics must not appear on the main page.

## Candidate Capture Confirmation (Highest Priority Requirement)

Purpose:

Before an operator can approve, reject, defer, record entry, record exit, or evaluate candidate state, they must be able to verify that the current session candidate capture completed successfully.

Operator question:

"Did Aegis successfully capture today's candidates?"

Required UI behavior:

The top of Today's Candidates must contain a Candidate Capture Confirmation panel.

Fields:

- Paper Session ID
- Candidate Capture Status
- Expected Candidate Count
- Output Candidates Captured
- Rejected Intents Excluded
- Current Session Lineage Count
- Actionable Count
- Ready for Operator Review
- Incomplete Count
- Approved Count
- Open Count
- Closed Count
- Failed Count
- Capture Timestamp

Status values:

- CAPTURED
- PARTIAL
- FAILED
- STALE

Required messaging examples:

```text
2 output candidates captured for PAPER-2026-05-28-0950. 23 rejected intents excluded.
```

```text
Candidate capture incomplete: expected 2 output candidates, captured 1. 23 rejected intents excluded.
```

Operator workflow order:

1. Confirm candidate capture and rejected-intent exclusions.
2. Review output candidates only.
3. Confirm Captured / Mark Not Captured / Defer.
4. Record Entry / Verify Open Position.
5. Monitor Open Position.
6. Record Exit.
7. Review Closed Position.

This workflow order is mandatory.

Candidate capture confirmation must appear before any candidate action controls.

Acceptance criteria:

The operator must be able to answer within five seconds:

- Were today's output candidates captured?
- How many output candidates were captured?
- How many rejected intents were excluded?
- Which session produced them?

No diagnostics page should be required to answer those questions.



## Rejected Intent Exclusion Policy

Rejected intents must not appear in Today's Candidates as operator-reviewable rows. They must appear only in diagnostics or history.

Rejected-intent rows must never show:

- Confirm Captured
- Mark Not Captured
- Defer

The expected main Positions page counts for `PAPER-2026-05-28-0950` are:

- Today's Candidates: 2
- Ready for review: 2
- Excluded rejected intents: 23

The diagnostics page should display the 23 rejected-intent lineage rows with their signal evidence boundary status and reason.

## Candidate Readiness Requirement

A row may not be operator-actionable unless it is a final output candidate that passed the signal evidence boundary and all required capture fields are present and valid:

- `planned_entry`
- `planned_stop`
- `quantity`

If any required field is missing or invalid, Aegis must place the candidate into `INCOMPLETE_CANDIDATE` before exposing operator capture actions.

For `INCOMPLETE_CANDIDATE` rows, the main Positions page must disable:

- Confirm Captured
- Mark Not Captured
- Defer

The row must clearly show which fields are missing. The operator must not need the diagnostics page to understand why the row is not actionable.

The Candidate Capture Confirmation panel must show, using signal evidence boundary and lifecycle data:

- 2 output candidates captured when 2 final output candidates passed signal evidence boundary for the paper session.
- 23 rejected intents excluded when 23 current-session lineage rows are rejected-intent or non-output lineage.
- 2 ready for operator review when exactly 2 output candidates have valid `planned_entry`, `planned_stop`, and `quantity`.

Rejected intents are not incomplete Today’s Candidate rows. They are excluded from the main Today’s Candidates list and may appear only in diagnostics/history.

`INCOMPLETE_CANDIDATE` rows must expose Details only as their enabled action set. Confirm Captured, Mark Not Captured, and Defer must be disabled or hidden for incomplete candidates.


## Candidate Capture Actions (Highest Priority Requirement)

Every row in Today's Candidates must always expose at least one operator-facing capture confirmation path. Internal lifecycle state must never hide every action. Diagnostics must not be required for the operator to confirm what happened to a candidate.

The supported operator paths are:

- Confirm Captured
- Mark Not Captured
- Defer
- View / Correct Capture

Meaning:

- Confirm Captured means the operator manually created or captured the paper trade outside Aegis and is recording that capture in Aegis.
- Mark Not Captured means the operator explicitly did not take the candidate.
- Defer means the operator postpones the decision.
- View / Correct Capture means the operator can inspect or correct the recorded capture.

Today's Candidates actions by state:

GENERATED:
- Confirm Captured
- Mark Not Captured
- Defer

APPROVED_FOR_PAPER:
- Confirm Captured
- Mark Not Captured
- Defer

FAILED:
- Confirm Captured
- Mark Not Captured
- View Error

POSITION_OPEN:
- View / Correct Capture
- View Position
- View Entry Receipt

POSITION_CLOSED:
- View / Correct Capture
- View Position History
- View Entry Receipt
- View Exit Receipt

REJECTED:
- View / Correct Decision
- Reopen

DEFERRED:
- Confirm Captured
- Mark Not Captured

Command names:

- CONFIRM_CANDIDATE_CAPTURED
- MARK_CANDIDATE_NOT_CAPTURED
- DEFER_CANDIDATE
- CORRECT_CANDIDATE_CAPTURE

Do not require the operator to use Approve before Record Entry when the intent is capture confirmation. The main Positions page may support a one-click Confirm Captured path that records the durable command and receipt.

Candidate capture status must be plain English:

- Not reviewed
- Captured
- Not captured
- Deferred
- Capture failed
- Closed

No row may show `command_id`, `endpoint_status`, `click_received_at`, `last_error`, raw payloads, or raw command/debug fields on the main Positions page.

## 3. Candidate Workflow

The candidate lifecycle is:

```text
GENERATED
-> APPROVED_FOR_PAPER / REJECTED / DEFERRED
-> ENTRY_RECORDED
-> POSITION_OPEN
-> EXIT_RECORDED
-> POSITION_CLOSED
```

Candidate-facing capture actions by state are defined in Candidate Capture Actions above. The main Today's Candidates table must use those operator-facing labels, not internal approval/entry labels.

Record Exit remains a position action only. It must never appear in Today's Candidates.

## 4. Existing Position Workflow

Open Positions actions for POSITION_OPEN:
- Record Exit
- View Position
- View Entry Receipt
- Details

Closed Positions actions for POSITION_CLOSED:
- View Position History
- View Entry Receipt
- View Exit Receipt
- Details

Record Exit belongs only in Open Positions. It must never appear in Today's Candidates. Candidate rows use View / Correct Capture, View Position, and receipt/history actions after capture.

## 5. Today's Candidates Rules

Today's Candidates must:
- Show only final output candidates that passed `aegis_signal_evidence_boundary_v1` as `SIGNAL_EVIDENCE_PRESENT`.
- Exclude rejected intents and non-output lineage from operator-reviewable rows.
- Keep output candidates visible throughout the day.
- Change output candidate row state instead of moving rows out of the list.
- Show plain-English capture status and view/correct actions when an output candidate also has an open or closed position.
- Never show Record Exit in candidate rows.
- Never hide all operator actions; every row must show at least one visible capture, decision, view, or correction action.

## 6. Open Positions Rules

Open Positions must:
- Show only actual open governed paper positions.
- Be the only main Positions page section where Record Exit appears.
- Exclude candidates that have not recorded entry.

## 7. Closed Positions Rules

Closed Positions must:
- Show only actual closed governed paper positions.
- Never show Record Exit.
- Show entry receipt, exit receipt, and position history actions.

## 8. Action Required Section

Action Required must show only output-candidate items requiring operator action:

- GENERATED output candidates needing Confirm Captured / Mark Not Captured / Defer.
- APPROVED_FOR_PAPER output candidates needing Confirm Captured / Mark Not Captured / Defer.
- FAILED rows needing View Error, or Retry only when retry is valid.
- Open positions needing Record Exit only when an exit action is valid.

## 9. Diagnostics Separation

The main Positions page must not show:

- command_id
- endpoint_status
- click_received_at
- raw last_error fields
- source paths
- payloads
- command inbox internals
- lifecycle projection internals

All diagnostics belong on:

```text
/aegis-positions-diagnostics
```

## 10. Required UI Acceptance Tests

Tests must prove:

- Today's Candidates shows the full output-candidate count, not rejected-intent lineage.
- Generated output-candidate rows show Confirm Captured / Mark Not Captured / Defer.
- Approved output-candidate rows show Confirm Captured / Mark Not Captured / Defer.
- Rejected-intent lineage rows do not appear in Today's Candidates.
- Diagnostics/history displays the 23 rejected-intent lineage rows.
- Open candidate rows do not show Record Exit.
- Open Positions rows show Record Exit.
- Closed Positions rows do not show Record Exit.
- Closed candidate rows show View Position History / View Entry Receipt / View Exit Receipt.
- Main page contains no command_id, endpoint_status, click_received_at, last_error, raw payload, or raw command/debug text.
- Diagnostics page contains command/debug details.
- Every Today's Candidates row has at least one visible operator action.
- Failed output-candidate rows still allow Confirm Captured or Mark Not Captured when readiness permits.
- Open and closed candidate rows show View / Correct Capture.
- No row has contradictory state/action.
- No visible button or label says "Paper Trade."

## 11. Manual Browser Verification Workflow

Before any future Positions UI change is accepted, manual browser proof must show:

1. Confirm candidate capture for one visible output candidate.
2. Confirm the durable command and receipt are written.
3. Confirm it appears in Open Positions.
4. Confirm Today's Candidates still shows the row as captured or POSITION_OPEN with view/correct actions.
5. Record Exit from Open Positions.
6. Confirm it appears in Closed Positions.
7. Confirm Today's Candidates shows Closed with history/receipt actions only.
8. Confirm no candidate row shows Record Exit.
9. Confirm every Today's Candidates row has at least one visible operator action.
10. Confirm rejected intents are absent from Today's Candidates and visible only in diagnostics/history.

## Requirements Change Governance

No Positions UI implementation change may be made unless:

1. The change is reviewed against AEGIS_POSITIONS_UI_REQUIREMENTS.md.
2. Any newly discovered operator workflow requirement is added to the document first.
3. Acceptance tests are updated before implementation.
4. Regression tests are updated before implementation.

The requirements document is the product authority.

Implementation is subordinate to requirements.

Tests are subordinate to requirements.

If implementation behavior conflicts with requirements, requirements win.

## Manual Acceptance Workflow

Every release affecting Positions must be validated in this order:

1. Confirm candidate capture and rejected-intent exclusions.
2. Confirm Captured for an output candidate.
3. Verify durable receipt.
4. Verify Open Position.
5. Record Exit.
6. Verify Closed Position.

Any release that cannot complete this workflow is not accepted.

## 12. Safety Invariants

The Positions UI and manual paper workflow must preserve these invariants:

- No trade advice is enabled.
- No live broker submit/transmit is enabled.
- No autonomous execution is enabled.
- This workflow is manual paper tracking only.
