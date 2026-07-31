# Aegis Operator Workflow

## Document Hierarchy

1. `AEGIS_OPERATOR_WORKFLOW.md` = business/operator workflow authority
2. `AEGIS_POSITIONS_UI_REQUIREMENTS.md` = UI/product requirements implementing the workflow
3. Code = implementation
4. Tests = verification

## Core Principle

Aegis does not place trades.

Aegis helps the operator:

* confirm the daily candidate capture completed
* distinguish final output candidates from rejected-intent lineage
* confirm captured, mark not captured, or defer candidates
* record manual paper entries and capture receipts
* monitor paper positions
* record manual paper exits
* review closed positions and performance

## Daily Workflow

1. Open Positions page

2. Confirm Candidate Capture

   * verify paper session ID
   * verify capture status
   * verify output candidate count
   * verify rejected-intent excluded count
   * verify candidates are final output candidates for today

3. Review Today's Candidates

   * review only final output candidates that passed the signal evidence boundary
   * review symbol, direction, planned entry, planned stop, quantity
   * confirm each row is understandable before taking action

4. Confirm Candidate Capture

   * Confirm Captured
   * Mark Not Captured
   * Defer

5. Record Entry / Capture Receipt

   * manually create the paper trade outside Aegis
   * record the entry receipt in Aegis through Confirm Captured or capture correction
   * confirm the candidate becomes an open paper position

6. Monitor Open Positions

   * review entry, current value, P&L, stop, and days held

7. Record Exit

   * manually close the paper position outside Aegis
   * record the exit receipt in Aegis
   * confirm the position moves to Closed Positions

8. Review Closed Positions

9. Review Performance

## Mandatory Acceptance Flow

Before any Positions UI change is accepted, this must work in the live browser:

Confirm Daily Candidate Capture
-> Confirm Candidate Captured
-> Verify Durable Receipt
-> Verify Open Position
-> Record Exit
-> Verify Closed Position
-> Review Performance

## Non-Goals

Aegis must not:

* provide trade advice
* submit live broker trades
* transmit orders
* perform autonomous execution
* show rejected intents as operator-reviewable Today’s Candidates
* show backend/debug implementation details on the main Positions page

Rejected intents must appear only in diagnostics or history, not on the main Positions page. Diagnostics should display the 23 rejected-intent lineage rows for the current session. They must not show Confirm Captured, Mark Not Captured, or Defer on the main Positions page.

## Relationship to Requirements

`AEGIS_POSITIONS_UI_REQUIREMENTS.md` must implement this workflow.

If the UI requirements conflict with this workflow, update the workflow first or treat the UI requirement as invalid.

## Governance Rule

No new Positions UI feature may be implemented unless it supports this workflow and passes the mandatory acceptance flow.
