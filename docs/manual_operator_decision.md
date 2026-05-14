# manual_operator_decision.v1

`manual_operator_decision.v1` captures what the operator decided to do with an Aegis Lite candidate.

Decisions are:

- `ENTERED`
- `SKIPPED`
- `MODIFIED`
- `WATCHLIST`
- `REJECTED`

The artifact includes run and candidate identity, sleeve ownership, optional edge cluster, reason codes, notes, decision time, any deviation from recommendation, and whether manual review was required.

It is observational only: `broker_submit_required=false` and `transmit_automation_required=false`.
