# Aegis UI Operator Model

## Current Navigation Model

The primary Aegis operator path is:

1. Aegis Lite
2. Research Lab
3. Operator Inbox

Aegis Lite is the daily tactical surface. It is advisory-only, broker-independent, and built for manual operator-entered trades.

## Aegis Lite Primary Sections

- Today / Operator Status
- EOD Queue
- Event Monitoring
- Manual Trade Packets
- Receipts / Outcomes
- Sleeve Performance
- AI Feedback / EOD-EOW Review
- Research Lab
- Operator Inbox

The Aegis Lite landing page should answer:

- what ran today
- what needs operator action
- whether actionable trades exist
- whether receipts are missing
- whether outcomes are missing
- whether event monitoring is active
- what is blocked
- the next operator step

## Research Lab

Research Lab is offline-only. It owns hypothesis capture, task queues, evidence, result ledgers, conclusions, and governed promotion review. Research Lab artifacts cannot create trades, mutate Aegis Lite runtime, or bypass human approval.

The current UI surface is a lightweight operator entry point with CLI commands. A full Research Lab dashboard remains a known gap.

## Operator Inbox

Operator Inbox is lightweight idea capture. Inbox items cannot create Research tasks, trades, sleeves, or production state. Promotion to a Research idea or hypothesis must be explicit and lineage-preserving.

The current UI surface is a lightweight operator entry point with CLI commands. A form-based inbox UI remains a known gap.

## Legacy / Deferred

Legacy/full broker-era surfaces are isolated under `Legacy / Deferred`:

- Legacy IB automation
- Deferred broker integration
- Deprecated PAPER orchestration
- Internal/debug runtime diagnostics

These are diagnostic-only and must not appear as normal Aegis Lite operator workflows.

## Copy Rules

Use:

- manual execution
- operator-entered trades
- broker-independent runtime
- advisory-only
- event monitoring
- promoted sleeves
- Research feedback loop

Avoid in primary Aegis Lite UI:

- autonomous trading
- auto-submit
- broker required
- IB execution pipeline
- full Aegis
- Aegis Advisory
- live execution

## Runtime Truth Guardrails

The UI must visibly label:

- `REAL_RUNTIME`
- `DEMO_ONLY`
- `DRY_RUN_ONLY`
- `ADVISORY_ONLY`
- `GATE_ONLY_NO_TRANSPORT`

`DEMO_ONLY` and `DRY_RUN_ONLY` packets must not appear actionable.

## Known Remaining UI Gaps

- Research Lab remains mostly CLI/artifact-backed.
- Operator Inbox remains mostly CLI/artifact-backed.
- Receipt and outcome entry are CLI-backed; no dedicated browser form is proven.
- AI Feedback review is artifact/CLI-backed; no full read model dashboard is proven yet.
- Legacy diagnostic views remain available for internal reference, but are no longer primary workflows.
