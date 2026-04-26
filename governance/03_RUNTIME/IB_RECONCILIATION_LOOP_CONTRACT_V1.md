# IB_RECONCILIATION_LOOP_CONTRACT_V1

Status: ACTIVE
Owner: Constellation Runtime Governance
Effective Date: 2026-04-26

## Purpose

Define the v1 operating contract for deterministic IB reconciliation between broker-observed truth and Aegis expected execution truth.

## Contract

1. Interactive Brokers (IB) owns broker-side truth for executed trades, positions, and cash snapshots.
2. Aegis owns expected/intended truth for orders, intended executions, and expected state transitions.
3. Reconciliation owns comparison truth and must produce deterministic PASS/WARN/FAIL outcomes.
4. AI reviews exceptions only and may consume reconciliation outputs after deterministic classification completes.
5. AI cannot override deterministic matching decisions or mutate mismatch severity/status outcomes.
6. AI cannot place trades or issue broker-side execution actions.
7. V1 is reporting-only and does not block submit boundary decisions.
8. A future governed version may enforce fail-closed next-day trading gates on reconciliation FAIL.
9. Runtime artifacts for this loop must live only under `/home/node/constellation_runtime_data`, with loop-local writes rooted at `/home/node/constellation_runtime_data/ib_reconciliation`.
10. IB Flex reports are treated as broker snapshots (daily/intraday delayed), not as a real-time execution feed.

## Runtime Paths

- Loop runtime root: `/home/node/constellation_runtime_data/ib_reconciliation`
- Read-only truth surface copy: `/home/node/constellation_runtime_data/truth/reports/ib_reconciliation_v1/<YYYY-MM-DD>/ib_reconciliation.v1.json`

## Guardrails

- Deterministic matching precedence is mandatory: `PERM_ID_EXACT`, `ORDER_ID_EXACT`, `SUBMISSION_ID_REFERENCE`, then low-confidence fallback.
- Symbol-only, timestamp-only, quantity-only, and AI-proposed matching are forbidden.
- Reconciliation must fail closed on malformed IB input artifacts.

## Daily Operation Note

After the IB Flex report is downloaded after market close, run:

`python3 ops/tools/run_ib_reconciliation_daily_loop_v1.py --day_utc <YYYY-MM-DD> --ib_flex_xml <path>`

Future automation may trigger this command when the IB report file appears. This v1 task does not install cron/systemd timers.
