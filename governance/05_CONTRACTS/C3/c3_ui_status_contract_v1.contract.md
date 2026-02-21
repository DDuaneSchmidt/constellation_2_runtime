id: C3_UI_STATUS_CONTRACT_V1
title: "C3 UI Status Contract v1"
status: DRAFT
version: 1
created_utc: 2026-02-21
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C3 UI Status Contract v1

## Purpose
Bind the Constellation Viewer (PhaseL Ops Dashboard) to **C3 authoritative runtime truth** only.

The UI must not derive state from:
The UI must not derive state from:
- any non-authoritative truth root outside `constellation_2/runtime/truth`
- any legacy gates/status registry living under `ops/`
- any hardcoded component roster (component names must be derived from runtime truth only)
- any compatibility layer or silent fallback

## Canonical truth root (authoritative)
The UI must read only from:

- `constellation_2/runtime/truth/`

## Required sources (only)
The UI status model MUST derive exclusively from:

1) `constellation_2/runtime/truth/latest.json`
   - Determines the authoritative `DAY_UTC` consumed by the status contract

2) `constellation_2/runtime/truth/reports/gate_stack_verdict_v1/<DAY>/gate_stack_verdict.v1.json`
   - Canonical final verdict
   - Authoritative verdict state: `PASS|FAIL` (anything else is degraded)

3) `constellation_2/runtime/truth/reports/broker_reconciliation_*`
   - Broker reconciliation state for the same `DAY_UTC`
   - If missing/unreadable → degrade (fail-closed)

4) `constellation_2/runtime/truth/market_data_snapshot_v1`
   - Market data presence
   - Presence is determined only by directory-day discovery under:
     - `market_data_snapshot_v1/broker_marks_v1/<DAY>/...`

5) `constellation_2/runtime/truth/monitoring_*`
   - Component presence signals (if present)
   - Derived dynamically from monitoring directories, never from hardcoded lists

No other source is allowed.

## Status JSON schema (C3)
The UI status endpoint MUST emit a JSON object containing:

```json
{
  "schema_version": "C3",
  "generated_at_utc": "...",

  "verdict": {
    "state": "PASS|FAIL|DEGRADED|UNKNOWN",
    "source": "gate_stack_verdict_v1",
    "day": "<YYYY-MM-DD>"
  },

  "broker_reconciliation": {
    "state": "...",
    "day": "<YYYY-MM-DD>",
    "account": "..."
  },

  "market_data": {
    "state": "PRESENT|MISSING",
    "latest_snapshot_day": "<YYYY-MM-DD>|n/a"
  },

  "components": [
    {
      "name": "<derived from monitoring dirs>",
      "state": "PRESENT|MISSING|UNKNOWN",
      "reason_code": "..."
    }
  ],

  "overall_state": "PASS|FAIL|DEGRADED|UNKNOWN"
}
