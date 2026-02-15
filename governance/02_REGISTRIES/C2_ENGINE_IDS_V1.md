---
id: C2_ENGINE_IDS_V1
title: "Constellation 2.0 Engine IDs (Sleeve Registry) v1"
status: ACTIVE
version: 1
created_utc: 2026-02-15
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Engine IDs v1 (Sleeve Registry)

This registry defines the only valid `engine.engine_id` identifiers for Constellation 2.0.

## Non-negotiable rule

Any `engine_id` not listed in this registry is invalid and must cause fail-closed behavior at validation time.

## Canonical engine IDs

1. `C2_TREND_EQ_PRIMARY_V1`
   - Sleeve: Trend Sleeve (Equity Primary)
   - Horizon: 10–60 days
   - Role: Core CAGR contributor

2. `C2_VOL_INCOME_DEFINED_RISK_V1`
   - Sleeve: Volatility Income (Defined-Risk Options)
   - Constraint: defined-risk spreads only (no naked short options)

3. `C2_MEAN_REVERSION_EQ_V1`
   - Sleeve: Mean Reversion (Short Horizon Equity)

4. `C2_EVENT_DISLOCATION_V1`
   - Sleeve: Event / Dislocation (Optional Expansion)

5. `C2_DEFENSIVE_TAIL_V1`
   - Sleeve: Defensive / Tail Hedge (Optional Stabilizer)

## Legacy identifiers

Constellation 1.0-style engine identifiers are forbidden in Constellation 2.0.

This includes any short alphanumeric engine codes and any identifiers not listed above.
