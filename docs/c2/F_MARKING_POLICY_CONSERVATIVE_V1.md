---
id: C2_F_MARKING_POLICY_CONSERVATIVE_V1
title: "Bundle F — Conservative Marking Policy (Deterministic, Audit Grade)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
truth_root: constellation_2/runtime/truth
---

# 1. Purpose

This policy defines the **conservative marking rules** used by Bundle F for unrealized P&L and position market value.

Goals:
- eliminate optimistic bias (no mid unless explicitly allowed)
- be deterministic and replayable
- be robust to missing data (explicit degraded modes)
- be suitable for hostile audit review

This policy is binding for Bundle F outputs.

# 2. Definitions

- **BID**: price at which the market buys from you
- **ASK**: price at which the market sells to you
- **LAST**: last traded price (not necessarily actionable)
- **MID**: (BID + ASK) / 2 (optimistic for liquidation; not used in OK by default)

- **Long**: positive quantity (you own)
- **Short**: negative quantity (you owe)

- **Mark record**: `{bid, ask, last, source, asof_utc}`

# 3. Default Conservative Marking Rules (Binding)

## 3.1 Options / derivatives

For each option leg:

- Long (qty > 0): mark = BID
- Short (qty < 0): mark = ASK

## 3.2 Underlyings (equities/ETFs)

- Long shares: mark = BID
- Short shares: mark = ASK

## 3.3 Spreads / multi-leg positions

- Marked value is the sum of leg values marked independently per above rules.
- No netting across legs is allowed unless the position structure is explicitly represented and proven in inputs.

# 4. Mark Source Preference (Deterministic Priority)

Bundle F must choose marks deterministically using this precedence order:

1) **Primary bid/ask mark** from the authoritative options/underlying marks input for the day
2) If bid/ask missing:
   - use LAST as a fallback only if LAST exists and is proven day-scoped
   - and record a degraded status
3) If only a single price exists (no bid/ask, no last):
   - fail closed unless governance explicitly permits a synthetic mark for that instrument class

This policy defaults to being strict: missing actionable marks is a degradation, not silent substitution.

# 5. Missing Data Handling (Degraded vs Fail)

## 5.1 Degraded (allowed)

Bundle F may proceed in `DEGRADED_MISSING_MARKS` if and only if:

- The instrument’s mark record lacks bid/ask but provides LAST (or a single price) AND
- the system records:
  - `status = DEGRADED_MISSING_MARKS`
  - a reason code indicating the exact fallback used

Required reason codes (strings):

- `MISSING_BID_ASK_FALLBACK_LAST`
- `MISSING_BID_ASK_FALLBACK_SINGLE_PRICE`

## 5.2 Fail closed (required)

Bundle F must FAIL if:

- an instrument required for NAV cannot be assigned any mark value under this policy
- the mark is not day-scoped or lacks an `asof_utc` timestamp
- the mark is non-finite (NaN/Inf) or negative where prohibited by instrument class
- input mark file cannot be hash-verified

Failure codes:
- `FAIL_CORRUPT_INPUTS` (missing/unreadable/unhashable)
- `FAIL_SCHEMA_VIOLATION` (mark record violates schema)
- `FAIL_MARKS_UNUSABLE` (policy cannot produce a mark deterministically)

# 6. Staleness and Freshness Rules

A mark must be accompanied by `asof_utc`.

Bundle F must apply a day-scoped freshness rule:

- The mark’s `asof_utc` must fall within day `D` UTC, or within an explicitly governed tolerance window.

If a mark is outside tolerance:
- default action: `DEGRADED_MISSING_MARKS` with reason `STALE_MARK_OUTSIDE_TOLERANCE`
- if the instrument is material to NAV and no alternative mark exists: `FAIL_MARKS_UNUSABLE`

# 7. No-Silent-Zero Rule

Bundle F must never:
- treat missing marks as zero value
- treat missing positions as zero exposure
- treat missing cash as zero cash

Any such condition must be explicit:
- degraded with reason codes, or
- fail closed.

# 8. Mark Transparency in Outputs

Whenever a mark is used for valuation, the output must include:
- the chosen mark value
- the full mark record (bid/ask/last/source/asof_utc)
- the rule used (e.g. `LONG_BID`, `SHORT_ASK`, `FALLBACK_LAST`)

This is required for hostile audit review.

# 9. Acceptance Tests (Policy)

Bundle F must include tests proving:

1) Long option uses bid; short option uses ask
2) Long underlying uses bid; short underlying uses ask
3) Missing bid/ask uses last only with degraded status and reason code
4) Stale mark triggers degraded or fail as specified
5) No-silent-zero enforced: missing mark causes degraded/fail, never zero

# 10. Change Control

Any change to this policy requires:
- a new versioned policy doc (V2)
- corresponding schema updates if outputs change
- acceptance tests updated and passing
