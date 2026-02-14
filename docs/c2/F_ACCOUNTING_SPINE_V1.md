---
id: C2_F_ACCOUNTING_SPINE_V1
title: "Bundle F — Accounting + Reporting Spine (Audit Grade)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
truth_root: constellation_2/runtime/truth
---

# 1. Objective

Bundle F defines and implements the **Accounting + Reporting Spine** for Constellation 2.0.

It produces **deterministic, replayable, immutable** daily accounting truth artifacts used by:
- operators (audit, review, debugging),
- downstream risk/allocation (Bundle G),
- external hostile review (reconstruction guarantee).

Bundle F’s outputs are not “best effort.” They are:
- **hash-bound** to inputs,
- **deterministically serialized**,
- **immutable** under truth root,
- **fail-closed** when correctness cannot be proven.

# 2. Scope and Non-Claims

## 2.1 In scope

- Deterministic daily accounting outputs:
  - NAV (cash + unrealized)
  - realized P&L (lifecycle terminal events only)
  - unrealized P&L (conservative marking)
  - exposure (defined-risk) totals and breakdowns
  - attribution (engine-level)
  - peak NAV and drawdown (when provable from prior immutable NAV history)
- Explicit failure semantics and reason codes
- Deterministic reconstruction algorithm
- Acceptance tests and determinism tests

## 2.2 Out of scope (explicit non-claims)

Bundle F does **not** claim:

- tax accounting or regulatory reporting
- corporate actions correctness unless a governed input provides it
- reconciliation to broker statements unless a governed cash ledger + reconciliation truth exists
- intraday MTM precision beyond the defined marking policy
- slippage attribution or execution quality (that is another bundle)

# 3. Authoritative Inputs (Hash-Bound)

Bundle F consumes only **authoritative, hash-bound** inputs. It must refuse to infer.

## 3.1 Required input categories

1) **Cash ledger** (authoritative cash state)
2) **Marks** (underlying + options bid/ask)
3) **Positions source of truth**
   - either fills → positions truth, or an already-produced positions snapshot truth
4) **Calendar/time authority** (day key derivation / UTC day)

## 3.2 Optional inputs (degraded operation allowed)

- lifecycle outcomes (terminal closure evidence)
- trade intents, order plans, broker submissions (traceability / attribution enhancement)

## 3.3 Input binding record (required in outputs)

Every output must embed a manifest with entries:

- `path` (repo-relative canonical path)
- `sha256` (content hash)
- `producer` (if known; else `unknown`)
- `type` (cash_ledger / marks / positions / lifecycle / intents / plans / submissions)
- `day_utc` (if day-scoped)

If any required input is missing, unreadable, or hash cannot be computed → `FAIL_CORRUPT_INPUTS`.

# 4. Authoritative Outputs (Immutable Truth)

Bundle F writes immutable truth under:

`constellation_2/runtime/truth/accounting_v1/`

## 4.1 Daily immutable artifacts

- `constellation_2/runtime/truth/accounting_v1/nav/<DAY_UTC>/nav.json`
- `constellation_2/runtime/truth/accounting_v1/exposure/<DAY_UTC>/exposure.json`
- `constellation_2/runtime/truth/accounting_v1/attribution/<DAY_UTC>/engine_attribution.json`

## 4.2 Latest pointer (pointer-only)

- `constellation_2/runtime/truth/accounting_v1/latest.json`

Hard rule: `latest.json` is pointer-only. It contains:
- the canonical paths to daily artifacts
- their sha256 hashes
- the producing git commit sha
- run status summary

It must not duplicate day payloads.

## 4.3 Failure artifact

On FAIL states, Bundle F writes:

- `constellation_2/runtime/truth/accounting_v1/failures/<DAY_UTC>/failure.json`

and **must not** update `latest.json`.

# 5. Immutability & Rerun Behavior

Bundle F must never overwrite a truth artifact.

For any target output path:

- If the path does not exist: write it.
- If the path exists:
  - compute sha256(existing)
  - compute sha256(candidate)
  - if equal: **skip write** (idempotent success)
  - if different: **FAIL** with `ATTEMPTED_REWRITE` and exit non-zero.

# 6. Deterministic Accounting Rules

## 6.1 NAV

For day `D`:

`NAV(D) = CASH(D) + Σ MV_i(D)`

Where:
- `CASH(D)` is authoritative cash balance from cash ledger
- `MV_i(D)` is the marked value of each position component per §7

Outputs must provide:
- `nav_total`
- `cash_total`
- `gross_positions_value`
- `realized_pnl_to_date`
- `unrealized_pnl`
- `status` + `reason_codes`

## 6.2 Realized P&L (strict lifecycle rule)

Realized P&L is recognized **only** from terminal lifecycle closure events.

Terminal closure events are governed by the lifecycle truth contract (Bundle D / execution lifecycle truth).

If lifecycle truth is missing or incomplete:
- realized P&L is computed only from provable closures
- Bundle F status becomes `DEGRADED_MISSING_LIFECYCLE`
- output must explicitly state realized P&L is incomplete

## 6.3 Peak NAV and Drawdown

If prior immutable NAV history exists and is readable:

- `peak_nav(D) = max(NAV(d)) for d <= D`
- `dd_abs(D) = NAV(D) - peak_nav(D)`
- `dd_pct(D) = dd_abs(D) / peak_nav(D)`

If prior nav history is missing or incomplete:
- set `peak_nav` and drawdown fields to `null`
- add `DEGRADED_HISTORY_INCOMPLETE` to reason codes (still a DEGRADED class)

# 7. Conservative Marking Policy (Deterministic)

This policy must be implemented exactly and referenced by Bundle F.

## 7.1 Options / derivatives

For each option leg:

- If position is **long**: mark at **BID**
- If position is **short**: mark at **ASK**

## 7.2 Underlyings (equities/ETFs)

- Long shares: BID
- Short shares: ASK

## 7.3 Missing bid/ask

If only a single price is available:
- treat it as both bid and ask
- set status `DEGRADED_MISSING_MARKS`
- record reason code `MISSING_BID_ASK_FALLBACK_USED`

No mid-price substitution is allowed in `OK`.

# 8. Exposure (Defined-Risk)

Bundle F must compute defined-risk exposure, used by Bundle G.

## 8.1 Allowed structures

Defined-risk structures must have a deterministically computable max loss. Examples:

- Debit-defined-risk: max loss = debit paid
- Credit-defined-risk (vertical spreads): max loss = width * contract_multiplier - credit_received

If an open position is present whose max loss cannot be proven deterministically:
- `FAIL_UNDEFINED_RISK_POSITION_PRESENT`

## 8.2 Output breakdowns

Exposure output must include:
- total defined-risk exposure
- per-engine exposure
- per-underlying exposure
- per-expiry bucket exposure (YYYY-MM)

# 9. Failure Semantics

## 9.1 Status codes (required)

- `OK`
- `DEGRADED_MISSING_MARKS`
- `DEGRADED_MISSING_LIFECYCLE`
- `FAIL_CORRUPT_INPUTS`
- `FAIL_SCHEMA_VIOLATION`

## 9.2 Fail-closed rules

- Schema validation failure on any produced artifact → `FAIL_SCHEMA_VIOLATION`
- Required input missing/unreadable/unhashable → `FAIL_CORRUPT_INPUTS`
- Attempted rewrite (existing bytes differ) → `ATTEMPTED_REWRITE` (treated as FAIL)
- In FAIL states:
  - write `failure.json`
  - do not write/update `latest.json`
  - exit non-zero

# 10. Reconstruction Guarantee (Replay Algorithm)

Given:
- the immutable output artifact(s) for day D
- the embedded input manifest (paths + hashes)
- the producing git commit sha
- the deterministic serialization rules

An auditor must be able to:

1) Load each input by `path`
2) Verify sha256(input bytes) == recorded `sha256`
3) Re-run Bundle F computation for day D
4) Verify sha256(output bytes) == recorded sha256

If the reconstructed output differs, the system is in contract violation.

# 11. Acceptance Tests (Must Be Runnable)

Bundle F must include tests that prove:

1) Marking correctness (BID/ASK selection by side)
2) Realized P&L changes only on terminal lifecycle closures
3) Determinism: re-run with same inputs produces identical bytes
4) Reconstruction: recompute from historical ledgers matches hashes
5) Immutability: attempted rewrite fails closed

# 12. Interface Contract to Bundle G

Bundle G depends on Bundle F’s latest NAV + drawdown.

Bundle F must provide:
- a `latest.json` pointer artifact with:
  - nav artifact path + sha
  - exposure artifact path + sha
  - status code
  - drawdown metrics (or explicit null + reason codes)
  - producing git sha

Bundle G must treat:
- `status != OK` as **BLOCK** for new entries (per Bundle G contract).

# 13. Open Items Requiring Proof Before Implementation

Before implementing Bundle F code, we must prove:

- authoritative paths for cash ledger in C2
- authoritative marks paths in C2
- authoritative fills/positions snapshot truth paths in C2
- lifecycle truth paths in C2
- existing canonical JSON writer / immutability utility modules in C2, if any
