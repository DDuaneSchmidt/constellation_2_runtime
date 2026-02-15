---
id: C2_DRAWDOWN_CONVENTION_V1
title: "Constellation 2.0 Drawdown Convention v1 (Canonical, Negative Underwater)"
status: DRAFT
version: 1
created_utc: 2026-02-15
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Constellation 2.0 — Canonical Drawdown Convention v1

## 1. Purpose

This contract defines the **single canonical drawdown convention** for Constellation 2.0.

All code, docs, schemas, truth artifacts, and risk scaling must use this convention.

**No dual conventions. No adapters.**

## 2. Canonical definition (REQUIRED)

Let:

- `NAV_t` = NAV total for day `t` (C2 uses USD integer totals in accounting outputs).
- `peak_nav_t` = rolling peak NAV up to and including day `t`.

Then:

- `peak_nav_t = max(NAV_d) for all d <= t`
- `drawdown_abs_t = NAV_t - peak_nav_t`
- `drawdown_pct_t = (NAV_t - peak_nav_t) / peak_nav_t`

### Sign convention (non-negotiable)

- At peaks: `drawdown_pct_t == 0`
- Underwater: `drawdown_pct_t < 0` (negative)
- Example: peak 100, NAV 92 → `drawdown_pct = (92-100)/100 = -0.08`

## 3. Deterministic computation rules

### 3.1 Peak calculation
- Rolling peak is computed **including** current day.
- If `NAV_t` is the first available NAV observation, then:
  - `peak_nav_t = NAV_t`
  - `drawdown_abs_t = 0`
  - `drawdown_pct_t = 0`

### 3.2 Domain constraints (fail-closed)

- `NAV_t` must be **an integer >= 0**.
  - `NAV_t == 0` is allowed (e.g., bootstrap accounts) and produces a valid drawdown when `peak_nav_t > 0`.
  - `NAV_t < 0` is prohibited → FAIL-CLOSED.
- `peak_nav_t` must be **positive**.
  - If `peak_nav_t <= 0`, drawdown cannot be defined → FAIL-CLOSED.
- If `NAV_t` is missing or non-numeric → FAIL-CLOSED.

### 3.3 Precision and rounding (exact rule)
To ensure reproducible replay across machines:

- All internal computation of `drawdown_pct` must use **Decimal** arithmetic.
- `drawdown_pct` must be quantized to **6 decimal places** using `ROUND_HALF_UP`.
  - Quantization grid: `0.000001`
- The quantized value is the authoritative stored value for truth outputs.

## 4. Canonical drawdown-to-multiplier rule (Bundle G / Phase H)

Given `drawdown_pct` computed per this contract, the canonical multiplier table is:

| drawdown_pct threshold | multiplier |
|---:|---:|
| `>= 0.000000` | `1.00` |
| `<= -0.050000` | `0.75` |
| `<= -0.100000` | `0.50` |
| `<= -0.150000` | `0.25` |

### Boundary behavior (explicit)
- Thresholds are evaluated **from most severe to least severe** (monotone).
- Comparisons are **inclusive** at each threshold.
- The multiplier is **clamped** at the most severe tier:
  - If `drawdown_pct < -0.150000`, multiplier remains `0.25`.

### Missing drawdown (fail-closed)
- If `drawdown_pct` is missing/null at the point of enforcement:
  - The system must FAIL-CLOSED (no sizing / no allocation).

## 5. Required audit fields

Any artifact claiming drawdown enforcement must be able to reproduce:

- `nav_asof_day_utc`
- `nav_total`
- `rolling_peak_nav`
- `drawdown_abs`
- `drawdown_pct` (quantized per §3.3)
- `multiplier`
- the exact threshold table used (same ordering and numeric values)

## 6. Required references

The following artifacts MUST reference this contract ID:

- `docs/c2/G_THROTTLE_RULES_V1.md`
- Phase H drawdown scaling implementation (risk transformer)
- Bundle F accounting NAV history writer (population of peak/drawdown)
- Bundle G allocation summary output (audit block that includes inputs + decision)

Any artifact using a different convention is non-compliant.
