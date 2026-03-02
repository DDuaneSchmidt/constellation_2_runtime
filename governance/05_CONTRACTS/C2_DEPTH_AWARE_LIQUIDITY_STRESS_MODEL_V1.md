---
id: C2_DEPTH_AWARE_LIQUIDITY_STRESS_MODEL_V1
status: active
owner: constellation-risk
created_utc: 2026-03-02T00:00:00Z
supersedes: []
tags:
  - c2
  - liquidity
  - convex
  - determinism
  - fail-closed
---

# C2 Depth-Aware Liquidity Stress Model v1

## Scope
This contract defines the Depth-Aware Liquidity Stress Model (DALSM) and its binding integration into Convex Shock Envelope enforcement. It introduces exactly one new persistent truth artifact and does not alter strategy logic or capital allocation formulas.

## Hard Constraints
- Deterministic: no randomness, no wall-clock time reads, no external nondeterministic calls.
- Artifact-backed: all computations must use only day-scoped runtime truth artifacts already present plus governed policy.
- Pre-trade enforced: DALSM must bind allocation caps before any broker submission.
- Fail-closed: missing/stale/invalid liquidity inputs must block, not monitor.
- Replay-certifiable: two-run identity equality required.
- No strategy changes: signals, intent generation logic, and allocation formulas must not be modified.
- No envelope regression: DALSM may only tighten existing envelopes; it must never weaken prior gates.
- No new persistent spines beyond the DALSM depth stress artifact.
- No duplicate truth trees.

## New Truth Artifact (Single New Spine)
Writer MUST produce:
- constellation_2/runtime/truth/reports/depth_liquidity_stress_v1/{DAY}/depth_liquidity_stress.v1.json

This artifact MUST be schema-validated and immutable-or-compare written.

## Deterministic Inputs
DALSM MUST use ONLY:
- constellation_2/runtime/truth/market_data_snapshot_v1/dataset_manifest.json
- constellation_2/runtime/truth/market_data_snapshot_v1/{SYMBOL}/{YEAR}.jsonl (or equivalent existing bars path)
- constellation_2/runtime/truth/intents_v1/snapshots/{DAY}/...
- governance/02_REGISTRIES/C2_DEPTH_LIQUIDITY_STRESS_POLICY_V1.json

No other data sources are permitted.

## Regimes
DALSM MUST support exactly three regimes:
- NORMAL
- VOL_EXPANSION
- LIQ_CONTRACTION

Regime selection MUST be deterministic and policy-governed.

## Cost Model Requirements
DALSM MUST model:
- Depth removal under stress (X% reduction of synthetic depth proxy)
- Spread widening multiplier under stress
- Nonlinear impact curvature (q^alpha) under stress
- Cross-engine stacking aggregation for symbol demand

All numeric values MUST be encoded deterministically (canonical decimals-as-strings) in the DALSM artifact.

## Convex Integration (Binding Enforcement)
Convex risk assessment MUST:
- Read the DALSM artifact
- Record its path and sha256 in convex_risk_assessment.v1.json
- Compute a binding depth-derived cap (depth_scale_bp)
- Apply final cap as:
  cse_scale_bp_final = min(cse_scale_bp_after_liquidity_existing, depth_scale_bp)

If DALSM fails closed, depth_scale_bp MUST be 0 and Convex MUST block all allocation.

## Fail-Closed Conditions (Non-Exhaustive)
DALSM MUST fail closed if:
- dataset_manifest.json missing/invalid/stale for DAY
- required symbol bars missing and not policy-allowlisted
- intents missing or unparsable
- any computed stressed depth <= 0 for an active symbol
- policy invalid or missing

Fail-closed MUST propagate to a pre-trade block.

## Replay Certification Requirements
Replay certification bundle MUST include:
- depth_liquidity_stress artifact path
- depth_liquidity_stress artifact sha256

Replay integrity checks MUST assert bit-identical bytes across two runs for:
- depth_liquidity_stress.v1.json
- convex_risk_assessment.v1.json
- allocation summary
- submission bundle
- replay certification bundle

Any drift MUST hard-fail replay certification.

## No Envelope Regression
DALSM MUST not remove or loosen any existing envelope gate.
It may only introduce additional tightening through min() composition of caps.

## Acceptance Tests
Minimum required scenario-days:
- Thin depth + 3 engines stacking
- Spread widening + vol expansion
- Liquidity contraction + correlation spike
- Stale depth inputs -> fail-closed block

Each scenario MUST produce deterministic artifacts and satisfy replay identity equality.
