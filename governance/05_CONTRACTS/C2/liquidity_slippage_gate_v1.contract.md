---
id: C2_LIQUIDITY_SLIPPAGE_GATE_CONTRACT_V1
title: "Liquidity + Slippage Envelope Gate Contract v1 (Hard Pre-Trade)"
status: DRAFT
version: 1
created_utc: 2026-03-02
owner: Constellation
authority: governance+git+runtime_truth
tags:
  - risk
  - liquidity
  - slippage
  - pre-trade
  - gate
  - deterministic
  - fail-closed
---

# Liquidity + Slippage Envelope Gate Contract v1

## 1. Purpose

This gate provides an institutional **capacity + execution survivability** control:

- compute deterministic liquidity proxies per intended intent
- compute deterministic estimated slippage bps
- enforce governed limits **before submission** via gate_stack_verdict enforcement

## 2. Policy authority

The governed policy registry is:

- `governance/02_REGISTRIES/C2_LIQUIDITY_SLIPPAGE_POLICY_V1.json`

Policy MUST be schema-valid against:

- `governance/04_DATA/SCHEMAS/C2/RISK/liquidity_slippage_policy.v1.schema.json`

## 3. Market data authority (v1)

Liquidity proxies are derived deterministically from:

- `constellation_2/runtime/truth/market_data_snapshot_v1/<SYMBOL>/<YEAR>.jsonl`

Each JSONL record MUST include at least:
- `timestamp_utc`, `close`, `volume`, `symbol`

If required market data is missing for a symbol, the gate MUST fail-closed,
unless the symbol is explicitly allow-listed by the policy.

## 4. Inputs

At minimum, the gate consumes:

- day UTC key: `<DAY>`
- accounting NAV truth (v2 preferred, else v1)
- intents snapshot directory: `constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/`
- market data dataset manifest: `constellation_2/runtime/truth/market_data_snapshot_v1/dataset_manifest.json`
- governed policy manifest and policy schema

All inputs MUST be listed in `input_manifest` with sha256.

## 5. Computation (deterministic)

For each exposure intent:

- derive symbol (underlying)
- derive target_notional_pct (string, 6dp)
- derive est_notional_usd = NAV * target_notional_pct
- derive close from most recent daily bar <= DAY
- derive est_shares = floor(est_notional_usd / close)
- derive ADV_shares = average(volume) over lookback window
- participation_pct_adv = est_shares / ADV_shares
- estimated slippage bps = base_slippage_bps + slippage_bps_per_1pct_adv * (participation_pct_adv * 100)

If regime snapshot is present and indicates non-normal regime, caps may be tightened
by `non_normal_regime_caps_multiplier`.

## 6. Output (immutable truth)

The gate writes:

- `constellation_2/runtime/truth/reports/liquidity_slippage_gate_v1/<DAY>/liquidity_slippage_gate.v1.json`

The artifact MUST conform to:

- `governance/04_DATA/SCHEMAS/C2/REPORTS/liquidity_slippage_gate.v1.schema.json`

The output MUST include:
- policy path + sha
- policy schema path + sha
- input_manifest with sha256 for all consumed inputs
- per-intent PASS/FAIL/SKIP decisions with reason codes
- gate_sha256 = sha256 of canonical JSON excluding gate_sha256

## 7. Enforcement

This gate MUST be listed in:

- `governance/02_REGISTRIES/GATE_HIERARCHY_V1.json`

as `required: true` and `blocking: true`.

Therefore, failure or missing artifact forces gate_stack_verdict FAIL, and submit_boundary refuses submission.
