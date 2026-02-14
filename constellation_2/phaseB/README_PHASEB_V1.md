---
id: C2_PHASEB_README_V1
title: Constellation 2.0 Phase B — Options Market Data Truth Spine (Offline)
version: 1
status: DRAFT
type: phase_readme
created: 2026-02-14
authority_level: BUNDLE_B_IMPLEMENTATION
---

# Phase B — Options Market Data Truth Spine (Offline)

## Scope
Phase B produces two immutable truth artifacts (offline only):
- options_chain_snapshot.v1.json
- freshness_certificate.v1.json
No broker calls. No network calls.

## Determinism
- No floats allowed anywhere.
- Canonical JSON: UTF-8, sorted keys, no insignificant whitespace.
- SHA-256 over canonical UTF-8 bytes (lowercase hex).
- Decimals emitted at fixed 2dp using Decimal(ROUND_HALF_UP).
- Contracts sorted by contract_key; derived.features aligned in identical order.

## Freshness Rules
- snapshot_hash = SHA-256(canonical snapshot JSON with canonical_json_hash forced null)
- snapshot_as_of_utc == snapshot.as_of_utc
- issued_at_utc == snapshot.as_of_utc
- valid_from_utc == snapshot.as_of_utc
- valid_until_utc == snapshot.as_of_utc + policy.max_age_seconds

## Fail-Closed
- Invalid raw input => non-zero exit and writes NOTHING.
- Schema validation failure => non-zero exit and writes NOTHING.
- Refuse overwrite if output directory exists.

## Raw Input (Phase B Raw Chain Input v1)
Top-level: as_of_utc, underlying{symbol,spot_price,spot_as_of_utc}, contracts[], provenance{source,capture_method,capture_host,capture_run_id}, policy{dte_method=="CALENDAR_DAYS_UTC", liquidity_policy{min_open_interest,min_volume,max_bid_ask_spread}, pricing_policy{mid_definition=="(bid+ask)/2"}}.
Each contract: expiry_utc, strike, right(CALL/PUT), bid, ask, open_interest, volume, ib{conId,localSymbol,tradingClass,exchange,currency,multiplier==100}.
Derived: contract_key, dte_days, bid_ask_spread, mid, is_liquid per README_PHASEB_V1 policy.

## Run (single line)
python3 -m constellation_2.phaseB.tools.c2_build_chain_truth_v1 --raw_input constellation_2/phaseB/inputs/sample_raw_chain_input.v1.json --out_dir constellation_2/phaseB/outputs/sample_run_20260213T215000Z --max_age_seconds 300 --clock_skew_tolerance_seconds 5

## Tests (single line)
python3 -m constellation_2.phaseB.tests.test_phaseB_determinism_v1 && python3 -m constellation_2.phaseB.tests.test_phaseB_failclosed_v1

## Non-Claims
No claims about market data correctness, broker correctness, or profitability. Claims ONLY: schema-valid deterministic artifacts + canonical hashing + fail-closed truth production.
