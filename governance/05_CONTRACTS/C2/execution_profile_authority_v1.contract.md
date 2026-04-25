---
id: C2_EXECUTION_PROFILE_AUTHORITY_CONTRACT_V1
title: "C2 Execution Profile Authority Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_execution
---

# C2 Execution Profile Authority Contract V1

## Purpose

This contract resolves the governed owner for the live PAPER execution profile:

- host
- port
- order-submit client id
- broker-observer / handshake-bootstrap client id
- account binding

## Canonical owners

### Account binding

Account eligibility is authoritative only from:

- `governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json`

Sleeve-to-account binding is authoritative only from:

- `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`

### Gateway/profile binding

The canonical PAPER execution gateway profile is authoritative only from:

- `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`
- field: `sleeves[].ib_gateway_profile`

For governed PAPER execution, the live-authoritative fields are:

- `host`
- `port`
- `client_id_orders`
- `client_id_observer`

`client_id_market_data` remains topology metadata for market-data workflows and is not the submit-boundary execution-policy authority in this contract.

## Current governed PRIMARY/PAPER profile

- host: `127.0.0.1`
- port: `4002`
- client_id_orders: `7`
- client_id_observer: `179`
- ib_account: `DUO847203`

## Required consumer behavior

The following consumers MUST resolve governed execution profile from the canonical owners above and MUST NOT fall back to ad-hoc env/default values for live governed PAPER execution:

- governed submit command assembly
- submit boundary pre-submit validation
- readiness broker-events bootstrap
- execution observer service configuration

## Proof basis

- `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`
- `governance/05_CONTRACTS/C2/sleeve_registry_v1.contract.md`
- live runtime handshake and readiness artifacts under canonical truth for `2026-04-14`
- `ops/tools/run_c2_paper_day_orchestrator_v2.py`
- `ops/tools/run_session_readiness_refresh_v1.py`
- `ops/systemd/user/c2-execution-observer.service`

## Legacy/non-authoritative settings

The following are non-authoritative for governed PAPER execution profile once this contract is in force:

- orchestrator env/default fallbacks such as `C2_IB_PORT` or `C2_IB_CLIENT_ID` when they disagree with the governed sleeve registry
- stale sleeve-registry values superseded by an approved registry update
- proof-only runbook examples that bypass the governed profile resolver
