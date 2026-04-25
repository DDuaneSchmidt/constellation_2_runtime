---
id: C2_RECONCILED_TRADE_STATE_CONTRACT_V1
title: "C2 Reconciled Trade State Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_current_trade_truth
---

# C2 Reconciled Trade State Contract V1

## Canonical purpose

Core 2 is the single canonical owner of current post-entry trade truth after Core 1 has produced governed broker observation facts.

Core 2 exists to answer:

- which broker-observed facts belong to the same governed trade identity
- what broker-derived state has been deterministically incorporated into Constellation truth
- how that incorporated state should be described for downstream consumers
- why the current materialization is explainable, replayable, trusted, degraded, or blocked

## Canonical owner

The one canonical current-truth owner inside Core 2 is:

- `incorporated_broker_trade_state.v1`

There is one canonical incorporated state object per governed `trade_identity_id` per materialization boundary.

## Permanent internal layers

### Layer A — Trade Identity Authority

Canonical owner:

- `trade_identity.v1`

This layer owns only:

- canonical trade identity resolution
- ownership classification
- lineage attachment rules
- open/close continuity rules
- foreign/manual classification boundary

This layer must not own:

- current broker-derived truth
- action eligibility
- broker submission

### Layer B — Incorporated Broker Trade State

Canonical owner:

- `incorporated_broker_trade_state.v1`

This layer owns only the broker-derived current state that has been deterministically incorporated from governed Core 1 facts, including:

- current quantity
- side
- average cost when broker evidence provides it
- incorporated fills
- current working order set
- terminal order lineage
- orphan-order facts
- last reconciled fact boundary
- drift basis facts
- freshness basis facts

This is the sole Core 2 truth owner for current post-entry trade state.

### Layer C — Descriptive Trade State Derivation

Derived-only owner:

- `reconciled_trade_description.v1`

This layer derives descriptive state from `incorporated_broker_trade_state.v1` plus governed rules only.

It may describe:

- lifecycle state
- protection state
- reconciliation state
- safe / degraded / blocked downstream posture

It must not:

- authorize actions
- decide policy
- transmit orders

### Layer D — Reconciliation Provenance

Derived audit owner:

- `reconciliation_provenance.v1`

This layer records:

- why facts were incorporated
- why facts were ignored
- why facts were blocked
- what changed from prior materialization
- what rule/version produced the materialization
- what exact Core 1 evidence references were used

## Scope

Core 2 owns only governed post-entry current-truth materialization from Core 1 broker fact inputs.

Core 2 is in scope for:

- identity resolution
- deterministic incorporation of broker-derived truth
- descriptive read models derived from incorporated truth
- trust / degraded / blocked reconciliation health
- provenance sufficient for replay and audit

## Non-scope

Core 2 does not own:

- raw broker evidence ingestion
- Core 1 fact normalization
- action authorization
- stop movement policy
- close / reduce policy
- broker transmission
- scheduler behavior
- operator summary truth ownership
- later-core action outputs as current-truth inputs

## Canonical runtime families

Core 2 writes under the governed sleeve execution root:

- `truth_sleeves/<sleeve_id>/<mode>/reconciled_trade_state_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/trade_identity.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/reconciled_trade_state_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/incorporated_broker_trade_state.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/reconciled_trade_state_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/reconciled_trade_description.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/reconciled_trade_state_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/reconciliation_health.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/reconciled_trade_state_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/reconciliation_provenance.v1.json`

Core 2 may also publish derived-only aggregate read models under:

- `truth_sleeves/<sleeve_id>/<mode>/reports/reconciled_trade_state_summary_v1/<DAY>/<materialization_set_id>/reconciled_trade_state_summary.v1.json`

## Uncertainty and fail-closed behavior

Core 2 must preserve explicit uncertainty. It must be able to materialize:

- unresolved
- degraded
- blocked
- ambiguous
- insufficient evidence

Core 2 must not coerce contradictions into clean-looking certainty.

If reconciliation is not trustworthy for downstream action reliance, Core 2 must materialize a fail-closed posture. Downstream consumers must treat `BLOCKED` posture as non-actionable and must not infer action authorization from Core 2.

## Hard invariants

1. `trade_identity.v1` is a first-class canonical authority.
2. `incorporated_broker_trade_state.v1` is the one canonical owner of current post-entry trade truth.
3. Core 2 consumes Core 1 evidence and does not replace Core 1 as evidence authority.
4. Given identical Core 1 facts and governed rules, Core 2 materializes identical incorporated state.
5. Descriptive state is derived only.
6. Ownership classification must resolve to one of:
   - `CONSTELLATION_OWNED`
   - `FOREIGN_MANUAL`
   - `AMBIGUOUS_OWNERSHIP`
   - `INSUFFICIENT_EVIDENCE`
7. Every materialized trade-state object must carry explicit provenance.
8. Core 2 must not leak action or submit decisions.

## Proof basis

- `governance/05_CONTRACTS/C2/broker_fact_spine_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_root_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_identity_binding_v1.contract.md`
- `governance/05_CONTRACTS/C2/session_authority_status_v1.contract.md`

