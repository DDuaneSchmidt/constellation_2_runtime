---
id: C2_OPERATOR_TRADE_HEALTH_CONTRACT_V1
title: "C2 Operator Trade Health Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_operator_health
---

# C2 Operator Trade Health Contract V1

## Canonical purpose

Core 5 is the single governed derived operator and audit plane for post-entry trade management.

Core 5 exists to answer only:

- what exact cross-core snapshot is being summarized
- what strict governed operator-facing rollup state applies to that snapshot
- what provenance explains the rollup
- what human-readable narrative, timeline, queues, and indices derive from that governed rollup

## Canonical owner

The one external canonical Core 5 owner is:

- `operator_trade_health.v1`

There is one canonical `operator_trade_health.v1` artifact per governed `trade_identity_id` per sealed cross-core snapshot binding.

## Permanent boundaries

Core 5 owns only:

- explicit cross-core snapshot binding
- governed structured rollup
- canonical operator-facing derived health state
- explicit summary provenance
- derived narrative/timeline rendering
- derived queues and indices built only from canonical Core 5 artifacts

Core 5 does not own:

- raw broker evidence
- broker fact normalization
- reconciled trade truth
- action policy truth
- transmit authorization truth
- workflow side effects

## Hard requirements

1. `operator_trade_health.v1` is the one external canonical Core 5 truth owner.
2. Core 5 must bind one exact Core 1 / Core 2 / Core 3 / Core 4 snapshot before rollup.
3. Core 5 outputs remain derived only.
4. Core 5 must not create underlying business truth not supported by lower cores.
5. Every operator-health conclusion must support drill-down to lower-core artifacts and summary provenance.
6. Narrative, timeline, queue, and index artifacts must never become parallel truth owners.
7. Missing, stale, blocked, ambiguous, degraded, or incoherent lower-core truth must remain explicit.
8. Core 5 may label required operator action but must not mutate lower-core truth or broker state.

## Canonical runtime families

Core 5 writes under the governed sleeve execution root:

- `truth_sleeves/<sleeve_id>/<mode>/operator_trade_health_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/operator_snapshot_binding.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/operator_trade_health_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/operator_trade_health.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/operator_trade_health_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/operator_summary_provenance.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/operator_trade_health_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/operator_timeline_view.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/reports/operator_trade_health_queues_v1/<DAY>/<queue_set_id>/operator_trade_health_queues.v1.json`

## Non-scope enforcement

Core 5 must not:

- re-materialize Core 2 truth as a new owner
- re-run Core 3 policy as hidden authority
- re-state Core 4 authorization as a cleaner canonical fact
- summarize lower cores independently in multiple inconsistent ways
- emit unsupported causal narratives

## Proof basis

- `governance/05_CONTRACTS/C2/broker_fact_spine_v1.contract.md`
- `governance/05_CONTRACTS/C2/reconciled_trade_state_v1.contract.md`
- `governance/05_CONTRACTS/C2/lifecycle_action_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/submit_boundary_status_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_snapshot_binding_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_rollup_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_summary_provenance_v1.contract.md`
