---
id: C2_VALUE_STATE_KERNEL_V1
title: "C2 Value State Kernel Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_value_plane
---

# value_state_kernel_v1

Core law:
- `constellation_2.common.value_state_kernel_v1` is the only legal semantic source for realized value truth and allowed value claims on the active C2 path.
- sleeve, outcome, product, and report surfaces MUST NOT classify effectiveness, attribution, or claim strength outside this kernel.

Allowed inputs:
- `tax_state_v1`
- `opportunity_state_v1`
- `opportunity_review_snapshot_v1`
- `product_summary_v1`
- `product_snapshot_v1`
- governed realized execution truth already ratified in repo, including `fill_ledger_v1`
- governed realized reconciliation truth already ratified in repo, including `reconciliation_report_v3`
- governed sleeve identity and sleeve-mapping truth already proven in repo

Forbidden inputs:
- page-local performance storytelling
- AI-local value interpretation
- raw runtime inspection outside governed artifact paths
- broad speculative simulation
- unconstrained counterfactuals

Canonical object law:
- one evaluation yields one canonical `value_state_v1` object
- the same governed inputs MUST yield the same value classification except for ratified volatile timestamps
- the canonical object MUST include one explicit claim-strength result, one explicit effectiveness/attribution result, and explicit `sleeve_refs`

Comparison law:
- comparisons MUST remain subordinate to the value object
- unsupported comparison types MUST be rejected explicitly
