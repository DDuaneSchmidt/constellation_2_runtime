# Atlas Research OS Design 003: Knowledge Retirement

Date: 2026-06-04
Status: Design only

Scope: defines lifecycle policy for future Research OS knowledge objects. This document does not implement lifecycle state mutation, candidate suppression, candidate promotion, trading policy, or capital approval.

Runtime posture: the verified runtime graph for 2026-06-04 is `BLOCKED`. Retirement policy must query verified truth when implemented and must not infer readiness from code.

## Lifecycle States

| State | Meaning |
| --- | --- |
| `SUPPORTED` | Evidence currently supports the claim or lesson within declared scope. |
| `STALE` | Evidence may be obsolete because freshness, source, regime, or review thresholds have expired. |
| `WEAKENED` | New evidence reduces confidence but does not yet falsify the object. |
| `FALSIFIED` | Evidence contradicts the object within its declared scope beyond the falsification threshold. |
| `RETIRED` | The object should not be used for positive prioritization or candidate influence unless reopened. |
| `REOPENED` | A retired or stale object is explicitly reconsidered under a materially new scope or evidence trail. |
| `QUARANTINED` | The object is unsafe to consume because lineage, label, source, or authority is compromised. |

## Required Transitions

### `SUPPORTED -> STALE`

Occurs when freshness thresholds expire without adequate refresh.

Default stale thresholds:
- generated or mock evidence: 14 calendar days without review
- historical replay: 90 calendar days or after material source/schema change
- paper-forward observation: when expected observation or close-condition review is overdue by 5 trading days
- external validation: 180 calendar days or immediately after source retraction/material revision
- operator approval: at the explicit review due date, or 30 calendar days if no due date exists

### `STALE -> RETIRED`

Occurs when stale knowledge is not refreshed, remains unsupported, or creates duplicate work risk.

Retirement triggers:
- stale for two review cycles
- no available source to refresh
- materially superseded by stronger evidence
- repeatedly spawns duplicate low-value tasks
- no longer relevant to active mechanisms, regimes, or candidate-factory measurement

### `WEAKENED -> FALSIFIED`

Occurs when contradiction crosses a declared threshold.

Falsification thresholds:
- historical replay contradicts the object across the declared scope with reproducible lineage
- paper-forward observation fails the predeclared success metric across the minimum sample
- external validation refutes the mechanism or invalidates the source assumption
- repeated failure pattern shows the object causes avoidable false positives or blocked flows
- operator-reviewed evidence determines the original scope was materially wrong

Falsification must state scope. A claim may be falsified for one regime and still untested elsewhere.

### `RETIRED -> REOPENED`

Occurs only when a material difference justifies reconsideration.

Reopening requirements:
- reference to the retired object
- new evidence trail or new regime context
- explanation of material difference
- duplicate check against existing reopened work
- bounded allowed scope
- review due date
- operator approval for reopening when prior retirement involved falsification, safety risk, or repeated waste

Reopening does not restore prior confidence. It creates a new scoped research question.

### `ANY -> QUARANTINED`

Occurs when safe consumption cannot be guaranteed.

Quarantine triggers:
- missing or broken lineage
- evidence label conflict
- generated or mock evidence presented as validated
- source unavailable or unverifiable
- stale runtime truth dependency
- artifact hash mismatch
- candidate, sleeve, trade, or capital leakage risk
- external source retraction
- operator approval used outside scope

Quarantined objects are audit-only until repaired, reopened, or permanently retired.

## Audit Trail Requirements

Every lifecycle transition must record:

- object id and object type
- prior state and new state
- transition reason code
- triggering evidence ids and hashes
- evidence levels before and after
- scope and regime affected
- worker or operator identity
- timestamp
- policy version
- duplicate and retirement checks
- allowed uses and forbidden uses after transition

Audit trails must be append-only. A transition may be superseded but not erased.

## Candidate And Capital Boundaries

Retired, stale, weakened, falsified, or quarantined knowledge must not provide positive evidence for candidate generation or candidate promotion.

Reopened knowledge may create bounded research work only. It cannot authorize a candidate, paper observation, sleeve mutation, trade advice, broker execution, or capital allocation.

`OPERATOR_APPROVED` reopening or retirement means the lifecycle decision was approved. It does not mean capital was approved.
