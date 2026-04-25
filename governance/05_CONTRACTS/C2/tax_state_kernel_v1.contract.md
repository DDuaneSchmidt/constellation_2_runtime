# tax_state_kernel_v1

This contract governs the Bundle 10 certified tax state plane.

Core law:
- `constellation_2.common.tax_state_kernel_v1` is the only legal semantic source for live tax decision support on the active C2 path
- tax/operator/advisory surfaces MUST NOT recompute tax completeness, blocker state, opportunity state, wash-sale state, or explanation semantics outside that kernel
- tax state evaluation MUST consume deterministic governed truth only and MUST fail closed on forbidden or unprovable upstream truth

Allowed inputs:
- deterministic tax logic outputs from:
  - `constellation_2.common.tax.state_v1`
  - `constellation_2.common.tax.decision_v1`
  - `constellation_2.common.tax.harvest_v1`
  - `constellation_2.common.tax.execution_gate_v1`
  - `constellation_2.common.tax.replay_v1`
- certified or governed runtime portfolio truth:
  - `positions_snapshot.v5`
  - `cash_ledger_snapshot.v1`
  - `accounting_nav.v2`
  - `C2_IB_ACCOUNT_REGISTRY_V1.json`
- accepted tax fact journals and corrections when they are present on the canonical tax path
- release/readiness and advisory inputs only through the separately governed binding path when required

Forbidden inputs:
- UI-local tax recomputation
- advisory-local tax heuristics or tax ranking
- freeform tax inference
- direct inspection of repo-local or runtime-copy truth roots
- unsupported tax confidence scoring

Canonical tax-state law:
- one tax evaluation MUST yield one canonical tax state object
- the canonical object MUST carry:
  - `completeness_state`
  - `freshness_state`
  - `visibility_state`
  - ordered blocker states
  - ordered opportunity states
  - `lot_basis_state`
  - `holding_period_state`
  - `wash_sale_state`
  - governing refs
  - primary degraded or invalidation rule
  - deterministic explanation payload
  - advisory binding summary
- the same deterministic truth basis MUST yield the same tax state object except for ratified volatile timestamps

Precedence law:
- tax state MUST be resolved by one deterministic precedence matrix, not scattered conditionals
- higher-risk degraded or blocker states dominate lower-priority opportunity visibility
- the precedence matrix is governed separately by `tax_state_precedence_v1`

Explanation law:
- tax explanation mapping MUST be table-driven
- explanation MUST preserve authority label, governing refs, completeness, freshness, blocker state, opportunity state, and degraded reason
- explanation MUST NOT invent unsupported tax meaning, tax advice, or confidence claims

Historical law:
- tax state artifacts MUST be append-only
- superseding tax state artifacts MUST preserve explicit lineage
- historical-only and suppressed visibility MUST be explicit and deterministic

