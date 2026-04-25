# tax_aware_advisory_binding_v1

This contract governs the single legal Bundle 10 path by which tax state may influence advisory.

Core law:
- advisory MUST consume tax meaning only through `tax_state_v1`
- advisory MUST NOT perform UI-local, CLI-local, or advisory-local tax ranking
- tax-aware advisory items MUST preserve explicit tax refs

Required binding fields when tax-aware advisory is evaluated:
- the advisory decision MUST preserve the bound tax artifact through:
  - `governing_tax_refs`
  - `tax_binding_state`
- `tax_binding_state` MUST preserve:
  - `effect_state`
  - `completeness_state`
  - `freshness_state`
  - `blocker_states`
  - `opportunity_states`
  - `binding_reason_id`

Binding rules:
- incomplete, degraded, stale, or superseded tax state MUST downgrade or block advisory according to the tax binding summary
- tax blocker states MAY block advisory actionability
- tax opportunity states MAY enrich advisory explanation only when tax state is complete and current
- advisory MUST NOT invent unsupported tax priority, certainty, or action framing

