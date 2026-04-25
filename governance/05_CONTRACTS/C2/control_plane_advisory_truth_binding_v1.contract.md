# control_plane_advisory_truth_binding_v1

This contract governs the Bundle 6 advisory truth-binding projection.

Truth basis law:
- every advisory truth-binding projection MUST bind to explicit control-plane refs
- advisory truth-binding MUST NOT read advisory or reporting surfaces as upstream truth
- advisory outputs MUST NOT become upstream truth for other trust-plane projections

Required binding refs:
- stage ref
- transition-record ref
- certification ref when the requested advisory authority class requires certified basis
- startup-chain certification ref when promotion-eligibility is claimed

Advisory authority classes:
- `informational`
- `diagnostic`
- `recommendation`
- `promotion_eligible`

Freshness states:
- `fresh`
- `stale`
- `superseded`
- `uncertified`
- `historical_only`

Downgrade and suppression law:
- `informational` and `diagnostic` views MAY remain visible when stale or superseded, but they MUST downgrade explicitly
- `recommendation` views MUST become `historical_only` when stale or superseded and MUST NOT appear current when uncertified
- `promotion_eligible` requires certified non-superseded truth and MUST be suppressed from current views when stale, superseded, or uncertified

