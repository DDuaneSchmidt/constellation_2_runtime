# tax_state_precedence_v1

This contract governs the deterministic Bundle 10 tax blocker and opportunity precedence matrix.

Core law:
- precedence resolution MUST be table-driven
- higher-risk states dominate lower-priority opportunity visibility
- multiple blockers or opportunities MUST be ordered deterministically

Precedence order from highest to lowest:
1. `SUPERSEDED_TAX_STATE`
2. `STALE_TAX_STATE`
3. `INCOMPLETE_TAX_BASIS`
4. `DEGRADED_TAX_RUNTIME`
5. `TAX_BLOCKER_PRESENT`
6. `HISTORICAL_VISIBILITY_ONLY`
7. `TAX_OPPORTUNITY_VISIBLE`
8. `TAX_CURRENT_NO_OPPORTUNITY`

Dominance rules:
- `incomplete_basis` dominates all opportunity visibility
- `degraded_runtime` dominates all opportunity visibility
- `superseded` forces `historical_only` visibility
- `stale` downgrades advisory usefulness and suppresses promotion-like tax claims
- `wash_sale_conflict` dominates harvest opportunity visibility
- historical-only tax opportunity MUST NOT remain current

Visibility law:
- blocker or degraded states MAY remain `current_downgraded` when the current state itself is the operator-facing truth
- superseded states MUST be `historical_only`
- suppressed opportunity visibility MUST remain explicit through `visibility_state`
