# advisory_decision_state_precedence_v1

This contract governs the deterministic advisory invalidation and downgrade matrix for Bundle 8.

Matrix law:
- precedence MUST be table-driven
- the first matching rule in the governed order below is the primary decision-state outcome
- lower-priority rows MUST NOT override a higher-priority row once matched

Primary precedence order:
1. `uncertified`
2. `incomplete_basis`
3. `superseded`
4. `blocked`
5. `stale`
6. `historical_only`
7. `promotion_eligible`
8. `actionable`

Required dominance behavior:
- `uncertified` dominates all lower-priority states
- `incomplete_basis` dominates stale, blocked, actionable, and promotion-eligible outcomes
- `superseded` dominates blocked, stale, actionable, and promotion-eligible outcomes
- `blocked` dominates actionable and promotion-eligible outcomes
- `stale` revokes promotion eligibility and current actionability
- `historical_only` MUST suppress current-view promotion claims

Deterministic outcome table:

| precedence_state | required condition | decision_state | actionability_state | visibility_state | promotion_eligibility_state |
| --- | --- | --- | --- | --- | --- |
| `uncertified` | required certification or startup-chain proof missing | `uncertified` | `blocked` | class-governed downgraded visibility | `revoked` or `not_applicable` |
| `incomplete_basis` | required release/baseline proof missing or invalid when actionability is claimed | `incomplete_basis` | `blocked` | `current_downgraded` | `revoked` or `not_applicable` |
| `superseded` | governing transition basis superseded by certified later truth | `superseded` | `inspect_only` | class-governed historical visibility | `revoked` or `not_applicable` |
| `blocked` | certified advisory basis exists but release/deployment state blocks actionability | `blocked` | `blocked` | `current_downgraded` | `revoked` or `not_applicable` |
| `stale` | certified basis is older than the latest certified stage basis | `stale` | `inspect_only` | class-governed stale visibility | `revoked` or `not_applicable` |
| `historical_only` | current visibility is historical-only even though no higher-priority invalidator fired | `historical_only` | `inspect_only` | `historical_only` | `revoked` or `not_applicable` |
| `promotion_eligible` | certified fresh basis plus release-ready actionability for a `promotion_eligible` advisory item | `promotion_eligible` | `promotion_eligible` | `current` | `eligible` |
| `actionable` | certified fresh basis and no higher-priority invalidator | `actionable` | `actionable` | `current` | `not_applicable` |

Class-governed visibility law:
- `informational` and `diagnostic` MAY remain `current_downgraded` when stale or blocked
- `recommendation` MUST be `historical_only` when stale, superseded, or uncertified
- `promotion_eligible` MUST be `suppressed` when stale, superseded, or uncertified
