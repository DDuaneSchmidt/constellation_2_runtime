# decision_plan_delta_v1

Defines the governed delta artifact between two decision plans.

Allowed classifications:
- `fact_change`
- `policy_change`
- `recommendation_change`
- `constraint_change`
- `no_change`

Rules:
- must compare validated decision plans only
- must be deterministic
- must fail closed on missing or invalid plans
