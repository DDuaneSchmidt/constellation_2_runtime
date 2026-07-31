# AI Factor Generator

`src/ai_factor_generator.py` generates deterministic candidate factor and portfolio hypotheses for research review.

Boundary:

- Generation only.
- No Portfolio123 calls.
- No network or API dependency.
- No backtests, validation, candidate promotion, paper positions, trade advice, broker execution, capital allocation, or runtime gate changes.

Public functions:

- `generate_candidates()` returns one deterministic candidate for each supported factor family.
- `generate_by_family(factor_family)` returns the deterministic candidate for a single supported family.
- `generate_anti_ultrasafe_candidates()` returns the anti-UltraSafe diversification candidate set.

Each candidate contains `candidate_id`, `name`, `hypothesis`, `factor_family`, `economic_rationale`, `expected_behavior`, `why_it_may_diversify_ultrasafe`, `complexity_score`, `overfit_risk_score`, and `novelty_score`.

Supported families are `quality`, `value`, `momentum`, `low_volatility`, `earnings_revisions`, `cash_flow`, `balance_sheet_strength`, `shareholder_yield`, `small_cap_quality`, `turnaround`, `defensive_growth`, `anti_ultrasafe`, `capital_efficiency`, `margin_stability`, `earnings_quality`, and `financial_strength`.
