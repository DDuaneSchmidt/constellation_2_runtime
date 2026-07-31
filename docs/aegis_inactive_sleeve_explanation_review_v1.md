# Aegis Inactive Sleeve Explanation Review v1

Review date: `2026-06-01`
Evidence sources:

- `/home/node/constellation_runtime_data/truth/reports/sleeve_evaluation_kernel_v1/2026-05-29/sleeve_evaluation_rollup.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_research_portfolio_v1/2026-06-01/research_portfolio.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_research_capital_allocation_v1/2026-06-01/research_capital_allocation.v1.json`

Scope note: the latest complete sleeve rollup reviewed here is `2026-05-29`. That rollup shows `2` intent-producing sleeves, `5` non-simulator `NO_INTENT` sleeves, and `1` optional simulator filtered out. Weekly output counts below summarize available rollups from `2026-05-25` through `2026-05-29`; they are review context, not an instruction to change sleeve behavior.

## Classification Definitions

Allowed reasons: `EXPECTED_NO_SETUP`, `MARKET_CONDITION_NOT_PRESENT`, `DATA_BLOCKED`, `RULES_TOO_RESTRICTIVE`, `DEAD_OR_STALE_HYPOTHESIS`, `DUPLICATIVE_WITH_ACTIVE_SLEEVE`, `NEEDS_PARAMETER_REVIEW`, `UNKNOWN`.

Allowed recommendations: `KEEP_WAITING`, `INVESTIGATE_RULES`, `RELAX_PARAMETERS_FOR_RESEARCH_ONLY`, `PAUSE`, `RETIRE_CANDIDATE`, `MERGE_WITH_RELATED_SLEEVE`, `NEEDS_MORE_DATA`.

## Latest Inactive Sleeves

| Sleeve | Linked thesis | Linked hypothesis | Candidate count last week | Raw signal count last week | Rejection / blocker evidence | Diagnosis | Recommended action |
|---|---|---|---:|---:|---|---|---|
| `C2_DEFENSIVE_TAIL_V1` | `THESIS_DEFENSIVE_CONVEXITY_V1` | `HYP_DEFENSIVE_TAIL_CONVEXITY_V1` | 0 | 0 | Status mix: `BLOCKED` on four reviewed days, `NO_INTENT` on `2026-05-29`; reasons include `MISSING_REQUIRED_INPUTS`, `SLEEVE_INPUT_REQUIREMENT_BLOCKED`, `market.price.TLT`, then `NO_INTENT_DECLARED`; signal state `INACTIVE`. | `DATA_BLOCKED` plus `MARKET_CONDITION_NOT_PRESENT`. This is not retirement evidence; it means the sleeve has not had enough clean input and setup evidence. | `NEEDS_MORE_DATA`; confirm TLT/hedge input coverage and then keep waiting for risk-off/convexity trigger evidence. |
| `C2_EVENT_DISLOCATION_V1` | `THESIS_EVENT_DISLOCATION_V1` | `HYP_EVENT_DISLOCATION_REPRICING_V1` | 23 weekly outputs, but 0 on latest rollup | 0 latest evaluated symbols on `2026-05-29` | Status mix includes `INTENT_CREATED` once, `NO_INTENT` twice, `BLOCKED` twice; reasons include `ALLOWED_SYMBOL_MISMATCH`, `MISMATCHED_INTENT_REJECTED`, `SLEEVE_INPUT_REQUIREMENT_BLOCKED`, and latest `NO_INTENT_DECLARED`; top blocker examples include missing bars under day-zero bootstrap allowance. | `NEEDS_PARAMETER_REVIEW` and `DATA_BLOCKED`. The hypothesis is broad and may be episodic; latest dormancy alone is expected, but weekly evidence shows noisy contract/input matching. | `INVESTIGATE_RULES`; review event trigger branches and symbol/input matching before relaxing anything. |
| `C2_MARKET_NEUTRAL_SPREAD_V1` | `THESIS_RELATIVE_VALUE_SPREAD_V1` | `HYP_MARKET_NEUTRAL_SPREAD_CONVERGENCE_V1` | 0 | 0 | Status mix: `BLOCKED` twice, `NO_INTENT` three times; reasons include `MARKET_DATA_SHA_MISMATCH`, `SLEEVE_INPUT_REQUIREMENT_BLOCKED`, and missing market price dependencies across `HYG`, `IWM`, `LQD`, `QQQ`, `SPY`; signal state `INACTIVE`. | `DATA_BLOCKED`. The spread hypothesis cannot be judged until pair data and hash consistency are clean. | `NEEDS_MORE_DATA`; do not retire; verify pair universe data freshness and spread calculation inputs. |
| `C2_MEAN_REVERSION_EQ_V1` | `THESIS_EQUITY_MEAN_REVERSION_V1` | `HYP_EQUITY_SHORT_HORIZON_MEAN_REVERSION_V1` | 1 weekly output, 0 on latest rollup | 8 evaluated symbols on latest rollup | Latest `NO_INTENT_DECLARED`; nearest misses show candidates close to z-score threshold, e.g. `JBS` at z `-1.73` versus entry threshold `-2.0`; earlier reasons include `ALLOWED_SYMBOL_MISMATCH`, `MISMATCHED_INTENT_REJECTED`, and `SLEEVE_INPUT_REQUIREMENT_BLOCKED`. | `MARKET_CONDITION_NOT_PRESENT` plus `NEEDS_PARAMETER_REVIEW`. This looks like healthy dormancy near threshold, not dead hypothesis evidence. | `KEEP_WAITING`; optionally `RELAX_PARAMETERS_FOR_RESEARCH_ONLY` in a separate approved experiment to study near-miss sensitivity, not production behavior. |
| `C2_VOL_INCOME_DEFINED_RISK_V1` | `THESIS_VOLATILITY_RISK_PREMIUM_V1` | `HYP_DEFINED_RISK_VOL_PREMIUM_V1` | 0 | 6 evaluated symbols on latest rollup | Status mix: `BLOCKED` three times, `NO_INTENT` twice; reasons include `market.volatility.VIX`, `market.price.IWM`, and `NO_INTENT_DECLARED`; nearest misses show vol percentile below `0.75` threshold, with `IWM` pct rank `0.43`. | `MARKET_CONDITION_NOT_PRESENT` plus prior `DATA_BLOCKED`. Current vol setup does not meet entry conditions; this is expected dormancy. | `KEEP_WAITING`; ensure VIX/volatility inputs remain certified and collect near-miss history. |

## Simulator Sleeve

`C2_INTENT_SIMULATOR_V1` was `FILTERED_OUT` as `OPTIONAL_SIMULATION` / `ACTIVE_SIMULATOR_NOT_PRESTART_REQUIRED`. It is excluded from inactive-alpha diagnosis because it is workflow-control infrastructure, not an investment sleeve.

## Key Findings

1. No sleeve should be retired based only on the latest inactive week.
2. `C2_MEAN_REVERSION_EQ_V1` and `C2_VOL_INCOME_DEFINED_RISK_V1` show useful nearest-miss evidence and should remain in observation.
3. `C2_DEFENSIVE_TAIL_V1` and `C2_MARKET_NEUTRAL_SPREAD_V1` need input/data quality confirmation before hypothesis quality can be judged.
4. `C2_EVENT_DISLOCATION_V1` needs rule and hypothesis refinement because it generated historical outputs but was inactive in the latest rollup and its current hypothesis mixes follow-through and reversal behavior.
