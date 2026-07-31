# Aegis Retirement Criteria Policy v1

Purpose: define deterministic pause and retirement criteria for hypotheses, sleeves, and research programs without changing trading behavior, broker policy, or candidate-generation rules.

This policy is review guidance only until separately implemented. It must not retire a sleeve merely because it produced no candidates last week.

## Common States

- `ACTIVE`: evidence generation is functioning or the sleeve/hypothesis is legitimately waiting for setup.
- `WATCH`: early signs of weakness, dormancy, duplication, or blocker accumulation.
- `PAUSE_CANDIDATE`: temporarily stop allocating additional research attention or new paper-testing priority pending review.
- `RETIRE_CANDIDATE`: recommended for retirement after criteria and manual review.
- `RETIRED`: no longer active for candidate generation or validation, after manual review approval.

All retire transitions require `manual_review_required_before_retire: true`.

## Hypothesis Retirement Criteria

| Criterion | Policy fields | WATCH | PAUSE_CANDIDATE | RETIRE_CANDIDATE |
|---|---|---|---|---|
| Candidate drought | `lookback_window_days: 30`, `minimum_candidate_expectation: 1`, `inactivity_threshold: 30` | 14 days with no candidates and no valid near-miss evidence | 30 days with no candidates and no market-condition explanation | 60 days with no candidates, no near misses, and no data blockers remaining |
| Repeated validation failure | `validation_failure_threshold: 2 failed validation windows` | 1 weak validation window | 2 failed windows or confidence deterioration | 3 failed windows with no regime-specific rescue evidence |
| Negative expectancy | `outcome_failure_threshold: expectancy < 0 after min samples` | expectancy below benchmark after 30 samples | negative expectancy after 60 samples | negative expectancy after 90 samples with acceptable independence/regime coverage |
| Severe drawdown | `drawdown_threshold: policy-defined max drawdown breach` | drawdown near threshold | drawdown breach in resolved outcomes | repeated drawdown breach or unacceptable tail risk |
| Persistent data blockers | `blocker_persistence_threshold: 10 trading days` | blockers for 5 days | blockers for 10 days | blockers for 20 days and no owner/path to repair |
| Duplicative lower-quality hypothesis | `required_reason_codes: DUPLICATIVE_WITH_HIGHER_CONFIDENCE_HYPOTHESIS` | overlap identified | lower-quality duplicate with less evidence | duplicate remains lower quality after 30-day review |
| Regime-dependent dormancy | `regime_dormancy_window_days: 60` | dormant outside required regime | pause if required regime absent for long window | retire only if regime tag taxonomy shows opportunity is structurally unavailable or hypothesis is obsolete |

## Sleeve Retirement Criteria

A sleeve is an implementation. Sleeve retirement should usually follow hypothesis review, not precede it.

| Criterion | WATCH | PAUSE_CANDIDATE | RETIRE_CANDIDATE |
|---|---|---|---|
| No candidates | No candidates for 14 days, but classify setup and data state first | No candidates for 30 days with no near misses and no expected dormant regime | No candidates for 60 days, no near misses, no data blockers, and hypothesis still source-declared but ineffective |
| Rules too restrictive | Persistent near misses just outside thresholds | Research-only parameter review indicates production thresholds block all samples | Retire or replace sleeve if parameters cannot produce statistically useful samples without overfitting |
| Data blocked | Any blocker lasting 5 trading days | Blocker lasting 10 trading days | Retire only if data source is unavailable and no substitute is approved |
| Duplicative sleeve | Overlaps active stronger sleeve | Lower evidence quality or redundant implementation | Merge/retire after mapping review and manual approval |
| Negative outcome evidence | Weak closed-sample performance | Resolved outcomes fail validation thresholds | Retire after hypothesis-level disproof or sleeve-specific implementation failure |

## Research Program Retirement Criteria

| Criterion | WATCH | PAUSE_CANDIDATE | RETIRE_CANDIDATE |
|---|---|---|---|
| Stale program | No new candidate, outcome, or thesis refinement for 30 days | No useful evidence for 60 days | No useful evidence for 90 days and no valid regime explanation |
| Duplicative program | Program overlaps another program | Lower-quality duplicate with weaker evidence | Merge or retire after grouping review |
| Insufficient opportunity | Regime taxonomy shows rare opportunity | Opportunity absent for multiple windows | Retire only after historical regime review proves opportunity is structurally too rare |
| Persistent validation failure | One hypothesis disproven | Most linked hypotheses degraded/disproven | All linked hypotheses disproven or retired |

## Required Reason Codes Before Retirement

At least one primary reason and one evidence reason must be present:

Primary reasons:

- `CANDIDATE_DROUGHT_CONFIRMED`
- `VALIDATION_FAILURE_CONFIRMED`
- `NEGATIVE_EXPECTANCY_CONFIRMED`
- `DRAWDOWN_LIMIT_BREACH_CONFIRMED`
- `PERSISTENT_DATA_BLOCKER_CONFIRMED`
- `DUPLICATIVE_LOWER_QUALITY_CONFIRMED`
- `INSUFFICIENT_MARKET_OPPORTUNITY_CONFIRMED`

Evidence reasons:

- `LOOKBACK_WINDOW_MET`
- `MINIMUM_SAMPLE_REQUIREMENT_MET`
- `REGIME_CONTEXT_REVIEWED`
- `NEAR_MISS_HISTORY_REVIEWED`
- `MANUAL_REVIEW_APPROVED`

## Guardrails

- Do not retire from one inactive week.
- Do not retire from open unrealized PnL.
- Do not use missing closed outcomes as alpha disproof.
- Do not let retirement policy affect broker execution or live trading.
- Do not automatically retire without manual review approval.
