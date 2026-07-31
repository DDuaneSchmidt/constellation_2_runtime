# Research Debt Trend 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Scope: read-only trend report from existing Atlas research debt, coverage, validation, replay, and qualification artifacts. No production integration, governance change, candidate change, replay change, qualification change, paper-forward change, trade recommendation, capital authority, broker execution, position sizing, or automatic memory write.

## Question

Can Atlas research debt be represented as a time series?

Answer: `YES_WITH_LIMITATIONS`.

Atlas now has enough milestone snapshots to track debt movement across data coverage and direct validation. The time series is still sparse and same-day, so rates are useful as local trend estimates, not stable long-term forecasts.

## Inputs Reviewed

- `research_journal/reports/atlas_research_debt_inventory_001.md`
- `research_journal/reports/research_debt_dashboard.md`
- `research_journal/reports/research_debt_post_75pct_data_update_001.md`
- `research_journal/reports/post_daily_completion_validation_001.md`
- `reports/atlas_v2_research_os/market_data_coverage_tracker/latest.json`
- `reports/atlas_v2_research_os/market_data_coverage_tracker/latest_summary.md`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest.json`
- `reports/atlas_v2_research_os/replay_sample_yield/latest_summary.md`

## Metric Definitions

Tracked classes:

| Metric | Definition | Baseline Source |
| --- | --- | --- |
| Debt Score | Weighted debt index normalized to the inventory baseline. | This report. |
| Missing Data | Missing required candidate symbols for focused direct validation. | Direct replay coverage audit and coverage tracker. |
| Unvalidated Candidates | Direct validation candidates still classified `INSUFFICIENT_DATA`. | Direct candidate validation. |
| Proxy Dependence | Candidate universe still dependent on proxy evidence. | Final candidate ranking. |
| Unresolved Findings | Generated adversary findings not yet human-scored or resolved. | Research debt dashboard. |
| Replay Gaps | `BACKTEST_WEAK` plus `INSUFFICIENT_DATA` candidates in backtest-aware qualification. | Research debt inventory and backtest-aware final qualification. |
| Qualification Gaps | Candidates below final score threshold. | Backtest-aware final qualification. |

Expanded Debt Score formula:

```text
debt_score =
  100 * (
    0.20 * missing_data / 14
  + 0.15 * unvalidated_candidates / 8
  + 0.20 * proxy_dependence / 600
  + 0.15 * unresolved_findings / 150
  + 0.15 * replay_gaps / 372
  + 0.15 * qualification_gaps / 490
  )
```

This expanded score keeps the existing dashboard classes but adds replay and qualification gaps as first-class debt dimensions.

## Time Series

| Snapshot | Approx Time | Debt Score | Missing Data | Unvalidated Candidates | Proxy Dependence | Unresolved Findings | Replay Gaps | Qualification Gaps | Evidence |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Inventory baseline | 2026-06-05T18:47Z | 100.0 | 14 | 8 | 600 | 150 | 372 | 490 | Research debt inventory and direct replay coverage audit. |
| Post 75pct data update | 2026-06-05T19:34Z | 86.7 | 6 | 7 | 600 | 150 | 372 | 490 | Post-75pct debt update; acquisition-normalized missing data. |
| Daily completion refresh | 2026-06-05T20:10Z | 76.3 | 0 | 6 | 600 | 150 | 372 | 490 | Coverage tracker and direct validation latest refresh. |

## Delta Table

| Interval | Debt Score Change | Missing Data Change | Unvalidated Candidate Change | Proxy Dependence Change | Unresolved Findings Change | Replay Gap Change | Qualification Gap Change | Interpretation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Baseline -> Post 75pct | -13.3 | -8 | -1 | 0 | 0 | 0 | 0 | Data acquisition removed the first layer of coverage debt and confirmed one candidate. |
| Post 75pct -> Daily completion | -10.4 | -6 | -1 | 0 | 0 | 0 | 0 | Remaining daily data coverage was cleared; one more direct validation improved. |
| Baseline -> Latest | -23.7 | -14 | -2 | 0 | 0 | 0 | 0 | Debt reduction is real but concentrated entirely in missing data and direct validation. |

## Rates

Observed elapsed time from baseline to latest: approximately `1.39` hours.

| Measure | Value | Interpretation |
| --- | ---: | --- |
| Debt score reduction | 23.7 points | Expanded score moved from `100.0` to `76.3`. |
| Debt reduction rate | 17.1 score points/hour | Same-day acquisition-driven rate; not safe to extrapolate across proxy/replay/qualification debt. |
| Count reduction | 16 tracked items | Missing data fell by 14 and unvalidated candidates fell by 2. |
| Count reduction rate | 11.5 tracked items/hour | Mostly data-coverage cleanup, not general debt retirement velocity. |
| Debt growth | 0 tracked items | No tracked class increased across the available snapshots. |
| Debt growth rate | 0.0 tracked items/hour | No observed same-day growth in this metric set. |

## Projected Debt Clearance

| Debt Class | Latest Count | Observed Reduction | Class-Specific Clearance Projection | Projection Quality |
| --- | ---: | ---: | --- | --- |
| Missing Data | 0 | 14 | Cleared for focused daily-symbol coverage. | HIGH for daily coverage; does not imply intraday/event data is cleared. |
| Unvalidated Candidates | 6 | 2 | About `4.2` hours at the observed direct-validation reduction rate. | LOW; remaining cases are replay/filter/evidence problems, not simple data acquisition. |
| Proxy Dependence | 600 | 0 | No finite projection from observed data. | HIGH confidence that current rate is zero. |
| Unresolved Findings | 150 | 0 | No finite projection from observed data. | HIGH confidence that current rate is zero. |
| Replay Gaps | 372 | 0 | No finite projection from observed data. | HIGH confidence that current rate is zero in canonical qualification reports. |
| Qualification Gaps | 490 | 0 | No finite projection from observed data. | HIGH confidence that current rate is zero in canonical qualification reports. |

Naive blended projection:

```text
latest tracked count = 1,618
observed reduction rate = 11.5 items/hour
naive clearance = about 141 hours
```

Judgment: reject the naive blended projection as operationally misleading. The observed reduction came from a finite missing-data acquisition task. The remaining debt is dominated by unchanged proxy dependence, replay gaps, qualification gaps, and unresolved findings. Those classes require different work and currently have zero measured clearance velocity.

## Trend Interpretation

Research debt is improving, but unevenly.

Data coverage debt moved from hard blocker to cleared daily-symbol coverage for the focused validation set:

- Missing required symbols: `14 -> 6 -> 0`
- Candidate coverage: latest tracker reports `15/15` required symbols and `8/8` full candidate coverage.
- Coverage-derived validation block rate: latest tracker reports `0.0%`.

Direct validation debt improved but remains material:

- `INSUFFICIENT_DATA`: `8 -> 7 -> 6`
- Confirmed direct validations: latest report shows `2`
- Remaining insufficient cases are no longer missing-daily-data problems; they are replay/filter/evidence-strength problems.

The main debt plateau is downstream:

- Proxy dependence remains `600`.
- Replay gaps remain `372`.
- Qualification gaps remain `490`.
- Unresolved findings remain `150`.

This means the next trend break will not come from more daily data alone. It must come from candidate-specific evidence replacement, replay attrition remediation, qualification blocker attribution, and human scoring or retirement of unresolved findings.

## Result

`PASS_WITH_LIMITATIONS`

Atlas can track research debt as a time series now. The current series is sufficient to show debt reduction in missing data and direct validation, but insufficient to project full clearance because most high-mass debt classes have not started declining.

## Authority Boundary

This report is measurement only. It does not modify production, runtime truth, governance, candidate state, replay state, qualification state, paper-forward state, research memory, trading state, broker execution, capital allocation, or position sizing.
