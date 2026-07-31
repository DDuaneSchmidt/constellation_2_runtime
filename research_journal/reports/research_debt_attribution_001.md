# Research Debt Attribution 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Scope: measurement-only attribution from `research_debt_dashboard` and `research_debt_trend`. No recommendations, learning claims, governance changes, candidate changes, replay changes, qualification changes, paper-forward changes, trading changes, broker actions, position sizing, or capital allocation.

## Inputs

- `research_journal/reports/research_debt_dashboard.csv`
- `research_journal/reports/research_debt_dashboard.md`
- `research_journal/reports/research_debt_trend_001.md`

## Measurement Basis

Dashboard score basis:

| Dashboard Metric | Severity | Current Count | Baseline | Reduced | Progress |
| --- | --- | ---: | ---: | ---: | ---: |
| Missing datasets | CRITICAL | 14 | 14 | 0 | 0.0% |
| Unvalidated candidates | CRITICAL | 7 | 8 | 1 | 12.5% |
| Proxy-dependent candidates | CRITICAL | 600 | 600 | 0 | 0.0% |
| Stale observations | HIGH | 47 | 47 | 0 | 0.0% |
| Unresolved adversary findings | HIGH | 150 | 150 | 0 | 0.0% |

Dashboard aggregate:

| Measure | Value |
| --- | ---: |
| Dashboard debt score | 97.5 |
| Dashboard total baseline count | 819 |
| Dashboard total current count | 818 |
| Dashboard total reduced count | 1 |
| Dashboard count reduction progress | 0.1% |

Trend score basis:

| Debt Class | Baseline | Latest | Weight |
| --- | ---: | ---: | ---: |
| Missing Data | 14 | 0 | 0.20 |
| Unvalidated Candidates | 8 | 6 | 0.15 |
| Proxy Dependence | 600 | 600 | 0.20 |
| Unresolved Findings | 150 | 150 | 0.15 |
| Replay Gaps | 372 | 372 | 0.15 |
| Qualification Gaps | 490 | 490 | 0.15 |

Trend aggregate:

| Measure | Value |
| --- | ---: |
| Baseline expanded debt score | 100.0 |
| Latest expanded debt score | 76.3 |
| Expanded score reduction | 23.7 |
| Elapsed time baseline to latest | 1.39 hours |
| Total tracked count reduction | 16 |
| Count reduction rate | 11.5 items/hour |
| Debt growth | 0 tracked items |

## Time Series

| Debt Class | Baseline 2026-06-05T18:47Z | Post 75pct 2026-06-05T19:34Z | Daily Completion 2026-06-05T20:10Z | Latest Count |
| --- | ---: | ---: | ---: | ---: |
| Missing Data | 14 | 6 | 0 | 0 |
| Unvalidated Candidates | 8 | 7 | 6 | 6 |
| Proxy Dependence | 600 | 600 | 600 | 600 |
| Unresolved Findings | 150 | 150 | 150 | 150 |
| Replay Gaps | 372 | 372 | 372 | 372 |
| Qualification Gaps | 490 | 490 | 490 | 490 |

## Impact Attribution

Impact is measured as latest weighted contribution to the expanded debt score:

```text
impact = 100 * weight * latest_count / baseline_count
```

| Rank | Debt Class | Latest Count | Baseline | Weight | Latest Score Contribution | Share Of Latest Expanded Score |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | Proxy Dependence | 600 | 600 | 0.20 | 20.00 | 26.23% |
| 2 | Unresolved Findings | 150 | 150 | 0.15 | 15.00 | 19.67% |
| 3 | Replay Gaps | 372 | 372 | 0.15 | 15.00 | 19.67% |
| 4 | Qualification Gaps | 490 | 490 | 0.15 | 15.00 | 19.67% |
| 5 | Unvalidated Candidates | 6 | 8 | 0.15 | 11.25 | 14.75% |
| 6 | Missing Data | 0 | 14 | 0.20 | 0.00 | 0.00% |

## Persistence Attribution

Persistence is measured across the three trend snapshots.

| Debt Class | Snapshot Counts | Nonzero Snapshots | Nonzero Persistence | Unchanged Intervals | Latest Status |
| --- | --- | ---: | ---: | ---: | --- |
| Missing Data | 14 -> 6 -> 0 | 2/3 | 66.67% | 0/2 | zero latest count |
| Unvalidated Candidates | 8 -> 7 -> 6 | 3/3 | 100.00% | 0/2 | nonzero latest count |
| Proxy Dependence | 600 -> 600 -> 600 | 3/3 | 100.00% | 2/2 | nonzero latest count |
| Unresolved Findings | 150 -> 150 -> 150 | 3/3 | 100.00% | 2/2 | nonzero latest count |
| Replay Gaps | 372 -> 372 -> 372 | 3/3 | 100.00% | 2/2 | nonzero latest count |
| Qualification Gaps | 490 -> 490 -> 490 | 3/3 | 100.00% | 2/2 | nonzero latest count |

## Reduction Rate Attribution

Reduction rate is measured from the trend baseline to latest snapshot over approximately `1.39` hours.

| Debt Class | Baseline Count | Latest Count | Count Reduction | Count Reduction Rate | Score Contribution Reduction | Score Reduction Rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Missing Data | 14 | 0 | 14 | 10.07 items/hour | 20.00 | 14.39 points/hour |
| Unvalidated Candidates | 8 | 6 | 2 | 1.44 items/hour | 3.75 | 2.70 points/hour |
| Proxy Dependence | 600 | 600 | 0 | 0.00 items/hour | 0.00 | 0.00 points/hour |
| Unresolved Findings | 150 | 150 | 0 | 0.00 items/hour | 0.00 | 0.00 points/hour |
| Replay Gaps | 372 | 372 | 0 | 0.00 items/hour | 0.00 | 0.00 points/hour |
| Qualification Gaps | 490 | 490 | 0 | 0.00 items/hour | 0.00 | 0.00 points/hour |

## Most Damaging Debt Categories

Ranking basis: latest expanded score contribution, then persistence, then zero reduction rate.

| Damage Rank | Debt Class | Latest Score Contribution | Nonzero Persistence | Count Reduction Rate | Score Reduction Rate |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | Proxy Dependence | 20.00 | 100.00% | 0.00 items/hour | 0.00 points/hour |
| 2 | Replay Gaps | 15.00 | 100.00% | 0.00 items/hour | 0.00 points/hour |
| 3 | Qualification Gaps | 15.00 | 100.00% | 0.00 items/hour | 0.00 points/hour |
| 4 | Unresolved Findings | 15.00 | 100.00% | 0.00 items/hour | 0.00 points/hour |
| 5 | Unvalidated Candidates | 11.25 | 100.00% | 1.44 items/hour | 2.70 points/hour |
| 6 | Missing Data | 0.00 | 66.67% | 10.07 items/hour | 14.39 points/hour |

Tie handling:

| Tied Score Contribution | Debt Classes | Tie-Break Values |
| ---: | --- | --- |
| 15.00 | Replay Gaps; Qualification Gaps; Unresolved Findings | All have 100.00% nonzero persistence, 0.00 items/hour reduction, and 0.00 points/hour reduction. |

## Dashboard-To-Trend Mapping

| Dashboard Metric | Trend Debt Class | Dashboard Current | Trend Latest | Notes |
| --- | --- | ---: | ---: | --- |
| Missing datasets | Missing Data | 14 | 0 | Dashboard and trend snapshots use different latest measurement points. |
| Unvalidated candidates | Unvalidated Candidates | 7 | 6 | Dashboard and trend snapshots use different latest measurement points. |
| Proxy-dependent candidates | Proxy Dependence | 600 | 600 | Counts match. |
| Unresolved adversary findings | Unresolved Findings | 150 | 150 | Counts match. |
| Stale observations | No expanded trend class | 47 | n/a | Present in dashboard only. |
| Replay Gaps | Replay Gaps | n/a | 372 | Present in expanded trend only. |
| Qualification Gaps | Qualification Gaps | n/a | 490 | Present in expanded trend only. |

## Missing Measurements

| Measurement | Missing Field |
| --- | --- |
| Stale observations trend contribution | No expanded trend weight or time-series row for stale observations. |
| Stale observations persistence | No trend snapshots beyond dashboard baseline/current count. |
| Stale observations reduction rate | Dashboard shows 0 reduced items, but no multi-snapshot trend row. |
| Category-level severity for replay gaps | Expanded trend includes count and weight, not severity. |
| Category-level severity for qualification gaps | Expanded trend includes count and weight, not severity. |

## Authority Boundary

This report is measurement-only. It does not recommend actions and does not modify production, runtime truth, governance, candidate state, replay state, qualification state, paper-forward state, research memory, trading state, broker execution, capital allocation, or position sizing.
