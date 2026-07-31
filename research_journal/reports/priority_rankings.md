# Priority Rankings

Status: `GENERATED_ONLY`

Scope: offline Research Director V0.1 bottleneck selection test. No implementation, production integration, candidate changes, replay changes, qualification changes, paper-forward changes, trading authority, broker execution, capital allocation, or position sizing.

Scoring formula from `research_director_v0_1_scoring_model_001.md`:

```text
research_priority_score =
  0.30 * information_gain_potential
+ 0.20 * search_space_coverage
+ 0.15 * novelty
+ 0.15 * failure_history
+ 0.10 * research_cost_score
+ 0.10 * adversary_routing_value
```

| Rank | Priority | Research Priority Score | Route | Selected As Primary Bottleneck | Justification |
| ---: | --- | ---: | --- | --- | --- |
| 1 | Data Coverage | 0.848 | `REPAIR_UNCERTAINTY` | YES | Direct replay coverage audit shows `6.67%` unique-symbol coverage, `100%` candidate validation block rate, `8/8 INSUFFICIENT_DATA`, and `0` complete validations; resolving this changes validation throughput and evidence quality across the candidate set. |
| 2 | Failure Taxonomy | 0.815 | `REVIEW_FAILURE_PATTERN` | NO | High reuse and high failure-history value, but it classifies and predicts blockers rather than directly removing the current hard validation blocker. |
| 3 | Research Adversary | 0.765 | `DESIGN_EXPERIMENT` | NO | Strong route-clarity and falsification value, but generated-only critique cannot by itself create direct data coverage or complete candidate validation. |
| 4 | Research Economics | 0.728 | `INVESTIGATE` | NO | Clearly quantifies the expected value of missing data and supports targeting `75%` coverage, but it is an analysis layer rather than the bottleneck-removal workstream itself. |
| 5 | Search Space Mapping | 0.710 | `COMPRESS_SEARCH_SPACE` | NO | High long-term coverage and novelty value, but less direct evidence that it would unblock the current validation queue before data coverage is repaired. |

## Factor Scores

| Priority | Information Gain | Search Space Coverage | Novelty | Failure History | Research Cost Score | Adversary Routing Value |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Data Coverage | 1.00 | 0.90 | 0.65 | 1.00 | 0.45 | 0.75 |
| Failure Taxonomy | 0.85 | 0.85 | 0.65 | 0.95 | 0.75 | 0.70 |
| Research Adversary | 0.80 | 0.65 | 0.70 | 0.80 | 0.80 | 0.90 |
| Research Economics | 0.82 | 0.70 | 0.60 | 0.70 | 0.80 | 0.48 |
| Search Space Mapping | 0.70 | 0.95 | 0.85 | 0.45 | 0.75 | 0.40 |

## Result

`PASS`: Research Director V0.1 would have selected Data Coverage as the #1 bottleneck under the proposed model when evaluated against the current Atlas validation evidence.
