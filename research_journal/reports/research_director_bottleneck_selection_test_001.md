# Research Director Bottleneck Selection Test 001

Status: `GENERATED_ONLY`

Date: 2026-06-05

## Question

Would the proposed Research Director design have correctly identified Data Coverage as Atlas's primary bottleneck?

## Result

`PASS`

Research Director V0.1 would have selected Data Coverage as the #1 bottleneck when the scoring model is applied to the historical Atlas priority set and the current direct replay coverage evidence.

## Authority Boundary

- No implementation.
- No production changes.
- No replay, qualification, candidate, governance, paper-forward, or memory changes.
- No candidate promotion or demotion.
- No trade recommendations.
- No live trading, broker execution, capital allocation, position sizing, portfolio construction, or automatic paper placement.
- This report is an offline retrospective scoring artifact only.

## Inputs Reviewed

- `research_journal/design/research_director_v0_1_scoring_model_001.md`
- `research_journal/design/research_director_v0_1_architecture_001.md`
- `research_journal/reports/research_director_retrospective_001.md`
- `reports/atlas_v2_research_os/direct_replay_coverage_audit_001.md`
- `research_journal/reports/candidate_validation_economics_001.md`

## Method

The test uses the Research Director V0.1 `ResearchPriorityRanking` formula:

```text
research_priority_score =
  0.30 * information_gain_potential
+ 0.20 * search_space_coverage
+ 0.15 * novelty
+ 0.15 * failure_history
+ 0.10 * research_cost_score
+ 0.10 * adversary_routing_value
```

Scores are qualitative 0.00-1.00 retrospective estimates. Higher `research_cost_score` means cheaper or more immediately reducible. The scoring target is research-process value, not candidate quality, trading value, or investment value.

## Historical Priorities Evaluated

- Research Adversary
- Failure Taxonomy
- Research Economics
- Search Space Mapping
- Data Coverage

## Priority Ranking

| Rank | Priority | Research Priority Score | Route | Bottleneck Selection | Main Reason |
| ---: | --- | ---: | --- | --- | --- |
| 1 | Data Coverage | 0.848 | `REPAIR_UNCERTAINTY` | Selected | Current validation is blocked by missing candidate-specific data: `6.67%` unique-symbol coverage, `100%` candidate validation block rate, `8/8 INSUFFICIENT_DATA`, and `0` complete validations. |
| 2 | Failure Taxonomy | 0.815 | `REVIEW_FAILURE_PATTERN` | Not selected | Strong failure-history and reuse value, but it classifies blockers rather than directly unblocking validation evidence. |
| 3 | Research Adversary | 0.765 | `DESIGN_EXPERIMENT` | Not selected | Strong critique and falsification-routing value, but cannot resolve missing direct replay data. |
| 4 | Research Economics | 0.728 | `INVESTIGATE` | Not selected | Shows missing data has high expected value through `75%` coverage, but is an analysis layer rather than the repair itself. |
| 5 | Search Space Mapping | 0.710 | `COMPRESS_SEARCH_SPACE` | Not selected | High long-term search coverage value, but weaker near-term bottleneck removal for the active validation queue. |

Detailed ranking outputs:

- `research_journal/reports/priority_rankings.csv`
- `research_journal/reports/priority_rankings.md`

## Why Data Coverage Ranked First

Data Coverage scored highest because it is the only evaluated priority that directly removes a hard validation blocker. The coverage audit shows:

- `candidate_count`: `8`
- `candidate_symbol_count`: `15`
- `symbols_with_data`: `1`
- `symbols_missing_data`: `14`
- `coverage_percent`: `6.67%`
- `candidate_validation_block_rate`: `100.00%`
- validation classifications: `8 INSUFFICIENT_DATA`
- full candidate universe coverage: `0`
- partial candidate universe coverage: `2`
- no candidate universe coverage: `6`

The candidate validation economics report independently supports the same conclusion: current coverage is insufficient for complete validation, `50%` coverage is the first useful decision threshold, and `75%` coverage is the strongest economic target before treating the candidate set as a clean validation queue.

## Why Other Priorities Did Not Rank First

### Research Adversary

Research Adversary would have helped identify assumptions, constraints, null explanations, and falsification tests. It scores well on adversary routing value and research cost. It does not rank first because generated-only critique cannot run direct replay, add missing symbols, or convert proxy evidence into candidate-specific validation.

### Failure Taxonomy

Failure Taxonomy is the closest competitor. It has high failure-history value and strong search-space compression. It does not rank first in this test because Atlas's observed active bottleneck is not merely that failures are poorly categorized; it is that validation cannot complete due to missing data. Taxonomy would help name `DATA_SOURCE_GAP`, `PROXY_DEPENDENCY`, and related blockers, but Data Coverage repairs the blocker.

### Research Economics

Research Economics correctly quantifies the value of missing data and would support the Data Coverage decision. It does not rank first because it is diagnostic. The bottleneck-removal action is coverage repair, not another expected-value report.

### Search Space Mapping

Search Space Mapping has the highest long-term portfolio upside and high search-space coverage value. It does not rank first here because it would not unblock the current candidate validation queue before the missing direct data problem is addressed.

## Scoring Interpretation

Research Director would have selected Data Coverage because the scoring model rewards:

- high information gain from resolving a decision-changing uncertainty
- broad downstream search-space and validation impact
- direct match to repeated failure patterns such as data gaps and proxy dependence
- clear route classification as `REPAIR_UNCERTAINTY`
- evidence that more generated research would not solve the active blocker

The main penalty against Data Coverage is research cost: acquiring and organizing missing data is more expensive than writing a taxonomy, adversary review, or economics report. The penalty is not large enough to overcome the hard-blocker evidence.

## PASS Justification

This test passes because the Research Director model would have elevated Data Coverage above attractive but less directly blocking work. The decisive evidence is that Atlas currently has enough candidate interest to need validation, but not enough direct data coverage to complete validation or interpret proxy support.

A reasonable Research Director recommendation would have been:

1. Select Data Coverage as the primary bottleneck.
2. Route it as `REPAIR_UNCERTAINTY`.
3. Use Research Economics to set the target threshold, with `75%` coverage as the practical research-economics goal.
4. Use Failure Taxonomy and Research Adversary as supporting layers after the data bottleneck is named and routed.
5. Defer broad Search Space Mapping until validation evidence is trustworthy enough to measure which explored regions actually produce survivors.

## Caveats

- This is a retrospective qualitative score, not an implemented Research Director run.
- Hindsight may make the data bottleneck look more obvious than it would have been earlier.
- Failure Taxonomy remains a near-tie contender and could rank first in a period dominated by conceptual failure rather than validation blockage.
- The result depends on treating hard validation blockers as higher priority than generated critique or portfolio mapping when the active queue cannot validate.

## Final Answer

`PASS`: Research Director V0.1 would have selected Data Coverage as Atlas's #1 bottleneck under the proposed model and the reviewed evidence.
