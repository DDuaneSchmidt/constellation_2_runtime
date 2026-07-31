# Meta-Research Baseline 001

Date: 2026-06-05

Status: non-authoritative baseline

Mode: read-only analysis

## Boundary

This baseline uses only `reports/meta_research_dataset/`.

No learning system is created.
No production integration is created.
No candidate state is changed.
No replay state is changed.
No qualification state is changed.
No governance state is changed.
No recommendation is made.
No trading, broker, capital, position sizing, portfolio, or paper-placement authority is created.

The source dataset manifest is `DATA_COLLECTION_ONLY` and declares `NO_CONCLUSIONS`, `NO_LEARNING`, and `NO_RECOMMENDATIONS`. This report describes measured historical patterns only.

## Dataset Coverage

| History | Records | Source Files | Field Completeness |
| --- | ---: | ---: | ---: |
| observations | 7,238 | 105 | 75.27% |
| claims | 331 | 230 | 37.25% |
| hypotheses | 1,075 | 1,063 | 71.15% |
| candidates | 713 | 38 | 50.48% |
| failures | 63 | 24 | 51.47% |
| bottlenecks | 61 | 27 | 38.93% |
| research debt | 27 | 6 | 62.96% |
| adversary records | 311 | 311 | 31.43% |

Important measurement limits:

- claim IDs are absent in all 331 claim records;
- candidate validation state is absent in all 713 candidate records;
- bottleneck stage is absent in all 61 bottleneck records;
- explicit claim-to-hypothesis and hypothesis-to-candidate lineage fields are not present in the normalized dataset;
- candidate records contain duplicates and sample rows, so unique candidate counts are more conservative than raw candidate record counts.

## Funnel Baseline

| Funnel Step | Numerator | Denominator | Measured Ratio | Measurement Basis |
| --- | ---: | ---: | ---: | --- |
| observation -> claim | 331 claims | 7,238 observations | 4.57% | raw records |
| observation -> unique claim text | 50 unique claim texts | 7,238 observations | 0.69% | deduplicated claim text |
| claim -> hypothesis | 1,075 hypotheses | 331 claims | 324.77% | raw records; indicates fanout, not one-to-one conversion |
| unique claim text -> hypothesis | 1,075 hypotheses | 50 unique claim texts | 21.50x | deduplicated claim text proxy |
| hypothesis -> unique candidate | 165 unique candidates | 1,075 hypotheses | 15.35% | unique `candidate_id` proxy |
| candidate -> exact confirmed | 2 exact `CONFIRMED` candidates | 165 unique candidates | 1.21% | exact status only |
| candidate -> confirmed-like support | 11 supported or confirmed-like candidates | 165 unique candidates | 6.67% | `CONFIRMED`, `BACKTEST_SUPPORTED`, or observation-ready support |

The measured funnel is not a strict lineage chain. It is a dataset-level conversion proxy because normalized source records do not retain enough explicit source IDs for exact item-by-item conversion accounting.

## Observation To Claim Conversion

Observed records:

- observations: 7,238
- claims: 331
- claim records with claim text: 200
- unique claim texts: 50

Measured pattern:

- raw claims are 4.57% of raw observations;
- unique claim texts are 0.69% of raw observations;
- claim text repeats across artifacts, producing 200 claim-text-bearing rows but only 50 unique claim texts;
- normalized `source_observation_ids` are absent from claim records, so exact observation-level conversion cannot be measured from this dataset.

Top observation mechanisms:

| Mechanism | Observation Records |
| --- | ---: |
| BREAKOUT | 752 |
| MEAN_REVERSION | 694 |
| EVENT_REACTION | 684 |
| LIQUIDITY_SWEEP | 683 |
| REVERSAL | 677 |
| VWAP_OR_AVERAGE_RECLAIM | 673 |
| TREND_CONTINUATION | 669 |
| VOLATILITY_EXPANSION | 669 |
| SESSION_TIMING | 668 |
| OPENING_RANGE | 668 |

Parsed claim mechanisms are evenly distributed across the same 10 mechanism families, with 20 records each among claim texts that follow the mechanism-pattern format.

## Claim To Hypothesis Conversion

Observed records:

- claims: 331
- unique claim texts: 50
- hypotheses: 1,075
- unique hypothesis IDs: 1,051

Measured pattern:

- the raw claim-to-hypothesis ratio is 3.25 hypotheses per claim record;
- the deduplicated claim-text proxy is 21.5 hypotheses per unique claim text;
- each of the 10 main mechanisms appears in 105 hypothesis records;
- normalized `source_artifact_ids` are absent from hypothesis records, so exact claim-level conversion cannot be measured from this dataset.

Top hypothesis mechanisms:

| Mechanism | Hypothesis Records |
| --- | ---: |
| VWAP_OR_AVERAGE_RECLAIM | 105 |
| MEAN_REVERSION | 105 |
| LIQUIDITY_SWEEP | 105 |
| TREND_CONTINUATION | 105 |
| VOLATILITY_EXPANSION | 105 |
| BREAKOUT | 105 |
| REVERSAL | 105 |
| OPENING_RANGE | 105 |
| EVENT_REACTION | 105 |
| SESSION_TIMING | 105 |

Hypothesis regimes are more fragmented than observation regimes. The largest hypothesis regime bucket is `OPENING_SESSION` with 101 records, followed by `TRENDING` with 77, `COMPRESSED_VOLATILITY` with 68, `RANGE_BOUND` with 68, and `EVENT_WINDOW` with 67.

## Hypothesis To Candidate Conversion

Observed records:

- hypotheses: 1,075
- unique hypothesis IDs: 1,051
- candidate records: 713
- candidate records with candidate ID: 566
- unique candidate IDs: 165

Measured pattern:

- raw candidate records are 66.33% of hypothesis records;
- unique candidates are 15.35% of hypothesis records;
- candidate records are duplicated across reports, sample rows, family summaries, rankings, reviews, and validation artifacts;
- normalized `source_hypothesis_id` is absent from candidate records, so exact hypothesis-level conversion cannot be measured from this dataset.

Unique candidate mechanisms by majority assignment:

| Mechanism | Unique Candidates |
| --- | ---: |
| MEAN_REVERSION | 25 |
| VWAP_OR_AVERAGE_RECLAIM | 24 |
| BREAKOUT | 21 |
| EVENT_REACTION | 20 |
| REVERSAL | 20 |
| LIQUIDITY_SWEEP | 19 |
| SESSION_TIMING | 13 |
| OPENING_RANGE | 10 |
| TREND_CONTINUATION | 9 |
| VOLATILITY_EXPANSION | 3 |

Unique candidate regimes by majority assignment:

| Regime | Unique Candidates |
| --- | ---: |
| TRENDING | 55 |
| CHOP | 41 |
| UNKNOWN | 18 |
| HIGH_VOLATILITY | 15 |
| LOW_VOLATILITY | 12 |
| RANGE_BOUND | 11 |

## Candidate To Confirmed Conversion

Observed records:

- unique candidates: 165
- exact `CONFIRMED` candidates: 2
- confirmed-like or support-bearing candidates: 11

Exact confirmed candidates:

- `ptc_backtest_final_4df2e8e80685a054`
- `ptc_backtest_final_854ad10b904e1ae9`

Candidate statuses observed across unique candidates:

| Status | Unique Candidate Count |
| --- | ---: |
| READY_FOR_PAPER_FORWARD_OBSERVATION | 58 |
| REJECT_FOR_NOW | 56 |
| supported | 20 |
| REQUEST_CLARIFICATION | 10 |
| NEEDS_CLARIFICATION | 10 |
| BACKTEST_SUPPORTED | 8 |
| INSUFFICIENT_DATA | 6 |
| NEEDS_DATA_IMPROVEMENT | 6 |
| BACKTEST_WEAK | 4 |
| CONFIRMED | 2 |

Measured pattern:

- exact confirmed conversion is 1.21% of unique candidates;
- broader support-bearing conversion is 6.67% of unique candidates;
- most candidate records stop at observation readiness, rejection, clarification, backtest support, or insufficient-data states rather than exact confirmation.

## Failure Frequencies

Observed failure records:

- total failure records: 63
- unique failure IDs: 31
- statuses present: 13 `OPEN`, 1 `GENERATED_ONLY`
- severities present: 10 `ERROR`, 7 `CERTIFICATION_BLOCK`

Frequency proxy by text-pattern category:

| Failure Pattern Category | Matching Records |
| --- | ---: |
| sample or evidence insufficiency | 26 |
| certification or governance block | 13 |
| candidate quality or duplication | 2 |
| artifact or lineage gap | 2 |
| worker or execution compatibility | 1 |
| proxy or timeframe mismatch | 1 |
| uncategorized sparse/document record | 22 |

The failure-history dataset is partly sparse. Twenty-two records did not carry enough normalized text to assign one of the measured pattern categories.

## Bottleneck Frequencies

Observed bottleneck records:

- total bottleneck records: 61
- records with candidate ID: 40
- records with blocker text: 28
- records with stage: 0

Frequency proxy by text-pattern category:

| Bottleneck Pattern Category | Matching Records |
| --- | ---: |
| sample or evidence insufficiency | 61 |
| artifact or lineage gap | 61 |
| data coverage or readiness | 24 |
| regime or vocabulary mismatch | 17 |
| proxy or timeframe mismatch | 12 |
| certification or governance block | 1 |

The sample/evidence and artifact/lineage counts cover all bottleneck records because the sparse normalized records carry missing-field and source-artifact context. More specific bottleneck types are concentrated in data coverage, regime vocabulary, and daily/intraday mismatch artifacts.

Common bottleneck source artifacts:

| Source Artifact / Topic | Records |
| --- | ---: |
| `market_data_coverage_report.json` | 8 |
| `latest_coverage.json` | 8 |
| `market_data_readiness_report.json` | 8 |
| `regime_filter_root_cause_table.csv` | 5 |
| `insufficient_data_root_cause_table.csv` | 7 |
| `zero_sample_root_cause_table.csv` | 4 |

## Research Debt Contributors

Observed research debt records:

- total research debt records: 27
- records with category: 18
- records with severity: 23
- records with priority: 18

Research debt categories:

| Category | Records |
| --- | ---: |
| Missing Market Data | 2 |
| Unvalidated Candidates | 2 |
| Qualification Gaps | 2 |
| Replay Coverage Gaps | 2 |
| Duplicate Candidate Clusters | 2 |
| Unresolved Hypotheses | 2 |
| Stale Observations | 2 |
| Unresolved Adversary Findings | 2 |
| Failure Patterns Not Yet Encoded | 2 |

Research debt severity:

| Severity | Records |
| --- | ---: |
| HIGH | 11 |
| CRITICAL | 6 |
| MEDIUM | 5 |
| LOW | 1 |

Measured pattern:

- debt categories are evenly represented in the normalized register;
- high and critical severity records dominate the severity-bearing subset;
- owner and status fields are absent across all normalized debt records.

## Top 10 Historical Research Patterns

1. Observation volume is large relative to claim formation.
   - 7,238 observation records produced 331 claim records and 50 unique claim texts.

2. Claim formation is highly deduplicated.
   - 200 claim-text-bearing rows collapse to 50 unique claim texts.

3. Hypothesis generation fans out from claim families.
   - 1,075 hypothesis records appear against 331 claim records and 50 unique claim texts.

4. Hypotheses are mechanically balanced across the 10 main mechanism families.
   - Each main mechanism appears in 105 hypothesis records.

5. Candidate formation compresses hypothesis volume sharply.
   - 1,075 hypothesis records compress to 165 unique candidate IDs.

6. Candidate confirmation is rare.
   - 2 of 165 unique candidates carry exact `CONFIRMED` status.

7. Candidate evidence often stops before confirmation.
   - Common unique-candidate statuses include `READY_FOR_PAPER_FORWARD_OBSERVATION`, `REJECT_FOR_NOW`, `supported`, `BACKTEST_SUPPORTED`, `INSUFFICIENT_DATA`, and clarification states.

8. Regime vocabulary fragments between stages.
   - Observations cluster around `CHOP`, `HIGH_VOLATILITY`, `TRENDING`, `LOW_VOLATILITY`, and `UNKNOWN`; hypotheses use many more specific regimes such as `OPENING_SESSION`, `COMPRESSED_VOLATILITY`, `RANGE_BOUND`, and `EVENT_WINDOW`; candidates concentrate in `TRENDING`, `CHOP`, and `UNKNOWN`.

9. Evidence/sample insufficiency is the most visible failure and bottleneck pattern.
   - It appears in 26 failure records and all 61 bottleneck records by text-pattern proxy.

10. Data readiness, replay coverage, duplicate clusters, unresolved hypotheses, stale observations, and unresolved adversary findings are recurring research-debt contributors.
   - Each appears as a category in the normalized debt register, while HIGH and CRITICAL severities dominate severity-bearing debt records.

## Baseline Interpretation Limits

This report measures historical dataset patterns, not causal effects.

The conversion ratios are proxies, not strict lineage measurements, because normalized source records do not preserve enough IDs for exact observation-to-claim, claim-to-hypothesis, or hypothesis-to-candidate tracing.

The report does not recommend actions, remediation, integration, prioritization, candidate changes, replay changes, qualification changes, governance changes, or learning-system behavior.
