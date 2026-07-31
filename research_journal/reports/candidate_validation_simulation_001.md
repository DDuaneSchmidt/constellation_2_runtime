# Candidate Validation Simulation 001

Date: 2026-06-05

Status: Analysis only

Scope: translates direct replay coverage levels into expected Atlas research value using the current candidate validation queue, direct replay coverage audit, and candidate validation economics report. This artifact is not implementation. It does not download data, integrate data providers, modify replay, modify qualification, create candidates, promote candidates, approve paper-forward activity, recommend trades, allocate capital, size positions, authorize broker execution, or change governance state.

## Inputs

- `research_journal/reports/candidate_validation_economics_001.md`
- `reports/atlas_v2_research_os/direct_replay_coverage_audit_001.md`
- `reports/atlas_v2_research_os/candidate_validation_queue_001.md`
- `research_journal/reports/candidate_coverage_unlock_analysis_001.md`

## Baseline

Current direct replay coverage is constrained by missing candidate-specific symbol data.

Observed baseline:

- candidates reviewed: 8
- unique attributed candidate symbols: 15
- symbols with local data: 1, SPY
- unique-symbol coverage: 6.67%
- candidates with partial local universe coverage: 2
- candidates with full local universe coverage: 0
- candidates classified as `INSUFFICIENT_DATA`: 8
- direct validation block rate: 100%
- focused-set proxy replay sample-equivalents: 1,215

Important denominator note: `candidate_validation_economics_001.md` uses a candidate-count coverage framing where 25% means 2 of 8 candidates have direct replay availability. The coverage audit uses unique-symbol coverage, where current coverage is 1 of 15 symbols, or 6.67%. This simulation uses the unique-symbol coverage denominator because the user-requested current level is 6.67%.

## Model

Coverage levels are modeled as high-impact symbol coverage, not arbitrary symbol coverage. Symbols are added in the validation-impact order already identified by the coverage unlock analysis:

```text
SPY, DIA, QQQ, TSLA, META, BAC, MSFT, TLT, USO, AMZN, NFLX, DBC, AAPL, JPM, GOOGL
```

Definitions:

- `candidates partially validated`: candidate has at least one covered missing symbol from its attributed universe; current SPY-only coverage is counted as partial coverage for two candidates but not as useful direct validation.
- `candidates fully validated`: all currently missing direct symbols for that candidate are covered, before any later sample-size, event-timing, or human-review judgment.
- `expected false positives removed`: estimated research false positives exposed by direct evidence; this is not a trading outcome estimate.
- `expected replay support improvement`: expected share of proxy replay evidence converted into more truthful candidate-specific replay evidence.
- `expected paper-forward risk reduction`: expected reduction in human-review and observation waste from filtering proxy artifacts before paper-forward observation.
- `expected research hours saved`: estimated human review hours avoided through earlier rejection, clearer prioritization, and less manual proxy interpretation.
- `expected uncertainty reduction`: estimated reduction in validation ambiguity from converting proxy evidence into direct replay evidence.

## Simulation Matrix

| Coverage Level | Modeled Symbol Coverage | Assumed Covered Symbols | Candidates Partially Validated | Candidates Fully Validated | Expected False Positives Removed | Expected Replay Support Improvement | Expected Paper-Forward Risk Reduction | Expected Research Hours Saved | Expected Uncertainty Reduction |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Current 6.67% | 1/15 | SPY | 2 | 0 | 0.0 | 0.0% | 0.0% | 0.0 | 0.0% |
| 25% | 4/15 | SPY; DIA; QQQ; TSLA | 6 | 3 | 1.3 | 34.0% | 36.8% | 10.3 | 33.6% |
| 50% | 8/15 | SPY; DIA; QQQ; TSLA; META; BAC; MSFT; TLT | 8 | 3 | 1.5 | 47.5% | 45.8% | 12.1 | 45.7% |
| 75% | 12/15 | SPY; DIA; QQQ; TSLA; META; BAC; MSFT; TLT; USO; AMZN; NFLX; DBC | 8 | 6 | 2.3 | 74.0% | 62.3% | 16.9 | 72.6% |
| 90% | 14/15 | SPY; DIA; QQQ; TSLA; META; BAC; MSFT; TLT; USO; AMZN; NFLX; DBC; AAPL; JPM | 8 | 7 | 2.5 | 94.8% | 70.2% | 18.6 | 94.4% |
| 100% | 15/15 | SPY; DIA; QQQ; TSLA; META; BAC; MSFT; TLT; USO; AMZN; NFLX; DBC; AAPL; JPM; GOOGL | 8 | 8 | 2.8 | 100.0% | 75.0% | 20.2 | 100.0% |

The companion files are:

- `research_journal/reports/simulation_matrix.csv`
- `research_journal/reports/simulation_matrix.md`

## Coverage Level Findings

### Current 6.67%

Atlas has SPY local coverage, but the queue remains blocked:

- 2 candidates have partial universe coverage.
- 0 candidates have full local universe coverage.
- direct replay produced zero useful post-filter samples for the partially covered cases.
- the evidence base remains proxy-dependent.

Research value: low. Current coverage can identify the existence of the data problem, but it does not provide meaningful validation feedback.

### 25%

The 25% case assumes coverage of SPY, DIA, QQQ, and TSLA.

Expected value:

- 6 of 8 candidates become partially direct-testable.
- 3 of 8 candidates become fully covered for direct replay.
- replay support improvement reaches 34.0%.
- paper-forward risk reduction reaches 36.8%.
- expected research hours saved reaches 10.3.

Research value: meaningful. This is the first level where Atlas can compare proxy support against complete direct symbol coverage for a cluster of candidates.

### 50%

The 50% case assumes coverage of SPY, DIA, QQQ, TSLA, META, BAC, MSFT, and TLT.

Expected value:

- all 8 candidates become partially direct-testable.
- fully covered candidates remain at 3 because the single-stock and ETF clusters still need completion symbols.
- replay support improvement reaches 47.5%.
- paper-forward risk reduction reaches 45.8%.

Research value: useful breadth. Atlas can inspect every candidate against at least some direct evidence, but still cannot complete most candidate-specific universes.

### 75%

The 75% case assumes coverage of SPY, DIA, QQQ, TSLA, META, BAC, MSFT, TLT, USO, AMZN, NFLX, and DBC.

Expected value:

- all 8 candidates remain partially direct-testable.
- 6 of 8 candidates become fully covered.
- replay support improvement reaches 74.0%.
- paper-forward risk reduction reaches 62.3%.
- expected uncertainty reduction reaches 72.6%.

Research value: strongest operating target. This level converts most of the candidate queue from proxy-dependent to directly testable while avoiding the lower marginal return of the last few symbols.

### 90%

The 90% case assumes coverage of every symbol except GOOGL.

Expected value:

- all 8 candidates are partially direct-testable.
- 7 of 8 candidates are fully covered.
- replay support improvement reaches 94.8%.
- uncertainty reduction reaches 94.4%.

Research value: quality target. This is near-complete validation coverage, but the marginal research value over 75% is smaller than the earlier jump.

### 100%

The 100% case assumes all 15 attributed symbols are covered.

Expected value:

- all 8 candidates are partially direct-testable.
- all 8 candidates are fully covered.
- replay support improvement reaches 100.0%.
- uncertainty reduction reaches 100.0%.

Research value: complete focused-set coverage. This is useful for audit completeness, but not required before Atlas starts receiving meaningful validation feedback.

## Best ROI Coverage Threshold

Best first-dollar ROI threshold: 25% unique-symbol coverage.

This threshold is driven by the DIA/QQQ pair. Once DIA and QQQ are covered alongside existing SPY coverage, the top three DIA/QQQ candidates can move from proxy-dependent to fully covered direct replay cases. Adding TSLA at the same modeled 25% level broadens partial direct evidence into the single-stock cluster.

Best operating ROI threshold: 75% unique-symbol coverage.

This threshold is the best practical target for Atlas research operations because it fully covers 6 of 8 candidates, raises replay support improvement to 74.0%, and reduces uncertainty by 72.6%. Moving from 75% to 90% improves completeness, but the queue has already crossed from proxy-heavy to mostly direct-testable.

## Meaningful Validation Feedback

Atlas begins receiving meaningful validation feedback at 25% unique-symbol coverage, provided the covered set includes DIA and QQQ.

The reason is concrete: DIA and QQQ complete direct symbol coverage for three candidates, including the top two priorities in the validation queue. Current 6.67% coverage does not meet this bar because SPY-only coverage leaves all 8 candidates `INSUFFICIENT_DATA` and does not produce useful post-filter direct samples.

The stronger operational threshold is 75% unique-symbol coverage. At that point, meaningful feedback becomes broad rather than cluster-specific.

## Risks And Caveats

- The estimates assume high-impact symbol ordering. A random 25% symbol set would not produce the same value.
- Direct symbol coverage does not guarantee candidate support. It only makes candidate-specific replay evidence measurable.
- Event-reaction candidates may still require event timestamp and calendar evidence after symbol OHLCV coverage exists.
- Intraday validation needs can remain unresolved even if daily replay data exists.
- False-positive removal is a research-quality benefit; fewer candidates passing direct replay can be a successful outcome.
- Research-hours-saved estimates are planning values, not measured operational data.

## Authority Boundary

This simulation is research planning only. It does not authorize data acquisition, production integration, replay changes, qualification changes, governance changes, candidate promotion, paper-forward placement, trade recommendations, broker execution, capital allocation, or position sizing.
