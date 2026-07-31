# Candidate Counterfactual Validation 001

Date: 2026-06-05
Status: ANALYSIS_ONLY

## Scope

Determine whether a validation vocabulary bridge would materially change direct candidate validation outcomes.

Requested input status:

- `vocabulary_bridge_simulation_001.md`: not found under `/home/node/constellation`
- `research_journal/reports/candidate_replay_trace_001.md`: used
- direct validation reports: used
- supporting sensitivity input: `research_journal/reports/regime_filter_sensitivity_001.md`
- supporting mapping input: `research_journal/reports/validation_regime_mapping_study_001.md`

Because the named vocabulary bridge simulation file was unavailable, this report treats the existing relaxed regime sensitivity as the bridge-enabled counterfactual:

```text
candidate CHOP compatibility proxy = RANGE_BOUND + LOW_VOLATILITY + UNKNOWN
```

The stricter `RANGE_BOUND`-only bridge is noted separately where it changes interpretation.

This report does not implement a bridge, change replay behavior, change validation behavior, promote candidates, override qualification, override governance, recommend trades, allocate capital, size positions, authorize broker execution, or place paper trades.

## Current Baseline

Direct validation reviewed 8 candidates:

- confirmed: 2
- insufficient data: 6
- affected zero-sample candidates: 5

The affected candidates all had:

- current validation status: `INSUFFICIENT_DATA`
- trigger samples greater than zero
- surviving samples equal to zero
- first fatal stage: `regime_filter`
- candidate regime: `CHOP`

The current affected-candidate block rate is 5/5, or 100 percent.

Across all reviewed candidates, the current insufficient-data block rate is 6/8, or 75 percent.

## Counterfactual Table

| candidate_id | mechanism / regime | current validation status | trigger samples | current surviving samples | bridge-enabled status | bridge-enabled samples | validation delta | impact |
| --- | --- | --- | ---: | ---: | --- | ---: | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | BREAKOUT / CHOP | INSUFFICIENT_DATA | 1049 | 0 | VALIDATION_UNBLOCKED_SAMPLE_SUFFICIENT | 208 | +208 retained samples; zero-sample block removed | HIGH IMPACT |
| `ptc_backtest_final_3a4ac24107c77136` | BREAKOUT / CHOP | INSUFFICIENT_DATA | 1049 | 0 | VALIDATION_UNBLOCKED_SAMPLE_SUFFICIENT | 208 | +208 retained samples; zero-sample block removed | HIGH IMPACT |
| `ptc_backtest_final_d5931b24bd391113` | EVENT_REACTION / CHOP | INSUFFICIENT_DATA | 503 | 0 | PARTIAL_UNBLOCK_TIMEFRAME_STILL_BLOCKED | 75 | +75 retained samples, but intraday requirement remains unresolved | MEDIUM IMPACT |
| `ptc_backtest_final_7d839944a8a4070a` | BREAKOUT / CHOP | INSUFFICIENT_DATA | 176 | 0 | VALIDATION_UNBLOCKED_SAMPLE_SUFFICIENT_LOW_MARGIN | 38 | +38 retained samples; zero-sample block removed with thin margin | MEDIUM IMPACT |
| `ptc_backtest_final_b23c6756bfb3263a` | BREAKOUT / CHOP | INSUFFICIENT_DATA | 626 | 0 | VALIDATION_UNBLOCKED_SAMPLE_SUFFICIENT | 57 | +57 retained samples; zero-sample block removed | MEDIUM IMPACT |

Bridge-enabled status means the candidate would have enough retained samples to proceed past the current zero-sample regime filter in the counterfactual. It does not mean `CONFIRMED`, qualified, approved, or tradable.

## Metrics

Additional candidates validated:

- 4 candidates become validation-unblocked with sample-sufficient daily direct replay pools.
- 1 additional candidate becomes partially unblocked, but remains blocked by the separate intraday-plus-daily confirmation requirement.
- 0 candidates can be counted as newly `CONFIRMED` from this counterfactual alone, because replay metrics were not recomputed.

Additional samples retained:

- current affected surviving samples: 0
- bridge-enabled retained samples: 581
- additional retained samples: +581

Validation block-rate delta:

- affected-candidate zero-sample block rate: 100 percent to 0 percent
- affected-candidate unresolved-validation block rate after accounting for the event-reaction timeframe blocker: 100 percent to 20 percent
- affected-candidate delta with timeframe blocker counted: -80 percentage points
- all-reviewed insufficient-data block rate, if the 4 daily-compatible candidates become validation-unblocked: 75 percent to 25 percent
- all-reviewed delta: -50 percentage points

Candidate evidence improvement:

| candidate_id | evidence improvement |
| --- | --- |
| `ptc_backtest_final_469607b8340421b7` | HIGH: direct evidence moves from no surviving samples to 208 bridge-compatible samples. |
| `ptc_backtest_final_3a4ac24107c77136` | HIGH: direct evidence moves from no surviving samples to 208 bridge-compatible samples. |
| `ptc_backtest_final_d5931b24bd391113` | MEDIUM: sample evidence improves to 75, but timeframe evidence remains insufficient for the stated validation path. |
| `ptc_backtest_final_7d839944a8a4070a` | MEDIUM: sample evidence improves to 38, enough to clear the minimum sample threshold but with limited margin. |
| `ptc_backtest_final_b23c6756bfb3263a` | MEDIUM: sample evidence improves to 57, enough to clear the minimum sample threshold but still materially smaller than the trigger pool. |

## Strict Bridge Sensitivity

If the bridge only mapped `CHOP` to `RANGE_BOUND`, without `LOW_VOLATILITY` or `UNKNOWN`, the retained sample counts would be:

| candidate_id | RANGE_BOUND-only samples | interpretation |
| --- | ---: | --- |
| `ptc_backtest_final_469607b8340421b7` | 146 | still sample-sufficient |
| `ptc_backtest_final_3a4ac24107c77136` | 146 | still sample-sufficient |
| `ptc_backtest_final_d5931b24bd391113` | 49 | sample-sufficient, but timeframe still blocked |
| `ptc_backtest_final_7d839944a8a4070a` | 27 | below the 30-sample minimum |
| `ptc_backtest_final_b23c6756bfb3263a` | 28 | below the 30-sample minimum |

Under the strict bridge, the result is still material but weaker:

- 2 daily-compatible candidates become validation-unblocked.
- 1 event-reaction candidate becomes partially unblocked but remains timeframe-blocked.
- 2 candidates remain below the minimum sample threshold.
- additional retained samples fall from 581 to 400.

## Impact Classification

HIGH IMPACT:

- The bridge materially changes validation outcomes for the two largest affected BREAKOUT / CHOP candidates.
- Each moves from zero surviving samples to 208 relaxed bridge-compatible samples.
- Both would clear the minimum sample threshold by a large margin.

MEDIUM IMPACT:

- Three candidates improve materially but retain caveats.
- The EVENT_REACTION / CHOP candidate remains blocked by intraday validation requirements.
- Two smaller BREAKOUT / CHOP candidates clear the threshold only under the relaxed bridge proxy, not under a strict `RANGE_BOUND`-only bridge.

LOW IMPACT:

- No affected candidate is merely low impact under the relaxed bridge proxy, because every affected candidate gains at least 38 retained samples.

NO IMPACT:

- No affected candidate has zero bridge-enabled sample recovery.

Overall classification: HIGH IMPACT.

## Interpretation

The bridge would matter. The current zero-sample results are not caused by missing trigger opportunities or missing local daily data. They are primarily caused by vocabulary incompatibility: candidate `CHOP` is not emitted by the direct validation daily regime classifier.

A bridge would not prove the candidates are good. It would change the validation question from:

```text
Can the validator produce any direct samples?
```

to:

```text
Do the bridge-compatible direct samples support the candidate after replay metrics are recomputed?
```

That is a meaningful evidence improvement because it moves four daily-compatible candidates from structural blockage into measurable direct validation. It also cleanly separates the event-reaction candidate's vocabulary issue from its separate intraday validation requirement.

## Conclusion

Bridge implementation would be justified as a validation-unblock and evidence-quality improvement, provided it is implemented as an explicit, auditable compatibility layer rather than as a silent regime-filter relaxation.

The justified scope is narrow:

- report original candidate regime and translated validation regime separately
- classify unmappable labels explicitly
- preserve `CHOP` as source vocabulary
- treat bridge output as compatibility metadata, not truth
- require replay metrics before any candidate support classification changes
- keep intraday-required candidates blocked until an intraday validation path exists

The bridge is justified for validation measurement. It is not justified as a readiness, qualification, governance, or trading authority mechanism.

## Authority Boundary

This report is analysis only. It does not implement vocabulary bridging, modify Aegis behavior, modify manifests, alter verified runtime truth, promote candidates, override replay, override qualification, override governance, recommend trades, allocate capital, size positions, authorize broker execution, or place paper trades.
