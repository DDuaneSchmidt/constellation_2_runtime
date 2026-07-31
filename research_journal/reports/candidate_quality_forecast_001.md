# Candidate Quality Forecast 001

Date: 2026-06-05
Status: GENERATED_ONLY
Scope: evaluation forecast only; no implementation changes and no workflow authority.

## Question

What scorecard outcomes are likely if the current validation limitations are removed?

The forecast uses the focused direct-validation cohort from `candidate_quality_scoreboard_002.md`: 8 candidates, 2 confirmed, 0 weakened, and 6 blocked by insufficient data or validation limitations.

## Baseline

| Metric | Current |
|---|---:|
| Focused candidates | 8 |
| Confirmed candidates | 2 |
| Blocked candidates | 6 |
| Weakened / rejected candidates | 0 |
| Confirmed rate | 25.0% |
| Blocked rate | 75.0% |
| Validation resolution rate | 25.0% |

Interpretation: Atlas has evidence of real candidate quality, but the current scorecard is still dominated by validation blockage rather than decisive confirmation or rejection.

## Forecast Method

This is a constrained forecast, not a new validation run. Estimates are based on:

- current direct-validation outcomes,
- vocabulary bridge effectiveness,
- intraday/event evidence readiness,
- candidate-quality decision-gate thresholds,
- whether each limitation removes a known bottleneck or only reduces generic friction.

`Blocked` means still unevaluable or materially insufficient, not rejected. A candidate can leave the blocked set by becoming confirmed, weakened, or a true rejection.

## Scenario Forecast

| Scenario | Limitation Removed | Confirmed Candidates | Blocked Candidates | Other Resolved Outcomes | Candidate Quality Improvement |
|---|---|---:|---:|---:|---|
| Current | None | 2 | 6 | 0 | Baseline |
| A | Sample-size limitations resolved | 2-3 | 5-6 | 0-1 | Low to modest |
| B | Vocabulary bridge active | 4 | 3 | 1 | Medium |
| C | Intraday evidence added | 4-5 | 2-3 | 1 | High |
| D | All validation limitations resolved | 5-6 | 0 | 2-3 | Very high |

## Scenario A: Sample-Size Limitations Resolved

Expected confirmed candidates: 2-3  
Expected blocked candidates: 5-6  
Expected candidate quality improvement: low to modest

Resolving sample size helps candidates that already have the right vocabulary, market data, and replay shape but lack enough observations to satisfy confidence thresholds. It does not by itself fix zero-sample CHOP candidates, intraday-dependent candidates, or event-required candidates.

The likely result is a small improvement in validation resolution, with one borderline candidate possibly moving from blocked into weakened, rejected, or confirmed status. The confirmed count probably remains close to the current 2 unless the added samples are concentrated in already-promising setups.

## Scenario B: Vocabulary Bridge Active

Expected confirmed candidates: 4  
Expected blocked candidates: 3  
Expected other resolved outcomes: 1 weakened or weakly evaluable  
Expected candidate quality improvement: medium

The vocabulary bridge is the clearest already-measured unlock. The bridge analysis showed that mapping `CHOP` into a replay-compatible `RANGE_BOUND` regime produced 396 additional samples, improved 5 affected candidates, and added 2 confirmed outcomes.

This would raise the focused cohort from 2 confirmed to about 4 confirmed, reduce blocked candidates from 6 to about 3, and create at least one additional non-blocked candidate whose evidence is weak rather than absent. This is a quality improvement, not just infrastructure cleanup, because it converts previously untestable hypotheses into candidate-specific validation outcomes.

Limitation: false-positive risk remains medium. Vocabulary bridging improves evaluability, but it can also blur regime semantics unless the bridge is validated against downstream replay behavior.

## Scenario C: Intraday Evidence Added

Expected confirmed candidates: 4-5  
Expected blocked candidates: 2-3  
Expected other resolved outcomes: 1 weakened or rejected  
Expected candidate quality improvement: high

The intraday readiness pack identified 4 high-value intraday candidates and 1 event-required candidate. Intraday evidence is likely to produce larger quality gains than sample-size expansion because several blocked candidates depend on subdaily timing rather than more daily bars.

The likely scorecard effect is 2-3 candidates leaving the blocked set. Some should confirm, while at least one may weaken or reject once the correct evidence resolution is applied. That still improves anomaly quality because decisive rejection is better than persistent insufficient-data status.

This scenario does not fully clear the event-required candidate unless event metadata is also added. It also does not automatically solve vocabulary mismatch unless intraday evidence is combined with the bridge.

## Scenario D: All Validation Limitations Resolved

Expected confirmed candidates: 5-6  
Expected blocked candidates: 0  
Expected other resolved outcomes: 2-3 weakened or true rejections  
Expected candidate quality improvement: very high

If sample size, vocabulary mismatch, intraday evidence, event metadata, and proxy dependence are all resolved, the focused cohort should no longer be dominated by blocked outcomes. The likely endpoint is not that all 8 candidates confirm. The more credible outcome is that 5-6 confirm and 2-3 become weakened or true rejections.

That would be the strongest evidence that Atlas is improving anomaly quality rather than only reducing infrastructure friction: every focused candidate would receive a candidate-specific validation outcome, and the scorecard would contain both confirmations and falsifications.

## Quality Index

Validation resolution rate is defined as:

`(confirmed + weakened + true rejections) / focused candidates`

| Scenario | Confirmed Rate | Blocked Rate | Validation Resolution Rate | Improvement vs Current |
|---|---:|---:|---:|---:|
| Current | 25.0% | 75.0% | 25.0% | Baseline |
| A | 25.0-37.5% | 62.5-75.0% | 25.0-37.5% | +0.0 to +12.5 pp |
| B | 50.0% | 37.5% | 62.5% | +37.5 pp resolution |
| C | 50.0-62.5% | 25.0-37.5% | 62.5-75.0% | +37.5 to +50.0 pp resolution |
| D | 62.5-75.0% | 0.0% | 100.0% | +75.0 pp resolution |

## Answer

Atlas is likely to show real candidate-quality improvement if the current validation limitations are removed, but the improvement will be uneven by bottleneck.

Sample-size repair alone is unlikely to move the scorecard much. Vocabulary bridging and intraday evidence are the highest-yield unlocks because they address the specific reasons candidate replay collapses: incompatible regime vocabulary and missing evidence resolution. Resolving all limitations should convert the focused cohort from a mostly blocked scorecard into a fully resolved scorecard with roughly 5-6 confirmations and 2-3 weakened or rejected candidates.

The expected end state is healthier anomaly quality, not merely lower infrastructure friction, only if blocked candidates become candidate-specific confirmations or falsifications. A rising confirmed count without a falling blocked count would remain ambiguous.

## Forecast Output

Status: GENERATED_ONLY

Likely outcome if all validation limitations are removed:

- Confirmed candidates: 5-6 of 8 focused candidates
- Blocked candidates: 0 of 8 focused candidates
- Candidate quality improvement: very high
- Primary drivers: vocabulary bridge, intraday evidence, event metadata, and proxy-dependence reduction
- Main residual risk: resolved evidence may weaken or reject some candidates rather than confirm them, which is still a quality improvement if it reduces unresolved claims
