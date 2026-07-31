# C2_TREND_EQ_PRIMARY_V1 Outcome Review 001

Objective: understand why C2_TREND_EQ_PRIMARY_V1 contains nearly all usable outcome evidence.

Scope: existing AEGIS artifacts only. This review adds no architecture, systems, schemas, sleeve modifications, candidate changes, exit-rule changes, or trade recommendations.

## Findings

C2_TREND_EQ_PRIMARY_V1 contains nearly all usable outcome evidence because it is the only sleeve with both high paper-position volume and closed outcomes. It is not yet proven to be the best hypothesis by quality. The available evidence supports a narrower conclusion: it is the only currently functioning evidence source at meaningful sample scale, and the easiest source for near-term outcome accumulation.

The evidence is cautionary rather than positive. C2_TREND_EQ_PRIMARY_V1 had 12 closed paper positions and 12 usable validation samples, but sleeve evidence certification still classified it as UNDERPOWERED and BUILDING_SAMPLE, with early weak realized evidence.

## Evidence Base

- Sleeve performance truth for 2026-06-03: C2_TREND_EQ_PRIMARY_V1 had 53 paper trades, 41 open paper positions, 12 closed paper positions, data quality PASS, no missing authorities, average hold time 3.878127, and exit reasons of 9 STOP_LOSS_THRESHOLD_REACHED and 3 TAKE_PROFIT_THRESHOLD_REACHED.
- Sleeve evidence certification for 2026-06-03: C2_TREND_EQ_PRIMARY_V1 was UNDERPOWERED and BUILDING_SAMPLE, with 41 active positions, 12 closed positions, realized PnL -45.231692, unrealized PnL -113.795309, win rate 0.333333, expected value -3.769308, and profit factor 0.584508.
- Hypothesis outcome ledger for 2026-06-03: HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1 had 53 paper positions, 41 open positions, 12 closed positions, 12 usable validation samples, 54 candidates, and VALIDATION_READY state.
- Other hypothesis outcome ledger rows had 0 usable validation samples. Several had 0 paper positions; the largest non-trend candidate count was HYP_EVENT_DISLOCATION_REPRICING_V1 with 23 candidates but 0 paper positions and 0 usable validation samples.
- Sleeve evidence certification for 2026-06-03: the other evaluated sleeves were ZERO_SAMPLE with 0 closed positions.
- Sleeve throughput evidence scorecard for 2026-06-03: C2_TREND_EQ_PRIMARY_V1 had 53 candidates and 53 outcomes in the scorecard; the next largest flowing sleeve, C2_CROSS_ASSET_TREND_V1, had 5 candidates and 5 outcomes.
- Research quality engine for 2026-06-03: 10 hypotheses, 6 underpowered, 4 blocked, and 0 ready for capital review.
- Hypothesis decision policy for 2026-06-03: HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1 received DECREASE_ATTENTION, with reason codes including HIGH_CANDIDATE_YIELD, VALIDATION_READY_THRESHOLD_MET, EARLY_OUTCOME_PERFORMANCE_POOR, NEGATIVE_EXPECTANCY, DRAWDOWN_LIMIT_BREACH, and UNDERPOWERED_EARLY_OUTCOME_PERFORMANCE_POOR_LOW_CONFIDENCE.

## Why Evidence Is Concentrated

1. More paper positions.

C2_TREND_EQ_PRIMARY_V1 had 53 paper trades, while the other evaluated sleeves had 5, 2, 2, and 1 paper trades. This volume advantage is the strongest reason usable outcomes are concentrated here.

2. More closed outcomes.

C2_TREND_EQ_PRIMARY_V1 had 12 closed paper positions. Every other evaluated sleeve had 0 closed positions and was classified ZERO_SAMPLE.

3. Functioning exit path.

The sleeve had deterministic exit reasons recorded: 9 stop-loss closures and 3 take-profit closures. Other evaluated sleeves had open positions but no closed outcomes, so their evidence remained mark-to-market only.

4. Usable validation sample flow.

The associated hypothesis, HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1, was the only hypothesis with usable validation samples and the only one marked VALIDATION_READY on 2026-06-03.

5. Not because of proven superior quality.

The same artifacts that show functioning outcome flow also show weak early realized evidence and underpowered status. The concentration is primarily explained by throughput and closure mechanics, not by proven hypothesis quality.

## Distinguishing Features

- High candidate and paper-position inventory compared with other sleeves.
- Actual closed outcomes rather than only open positions.
- Deterministic exits already firing.
- Data quality PASS and no missing authorities.
- Validation-ready status at the hypothesis layer.
- Early performance evidence is measurable but weak.

## Cause Classification

Better hypothesis quality: UNSUPPORTED.

Evidence: The sleeve has the most measurable evidence, but early realized evidence is weak and underpowered. Hypothesis decision policy included DECREASE_ATTENTION and negative/cautionary reason codes.

More candidates: SUPPORTED.

Evidence: The related hypothesis had 54 candidates and the throughput scorecard showed 53 candidates for the sleeve. Other flowing sleeves had far fewer candidate/outcome rows.

More open positions: SUPPORTED.

Evidence: The sleeve had 41 open paper positions and 53 total paper trades, far more than other evaluated sleeves.

Shorter outcome cycle: PARTIALLY_SUPPORTED.

Evidence: The sleeve had 12 closed positions with average hold time 3.878127 and deterministic stop/take-profit exits. Other sleeves had open positions and no closed outcomes. This suggests a functioning closure cycle, though not enough evidence proves it is intrinsically shorter than all other sleeves.

Easier validation: SUPPORTED.

Evidence: It was the only hypothesis with 12 usable validation samples and VALIDATION_READY state. Other sleeves were ZERO_SAMPLE or had candidate flow without paper positions.

Accidental concentration: PARTIALLY_SUPPORTED.

Evidence: The concentration may be partly historical or operational because one sleeve accumulated most paper positions before other sleeves generated closed samples. But it is not random in the artifacts: paper-position volume, exit firing, and validation sample creation all point to a functioning evidence path.

## Evidence Source Classification

Strongest evidence source: PARTIALLY_SUPPORTED.

It is strongest by sample count and measurability, but not by positive result quality. Early results are weak and underpowered.

Easiest evidence source: SUPPORTED.

It already has paper positions, closed outcomes, deterministic exits, validation samples, and no missing authorities.

Only functioning evidence source: SUPPORTED FOR MEANINGFUL CLOSED-SAMPLE FLOW.

Other sleeves have paper observations or candidate flow, but C2_TREND_EQ_PRIMARY_V1 is the only evaluated sleeve producing closed outcomes and usable validation samples at meaningful scale.

## Transferable Lessons

1. Outcome learning needs paper-position depth, not just candidate flow.

HYP_EVENT_DISLOCATION_REPRICING_V1 had 23 candidates but 0 paper positions and 0 usable validation samples. Candidate flow without paper observation flow does not create outcome maturity.

2. Sleeves need deterministic closure paths that actually fire.

C2_TREND_EQ_PRIMARY_V1 produced validation samples because exits occurred. Other sleeves with open positions remained ZERO_SAMPLE.

3. Data quality and authority cleanliness matter, but are not enough.

Other evaluated sleeves also had data quality PASS and no missing authorities, yet produced no closed samples. Those controls are prerequisites, not sufficient conditions.

4. Mark-to-market gains should not be treated as outcomes.

Several ZERO_SAMPLE sleeves had positive unrealized PnL, but evidence certification blocked evaluation with POSITIVE_UNREALIZED_PNL_IS_MARK_TO_MARKET_ONLY and ZERO_CLOSED_POSITIONS.

5. A sleeve can be the best learning surface while still showing weak performance.

C2_TREND_EQ_PRIMARY_V1 is the best current outcome-learning source, but early realized evidence is negative and underpowered. Outcome accumulation should separate learning value from return quality.

## Recommended Next Action

Perform a focused outcome-flow check on the 41 open C2_TREND_EQ_PRIMARY_V1 paper positions. Identify which positions are closest to deterministic closure under existing exit recommendations, then rerun existing outcome auto-closure, outcome validation, sleeve evidence certification, and statistical sufficiency for the pinned review day.

Do not change exit rules, candidate rules, sleeve behavior, validation schemas, governance systems, or architecture.
