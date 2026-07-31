# Outcome Evidence Concentration Watch 001

Date: 2026-06-03
Status: READ_ONLY_WATCH
Scope: Evidence concentration in closed paper outcomes across high-priority sleeves

Evidence base:
- `research_journal/reports/open_paper_position_outcome_follow_through_review_001.md`
- `research_journal/reports/c2_trend_eq_outcome_review_001.md`
- `research_journal/reports/outcome_maturity_acceleration_review_001.md`
- `research_journal/reports/evidence_accumulation_forecast_review_001.md`

This watch does not change rules, exits, candidates, sleeves, schemas, governance, runtime truth, architecture, or capital allocation. It is not trade advice, exit advice, portfolio advice, or position management advice.

## 1. Purpose

Define a read-only watch for whether new deterministic closed outcomes reduce the current evidence concentration in `C2_TREND_EQ_PRIMARY_V1`.

The watch is intended to distinguish two different forms of progress:

- local sample maturity: more closed samples inside Trend EQ;
- distribution improvement: first or additional closed samples outside Trend EQ.

The strategic concern is not whether Trend EQ can add more samples. The concern is whether AEGIS can begin accumulating validation evidence across other sleeves without forcing exits, changing candidate rules, or adding architecture.

## 2. Current Evidence Concentration Baseline

Baseline from the 2026-06-03 research reviews:

| Sleeve | Closed samples | Open positions | Baseline state | Concentration implication |
| --- | ---: | ---: | --- | --- |
| `C2_TREND_EQ_PRIMARY_V1` | 12 | 41 | `BUILDING_SAMPLE` / `UNDERPOWERED` | Only sleeve with usable closed validation samples. |
| `C2_MEAN_REVERSION_EQ_V1` | 0 | 2 | `ZERO_SAMPLE` / `UNDERPOWERED` | First closed sample would reduce concentration. |
| `C2_VOL_INCOME_DEFINED_RISK_V1` | 0 | 2 | `ZERO_SAMPLE` / `UNDERPOWERED` | First closed sample would reduce concentration. |
| `C2_CROSS_ASSET_TREND_V1` | 0 | 5 | `ZERO_SAMPLE` / `UNDERPOWERED` | First closed sample would reduce concentration. |
| `C2_OIL_SHOCK_REVERSAL_V1` | 0 | 1 | `ZERO_SAMPLE` / `UNDERPOWERED` | First closed sample would reduce concentration. |

Current concentration baseline: 12 of 12 closed samples across the high-priority sleeves are from `C2_TREND_EQ_PRIMARY_V1`. Mean Reversion, Vol Income, Cross Asset, and Oil Shock have no closed validation samples in the cited evidence.

The open-position follow-through review found 51 open positions across the five high-priority sleeves and no deterministic closure action at the review point because the relevant positions remained `HOLD` / `NO_EXIT_RULE_TRIGGERED`.

## 3. Watch Positions

Primary watch positions:

| Symbol | Sleeve | Paper position id | Watch purpose | Current review state |
| --- | --- | --- | --- | --- |
| CACC | `C2_TREND_EQ_PRIMARY_V1` | `paper-position:candidate_contract_f310565767e611c39a7ffcb6` | Possible Trend EQ sample count increase. | `HOLD` / `NO_EXIT_RULE_TRIGGERED`; near stop-loss proximity in the follow-through review. |
| BMY | `C2_TREND_EQ_PRIMARY_V1` | `paper-position:candidate_contract_f7e6ace83d1ac6549c7efaa0` | Possible Trend EQ sample count increase. | `HOLD` / `NO_EXIT_RULE_TRIGGERED`; near stop-loss proximity in the follow-through review. |
| IMO | `C2_MEAN_REVERSION_EQ_V1` | `paper-position:candidate_contract_8cb667fe49798b8276df6ce8` | Possible first Mean Reversion sample. | `HOLD` / `NO_EXIT_RULE_TRIGGERED`; near max-hold path in the follow-through review. |
| GLD | `C2_VOL_INCOME_DEFINED_RISK_V1` | `paper-position:candidate_contract_f60b1a951187a1cbc9213ef4` | Possible first Vol Income sample. | `HOLD` / `NO_EXIT_RULE_TRIGGERED`; near max-hold path in the follow-through review. |

Sleeve-level secondary watch:

- `C2_CROSS_ASSET_TREND_V1`: monitor for any first authoritative included closed sample, but the cited follow-through review did not identify a named position with near-term deterministic closure proximity.
- `C2_OIL_SHOCK_REVERSAL_V1`: monitor for any first authoritative included closed sample, but the cited follow-through review did not identify immediate deterministic closure proximity beyond continued open observation.

## 4. Distribution Improvement Criteria

Strong distribution improvement:

- Any new authoritative included closed validation sample in `C2_MEAN_REVERSION_EQ_V1`, `C2_VOL_INCOME_DEFINED_RISK_V1`, `C2_CROSS_ASSET_TREND_V1`, or `C2_OIL_SHOCK_REVERSAL_V1`.
- Any first closed sample in a previously zero-sample sleeve.
- Trend EQ's share of closed samples across the five high-priority sleeves falls below 100%.

Moderate progress:

- CACC or BMY closes under existing deterministic rules and increases Trend EQ from 12 closed samples to 13 or 14.
- This improves Trend EQ sample maturity but does not reduce evidence concentration unless accompanied by a non-Trend closed sample.

No distribution improvement:

- Positions remain `HOLD` / `NO_EXIT_RULE_TRIGGERED`.
- Artifacts are blocked, stale, non-authoritative, or excluded from validation samples.
- New open paper positions are created without deterministic closure.
- Candidate volume increases without closed paper outcomes.

Strategic weighting: any new closed sample outside Trend EQ has higher learning value for the research organization than another Trend EQ-only sample, because it begins to test whether the validation pipeline can mature evidence beyond the currently concentrated sleeve.

## 5. Refresh Trigger

Refresh this watch after the next authoritative outcome refresh that includes:

- paper position ledger;
- deterministic exit recommendations;
- paper outcome auto-closure;
- outcome validation;
- validation sample summary;
- sleeve evidence certification or equivalent authoritative sleeve-performance truth artifact.

If the verified graph, runtime truth kernel, or required source artifact is blocked or non-authoritative for the target day, record no concentration update for that day. A blocked artifact can explain why the watch cannot refresh, but it must not be treated as evidence of progress.

## 6. What Counts As Progress

- IMO becomes an authoritative included closed validation sample for `C2_MEAN_REVERSION_EQ_V1`.
- GLD becomes an authoritative included closed validation sample for `C2_VOL_INCOME_DEFINED_RISK_V1`.
- Any `C2_CROSS_ASSET_TREND_V1` position becomes an authoritative included closed validation sample.
- Any `C2_OIL_SHOCK_REVERSAL_V1` position becomes an authoritative included closed validation sample.
- CACC or BMY becomes an authoritative included closed validation sample for Trend EQ, while clearly classified as Trend-only sample maturity rather than distribution improvement.
- The closed-sample distribution changes from 12 Trend EQ / 0 non-Trend to any nonzero non-Trend count.

## 7. What Does Not Count As Progress

- Unrealized mark-to-market movement.
- A position approaching a stop, target, or holding-period threshold without deterministic closure.
- A position remaining `HOLD` / `NO_EXIT_RULE_TRIGGERED`.
- Another Trend EQ-only closed sample when the claim being tested is concentration reduction.
- Blocked, stale, missing, or non-authoritative artifacts.
- Manual closure, rule changes, candidate changes, sleeve changes, or architecture changes.
- Capital allocation, position management, or trade recommendation activity.

## 8. Recommended Next Evidence Action

Run the existing read-only outcome refresh after the next deterministic outcome cycle and compare only authoritative included validation samples against this baseline:

- Trend EQ: did CACC or BMY close and raise the closed sample count above 12?
- Mean Reversion: did IMO create the first closed sample?
- Vol Income: did GLD create the first closed sample?
- Cross Asset or Oil Shock: did either sleeve create any first closed sample?
- Concentration: did Trend EQ's share of closed samples across the five high-priority sleeves fall below 100%?

Fastest evidence action without changing rules: continue deterministic follow-through on the existing open paper positions and refresh the concentration watch only when authoritative closed-outcome artifacts exist. Do not force closure, modify exits, add candidates, create sleeves, or add architecture to accelerate the count.
