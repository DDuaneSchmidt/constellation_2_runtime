# Open Paper Position Outcome Follow-Through Review 001

Objective: run a read-only outcome follow-through review on existing open paper positions for the five high-priority outcome-learning sleeves.

Scope: existing AEGIS artifacts only. This review does not change exit rules, candidate rules, sleeve behavior, schemas, governance, architecture, runtime truth logic, or capital allocation. It creates no trade advice, exit recommendation, portfolio advice, position-management instruction, new candidate, or sleeve change.

Pinned evidence day: 2026-06-03.

Evidence base:
- `research_journal/reports/outcome_maturity_acceleration_review_001.md`
- `research_journal/reports/c2_trend_eq_outcome_review_001.md`
- `research_journal/reports/c2_trend_eq_open_outcome_flow_review_001.md`
- Paper position ledger for 2026-06-03
- Exit recommendations for 2026-06-03
- Paper outcome auto-closure for 2026-06-03
- Validation samples for 2026-06-03
- Sleeve performance truth for 2026-06-03
- Sleeve evidence certification for 2026-06-03
- Sleeve throughput diagnostics for 2026-06-03

## 1. Executive Summary

The 2026-06-03 high-priority open-position population contained 51 open paper positions across the five target sleeves. All 51 had exit recommendation `HOLD` with reason code `NO_EXIT_RULE_TRIGGERED`; no open position was already eligible for deterministic closure in the reviewed exit recommendation artifact.

Near-term likely closure paths split into two different learning modes:

- Same-sleeve sample deepening: `C2_TREND_EQ_PRIMARY_V1` had two positions within 1 percentage point of the existing stop-loss threshold. If existing deterministic thresholds are reached, these would add samples to the only current building-sample sleeve.
- First-sample distribution: `C2_MEAN_REVERSION_EQ_V1` and `C2_VOL_INCOME_DEFINED_RISK_V1` each had one open position within 3 days of the existing max-hold threshold. If existing max-hold or other deterministic closure conditions are reached, these would be the most plausible near-term first usable samples for currently ZERO_SAMPLE sleeves.

`C2_CROSS_ASSET_TREND_V1` had five open positions, all certified, but none were close to stop-loss, take-profit, or max-hold closure under the proximity rules used here. `C2_OIL_SHOCK_REVERSAL_V1` had one open certified position, but it was not close to a deterministic closure threshold in the 2026-06-03 artifact.

The evidence action most likely to increase usable validation samples fastest is to rerun existing outcome closure, validation sample, sleeve evidence certification, and statistical sufficiency artifacts after the next deterministic outcome cycle, with focused checks on the two Trend EQ near-stop positions and the two short-horizon zero-sample max-hold-proximity positions.

## 2. Open Position Population By Sleeve

| Sleeve | Open positions | Closed positions | Exit recommendation state | Validation samples | Evidence status |
|---|---:|---:|---|---|---|
| `C2_TREND_EQ_PRIMARY_V1` | 41 | 12 | 41 HOLD | 12 included, 41 excluded | BUILDING_SAMPLE, UNDERPOWERED |
| `C2_CROSS_ASSET_TREND_V1` | 5 | 0 | 5 HOLD | 0 included, 5 excluded | ZERO_SAMPLE, UNDERPOWERED |
| `C2_MEAN_REVERSION_EQ_V1` | 2 | 0 | 2 HOLD | 0 included, 2 excluded | ZERO_SAMPLE, UNDERPOWERED |
| `C2_VOL_INCOME_DEFINED_RISK_V1` | 2 | 0 | 2 HOLD | 0 included, 2 excluded | ZERO_SAMPLE, UNDERPOWERED |
| `C2_OIL_SHOCK_REVERSAL_V1` | 1 | 0 | 1 HOLD | 0 included, 1 excluded | ZERO_SAMPLE, UNDERPOWERED |

Total target-sleeve population:

- 51 open positions.
- 12 closed positions.
- 12 included validation samples.
- 51 excluded validation samples.
- 12 auto-closed outcomes in paper outcome auto-closure.
- 51 hold/not-eligible rows in paper outcome auto-closure.
- All reviewed open-position exit recommendations were `HOLD`.

## 3. Positions Closest To Deterministic Closure

Classification rules used in this review:

- Near stop-loss: within 1 percentage point of the existing stop-loss threshold.
- Near take-profit: within 1 percentage point of the existing take-profit threshold.
- Near max-hold: within 3 days of the existing max-hold threshold.
- No near-term deterministic proximity: none of the above.

Closest positions:

| Rank | Sleeve | Symbol | Position ID | Return | Holding period | Existing threshold proximity | Source recommendation |
|---:|---|---|---|---:|---:|---|---|
| 1 | `C2_MEAN_REVERSION_EQ_V1` | IMO | `paper-position:candidate_contract_8cb667fe49798b8276df6ce8` | +2.7415% | 2 days | 3 days to 5-day max-hold | HOLD |
| 2 | `C2_VOL_INCOME_DEFINED_RISK_V1` | GLD | `paper-position:candidate_contract_f60b1a951187a1cbc9213ef4` | -0.0134% | 2 days | 3 days to 5-day max-hold | HOLD |
| 3 | `C2_TREND_EQ_PRIMARY_V1` | CACC | `paper-position:candidate_contract_f310565767e611c39a7ffcb6` | -4.5931% | 2 days | 0.4069 percentage points from 5% stop-loss | HOLD |
| 4 | `C2_TREND_EQ_PRIMARY_V1` | BMY | `paper-position:candidate_contract_f7e6ace83d1ac6549c7efaa0` | -4.3555% | 5 days | 0.6445 percentage points from 5% stop-loss | HOLD |

Next closest Trend EQ positions by stop-loss proximity, but outside the 1 percentage point near-closure rule:

- `C2_TREND_EQ_PRIMARY_V1` CACC, `paper-position:candidate_contract_3aecd130e704ec57e4a8be68`: -3.3734%, 1.6266 percentage points from stop-loss, HOLD.
- `C2_TREND_EQ_PRIMARY_V1` BTSG, `paper-position:candidate_contract_2ef716320940a4334c6e2191`: -2.5296%, 2.4704 percentage points from stop-loss, HOLD.
- `C2_TREND_EQ_PRIMARY_V1` BDX, `paper-position:candidate_contract_db4abdaead6eba8c640f3294`: -2.4781%, 2.5219 percentage points from stop-loss, HOLD.

Closest take-profit candidates, but not near threshold:

- `C2_TREND_EQ_PRIMARY_V1` CSCO, `paper-position:candidate_contract_818542d7ada06f9e33c53f2b`: +8.2269%, 3.7731 percentage points from the 12% take-profit threshold, HOLD.
- `C2_TREND_EQ_PRIMARY_V1` APTV, `paper-position:candidate_contract_d5055d163a2cfdca59f7c903`: +7.8732%, 4.1268 percentage points from the 12% take-profit threshold, HOLD.

Non-Trend open positions:

| Sleeve | Symbol | Position ID | Return | Holding period | Policy thresholds | Proximity judgment |
|---|---|---|---:|---:|---|---|
| `C2_CROSS_ASSET_TREND_V1` | QQQ | `paper-position:candidate_contract_00674e1c66ff9e28aad23b7e` | +1.4797% | 6 days | 10% stop, 20% take-profit, 60-day max-hold | No near-term proximity |
| `C2_CROSS_ASSET_TREND_V1` | QQQ | `paper-position:candidate_contract_13feb55ab2f42c131551768c` | +0.7603% | 5 days | 10% stop, 20% take-profit, 60-day max-hold | No near-term proximity |
| `C2_CROSS_ASSET_TREND_V1` | QQQ | `paper-position:candidate_contract_76089c5026458b94b8236d5c` | +0.7780% | 2 days | 10% stop, 20% take-profit, 60-day max-hold | No near-term proximity |
| `C2_CROSS_ASSET_TREND_V1` | QQQ | `paper-position:candidate_contract_1eea42feea3649a2f7c61e35` | +0.1154% | 1 day | 10% stop, 20% take-profit, 60-day max-hold | No near-term proximity |
| `C2_CROSS_ASSET_TREND_V1` | QQQ | `paper-position:candidate_contract_3407a097d46f965b533bbe06` | +0.4037% | 0 days | 10% stop, 20% take-profit, 60-day max-hold | No near-term proximity |
| `C2_MEAN_REVERSION_EQ_V1` | IMO | `paper-position:candidate_contract_8cb667fe49798b8276df6ce8` | +2.7415% | 2 days | 3.5% stop, 6% take-profit, 5-day max-hold | Near max-hold |
| `C2_MEAN_REVERSION_EQ_V1` | BMY | `paper-position:candidate_contract_ab2f67321ea381ea2f97131f` | +0.0000% | 1 day | 3.5% stop, 6% take-profit, 5-day max-hold | No near-term proximity |
| `C2_VOL_INCOME_DEFINED_RISK_V1` | GLD | `paper-position:candidate_contract_f60b1a951187a1cbc9213ef4` | -0.0134% | 2 days | 5% stop, 10% take-profit, 5-day max-hold | Near max-hold |
| `C2_VOL_INCOME_DEFINED_RISK_V1` | GLD | `paper-position:candidate_contract_d539cbd061c90580745d1d8f` | +0.0000% | 1 day | 5% stop, 10% take-profit, 5-day max-hold | No near-term proximity |
| `C2_OIL_SHOCK_REVERSAL_V1` | USO | `paper-position:candidate_contract_a3dd44d21952f8098131aa04` | +1.3875% | 1 day | 5% stop, 10% take-profit, 5-day max-hold | No near-term proximity |

## 4. Closure Path Categories

| Category | Count | Sleeves affected | Interpretation |
|---|---:|---|---|
| Near stop-loss | 2 | `C2_TREND_EQ_PRIMARY_V1` | Two Trend EQ positions were close to the existing 5% stop-loss threshold. |
| Near take-profit | 0 | None | No reviewed open position was within 1 percentage point of take-profit. |
| Near max-hold | 2 | `C2_MEAN_REVERSION_EQ_V1`, `C2_VOL_INCOME_DEFINED_RISK_V1` | One position in each short-horizon sleeve was within 3 days of max-hold. |
| No near-term deterministic proximity | 47 | All five sleeves | Most open positions had no threshold or max-hold proximity in the reviewed artifact. |
| Data/certification blocker | 0 in reviewed open-position population | None | Reviewed open positions had certified marks. |
| Paper-path blocker | 0 in reviewed open-position population | None | These positions already reached paper observation and validation-sample exclusion as open positions. |

All open positions remained `HOLD` in the source exit recommendation artifact. The categories above are evidence-proximity categories only, not recommendations to exit or manage positions.

## 5. Expected Near-Term Validation Sample Yield

Expected near-term yield is low to moderate under existing rules.

Most likely same-sleeve additions:

- Up to 2 near-term Trend EQ samples if the two near-stop positions naturally reach existing deterministic stop-loss closure.
- These would raise `C2_TREND_EQ_PRIMARY_V1` from 12 to at most 14 included samples in this narrow scenario, still likely underpowered and still concentrated in the same sleeve.

Most likely first-sample distribution additions:

- Up to 1 `C2_MEAN_REVERSION_EQ_V1` sample if the IMO position reaches deterministic closure through max-hold or another existing rule.
- Up to 1 `C2_VOL_INCOME_DEFINED_RISK_V1` sample if the GLD position reaches deterministic closure through max-hold or another existing rule.
- These would be high learning value because they would convert ZERO_SAMPLE sleeves into first-sample sleeves, but they would not by themselves create statistical sufficiency.

Less likely near-term additions:

- `C2_CROSS_ASSET_TREND_V1`: 5 open positions, but long 60-day max-hold and no price-threshold proximity in the reviewed artifact.
- `C2_OIL_SHOCK_REVERSAL_V1`: 1 open position, certified and paper-observed, but not close to deterministic closure in the 2026-06-03 artifact.

Near-term closures are therefore mixed rather than purely concentrated: Trend EQ remains the largest likely same-sleeve sample source, while the highest distribution value comes from the short-horizon zero-sample sleeves.

## 6. Outcome Learning Value By Sleeve

`C2_TREND_EQ_PRIMARY_V1`

- Learning value: highest for sample count.
- Reason: 41 open positions, 12 closed positions, 12 included samples, and existing deterministic exits already fired historically.
- Limitation: additional samples deepen an already concentrated evidence stream and do not solve cross-sleeve distribution.

`C2_MEAN_REVERSION_EQ_V1`

- Learning value: high for first-sample distribution.
- Reason: 2 open certified positions and one position within 3 days of the 5-day max-hold threshold.
- Limitation: even one closure would remain a very small sample.

`C2_VOL_INCOME_DEFINED_RISK_V1`

- Learning value: high for first-sample distribution.
- Reason: 2 open certified positions and one position within 3 days of the default 5-day max-hold threshold.
- Limitation: first closure would be useful for distribution, not sufficiency.

`C2_CROSS_ASSET_TREND_V1`

- Learning value: medium.
- Reason: 5 open certified positions and an existing paper-observation path.
- Limitation: no near-term deterministic proximity was visible because the policy uses wider thresholds and a 60-day max-hold.

`C2_OIL_SHOCK_REVERSAL_V1`

- Learning value: medium but blocked by closure condition.
- Reason: 1 open certified position and generated-hypothesis artifacts show paper observation flow reached.
- Limitation: the 2026-06-03 exit recommendation was HOLD, and generated-hypothesis outcome artifacts identify close condition not met for the open observation.

## 7. Blockers To Outcome Conversion

Primary blocker: no deterministic closure proximity for most positions.

- 47 of 51 open positions were not near stop-loss, take-profit, or max-hold under this review's proximity rules.
- All 51 open positions had `HOLD` and `NO_EXIT_RULE_TRIGGERED`.

Secondary blocker: concentration.

- The only included validation samples came from `C2_TREND_EQ_PRIMARY_V1`.
- The four other high-priority sleeves remained ZERO_SAMPLE on 2026-06-03.

Not primary blockers in the reviewed open-position population:

- Missing data: reviewed open positions had certified marks.
- Certification: no open reviewed position was excluded because of mark certification failure.
- Paper path: every reviewed open position already existed as a paper observation and validation-sample candidate, but remained excluded because it was unresolved.
- Capital review: not relevant; research quality and allocation artifacts remained at 0 ready for capital review in the broader journal evidence.

Sleeve-specific blockers:

- `C2_TREND_EQ_PRIMARY_V1`: mostly threshold/time non-proximity; two near-stop positions are the exception.
- `C2_CROSS_ASSET_TREND_V1`: wide thresholds and long max-hold make near-term closure unlikely in the reviewed artifact.
- `C2_MEAN_REVERSION_EQ_V1`: one position close by max-hold timing; the other lacks proximity.
- `C2_VOL_INCOME_DEFINED_RISK_V1`: one position close by max-hold timing; the other lacks proximity.
- `C2_OIL_SHOCK_REVERSAL_V1`: close condition not met; HOLD remained the deterministic state.

## 8. Recommended Next Evidence Action

Run existing read-only outcome artifacts after the next deterministic outcome cycle and compare the pinned 2026-06-03 population against the next available outcome state.

Focus checks:

- Whether CACC and BMY in `C2_TREND_EQ_PRIMARY_V1` crossed existing stop-loss thresholds.
- Whether IMO in `C2_MEAN_REVERSION_EQ_V1` reached max-hold or another existing deterministic closure condition.
- Whether GLD in `C2_VOL_INCOME_DEFINED_RISK_V1` reached max-hold or another existing deterministic closure condition.
- Whether any `C2_CROSS_ASSET_TREND_V1` or `C2_OIL_SHOCK_REVERSAL_V1` open position moved from HOLD into deterministic closure under existing rules.
- Whether included validation samples increased without changing rules, sleeves, candidates, schemas, governance, architecture, or runtime truth.

No new Research Journal knowledge or failure entry was created by this review. The findings reinforce existing journal conclusions about outcome maturity and first-sample distribution, but they do not yet add a reusable learning that is stronger than the existing knowledge objects.
