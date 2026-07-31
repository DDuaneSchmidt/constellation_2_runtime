# Research Director Prediction Accuracy 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Scope: offline measurement of whether Research Director identified the correct bottlenecks in the correct order. No implementation, no production changes, no replay changes, no candidate changes, no qualification changes, no governance changes, no memory writes, no trading recommendation, no capital authority, no broker execution, no position sizing, and no paper-placement authority.

## Inputs Reviewed

- `research_journal/reports/research_director_retrospective_001.md`
- `research_journal/reports/research_director_bottleneck_selection_test_001.md`
- `reports/atlas_v2_research_os/direct_replay_coverage_audit_001.md`
- `reports/atlas_v2_research_os/market_data_acquisition/market_data_acquisition_001.md`
- `reports/atlas_v2_research_os/daily_completion_acquisition/market_data_acquisition_001.md`
- `research_journal/reports/insufficient_data_root_cause_analysis_001.md`
- `research_journal/reports/regime_filter_root_cause_001.md`

Note: `direct_replay_coverage_audit_001.md` and `market_data_acquisition_001.md` were found under `reports/atlas_v2_research_os/`, not under `research_journal/reports/`.

## Executive Result

`PASS_WITH_LIMITATIONS`

Research Director identified the dominant validation bottleneck correctly and early: Data Coverage. It also recognized Research Adversary as high-leverage and recognized replay/backtest weakness as a validation bottleneck class. However, it did not precisely order all later bottlenecks from the outset. Replay attrition and regime vocabulary mismatch were identified only after direct coverage repair exposed them.

The correct conclusion is that Research Director should advance from design-only to an evaluation prototype, but only as a measured, non-authoritative prioritization system.

## Timeline Assessment

| Stage | Bottleneck | Was It Identified? | When | Accuracy | Impact | Research Hours Saved | Counterfactual Delay Estimate |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 1 | Research Adversary | Yes, as high-priority support layer | Retrospective report; ranked #3 in bottleneck selection test | Medium | Medium-high | 4-8 hours | 1-2 review cycles |
| 2 | Data Coverage | Yes, explicitly as #1 bottleneck | Bottleneck selection test, 2026-06-05; confirmed by direct replay coverage audit at 18:47:31Z | High | Very high | 8-16 hours | 1-3 validation cycles |
| 3 | Replay Attrition | Partially | Broadly predicted as weak replay/backtest and validation blockage; specifically diagnosed in insufficient data root cause report at 19:55:09Z | Medium | High | 4-10 hours | 1-2 validation cycles after coverage repair |
| 4 | Regime Vocabulary Mismatch | Partially, late | Broadly covered by failure taxonomy/regime dependency; specifically diagnosed in regime filter root cause report on 2026-06-05 | Medium-low before root cause; high after root cause | Medium-high | 3-6 hours | 1 validation cycle |

## Stage Findings

### Stage 1: Research Adversary

Assessment: `PASS_WITH_LIMITATIONS`

Research Director Retrospective identified Research Adversary as high priority because Atlas repeatedly needed competing explanations, null explanations, assumption extraction, and falsification proposals before candidate advancement. It estimated medium-to-high research hours saved and high uncertainty reduction.

The bottleneck selection test did not rank Research Adversary first. It ranked it third with score `0.765`, below Data Coverage and Failure Taxonomy, because adversarial critique cannot run replay, add missing symbols, or convert proxy evidence into candidate-specific validation.

Accuracy:

- Correct that Research Adversary was valuable.
- Correct not to place it above a hard data blocker during active validation blockage.
- Limitation: in the requested historical timeline, Research Adversary appears as Stage 1, but the Research Director scoring model would not have selected it as the first active bottleneck once direct replay coverage evidence was available.

Impact:

- High for critique quality and falsification routing.
- Medium for validation throughput because it does not remove data/replay blockers directly.

Research hours saved estimate: 4-8 hours from reduced manual critique reconstruction and fewer repeated assumption reviews.

### Stage 2: Data Coverage

Assessment: `PASS`

Research Director Bottleneck Selection Test explicitly selected Data Coverage as the #1 bottleneck. It cited:

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

The direct replay coverage audit independently confirmed the same evidence at 2026-06-05T18:47:31Z. Market data acquisition then acquired and validated DIA, QQQ, BAC, META, MSFT, TSLA, AMZN, and NFLX at 2026-06-05T19:11:47Z, followed by AAPL, JPM, TLT, USO, and DBC at 2026-06-05T20:10:37Z.

Accuracy:

- Correct bottleneck.
- Correct order relative to later direct replay work.
- Correct action class: repair uncertainty by acquiring candidate-specific data.

Impact:

- Very high. Data acquisition reduced the first-order blocker and allowed later root causes to be observed.

Research hours saved estimate: 8-16 hours from avoiding additional candidate review while direct validation had only `6.67%` symbol coverage and `100%` block rate.

### Stage 3: Replay Attrition

Assessment: `PASS_WITH_LIMITATIONS`

Research Director Retrospective identified validation bottlenecks including insufficient sample sizes and weak replay/backtest support. That was directionally correct, but it did not initially isolate replay attrition as a second-stage bottleneck after coverage repair.

The precise diagnosis appeared in Insufficient Data Root Cause Analysis 001:

- direct validation candidates reviewed: `8`
- confirmed: `1`
- remaining insufficient data: `7`
- current unique-symbol coverage: `66.67%`
- current full candidate coverage: `5/8`
- current validation block rate: `37.5%`
- insufficient sample size after trigger/regime filtering: `4` candidates
- replay logic limitation: `4` candidates
- timeframe or daily-vs-intraday mismatch: `7` candidates

The report shows that daily symbol acquisition removed the first layer of blockage, but daily proxy replay still produced zero usable samples for several candidates after trigger/regime filtering.

Accuracy:

- Correct broad category: validation/replay bottlenecks.
- Correct dependency: replay attrition became visible after data coverage improved.
- Limitation: the Research Director did not name "replay attrition after trigger/regime filtering" as the next bottleneck with enough specificity before the root-cause analysis.

Impact:

- High. It prevented the wrong conclusion that data acquisition had failed or that candidate ideas were necessarily invalid.

Research hours saved estimate: 4-10 hours from avoiding repeated data-acquisition loops and redirecting attention toward replay logic, intraday requirements, event metadata, and universe aggregation.

### Stage 4: Regime Vocabulary Mismatch

Assessment: `PASS_WITH_LIMITATIONS`

Research Director Retrospective and related taxonomy thinking recognized `REGIME_DEPENDENCY` and proxy-dependency failure classes, but the exact mismatch was not identified until Regime Filter Root Cause 001.

The regime root-cause report found that direct validation used exact allowed-regime matching:

`triggered_sample.regime in allowed_regimes`

For five affected candidates, `allowed_regimes=[CHOP]`, while the daily proxy replay classifier emits only:

- `HIGH_VOLATILITY`
- `TRENDING`
- `RANGE_BOUND`
- `LOW_VOLATILITY`
- `UNKNOWN`

It does not emit `CHOP`, so exact `CHOP` matching is impossible under the current daily proxy vocabulary.

Accuracy:

- Correct broad prediction: regime/proxy mismatch is a likely failure class.
- Correct after root cause: the true blocker is vocabulary incompatibility, not merely weak candidate quality.
- Limitation: Research Director did not predeclare the exact `CHOP` versus daily-regime-label mismatch.

Impact:

- Medium-high. It prevents false negative candidate interpretation and prevents relaxing production filters without a compatibility diagnostic.

Research hours saved estimate: 3-6 hours from avoiding repeated inspection of individual candidates whose samples were erased by a shared vocabulary mismatch.

## Metrics

### Bottleneck Detection Accuracy

Definition: share of realized bottlenecks that Research Director identified at least at the right abstraction level.

Result: `4/4` broad bottlenecks identified.

- Research Adversary: identified.
- Data Coverage: identified explicitly.
- Replay Attrition: identified broadly as replay/backtest weakness and validation blockage.
- Regime Vocabulary Mismatch: identified broadly through regime/proxy failure classes.

Score: `100% broad detection`, `50% precise detection`.

### Prediction Accuracy

Definition: whether Research Director predicted the bottleneck with enough specificity to route the next useful work.

Result: `2.5/4`.

- Research Adversary: correct value, but not the active hard blocker once coverage evidence existed.
- Data Coverage: correct and actionable.
- Replay Attrition: directionally correct but required later root-cause analysis.
- Regime Vocabulary Mismatch: directionally correct but not precise until later.

Score: `62.5% precise prediction accuracy`.

### Research Hours Saved

Estimated total research hours saved: `19-40 hours`.

Breakdown:

- Research Adversary: `4-8`
- Data Coverage: `8-16`
- Replay Attrition: `4-10`
- Regime Vocabulary Mismatch: `3-6`

Interpretation: the largest savings came from selecting Data Coverage before more hypothesis generation or critique work.

### Time-To-Bottleneck Identification

| Bottleneck | Identification Latency |
| --- | --- |
| Research Adversary | Early retrospective identification; not top active blocker under coverage evidence |
| Data Coverage | Immediate once direct replay coverage audit evidence existed |
| Replay Attrition | Same day after data coverage repair exposed remaining insufficient-data cases |
| Regime Vocabulary Mismatch | Same day after replay attrition was decomposed into trigger/regime-filter failure |

Interpretation: Research Director was strongest at first-order bottleneck selection. It needed sequential evidence to identify second- and third-order bottlenecks.

### Counterfactual Delay Estimate

Without Research Director-style prioritization:

- Data Coverage could have been delayed by 1-3 validation cycles while more generated critique, taxonomy, or candidate review work accumulated.
- Replay Attrition could have been delayed by 1-2 additional validation cycles if the team treated post-acquisition insufficiency as still a pure data problem.
- Regime Vocabulary Mismatch could have been delayed by 1 additional validation cycle if each zero-sample candidate were debugged separately instead of as a shared vocabulary issue.

Total counterfactual delay avoided: approximately `3-6 research/validation cycles`.

## Overall Verdict

`PASS_WITH_LIMITATIONS`

Research Director passed the most important test: it selected Data Coverage as the active hard bottleneck before less direct but attractive workstreams. It also preserved the correct conceptual map: adversarial critique and taxonomy help, but they do not replace direct validation data.

The limitation is precision and sequencing after the first bottleneck. Research Director did not fully predict the exact replay attrition mechanism or the exact regime vocabulary mismatch. Those emerged only after coverage repair and root-cause analysis.

## Advancement Decision

Research Director should advance from design-only to evaluation prototype.

Conditions:

- Evaluation prototype must remain non-authoritative.
- It may rank bottlenecks and route research work only.
- It must be scored against realized bottlenecks, not against narrative plausibility.
- It must separate broad bottleneck detection from precise causal diagnosis.
- It must record counterfactual delay estimates and realized hours saved after each research cycle.
- It must not change replay, qualification, candidate state, governance, memory, paper placement, capital, or trading systems.

Recommended next evaluation target:

Run Research Director as a shadow scorer over the next validation cycle and compare its ranked bottleneck predictions against realized root-cause reports.
