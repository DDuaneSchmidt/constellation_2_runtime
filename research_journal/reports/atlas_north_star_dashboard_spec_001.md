# Atlas North Star Dashboard Specification 001

Objective: define the smallest set of metrics Atlas should track based on the conclusions of `aegis_north_star_metrics_review_001.md`.

Scope: specification only. This report does not implement a dashboard, add UI, add schemas, add mechanisms, change runtime truth, change candidate rules, change governance, create trade advice, create candidates, or alter allocation logic.

Pinned evidence day: 2026-06-03.

Inputs:
- `aegis_north_star_metrics_review_001.md`
- `evidence_flow_bottleneck_review_001.md`
- `outcome_bottleneck_decomposition_review_001.md`
- `decision_risk_monitor_001.md`

## Evidence Basis

- AEGIS North Star Metrics Review 001 selected Distributed Mature Validation Evidence as the North Star and warned against treating raw signals, candidate counts, generated hypothesis counts, or paper position counts as health indicators.
- Evidence Flow Bottleneck Review 001 found 42 raw signals produced 1 generated candidate and 1 valid contract, while 63 paper positions produced 12 included validation samples and 51 open or excluded samples.
- Evidence Flow Bottleneck Review 001 found concentration at the paper/outcome layer: `C2_TREND_EQ_PRIMARY_V1` had 53 of 63 paper positions and all 12 included validation samples.
- Outcome Bottleneck Decomposition Review 001 found outcome maturity is the downstream decision bottleneck, but the deeper root bottleneck is measurable, distributed evidence flow.
- Decision-Risk Monitor 001 identified the dangerous-if-wrong conclusion as research attention allocation being more useful than capital allocation, and defined re-review triggers for capital-readiness, sleeve weakness, outcome maturity, candidate volume, and technical evidence claims.

## Primary North Star Metric

Metric: Distributed Mature Validation Evidence.

Definition:

The count and distribution of sleeves and hypotheses with included validation samples that are advancing evidence maturity beyond ZERO_SAMPLE or UNDERPOWERED states.

Calculation:

- Count included validation samples by sleeve and hypothesis.
- Count sleeves with at least one included validation sample.
- Count hypotheses with at least one included validation sample.
- Join those counts to evidence maturity status: ZERO_SAMPLE, BUILDING_SAMPLE, UNDERPOWERED, VALIDATION_READY, VALIDATED, and positive evidence where available.
- Treat maturity as stronger only when samples are included, closed or resolved, certified, sufficiently independent, and not concentrated in one sleeve or hypothesis.

Why it matters:

This is the smallest metric that captures whether Atlas is helping AEGIS convert research activity into reusable learning. It reflects the key insight from the reviewed evidence: AEGIS does not get stronger from more ideas, more raw signals, more candidates, or more paper positions unless those flows become distributed mature validation evidence.

What it should not be used for:

- It should not authorize capital allocation, trade advice, broker execution, or sleeve changes.
- It should not be reduced to aggregate sample count without distribution.
- It should not imply positive research quality unless performance, robustness, sample independence, and certification also support the claim.

## Supporting Metrics

Metric: Evidence Maturity Status.

Definition:

The current maturity class of each sleeve and hypothesis based on included samples, statistical sufficiency, robustness, sample independence, and positive evidence status.

Calculation:

- Count rows by evidence maturity status.
- Track status by sleeve and hypothesis.
- Highlight transitions from ZERO_SAMPLE to BUILDING_SAMPLE, UNDERPOWERED to VALIDATION_READY, and VALIDATION_READY to VALIDATED.
- Track ready-for-capital-review rows only as a guarded terminal state under existing policy artifacts.

Why it matters:

It prevents Atlas from treating sample flow as learning quality when evidence remains underpowered. On 2026-06-03, all 5 evaluated sleeves were UNDERPOWERED, 4 were ZERO_SAMPLE, 1 was BUILDING_SAMPLE, and 0 had positive evidence.

What it should not be used for:

- It should not be inferred from code, architecture, or paper workflow state.
- It should not be relaxed to promote hypotheses early.
- It should not treat VALIDATION_READY as equivalent to capital readiness.

Metric: Outcome Maturity.

Definition:

The degree to which paper observations have become closed or resolved, included validation samples.

Calculation:

- Closed or resolved outcomes divided by total paper positions.
- Included validation samples divided by total validation sample rows.
- Open and excluded samples by sleeve and hypothesis.
- Track absolute counts and conversion rates.

Why it matters:

Outcome maturity is the most visible downstream decision bottleneck. The 2026-06-03 evidence showed 63 paper positions, 12 included samples, and 51 open or excluded samples.

What it should not be used for:

- It should not be treated as the deepest root cause by itself.
- It should not imply AEGIS is stronger if new outcomes remain concentrated, negative, uncertified, or underpowered.
- It should not encourage forced outcome closure or exit-rule changes.

Metric: Candidate-To-Paper Conversion Quality.

Definition:

The quality of conversion from raw signal to governed candidate contract to paper observation, with expected governance suppression and certification bottlenecks separated.

Calculation:

- Raw signals by sleeve.
- Expected portfolio-gate suppressions.
- Certification or non-certified snapshot blockers.
- Valid governed candidate contracts.
- Paper observations created.
- Closed included samples eventually produced from those observations.

Why it matters:

It is the highest-leverage upstream metric for future distributed evidence maturity. On 2026-06-03, 42 raw signals produced 1 valid candidate contract; most rejected rows were expected governance suppressions, with a smaller certification bottleneck.

What it should not be used for:

- It should not score candidate quality from raw count.
- It should not label expected governance suppression as failure.
- It should not imply trade readiness or capital readiness.

Metric: Research Attention Decision Usefulness.

Definition:

The extent to which existing policy artifacts produce differentiated, evidence-supported research triage while avoiding unsupported capital review.

Calculation:

- Count decisions by CONTINUE, REDESIGN, NEEDS_DATA, DECREASE_ATTENTION, HOLD, PAUSE, CAPITAL_REVIEW, and READY_FOR_CAPITAL_REVIEW.
- Compare decisions to evidence maturity and outcome maturity.
- Flag any capital-review state as a re-review trigger.

Why it matters:

Decision-Risk Monitor 001 identified research attention over capital allocation as robust but dangerous if wrong. Atlas should show whether the system remains in research triage mode or whether evidence has changed enough to re-review capital readiness.

What it should not be used for:

- It should not generate allocation instructions.
- It should not translate research attention into trade sizing.
- It should not hide the fact that capital-review rows remain 0 while evidence is underpowered.

## Anti-Metrics

Metric: Raw Signal Count.

Definition:

The number of raw signals observed before governance, certification, portfolio scoring, candidate contract validation, paper observation, or outcome maturity.

Calculation:

- Count raw signals by sleeve and day.

Why it matters:

It can diagnose upstream activity and producer behavior.

What it should not be used for:

- It should not be used as a research-health indicator.
- It should not be used as a candidate-quality proxy.
- It should not be optimized as a North Star.

Metric: Generated Candidate Count.

Definition:

The number of generated candidates or candidate inventory rows before confirming valid governed contracts, paper observations, and included outcomes.

Calculation:

- Count candidates by sleeve and hypothesis.

Why it matters:

It can expose throughput and conversion questions.

What it should not be used for:

- It should not imply quality, evidence maturity, or outcome maturity.
- It should not be compared across sleeves without governance and certification context.

Metric: Paper Position Count.

Definition:

The number of paper positions or paper observations created.

Calculation:

- Count paper positions by sleeve and hypothesis.

Why it matters:

It shows whether a hypothesis reached measurable paper observation flow.

What it should not be used for:

- It should not be treated as mature evidence.
- It should not be treated as live readiness.
- It should not hide open-position latency or concentration.

Metric: Generated Hypothesis Count.

Definition:

The number of generated or proposed hypotheses.

Calculation:

- Count generated hypotheses by state.

Why it matters:

It can show idea-flow inventory.

What it should not be used for:

- It should not be treated as learning progress.
- It should not justify more architecture or search-space expansion when evidence flow is blocked.

## Decision-Risk Metrics

Metric: Capital-Readiness Re-Review Trigger.

Definition:

Whether any existing research quality, hypothesis decision policy, allocation recommendation, or capital-readiness artifact emits READY_FOR_CAPITAL_REVIEW or CAPITAL_REVIEW.

Calculation:

- Count READY_FOR_CAPITAL_REVIEW rows.
- Count CAPITAL_REVIEW rows.
- Show nonzero rows as re-review triggers, not automatic action.

Why it matters:

This is the highest-risk decision boundary. Decision-Risk Monitor 001 warned that premature capital framing can create false confidence and pressure toward trade advice.

What it should not be used for:

- It should not authorize capital allocation.
- It should not bypass runtime truth, policy gates, or evidence sufficiency.

Metric: Sleeve-Weakness Re-Review Trigger.

Definition:

Whether low or absent candidate production remains explained by valid no-signal, threshold, data, governance, or certification states, or whether it starts indicating true sleeve weakness.

Calculation:

- Track HEALTHY_NO_CANDIDATE, NEEDS_DATA, NEEDS_REPAIR, NEEDS_GOVERNANCE, underproducing, and blocked classifications.
- Flag sleeves that remain empty after favorable, threshold-satisfying conditions or after data/paper-path blockers are resolved.

Why it matters:

The weakest conclusion tonight was that low candidate production is not primarily sleeve weakness. Atlas should monitor for evidence that would overturn that conclusion.

What it should not be used for:

- It should not label a sleeve weak from low volume alone.
- It should not drive sleeve redesign without follow-up evidence.

Metric: Outcome-Bottleneck Re-Review Trigger.

Definition:

Whether outcome maturity remains the main decision bottleneck or whether candidate conversion, paper-path design, data authority, or validation structure becomes the stronger explanation.

Calculation:

- Track closed samples, included samples, open positions, excluded samples, candidate-to-paper conversion, data blockers, and paper-path blockers together.
- Flag cases where closed samples accumulate but remain uninformative or concentrated.

Why it matters:

Outcome maturity is decision-relevant but downstream. Atlas should avoid misdiagnosing the symptom as the root cause.

What it should not be used for:

- It should not encourage waiting as the only intervention.
- It should not justify forced closure, rule changes, or new mechanisms.

Metric: Runtime/Authority Risk Guard.

Definition:

Whether runtime truth, verified graph, safety gates, or policy state allow only research monitoring versus any stronger authority.

Calculation:

- Track verified graph status and audit blocker count.
- Track trade_advice_allowed, broker_execution_allowed, live_trading_allowed, real_capital_allowed, autonomous_execution_allowed, and manual_trade_capture_allowed.

Why it matters:

Atlas must keep research metrics separate from runtime or trading authority. Current reviewed evidence repeatedly kept trade advice, broker execution, live trading, and real capital disabled.

What it should not be used for:

- It should not be used to infer research quality.
- It should not be softened by dashboard wording.

## Evidence Flow Metrics

Metric: Hypothesis-To-Candidate Flow.

Definition:

Whether hypotheses or sleeves produce candidate flow, and whether absence of candidates is expected or blocked.

Calculation:

- Count sleeves by FLOWING, HEALTHY_NO_CANDIDATE, NEEDS_DATA, NEEDS_REPAIR, NEEDS_GOVERNANCE, DORMANT, BLOCKED, and UNDERPRODUCING.
- Track no-signal, threshold-not-met, data, governance, and paper-path reasons.

Why it matters:

It identifies where evidence cannot start moving.

What it should not be used for:

- It should not diagnose sleeve weakness without conditions and blockers.
- It should not be optimized for maximum candidate output.

Metric: Candidate-To-Paper Flow.

Definition:

Whether valid candidate contracts become paper observations.

Calculation:

- Valid candidate contracts divided by raw signals.
- Paper observations divided by valid candidate contracts.
- Split rejected rows by expected suppression, certification, data, contract, and quality categories.

Why it matters:

It is the highest upstream leverage for distributed future evidence maturity.

What it should not be used for:

- It should not count expected suppression as failure.
- It should not substitute for outcome evidence.

Metric: Paper-Observation-To-Outcome Flow.

Definition:

Whether paper observations become closed or resolved outcomes.

Calculation:

- Closed outcomes divided by paper positions.
- Open outcomes by sleeve and close-condition status.
- Near-closure or deterministic follow-through categories where existing artifacts provide them.

Why it matters:

It is the strongest immediate bottleneck for included sample growth.

What it should not be used for:

- It should not imply changing exit logic.
- It should not treat open positions as failed outcomes.

Metric: Outcome-To-Validation-Sample Flow.

Definition:

Whether closed or resolved outcomes become included validation samples.

Calculation:

- Included samples divided by total validation sample rows.
- Excluded samples by reason, especially open outcome exclusion.

Why it matters:

It tells Atlas whether outcome closure is actually entering the research evidence base.

What it should not be used for:

- It should not blame validation when the blocker is an open paper observation.
- It should not treat excluded open samples as negative evidence.

Metric: Validation-Sample-To-Evidence-Maturity Flow.

Definition:

Whether included samples are sufficient, distributed, independent, robust, and positive enough to mature sleeve or hypothesis evidence.

Calculation:

- Evidence maturity transitions by sleeve and hypothesis.
- Sample sufficiency gaps.
- Positive evidence status and robustness state.
- Capital-review-ready count under existing policies.

Why it matters:

It is the final research-quality conversion before capital-review eligibility can even be discussed.

What it should not be used for:

- It should not turn sample count into maturity without quality conditions.
- It should not imply readiness from one validation-ready but underpowered or negative stream.

## Concentration Metrics

Metric: Validation Sample Concentration.

Definition:

How much included validation evidence comes from the largest sleeve or hypothesis.

Calculation:

- Largest sleeve included samples divided by total included samples.
- Largest hypothesis included samples divided by total included samples.
- Count sleeves and hypotheses with nonzero included samples.

Why it matters:

The 2026-06-03 baseline was fully concentrated: all 12 included validation samples came from one hypothesis/sleeve. Atlas should expose this because aggregate sample growth can mask concentration.

What it should not be used for:

- It should not penalize a sleeve for being the first evidence source.
- It should not imply diversification by forcing candidate or sleeve changes.

Metric: Paper Position Concentration.

Definition:

How much paper-observation inventory is concentrated in the largest sleeve or hypothesis.

Calculation:

- Largest sleeve paper positions divided by total paper positions.
- Largest hypothesis paper positions divided by total paper positions.

Why it matters:

Paper concentration can predict future validation concentration. On 2026-06-03, `C2_TREND_EQ_PRIMARY_V1` held 53 of 63 paper positions.

What it should not be used for:

- It should not be interpreted as evidence maturity.
- It should not encourage reducing a working stream without evidence.

Metric: Zero-Sample Sleeve Count.

Definition:

The number of paper-active or evaluated sleeves with no included validation samples.

Calculation:

- Count evaluated sleeves with ZERO_SAMPLE status.
- Count paper-active sleeves with open positions but zero closed included samples.

Why it matters:

It shows where evidence is failing to distribute. On 2026-06-03, 4 of 5 evaluated sleeves were ZERO_SAMPLE.

What it should not be used for:

- It should not label sleeves weak by itself.
- It should not ignore expected no-signal, threshold, or data conditions.

## If Atlas Could Display Only Five Metrics

1. Distributed Mature Validation Evidence.

This is the primary North Star: included validation samples by sleeve and hypothesis, interpreted with evidence maturity status.

2. Evidence Maturity Status.

This prevents Atlas from mistaking sample count or paper workflow progress for research quality.

3. Outcome Flow: Paper Positions To Included Validation Samples.

This captures the main immediate bottleneck: 63 paper positions to 12 included samples, with 51 open or excluded on the pinned evidence day.

4. Candidate-To-Paper Conversion Quality.

This captures the highest upstream leverage for future distributed maturity, while separating expected governance suppression and certification blockers from candidate quality.

5. Evidence Concentration.

This prevents aggregate improvement from hiding the fact that learning is concentrated in one sleeve or hypothesis.

Final recommendation: Atlas should optimize for evidence flow into distributed mature validation evidence. It should display operational counts only as diagnostics, never as proof of research health.
