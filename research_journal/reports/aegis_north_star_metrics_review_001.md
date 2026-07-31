# AEGIS North Star Metrics Review 001

Objective: identify the smallest set of metrics that best represent AEGIS research health.

Scope: existing evidence only. This review adds no architecture, schemas, mechanisms, runtime truth changes, candidate rules, sleeve changes, governance changes, trade advice, candidates, or allocation authority.

Pinned evidence day: 2026-06-03.

Inputs:
- `evidence_flow_bottleneck_review_001.md`
- `outcome_bottleneck_decomposition_review_001.md`
- `root_cause_review_001.md`
- `journal_review_001.md`
- `journal_review_002.md`
- `fragile_conclusion_review_001.md`

## Evidence Base

- Evidence Flow Bottleneck Review 001 found the highest same-day numeric drop was candidate conversion: 42 raw signals to 1 generated candidate and 1 valid contract. It also found the highest decision-readiness drop was 63 paper positions to 12 included validation samples, with 51 open and excluded.
- Outcome Bottleneck Decomposition Review 001 found outcome maturity is the primary downstream decision bottleneck, but the deeper root bottleneck is measurable, distributed evidence flow through candidate, paper observation, outcome, validation sample, and evidence maturity.
- Root Cause Review 001 found AEGIS had 63 paper trades, 51 open positions, 12 included validation samples, 0 validated hypotheses, and 0 capital-review-ready rows. It ranked underpowered outcomes, narrow candidate conversion, concentrated sleeve evidence, paper-path/data blockers, and weak early realized performance as the main causes.
- Journal Review 001 found OBS_0006 SUPPORTED, OBS_0007 SUPPORTED, OBS_0017 MIXED_CLASS, OBS_0012 STILL_NOT_READY, OBS_0013 APPROPRIATELY_CONSERVATIVE, and OBS_0014 SUPPORTED. These reviews repeatedly rejected raw volume and capital-review framing as current health indicators.
- Journal Review 002 ranked outcome maturity as the strongest bottleneck, candidate conversion/certification/governance interpretation as a strong bottleneck, and evidence contract/certification resolution as a concrete high-leverage bottleneck.
- Fragile Conclusion Review 001 judged the most robust conclusion to be that research attention allocation is currently more useful than capital allocation, and judged candidate volume as a poor quality proxy with high confidence.

## Misleading Metrics

Raw signal count.

Classification: Operational Metric / Anti-Metric when used as research health.

Raw signals measure upstream activity, not learning quality. On 2026-06-03, 42 raw signals produced only 1 generated candidate and 1 valid contract. Journal Review 001 showed raw-signal rows mixed expected portfolio-gate suppression, certification bottlenecks, and actual candidate-quality questions. Raw signal count is useful for funnel diagnostics, but misleading as a health indicator.

Candidate count.

Classification: Operational Metric / Anti-Metric when used as research health.

Candidate volume repeatedly failed as a quality proxy. OBS_0007 found candidate count did not correlate cleanly with accepted contracts, outcome maturity, or evidence maturity. The event-dislocation example had candidate inventory without paper positions or usable validation samples, while all evaluated sleeves remained UNDERPOWERED despite candidate and paper-trade inventories.

Paper position count.

Classification: Operational Metric / Anti-Metric when used as research health.

Paper positions show that observation workflow exists, but they do not prove mature evidence. AEGIS had 63 paper positions on 2026-06-03, yet 51 were still open and excluded from validation samples. Paper position count can grow while evidence remains open, concentrated, underpowered, or negative.

Generated hypothesis count or search-space expansion.

Classification: Operational Metric / Anti-Metric when used as research health.

The reviewed evidence did not show lack of ideas as the binding constraint. Outcome Bottleneck Decomposition Review 001 ranked raw search-space or hypothesis count lowest among supported causal levers. AEGIS needs hypotheses to move into measurable evidence, not simply more generated ideas.

Capital-review count before evidence maturity.

Classification: Research Quality Metric only when nonzero under existing readiness standards / Anti-Metric if treated as a target.

Capital-review readiness is a valid terminal quality surface, but trying to maximize it before mature evidence exists would be misleading. Research quality, decision policy, allocation recommendation, and capital authority surfaces all showed 0 capital-review-ready rows. The current system health question is whether AEGIS is learning, not whether it can justify capital review.

## Useful Metrics

Distributed included validation samples by sleeve and hypothesis.

Classification: Learning Metric and Research Quality Metric.

This is the strongest repeated metric because it captures whether paper research is becoming usable evidence and whether that evidence is broad enough to reduce concentration risk. On 2026-06-03, AEGIS had 12 included validation samples, all concentrated in `HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1` / `C2_TREND_EQ_PRIMARY_V1`. The absence of samples for other paper-active sleeves explained why aggregate activity did not translate into broad maturity.

Evidence maturity status.

Classification: Research Quality Metric.

Sleeve evidence certification and statistical sufficiency directly represented whether observations had become credible research evidence. On 2026-06-03, all 5 evaluated sleeves were UNDERPOWERED, 4 were ZERO_SAMPLE, 1 was BUILDING_SAMPLE, 0 had positive evidence, 8 of 9 hypotheses were underpowered, and 0 were validated.

Outcome maturity.

Classification: Learning Metric and Research Quality Metric.

Outcome maturity is the clearest downstream bottleneck and the most visible prerequisite for stronger research claims. It should be measured as closed/resolved, included, usable outcomes rather than total paper positions. The key 2026-06-03 contrast was 63 paper positions versus 12 included validation samples.

Open-to-closed conversion by sleeve.

Classification: Learning Metric.

This metric explains whether paper observations are moving into usable sample flow. On 2026-06-03, 51 open positions blocked sample inclusion, with Trend EQ holding 41 open positions and the other paper-active sleeves holding open zero-sample positions.

Candidate-to-paper conversion quality.

Classification: Operational Metric and Learning Metric.

The useful version is not raw candidate count. It separates expected portfolio suppression, certification status, valid governed contract creation, portfolio-scoring status, paper observation creation, and eventual closed sample inclusion. This conversion-quality view is the metric that prevents AEGIS from mistaking raw throughput for learning.

Capital-review-ready rows under existing policy.

Classification: Research Quality Metric.

This remains useful as a guardrail, not as the current North Star. It should stay at 0 until sufficient, positive, distributed, closed-outcome evidence exists. A nonzero value would matter only if produced by existing research quality, decision-policy, allocation-recommendation, and capital-readiness artifacts under their current standards.

## North Star Metric

Primary Metric:

Distributed mature validation evidence.

Definition:

Count of sleeves and hypotheses with included validation samples that have advanced from ZERO_SAMPLE or UNDERPOWERED toward BUILDING_SAMPLE, VALIDATION_READY, or VALIDATED status, while preserving evidence quality, sample independence, certification, and governance separation.

Practical expression:

`included_validation_samples_by_sleeve_and_hypothesis`, interpreted together with `evidence_maturity_status`.

Reason:

This metric best predicts future research quality because it measures the point where AEGIS stops merely generating activity and starts accumulating reusable learning. It catches the problems that repeatedly mattered tonight: open outcomes, sample scarcity, sleeve concentration, certification/actionability gaps, and premature capital-review pressure.

It is deliberately not a single aggregate sample count. Aggregate included samples can improve while learning remains concentrated in one sleeve. The metric must remain distributed by sleeve and hypothesis.

## Supporting Metrics

Secondary Metrics:

1. Outcome maturity by sleeve and hypothesis.
Classification: Learning Metric / Research Quality Metric.
Use: track closed or resolved outcomes, included validation samples, excluded open samples, and open-to-closed conversion.

2. Evidence maturity status by sleeve and hypothesis.
Classification: Research Quality Metric.
Use: track ZERO_SAMPLE, BUILDING_SAMPLE, UNDERPOWERED, VALIDATION_READY, VALIDATED, positive evidence, robustness, sample independence, and capital-review eligibility.

3. Candidate-to-paper conversion quality.
Classification: Operational Metric / Learning Metric.
Use: track raw signal to expected suppression, certification blocker, valid governed contract, paper observation, closed outcome, and included validation sample.

4. Evidence concentration.
Classification: Learning Metric / Research Quality Metric.
Use: track how much usable evidence comes from the largest sleeve or hypothesis and how many sleeves have at least one included validation sample.

5. Research attention decision usefulness.
Classification: Research Quality Metric.
Use: track whether policy outputs remain differentiated and evidence-supported through CONTINUE, REDESIGN, NEEDS_DATA, DECREASE_ATTENTION, HOLD, PAUSE, or capital-review states. This is currently more useful than capital allocation.

Diagnostic Metrics:

- Raw signals by sleeve.
- Candidate count by sleeve.
- Portfolio-gate suppression count split by expected versus bottleneck.
- Certification failures or pending states.
- Valid governed candidate contracts.
- Paper positions created.
- Open paper positions by sleeve.
- Close-condition proximity for open positions.
- Data-source blockers and paper-path blockers.
- Verified graph and runtime truth blocker counts.

These metrics should explain where evidence flow stalls. They should not be promoted to overall health indicators.

## Anti-Metrics

Anti-Metrics are metrics that should not be used as AEGIS research-health indicators:

- Raw signal count.
- Generated candidate count.
- Total candidate inventory.
- Total paper position count.
- Generated hypothesis count.
- Number of dormant sleeves without no-signal, threshold, data, and governance context.
- Portfolio-gate suppression count without expected-versus-bottleneck separation.
- Paper workflow progress treated as live readiness.
- Architecture readiness treated as evidence maturity.
- Capital-review count treated as a target before evidence maturity exists.

These are not useless. They are operational or diagnostic. They become harmful when interpreted as learning quality or research health.

## Final Recommendation

AEGIS should use one North Star metric:

Distributed mature validation evidence.

The minimum supporting dashboard should contain:

- Included validation samples by sleeve and hypothesis.
- Evidence maturity status by sleeve and hypothesis.
- Open-to-closed outcome conversion by sleeve.
- Candidate-to-paper conversion quality with governance and certification split out.
- Evidence concentration across sleeves and hypotheses.

The metric most likely to predict future research quality is not raw signal volume, candidate volume, or paper position count. It is whether multiple sleeves and hypotheses accumulate included validation samples that mature evidence status without concentration, certification ambiguity, unresolved data blockers, or premature capital-review framing.

Final classification:

- Operational Metrics: raw signals, candidate counts, paper positions, valid contracts, suppression counts, certification states, graph/runtime blockers, data blockers.
- Learning Metrics: included validation samples, open-to-closed conversion, candidate-to-paper conversion quality, evidence concentration, sample distribution.
- Research Quality Metrics: evidence maturity, statistical sufficiency, positive sleeve evidence, policy decision appropriateness, capital-review readiness under existing standards.

AEGIS research health should be measured by learning throughput into distributed mature evidence, not by activity volume.
