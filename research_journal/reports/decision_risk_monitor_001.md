# Decision-Risk Monitor 001

Objective: turn `fragile_conclusion_review_001.md` into a read-only monitoring plan for conclusions that are fragile or dangerous if wrong.

Scope: existing Research Journal evidence only. This monitor does not implement automation, add architecture, modify schemas, modify sleeves, change candidates, change validation logic, change runtime truth, modify trading logic, or alter capital allocation.

Inputs:
- `research_journal/reports/fragile_conclusion_review_001.md`
- `research_journal/reports/aegis_misdiagnosis_review_001.md`
- `research_journal/reports/journal_review_002.md`
- `research_journal/reports/outcome_evidence_concentration_watch_001.md`

## 1. Executive Summary

Two conclusions require active read-only monitoring:

1. Fragile conclusion: low candidate production is not primarily sleeve weakness.
2. Dangerous-if-wrong conclusion: research attention allocation is more useful than capital allocation.

The robust conclusion is also the dangerous-if-wrong conclusion: research attention allocation is more useful than capital allocation. It is robust because all reviewed capital-readiness surfaces showed 0 eligible rows while research triage produced differentiated read-only value. It remains dangerous if wrong because premature capital framing could create false confidence, allocation pressure, or implied trade advice.

The monitor should not make decisions. It should identify when existing evidence has changed enough to justify re-review.

## 2. Conclusions To Monitor

1. Low candidate production is not primarily sleeve weakness.
   - Current status: FRAGILE.
   - Current confidence: MEDIUM.
   - Reason to monitor: existing evidence rejects a broad weak-sleeve diagnosis, but it does not prove sleeve health.

2. Research attention allocation is more useful than capital allocation.
   - Current status: DANGEROUS_IF_WRONG and ROBUST.
   - Current confidence: HIGH.
   - Reason to monitor: the conclusion is strongly supported today, but any false shift toward capital readiness would have high decision risk.

3. Outcome maturity is the primary bottleneck.
   - Current status: SECONDARY_MONITOR.
   - Current confidence: HIGH with upstream-cause caveat.
   - Reason to monitor: outcome maturity may be a downstream symptom of candidate conversion, paper-path, or data-authority bottlenecks.

4. Candidate volume is a poor proxy for quality.
   - Current status: SECONDARY_MONITOR.
   - Current confidence: HIGH.
   - Reason to monitor: volume may still be useful as a stage-specific throughput signal after governance and certification effects are separated.

5. Technical indicator claims are weak.
   - Current status: SECONDARY_MONITOR.
   - Current confidence: MEDIUM-HIGH for broad standalone claims; MEDIUM for all technical evidence.
   - Reason to monitor: broad indicator claims are weak, but context-linked technical structures remain incompletely tested.

## 3. Why Each Conclusion Matters

Low candidate production is not primarily sleeve weakness:

If this conclusion is wrong, AEGIS may over-prioritize outcome follow-through and under-prioritize sleeve redesign, trigger calibration, data repair, implementation repair, and paper-path reconstruction.

Research attention allocation is more useful than capital allocation:

If this conclusion is wrong because capital review becomes genuinely supported, AEGIS may delay useful review of validated evidence. If it is wrong in the opposite direction and capital framing appears too early, the risk is more severe: false confidence, premature allocation logic, and accidental pressure toward trade advice.

Outcome maturity is the primary bottleneck:

If this conclusion is wrong, AEGIS may wait for deterministic closures while the true bottleneck remains candidate conversion, paper-path design, or data authority.

Candidate volume is a poor proxy for quality:

If this conclusion is wrong, AEGIS may underweight high-throughput discovery surfaces that are actually improving evidence maturity.

Technical indicator claims are weak:

If this conclusion is wrong, AEGIS may underinvest in technical discovery surfaces that become valid under relationship-specific, out-of-fixture, baseline-adjusted validation.

## 4. Evidence That Would Strengthen Each Conclusion

Low candidate production is not primarily sleeve weakness:

- Continued evidence that non-producing sleeves are classified as HEALTHY_NO_CANDIDATE under valid no-signal or threshold-not-met conditions.
- Data and paper-path blockers resolving without revealing broad sleeve-quality failures.
- First closed samples from previously zero-sample sleeves showing that low prior production reflected immature evidence flow rather than weak sleeve logic.
- Candidate diagnostics continuing to separate expected governance suppression from certification, data, or quality failures.

Research attention allocation is more useful than capital allocation:

- Research quality, hypothesis decision policy, and allocation recommendation artifacts continue to show 0 READY_FOR_CAPITAL_REVIEW or CAPITAL_REVIEW rows.
- Research attention artifacts continue to produce useful read-only triage decisions.
- Outcome evidence remains underpowered, concentrated, negative, or otherwise insufficient for capital review.
- Runtime truth and policy gates continue to keep trade advice, broker execution, live trading, and real capital disabled.

Outcome maturity is the primary bottleneck:

- Additional reviews show closed samples remain scarce, concentrated, or statistically insufficient.
- Open paper positions continue to dominate validation sample exclusions.
- Zero-sample sleeves remain blocked primarily by unresolved deterministic closure rather than missing paper paths or data authority.

Candidate volume is a poor proxy for quality:

- High candidate counts continue to fail to produce valid contracts, paper positions, included closed samples, or positive evidence.
- Candidate counts remain mixed with expected portfolio suppression, certification status, and paper-observation state.
- Low or moderate candidate volume produces better evidence quality than raw volume would predict.

Technical indicator claims are weak:

- Broad or standalone technical claims continue to fail false-positive, baseline, out-of-fixture, regime, or sample-independence tests.
- Context-linked technical structures remain unproven after stricter validation.
- Momentum-adjacent evidence remains measurable but weak, underpowered, or not representative.

## 5. Evidence That Would Weaken Each Conclusion

Low candidate production is not primarily sleeve weakness:

- HEALTHY_NO_CANDIDATE sleeves repeatedly fail to produce candidates during favorable, threshold-satisfying market conditions.
- No-signal or threshold explanations are shown to be stale, miscalibrated, or masking defective sleeve design.
- Data and paper-path repairs complete but candidate production and usable outcomes remain absent.
- Closed-outcome samples show low-producing sleeves consistently produce weak results once measurable.

Research attention allocation is more useful than capital allocation:

- Existing artifacts emit one or more hypotheses or sleeves as READY_FOR_CAPITAL_REVIEW or CAPITAL_REVIEW with sufficient, positive, distributed, closed-outcome evidence.
- Capital-review-style analysis improves research judgment while remaining explicitly read-only and non-trading.
- Runtime truth and policy gates change only after mature validation evidence exists and capital review becomes a legitimate review surface.

Outcome maturity is the primary bottleneck:

- Closed outcomes accumulate but remain uninformative because candidate quality, paper-path design, validation structure, or data authority is defective.
- Most existing open paper positions cannot create material validation maturity under deterministic closure rules.
- Root-cause review shows candidate conversion, data readiness, or implementation repair blocks more learning than closed-sample scarcity.

Candidate volume is a poor proxy for quality:

- Cross-sleeve evidence shows higher candidate volume consistently predicts valid contracts, paper positions, included validation samples, and positive evidence after certification and governance effects are controlled.
- Low-volume sleeves repeatedly remain low-learning while high-volume sleeves repeatedly improve validation quality.

Technical indicator claims are weak:

- Out-of-fixture, baseline-adjusted, relationship-specific technical tests produce durable support.
- Closed-outcome evidence shows technical structures improve discovery quality or benchmark-relative results without hindsight dependence.
- Technical claims survive false-positive, regime, sample-independence, and competing-baseline controls.

## 6. Trigger Conditions For Re-Review

Re-review low candidate production if any of the following occur:

- A sleeve classified as HEALTHY_NO_CANDIDATE remains candidate-empty across multiple favorable, threshold-satisfying review periods.
- A NEEDS_DATA or paper-path blocker is resolved and the sleeve still produces no candidates, paper observations, or usable outcomes.
- Any previously zero-sample sleeve produces first closed samples that materially contradict the current sleeve-health interpretation.
- Dormant-sleeve diagnostics shift from valid no-signal or threshold-not-met causes toward NEEDS_REPAIR, NEEDS_GOVERNANCE, implementation defects, or repeated runtime defects.

Re-review research attention versus capital allocation if any of the following occur:

- Any existing research quality, hypothesis decision policy, or allocation recommendation artifact emits READY_FOR_CAPITAL_REVIEW or CAPITAL_REVIEW.
- Any sleeve or hypothesis reaches sufficient, positive, distributed, closed-outcome evidence.
- Capital-related language appears in a way that could be mistaken for trade sizing, trade advice, live capital allocation, or broker execution.
- Runtime truth or policy gates change for capital allocation, trade advice, broker execution, autonomous execution, or live trading.

Re-review outcome maturity if any of the following occur:

- Closed samples increase materially but remain concentrated in `C2_TREND_EQ_PRIMARY_V1`.
- Any non-Trend sleeve produces an authoritative included closed validation sample, reducing the 12 Trend EQ / 0 non-Trend baseline.
- Existing open positions remain unable to close under deterministic rules across multiple refreshes.
- Data authority or evidence-contract blockers prevent otherwise valid outcomes from counting.

Re-review candidate volume if any of the following occur:

- A high-volume sleeve repeatedly converts raw signals into valid contracts, paper positions, included closed samples, and positive evidence.
- A high-candidate hypothesis with no paper positions begins producing usable validation samples.
- Candidate diagnostics show raw volume becoming a reliable early signal after expected suppression and certification states are separated.

Re-review technical indicator claims if any of the following occur:

- Relationship-specific technical structures pass hostile out-of-fixture validation.
- Technical evidence survives baseline, false-positive, regime, and sample-independence controls.
- Momentum-adjacent closed-outcome evidence becomes positive, sufficiently sampled, and benchmark-relevant.

## 7. Recommended Monitoring Cadence

- After each authoritative outcome refresh: check outcome concentration, first non-Trend closed samples, and capital-readiness outputs.
- Weekly: re-read sleeve throughput diagnostics, dormant-sleeve diagnostics, candidate diagnostics, and Research Journal conclusions for changes in sleeve-health interpretation.
- After any data or paper-path repair: re-check whether repaired sleeves produce candidates, paper observations, or usable closed outcomes.
- After any Technical Strategy Factory validation artifact: re-check whether broad standalone claims remain weak or whether relationship-specific technical evidence has improved.
- Immediately: re-review if any artifact emits READY_FOR_CAPITAL_REVIEW, CAPITAL_REVIEW, positive sleeve evidence, validated hypothesis status, or a runtime truth policy change touching capital or trading authority.

## 8. Explicit Non-Actions

- No capital allocation changes.
- No sleeve retirement.
- No candidate rule changes.
- No architecture expansion.
- No autonomous policy changes.
- No trade advice.
- No manual trade capture.
- No runtime truth changes.
- No validation logic changes.
- No forced outcome closure.
- No exit-rule changes.
- No sleeve behavior changes.
- No dashboards, registries, APIs, databases, workflow systems, or automation pipelines.

## Recommended Next Evidence Check

Run a read-only follow-up after the next authoritative outcome refresh and answer only:

- Did any non-Trend sleeve produce an authoritative included closed validation sample?
- Did any artifact emit READY_FOR_CAPITAL_REVIEW or CAPITAL_REVIEW?
- Did any HEALTHY_NO_CANDIDATE sleeve remain empty under favorable, threshold-satisfying conditions?
- Did any repaired data or paper-path blocker still fail to produce measurable evidence flow?
