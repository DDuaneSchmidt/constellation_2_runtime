# Research Director Retrospective Scoring 001

## Purpose

Apply the Research Director scoring concept to historical Atlas work in an offline-only retrospective.

Question:

Would Research Director have identified Data Coverage as the highest-value bottleneck earlier?

This report is not production integration. It does not change workflow policy, candidate state, governance state, replay results, qualification results, capital allocation, paper placement, or trading authority.

## Scope

Reviewed historical work categories:

- failure reports and recurring blockers
- candidate and candidate-to-paper lifecycle evidence
- adversary-related design and review artifacts
- validation bottlenecks
- data coverage and source-readiness blockers

The scoring is retrospective and qualitative-to-structured. It is intended to test whether the Research Director framework points to the same bottleneck a human reviewer would now identify with hindsight.

## Scoring Model

Each workstream is scored from 1 to 5 on four dimensions:

- estimated research hours saved
- estimated uncertainty reduction
- estimated search-space reduction
- estimated validation acceleration

Overall value score:

`research_hours_saved + uncertainty_reduction + search_space_reduction + validation_acceleration`

Tie-breakers:

1. Prefer the workstream that removes hard blockers over the workstream that improves review quality.
2. Prefer reusable infrastructure over one-off research output.
3. Prefer bottlenecks affecting multiple downstream stages.
4. Penalize workstreams that increase review burden before they reduce blockers.

## Historical Signals

### Data Coverage Signals

Observed historical signals:

- missing or stale market data appears repeatedly as a validation and candidate-flow blocker
- macro calendar readiness requires an external source
- market data universe consistency reports missing required symbols
- generated-hypothesis validation remains blocked by data readiness / shadow validation
- outcome readiness can stall on mark source or authoritative price availability
- candidate construction can be blocked by missing current market data, entry reference price, risk estimate, sizing policy, and selected exposure context

Interpretation:

Data Coverage is a hard bottleneck because it blocks replay validation, candidate construction, paper-forward readiness, outcome certification, and daily research integrity. It is not merely a quality improvement; it determines whether downstream evidence can be trusted.

### Failure Taxonomy Signals

Observed historical signals:

- failure categories recur across Atlas research
- blockers need normalization to become reusable
- adversary review depends on known failure labels
- failure taxonomy can convert postmortems into future review prompts

Interpretation:

Failure Taxonomy is high value, but it becomes more predictive after outcomes and blockers are reliably joined to candidates and paper-forward records. It improves reasoning but does not by itself unblock validation.

### Candidate Attribution Signals

Observed historical signals:

- candidate outcomes require lineage to source hypothesis, mechanism, regime, data source, and blocker history
- without attribution, failures are hard to assign to idea quality versus data availability versus construction path
- attribution is needed to learn from paper-forward and candidate outcomes

Interpretation:

Candidate Attribution is high value, especially for learning from outcomes, but it depends on enough valid candidate and outcome data to explain.

### Research Adversary Signals

Observed historical signals:

- competing explanations, null explanations, assumption extraction, and falsification tests are valuable before review
- adversary output must remain generated-only
- adversary usefulness depends on taxonomy lookup and historical pattern matching

Interpretation:

Research Adversary is valuable, but premature scaling risks adding review burden if Data Coverage and Failure Taxonomy are not strong enough to ground its critiques.

## Retrospective Scores

| Workstream | Research Hours Saved | Uncertainty Reduction | Search-Space Reduction | Validation Acceleration | Total | Rank |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Data Coverage | 5 | 4 | 3 | 5 | 17 | 1 |
| Failure Taxonomy | 4 | 5 | 5 | 3 | 17 | 2 |
| Candidate Attribution | 3 | 5 | 3 | 4 | 15 | 3 |
| Research Adversary | 3 | 4 | 3 | 2 | 12 | 4 |

Tie-break result:

Data Coverage ranks above Failure Taxonomy because it removes hard validation blockers across multiple downstream stages. Failure Taxonomy has equal total score, but it improves classification and prioritization rather than directly resolving the evidence gap.

## Data Coverage Bottleneck Test

### Test 1: Does the bottleneck affect multiple stages?

Result: Yes.

Affected stages:

- data readiness
- replay and validation
- candidate construction
- paper-forward readiness
- outcome certification
- daily research integrity

Score implication:

Data Coverage should be elevated because it is a shared dependency rather than a local defect.

### Test 2: Does unresolved Data Coverage prevent trustworthy evaluation?

Result: Yes.

Missing or stale data prevents the system from distinguishing:

- bad hypothesis
- missing evidence
- weak source lineage
- stale market context
- unresolved outcome mark
- incomplete candidate construction

Score implication:

Data Coverage reduces uncertainty and accelerates validation, but its largest value is preventing false conclusions from incomplete evidence.

### Test 3: Would more hypothesis or adversary generation solve the bottleneck?

Result: No.

Additional generated research can identify the blocker, but it cannot resolve source availability, coverage, freshness, or lineage by itself.

Score implication:

Research Director should prioritize Data Coverage before scaling generated critique or idea generation.

### Test 4: Would Data Coverage have saved research hours earlier?

Result: Likely yes.

Estimated saved work:

- fewer repeated investigations into why validation could not close
- fewer candidate-review cycles blocked by missing inputs
- fewer manual checks of source readiness
- fewer generated hypotheses advancing into known data gaps

Score implication:

Data Coverage likely would have produced earlier and compounding time savings.

## Answer

Yes. Applying Research Director scoring retrospectively suggests Data Coverage should have been identified as the highest-value bottleneck earlier.

Reason:

Data Coverage had the strongest combination of hard-blocker removal and downstream leverage. It affected validation, candidate construction, paper-forward readiness, outcome certification, and research integrity. Other workstreams were valuable, but Data Coverage was the infrastructure prerequisite for trusting their outputs.

## What Research Director Should Have Recommended

Earlier recommendation:

1. Stop expanding generated research volume until critical data coverage blockers are classified and routed.
2. Build a current source-readiness map for required symbols, macro events, market context, and outcome marks.
3. Rank missing data by downstream blocked artifacts, not by raw symbol count.
4. Add Data Coverage as a first-class bottleneck score in research prioritization.
5. Only then fund Failure Taxonomy, Candidate Attribution, and Research Adversary expansion.

## Caveats

- This is a retrospective offline scoring exercise, not a statistically validated causal study.
- Historical data may overstate the obviousness of Data Coverage because current blockers are visible with hindsight.
- Failure Taxonomy tied Data Coverage on total score, so the conclusion depends on tie-breaker policy.
- If the historical bottleneck had been poor idea quality rather than evidence blockage, Failure Taxonomy or Research Adversary might have ranked first.

## Conclusion

Research Director scoring would likely have identified Data Coverage as the highest-value bottleneck earlier if it weighted hard validation blockers and downstream dependency breadth correctly.

The main lesson is that Research Director should prioritize bottlenecks that make evidence trustworthy before funding systems that generate more review material.
