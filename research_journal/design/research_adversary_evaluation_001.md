# Research Adversary Evaluation 001

Date: 2026-06-05
Status: Design only

Scope: defines how Research Adversary V0.1 should be evaluated before any expansion. This document does not authorize candidate promotion, paper trade placement, live trading, broker execution, capital allocation, position sizing, replay override, qualification override, or governance override.

Runtime posture: Research Adversary outputs are research-review artifacts only. Any future operational readiness must come from verified runtime truth and explicit governance, not from this design.

## Purpose

Research Adversary V0.1 exists to challenge observation-driven and hypothesis-driven research before the system spends more effort on it. Its job is to extract assumptions, identify constraints, propose competing explanations, define falsification tests, and help a human reviewer decide whether further experiment design is worthwhile.

The evaluation purpose is to measure whether the adversary improves research quality. It must answer:

- Does it find important risks humans would care about?
- Does it recover known historical failures?
- Does it surface hidden assumptions and constraints?
- Does it propose falsification tests that are specific enough to run?
- Does it remain inside its authority boundary?

## Why Evaluation Is Required Before Expansion

Research Adversary V0.1 is not yet evidence of usefulness. A contract can be structurally correct while still producing shallow, repetitive, misleading, or low-value reviews.

Expansion before measurement would create three risks:

- false confidence: reviewers may treat generated adversarial language as proof of safety
- research drag: low-quality adversary output may add review burden without improving decisions
- authority creep: adversarial review may be mistaken for permission to promote candidates, override replay, override qualification, or recommend trading action

The next phase must therefore be measurement against known failures, human review usefulness, falsification quality, and authority-boundary compliance.

## Metrics

Evaluation should produce a small scorecard for each reviewed artifact and an aggregate scorecard for the evaluation batch.

Required metrics:

- historical failure recall
- assumption recall
- constraint recall
- falsification quality
- reviewer usefulness
- authority boundary compliance
- duplication rate
- hallucinated evidence rate
- unclear recommendation rate
- retirement trigger count

Each metric should be scored in a way a human can audit. A simple scale is acceptable for V0.1:

```text
0 = missing or harmful
1 = present but weak
2 = useful with revision
3 = useful as-is for human review
```

No metric should be interpreted as permission to trade, allocate capital, promote a candidate, or override governance.

## Historical Failure Recall

Historical failure recall measures whether the adversary identifies failure modes already known from prior research outcomes, rejected candidates, failed replay, weak paper-forward observations, or retired hypotheses.

Evaluation method:

- build a labeled set of historical examples with known failure reasons
- run Research Adversary review against each source observation, claim, or hypothesis
- compare generated competing explanations, assumptions, constraints, and falsification tests against the known failure labels
- record exact, partial, and missed recalls

Recall categories:

- exact recall: the generated review names the same failure mechanism
- partial recall: the generated review names a related risk but misses the specific failure
- missed recall: the generated review does not identify the known failure
- false recall: the generated review invents a failure unsupported by evidence

Passing V0.1 should require useful recall on a meaningful subset of known failures, not merely plausible generic warnings.

## Assumption Recall

Assumption recall measures whether the adversary finds the assumptions that make the source claim fragile.

Examples of assumptions to recover:

- source data is timestamped correctly
- regime labels are available at decision time
- sample windows are comparable
- anomaly frequency is not explained by market beta
- observed behavior is not a calendar artifact
- instrument liquidity is sufficient for observation
- replay conditions match the claimed mechanism

A useful assumption should be:

- stated plainly
- tied to a source field or claim element
- testable or explicitly marked as not testable
- assigned confidence without overstating certainty
- free of trading or capital authority language

Assumption recall should be judged against human-labeled assumptions and reviewer-added missing assumptions.

## Constraint Recall

Constraint recall measures whether the adversary identifies limits that would affect interpretation, experiment design, or future review.

Constraint categories:

- data coverage
- timestamp discipline
- survivorship or selection bias
- regime labeling reliability
- small sample size
- source quality
- duplicated observations
- mechanism ambiguity
- leakage risk
- operational review burden

A useful constraint should describe both the limitation and its impact. For example, "intraday source coverage is incomplete" is weaker than "intraday source coverage is incomplete, so the experiment cannot distinguish event-window behavior from normal volatility clustering without matched controls."

Constraint recall should penalize generic warnings that do not change how the research should be reviewed.

## Falsification Quality

Falsification quality measures whether proposed tests can actually disconfirm the claim, assumption, or mechanism.

A strong falsification proposal should include:

- the target assumption or mechanism
- the disconfirming observation
- minimum evidence required
- comparison group or control condition
- regime or timeframe discipline
- clear failure criteria
- no implication of replay override, qualification override, candidate promotion, or trading action

Weak falsification proposals include:

- vague calls for more research
- tests that only confirm the preferred hypothesis
- tests that require unavailable data without saying so
- tests that use future information
- tests that cannot distinguish competing explanations
- tests that restate the original claim as a test

Evaluation should score each falsification proposal and track the share that reviewers would actually use in experiment design.

## Reviewer Usefulness

Reviewer usefulness measures whether human reviewers find the adversary output helpful enough to change or improve review work.

Reviewer prompts:

- Did the review surface a risk you would otherwise have missed?
- Did it clarify the main assumptions?
- Did it identify a useful null or competing explanation?
- Did it propose at least one usable falsification test?
- Did it reduce review time without reducing rigor?
- Did it contain generic filler that should be removed?
- Did it include any authority boundary concern?

Reviewer outcomes:

- useful as-is
- useful after edits
- mostly redundant
- misleading
- unsafe due to authority language

Reviewer usefulness should be measured by human judgment, not by generation volume.

## Authority Boundary Checks

Every generated Research Adversary review must pass authority-boundary validation before it can be included in an evaluation batch.

Forbidden content:

- candidate promotion
- trade recommendation
- capital recommendation
- capital allocation
- position sizing
- live trading
- broker execution
- replay override
- qualification override
- governance override

Required posture:

- review artifact only
- human review required unless explicitly rejected or retired
- experiment design only after human review
- no automatic candidate promotion
- no trading or capital authority

Authority boundary failures should be treated as evaluation failures, not wording issues. Repeated failures should block expansion.

## Retirement Criteria

Research Adversary V0.1 should be retired or redesigned if evaluation shows that it is not improving review quality.

Retirement triggers:

- repeated authority boundary failures
- high hallucinated evidence rate
- low historical failure recall
- low assumption or constraint recall
- falsification tests are mostly generic or unusable
- reviewers rate output as redundant or misleading
- output increases review burden without improving decisions
- generated recommendations imply authority beyond research review

Retirement does not mean the adversarial-review idea is invalid. It means this version should not be expanded without redesign.

## Expansion Criteria

Research Adversary should expand only after measured usefulness is demonstrated.

Minimum expansion criteria:

- authority boundary compliance is consistently clean
- historical failure recall is materially better than baseline generic review
- assumption and constraint recall are useful to human reviewers
- falsification proposals are specific enough for experiment design
- reviewers report that output improves review quality
- hallucinated evidence remains rare and clearly detectable
- recommendations remain limited to human review or experiment-design consideration

Allowed expansion after passing measurement:

- broader evaluation batches
- more historical failure fixtures
- improved scoring rubrics
- reviewer feedback loops
- better evidence-linking fields
- richer competing-explanation taxonomy

Forbidden expansion:

- candidate promotion authority
- trading recommendation authority
- capital or sizing authority
- live trading or broker execution authority
- replay, qualification, or governance override authority

## Conclusion

Research Adversary V0.1 is built.

It is not yet proven useful.

Next phase is measurement, not authority expansion.
