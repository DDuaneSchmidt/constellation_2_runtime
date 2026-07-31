# Atlas Candidate Quality Framework 001

Date: 2026-06-04
Status: Design only
Runtime posture: Not implemented, not enabled, not certified

Purpose: define how Atlas can evaluate whether Research OS learning improves candidate generation quality without promoting any candidate, altering candidate factory logic, creating capital artifacts, creating trade artifacts, creating sleeve artifacts, creating portfolio artifacts, or authorizing live use.

Runtime truth note: this framework treats the runtime truth kernel and verified runtime graph as the only source of runtime readiness. Candidate-quality evaluation is research measurement only. No consumer may infer trading readiness, candidate promotion eligibility, capital approval, or live authority from these metrics.

## 1. Evaluation Question

Primary question:

Does Research OS learning reduce candidate-generation failure and improve evidence maturity versus baseline?

The framework evaluates candidate-generation quality, not investment approval. A positive result means Atlas learning may have helped produce fewer repeated failures, stronger evidence lineage, and better hypothesis linkage under controlled conditions. It does not mean any candidate, strategy, sleeve, portfolio action, trade, or live use is approved.

## 2. Baseline Definition

Baseline measurement captures candidate-generation behavior before Atlas Research OS learning can influence the process.

The baseline must be measured from a fixed candidate factory version, fixed input universe, fixed governance state, and reproducible observation or replay window. The baseline records what happens when raw signals move through the existing candidate path without Research OS learning as an input.

Required baseline observations:

- raw signals observed
- candidates generated
- conversion rejections
- gate suppressions
- portfolio-scoring rejections
- evidence maturity
- hypothesis linkage
- repeated failure patterns

Baseline records must preserve lineage to the source signal set, candidate factory version, runtime graph hash, governance policy hash, market context, and evaluation window. Generated-only or mock-only artifacts may be included as labeled observations, but they cannot be treated as production improvement evidence.

## 3. Treatment Definition

Treatment measurement captures candidate-generation behavior after Atlas Research OS learning is allowed to influence candidate generation as labeled input.

The treatment must use:

- the same signal universe as baseline
- the same candidate factory version as baseline
- the same time window or a fixed replay window equivalent to baseline
- the same governance and scoring contracts unless explicitly recorded as an experimental variable
- Research OS learning only as labeled input

Research OS learning must not become implicit authority. Every learning-derived input must identify its source learning node, evidence level, lineage parents, label integrity status, and allowed use. Treatment evaluation must distinguish whether a quality change came from learning input, unrelated market-state differences, governance changes, code changes, or candidate factory version drift.

## 4. Core Metrics

The evaluation report must compute the following metrics for both baseline and treatment.

```text
candidate_conversion_rate =
  generated_candidates / raw_signals

rejection_rate =
  rejected_candidates / raw_signals

portfolio_scoring_pass_rate =
  portfolio_scoring_passes / generated_candidates

evidence_maturity_score =
  weighted average evidence level of supporting artifacts

hypothesis_survival_rate =
  hypotheses_not_falsified / hypotheses_tested

repeated_failure_reduction =
  repeated_failures_after / repeated_failures_before
```

Metric interpretation:

- `candidate_conversion_rate` should be interpreted with failure context. A higher conversion rate is not automatically better if evidence maturity falls or forbidden artifacts appear.
- `rejection_rate` should separate useful rejection from repeated avoidable failure. Some rejection is expected and healthy.
- `portfolio_scoring_pass_rate` measures quality at the scoring boundary only; it does not authorize portfolio use.
- `evidence_maturity_score` measures support quality, not trade readiness.
- `hypothesis_survival_rate` measures whether tested hypotheses remain unfalsified under declared tests, not whether they are approved.
- `repeated_failure_reduction` is favorable only when the ratio falls below `1.0` without reducing test coverage or hiding failures.

## 5. Evidence Maturity Weights

Proposed evidence maturity weights:

| Evidence level | Weight |
| --- | ---: |
| `GENERATED_ONLY` | 0.10 |
| `MOCK_ONLY` | 0.15 |
| `HISTORICAL_REPLAY` | 0.40 |
| `PAPER_FORWARD_OBSERVATION` | 0.70 |
| `EXTERNALLY_VALIDATED` | 0.90 |
| `OPERATOR_APPROVED` | 0.95 |

`OPERATOR_APPROVED` means an operator approved the research artifact or evaluation classification for candidate-quality measurement. It is not capital approval, trade approval, sleeve approval, portfolio approval, strategy approval, or live-use approval.

Evidence maturity should be calculated across supporting artifacts after label validation. Unsupported artifacts, missing-lineage artifacts, authority-contaminated artifacts, or artifacts with broken hashes must be excluded or assigned a failure state rather than silently scored.

## 6. Experimental Design

The framework supports multiple comparison methods. Each method must preserve lineage, fixed inputs, and explicit authority boundaries.

### Before/After

Compare a baseline window before Research OS learning influence with a treatment window after labeled Research OS learning input is available.

This method is operationally simple but vulnerable to market regime drift, input drift, and governance changes. It should include confidence notes explaining any non-learning differences between windows.

### Fixed Replay Set

Replay a fixed historical signal set through the candidate evaluation path once with learning disabled and once with labeled learning enabled.

This is the preferred design for first certification because the signal universe, candidate factory version, and replay window can be held constant. Replay output remains research evidence unless separately certified through other systems.

### A/B Labeled Learning Enabled vs Disabled

Run two controlled evaluation arms:

- control: candidate evaluation with learning disabled
- treatment: candidate evaluation with labeled Research OS learning enabled

Both arms must use the same signal universe, factory version, governance state, market context snapshot, and scoring contracts. The treatment arm must record every learning-derived input consumed.

### Failure-Category Distribution Comparison

Compare the distribution of failure categories before and after learning influence. Required categories should include conversion rejection, gate suppression, portfolio-scoring rejection, evidence insufficiency, hypothesis mismatch, duplicate or repeated failure, stale context, and forbidden-artifact risk.

The goal is not to eliminate rejection. The goal is to reduce avoidable repeated failure while preserving safety and evidence rigor.

### Repeated-Failure Cohort Tracking

Track cohorts of repeated failure patterns across baseline and treatment. A cohort should be defined by mechanism tag, failure type, source lineage, regime context, and candidate factory version.

Treatment improves quality only when repeated failure cohorts shrink, retire, or become better-labeled without disappearing through weaker measurement or hidden suppression.

## 7. Guardrails

Hard rules:

- improvement in metrics does not authorize trading
- candidate quality certification is separate from capital certification
- mock-only results cannot count as production improvement
- generated-only learning must remain labeled
- no strategy is approved by this framework

Additional guardrails:

- The framework must not implement candidate promotion.
- The framework must not modify candidate factory logic.
- The framework must not create capital, trade, sleeve, or portfolio artifacts.
- The framework must not create live recommendations.
- The framework must not let learning-derived labels overwrite runtime truth, verified graph state, governance policy, or operator authority.
- Any authority contamination invalidates the evaluation result until repaired and re-run.

## 8. Reports

Future evaluation runs should emit reports under:

```text
reports/atlas_candidate_quality/YYYY-MM-DD/
  candidate_quality_evaluation.v1.json
  candidate_quality_summary.md
```

`candidate_quality_evaluation.v1.json` should include:

- report id
- report version
- generated timestamp
- evaluation window
- replay window, when applicable
- signal universe id and hash
- candidate factory version
- candidate factory hash, when available
- runtime graph path and hash
- runtime truth kernel path and hash
- governance policy hash
- learning input manifest
- baseline metrics
- treatment metrics
- delta
- confidence notes
- limitations
- safety audit result
- recommendation

`candidate_quality_summary.md` should include:

- concise human-readable evaluation question
- baseline summary
- treatment summary
- metric deltas
- failure-category changes
- repeated-failure cohort changes
- evidence maturity interpretation
- lineage and label-integrity notes
- limitations
- safety audit result
- recommendation

Recommendations must use research-only language such as `CONTINUE_RESEARCH`, `REPLAY_AGAIN`, `INSUFFICIENT_EVIDENCE`, `LEARNING_EFFECT_OBSERVED`, or `LEARNING_EFFECT_NOT_OBSERVED`. Recommendations must not use language that implies candidate approval, capital approval, trading approval, sleeve approval, portfolio approval, or live authorization.

## 9. Certification Criteria

Minimum candidate-quality certification target:

- measurable reduction in repeated failure rate
- no increase in forbidden artifacts
- no authority contamination
- clear lineage from learning to candidate-quality change
- reproducible evaluation

Certification must also require:

- fixed or explicitly versioned signal universe
- fixed candidate factory version
- declared runtime graph and truth-kernel references
- complete learning input manifest
- label integrity pass
- safety audit pass
- repeatable report generation

Candidate-quality certification means only that the evaluation framework observed a reproducible candidate-quality improvement under declared conditions. It does not approve any candidate, strategy, capital allocation, trade, sleeve, portfolio action, broker action, or live use.
