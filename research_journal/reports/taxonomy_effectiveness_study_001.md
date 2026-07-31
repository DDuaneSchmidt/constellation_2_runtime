# Taxonomy Effectiveness Study 001

Date: 2026-06-05
Status: GENERATED_ONLY
Mode: evaluation framework only, offline only

## Purpose

Measure whether Atlas Failure Taxonomy integration materially improves Research Adversary failure recall compared with the current baseline.

This study defines the evaluation framework only. It does not claim that taxonomy-enhanced generation has achieved the target band yet.

## Current Baseline

Source baseline: `reports/atlas_v2_research_os/research_adversary_validation/failure_recall_study_001.md`

- failures evaluated: 19
- direct hits: 0
- partial hits: 9
- misses: 10
- current failure recall: 23.7% (`0.236842`)

## Expected Taxonomy-Enhanced Range

Target range for taxonomy-enhanced Research Adversary review:

- minimum useful: 63%
- strong result: 70%+
- expected upper band: 79%

Taxonomy integration should advance only if measured recall lands in the 63-79% range without creating governance violations or materially increasing false positives.

## Comparison Design

Use the same historical failure set for both arms.

| Arm | Description | Taxonomy Fields | Authority |
| --- | --- | --- | --- |
| baseline | Existing Research Adversary review output without taxonomy lookup | disabled or ignored | unchanged, GENERATED_ONLY |
| taxonomy-enhanced | Same input artifacts reviewed with suspected taxonomy categories, related patterns, confidence, and taxonomy reasoning | enabled | unchanged, GENERATED_ONLY |

Each failure is scored against both arms using the same reviewer labels and the same matching rubric.

## Required Inputs

- Fixed historical failure set with stable failure IDs.
- Known relevant failure taxonomy labels for each historical failure.
- Baseline Research Adversary outputs.
- Taxonomy-enhanced Research Adversary outputs.
- Reviewer-labeled expected failure modes.
- Reviewer-labeled material assumptions and constraints, where available.
- Governance validator results for each generated output.

## Primary Metric

Failure recall:

`identified_relevant_failure_modes / known_relevant_failure_modes`

Scoring:

- direct hit: specific failure mode is identified clearly enough for a reviewer to act on it.
- partial hit: related failure category or failure mechanism is identified, but the failure is incomplete or underspecified.
- miss: relevant failure mode is absent.

Recommended weighted score:

`(direct_hits + 0.5 * partial_hits) / total_known_failures`

The existing 23.7% baseline should be preserved as the baseline reference unless the historical set changes. If the historical set changes, both arms must be rescored from scratch.

## Secondary Metrics

Assumption recall:

`identified_material_assumptions / reviewer_labeled_material_assumptions`

Constraint recall:

`identified_material_constraints / reviewer_labeled_material_constraints`

False positive rate:

`non_material_or_incorrect_taxonomy_objections / total_taxonomy_objections`

Authority violation rate:

`outputs_with_authority_violation / total_outputs`

Reviewer burden:

`taxonomy_review_minutes - baseline_review_minutes`

## Success Criteria

Taxonomy-enhanced generation succeeds only if all conditions hold:

- failure recall is at least 63%.
- failure recall improves from the 23.7% baseline by at least 39.3 percentage points.
- no authority violation is present.
- all generated outputs remain GENERATED_ONLY.
- false positive rate is no greater than 30%.
- assumption recall does not regress.
- constraint recall does not regress.
- reviewer burden is justified by the recall gain.

## Rejection Criteria

Reject or redesign taxonomy integration if any condition holds:

- taxonomy-enhanced failure recall remains below 63%.
- taxonomy labels increase generic warnings without improving direct or partial hits.
- false positive rate exceeds 30%.
- any output expands authority or implies replay, qualification, candidate, governance, capital, trading, broker, paper placement, or position sizing authority.
- taxonomy-enhanced review takes materially longer to review without a clear recall gain.

## Evaluation Procedure

1. Freeze the historical failure set and label file.
2. Generate baseline reviews with taxonomy fields disabled or ignored.
3. Generate taxonomy-enhanced reviews with taxonomy lookup and pattern matching enabled.
4. Score each failure as direct hit, partial hit, or miss for both arms.
5. Compute weighted failure recall for both arms.
6. Compute assumption recall, constraint recall, false positive rate, authority violation rate, and review burden.
7. Compare taxonomy-enhanced recall against the 63-79% expected range.
8. Record deltas and reviewer notes.

## Reporting Template

| Metric | Baseline | Taxonomy-Enhanced | Delta | Pass |
| --- | ---: | ---: | ---: | --- |
| failure recall | 23.7% | TBD | TBD | TBD |
| direct hits | 0 | TBD | TBD | TBD |
| partial hits | 9 | TBD | TBD | TBD |
| misses | 10 | TBD | TBD | TBD |
| assumption recall | TBD | TBD | TBD | TBD |
| constraint recall | TBD | TBD | TBD | TBD |
| false positive rate | TBD | TBD | TBD | TBD |
| authority violation rate | 0% required | 0% required | 0 | required |
| reviewer burden | TBD | TBD | TBD | TBD |

## Authority Boundary

This study is evaluation-only.

No live trading authority is added.
No broker execution authority is added.
No capital authority is added.
No automatic paper placement authority is added.
No replay authority is added.
No qualification authority is added.
No candidate authority is added.
No governance authority is added.
No position sizing authority is added.

All taxonomy-enhanced outputs must remain GENERATED_ONLY.

## Decision

Proceed to measurement only after the historical failure set, labels, and scorer are frozen. Taxonomy integration should advance only if the measured taxonomy-enhanced arm reaches 63-79% failure recall and the added review burden is lower than the value of the recovered failure modes.
