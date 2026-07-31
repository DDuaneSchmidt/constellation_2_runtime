# Atlas Failure Prediction Study 001

## Purpose

Determine whether Atlas failure taxonomy categories can predict future failure in:

- paper-forward outcomes
- candidate outcomes

This study is research-only. It does not authorize replay override, qualification override, candidate promotion, governance approval, capital allocation, broker execution, live trading, automatic paper placement, or position sizing.

## Core Question

Can taxonomy categories observed before or during review predict later failure outcomes?

The study should answer whether recurring taxonomy labels are merely descriptive after the fact or useful as forward-looking risk indicators.

## Taxonomy Categories To Measure

### REGIME_DEPENDENCY

Definition: The research claim or candidate depends materially on a specific market regime, and performance is expected to degrade outside that regime.

Prediction hypothesis: Items with unresolved `REGIME_DEPENDENCY` should have lower paper-forward survival and higher failure rates when regime labeling is weak, stale, or too broad.

Required fields:
- taxonomy category
- regime label
- regime confidence
- regime source lineage
- outcome status
- failure reason if failed

### PROXY_DEPENDENCY

Definition: The research claim relies on proxy instruments, proxy data, or indirect measurements instead of direct evidence for the target mechanism.

Prediction hypothesis: Items with unresolved `PROXY_DEPENDENCY` should fail more often when proxy-to-target mapping is not validated.

Required fields:
- proxy source
- target object
- proxy mapping rationale
- proxy validation status
- outcome status
- failure reason if failed

### WARNING_RECURRENCE

Definition: The same warning, concern, or adversary objection appears repeatedly across related artifacts or review cycles.

Prediction hypothesis: Recurring warnings predict future failure better than one-off warnings, especially when no resolution event exists.

Required fields:
- warning text or normalized warning ID
- recurrence count
- first seen timestamp
- latest seen timestamp
- resolution status
- outcome status

### CERTIFICATION_BLOCKER

Definition: The item encounters a certification blocker before paper-forward or candidate advancement.

Prediction hypothesis: Items with unresolved or repeatedly repaired `CERTIFICATION_BLOCKER` labels should have lower survival and higher downstream rework.

Required fields:
- certification gate
- blocker code
- blocker owner
- blocker resolution status
- repair count
- outcome status

### DATA_SOURCE_GAP

Definition: Required evidence, market data, macro data, or source lineage is missing, stale, indirect, or incomplete.

Prediction hypothesis: `DATA_SOURCE_GAP` should predict failure when the item advances before source readiness is resolved.

Required fields:
- missing source type
- freshness status
- source owner
- resolution status
- candidate or paper-forward state
- outcome status

### DUPLICATE_CLUSTER_RISK

Definition: The reviewed item overlaps materially with an existing mechanism, claim, hypothesis, or observation cluster.

Prediction hypothesis: Duplicate-cluster risk predicts weaker incremental usefulness and higher rejection or redesign rates unless the duplicate is explicitly framed as independent confirmation.

Required fields:
- duplicate score
- matched cluster IDs
- duplicate disposition
- independent-confirmation rationale if applicable
- outcome status

### ASSUMPTION_UNRESOLVED

Definition: A material assumption is identified but not tested, bounded, or accepted by human review.

Prediction hypothesis: Unresolved assumptions should predict later rejection, redesign, or failure-to-survive paper-forward review.

Required fields:
- assumption ID
- assumption text
- materiality score
- test coverage status
- resolution status
- outcome status

## Outcome Targets

### Paper-Forward Outcomes

Measured outcome fields:
- paper-forward item ID
- source hypothesis ID
- source candidate ID if present
- paper-forward readiness status
- paper observation created
- paper observation survived review window
- outcome classification
- failure reason
- redesign required
- human review decision

Primary paper-forward failure labels:
- rejected before observation
- observation not created
- observation created but failed
- insufficient evidence
- unresolved blocker
- duplicate or redundant
- retired or redesigned

### Candidate Outcomes

Measured outcome fields:
- candidate ID
- candidate lifecycle state
- candidate review status
- backtest support status
- paper-forward eligibility
- paper-forward survival
- final candidate disposition
- failure reason

Primary candidate failure labels:
- disqualified
- weak replay support
- weak backtest support
- unresolved governance blocker
- unresolved data blocker
- duplicate candidate
- insufficient lineage
- retired or redesigned

## Measurement Design

Each reviewed item should be represented as one row per taxonomy category assignment.

Minimum columns:

- `artifact_id`
- `artifact_type`
- `taxonomy_category`
- `taxonomy_confidence`
- `taxonomy_detected_at`
- `taxonomy_source`
- `resolution_status`
- `resolution_timestamp`
- `paper_forward_outcome`
- `candidate_outcome`
- `failure_reason`
- `reviewer_validated`

Recommended derived fields:

- `category_present`
- `category_unresolved_at_advancement`
- `category_recurred`
- `recurrence_count`
- `days_from_taxonomy_detection_to_outcome`
- `advanced_despite_blocker`

## Metrics

### Category Failure Rate

Formula:

`failed_items_with_category / total_items_with_category`

Interpretation: Higher values indicate categories associated with later failure.

### Unresolved Category Failure Rate

Formula:

`failed_items_with_unresolved_category_at_advancement / total_items_with_unresolved_category_at_advancement`

Interpretation: Measures whether unresolved taxonomy labels are predictive when ignored or left open.

### Lift Over Baseline

Formula:

`category_failure_rate / all_item_failure_rate`

Interpretation:
- `> 1.0`: category is associated with above-baseline failure.
- `~ 1.0`: category is not predictive.
- `< 1.0`: category may not be a failure predictor or may indicate useful caution.

### Precision

Formula:

`true_future_failures_flagged_by_category / all_items_flagged_by_category`

Interpretation: How often a category warning corresponds to future failure.

### Recall

Formula:

`future_failures_flagged_by_category / all_future_failures`

Interpretation: How many future failures were warned by the taxonomy before failure.

### Time Advantage

Formula:

`failure_timestamp - taxonomy_detected_at`

Interpretation: Measures whether the category appears early enough to be useful.

## Study Thresholds

Minimum useful signal:

- Category lift over baseline `>= 1.25`
- Precision `>= 0.50`
- Recall `>= 0.30`
- Median time advantage `>= 2 review steps` or `>= 1 calendar day`

Strong signal:

- Category lift over baseline `>= 1.75`
- Precision `>= 0.65`
- Recall `>= 0.45`
- Median time advantage `>= 3 review steps` or `>= 3 calendar days`

Retire as predictor:

- Category lift between `0.90` and `1.10` after sufficient sample size
- Precision `< 0.35`
- No meaningful time advantage
- High false-positive burden in reviewer notes

## Sample Size Requirements

Minimum exploratory threshold:

- 30 items with taxonomy labels
- 10 failed outcomes
- 5 examples per measured category

Minimum decision threshold:

- 100 items with taxonomy labels
- 30 failed outcomes
- 15 examples per measured category

Categories below sample threshold should be reported as `INSUFFICIENT_SAMPLE`, not predictive or non-predictive.

## Analysis Plan

1. Build a taxonomy-labeled dataset from Atlas review artifacts.
2. Join taxonomy labels to paper-forward and candidate outcomes.
3. Mark whether each taxonomy category was resolved before advancement.
4. Compute failure rate, unresolved failure rate, lift, precision, recall, and time advantage.
5. Separate paper-forward outcomes from candidate outcomes.
6. Rank categories by predictive lift and reviewer usefulness.
7. Identify categories that should become hard review prompts versus soft warnings.

## Expected Findings To Test

Hypotheses:

- `CERTIFICATION_BLOCKER` and `DATA_SOURCE_GAP` will likely have the strongest failure prediction.
- `WARNING_RECURRENCE` may predict failures better than one-off warnings.
- `REGIME_DEPENDENCY` may predict failure only when regime labels are low-confidence or stale.
- `PROXY_DEPENDENCY` may be predictive when proxy validation is absent.
- `DUPLICATE_CLUSTER_RISK` may predict redesign or rejection more than direct performance failure.
- `ASSUMPTION_UNRESOLVED` may predict slower review and higher redesign rates.

## Governance Boundary

This study can recommend measurement changes and review prompts only.

It cannot:

- approve or reject candidates automatically
- override replay
- override qualification
- promote paper-forward items
- change governance status
- allocate capital
- recommend trades
- place paper or live trades

All outputs remain research evidence and must be reviewed by a human before affecting workflow policy.

## Conclusion

Atlas should fund this adversary-related study because it tests whether failure taxonomy is predictive rather than merely descriptive. If taxonomy categories show measurable lift against paper-forward and candidate outcomes, they can justify stronger review prompts and earlier redesign. If they do not, the taxonomy should remain explanatory context and should not add review burden.
