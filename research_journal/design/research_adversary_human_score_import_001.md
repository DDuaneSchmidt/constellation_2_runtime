# Research Adversary Human Score Import 001

Date: 2026-06-05
Status: DESIGN_ONLY
Mode: IMPORT_WORKFLOW_SPEC_ONLY

## Scope

Prepare the Research Adversary human review score import workflow.

Inputs reviewed:

- `research_journal/reports/research_adversary_human_review_scores_001.csv`
- `research_journal/reports/research_adversary_human_review_instructions_001.md`
- `research_journal/reports/research_adversary_human_review_batch_001.md`
- `research_journal/design/research_adversary_human_review_protocol_001.md`

This document defines an import contract only. It does not import scores, fabricate scores, backfill blank fields, compute current batch quality, change candidate state, change replay, change qualification, change governance, write memory, create paper observations, or authorize production integration.

## Import Purpose

The import workflow should convert a human-filled score ledger into an evaluation-only summary of Research Adversary review quality.

The workflow must distinguish three states:

| state | meaning | import behavior |
| --- | --- | --- |
| `PENDING_HUMAN_REVIEW` | Score fields are intentionally blank. | Preserve row as unscored; do not average, impute, or treat blanks as zero. |
| `SCORED_HUMAN_REVIEW` | Human reviewer supplied all required score fields and classification. | Eligible for validation and summary metrics. |
| `INVALID_REVIEW_ROW` | Row has malformed, missing, inconsistent, or unauthorized values. | Exclude from summary metrics until corrected. |

## Required CSV Columns

The CSV must contain exactly these columns for Batch 001 import compatibility:

| column | required | type | import rule |
| --- | --- | --- | --- |
| `batch_id` | yes | string | Must identify the review batch. |
| `artifact_id` | yes | path/string | Must identify the Research Adversary review artifact. |
| `artifact_ordinal` | yes | integer | Must be a stable selected artifact ordinal. |
| `source_artifact_id` | yes | string | Must identify the source artifact or source object. |
| `source_type` | yes | enum/string | Must identify the source artifact type. |
| `source_path` | yes | path/string | Must point to the source artifact reviewed by the human. |
| `review_summary` | yes | path/string or blank | Navigation aid only; not a scoring substitute. |
| `reviewer` | required when scored | string | Must be populated for scored rows. |
| `reviewed_at` | required when scored | timestamp/string | Must be populated for scored rows. |
| `usefulness_score` | required when scored | integer | Valid range `1-5`. |
| `correctness_score` | required when scored | integer | Valid range `1-5`. |
| `novelty_score` | required when scored | integer | Valid range `1-5`. |
| `time_saved_score` | required when scored | integer | Valid range `1-5`. |
| `average_score` | required when scored | decimal or blank before import | Must equal the simple average of the four score columns if supplied. |
| `reviewer_notes` | required when scored | free text | Must contain artifact-specific explanation. |
| `final_classification` | required when scored | enum | Must be one of the allowed final classifications. |
| `authority_boundary_pass` | yes | boolean | Must be `true` or `false`; false triggers authority review metrics. |
| `material_issue_count` | required when scored | non-negative integer | Count of material review issues found by the human reviewer. |
| `recommended_follow_up` | required when scored | enum/string | Evaluation-only follow-up label. |
| `human_review_status` | yes | enum | Must reflect pending, scored, or invalid review state. |
| `score_source` | yes | enum/string | Must identify whether scores are human-supplied or still unscored. |

Columns may not be silently added, removed, renamed, or reordered for Batch 001 without a new import-contract version.

## Valid Score Ranges

The four score dimensions are:

- `usefulness_score`
- `correctness_score`
- `novelty_score`
- `time_saved_score`

Valid score values:

```text
1, 2, 3, 4, 5
```

Invalid score values:

- blank on a row marked scored
- `0`
- values above `5`
- negative values
- decimals
- non-numeric strings
- inferred scores
- generated scores
- values copied from model output without human review

Score meaning:

| score | import interpretation |
| ---: | --- |
| 1 | low quality, incorrect, generic, harmful, or time-wasting |
| 2 | weak signal with limited practical value |
| 3 | partially useful signal |
| 4 | useful and materially helpful |
| 5 | highly useful and likely to prevent wasted or misleading review work |

`average_score` must be calculated as:

```text
(usefulness_score + correctness_score + novelty_score + time_saved_score) / 4
```

If `average_score` is blank on an otherwise valid scored row, an import process may calculate it for evaluation reporting only. If it is present and does not match the four score columns within the chosen rounding policy, the row must be marked invalid until corrected.

## Valid Final Classifications

Allowed `final_classification` values:

| value | meaning |
| --- | --- |
| `USEFUL` | Mostly correct, artifact-specific, and materially changes or narrows the review path. |
| `PARTIALLY_USEFUL` | Contains useful critique but also generic, incomplete, ambiguous, redundant, or weakly grounded material. |
| `NOT_USEFUL` | Mostly incorrect, generic, misleading, redundant, or not worth the review overhead. |

Blank `final_classification` is valid only when `human_review_status=PENDING_HUMAN_REVIEW`.

Invalid final classifications include:

- `PASS`
- `FAIL`
- `APPROVED`
- `REJECTED`
- `VALIDATED`
- `QUALIFIED`
- `PROMOTED`
- `PAPER_READY`
- any value implying candidate, replay, qualification, governance, trading, or paper-forward authority

## Review Status And Score Source

Recommended `human_review_status` values:

| value | valid use |
| --- | --- |
| `PENDING_HUMAN_REVIEW` | Row has not been scored; score fields remain blank. |
| `SCORED_HUMAN_REVIEW` | Row has all required human-entered scores, notes, classification, reviewer, and timestamp. |
| `INVALID_REVIEW_ROW` | Row failed import validation and must be corrected before summary inclusion. |

Recommended `score_source` values:

| value | valid use |
| --- | --- |
| `UNSCORED_HUMAN_REQUIRED` | Current pending state; no human score exists. |
| `HUMAN_REVIEWER` | Human reviewer supplied the row scores. |
| `IMPORT_CALCULATED_AVERAGE_ONLY` | Import process calculated `average_score` from human-entered dimension scores. |

Forbidden `score_source` values:

- `MODEL_GENERATED`
- `INFERRED`
- `BACKFILLED`
- `FABRICATED`
- `SIMULATED`

## Validation Errors

The import workflow should fail the row, not the whole batch, when row-level errors occur. Batch-level summary metrics must exclude invalid rows and report their count.

Required validation errors:

| error code | trigger | handling |
| --- | --- | --- |
| `MISSING_REQUIRED_COLUMN` | Required CSV column absent. | Block import for the file. |
| `UNEXPECTED_COLUMN_SET` | Columns added, removed, renamed, or reordered without a new contract. | Block import for the file. |
| `DUPLICATE_ARTIFACT_ID` | Same `artifact_id` appears more than once in the same batch. | Mark duplicate rows invalid. |
| `MISSING_ARTIFACT_ID` | `artifact_id` blank. | Mark row invalid. |
| `MISSING_SOURCE_PATH` | `source_path` blank. | Mark row invalid. |
| `MISSING_REVIEWER_ON_SCORED_ROW` | Scores present but `reviewer` blank. | Mark row invalid. |
| `MISSING_REVIEWED_AT_ON_SCORED_ROW` | Scores present but `reviewed_at` blank. | Mark row invalid. |
| `PARTIAL_SCORE_SET` | Some but not all four score fields populated. | Mark row invalid. |
| `SCORE_OUT_OF_RANGE` | Score not an integer from `1` to `5`. | Mark row invalid. |
| `SCORE_ON_PENDING_ROW` | Scores present while `human_review_status=PENDING_HUMAN_REVIEW`. | Mark row invalid or require status correction. |
| `BLANK_SCORE_ON_SCORED_ROW` | Any score blank while row is marked scored. | Mark row invalid. |
| `AVERAGE_SCORE_MISMATCH` | Supplied average does not equal the four-score average under the reporting rounding policy. | Mark row invalid or recalculate only if explicitly allowed. |
| `MISSING_REVIEWER_NOTES` | Scored row lacks reviewer notes. | Mark row invalid. |
| `GENERIC_REVIEWER_NOTES` | Notes are only generic text such as `good`, `bad`, or `seems fine`. | Flag for review; optionally mark invalid under strict mode. |
| `INVALID_FINAL_CLASSIFICATION` | Classification not in allowed enum. | Mark row invalid. |
| `AUTHORITY_CLASSIFICATION_VALUE` | Classification implies approval, validation, candidate promotion, or trading authority. | Mark row invalid and flag authority review. |
| `INVALID_AUTHORITY_BOUNDARY_PASS` | Value is not `true` or `false`. | Mark row invalid. |
| `AUTHORITY_BOUNDARY_FAILURE` | `authority_boundary_pass=false`. | Keep score if otherwise valid, but count separately and require authority follow-up. |
| `INVALID_MATERIAL_ISSUE_COUNT` | Material issue count is negative, decimal, non-numeric, or blank on scored row. | Mark row invalid. |
| `INVALID_SCORE_SOURCE` | Score source implies generated, inferred, simulated, or fabricated score. | Mark row invalid and block summary use. |

## Summary Metrics

Summary metrics may be computed only from valid scored human-review rows.

Required batch-level metrics:

- total rows
- pending rows
- valid scored rows
- invalid rows
- completion rate
- average usefulness score
- average correctness score
- average novelty score
- average time-saved score
- average overall score
- median overall score
- score distribution by dimension
- final classification counts
- `USEFUL` rate
- `PARTIALLY_USEFUL` rate
- `NOT_USEFUL` rate
- authority boundary pass rate
- authority boundary failure count
- total material issue count
- average material issue count per scored row
- recommended follow-up counts
- rows needing second review
- rows requiring authority review

Optional metrics:

- score distribution by `source_type`
- score distribution by reviewer
- low-correctness count, defined as `correctness_score <= 2`
- low-usefulness count, defined as `usefulness_score <= 2`
- high-value count, defined as `usefulness_score >= 4` and `correctness_score >= 4`
- noisy-but-useful count, defined as `usefulness_score >= 4` and `correctness_score <= 3`

Metrics that must not be computed from pending rows:

- averages
- medians
- quality rates
- usefulness claims
- correctness claims
- Research Adversary acceptance decisions

Blank score fields are not zero. Pending rows are not negative evidence. Pending rows mean only that human review has not happened yet.

## Reviewer Notes Handling

Reviewer notes are required for every scored row.

Notes should be preserved as human-authored text and handled as qualitative evaluation evidence only.

Import handling rules:

- preserve reviewer notes verbatim except for CSV parsing normalization
- do not summarize notes into candidate evidence
- do not overwrite notes with generated text
- do not infer missing notes from scores
- do not treat notes as authority to change candidate or replay state
- flag notes that mention authority failures, hallucinated claims, unsupported critique, or material safety concerns
- allow notes to support Research Adversary prompt redesign, corpus cleanup, rubric refinement, or second-review routing

Good notes identify:

- source-specific issue
- unsupported adversary claim
- missed assumption
- useful falsification path
- duplicated boilerplate
- authority-risk wording
- net time saved or added

Weak notes that should be flagged:

- `good`
- `bad`
- `generic`
- `needs work`
- `seems fine`
- blank notes on a scored row

## No Score Fabrication

The import workflow must never create human scores.

Forbidden:

- filling blank score fields
- inferring scores from `final_classification`
- inferring scores from `reviewer_notes`
- inferring scores from source type, ordinal, authority precheck, artifact metadata, prior reports, model output, or batch position
- treating `PENDING_HUMAN_REVIEW` as a zero
- treating `UNSCORED_HUMAN_REQUIRED` as a score
- converting generated review quality claims into human scores

Allowed:

- validate human-entered scores
- calculate `average_score` from four existing human-entered dimension scores
- aggregate valid scored rows
- count pending and invalid rows
- report that no quality metrics are available when no valid human scores exist

## Import Readout

A future import readout should produce:

```text
batch_id
total_rows
valid_scored_rows
pending_rows
invalid_rows
completion_rate
average_usefulness_score
average_correctness_score
average_novelty_score
average_time_saved_score
average_overall_score
classification_counts
authority_boundary_pass_rate
material_issue_count
recommended_follow_up_counts
validation_error_counts
notes_flags
```

If `valid_scored_rows=0`, the readout must say:

```text
No human score metrics available. All score fields remain pending or invalid.
```

## No-Authority Boundaries

Human review score import is evaluation-only.

It may support:

- Research Adversary quality measurement
- prompt redesign decisions
- rubric refinement
- reviewer workflow improvement
- second-review routing
- future evaluation design

It must not:

- promote candidates
- reject candidates
- validate candidates
- qualify candidates
- change replay
- change validation
- change governance
- change runtime truth
- write memory automatically
- create paper-forward observations
- recommend trades
- authorize broker execution
- allocate capital
- construct portfolios
- size positions
- approve production integration

## Final Contract

The import workflow is valid only when:

```text
Scores are human-supplied.
Scores are integers from 1 to 5.
Final classification is USEFUL, PARTIALLY_USEFUL, or NOT_USEFUL.
Blank pending fields remain pending.
Invalid rows are excluded from summary metrics.
Reviewer notes are preserved and treated as qualitative evaluation evidence only.
No score changes candidate, replay, validation, qualification, governance, memory, paper-forward, trading, broker, capital, portfolio, or position-sizing authority.
```

## Authority Boundary

This document is design-only. It does not implement an importer, alter the score ledger, fabricate scores, compute current quality metrics, modify Research Adversary behavior, modify Atlas or Aegis behavior, change manifests, alter verified runtime truth, change candidate state, change replay, change qualification, change governance, write memory, create paper observations, recommend trades, allocate capital, authorize broker execution, size positions, or approve production integration.
