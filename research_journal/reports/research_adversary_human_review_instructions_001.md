# Research Adversary Human Review Instructions 001

Date: 2026-06-05

Status: `HUMAN_REVIEW_REQUIRED`

Purpose: provide human scoring instructions for `research_journal/reports/research_adversary_human_review_batch_001.md` and `research_journal/reports/research_adversary_human_review_scores_001.csv`.

## Authority Boundary

This instruction document does not expand authority.

No candidate influence is authorized.
No replay influence is authorized.
No qualification influence is authorized.
No governance influence is authorized.
No production integration is authorized.
No automatic memory write is authorized.
No candidate promotion or rejection is authorized.
No paper placement is authorized.
No trade recommendation, broker execution, capital allocation, portfolio construction, or position sizing is authorized.

Human scores measure only the quality of Research Adversary review artifacts.

## Required Inputs

- Batch report: `research_journal/reports/research_adversary_human_review_batch_001.md`
- Score ledger: `research_journal/reports/research_adversary_human_review_scores_001.csv`
- Selected review artifacts: the 20 artifacts already listed in the batch report
- Source artifacts: the corresponding `source_path` values in the score ledger
- Review summaries: the corresponding `review_summary` values in the score ledger, when available

## Preservation Rule

Preserve the existing 20 selected review artifacts. Do not add, remove, reorder, replace, or resample artifacts for Batch 001 while scoring this batch.

Do not fabricate scores. Blank score fields are intentional and mean `PENDING_HUMAN_REVIEW`.

## HUMAN_REVIEW_REQUIRED

Each row requires a human reviewer to read the source artifact, read the generated adversary review, optionally read the review summary, and enter scores in the score ledger.

Do not infer scores from prior reports, model output, corpus position, taxonomy labels, authority precheck, or artifact metadata. A row is scored only when a human reviewer has inspected the source and review artifact.

## Review Workflow

1. Open the row in `research_adversary_human_review_scores_001.csv`.
2. Open `source_path` and read the source artifact first.
3. Open `artifact_id` and read the full Research Adversary review.
4. Open `review_summary` if present and use it only as a navigation aid.
5. Score the Research Adversary output only.
6. Fill `usefulness_score`, `correctness_score`, `novelty_score`, and `time_saved_score` with integers from 1 to 5.
7. Fill `reviewer_notes` with source-specific explanation.
8. Fill `final_classification` with `USEFUL`, `PARTIALLY_USEFUL`, or `NOT_USEFUL`.
9. Fill reviewer identity and reviewed timestamp if the local review protocol requires it.
10. Leave authority-boundary fields unchanged unless the reviewed artifact itself contains a real authority-boundary issue.

## Reviewer Checklist

| Field | Required Value | Checklist Question |
| --- | --- | --- |
| usefulness_score | 1-5 integer | Did the review help shape the next research question or review path? |
| correctness_score | 1-5 integer | Are the critique and risk statements grounded in the source artifact? |
| novelty_score | 1-5 integer | Did it surface non-obvious issues beyond generic boilerplate? |
| time_saved_score | 1-5 integer | Would it reduce net review effort after accounting for time spent reading it? |
| reviewer_notes | free text | What source-specific evidence explains the scores? |
| final_classification | enum | `USEFUL`, `PARTIALLY_USEFUL`, or `NOT_USEFUL`. |

## Score Rubric

| Score | Meaning |
| ---: | --- |
| 1 | not useful, incorrect, generic, or time-wasting |
| 2 | weak signal with limited practical value |
| 3 | partially useful; helped frame at least one review question |
| 4 | useful; surfaced a material issue or saved meaningful review effort |
| 5 | highly useful; likely prevented a materially wasteful or misleading research path |

## Final Classification Rubric

`USEFUL`: mostly correct, source-specific, and materially changes or narrows the review path.

`PARTIALLY_USEFUL`: contains useful critique, but also generic, incomplete, ambiguous, redundant, or weakly grounded material.

`NOT_USEFUL`: mostly incorrect, generic, misleading, redundant, or not worth the review overhead.

## Non-Scoring Rules

- Do not score the underlying hypothesis, claim, observation, candidate, mechanism, or strategy idea.
- Do not use a high score as candidate evidence.
- Do not use a low score as candidate rejection evidence.
- Do not use human review scores to alter replay, qualification, governance, paper-forward state, or research memory automatically.
- Do not compute batch averages until human scores exist.
- Do not treat blank fields as zero scores.

## Completion Criteria

Batch 001 is human-scored only when all 20 selected rows have:

- usefulness_score populated;
- correctness_score populated;
- novelty_score populated;
- time_saved_score populated;
- reviewer_notes populated;
- final_classification populated with an allowed value;
- human_review_status changed from `PENDING_HUMAN_REVIEW` only by a human reviewer or an explicitly authorized scoring ledger update process.
