# Research Adversary Human Review Batch 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Purpose: start the missing human-scored evidence loop for Research Adversary by selecting a bounded review batch and creating the score ledger.

Authority boundary: no authority expansion. This batch does not modify governance, candidate state, replay state, qualification state, paper-forward state, research memory, trading state, broker execution, capital allocation, or position sizing.

## Inputs

- `reports/atlas_v2_research_os/research_adversary_corpus/corpus_index.json`
- `reports/atlas_v2_research_os/research_adversary_corpus/reviews/*/research_adversary_review.json`
- `research_journal/design/research_adversary_human_review_protocol_001.md`

## Batch Selection

Selection rule: sample 20 recent generated adversary reviews from the corpus index by descending ordinal, preserving corpus order in this report.

| Measure | Value |
| --- | ---: |
| Reviews selected | 20 |
| Lowest selected ordinal | 131 |
| Highest selected ordinal | 150 |
| Authority boundary precheck pass rows | 20 |
| Human-scored rows completed | 0 |

## Scoring Contract

Each reviewed artifact requires integer scores from 1 to 5:

- usefulness
- correctness
- novelty
- time saved

Allowed final classifications:

- `USEFUL`
- `PARTIALLY_USEFUL`
- `NOT_USEFUL`

The score ledger is `research_journal/reports/research_adversary_human_review_scores_001.csv`. Score fields are intentionally blank until a human reviewer fills them. Blank score fields are not evidence and must not be interpreted as Research Adversary quality measurements.


## HUMAN_REVIEW_REQUIRED

This batch is not complete until a human reviewer scores all 20 selected artifacts in `research_journal/reports/research_adversary_human_review_scores_001.csv`.

Do not infer, generate, backfill, average, or fabricate human scores. Empty score fields must remain empty until a human reviewer supplies them. `PENDING_HUMAN_REVIEW` and `UNSCORED_HUMAN_REQUIRED` are the only valid current score states for unreviewed rows.

Human review workflow:

1. Open the source artifact from `source_path`.
2. Open the generated Research Adversary review from `artifact_id`.
3. Read the review summary from `review_summary` when available.
4. Score only the Research Adversary output, not the underlying hypothesis, claim, observation, candidate, mechanism, or strategy idea.
5. Enter integer scores from 1 to 5 for usefulness, correctness, novelty, and time saved.
6. Add reviewer notes that explain the score using artifact-specific evidence.
7. Choose one final classification: `USEFUL`, `PARTIALLY_USEFUL`, or `NOT_USEFUL`.
8. Leave authority fields unchanged unless a reviewer finds an actual authority-boundary issue in the review artifact.

Reviewer checklist:

| Field | Required Human Input | Scoring Guidance |
| --- | --- | --- |
| usefulness_score | integer 1-5 | Did the adversary critique help shape the next research question or review path? |
| correctness_score | integer 1-5 | Were the critique, assumptions, constraints, and risks grounded in the source artifact? |
| novelty_score | integer 1-5 | Did the review surface non-obvious issues beyond generic boilerplate? |
| time_saved_score | integer 1-5 | Would the review reduce human research effort after accounting for review overhead? |
| reviewer_notes | free text | Explain the material reasons for the scores; cite source-specific issues where possible. |
| final_classification | enum | `USEFUL`, `PARTIALLY_USEFUL`, or `NOT_USEFUL`. |

Score meaning:

| Score | Meaning |
| ---: | --- |
| 1 | not useful, incorrect, generic, or time-wasting |
| 2 | weak signal with limited practical value |
| 3 | partially useful; helped frame at least one review question |
| 4 | useful; surfaced a material issue or saved meaningful review effort |
| 5 | highly useful; likely prevented a materially wasteful or misleading research path |

Final classification guidance:

- `USEFUL`: mostly correct, artifact-specific, and materially changes or narrows the review path.
- `PARTIALLY_USEFUL`: contains useful critique but also generic, incomplete, ambiguous, or weakly grounded material.
- `NOT_USEFUL`: mostly incorrect, generic, misleading, redundant, or not worth the review overhead.

Authority reminder: these human scores measure Research Adversary review quality only. They do not promote candidates, reject candidates, change replay, change qualification, change governance, authorize paper placement, or create trading, broker, capital, portfolio, or position-sizing authority.

## Selected Reviews

| # | Ordinal | Source Type | Source Artifact | Review Artifact | Authority Precheck |
| ---: | ---: | --- | --- | --- | --- |
| 1 | 131 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_031.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/131_hypothesis_hypothesis_031/research_adversary_review.json` | `true` |
| 2 | 132 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_032.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/132_hypothesis_hypothesis_032/research_adversary_review.json` | `true` |
| 3 | 133 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_033.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/133_hypothesis_hypothesis_033/research_adversary_review.json` | `true` |
| 4 | 134 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_034.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/134_hypothesis_hypothesis_034/research_adversary_review.json` | `true` |
| 5 | 135 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_035.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/135_hypothesis_hypothesis_035/research_adversary_review.json` | `true` |
| 6 | 136 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_036.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/136_hypothesis_hypothesis_036/research_adversary_review.json` | `true` |
| 7 | 137 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_037.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/137_hypothesis_hypothesis_037/research_adversary_review.json` | `true` |
| 8 | 138 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_038.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/138_hypothesis_hypothesis_038/research_adversary_review.json` | `true` |
| 9 | 139 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_039.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/139_hypothesis_hypothesis_039/research_adversary_review.json` | `true` |
| 10 | 140 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_040.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/140_hypothesis_hypothesis_040/research_adversary_review.json` | `true` |
| 11 | 141 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_041.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/141_hypothesis_hypothesis_041/research_adversary_review.json` | `true` |
| 12 | 142 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_042.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/142_hypothesis_hypothesis_042/research_adversary_review.json` | `true` |
| 13 | 143 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_043.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/143_hypothesis_hypothesis_043/research_adversary_review.json` | `true` |
| 14 | 144 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_044.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/144_hypothesis_hypothesis_044/research_adversary_review.json` | `true` |
| 15 | 145 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_045.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/145_hypothesis_hypothesis_045/research_adversary_review.json` | `true` |
| 16 | 146 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_046.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/146_hypothesis_hypothesis_046/research_adversary_review.json` | `true` |
| 17 | 147 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_047.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/147_hypothesis_hypothesis_047/research_adversary_review.json` | `true` |
| 18 | 148 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_048.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/148_hypothesis_hypothesis_048/research_adversary_review.json` | `true` |
| 19 | 149 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_049.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/149_hypothesis_hypothesis_049/research_adversary_review.json` | `true` |
| 20 | 150 | `hypothesis` | `reports/atlas_v2_research_os/research_adversary_corpus/source_inputs/hypotheses/hypothesis_050.json` | `reports/atlas_v2_research_os/research_adversary_corpus/reviews/150_hypothesis_hypothesis_050/research_adversary_review.json` | `true` |

## Review Instructions

For each row, the reviewer reads the source artifact first, then the generated adversary review, then records the four 1-5 scores, reviewer notes, material issue count, and one final classification.

Reviewer notes should identify the artifact-specific reason for the scores. The reviewer scores the Research Adversary output only, not the underlying candidate, claim, hypothesis, observation, or mechanism.

## Current Score State

| Field | State |
| --- | --- |
| usefulness_score | pending human review |
| correctness_score | pending human review |
| novelty_score | pending human review |
| time_saved_score | pending human review |
| reviewer_notes | pending human review |
| final_classification | pending human review |

## No Authority Expansion

This batch is evaluation-only. It does not authorize production integration, memory writes, candidate promotion, candidate rejection, replay override, qualification override, governance override, paper trade placement, trade recommendation, broker execution, capital allocation, or position sizing.

