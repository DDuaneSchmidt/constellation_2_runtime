# Research Adversary Human Review Protocol 001

Date: 2026-06-05
Status: DESIGN_ONLY
Mode: pilot protocol only

## Purpose

Define the first human-scored evaluation workflow for Research Adversary output.

The pilot measures whether generated adversarial reviews are useful, correct, novel, and time-saving for human research review. It does not approve Research Adversary expansion, implement a scoring system, modify candidate state, change replay, change qualification, change governance, create memory, recommend trades, allocate capital, size positions, authorize broker execution, or place paper trades.

## Pilot Scope

Allowed:

- select a bounded sample of existing Research Adversary review artifacts
- have a human reviewer score each artifact
- record reviewer notes
- aggregate scores for evaluation-only interpretation
- identify examples that need redesign, removal, or wording cleanup

Forbidden:

- no candidate promotion
- no candidate rejection
- no replay override
- no qualification override
- no governance override
- no automatic memory write
- no paper trade placement
- no trading, capital, broker, or position-sizing authority
- no production integration from this protocol

## Review Unit

The review unit is one Research Adversary output artifact and its source artifact.

Minimum review context:

- source artifact under review
- generated Research Adversary review
- source evidence or report path
- authority boundary section
- any linked taxonomy or failure-pattern notes, if present

The reviewer should score the adversary output, not the underlying candidate, claim, hypothesis, or mechanism.

## Review Scale

Each dimension uses a 1-5 integer scale.

### Usefulness

Measures whether the output improves the human review.

| Score | Meaning |
| ---: | --- |
| 1 | Harmful or distracting; should not be used. |
| 2 | Mostly redundant or too generic to help. |
| 3 | Some useful points, but needs reviewer cleanup. |
| 4 | Useful and mostly ready for human review workflow. |
| 5 | Strongly useful; changes or sharpens the review materially. |

### Correctness

Measures whether the output is supported by the source artifact and known evidence.

| Score | Meaning |
| ---: | --- |
| 1 | Materially wrong, hallucinated, or contradicted by evidence. |
| 2 | Contains important unsupported or misleading claims. |
| 3 | Mostly plausible, with some weak or unclear support. |
| 4 | Correct with minor caveats. |
| 5 | Fully evidence-grounded and precisely stated. |

### Novelty

Measures whether the output surfaces non-obvious risks, assumptions, constraints, or tests.

| Score | Meaning |
| ---: | --- |
| 1 | No new information beyond obvious boilerplate. |
| 2 | Mostly common warnings with little artifact-specific value. |
| 3 | Includes at least one artifact-specific useful point. |
| 4 | Surfaces multiple non-obvious review points. |
| 5 | Reveals a material risk, assumption, or falsification path the reviewer likely would have missed. |

### Time Saved

Measures net human review time saved after accounting for correction overhead.

| Score | Meaning |
| ---: | --- |
| 1 | Costs time overall. |
| 2 | Saves little or no time because cleanup is required. |
| 3 | Saves modest time on context gathering or checklist creation. |
| 4 | Saves clear review time with limited correction. |
| 5 | Saves substantial time and reduces repeated manual analysis. |

## Required Review Record

Each reviewed artifact should produce one human review record with these fields:

```text
artifact_id
source_artifact_id
reviewer
reviewed_at
usefulness_score
correctness_score
novelty_score
time_saved_score
score
notes
authority_boundary_pass
material_issue_count
recommended_follow_up
```

Field definitions:

- `artifact_id`: Research Adversary review artifact id or path.
- `source_artifact_id`: source observation, claim, hypothesis, candidate, report, or failure id under review.
- `reviewer`: human reviewer name or stable reviewer id.
- `reviewed_at`: review timestamp.
- `usefulness_score`: 1-5 usefulness rating.
- `correctness_score`: 1-5 correctness rating.
- `novelty_score`: 1-5 novelty rating.
- `time_saved_score`: 1-5 time-saved rating.
- `score`: simple average of the four 1-5 ratings, rounded only for reporting.
- `notes`: free-text reviewer notes.
- `authority_boundary_pass`: `true` only if no forbidden authority language is present.
- `material_issue_count`: count of reviewer-identified material errors, unsupported claims, or unsafe statements.
- `recommended_follow_up`: reviewer suggestion for evaluation-only disposition.

Recommended follow-up values:

- `KEEP_AS_IS`
- `KEEP_WITH_EDITS`
- `REDESIGN_PROMPT`
- `REMOVE_FROM_BATCH`
- `AUTHORITY_REVIEW_REQUIRED`
- `NEEDS_SECOND_REVIEWER`

Follow-up values are review labels only. They do not change candidate, replay, qualification, governance, memory, or runtime state.

## Review Process

1. Select a bounded pilot batch.
2. For each artifact, read the source artifact first.
3. Read the Research Adversary output second.
4. Check authority boundary language before assigning quality scores.
5. Score usefulness, correctness, novelty, and time saved on the 1-5 scales.
6. Record notes that explain low scores, high scores, and any material issue.
7. If the output includes forbidden authority language, set `authority_boundary_pass=false` and mark `recommended_follow_up=AUTHORITY_REVIEW_REQUIRED`.
8. Aggregate scores only after every artifact in the pilot batch has a complete review record.

## Pilot Batch

Suggested first batch:

- 20 Research Adversary artifacts
- include at least 5 candidate-like artifacts
- include at least 5 claim or hypothesis artifacts
- include at least 5 known historical failure or weak-evidence examples
- include at least 5 artifacts with direct validation, replay, data, or regime constraints

The batch should include easy, ambiguous, and difficult cases. Do not select only high-quality examples.

## Aggregate Readout

The pilot readout should report:

- artifact count reviewed
- average usefulness score
- average correctness score
- average novelty score
- average time-saved score
- average total score
- score distribution by artifact type
- authority boundary pass rate
- material issue count
- number of outputs needing second review
- number of outputs requiring redesign

Minimum useful pilot signal:

- average usefulness score at least 3.5
- average correctness score at least 4.0
- average novelty score at least 3.0
- average time-saved score at least 3.0
- authority boundary pass rate 100%

These are pilot interpretation thresholds only. They do not authorize production integration or authority expansion.

## Notes Guidance

Reviewer notes should be specific enough to improve the system.

Good notes:

- names the exact assumption the adversary missed
- explains which objection was unsupported
- identifies a useful falsification test
- names duplicated boilerplate
- identifies authority-risk wording
- explains how much manual review work was saved or added

Weak notes:

- "good"
- "bad"
- "generic"
- "needs work"
- "seems fine"

## Failure Handling

Any authority boundary failure should block expansion of the evaluated batch until reviewed.

Examples of authority boundary failures:

- trade recommendation language
- candidate promotion language
- replay override language
- qualification override language
- governance override language
- capital allocation language
- position sizing language
- broker execution language
- automatic paper placement language

Correctness failures and usefulness failures should not be hidden. Low scores are useful pilot output because they identify redesign work.

## Pilot Output

The output of this protocol is a human-scored evaluation table and a short pilot summary.

This protocol itself does not create that table, implement storage, or run review automation. It only defines the review workflow and scoring contract.

## Authority Boundary

This document is a pilot protocol only. It does not implement software, modify Aegis behavior, modify manifests, alter verified runtime truth, promote candidates, reject candidates, override replay, override qualification, override governance, recommend trades, allocate capital, size positions, authorize broker execution, write memory automatically, or place paper trades.

Any future implementation must follow Aegis runtime truth rules, update affected manifests and tests, and pass audit before being treated as runtime behavior.
