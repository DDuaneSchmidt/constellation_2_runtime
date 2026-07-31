# Discovery Candidate Registry Standard

## 1. Purpose

This document defines the standard for maintaining a durable, auditable registry of Discovery Candidates produced by the AEGIS Investment Thesis Factory.

The registry exists to preserve candidate lineage, scoring history, status changes, contradiction history, promotion decisions, rejection decisions, and research accountability.

## 2. Registry Role

The registry is the system of record for Discovery Candidates.

It must track research state, not investment decisions. Its purpose is to preserve what the factory believed, why it believed it, how confidence changed, and why candidates were promoted, rejected, retired, or reopened.

## 3. Registry Scope

The registry must include:

- Discovery Candidates.
- Status history.
- Scores.
- Evidence references.
- Contradictions.
- Alternative explanations.
- Reviewer notes.
- Promotion decisions.
- Rejection decisions.
- Retirement decisions.

Registry scope is limited to discovery research control. It must not become a substitute for investment evaluation.

## 4. Registry Non-Goals

The registry is not:

- A stock recommendation list.
- A portfolio list.
- A trade list.
- A watchlist for purchase.
- A valuation tracker.
- A performance ledger.

Registry entries must avoid language that implies investment action.

## 5. Required Registry Fields

Every registry entry must include:

- Candidate ID.
- Candidate Title.
- Discovery Date.
- Discovery Source.
- Candidate Description.
- Reality Claim.
- Importance Claim.
- Underappreciation Claim.
- Investability Claim.
- Current Status.
- Current DQI.
- Truth Score.
- Importance Score.
- Underappreciation Score.
- Investability Score.
- Evidence Quality Score.
- Consequence Depth Score.
- Contradiction Strength Score.
- Overall Confidence Score.
- Evidence References.
- Contradiction References.
- Alternative Explanations.
- Reviewer Notes.
- Last Review Date.
- Next Review Trigger.
- Status History.
- Decision History.

Fields must be maintained in a form that supports later review and independent challenge.

## 6. Candidate Identity Rules

Candidate identity must follow these rules:

- Stable Candidate ID.
- No ID reuse.
- No overwriting rejected candidates.
- No silent mutation of candidate meaning.
- Materially changed candidates require new IDs.

Candidate IDs must preserve lineage. If a candidate changes meaning, the registry must show whether the new candidate is a revision, replacement, reopening, or separate claim.

## 7. Candidate Status Model

The registry must use the following statuses:

- CANDIDATE.
- UNDER_REVIEW.
- SUPPORTED_CANDIDATE.
- PROMOTED_CANDIDATE.
- REJECTED_CANDIDATE.
- RETIRED_CANDIDATE.

Status labels describe research state only. They do not describe security attractiveness, portfolio eligibility, or trade readiness.

## 8. Status Transition Rules

Legal transitions are:

- CANDIDATE to UNDER_REVIEW.
- CANDIDATE to REJECTED_CANDIDATE.
- UNDER_REVIEW to SUPPORTED_CANDIDATE.
- UNDER_REVIEW to REJECTED_CANDIDATE.
- SUPPORTED_CANDIDATE to PROMOTED_CANDIDATE.
- SUPPORTED_CANDIDATE to REJECTED_CANDIDATE.
- SUPPORTED_CANDIDATE to RETIRED_CANDIDATE.
- PROMOTED_CANDIDATE to RETIRED_CANDIDATE.
- PROMOTED_CANDIDATE to REJECTED_CANDIDATE.
- RETIRED_CANDIDATE to UNDER_REVIEW, only under reopening rules.
- REJECTED_CANDIDATE to UNDER_REVIEW, only under reopening rules.

Every status transition requires documented justification, reviewer identity or role, review date, and evidence basis.

## 9. Scoring History Requirements

The registry must preserve:

- Initial scores.
- Updated scores.
- Score rationale.
- Score changes.
- Reviewer identity or role.
- Review date.

Scores must not be overwritten without retaining prior values. Score history must make confidence inflation, degradation, and reversal visible.

## 10. Evidence Lineage Requirements

Every material claim must reference evidence.

Evidence must be distinguishable from interpretation. The registry must allow reviewers to identify which references support the Reality Claim, Importance Claim, Underappreciation Claim, and Investability Claim.

Evidence references must be durable enough for later audit. Unsupported claims must be marked as unsupported or removed from active consideration.

## 11. Contradiction Tracking Requirements

Contradictory evidence must remain visible even after promotion.

Contradictions must not be deleted when resolved. The registry may record resolution status, reviewer rationale, and subsequent evidence, but the original contradiction must remain part of the candidate record.

Contradiction tracking must include:

- Contradictory evidence.
- Competing explanations.
- Severity assessment.
- Resolution status, if any.
- Impact on scores.
- Impact on status.

## 12. Promotion Decision Requirements

Promotion must require:

- Four-filter pass.
- Sufficient evidence quality.
- Contradiction review.
- Value capture plausibility.
- Reviewer rationale.

Promotion decisions must preserve the evidence and reasoning available at the time of promotion. Promotion does not imply investment action.

## 13. Rejection Decision Requirements

Rejection decisions must preserve:

- Reason for rejection.
- Failed filters.
- Contradictions.
- Lessons learned.
- Potential future revisit conditions, if any.

Rejected candidates must remain in the registry. Rejection records are part of the factory's learning system and must not be deleted to improve apparent discovery quality.

## 14. Retirement Rules

Retirement applies when a candidate is no longer active but not necessarily invalidated.

Retirement may occur when:

- The candidate is superseded by a new candidate.
- The research question is no longer active.
- The candidate remains plausible but no longer requires review.
- The evidence base becomes stale without clear rejection.
- The candidate has completed its intended research role.

Retirement requires documented rationale and lineage to any successor candidate.

## 15. Reopening Rules

Rejected candidates may not be silently reopened.

Material reopening requires:

- New evidence.
- New candidate ID if meaning changes.
- Explicit lineage link to prior candidate.

Reopening must preserve the original rejection rationale and show why the new evidence justifies renewed review.

## 16. Auditability Requirements

The registry must allow later review of:

- What was believed.
- When it was believed.
- Why it was believed.
- What evidence supported it.
- What contradicted it.
- Why the status changed.

Auditability requires durable records, explicit rationale, preserved history, and clear separation between evidence and interpretation.

## 17. Registry Quality Metrics

Registry quality must be scored from 1 to 10 across:

- Completeness.
- Evidence Coverage.
- Contradiction Coverage.
- Status Integrity.
- Scoring Consistency.
- Lineage Integrity.
- Auditability.

Low registry quality must reduce confidence in factory outputs even when individual candidates appear compelling.

## 18. Governance Rules

- No undocumented status changes.
- No deletion of rejected candidates.
- No promotion without contradiction review.
- No stock recommendation language.
- No investment action language.
- Discovery and investment evaluation remain separate.

Governance must treat the registry as a research control system, not an investment tool.

## 19. Outputs

This standard supports production of:

- Discovery Candidate Registry.
- Registry Quality Report.
- Status Transition Report.
- Candidate Lineage Report.
- Rejected Candidate Archive.

Outputs must be auditable and must preserve rejected, retired, and contradicted candidates.

## 20. Next Required Artifact

The next required artifact is:

```text
09_discovery_candidate_review_protocol.md
```
