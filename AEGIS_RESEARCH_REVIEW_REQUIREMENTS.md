# Aegis Research Review Requirements

## Purpose

Define the operator-facing review experience for autonomous Aegis research.

This document explains what the operator should see and decide when a hypothesis reaches Recommendation Ready.

This is a product/UX requirements document, not a pipeline implementation document.

It must answer:

* What did the research find?
* Why does it matter?
* How confident is Aegis?
* What evidence supports it?
* What risks or caveats exist?
* What decision is required from the operator?
* What actions are allowed?

## Core Principle

Research Lab must separate:

1. Research Queue / Execution Monitoring
2. Research Review / Operator Decision Support

The queue explains what the autonomous research engine is doing.

The review experience explains what the research found and what the operator can do with it.

## Product Purpose

Research Review exists to help the operator consume autonomous research findings.

It should translate research artifacts into an operator-readable brief.

It must not expose pipeline jargon as the primary experience.

## Required Canonical Artifact

Create/document:

```text
truth/reports/aegis_research_review_brief_v1/<day>/research_review_brief.v1.json
```

Every Recommendation Ready hypothesis must have a review brief.

## Required Review Brief Fields

Each brief must include:

```json
{
  "hypothesis_id": "...",
  "research_run_id": "...",
  "title": "...",
  "status": "RECOMMENDATION_READY|STALE|BLOCKED|DISMISSED|MONITORING|PROMOTED|ARCHIVED",
  "conclusion": "...",
  "confidence": "LOW|MEDIUM|HIGH",
  "confidence_reason": "...",
  "why_it_matters": "...",
  "affected_symbols": [],
  "evidence_summary": "...",
  "key_evidence": [],
  "counter_evidence": [],
  "risks": [],
  "decision_needed": "...",
  "allowed_actions": [],
  "staleness": {},
  "source_artifacts": [],
  "generated_at": "...",
  "as_of": "..."
}
```

## Required Operator-Facing Fields

The primary Research Review card must show:

* Title
* Conclusion
* Confidence
* Why it matters
* Affected symbols
* Key evidence
* Risks / caveats
* Decision needed
* Allowed actions

The operator should not need to open diagnostics to understand the recommendation.

## Forbidden Primary UI Language

Do not show these as primary operator-facing labels:

* `TIER_1_ACTIVE`
* `RESULT_REVIEW`
* `MANUAL_REVIEW`
* raw pipeline stage names
* raw run IDs
* raw artifact paths
* opaque recommendation labels without explanation

These may appear only in diagnostics/details.

## Allowed Operator Actions

Define these actions:

* `REVIEW_BRIEF`
* `MONITOR`
* `DISMISS`
* `ARCHIVE`
* `PROMOTE_TO_WATCHLIST`
* `REQUEST_MORE_RESEARCH`
* `OPEN_DIAGNOSTICS`

Do not enable:

* trade execution
* broker order submission
* autonomous trading
* trade advice

## Review States

### RECOMMENDATION_READY

Research findings are ready for operator review.

### MONITORING

Operator chose to monitor, but no action is required now.

### DISMISSED

Operator dismissed the recommendation.

### PROMOTED_TO_WATCHLIST

Operator promoted the research finding into a watchlist or monitoring workflow.

### REQUEST_MORE_RESEARCH

Operator asked the research engine to collect more evidence.

### STALE

The research finding is too old or source data has expired.

### BLOCKED

The review brief could not be produced or required evidence is missing.

## Required UI Structure

Create or evolve Research Review UI around:

1. Recommendation Ready
2. Monitoring
3. Dismissed / Archived
4. Blocked / Stale
5. Diagnostics

## Recommendation Ready Card

Example desired card:

```text
NVIDIA Earnings Event Dislocation

Conclusion:
NVDA earnings may create short-term volatility spillover into SMH, SOXX, QQQ, and XLK.

Confidence:
Medium

Why it matters:
The event may affect several open or candidate positions tied to semiconductor and large-cap technology exposure.

Key evidence:
- Upcoming earnings event
- Elevated volatility
- Historical post-event dislocation pattern

Risks:
- Event already priced in
- Broad market trend may dominate single-name reaction

Decision needed:
Monitor, dismiss, or request more research.

Actions:
[Review Brief] [Monitor] [Dismiss] [Request More Research]
```

## Research Queue Separation

The Hypotheses page may still show execution status:

* queued
* researching
* waiting
* blocked
* recommendations ready

But recommendation-ready cards must link to a proper Research Review brief.

Queue state must not substitute for review content.

## Auditability Requirements

Every review brief must include:

* hypothesis_id
* research_run_id
* evidence source IDs
* evidence timestamps
* generated_at
* confidence rationale
* allowed actions
* operator decision, when made
* operator decision timestamp
* stale/evidence freshness status

## Determinism Requirements

A review brief must be generated from structured fields.

Do not rely only on freeform recommendation text.

Required structured fields:

* conclusion
* confidence
* evidence_summary
* key_evidence
* risks
* decision_needed
* allowed_actions

If any required field is missing, the brief status must be:

```text
BLOCKED
```

with explicit missing fields.

## UI Requirements

The Research UI must show:

* Recommendations Ready count
* Review Brief button
* concise conclusion preview
* confidence
* decision needed
* operator action required/not required
* stale status if stale

The UI must not show "Recommendation Ready" without the recommendation content.

## Diagnostics

Diagnostics may include:

* raw pipeline state
* source artifact paths
* run ledger IDs
* scheduler state
* command IDs
* full evidence trace

Diagnostics should be collapsed by default.

## Required Commands

Add/document:

```bash
npm run aegis:research-review-brief
```

Builds the canonical review brief artifact.

Add/document:

```bash
npm run aegis:research-review-self-check
```

Self-check must fail if:

* a Recommendation Ready hypothesis has no review brief
* a review brief lacks conclusion
* a review brief lacks confidence
* a review brief lacks evidence
* a review brief lacks decision_needed
* UI exposes Recommendation Ready without Review Brief content
* stale findings are not marked stale

## Acceptance Tests

Add tests proving:

* Recommendation Ready hypothesis requires a review brief.
* Review brief contains conclusion, confidence, evidence, risks, and decision_needed.
* UI does not display raw pipeline jargon as the primary experience.
* UI shows Review Brief action for recommendation-ready hypotheses.
* Stale review brief is marked STALE.
* Missing required brief field produces BLOCKED status with explicit missing fields.
* Operator decisions are persisted with timestamp.
* Diagnostics remain available but collapsed.

## Safety Rules

Research Review may produce research findings only.

It must not:

* place trades
* recommend immediate trade execution
* submit broker orders
* enable autonomous trading
* override paper/live safety gates

If language could be interpreted as trade advice, label it as research finding or monitoring evidence, not execution recommendation.

## Relationship to Existing Documents

This document is subordinate to:

* `AEGIS_OPERATOR_WORKFLOW.md`
* `AEGIS_OPERATOR_STATUS_CLASSIFICATION_REQUIREMENTS.md`

It complements, but does not replace, Research Lab execution/doctor behavior.

## Governance Rule

No hypothesis may enter Recommendation Ready in the operator UI unless it has an operator-readable review brief or an explicit BLOCKED reason explaining why the brief could not be produced.

`aegis/modules/operator_portal/aegis.module.yaml` must reference this document as the product authority for Research Review.

## Validation

Run:

```bash
npm run aegis:research-doctor
npm run aegis:research-review-brief
npm run aegis:research-review-self-check
TARGET_DAY=2026-05-29 npm run aegis:portal-smoke
TARGET_DAY=2026-05-29 npm run aegis:audit
pytest relevant research/UI tests
```

