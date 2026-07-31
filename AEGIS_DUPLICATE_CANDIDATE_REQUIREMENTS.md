# Aegis Duplicate Candidate Requirements

## Purpose

Define all duplicate-candidate handling across Aegis so the operator is never repeatedly asked to review, capture, or trade the same setup without a clear explanation of why it reappeared.

This document replaces any future need for separate:

- same-session duplicate rules
- same-day duplicate rules
- cross-day duplicate rules
- open-position duplicate rules

All duplicate behavior must be governed by this document.

## Core Principle

The operator should never ask:

```text
Why am I seeing this symbol again?
```

Every repeated symbol must be classified before it becomes operator-reviewable.

Aegis must explicitly determine whether the candidate is:

- a duplicate
- an improved duplicate
- a distinct setup
- a continuation
- an add-on opportunity
- a suppressed repeat signal

## Canonical Duplicate Scopes

Every candidate must be evaluated against prior candidates and positions.

Supported scopes:

### SAME_SESSION

Example:

```text
PAPER-2026-05-29-0950

QQQ appears twice
```

Default outcome:

```text
DUPLICATE_SAME_SESSION
```

Action:

```text
Suppress
```

### SAME_DAY

Example:

```text
09:50 QQQ
14:50 QQQ
```

Possible outcomes:

- `DUPLICATE_SAME_DAY`
- `IMPROVED_SIGNAL`
- `DISTINCT_SETUP`
- `SIGNAL_REFRESH_NO_ACTION`

### OPEN_POSITION

Example:

```text
QQQ open position exists
QQQ candidate appears
```

Possible outcomes:

- `DUPLICATE_OPEN_POSITION`
- `IMPROVED_SIGNAL_ADD_ON`

### RECENT_CAPTURE

Example:

```text
Candidate captured recently
Same symbol appears again
```

Possible outcomes:

- `DUPLICATE_RECENT_CAPTURE`
- `IMPROVED_SIGNAL`
- `COOLDOWN_EXPIRED_REVIEW_ALLOWED`

### RECENT_REJECTION

Example:

```text
Candidate rejected recently
Same symbol appears again
```

Possible outcomes:

- `DUPLICATE_RECENT_REJECTION`
- `IMPROVED_SIGNAL`
- `COOLDOWN_EXPIRED_REVIEW_ALLOWED`

### RECENT_DEFERRED

Example:

```text
Candidate deferred recently
Same symbol appears again
```

Possible outcomes:

- `DUPLICATE_RECENT_DEFERRED`
- `REVIEW_DEFERRED_CANDIDATE`

### RECENTLY_CLOSED

Example:

```text
Position closed yesterday
Same symbol appears today
```

Possible outcomes:

- `DUPLICATE_RECENTLY_CLOSED`
- `COOLDOWN_EXPIRED_REVIEW_ALLOWED`
- `IMPROVED_SIGNAL`

## Duplicate Key

Minimum duplicate key:

```text
symbol + direction
```

Preferred duplicate key:

```text
symbol + direction + sleeve + setup_type
```

Duplicate comparison hierarchy:

1. Candidate ID exact match.
2. Preferred duplicate key exact match.
3. Minimum duplicate key exact match.
4. Symbol-only match, classified as ambiguous until direction/setup can be resolved.

Tie-breaking behavior:

1. Active open positions take precedence over prior candidates.
2. Same-session duplicates take precedence over same-day duplicates.
3. More recent decisions take precedence over older decisions.
4. Terminal closed/rejected/deferred states are evaluated after open-position and same-day checks.
5. If two prior records have equal precedence, the newest event timestamp wins.
6. If classification still cannot be determined, the candidate must be blocked from operator review and surfaced in diagnostics as an unclassified duplicate-boundary violation.

## Material Improvement Rules

A repeated candidate becomes reviewable only if it materially improves.

Examples:

- score improvement
- reward/risk improvement
- entry improvement
- stop improvement
- reduced risk
- additional sleeve confirmation
- resolved blocker
- improved market regime evidence

All thresholds must be configurable.

Example defaults:

```text
score_improvement_threshold_percent = 10
reward_risk_improvement_threshold_percent = 10
entry_improvement_threshold_percent = 2
cooldown_days_default = 5
```

Do not hardcode thresholds.

## Add-On Candidate Rules

If a position is already open:

Aegis must not present the same symbol as a normal new candidate.

Instead classify:

```text
IMPROVED_SIGNAL_ADD_ON
```

Requirements:

- risk budget available
- add-ons enabled
- improvement criteria met

Allowed actions:

- Review Add-On
- View Existing Position
- Reject Add-On
- Defer Add-On

Forbidden:

- Confirm Captured as a new trade

## Required Duplicate Outcomes

Every repeated symbol must receive one of:

- `NEW_DISTINCT_SETUP`
- `DUPLICATE_SAME_SESSION`
- `DUPLICATE_SAME_DAY`
- `DUPLICATE_OPEN_POSITION`
- `DUPLICATE_RECENT_CAPTURE`
- `DUPLICATE_RECENT_REJECTION`
- `DUPLICATE_RECENT_DEFERRED`
- `DUPLICATE_RECENTLY_CLOSED`
- `IMPROVED_SIGNAL`
- `IMPROVED_SIGNAL_ADD_ON`
- `SIGNAL_REFRESH_NO_ACTION`
- `COOLDOWN_EXPIRED_REVIEW_ALLOWED`

Silent duplicate handling is forbidden.

## Required Artifact

Create or document:

```text
truth/reports/aegis_duplicate_candidate_v1/<day>/duplicate_candidate.v1.json
```

Include:

- `paper_session_id`
- `duplicate_scope`
- `duplicate_classification`
- `prior_candidate_id`
- `prior_position_id`
- `prior_state`
- `improvement_score`
- `improvement_reasons`
- `cooldown_days_remaining`
- `allowed_actions`
- `blocked_actions`
- `operator_message`

## Required Command

Create or document:

```bash
npm run aegis:duplicate-candidate-status
```

Example:

```text
Duplicate candidate status:
- current output candidates: 26
- new distinct setups: 18
- duplicate open positions: 4
- duplicate recent captures: 2
- improved add-ons: 1
- suppressed duplicates: 5
```

## UI Requirements

Today’s Candidates must never display duplicate candidates as ordinary new candidates.

Examples:

```text
Duplicate of open position.
View Existing Position.
```

```text
Repeated signal suppressed.
No material improvement detected.
```

```text
Improved signal detected.
Review Add-On Candidate.
```

## Acceptance Tests

Tests must prove:

- same-session duplicates are suppressed
- same-day duplicates are classified
- open-position duplicates become `DUPLICATE_OPEN_POSITION`
- open-position duplicates do not expose normal capture actions
- materially improved duplicates become reviewable
- cooldown-expired duplicates become reviewable
- recently closed duplicates respect cooldown policy
- every duplicate receives explicit classification
- silent duplicate handling is impossible

## Relationship to Existing Documents

This document is subordinate to:

- `AEGIS_OPERATOR_WORKFLOW.md`
- `AEGIS_POSITIONS_UI_REQUIREMENTS.md`
- `AEGIS_CANDIDATE_READINESS_REQUIREMENTS.md`
- `AEGIS_CANDIDATE_CONSTRUCTION_REQUIREMENTS.md`
- `AEGIS_CONTRACT_GENERATION_REQUIREMENTS.md`
- `AEGIS_SIGNAL_EVIDENCE_BOUNDARY_REQUIREMENTS.md`
- `AEGIS_OPERATOR_STATUS_CLASSIFICATION_REQUIREMENTS.md`

## Governance Rule

No candidate may be presented as a new actionable candidate until duplicate classification has been evaluated.

Duplicate classification must occur before operator review.
