# Aegis Position Review Product Requirements

## Purpose

Position Review exists to help the operator understand:

* why a position was entered,
* what has changed since entry,
* whether the thesis is improving or deteriorating,
* what matters most now,
* what should be monitored.

Position Review is not an execution system.

It exists to improve operator understanding.

---

# Core Principle

Position Review should answer:

```text
What matters?
```

not:

```text
What data exists?
```

The system should prioritize evidence instead of presenting all evidence equally.

---

# Operator Questions

Every position review must answer:

1. Why was this position entered?
2. What has changed since entry?
3. Is the thesis stronger, weaker, or unchanged?
4. What is the most important supporting evidence?
5. What is the most important contradicting evidence?
6. What is the most important risk?
7. What is the most important confirmation signal?
8. What should be monitored next?

If a review does not answer these questions, it is incomplete.

---

# Position Health

Position Review introduces a deterministic position health classification.

Allowed values:

```text
IMPROVING
STABLE
DETERIORATING
```

Position Health must be computed from deterministic evidence.

AI may explain the classification.

AI may not decide the classification.

---

# Thesis Status

Position Review introduces a deterministic thesis status.

Allowed values:

```text
THESIS_STRENGTHENED
THESIS_UNCHANGED
THESIS_WEAKENED
```

The status must be derived from deterministic evidence.

AI may explain the status.

AI may not invent the status.

---

# Evidence Ranking

Not all evidence is equally important.

Required ranking:

```text
HIGH
MEDIUM
LOW
```

Evidence categories:

## High Priority

Market Evidence

Examples:

* trend condition
* momentum
* volatility
* breadth
* correlation
* regime

Signal Evidence

Examples:

* signal persistence
* signal quality
* signal confirmation
* signal degradation

## Medium Priority

Validation Evidence

Examples:

* qualification
* construction
* validation
* candidate state

## Low Priority

Process Evidence

Examples:

* receipt creation
* linkage
* attribution recovery
* internal workflow state

---

# Required Review Structure

Every position review must contain:

## Thesis Summary

Why the position exists.

## Position Health

One of:

```text
IMPROVING
STABLE
DETERIORATING
```

With explanation.

## Thesis Status

One of:

```text
THESIS_STRENGTHENED
THESIS_UNCHANGED
THESIS_WEAKENED
```

With explanation.

## Most Important Supporting Evidence

Top-ranked supporting evidence.

Maximum:

```text
3
```

items.

## Most Important Contradicting Evidence

Top-ranked contradicting evidence.

Maximum:

```text
3
```

items.

## Most Important Risk

Single highest-priority risk.

## Most Important Confirmation

Single highest-priority confirmation signal.

## Monitoring Points

Maximum:

```text
5
```

items.

Only operator-relevant monitoring points.

## Data Quality

Coverage, missing evidence, and confidence limitations.

---

# AI Brief Responsibilities

The AI brief should:

* explain
* summarize
* prioritize
* compare

The AI brief should not:

* restate every evidence row
* enumerate every diagnostic
* expose internal implementation details
* expose raw pipeline state

---

# Internal State Translation

Internal states must be translated.

Example:

```text
PROMOTION_BLOCKED
```

becomes:

```text
Evidence remains insufficient for stronger conviction.
```

Example:

```text
EVIDENCE_NOT_DEMANDED
```

becomes:

```text
Additional evidence exists but was not required.
```

Raw internal labels belong only in diagnostics.

---

# Key Insight Section

Every review must include:

```text
Key Insight
```

Format:

```text
The thesis remains stable because trend evidence
remains intact, although confirmation breadth has weakened.
```

The Key Insight should fit in:

```text
1-3 sentences
```

and appear near the top of the review.

---

# Context Requirements

Position Review must consume:

```text
aegis_position_review_context_v1
```

Position Review must not consume raw artifacts directly.

---

# Deterministic Scoring Layer

Create:

```text
aegis_position_review_score_v1
```

Purpose:

Compute:

* position_health
* thesis_status
* evidence_rankings
* confidence

This layer is deterministic.

The AI brief explains the output.

The AI does not generate the score.

---

# Auditability Requirements

Every review must expose:

* context hash
* source references
* generated_at
* data quality
* score version
* brief version

All AI claims must be traceable to evidence.

---

# Safety Requirements

Position Review may not:

* recommend entry
* recommend exit
* recommend sizing
* recommend execution
* override qualification
* override risk

The system may discuss:

* risks
* confirmations
* monitoring points

only.

---

# UX Requirements

The operator should be able to answer within:

```text
10 seconds
```

1. Is this position improving?
2. Is the thesis stronger or weaker?
3. What is the biggest risk?
4. What is the biggest confirmation?
5. What should I watch?

without reading diagnostics.

---

# Acceptance Criteria

A review is considered successful if:

* Position Health is present.
* Thesis Status is present.
* Key Insight is present.
* Supporting evidence is ranked.
* Contradicting evidence is ranked.
* A single primary risk is identified.
* A single primary confirmation is identified.
* Internal implementation language is absent from primary sections.

---

# Governance Rule

Position Review exists to improve operator understanding.

It does not participate in:

* candidate generation
* qualification
* sleeve activation
* execution
* broker workflows

Update:

```text
aegis/modules/operator_portal/aegis.module.yaml
```

to reference this document as the product authority for Position Review.
## Operator UI Architecture Authority

This document is subordinate to `AEGIS_OPERATOR_UI_ARCHITECTURE_REQUIREMENTS.md` for rendering architecture. Pages must consume the Operator Surface Contract through shared templates before rendering page-specific content.

