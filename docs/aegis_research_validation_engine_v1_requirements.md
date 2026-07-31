# Aegis Research Validation Engine V1 Requirements

## Purpose

Research Validation Engine V1 exists to deterministically test Aegis research hypotheses before they can influence candidates, strategies, portfolio attribution, or operator decisions.

The engine prevents weak, under-sampled, disproven, or freeform-only ideas from contaminating downstream workflows.

## Scope

V1 covers:

- versioned hypothesis registry records,
- deterministic validation protocol definitions,
- reproducible validation runs,
- deterministic validation results,
- promotion eligibility gates for candidate review,
- out-of-sample outcome feedback records.

## Non-Goals

V1 does not:

- create trades,
- recommend entries or exits,
- submit broker orders,
- enable live trading,
- enable autonomous execution,
- modify candidate generation behavior,
- modify sleeve logic,
- modify canonical trading artifacts,
- let AI certify hypotheses.

## Operator Problem Solved

The operator needs to know whether a research idea has deterministic evidence behind it before it appears in candidate, sleeve, or attribution workflows.

The operator should be able to see:

- what was tested,
- which protocol was used,
- how much evidence exists,
- whether the idea is supported, disproven, under-sampled, inconclusive, or not ready,
- whether the idea is eligible for candidate review.

## Research Problem Solved

Research needs a repeatable path from idea to evidence-backed conclusion. V1 creates a boundary between:

- proposed ideas,
- deterministic tests,
- validation results,
- candidate-review eligibility.

## Lifecycle Overview

```text
HYPOTHESIS_CAPTURED
  -> PROTOCOL_ASSIGNED
  -> VALIDATION_RUN_CREATED
  -> VALIDATION_RESULT_EMITTED
  -> PROMOTION_GATE_EVALUATED
  -> OUTCOME_FEEDBACK_RECORDED
```

## Hypothesis States

Allowed hypothesis registry statuses:

- CAPTURED
- ACTIVE
- READY_FOR_VALIDATION
- VALIDATION_RUNNING
- VALIDATED_SUPPORTED
- VALIDATED_DISPROVEN
- RETIRED

## Validation States

Allowed validation result statuses:

- NOT_READY
- UNDER_SAMPLED
- INCONCLUSIVE
- SUPPORTED
- DISPROVEN

Meanings:

- NOT_READY: required inputs or protocol binding are missing.
- UNDER_SAMPLED: deterministic test ran but sample count is below the protocol minimum.
- INCONCLUSIVE: enough data exists, but support/disproof thresholds are not met.
- SUPPORTED: deterministic thresholds support the hypothesis.
- DISPROVEN: deterministic thresholds contradict the hypothesis.

## Safety Rules

Research Validation may only produce research artifacts and candidate-review eligibility signals.

It must not:

- create orders,
- submit/transmit trades,
- produce trade advice,
- approve candidates,
- alter sleeve rules,
- bypass candidate readiness gates,
- enable live or autonomous trading.

SUPPORTED means eligible for candidate review only. It does not mean tradeable.

## Auditability Requirements

Every result must include:

- hypothesis_id,
- hypothesis_version,
- protocol_id,
- protocol_version,
- run_id,
- source artifacts,
- code version,
- generated timestamp,
- deterministic metrics,
- limitations,
- plain-English summary.

## Deterministic Validation Requirements

Validation must be protocol-driven and reproducible.

AI may summarize, explain, or propose follow-up. AI may not:

- choose the validation status,
- override deterministic metrics,
- promote a hypothesis,
- certify support.
