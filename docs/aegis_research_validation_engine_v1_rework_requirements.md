# Aegis Research Validation Engine V1 Rework Requirements

## Purpose

This rework package defines the changes required before `ACC-20260530-008 Research Validation Engine` may move from `IMPLEMENTED` to `VALIDATED`.

The first V1 implementation established deterministic research validation artifacts, but the hostile audit found that it is not yet strong enough to govern candidate eligibility. The rework must convert the engine from a fail-closed reporting layer into an enforceable validation boundary.

## Scope

The rework covers:

- candidate-generation promotion gate enforcement,
- protocol-specific deterministic evaluators,
- protocol-required metric enforcement,
- artifact-to-hypothesis binding validation,
- immutable reproducibility references,
- enforced lookahead, survivorship, and selection-bias controls,
- statistically defensible sample-size policy,
- expanded hostile tests and validation evidence.

## Non-Goals

The rework must not:

- change candidate generation logic beyond enforcing promotion eligibility,
- change sleeve thresholds or sleeve logic,
- create trades,
- recommend trades,
- submit broker orders,
- enable broker execution,
- enable live trading,
- enable autonomous execution,
- alter canonical trading artifacts,
- allow AI or free-form text to certify a hypothesis.

## Root Cause Summary

### P0-1: Candidate generation bypass risk

Root cause:

The promotion gate exists as an artifact, but downstream candidate-generation paths are not required to consume it. The engine emits `candidate_generation_behavior_changed=false`, and candidate systems can continue operating without proving that hypothesis-derived inputs have `eligible_for_candidate_review=true`.

Why this matters:

A weak or under-sampled hypothesis can still influence future candidate workflows if any candidate producer uses research claims directly instead of the promotion gate.

### P0-2: Generic evaluator allows protocol requirements to be skipped

Root cause:

The current evaluator applies a shared hit-rate and average-return rule to every protocol. It does not enforce each protocol's required metrics, required source artifacts, benchmark-relative thresholds, event-window rules, survival rules, or protocol-specific exclusion logic.

Why this matters:

A hypothesis can be marked `SUPPORTED` without satisfying the full protocol definition.

### P1: Weak artifact-to-hypothesis binding

Root cause:

Validation runs reference source artifacts, but the engine does not prove that each source artifact is owned by, scoped to, or correctly filtered for the hypothesis being evaluated.

Why this matters:

Samples from one hypothesis or universe could be reused incorrectly for another hypothesis and produce a deterministic-looking but false result.

### P1: Incomplete reproducibility

Root cause:

Validation runs include a `code_version` string, but not immutable code references, protocol hashes, evaluator hashes, input hashes, or deterministic configuration hashes.

Why this matters:

The same run cannot be independently reproduced with full confidence after code or protocol changes.

### P1: Bias controls documented but weakly enforced

Root cause:

Protocols list lookahead guardrails and exclusion rules, but the evaluator does not require concrete evidence that lookahead, survivorship, duplicate-event, stale-data, and selection-bias controls passed.

Why this matters:

The engine can validate data that appears complete but is contaminated by future information, cherry-picked universes, duplicate events, or survivorship bias.

### P1: Sample-size requirements are not defensible

Root cause:

Minimum sample sizes are hard-coded constants. They are explicit, but they are not tied to confidence targets, effect-size assumptions, per-protocol requirements, or statistical power.

Why this matters:

The engine may eventually mark a hypothesis `SUPPORTED` on evidence that is too weak for the claim being made.

## Required Rework Outcomes

1. Candidate generation must require promotion eligibility for any hypothesis-derived candidate input.
2. Protocol-specific evaluators must exist for:
   - `EVENT_WINDOW_RETURN_V1`,
   - `MEAN_REVERSION_FORWARD_RETURN_V1`,
   - `POST_SIGNAL_SURVIVAL_V1`.
3. Protocol-required metrics must be enforced before `SUPPORTED` or `DISPROVEN` can be emitted.
4. Artifact ownership and hypothesis binding must be validated before a run can count samples.
5. Reproducibility must include immutable version references.
6. Bias controls must be enforced as deterministic checks, not merely documented.
7. Sample-size policy must be defensible and auditable.
8. The hostile audit target score after rework is `>= 85/100`.

## Safety Requirements

The rework may only change research validation and candidate eligibility gating for hypothesis-derived inputs.

The following must remain false:

- `trade_advice_allowed=false`,
- `broker_execution_allowed=false`,
- `broker_submit_transmit_allowed=false`,
- `live_trading_allowed=false`,
- `autonomous_live_trading_allowed=false`.

`SUPPORTED` must continue to mean:

```text
eligible for candidate review only
```

It must not mean:

```text
tradeable
approved candidate
position should be opened
execution should occur
```

## Operator Requirements

Research UI must explain:

- why a hypothesis is not eligible,
- which protocol was used,
- which required metrics are missing or failed,
- whether sample count is sufficient,
- whether bias checks passed,
- whether source binding is valid,
- whether promotion eligibility is blocked.

The UI must not expose broker/trading actions from research validation.

## Auditability Requirements

Every validation result must expose:

- hypothesis ID and version,
- protocol ID and version,
- protocol hash,
- evaluator ID and version,
- evaluator code hash or immutable code reference,
- input context hash,
- source artifact paths and hashes,
- artifact binding status,
- required metric status,
- bias-control status,
- sample-size policy status,
- deterministic status basis,
- promotion gate decision.

## Required Failure Behavior

The engine must fail closed.

If any of the following cannot be proven, validation status must not be `SUPPORTED`:

- required source artifact exists and is current,
- artifact belongs to the evaluated hypothesis or explicit allowed shared source,
- required metrics are present,
- sample-size policy passes,
- lookahead guardrail passes,
- survivorship guardrail passes,
- selection-bias guardrail passes,
- protocol-specific evaluator completed,
- immutable version references are present.

## Change Control Requirement

`ACC-20260530-008` must remain `IMPLEMENTED` until the rework is complete and validation evidence exists.

It may move to `VALIDATING` only after:

- rework implementation records exist,
- tests prove the P0/P1 failures are closed,
- browser/UI evidence shows operator-facing validation state correctly,
- audit and portal smoke pass.

It may move to `VALIDATED` only after a hostile re-audit reaches at least `85/100` with no remaining P0 issues.
