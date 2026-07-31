# Aegis Research Capital Allocation Requirements v1

## Scope

Allocate research attention only: candidate-generation priority, research queue priority, compute effort, review attention, paper-testing bandwidth, and hypothesis development effort. This is not brokerage capital allocation and must not feed broker execution.

## Non-Goals

No broker automation, IB execution, real-money allocation, tax harvesting, Oak Harvest replacement, black-box AI allocation, discretionary allocation changes, or unrelated dashboards.

## Entities

Research Program: grouping of related theses, hypotheses, sleeves, and evidence with `research_program_id`, `name`, `description`, linked thesis/hypothesis/sleeve IDs, status, current allocation units, recommendation, score, and reason codes.

Research Capital Unit: normalized non-dollar attention unit.

Allocation Decision: deterministic daily recommendation with prior units, recommended units, delta, recommendation, reason codes, supporting artifacts, source hashes, model version, and generated time.

Allocation Event Log: immutable daily event rows recording recommendation and unit changes with evidence deltas.

## Artifact Contracts

Daily artifacts must be generated for program registry, scoring, allocation decisions, event log, and self-check. Artifacts must include `allocation_type: RESEARCH_ATTENTION_ONLY` and explicit safety disclaimers.

## Determinism

Use fixed thresholds, versioned config, stable sorting, explicit default values, explicit missing-data behavior, hashable inputs, and no AI free-text scoring input.

## Audit Requirements

Self-check must verify active hypotheses/sleeves map to programs, decisions include reason codes and source artifacts, recommendations are valid, score components are complete, missing outcomes are not used as proof, retired/disproven programs are not increased, data-blocked programs are not increased, output is deterministic, and program IDs are unique.
