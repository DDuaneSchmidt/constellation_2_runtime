# Aegis Outcome Validation Maturity Design v1

## Design

The maturity layer consumes the paper position ledger and research portfolio artifacts. It emits outcome, sample, ledger, sufficiency, and self-check artifacts. It extends the existing hypothesis state machine and Research Portfolio panel without replacing sleeve analytics, evidence lineage integrity, or portfolio structure.

## Modules

- `outcome_registry_v1`: classifies every paper position into an outcome state.
- `hypothesis_outcome_ledger_v1`: aggregates outcomes by hypothesis.
- `validation_sample_generator_v1`: converts resolved outcomes into included validation samples and emits excluded samples for incomplete/open evidence.
- `statistical_sufficiency_engine_v1`: evaluates deterministic sufficiency thresholds.
- `outcome_validation_maturity_self_check_v1`: audit gate for the full chain.

## Deterministic Transitions

The hypothesis state machine may consume sufficiency rows. Allowed transitions are `ACCUMULATING_EVIDENCE -> VALIDATION_READY`, `VALIDATION_READY -> VALIDATED`, `VALIDATION_READY -> DISPROVEN`, `VALIDATED -> DEGRADED`, `DEGRADED -> RETIRED`, and `DISPROVEN -> RETIRED`. Each transition includes evidence artifacts, sufficiency artifact, source hashes, and generated timestamp.

## Reproducible Outcome Construction

The outcome registry is the authority for validation-sample reproducibility. It must construct realized outcomes from evidence-bound marks only:

1. Entry price comes from the certified entry reference price that entered the paper ledger through the candidate contract and auto-promotion lifecycle.
2. Exit price comes from governed closure evidence. Review-only recommendations can identify a candidate closure condition, but cannot create a realized outcome by themselves.
3. Trigger timestamp comes from the exit evaluation or closure authority artifact that detected the condition.
4. Outcome timestamp comes from the outcome registry evaluation that records the resolved state.
5. Realized return is computed by a deterministic formula version over the evidence-bound entry and exit marks.

If any required source artifact, source hash, price timestamp, or formula version is missing, the position remains excluded from validation samples until the missing evidence exists. This is more important than auto-closure because validation samples are only useful when replay can reproduce the same realized return from the same evidence.

## Failure Modes

- Paper position missing outcome row.
- Resolved outcome missing validation sample.
- Validation sample missing hypothesis/sleeve/candidate/position/outcome linkage.
- Closed position missing exit mark or realized return.
- Closed position missing entry price source/hash, exit price source/hash, trigger timestamp, outcome timestamp, or deterministic return formula version.
- Resolved outcome realized return changes on rerun while source hashes are unchanged.
- `UNKNOWN_BLOCKED` without blocker reasons.
- Hypothesis missing ledger row.
- Sufficiency state without reason codes.
- Hypothesis transition without evidence artifacts.
