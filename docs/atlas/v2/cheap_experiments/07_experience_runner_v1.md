# Atlas V2 Experience Runner V1

The Experience Runner V1 is a fixture-based batch runner for producing outcome-linked Atlas V2 experience from cheap experiments.

It implements this loop:

CheapExperiment -> Prediction -> Outcome -> Regret -> CalibrationRecord -> optional BehaviorChange -> ExperienceEvent -> LearningVelocityMetric.

## Operating Scope

V1 runs fixed fixture definitions only. It does not crawl, discover, generate hypotheses, ingest YouTube, create candidates, create sleeves, create paper positions, place trades, allocate capital, recommend action, execute autonomously, or validate investment claims.

The runner is record/read/audit-only infrastructure. It writes append-only Atlas V2 ledger records through the existing `AtlasV2Ledger` object model.

## Initial Tier Policy

`TIER_0_DEDUPE` and `TIER_1_SANITY` are executable by the V1 runner at high volume.

`TIER_2_LIGHTWEIGHT_VALIDATION` is not executed by V1.

`TIER_3_ROBUST_VALIDATION` and `TIER_4_MATURITY_TRACKING` are rejected unless a valid `PromotionGateDecision` exists. V1 does not mint gate decisions.

## Batch Outputs

Each accepted fixture emits:

- `AttentionDecision`
- `Prediction`
- `Outcome`
- `Regret`
- `CalibrationRecord`
- `CheapExperiment`
- `ExperienceEvent`

The batch emits:

- `CheapExperimentBatch`
- `LearningVelocityMetric`
- one `ExperimentTierSummary` per tier present

## Safety Contract

The runner may only improve learning velocity by increasing cheap prediction-to-outcome cycles.

It may not create or imply:

- trading authority
- broker execution
- autonomous execution
- sleeve creation
- candidate creation
- paper-position creation
- capital allocation
- recommendations
- validation authority

Every emitted record remains append-only and audit-visible.

## Runner Budget

`RunnerBudget` bounds each run before records are emitted. Required fields are:

- `runner_id`
- `max_experiments_per_run`
- `max_experiments_per_day`
- `allowed_tiers`
- `max_tier_2_per_day`
- `tier_3_enabled`
- `tier_4_enabled`
- `stop_on_error_count`
- `dry_run`
- `created_at`

Budget pressure rejects fixtures instead of silently expanding scope. `dry_run=true` computes the same summary through an isolated temporary ledger and does not append records to the target ledger.

## CLI

Example dry run:

```bash
python -m ops.atlas.v2_experience_runner --count 1000 --tiers TIER_0_DEDUPE,TIER_1_SANITY --dry-run
```

Example append-only run:

```bash
python -m ops.atlas.v2_experience_runner --count 1000 --tiers TIER_0_DEDUPE,TIER_1_SANITY --ledger-root /tmp/atlas_v2_experience_runner_v1_ledgers
```

The CLI prints a JSON summary with requested, processed, rejected, prediction-outcome cycles, generated events, behavior changes, learning velocity values, and ledger paths written.
