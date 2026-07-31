# Atlas Search Space Mapping 001

Status: Design only

Scope: defines a future Atlas search-space mapping model. This document does not implement code, create schemas, modify Atlas, modify AEGIS runtime truth, create candidates, approve paper-forward work, place paper trades, provide trade advice, authorize broker execution, authorize live trading, allocate capital, or change qualification state.

## Objective

Search Space Mapping should become the long-term Atlas object that answers:

- What regions have been explored?
- What regions generate candidates?
- What regions generate failures?
- What combinations are underexplored?
- How should Atlas measure coverage?

The purpose is not to generate more ideas. The purpose is to understand where Atlas has spent attention, where that attention produced useful evidence, where it produced repeated failure, and where uncertainty remains high enough to justify future research.

## Core Claim

Atlas should treat `SearchSpace` as the organizing object above claims, observations, mechanisms, hypotheses, experiments, candidates, paper-forward observations, failures, and learning notes.

Current architecture already points in this direction:

```text
Research Domain
-> Search Space
-> Source / Observation / Claim Intake
-> Research Question
-> Mechanism Model
-> Hypothesis Family
-> Hypothesis Draft
-> Experiment Design
-> Evidence Flow
-> Knowledge Update
-> Source and Generator Attribution
-> Meta-Learning
```

The missing piece is a map. Atlas needs to know not only what it has found, but where it has searched.

## Search Space Coordinates

A search-space region should be described by a bounded coordinate tuple. The tuple should be expressive enough to locate research work, but not so detailed that every experiment becomes its own isolated point.

Recommended top-level coordinates:

```text
domain
mechanism_family
instrument_scope
timeframe
session_scope
regime_context
source_type
evidence_level
failure_taxonomy
candidate_lifecycle_state
paper_forward_state
data_readiness_state
authority_boundary
```

Example:

```text
domain: market_behavior
mechanism_family: BREAKOUT_CONTINUATION
instrument_scope: equities_index_proxy
timeframe: daily_with_intraday_claim
session_scope: regular_session
regime_context: TRENDING
source_type: observation_import
evidence_level: HISTORICAL_REPLAY
failure_taxonomy: proxy_dependency
candidate_lifecycle_state: paper_forward_observation_ready
paper_forward_state: planned_not_resolved
data_readiness_state: proxy_only
authority_boundary: research_only
```

## What Regions Have Been Explored?

Based on current Atlas V2 artifacts, explored regions are concentrated in:

- technical market-behavior mechanisms
- SPY or index-proxy-backed historical replay
- daily proxy evidence, even when the mechanism is intraday by nature
- paper-forward observation planning rather than resolved paper-forward outcomes
- CHOP and TRENDING regime labels in final candidate ranking
- mechanisms including BREAKOUT, MEAN_REVERSION, EVENT_REACTION, REVERSAL, VWAP_OR_AVERAGE_RECLAIM, SESSION_TIMING, OPENING_RANGE, TREND_CONTINUATION, LIQUIDITY_SWEEP, and VOLATILITY_EXPANSION
- failure analysis around evidence maturity, authority readiness, outcome maturity, candidate volume versus quality, fixture generalization, data readiness, runtime dependency, warning integrity, artifact lineage, and certification gates

Current quantitative signals:

- final candidate ranking evaluated 600 candidates
- 58 were classified ready for paper-forward observation
- 490 were rejected for now
- 44 were too fragile
- 6 needed data improvement
- 2 were too proxy-dependent
- final top-20 mechanisms were concentrated in BREAKOUT, MEAN_REVERSION, EVENT_REACTION, and REVERSAL
- campaign candidates were concentrated in BREAKOUT, MEAN_REVERSION, EVENT_REACTION, and REVERSAL
- top-20 regime distribution was concentrated in CHOP and TRENDING
- the largest remaining evidence weakness was proxy dependency across all evaluated candidates

Interpretation: Atlas has explored a large local technical-analysis region, but much of the work sits inside a proxy-heavy, replay-forward, unresolved-outcome region.

## What Regions Generate Candidates?

Candidate-producing regions are the regions that repeatedly create replay-positive, reviewable, or paper-forward-ready objects.

Current candidate-producing regions:

| Region | Evidence | Current signal |
| --- | --- | --- |
| BREAKOUT in CHOP/TRENDING proxy replay | final candidate ranking | largest share of top-20 candidates |
| MEAN_REVERSION in CHOP/TRENDING proxy replay | final candidate ranking and paper-forward campaign | recurring top candidate family |
| EVENT_REACTION imported observations | paper-forward campaign | ready when proxy backtest support exists, but source/event lineage risk remains |
| REVERSAL imported observations | paper-forward campaign | candidate-producing but overlap with mean reversion is material |
| VWAP_OR_AVERAGE_RECLAIM, SESSION_TIMING, OPENING_RANGE, TREND_CONTINUATION, LIQUIDITY_SWEEP, VOLATILITY_EXPANSION | paper-forward observation plans | candidate-producing in plan space, but often UNKNOWN-regime and not yet resolved |

Candidate production should not be treated as success by itself. A region can generate many candidates and still be low quality if it also generates proxy dependence, duplicate mechanisms, fragile drawdowns, low sample size, or unresolved paper-forward outcomes.

## What Regions Generate Failures?

Failure-producing regions are not only bad strategy regions. They include process, evidence, authority, data, and runtime regions where Atlas repeatedly mistakes one type of progress for another.

Current failure-producing regions:

| Region | Failure pattern |
| --- | --- |
| architecture/design maturity without empirical validation | architecture maturity mistaken for evidence maturity |
| paper workflow progress | paper progress mistaken for runtime authority readiness |
| open observations and paper positions | observation accumulation mistaken for outcome evidence |
| high raw signal or candidate count | volume mistaken for candidate quality |
| standalone indicator claims | broad support mistaken for relationship-specific durable support |
| active sleeves with low output | sleeve quality misdiagnosed before no-signal, data, or conversion checks |
| rich research documentation | capital-review readiness inferred from artifact richness |
| architecture deprecation plans | runtime dependencies ignored |
| macro/event workflow expansion | missing data treated as repairable by behavior expansion |
| clean or passing audits | auditability mistaken for understanding maturity |
| recurring non-blocking warnings | warnings treated as operational noise |
| generated-only regime memories | regime mismatch and validation confusion |
| selected backlog items | worker compatibility or artifact existence assumed |
| safety/certification progress | partial gate pass or lower blocker count mistaken for certification |

Search Space Mapping should track these as negative-yield regions. A region that generates repeated failure should not disappear from the map; it should become a well-labeled negative knowledge zone.

## What Combinations Are Underexplored?

Underexplored combinations are not just empty cells. A cell is meaningfully underexplored when it has high plausible learning value, low evidence maturity, and no clear retirement or duplicate reason.

Likely underexplored combinations:

| Combination | Why underexplored | Needed evidence |
| --- | --- | --- |
| candidate-specific data x currently proxy-backed candidates | final ranking flags proxy dependency across all selected candidates | direct candidate-specific historical replay and forward observation |
| intraday mechanisms x intraday data | many mechanisms are intraday but proxy evidence is daily | timestamped intraday source lineage and replay |
| event reaction x event metadata lineage | event candidates exist but event type/timing/surprise lineage is fragile | scheduled and unscheduled event source records |
| mechanism family x regime grid beyond CHOP/TRENDING | current top candidates concentrate in CHOP and TRENDING | high-volatility, range, event-distorted, liquidity-thin, gap-state coverage |
| mechanism clusters x duplicate attribution | mechanisms overlap across reversal, mean reversion, VWAP reclaim, liquidity sweep, and opening range | non-overlapping sample attribution and cluster-level duplicate checks |
| paper-forward planned x paper-forward resolved | plans exist but outcomes are sparse or unresolved | closed paper-forward outcomes with certified marks |
| source types beyond observation import | source quality is not yet broadly measured across papers, books, videos, human ideas, and failure-derived prompts | source-type attribution and downstream evidence yield |
| failure taxonomy x candidate generation | failure patterns are known, but not fully used to select or suppress search regions | failure-linked task scoring and retirement/compression rules |
| data readiness x mechanism feasibility | missing data often appears late as a blocker | pre-search data availability checks by coordinate |
| research cost x learning yield | effort is estimated after the fact | task-level time/cost tracking linked to search-space regions |

## Coverage Model

Atlas should measure coverage across cells, but the cells must carry evidence weight. A cell with 100 generated-only observations is not more covered than a cell with 5 certified resolved outcomes.

Suggested coverage dimensions:

```text
coverage_cell = (
  domain,
  mechanism_family,
  regime_context,
  timeframe,
  instrument_scope,
  source_type,
  evidence_level
)
```

Each cell should track:

- observations_count
- claims_count
- hypotheses_count
- experiment_design_count
- replay_count
- replay_supported_count
- replay_rejected_count
- candidate_count
- paper_forward_plan_count
- paper_forward_resolved_count
- failure_count
- retired_or_suppressed_count
- duplicate_count
- unresolved_assumption_count
- data_gap_count
- authority_boundary_violation_count
- human_review_count
- estimated_research_hours

## Coverage Metrics

Atlas should measure at least five types of coverage.

### 1. Surface Coverage

How many cells have been touched at all?

```text
surface_coverage = explored_cells / eligible_cells
```

This is the weakest metric. It is useful for map completeness but should not drive decisions by itself.

### 2. Evidence-Weighted Coverage

How much of the explored map has mature evidence?

Recommended evidence weights:

| Evidence level | Weight |
| --- | ---: |
| GENERATED_ONLY | 0.05 |
| CLAIM_ONLY | 0.10 |
| OBSERVATION_ONLY | 0.20 |
| HISTORICAL_REPLAY | 0.45 |
| HUMAN_REVIEWED_REPLAY | 0.55 |
| PAPER_FORWARD_PLAN | 0.50 |
| PAPER_FORWARD_OBSERVATION_OPEN | 0.65 |
| PAPER_FORWARD_OUTCOME_RESOLVED | 0.85 |
| VALIDATED_CLOSED_SAMPLE_SET | 1.00 |

```text
evidence_weighted_coverage =
  sum(cell_evidence_weight) / eligible_cells
```

### 3. Candidate-Yield Coverage

Where does exploration produce candidates?

```text
candidate_yield =
  candidate_count / explored_cell_count
```

This must be paired with quality and failure rates, because candidate volume alone is a known failure pattern.

### 4. Failure-Yield Coverage

Where does exploration produce useful negative knowledge?

```text
failure_yield =
  named_failure_count / explored_cell_count
```

High failure yield is not necessarily bad. It may mean the region is being usefully falsified. The bad case is high repeated failure with no retirement, suppression, or learning update.

### 5. Resolution Coverage

How much explored work has reached a resolved learning state?

```text
resolution_coverage =
  (resolved_positive_count + resolved_negative_count) / explored_cell_count
```

This should become the primary maturity metric. Atlas should prefer resolved knowledge over open exploration.

## Region State Model

Every region should have a state:

| State | Meaning |
| --- | --- |
| `UNSEEN` | No known work in this region. |
| `TOUCHED` | At least one observation, claim, or generated review exists. |
| `QUESTIONED` | A research question or uncertainty object exists. |
| `HYPOTHESIZED` | Hypothesis draft or family exists. |
| `REPLAYED` | Historical replay or equivalent test exists. |
| `CANDIDATE_GENERATING` | Region has generated candidates. |
| `PAPER_FORWARD_PLANNED` | Candidate has an observation plan. |
| `PAPER_FORWARD_OBSERVING` | Forward observation is open. |
| `RESOLVED_POSITIVE` | Resolved evidence supports continued study. |
| `RESOLVED_NEGATIVE` | Resolved evidence falsifies or weakens the region. |
| `FAILURE_PATTERNED` | Repeated named failures exist. |
| `RETIRED_OR_SUPPRESSED` | Region is intentionally avoided unless reopened with justification. |

State transitions must preserve lineage. A region should not move from `TOUCHED` to `CANDIDATE_GENERATING` without evidence of the intermediate path.

## Search-Space Heatmaps

Atlas should eventually produce heatmaps for:

- mechanism x regime
- mechanism x timeframe
- mechanism x source type
- mechanism x evidence level
- mechanism x failure taxonomy
- source type x candidate yield
- source type x failure yield
- regime x paper-forward survival
- data readiness x candidate yield
- evidence level x resolved outcome rate
- research hours x information gain

These heatmaps should be research dashboards, not execution dashboards.

## How To Answer The Five Questions

### What regions have been explored?

Count every cell with any source, observation, claim, hypothesis, replay, candidate, paper-forward item, or failure. Report both raw explored cells and evidence-weighted explored cells.

### What regions generate candidates?

For each explored cell, count candidate creation and paper-forward plan creation. Then normalize by exploration count and evidence maturity. Report candidate yield separately from candidate quality.

### What regions generate failures?

For each explored cell, count named failure taxonomy hits. Separate productive negative learning from repeated unresolved failure.

### What combinations are underexplored?

Find cells with:

- high uncertainty
- plausible mechanism importance
- low evidence-weighted coverage
- no retirement record
- no duplicate suppression
- feasible data path
- clear falsification test

### How should Atlas measure coverage?

Coverage should be measured as a portfolio of maps:

- surface coverage
- evidence-weighted coverage
- candidate-yield coverage
- failure-yield coverage
- resolution coverage
- duplicate-adjusted coverage
- cost-adjusted information gain

No single coverage percentage is enough.

## Design Risks

- The map can become false precision if coordinates are too granular.
- Search-space coverage can reward breadth over learning.
- Candidate-yield heatmaps can recreate the known failure of treating volume as quality.
- Failure-heavy regions may be incorrectly avoided even when they are high learning-yield.
- Evidence weights can become political if not fixed and versioned.
- Unknown or weak regime labels can make heatmaps look more complete than they are.
- Source-type coverage can be gamed by ingesting low-quality claims.

## Governance Rules

- Search-space state is advisory research metadata.
- Search-space coverage cannot authorize trading, broker execution, live activity, capital allocation, position sizing, paper placement, replay override, or qualification override.
- Generated-only cells must remain visibly low weight.
- Candidate-producing cells must still pass evidence, lineage, governance, and human review.
- Failure-producing cells should feed retirement, suppression, or redesign review, not automatic deletion.
- Underexplored cells should become research questions, not automatic candidates.

## Recommended Next Design Work

1. Define canonical `SearchSpaceRegion` fields.
2. Define a small initial eligible-cell universe instead of an unbounded grid.
3. Add a source-type taxonomy for observations, claims, papers, books, videos, human ideas, failures, and system diagnostics.
4. Add failure-taxonomy linkage to each region.
5. Define evidence-weighted coverage formulas with versioned weights.
6. Define duplicate and retirement handling at the region level.
7. Design human-readable heatmaps before any implementation.

## Final Recommendation

Approve Search Space Mapping as a high-upside design direction.

Do not implement it yet as automation. First, define the bounded coordinate system, evidence weights, and region-state model. The first useful version should be a read-only map that helps a human answer where Atlas has explored, where it is over-concentrated, where it is repeating failures, and where the next research hour is most likely to reduce uncertainty.
