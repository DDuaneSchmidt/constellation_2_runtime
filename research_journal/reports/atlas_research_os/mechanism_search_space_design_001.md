# Atlas Research OS Design 034A: Mechanism Search Space Specification

Date: 2026-06-05
Status: Design only

Scope: defines a bounded technical-analysis mechanism search space for future Atlas Research OS implementation. This document does not implement candidate generation, paper setup, broker execution, live trading, trade advice, position sizing, portfolio construction, sleeve mutation, or capital authorization.

Runtime posture: this is a build-ready research specification only. Runtime readiness, if later implemented, must come from the runtime truth kernel and verified runtime graph. Consumers must not infer readiness from this design.

Hard rule: Search market behaviors, not blind indicator permutations.

## Objective

Atlas should search for repeated market behavior patterns that can be stated as mechanisms, replayed historically, falsified, and reviewed by a human before any paper-forward observation. The search space must be small enough to audit and expressive enough to describe common price, volume, volatility, session, and regime interactions.

The output of the future implementation should be research candidates for historical replay, not trading recommendations.

## Design Principles

- Mechanisms are first-class objects. Indicators are only measurement tools for mechanisms.
- Every searched pattern must map to a human-readable behavior hypothesis.
- Every primitive must have a declared source column, lookback, timestamp discipline, and leakage control.
- Search breadth must be bounded by mechanism family, regime dimension, timeframe, confirmation filter, invalidation rule, and complexity budget.
- A simpler behavior explanation must dominate a more complex expression unless the complex expression materially improves replay evidence across regimes.
- Historical replay is required before paper-forward observation.
- No search result can authorize live trading, broker execution, capital allocation, position sizing, or trade advice.

## Mechanism Object

Each mechanism candidate should be represented by a structured object:

```text
mechanism_id
mechanism_family
behavior_hypothesis
instrument_scope
timeframe
session_scope
regime_context
entry_condition
confirmation_filters
exit_condition
invalidation_condition
complexity_score
required_replay_tests
forbidden_uses
```

`behavior_hypothesis` is mandatory. A candidate without a plain-language behavior hypothesis is invalid even if its numerical condition passes.

## Mechanism Families

Allowed families:

| Family | Behavior Searched | Examples Of Valid Hypotheses |
| --- | --- | --- |
| `MEAN_REVERSION` | Price stretches away from a local reference and later reverts when participation or exhaustion confirms. | Extended downside move reclaims prior value after volume exhaustion. |
| `BREAKOUT_CONTINUATION` | Price compresses or repeatedly rejects a boundary, then expands through it with confirmation. | Range compression breaks above resistance during active session volume. |
| `BREAKDOWN_CONTINUATION` | Support failure continues after failed reclaim or weak participation. | Failed support reclaim after heavy selling leads to follow-through weakness. |
| `VWAP_OR_AVERAGE_RECLAIM` | Price loses and then recovers a session or rolling value reference. | Intraday selloff reclaims VWAP after liquidity absorption. |
| `OPENING_RANGE_RESOLUTION` | Early-session range defines later directional behavior. | Opening range break with volume confirmation holds through mid-session. |
| `GAP_RESPONSE` | Price reacts predictably after an overnight or event gap. | Gap-down stabilizes and fills when initial selling fails. |
| `VOLATILITY_COMPRESSION_EXPANSION` | Low realized range precedes expansion when a boundary breaks. | Narrow multi-bar range resolves in direction of higher-timeframe pressure. |
| `TREND_PULLBACK_CONTINUATION` | Pullback into a trend reference resumes when rejection or reclaim confirms. | Uptrend pullback holds moving reference and resumes with expanding participation. |
| `FAILED_BREAKOUT_OR_TRAP` | A breakout fails and reverses after acceptance does not appear. | Break above prior high fails back into range with weak volume follow-through. |
| `EVENT_REACTION` | Scheduled or unscheduled event creates repeatable post-event behavior. | Macro event spike fades after spread normalizes and price returns inside pre-event range. |
| `RELATIVE_STRENGTH_ROTATION` | Instrument outperforms or underperforms peers during a defined market state. | Sector leader holds above reference while index pulls back. |
| `LIQUIDITY_SWEEP_REVERSAL` | Price sweeps a visible level and reverses after failed continuation. | Prior low is swept, selling stalls, and price reclaims the swept level. |

Forbidden families:

- unbounded indicator combination search
- opaque model-only signals without mechanism labels
- pure price-target prediction without entry, exit, and invalidation logic
- strategies whose main premise is position sizing, leverage, execution routing, or capital allocation
- live-market action triggers

## Allowed Primitive Conditions

Primitives must be composable, auditable, and bounded. Each primitive must declare its lookback and comparison reference.

### Price Location

Allowed:

- close above or below prior high, low, open, close, midpoint, VWAP, anchored VWAP, moving average, or range boundary
- reclaim or loss of a declared reference level
- distance from reference as percent, ATR multiple, or z-score
- gap size relative to prior close, prior range, or ATR
- inside, outside, higher-high, lower-low, higher-low, lower-high structure

### Range And Volatility

Allowed:

- realized range compression or expansion
- ATR-normalized move size
- rolling high-low percentile
- true range percentile
- narrow-range or wide-range bar classification
- realized volatility percentile

### Volume And Participation

Allowed:

- volume above or below rolling median
- relative volume percentile
- volume expansion on boundary break
- volume contraction during pullback
- volume climax followed by failed continuation
- dollar-volume liquidity floor

### Momentum And Slope

Allowed:

- rate of change over declared lookback
- slope of price, moving average, VWAP, or regression line
- acceleration or deceleration using bounded differences
- momentum divergence only when tied to price behavior and declared explicitly

### Relative Behavior

Allowed:

- instrument return relative to benchmark
- sector or peer relative strength percentile
- correlation regime bucket
- beta-adjusted outperformance or underperformance

### Event And Calendar Context

Allowed:

- pre-event, event-window, and post-event labels
- earnings, macro release, FOMC, CPI, jobs, inventory, sector event, and known scheduled catalyst flags where source lineage exists
- day-of-week and month-end labels only as context, not standalone mechanism

Forbidden primitives:

- future bars, final marks, or revised data unavailable at decision time
- unconstrained parameter sweeps
- indicator values with no behavior explanation
- primitives that imply order routing, sizing, capital use, or live action
- source fields without lineage or timestamp discipline

## Regime Dimensions

Each replay must tag regimes. A mechanism may specify required, excluded, or neutral regimes.

Required regime dimensions:

- market trend: `UPTREND`, `DOWNTREND`, `RANGE`, `MIXED`
- volatility: `LOW`, `NORMAL`, `HIGH`, `EXPANDING`, `CONTRACTING`
- liquidity: `NORMAL`, `THIN`, `HEAVY`, `EVENT_DISTORTED`
- breadth or participation: `BROAD`, `NARROW`, `DIVERGENT`, `UNKNOWN`
- gap state: `NO_GAP`, `GAP_UP`, `GAP_DOWN`, `LARGE_GAP`
- event state: `NO_EVENT`, `PRE_EVENT`, `EVENT_WINDOW`, `POST_EVENT`
- relative strength state: `LEADER`, `LAGGARD`, `INLINE`, `UNKNOWN`

Regime labels must be computed only from data available at or before the evaluated timestamp. A mechanism that works only in one thinly sampled regime must carry a fragility warning.

## Timeframe And Session Dimensions

Allowed timeframes:

- intraday bars: `1m`, `5m`, `15m`, `30m`, `60m`
- daily bars: `1d`
- weekly context: `1w` as context only

Allowed session scopes:

- `PREMARKET_CONTEXT`
- `OPENING_RANGE`
- `MORNING_SESSION`
- `MIDDAY_SESSION`
- `AFTERNOON_SESSION`
- `CLOSE_AUCTION_CONTEXT`
- `REGULAR_SESSION`
- `OVERNIGHT_GAP_CONTEXT`

Timeframe rules:

- Entry and invalidation must be evaluated on the same or lower timeframe than the mechanism decision timeframe.
- Higher timeframe context may filter a mechanism but must not introduce future information.
- Session-specific mechanisms must not be replayed outside their declared session without creating a separate mechanism variant.
- Cross-timeframe mechanisms pay a complexity penalty.

## Confirmation Filters

Confirmation filters are optional, but if used they must support the behavior hypothesis.

Allowed confirmation filters:

- volume confirmation: break or reclaim occurs with relative volume above threshold
- volatility confirmation: expansion follows compression
- structure confirmation: reclaimed level holds for `n` bars
- relative strength confirmation: instrument outperforms benchmark during setup
- spread or liquidity confirmation where reliable data exists
- event confirmation: move occurs inside declared event window
- regime confirmation: mechanism is active only in declared regime

Forbidden confirmation filters:

- filters added only because they improved a backtest with no behavior rationale
- more than three independent filters unless explicitly approved for replay as a complexity exception
- filters that reference unavailable future outcomes
- filters that encode target return, position size, or execution route

## Entry, Exit, And Invalidation

This design may define research conditions, but it must not create trade instructions.

Entry condition:

- describes the market behavior that would start observation
- must be reproducible from time-ordered data
- must not include quantity, account, order type, broker, or capital language

Exit condition:

- describes the observation endpoint for replay measurement
- may use time stop, level failure, opposite signal, range completion, or session close
- must be declared before replay
- must not imply live execution

Invalidation condition:

- describes what would falsify the mechanism behavior
- must be observable and specific
- must be distinct from a mere adverse return where possible
- should identify whether failure came from regime mismatch, false confirmation, weak follow-through, data quality, or mechanism decay

Allowed invalidation rules:

- failure to hold reclaimed level within `n` bars
- failed breakout returns inside range and stays there for `n` bars
- volume confirmation absent after boundary break
- volatility expansion fails after compression break
- event-window behavior does not occur before declared expiry
- relative strength reverses below threshold
- source data missing, stale, or inconsistent

## Complexity Penalties

Each mechanism receives a complexity score. Lower is better.

```text
complexity_score =
  primitive_count
+ 2 * confirmation_filter_count
+ 2 * timeframe_count_above_1
+ 2 * regime_filter_count
+ 3 * parameter_count
+ 4 * exception_count
+ 5 * discretionary_override_count
```

Default eligibility bands:

| Complexity Score | Interpretation | Default Action |
| ---: | --- | --- |
| `0-5` | Simple mechanism | Eligible for replay if lineage gates pass. |
| `6-10` | Moderate mechanism | Eligible with anti-overfit notes. |
| `11-15` | Complex mechanism | Requires explicit justification and broader replay. |
| `>15` | Overfit risk | Reject unless human review grants a research exception. |

Complexity cannot be justified by higher apparent return alone. It must improve falsifiability, robustness, or regime explanation.

## Anti-Overfit Rules

Hard anti-overfit rules:

- no blind permutation search across indicators
- no optimizing thresholds against a single instrument, session, or event sample
- no selecting only favorable regimes after observing replay outcomes
- no using overlapping variants as independent evidence
- no survivorship-biased universes
- no lookahead, revised data leakage, or final-bar leakage
- no hidden discretionary overrides
- no promoting a mechanism that lacks an invalidation rule

Parameter discipline:

- thresholds must come from predefined grids with small cardinality
- preferred threshold forms are percentiles, ATR multiples, and round behavior categories
- each parameter must have a behavioral rationale
- parameter changes create a new mechanism variant with lineage
- neighboring parameter stability must be reported

Duplicate discipline:

- variants sharing the same family, regime, timeframe, and core behavior are one mechanism cluster
- only the simplest surviving variant in a cluster can be the representative candidate
- duplicate variants may be retained only as sensitivity evidence

## Required Historical Replay Validation

Before any paper-forward observation, a mechanism must pass historical replay validation.

Minimum replay requirements:

- declared data source, source hash, and timestamp discipline
- fixed instrument universe or explicit instrument-selection rule
- fixed replay window with start and end dates
- out-of-sample or walk-forward segment where feasible
- regime coverage report
- leakage audit
- duplicate and variant-cluster audit
- complexity score and penalty explanation
- entry, exit, and invalidation event ledger
- failure attribution summary
- sensitivity check for neighboring parameters
- replay result reproducibility hash

Required replay outputs:

```text
mechanism_replay_report.json
mechanism_replay_summary.md
mechanism_event_ledger.csv
mechanism_variant_cluster.json
```

Replay pass criteria:

- mechanism behavior is observed with reproducible event extraction
- invalidation events are recorded, not hidden
- performance summary is secondary to behavior survival and failure attribution
- at least one meaningful failure mode is documented
- no authority contamination appears in artifacts
- human review can understand why the mechanism might work and what would falsify it

Replay fail criteria:

- missing source lineage
- lookahead or timestamp leakage
- mechanism depends on narrow overfit parameters
- result is driven by a single event unless the mechanism is explicitly event-specific
- behavior hypothesis is not supported by event examples
- invalidation is absent, vague, or added after outcome review
- artifact contains trade recommendation, broker instruction, position sizing, live action, or capital language

## Implementation Contract

Future implementation should expose a mechanism search registry with:

- allowed mechanism family enum
- primitive condition enum
- regime dimension enum
- timeframe enum
- session enum
- confirmation filter enum
- invalidation rule enum
- complexity scoring function
- anti-overfit validator
- replay eligibility validator
- authority boundary validator

The generator must build candidates from mechanism templates, not free-form indicator permutations. The implementation should reject any candidate that lacks:

- mechanism family
- behavior hypothesis
- at least one primitive entry condition
- explicit invalidation condition
- replay validation plan
- authority boundary fields

## Authority Boundary

Allowed:

- research design
- historical replay specification
- paper-forward observation planning after replay and human review
- failure attribution
- mechanism clustering
- human review queue preparation

Forbidden:

- trade recommendation
- live trading
- broker execution
- automatic paper trade placement
- capital allocation
- position sizing
- portfolio construction
- sleeve mutation
- order routing
- production promotion

Authority statement for all future artifacts:

```text
Research-only mechanism search. No live trading, no broker execution, no automatic paper trade placement, no capital authority, no position sizing, and no trade recommendations.
```

## Build Readiness Checklist

An implementation task is ready when it can:

- encode the allowed mechanism families as versioned constants
- encode primitives and dimensions as closed enums
- construct mechanism candidates only from approved templates
- compute complexity penalties deterministically
- reject forbidden primitives and authority-contaminated text
- emit replay validation plans before scoring outcomes
- cluster near-duplicate variants
- preserve source lineage and replay hashes
- produce human-readable hypotheses, evidence summaries, risk notes, and invalidation notes
- enforce the hard rule that Atlas searches market behaviors, not blind indicator permutations

