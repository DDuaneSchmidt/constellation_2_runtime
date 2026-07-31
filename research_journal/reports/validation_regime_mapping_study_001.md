# Validation Regime Mapping Study 001

Date: 2026-06-05

## Scope

This study determines whether candidate regime labels can be mapped to direct validation regimes. It is analysis-only and does not implement mapping logic.

Inputs used:

- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/candidate_data_validation_plan/latest.json`
- `research_journal/reports/candidate_replay_trace_table.csv`
- `research_journal/reports/regime_filter_root_cause_table.csv`
- Direct validation classifier output from `constellation_2/common/atlas_v2_research_os/candidate_backtests.py`

CSV companion: `research_journal/reports/validation_regime_mapping_table.csv`.

## Direct Validation Regime Vocabulary

The current daily proxy classifier emits:

- `HIGH_VOLATILITY`
- `TRENDING`
- `RANGE_BOUND`
- `LOW_VOLATILITY`
- `UNKNOWN`

It does not emit `CHOP`, `TREND`, `BREAKOUT`, `MEAN_REVERSION`, `REVERSAL`, or `EVENT_REACTION`.

## Candidate Metadata Labels

The direct validation candidate set has two actual candidate regime labels:

- `CHOP`
- `TRENDING`

It also has mechanism labels that can be confused with regimes:

- `BREAKOUT`
- `MEAN_REVERSION`
- `REVERSAL`
- `EVENT_REACTION`

Those mechanism labels should not be mapped as regimes without separate candidate metadata explicitly declaring them as regime labels.

## Aggregate Emitted Regime Counts

For candidates labeled `CHOP`, triggered samples emitted:

- `HIGH_VOLATILITY`: 698
- `LOW_VOLATILITY`: 2
- `RANGE_BOUND`: 396
- `TRENDING`: 2119
- `UNKNOWN`: 188

For candidates labeled `TRENDING`, triggered samples emitted:

- `HIGH_VOLATILITY`: 850
- `LOW_VOLATILITY`: 18
- `RANGE_BOUND`: 682
- `TRENDING`: 472
- `UNKNOWN`: 847

## Mapping Decisions

| candidate label | source | possible mapping | confidence | ambiguity | output |
|---|---|---|---|---|---|
| `CHOP` | candidate regime | `RANGE_BOUND`, `UNKNOWN`, `LOW_VOLATILITY` | MEDIUM | HIGH | PARTIAL_MAPPING |
| `TRENDING` | candidate regime | `TRENDING` | HIGH | MEDIUM | EXACT_MAPPING |
| `TREND` | requested alias | `TRENDING` | MEDIUM | MEDIUM | PARTIAL_MAPPING |
| `BREAKOUT` | mechanism label | none | HIGH | LOW | NO_MAPPING |
| `MEAN_REVERSION` | mechanism label | none | HIGH | LOW | NO_MAPPING |
| `REVERSAL` | mechanism label | none | HIGH | LOW | NO_MAPPING |
| `EVENT_REACTION` | mechanism label | none | HIGH | LOW | NO_MAPPING |
| `HIGH_VOLATILITY` | validation-regime only | `HIGH_VOLATILITY` if future candidate metadata uses it | LOW | MEDIUM | UNKNOWN |
| `RANGE_BOUND` | validation-regime only | `RANGE_BOUND`; possible CHOP proxy | MEDIUM | MEDIUM | PARTIAL_MAPPING |
| `LOW_VOLATILITY` | validation-regime only | `LOW_VOLATILITY`; weak CHOP proxy | LOW | HIGH | PARTIAL_MAPPING |
| `UNKNOWN` | validation-regime only | `UNKNOWN` only if candidate regime is explicitly unconstrained | LOW | HIGH | UNKNOWN |

## Interpretation

`TRENDING` can be mapped exactly at the vocabulary level because it is both a candidate regime label and an emitted validation regime. The mapping still has sample-distribution ambiguity because many triggered samples for `TRENDING` candidates emit `HIGH_VOLATILITY`, `RANGE_BOUND`, or `UNKNOWN`.

`CHOP` cannot be mapped exactly because the validation classifier never emits `CHOP`. It has only a partial, analysis-only relationship to `RANGE_BOUND`, `UNKNOWN`, and possibly `LOW_VOLATILITY`. That relationship needs explicit metadata or governance before it can be used in production validation.

Mechanism labels are not regime labels. `BREAKOUT`, `MEAN_REVERSION`, `REVERSAL`, and `EVENT_REACTION` should remain `NO_MAPPING` in a regime map.

## Recommendation

Before replay, direct validation should report whether each candidate regime label exists in the validation classifier vocabulary. If it does not, the report should mark the candidate as requiring regime-label compatibility review rather than silently eliminating all triggered samples.

This is a recommendation only. No implementation is included.

## Authority Boundary

This study is diagnostic only. It does not implement regime mapping, change replay behavior, relax production filters, promote candidates, change qualification, change governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.
