# Qualification Failure Attribution Ledger 001

Date: 2026-06-05

Status: ANALYSIS_ONLY

Companion CSV: `research_journal/reports/qualification_failure_attribution_ledger.csv`

## Scope

Convert qualification failure analysis into a structured attribution ledger for materialized failed candidates.

Inputs reviewed:

- `research_journal/reports/qualification_failure_analysis_001.md`
- `research_journal/reports/qualification_failure_table.csv`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest.json`
- `reports/atlas_v2_research_os/candidate_backtests/latest.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/historical_replay/latest.json`
- `reports/atlas_v2_research_os/replay_sample_yield/latest.json`

This is a diagnostic ledger only.

No candidate promotion is made. No qualification change is made. No replay change is made.

## Source Limits

The full qualification population is known:

| Measure | Count |
| --- | ---: |
| candidates evaluated | 600 |
| final eligible candidates | 110 |
| final score below 0.7 / `REJECT_FOR_NOW` | 490 |
| materialized excluded preview rows | 100 |
| materialized `REJECT_FOR_NOW` rows ledgered | 56 |

The ledger covers the 56 materialized failed candidates, not all 490 failed candidates. Aggregate failure groups remain in `qualification_failure_table.csv`; this ledger intentionally excludes `GROUP::` rows and records only candidate-level materialized failures.

## Ledger Fields

The CSV includes one row per materialized failed candidate with:

- `candidate_id`
- `mechanism`
- `regime`
- `final_score`
- `primary_failure_class`
- `secondary_failure_class`
- `fixable_vs_true_rejection`
- `evidence_gap`
- `proxy_dependence`
- `intraday_required`
- `event_metadata_required`
- `regime_vocab_issue`
- `sample_size_issue`
- `backtest_weak`
- `recommended_next_evidence_step`

## Classification Rules

Allowed classes:

- `TRUE_REJECTION`
- `VALIDATION_LIMITATION`
- `EVIDENCE_GAP`
- `VOCABULARY_GAP`
- `INTRADAY_REQUIRED`
- `EVENT_REQUIRED`
- `UNKNOWN`

Primary class rules:

- `BACKTEST_WEAK` maps to `TRUE_REJECTION`.
- `EVENT_REACTION` with event-data dependency maps to `EVENT_REQUIRED` unless weak backtest evidence is already the primary rejection.
- `INSUFFICIENT_DATA`, zero usable samples, or sample-size failure maps to `EVIDENCE_GAP`.
- `CHOP` regime without stronger evidence maps to `VOCABULARY_GAP` unless a stronger failure class applies.
- Intraday timeframes or intraday-native mechanisms map to `INTRADAY_REQUIRED` when no stronger primary class applies.
- `UNKNOWN` is used only when unresolved regime/source ambiguity is dominant and no stronger class applies.
- Remaining low-score-only failures map to `VALIDATION_LIMITATION`.

Secondary class rules:

- preserve event, intraday, vocabulary, and evidence-gap issues even when the primary class is true rejection.
- proxy dependence is treated as a universal validation limitation for this population because the source reports show proxy penalty count of 600.

## Summary Counts

### Primary Failure Class

| Primary class | Count |
| --- | ---: |
| `TRUE_REJECTION` | 28 |
| `EVIDENCE_GAP` | 14 |
| `INTRADAY_REQUIRED` | 13 |
| `EVENT_REQUIRED` | 1 |

### Secondary Failure Class

| Secondary class | Count |
| --- | ---: |
| `INTRADAY_REQUIRED` | 42 |
| `VOCABULARY_GAP` | 10 |
| `EVIDENCE_GAP` | 3 |
| `EVENT_REQUIRED` | 1 |

### Fixable Versus True Rejection

| Classification | Count |
| --- | ---: |
| `TRUE_REJECTION_OR_MAJOR_REDESIGN` | 28 |
| `FIXABLE_VALIDATION_LIMITATION` | 28 |

## Attribution Flags

| Flag | True Count | Interpretation |
| --- | ---: | --- |
| `evidence_gap` | 28 | Half the materialized failures still have fixable or retestable evidence gaps. |
| `proxy_dependence` | 56 | All materialized failures inherit the global proxy-dependence penalty. |
| `intraday_required` | 56 | All materialized failed rows carry intraday timeframes or intraday-sensitive mechanisms. |
| `event_metadata_required` | 2 | Two materialized failed rows are event-reaction candidates; one has event requirement as primary, one is primarily weak backtest. |
| `regime_vocab_issue` | 24 | `CHOP` and `UNKNOWN` regimes create vocabulary or unresolved-regime attribution issues. |
| `sample_size_issue` | 15 | Fifteen rows are direct sample-size or insufficient-data failures. |
| `backtest_weak` | 28 | Twenty-eight rows are likely true rejections or require major redesign. |

## Mechanism Distribution

| Mechanism | Count |
| --- | ---: |
| `VWAP_OR_AVERAGE_RECLAIM` | 10 |
| `MEAN_REVERSION` | 8 |
| `OPENING_RANGE` | 8 |
| `TREND_CONTINUATION` | 7 |
| `LIQUIDITY_SWEEP` | 6 |
| `SESSION_TIMING` | 6 |
| `BREAKOUT` | 5 |
| `EVENT_REACTION` | 2 |
| `REVERSAL` | 2 |
| `VOLATILITY_EXPANSION` | 2 |

## Regime Distribution

| Regime | Count |
| --- | ---: |
| `UNKNOWN` | 13 |
| `LOW_VOLATILITY` | 12 |
| `TRENDING` | 11 |
| `CHOP` | 11 |
| `HIGH_VOLATILITY` | 9 |

## Interpretation

The materialized failed-candidate population splits evenly:

- 28 rows are `TRUE_REJECTION_OR_MAJOR_REDESIGN`, driven by weak backtest evidence.
- 28 rows are `FIXABLE_VALIDATION_LIMITATION`, driven by evidence gaps, intraday requirements, event requirements, vocabulary gaps, or unresolved regime/source ambiguity.

This means the failed preview is not simply a bad-candidate list. It is half true rejection and half evidence-readiness debt.

The dominant cross-cutting issue is proxy dependence: all 56 rows inherit the global proxy penalty. The next most important cross-cutting issue is intraday dependence: all 56 materialized failed rows have intraday timeframes or intraday-sensitive mechanisms, so daily proxy evidence remains insufficient for strong qualification conclusions.

`CHOP` and `UNKNOWN` regimes create a separate attribution lane. These should not be treated as automatic candidate failures. They require vocabulary or source-regime clarification before exact regime comparisons are trusted.

## Recommended Evidence Steps

For `TRUE_REJECTION` rows:

- hold, retire, or redesign the hypothesis before retest
- do not spend validation work unless the mechanism thesis changes

For `EVIDENCE_GAP` rows:

- build a direct-data sample attrition ledger
- resolve missing samples or sample-size failure
- re-run analysis-only evidence scoring after sample recovery

For `INTRADAY_REQUIRED` rows:

- acquire matching intraday bars
- use mechanism-specific trigger templates
- compare direct intraday evidence against the daily proxy baseline

For `EVENT_REQUIRED` rows:

- acquire event metadata
- run event-window replay
- preserve daily replay only as context

For `VOCABULARY_GAP` rows:

- resolve `CHOP`, `UNKNOWN`, and related regime labels before exact matching
- if using a bridge, label the result as diagnostic approximation only

## Final Output

Created:

- `research_journal/reports/qualification_failure_attribution_ledger_001.md`
- `research_journal/reports/qualification_failure_attribution_ledger.csv`

Ledger rows:

- 56 materialized failed candidates

No candidate promotion, qualification change, replay change, governance change, trading recommendation, capital allocation, broker execution, position sizing, or paper placement is made.

## Authority Boundary

This ledger is diagnostic only. It does not change candidate state, replay state, qualification state, governance state, paper-forward state, memory state, trading authority, broker authority, capital authority, or production workflow.
