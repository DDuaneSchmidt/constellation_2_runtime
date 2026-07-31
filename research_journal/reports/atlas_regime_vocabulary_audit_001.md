# Atlas Regime Vocabulary Audit 001

Date: 2026-06-05

## Scope

This audit inventories regime-like vocabularies inside Atlas research artifacts and source producers, covering observations, claims, hypotheses, candidates, replay, direct validation, and qualification.

Primary sources reviewed:

- `constellation_2/common/atlas_v2_research_os/bulk_observation_import.py`
- `constellation_2/common/atlas_v2_research_os/methodology_trial.py`
- `constellation_2/common/atlas_v2_research_os/observation_trial.py`
- `constellation_2/common/atlas_v2_research_os/candidate_backtests.py`
- `constellation_2/common/atlas_v2_research_os/direct_candidate_data_validation.py`
- `constellation_2/common/atlas_v2_research_os/backtest_aware_final_qualification.py`
- `constellation_2/common/atlas_v2_research_os/historical_replay_engine.py`
- `constellation_2/common/atlas_v2_research_os/memory_models.py`
- `constellation_2/common/atlas_v2_research_os/mechanism_search_space.py`
- `reports/atlas_v2_research_os/*/latest.json`
- `research_journal/reports/regime_filter_root_cause_001.md`
- `research_journal/reports/replay_filter_sensitivity_study_001.md`

Companion crosswalk: `research_journal/reports/regime_vocabulary_crosswalk.csv`.

## Vocabulary A: Candidate Regimes

Candidate, observation, claim, and hypothesis producers use these market-context labels:

- Core candidate and observation labels: `CHOP`, `TRENDING`, `HIGH_VOLATILITY`, `LOW_VOLATILITY`, `UNKNOWN`.
- Methodology and replay-proxy observation labels: `RANGE_BOUND`, `TRENDING`, `HIGH_VOLATILITY`, `LOW_VOLATILITY`, `UNKNOWN`.
- Memory-regime labels: `TREND`, `CHOP`, `HIGH_VOLATILITY`, `LOW_VOLATILITY`, `OPENING_SESSION`, `MIDDAY_SESSION`, `CLOSING_SESSION`, `NEWS_EVENT`, `LOW_LIQUIDITY`, `HIGH_LIQUIDITY`, `UNKNOWN`.
- Mechanism search-space labels: `RANGE_BOUND`, `LOW_VOLATILITY`, `POST_EVENT_DIGESTION`, `COMPRESSED_VOLATILITY`, `TRENDING`, `CATALYST_PENDING`, `OPENING_SESSION`, `GAP_DAY`, `HIGH_OPENING_VOLUME`, `INTRADAY_TREND`, `PULLBACK`, `LIQUID_SESSION`, `MIDDAY`, `CLOSING_SESSION`, `EVENT_WINDOW`, `RANGE_BREAK`, `REVERSAL_CANDIDATE`, `MOMENTUM`, `RISK_ON`, `EXHAUSTION`, `RANGE_EXTREME`, `MACRO_RELEASE`, `EARNINGS_REACTION`.
- Composite/demo labels: `OPENING_SESSION_HIGH_VOLATILITY`, `MIDDAY_SESSION_CHOP`, `OPENING_SESSION_LOW_VOL`.

Candidate-facing artifacts currently show a heavy use of `CHOP` and `TRENDING`. The candidate validation queue includes `BREAKOUT / CHOP`, `MEAN_REVERSION / TRENDING`, `EVENT_REACTION / CHOP`, and `REVERSAL / TRENDING`.

## Vocabulary B: Validation Regimes

Direct validation has two distinct vocabularies:

- Direct validation classifications: `CONFIRMED`, `WEAKENED`, `INSUFFICIENT_DATA`.
- Candidate backtest classifications: `BACKTEST_SUPPORTED`, `BACKTEST_WEAK`, `INSUFFICIENT_DATA`, `BACKTEST_FAILED`, `SPEC_TOO_AMBIGUOUS`.

The direct validation path delegates sample construction to `candidate_backtests.py`, so its sample-regime classifier emits only:

- `HIGH_VOLATILITY`
- `TRENDING`
- `RANGE_BOUND`
- `LOW_VOLATILITY`
- `UNKNOWN`

It does not emit `CHOP`. Therefore direct validation can fail exact candidate-regime matching even when trigger samples exist.

## Vocabulary C: Replay Regimes

Historical replay has two separate regime-shaped concepts:

- Replay sample regime labels: copied from sample rows, commonly `HIGH_VOLATILITY`, `TRENDING`, `RANGE_BOUND`, `LOW_VOLATILITY`, `UNKNOWN`, plus composite demo labels such as `OPENING_SESSION_HIGH_VOLATILITY` and `MIDDAY_SESSION_CHOP`.
- Replay certification statuses: `INSUFFICIENT_SAMPLE`, `REPLAY_POSITIVE`, `REPLAY_NEUTRAL`, `REPLAY_NEGATIVE`.

Replay certification statuses are not market regimes. They describe replay evidence quality and must not be compared to candidate regimes.

## Vocabulary D: Qualification Regimes

Qualification uses stage and authority vocabulary, not market-regime vocabulary:

- Qualification stage: `FINAL_AFTER_BACKTEST`.
- Qualification threshold: `EDGE_ELIGIBILITY_THRESHOLD`, currently consumed as `0.70` in the final qualification report.
- Qualification eligibility: boolean `eligible`.
- Qualification authority level: `HUMAN_REVIEWED_PAPER_TESTING_CONSIDERATION`.
- Qualification disqualification reasons may embed validation classifications, for example `backtest classification BACKTEST_WEAK` and `backtest classification INSUFFICIENT_DATA`.

Qualification fields should be joined to regime fields through candidate IDs and evidence objects, not through string equality against market-regime labels.

## Overlaps

- `TRENDING`, `HIGH_VOLATILITY`, `LOW_VOLATILITY`, and `UNKNOWN` overlap between candidate/observation labels and validation/replay sample labels.
- `RANGE_BOUND` overlaps between methodology/replay-proxy observations, direct validation samples, and backtest regime-specific performance.
- `CHOP` appears in candidate/observation/memory labels but not in the daily proxy regime classifier.
- `TREND` appears in memory labels and is explicitly mapped to `TRENDING` in `REGIME_PROXY_MAP`.
- `CHOP` is explicitly mapped to `RANGE_BOUND` in `observation_trial.py` and `top_candidate_deep_backtest.py`, but direct validation does not apply that translation in its exact allowed-regime gate.

## Conflicts

- `CHOP` versus `RANGE_BOUND`: candidate artifacts use `CHOP`; daily proxy direct-validation samples use `RANGE_BOUND`. These are plausible near-neighbors, but exact equality fails.
- `TREND` versus `TRENDING`: memory and candidate-adjacent vocabulary can differ by suffix; observation trial maps it, but not every consumer does.
- `MIDDAY` versus `MIDDAY_SESSION`: mechanism search-space vocabulary and memory vocabulary differ.
- `LOW_VOL` and `HIGH_VOL` aliases exist only in `REGIME_PROXY_MAP`; source artifacts usually use `LOW_VOLATILITY` and `HIGH_VOLATILITY`.
- `risk_on` appears in morning-review regime-shaped fields as lowercase, while mechanism search-space uses uppercase `RISK_ON`.
- `YES` appears in generated `regime`/validation-shaped paths and is not a regime.
- `CHOP, TRENDING` appears as a comma-joined string in winner-pattern extraction, which is not comparable to scalar regime labels without parsing.

## Missing Translations

- Direct validation lacks a declared compatibility table for candidate `CHOP` to replay-proxy `RANGE_BOUND`.
- Direct validation lacks a declared compatibility table for `TREND` to `TRENDING`, `LOW_VOL` to `LOW_VOLATILITY`, and `HIGH_VOL` to `HIGH_VOLATILITY`, even though observation trial has this map.
- Composite labels such as `OPENING_SESSION_HIGH_VOLATILITY` and `MIDDAY_SESSION_CHOP` have no normalized decomposition into session plus market state.
- Mechanism search-space labels such as `POST_EVENT_DIGESTION`, `CATALYST_PENDING`, `GAP_DAY`, `PULLBACK`, `LIQUID_SESSION`, `RANGE_BREAK`, `REVERSAL_CANDIDATE`, `MOMENTUM`, `RISK_ON`, `EXHAUSTION`, `RANGE_EXTREME`, `MACRO_RELEASE`, and `EARNINGS_REACTION` do not map cleanly to the five daily proxy regimes.
- Memory labels `NEWS_EVENT`, `LOW_LIQUIDITY`, and `HIGH_LIQUIDITY` lack direct daily-proxy translations.

## Impossible Comparisons

These comparisons should be treated as invalid without an explicit translator:

- Candidate market regime versus replay certification status: `CHOP` is not comparable to `REPLAY_POSITIVE`.
- Candidate market regime versus direct validation classification: `TRENDING` is not comparable to `CONFIRMED` or `INSUFFICIENT_DATA`.
- Candidate market regime versus backtest classification: `HIGH_VOLATILITY` is not comparable to `BACKTEST_SUPPORTED`.
- Candidate market regime versus qualification stage: `CHOP` is not comparable to `FINAL_AFTER_BACKTEST`.
- Candidate market regime versus qualification authority level: `TRENDING` is not comparable to `HUMAN_REVIEWED_PAPER_TESTING_CONSIDERATION`.
- Candidate session/event labels versus daily volatility/trend labels: `OPENING_SESSION`, `EVENT_WINDOW`, and `MACRO_RELEASE` are dimensions, not direct substitutes for `HIGH_VOLATILITY` or `TRENDING`.

## Direct Validation Finding

The existing root-cause report found five candidates with trigger samples greater than zero and final replay samples equal to zero. Their candidate-side `allowed_regimes=[CHOP]` could not pass the daily proxy replay classifier because that classifier emitted only `HIGH_VOLATILITY`, `TRENDING`, `RANGE_BOUND`, `LOW_VOLATILITY`, and `UNKNOWN`.

This is a semantic mismatch, not evidence of candidate failure. The event-reaction case also has a temporal mismatch because it requires intraday plus daily confirmation; daily proxy replay cannot certify intraday event-regime evidence.

## Recommendations

1. Treat `regime_vocabulary_crosswalk.csv` as a diagnostic crosswalk, not an execution policy.
2. Add a single explicit Atlas regime-translation contract before changing any validation behavior.
3. Keep market-regime labels, replay statuses, validation classifications, and qualification states in separate namespaces.
4. Require composite labels to be decomposed before comparison, for example `OPENING_SESSION_HIGH_VOLATILITY` into `session=OPENING_SESSION` and `volatility=HIGH_VOLATILITY`.
5. Fail closed when a consumer tries to compare values across namespaces without a declared translator.

## Authority Boundary

This report is diagnostic only. It does not change replay behavior, validation behavior, candidate state, qualification state, governance, paper-forward state, promotion, trade recommendations, live trading, broker execution, capital allocation, position sizing, or automatic paper placement.
