# CHOP Semantics Audit 001

Date: 2026-06-05
Status: ANALYSIS_ONLY

## Scope

This audit determines what `CHOP` means inside Atlas by reviewing observations, claims, hypotheses, candidates, replay reports, validation reports, and relevant producer code.

Target terms reviewed:

- `CHOP`
- `RANGE_BOUND`
- `LOW_VOLATILITY`
- `UNKNOWN`

Primary sources reviewed:

- `constellation_2/common/atlas_v2_research_os/bulk_observation_import.py`
- `constellation_2/common/atlas_v2_research_os/observation_to_claim.py`
- `constellation_2/common/atlas_v2_research_os/observation_trial.py`
- `constellation_2/common/atlas_v2_research_os/methodology_trial.py`
- `constellation_2/common/atlas_v2_research_os/candidate_backtests.py`
- `constellation_2/common/atlas_v2_research_os/direct_candidate_data_validation.py`
- `reports/atlas_v2_research_os/*/latest.json`
- `reports/atlas_v2_research_os/*/2026-06-05/*.json`
- `research_journal/reports/regime_filter_root_cause_001.md`
- `research_journal/reports/regime_filter_root_cause_table.csv`
- `research_journal/reports/direct_validation_feasibility_study_001.md`

Companion CSV: `research_journal/reports/chop_semantics_crosswalk.csv`.

## Observed Usage

### Observations

Observation import defines `REGIMES = ["TRENDING", "CHOP", "HIGH_VOLATILITY", "LOW_VOLATILITY", "UNKNOWN"]`. Generated observations place `CHOP` directly in the `regime` field and in prose like `CHOP context` or `in CHOP regime sample`.

The observation generator does not define `CHOP` mathematically. It uses `CHOP` as a categorical market-context bucket alongside trend and volatility labels.

### Claims

Observation clusters convert into claim text such as:

`BREAKOUT observations may show repeatable breakout behavior in CHOP regimes...`

The claim generator preserves the regime label from the observation cluster. It does not add a definition beyond the inherited market-context bucket.

### Hypotheses

Observation-trial hypotheses introduce a `REGIME_PROXY_MAP`:

- `CHOP -> RANGE_BOUND`
- `RANGE -> RANGE_BOUND`
- `RANGE_BOUND -> RANGE_BOUND`
- `LOW_VOL -> LOW_VOLATILITY`
- `LOW_VOLATILITY -> LOW_VOLATILITY`
- `UNKNOWN -> UNKNOWN`

This is the strongest evidence that Atlas treats `CHOP` as closest to a range/sideways state when converting observation labels into proxy replay labels. It is still a proxy map, not a universal semantic contract.

### Candidates

Candidate-facing artifacts use `CHOP` in candidate rows, symbol attribution, validation plans, focused campaigns, final rankings, approval checklists, and family names such as:

- `BREAKOUT / CHOP / 30M`
- `BREAKOUT / CHOP / 5M`
- `EVENT_REACTION / CHOP / 1H`

The candidate queue contains multiple `BREAKOUT / CHOP` candidates and one `EVENT_REACTION / CHOP` candidate. In these artifacts, `CHOP` is a candidate regime constraint, not a replay result.

### Replay Reports

Replay and methodology reports commonly use `RANGE_BOUND`, `LOW_VOLATILITY`, and `UNKNOWN`. Methodology trial defines its regimes as:

`["UNKNOWN", "TRENDING", "RANGE_BOUND", "HIGH_VOLATILITY", "LOW_VOLATILITY"]`

It does not use `CHOP`. Historical replay copies sample regime labels and can carry either generated labels or proxy labels depending on the producer.

### Validation Reports

Direct validation delegates sample construction to `candidate_backtests.py`. The daily proxy classifier emits only:

- `HIGH_VOLATILITY`
- `TRENDING`
- `RANGE_BOUND`
- `LOW_VOLATILITY`
- `UNKNOWN`

It does not emit `CHOP`. Direct validation then uses exact matching:

`triggered_sample.regime in allowed_regimes`

For candidates with `allowed_regimes=[CHOP]`, exact matching rejects all daily proxy samples because no triggered sample can have `regime=CHOP`.

## What CHOP Means

### Volatility State

Classification: no.

`CHOP` is not a pure volatility state inside Atlas. `LOW_VOLATILITY` and `HIGH_VOLATILITY` exist as separate labels. If `CHOP` meant volatility, there would be no reason to keep it distinct from `LOW_VOLATILITY` in the observation generator, claim text, candidate rows, and final rankings.

### Sideways Market State

Classification: yes, inferred.

Atlas does not state this in prose, but `CHOP -> RANGE_BOUND` in `REGIME_PROXY_MAP` is a direct implementation hint. Candidate usage also pairs `CHOP` with mechanisms like `BREAKOUT`, which makes the most sense as a sideways or non-trending context from which breakouts are observed.

### Low Directional State

Classification: yes, partial.

`CHOP` appears to imply low directional clarity or non-trending behavior. It is not equivalent to `LOW_VOLATILITY`: a market can be choppy with meaningful volatility, and Atlas keeps the labels separate.

### Range State

Classification: yes, partial.

`RANGE_BOUND` is the closest explicit proxy. The proxy map makes this intentional in observation-trial conversion, and the direct-validation root cause shows that `RANGE_BOUND` is the daily replay label that can survive where `CHOP` cannot.

### Combination Of Multiple States

Classification: yes, recommended interpretation.

Inside Atlas, `CHOP` is best interpreted as a composite candidate-side regime: sideways/range-bound and low-directional, without necessarily being low-volatility. It is not decomposed into dimensions, so consumers should not silently compare it to a single daily replay label without a declared bridge.

## Comparisons

No reviewed Atlas source supports `EQUIVALENT` for `CHOP` against `RANGE_BOUND`, `LOW_VOLATILITY`, or `UNKNOWN`.

### CHOP vs RANGE_BOUND

Classification: `PARTIAL_EQUIVALENT`.

Evidence:

- `REGIME_PROXY_MAP` explicitly maps `CHOP` to `RANGE_BOUND`.
- Daily proxy replay emits `RANGE_BOUND`, not `CHOP`.
- Root-cause diagnostics show `RANGE_BOUND_as_CHOP_proxy` sample counts for CHOP candidates.

Reason not equivalent:

- `CHOP` is candidate/observation-side and underdefined.
- `RANGE_BOUND` is a daily proxy classifier output using a price-distance rule around SMA50.
- `CHOP` may imply low directional quality or noisy/sideways behavior beyond the daily range rule.

### CHOP vs LOW_VOLATILITY

Classification: `NOT_EQUIVALENT`.

Evidence:

- Observation import has both `CHOP` and `LOW_VOLATILITY` as separate regimes.
- Methodology and validation classifiers emit `LOW_VOLATILITY` separately from `RANGE_BOUND`.
- Root-cause diagnostics show `LOW_VOLATILITY_as_CHOP_proxy` as a separate sensitivity bucket, often with zero or minimal samples.

Reason:

`LOW_VOLATILITY` is a volatility magnitude state. `CHOP` is a direction/range/context state. They can overlap in the market, but Atlas does not define them as substitutes.

### CHOP vs UNKNOWN

Classification: `NOT_EQUIVALENT`.

Evidence:

- Observation import has both `CHOP` and `UNKNOWN`.
- Claims preserve `UNKNOWN regimes` when regime is unknown.
- Backtest logic treats `UNKNOWN` specially: if primary or allowed regimes are `UNKNOWN`, regime filtering can become permissive.

Reason:

`UNKNOWN` means unresolved or unspecified regime. `CHOP` is a positive regime assertion. Using `UNKNOWN` as a CHOP approximation would destroy semantic information.

## Approximation Decision

Can `CHOP` be safely approximated by:

- `RANGE_BOUND`: yes, for explicitly labeled diagnostic or proxy replay approximation only.
- `LOW_VOLATILITY`: no.
- Both `RANGE_BOUND` and `LOW_VOLATILITY`: no as a default, because it mixes directional/range and volatility dimensions.
- Neither: yes for exact validation, because current Atlas has no governed global semantic bridge.

Operationally: the safest approximation choice is `RANGE_BOUND` only, and only when the result is labeled as an approximation. For validation authority, the safe choice is `NEITHER` until Atlas declares a governed translation contract.

## Recommended Semantic Interpretation

Recommended interpretation:

`CHOP` is a candidate-side sideways/range/low-directional market state. It is not a volatility state. It can overlap with low volatility, but it should not be reduced to `LOW_VOLATILITY`. Its closest current replay proxy is `RANGE_BOUND`, but only as a partial equivalent.

Recommended comparison rule:

- Treat `CHOP == RANGE_BOUND` as false.
- Treat `CHOP ~= RANGE_BOUND` as true only under an explicit approximation namespace.
- Treat `CHOP ~= LOW_VOLATILITY` as false by default.
- Treat `CHOP ~= UNKNOWN` as false.

## Authority Boundary

This audit is diagnostic only. It does not implement mappings, relax filters, validate candidates, change replay logic, alter qualification, promote candidates, create paper observations, recommend trades, allocate capital, authorize broker execution, size positions, or alter runtime governance.
