# Sample Size Recheck Program 001

Date: 2026-06-05

Status: analysis-only

Companion table: `research_journal/reports/sample_size_recheck_table.csv`

## Scope

Investigate the 14 candidates classified as `SAMPLE_SIZE_RECHECK` in `research_journal/reports/fixable_validation_limitations_table.csv`.

This program does not run replay, override replay, change qualification, change candidate state, promote candidates, alter governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.

## Inputs Reviewed

- `research_journal/reports/fixable_validation_limitations_table.csv`
- `research_journal/reports/qualification_failure_attribution_ledger.csv`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest.json`
- `reports/atlas_v2_research_os/replay_sample_yield/latest.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/candidate_data_validation_plan/latest.json`

## Measurement Basis

The 14 candidates are materialized `REJECT_FOR_NOW` rows from final ranking. All 14 have:

- final score below 0.7;
- backtest classification `INSUFFICIENT_DATA`;
- proxy dependence;
- intraday-required evidence flags;
- sample-size issue in the attribution ledger.

Minimum required sample count is 30, based on the backtest-aware qualification sample-trial evidence that reports: `minimum sample size 30 not met after trigger/regime filtering`.

Only two of the 14 candidates have detailed sample-trial replay coverage rows in the qualification artifact. For the other 12, the report uses final-ranking sample counts and marks missing trigger/filter coverage explicitly.

## Classification Summary

| Classification | Count | Meaning |
| --- | ---: | --- |
| `READY_FOR_RECHECK` | 2 | Candidate is near the 30-sample threshold and has detailed coverage showing only a small sample gap. |
| `MORE_DATA_REQUIRED` | 1 | Candidate has nonzero samples but remains far below the minimum sample threshold. |
| `REPLAY_LIMITATION` | 11 | Candidate currently has zero usable samples, so sample attrition/replay coverage must be explained before more data can be scored. |
| `UNKNOWN` | 0 | Not used; every candidate had at least a final-ranking sample count. |

## Candidate Sets

### READY_FOR_RECHECK

Candidates: `ptc_backtest_final_0a75270e4aced70b`, `ptc_backtest_final_167bf746a7e16482`

These two candidates each have 26 current post-filter samples and need 4 additional samples to reach the 30-sample minimum. Both also have detailed sample-trial coverage:

- trigger_count: 811
- regime_filtered_count: 785
- post-filter sample_size: 26
- historical replay status: `REPLAY_POSITIVE`
- historical replay sample size: 12

Interpretation: these are the only candidates in the batch that can plausibly move from insufficient-data to evaluable with a narrow sample recovery.

### MORE_DATA_REQUIRED

Candidates: `ptc_backtest_final_133dce459bccef18`

This candidate has 2 current samples and needs 28 additional samples. It is not ready for immediate recheck; the evidence source needs substantially more direct intraday coverage and a sample attrition ledger.

### REPLAY_LIMITATION

Candidates: `ptc_backtest_final_00ddd14158f5530b`, `ptc_backtest_final_02630f234d5edbab`, `ptc_backtest_final_05e4c8563adf27d6`, `ptc_backtest_final_0df2fa30ffcbc127`, `ptc_backtest_final_0e47e54d68df23b2`, `ptc_backtest_final_0ea60d559ba9d153`, `ptc_backtest_final_11943bdfd5c2bfee`, `ptc_backtest_final_14143f615c830af7`, `ptc_backtest_final_152adfbd23003578`, `ptc_backtest_final_158689d9e7ef68a5`, `ptc_backtest_final_199c01b1a6e78de1`

These 11 candidates have 0 current usable samples in final ranking. They should not be treated as merely four or five samples short. The immediate issue is replay/sample attribution: the current evidence path does not explain enough candidate-specific post-filter samples to support an evaluable recheck.

## Expected Movement To Evaluable

Expected number of candidates that could move from insufficient-data to evaluable: 2.

Basis:

- `ptc_backtest_final_0a75270e4aced70b` has 26/30 samples and detailed coverage.
- `ptc_backtest_final_167bf746a7e16482` has 26/30 samples and detailed coverage.
- Both require only 4 additional post-filter samples.

Possible but not expected in the first recheck: 1.

- `ptc_backtest_final_133dce459bccef18` has 2/30 samples and would need 28 additional samples.

Not expected to become evaluable without replay limitation diagnosis: 11.

- These candidates have 0/30 samples and need mechanism-specific trigger/sample attribution before confidence can improve.

## Recommended Evidence Sources

For `READY_FOR_RECHECK`:

- direct intraday bars matching candidate symbols and timeframes;
- extended lookback or direct universe coverage sufficient to recover 4 additional post-filter samples;
- preserve current `INSUFFICIENT_DATA` status until evidence is actually rechecked.

For `MORE_DATA_REQUIRED`:

- direct 1h intraday bars for the candidate symbols;
- broader lookback coverage;
- sample attrition ledger showing raw triggers, regime-filter survivors, final post-filter samples, and why only 2 samples currently survive.

For `REPLAY_LIMITATION`:

- mechanism-specific trigger audit on direct intraday bars;
- zero-sample root-cause ledger;
- separate data absence from filter attrition, regime mismatch, and generic daily-proxy trigger failure.

## Program Table

The companion CSV includes one row per candidate with:

- candidate_id
- current sample count
- minimum required sample count
- missing sample count
- replay coverage
- validation status
- estimated confidence improvement
- recommended evidence source
- classification

## Authority Boundary

This report is diagnostic only.

No candidate promotion is made.
No replay override is made.
No qualification change is made.
No governance change is made.
No production integration is made.
No trade recommendation, broker execution, capital allocation, position sizing, portfolio construction, or automatic paper placement is authorized.
