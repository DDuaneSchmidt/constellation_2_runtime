# Replay Limitation Ledger 001

Date: 2026-06-05

Status: analysis-only ledger

Companion CSV: `research_journal/reports/replay_limitation_ledger.csv`

## Scope

Create a structured ledger for materialized replay-limited candidates found across direct validation, zero-sample root-cause analysis, qualification attribution, and sample-size recheck artifacts.

Inputs used:

- `research_journal/reports/candidate_replay_trace_table.csv`
- `research_journal/reports/regime_filter_root_cause_table.csv`
- `research_journal/reports/zero_sample_root_cause_table.csv`
- `research_journal/reports/insufficient_data_root_cause_table.csv`
- `research_journal/reports/sample_size_recheck_table.csv`
- `research_journal/reports/fixable_validation_limitations_table.csv`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/replay_sample_yield/latest.json`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest.json`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`

This ledger records replay limitations only. It does not rerun replay, change filters, change qualification, promote candidates, change governance, or authorize trading/paper/capital activity.

## Authority Boundary

No candidate promotion is made.
No replay override is made.
No qualification change is made.
No governance change is made.
No paper-forward state change is made.
No trade recommendation, broker execution, capital allocation, portfolio construction, position sizing, or automatic paper placement is authorized.

## Ledger Coverage

Rows in ledger: 21

| Classification | Count | Meaning |
| --- | ---: | --- |
| `REGIME_FILTER` | 4 | Trigger samples exist, but exact regime/filter logic removes all final replay samples. |
| `TIMEFRAME_MISMATCH` | 0 | Timeframe mismatch is the dominant replay limitation. |
| `PROXY_MISMATCH` | 2 | Direct coverage/proxy substitution prevents a clean replay readout. |
| `EVENT_METADATA` | 1 | Event-reaction replay requires event metadata and event-window evidence. |
| `LOW_SAMPLE_DENSITY` | 3 | Candidate has nonzero samples but remains below the minimum sample threshold. |
| `REPLAY_LOGIC` | 11 | Candidate has zero usable samples or missing trigger/filter attribution under the current replay path. |
| `UNKNOWN` | 0 | No materialized row required this classification. |

Timeframe mismatch appears as a cross-cutting factor in many rows, but no row is classified with `TIMEFRAME_MISMATCH` as primary after precedence. Rows with all triggered samples removed are classified as `REGIME_FILTER`; event-reaction rows are classified as `EVENT_METADATA`; zero-sample qualification-preview rows without materialized trigger/filter attribution are classified as `REPLAY_LOGIC`.

## Candidate Groups

### REGIME_FILTER

Candidates: `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_7d839944a8a4070a`, `ptc_backtest_final_b23c6756bfb3263a`

Observed pattern:

- trigger samples exist;
- candidate regime constraints remove all post-filter replay samples;
- required sample count remains 30;
- minimum remediation is stage-level attrition telemetry and regime-label diagnostics, not production filter relaxation.

Estimated validation gain: high. These candidates can become evaluable, or cleanly non-evaluable, if the replay pipeline identifies whether regime semantics, filter strictness, or daily proxy logic is the fatal stage.

### EVENT_METADATA

Candidates: `ptc_backtest_final_d5931b24bd391113`

Observed pattern:

- event-reaction candidate is represented by generic daily-bar proxy logic;
- event calendar/news/earnings metadata is absent;
- all current post-filter samples are zero.

Estimated validation gain: high for the candidate, medium at population level. Event metadata is a hard prerequisite for event-reaction replay.

### PROXY_MISMATCH

Candidates: `ptc_backtest_final_4df2e8e80685a054`, `ptc_backtest_final_624fdd85668e2c08`

Observed pattern:

- replay is blocked or incomplete because direct symbol coverage/proxy substitution prevents a complete candidate-level replay readout;
- one candidate has partial supportive samples but incomplete universe coverage;
- another has no direct replay due missing local direct data in the source artifact.

Estimated validation gain: high. These candidates are not proven weak by replay; the current limitation is evidence-source completeness.

### LOW_SAMPLE_DENSITY

Candidates: `ptc_backtest_final_0a75270e4aced70b`, `ptc_backtest_final_133dce459bccef18`, `ptc_backtest_final_167bf746a7e16482`

Observed pattern:

- candidates have nonzero samples but fewer than the required 30;
- two candidates have 26 of 30 samples and are near evaluability;
- one candidate has 2 of 30 samples and needs materially more direct evidence.

Estimated validation gain: medium to high. The two 26-sample rows are the clearest near-term candidates to move from insufficient-data to evaluable if additional post-filter samples are recovered.

### REPLAY_LOGIC

Candidates: `ptc_backtest_final_00ddd14158f5530b`, `ptc_backtest_final_02630f234d5edbab`, `ptc_backtest_final_05e4c8563adf27d6`, `ptc_backtest_final_0df2fa30ffcbc127`, `ptc_backtest_final_0e47e54d68df23b2`, `ptc_backtest_final_0ea60d559ba9d153`, `ptc_backtest_final_11943bdfd5c2bfee`, `ptc_backtest_final_14143f615c830af7`, `ptc_backtest_final_152adfbd23003578`, `ptc_backtest_final_158689d9e7ef68a5`, `ptc_backtest_final_199c01b1a6e78de1`

Observed pattern:

- final ranking reports zero usable samples;
- detailed candidate-level trigger/filter coverage is not materialized;
- current daily/proxy replay path does not explain why no post-filter samples survive;
- mechanism-specific trigger audits are required before requesting more data or interpreting failure.

Estimated validation gain: medium. The first gain is diagnostic clarity, not immediate candidate evaluability.

### TIMEFRAME_MISMATCH

Candidates: None

No candidate is assigned this as the primary class in the final ledger. Timeframe mismatch remains embedded in many failure reasons because intraday candidates are being assessed through daily/proxy replay, but stronger primary blockers are present in the materialized rows.

### UNKNOWN

Candidates: None

No candidate required `UNKNOWN` classification.

## Required Sample Count

The ledger uses 30 as the required sample count where candidate-specific minimums were not materialized. This matches the sample-size failure language in the qualification sample-trial artifacts: `minimum sample size 30 not met after trigger/regime filtering`.

## Minimum Remediation Themes

1. Add stage-level replay attrition telemetry for zero-sample candidates.
2. Preserve exact baseline replay results before any diagnostic bridge or sensitivity study.
3. Add regime-filter diagnostics for `CHOP` candidates zeroed by exact regime matching.
4. Add event metadata and event-window replay diagnostics for event-reaction candidates.
5. Complete direct symbol coverage where replay is blocked by proxy/no direct data.
6. Add mechanism-specific intraday trigger audits for zero-sample qualification-preview candidates.
7. Treat sample-density recovery as evaluability work, not candidate promotion.

## Output Files

Created:

- `research_journal/reports/replay_limitation_ledger_001.md`
- `research_journal/reports/replay_limitation_ledger.csv`

The CSV contains one row per replay-limited candidate with:

- candidate_id
- timeframe
- symbols
- replay stage
- sample count
- required sample count
- failure reason
- minimum remediation
- estimated validation gain
- classification
