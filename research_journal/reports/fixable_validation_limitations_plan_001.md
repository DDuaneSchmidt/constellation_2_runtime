# Fixable Validation Limitations Plan 001

Date: 2026-06-05

Status: analysis-only remediation plan

Companion table: `research_journal/reports/fixable_validation_limitations_table.csv`

## Scope

Resolve, at the planning level, the 28 fixable validation/evidence limitations from the qualification failure attribution ledger.

Inputs reviewed:

- `research_journal/reports/qualification_failure_attribution_ledger_001.md`
- `research_journal/reports/qualification_failure_attribution_ledger.csv`
- `research_journal/reports/proxy_dependence_reduction_plan_001.md`
- `research_journal/reports/candidate_quality_scoreboard_002.md`

This plan does not implement remediation. It identifies the minimum evidence needed to turn each fixable limitation into a cleaner validation readout.

## Authority Boundary

No candidate promotion is made.
No replay override is made.
No qualification change is made.
No governance change is made.
No trading recommendation, broker execution, capital allocation, position sizing, portfolio construction, automatic paper placement, or production integration is authorized.

All items remain evidence-readiness or diagnostic work only.

## Source Population

The attribution ledger contains 56 materialized failed candidates:

| Classification | Count |
| --- | ---: |
| `TRUE_REJECTION_OR_MAJOR_REDESIGN` | 28 |
| `FIXABLE_VALIDATION_LIMITATION` | 28 |

This plan includes only the 28 `FIXABLE_VALIDATION_LIMITATION` rows. True-rejection rows are intentionally excluded.

## Remediation Class Summary

| Remediation Class | Count | Primary Meaning |
| --- | ---: | --- |
| `SAMPLE_SIZE_RECHECK` | 14 | Candidate needs direct sample attrition and missing-evidence accounting before any stronger readout. |
| `VOCABULARY_BRIDGE_DIAGNOSTIC` | 10 | Candidate needs regime vocabulary/source-regime clarification before exact validation interpretation. |
| `INTRADAY_REQUIRED` | 3 | Candidate needs matching intraday bars and mechanism-specific trigger evidence. |
| `EVENT_REQUIRED` | 1 | Candidate needs event metadata and event-window evidence. |
| `DAILY_REVALIDATION` | 0 | No candidate in this set is cleanly resolvable by daily-only revalidation alone. |
| `REPORTING_LINEAGE_CLEANUP` | 0 | No candidate in this set is only a reporting-lineage issue. |

Priority summary:

| Priority | Count | Interpretation |
| --- | ---: | --- |
| P1 | 15 | First lane: concrete missing evidence, sample recovery, or event metadata. |
| P2 | 13 | Second lane: diagnostic bridge or intraday evidence setup. |
| P3 | 0 | Cleanup-only lane; unused in this fixable set. |

Complexity summary:

| Complexity | Count |
| --- | ---: |
| LOW | 10 |
| MEDIUM | 14 |
| HIGH | 4 |

Authority-risk summary:

| Authority Risk | Count | Boundary |
| --- | ---: | --- |
| LOW | 14 | Measurement-only or sample accounting if no thresholds change. |
| MEDIUM | 14 | Bridge, intraday, or event evidence can be misread as validation authority and must remain diagnostic. |
| HIGH | 0 | None in this plan. |

## Candidate Lanes

### P1: SAMPLE_SIZE_RECHECK

Candidates: `ptc_backtest_final_00ddd14158f5530b`, `ptc_backtest_final_02630f234d5edbab`, `ptc_backtest_final_05e4c8563adf27d6`, `ptc_backtest_final_0a75270e4aced70b`, `ptc_backtest_final_0df2fa30ffcbc127`, `ptc_backtest_final_0e47e54d68df23b2`, `ptc_backtest_final_0ea60d559ba9d153`, `ptc_backtest_final_11943bdfd5c2bfee`, `ptc_backtest_final_133dce459bccef18`, `ptc_backtest_final_14143f615c830af7`, `ptc_backtest_final_152adfbd23003578`, `ptc_backtest_final_158689d9e7ef68a5`, `ptc_backtest_final_167bf746a7e16482`, `ptc_backtest_final_199c01b1a6e78de1`

Evidence gap:

- insufficient data or sample-size issue;
- universal proxy dependence still applies;
- intraday-required secondary issue is present in these rows.

Required evidence:

- direct candidate sample attrition counts;
- trigger sample count;
- post-filter sample count;
- missing evidence cause;
- minimum usable sample threshold check.

Minimum remediation:

Build a read-only sample attrition ledger for each candidate and recheck whether direct data can recover enough samples. This is a sample accounting and evidence-readiness step only; it must not override replay, qualification, or candidate status.

Expected validation gain:

Medium-high. These candidates can move from ambiguous insufficient-data failure to either evaluable direct evidence or clean non-evaluable status.

### P1: EVENT_REQUIRED

Candidates: `ptc_backtest_final_1625b4258b327662`

Evidence gap:

- event metadata required;
- intraday/event-window evidence required;
- sample-size issue also present;
- proxy dependence still applies.

Required evidence:

- timestamped event metadata;
- event type;
- event window;
- expected versus actual catalyst context;
- direct symbol event-window samples.

Minimum remediation:

Attach event metadata lineage and run an analysis-only event-window evidence review. Preserve the current qualification failure and do not treat event evidence as paper-forward, trading, or qualification authority.

Expected validation gain:

High. Event metadata can separate real event reaction from ordinary volatility, proxy drift, or unrelated market movement.

### P2: VOCABULARY_BRIDGE_DIAGNOSTIC

Candidates: `ptc_backtest_final_009acc9a8738e8f0`, `ptc_backtest_final_0201292a9823e314`, `ptc_backtest_final_040dea316c6d1917`, `ptc_backtest_final_045abee318c2560a`, `ptc_backtest_final_04a687b081d95eae`, `ptc_backtest_final_0ee5a48c813c158d`, `ptc_backtest_final_111ba2c238976aea`, `ptc_backtest_final_132bed719ce20d48`, `ptc_backtest_final_17d8e91072182cdf`, `ptc_backtest_final_1996c1c3f3a598e8`

Evidence gap:

- `UNKNOWN` or vocabulary-sensitive regime attribution;
- proxy dependence still applies;
- intraday-required flag remains true.

Required evidence:

- candidate regime label;
- validator emitted regimes;
- possible mapping and confidence;
- ambiguity note;
- side-by-side exact-regime and bridged diagnostic sample counts.

Minimum remediation:

Run a diagnostic-only regime vocabulary crosswalk for each candidate. Any bridge result must be labeled approximation evidence, not direct validation truth and not qualification evidence.

Expected validation gain:

Medium. The work can identify whether regime semantics caused attrition, but most candidates in this lane still need intraday evidence before stronger validation.

### P2: INTRADAY_REQUIRED

Candidates: `ptc_backtest_final_02881049f0191b18`, `ptc_backtest_final_0d062d53e36215bc`, `ptc_backtest_final_17a594323c3c52e0`

Evidence gap:

- daily proxy evidence cannot validate the candidate's intraday-native mechanism;
- sample issue is not the primary flag for these rows;
- proxy dependence still applies.

Required evidence:

- matching intraday bars for candidate symbols and timeframes;
- mechanism-specific trigger evidence;
- direct intraday sample counts;
- comparison against daily proxy baseline.

Minimum remediation:

Acquire or attach matching intraday evidence in analysis-only form and compare against the daily proxy baseline. Do not change replay behavior or qualification status.

Expected validation gain:

High. Intraday evidence directly tests whether daily proxy failure is masking or overstating the mechanism.

## Why DAILY_REVALIDATION Is Not Sufficient

No candidate in the 28-row fixable set is classified as `DAILY_REVALIDATION` because every row carries intraday-sensitive evidence requirements, sample-size issues, vocabulary/source-regime issues, or event metadata requirements. Daily data can remain useful context, but it is not the minimum complete remediation for this fixable set.

## Why REPORTING_LINEAGE_CLEANUP Is Not Sufficient

Reporting and proxy-label cleanup are still needed across the broader proxy-dependence problem, but none of these 28 candidates is only a stale-reporting issue. Each requires at least sample recheck, vocabulary diagnostic, intraday evidence, or event evidence.

## Expected Validation Gain By Class

| Class | Expected Gain | Rationale |
| --- | --- | --- |
| `SAMPLE_SIZE_RECHECK` | Medium-high | Can distinguish recoverable sample attrition from genuine non-evaluable candidates. |
| `EVENT_REQUIRED` | High | Event metadata is a hard prerequisite for event-reaction validation. |
| `VOCABULARY_BRIDGE_DIAGNOSTIC` | Medium | Can identify semantic attrition, but bridge output remains approximate. |
| `INTRADAY_REQUIRED` | High | Directly addresses daily proxy limitations for intraday mechanisms. |

## Execution Boundary For Later Work

If implementation is requested later, each remediation should remain read-only until explicitly authorized:

- produce new evidence artifacts;
- preserve current qualification failure labels;
- preserve current replay outputs;
- keep bridge-derived outputs separate from exact validation;
- keep intraday and event outputs evidence-only;
- do not promote, qualify, paper-place, trade, allocate, or size anything.

## Final Output

Created:

- `research_journal/reports/fixable_validation_limitations_plan_001.md`
- `research_journal/reports/fixable_validation_limitations_table.csv`

Rows in companion table: 28

The table includes one row per fixable candidate with candidate id, failure class, evidence gap, required evidence, minimum remediation, expected validation gain, complexity, authority risk, and priority.
