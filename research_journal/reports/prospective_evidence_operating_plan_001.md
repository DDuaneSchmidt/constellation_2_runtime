# Prospective Evidence Operating Plan 001

Date: 2026-06-05

Status: `MEASUREMENT_PLAN_ONLY`

## Purpose

Create a 30-day prospective measurement plan for Atlas evidence collection across Research Adversary review quality, Research Director shadow predictions, candidate validation, vocabulary bridge diagnostics, intraday/event validation, and paper-forward outcomes.

This plan is operating guidance for measurement only. It does not implement software, change workflows, change prioritization, modify candidates, modify replay, modify validation, modify qualification, change governance, write memory automatically, authorize paper-forward actions, recommend trades, allocate capital, authorize broker execution, construct portfolios, or size positions.

## Inputs Reviewed

- `research_journal/reports/research_adversary_prospective_pilot_001.md`
- `research_journal/reports/research_adversary_human_review_instructions_001.md`
- `research_journal/reports/research_director_shadow_cycle_002.md`
- `research_journal/reports/candidate_quality_decision_gate_001.md`
- `research_journal/reports/candidate_survival_scorecard_001.md`
- `research_journal/reports/candidate_quality_scoreboard_002.md`

## Current Baseline

| Area | Current baseline | Measurement implication |
| --- | ---: | --- |
| Research Adversary prospective reviews | 0 live pilot items scored | Needs human-scored prospective cases before expansion claims. |
| Research Adversary human review batch | 20 selected rows pending human score | Scores must not be inferred or fabricated. |
| Research Director shadow mode | Cycle 002 recorded as prospective prediction ledger | Score only against future evidence created after the cycle. |
| Focused direct-validation candidates | 8 | Small but usable current validation cohort. |
| Focused confirmed candidates | 2 | Positive signal, not broad quality proof. |
| Focused insufficient-data candidates | 6 | Main focused validation blocker. |
| Qualification-scale proxy dependence | 600 of 600 | Universal validation-fidelity limitation. |
| Replay-gap candidates | 372 of 600 | Major candidate-quality bottleneck. |
| Paper-forward-ready candidates | 58 | Observation readiness, not outcome proof. |
| Linked paper-forward outcomes | 2 | Too sparse for survival-quality conclusions. |

## 30-Day Window

Start date: 2026-06-05

End date: 2026-07-05

Primary unit of measurement: each new research cycle or evidence artifact created during the 30-day window.

Secondary stop condition: if Research Adversary reaches 50 new reviewed observations, claims, or hypotheses before 2026-07-05, score that lane at the 50-item point while continuing to record the other lanes through day 30.

## Measurement Lanes

| Lane | Prospective records | Primary metrics | Scoring cadence | Review responsibility |
| --- | --- | --- | --- | --- |
| Research Adversary human scores | New observations, claims, hypotheses, and any selected human-review batch rows scored during the window | items reviewed, usefulness, correctness, novelty, time saved, false positives, net review minutes, authority-boundary issues | Score each reviewed item after human review; summarize weekly and at day 30 | Human reviewer scores; evidence steward aggregates without inventing scores |
| Research Director shadow predictions | Each new shadow-cycle prediction for bottleneck, uncertainty, experiment, and debt item | `EXACT`, `PARTIAL`, `MISS`, `FALSE`, `PENDING`, prediction accuracy, useful prioritizations, missed prioritizations, false prioritization rate, estimated research hours saved | Record before each cycle; score only after later evidence exists; summarize weekly | Research reviewer scores after-the-fact; Director has no authority |
| Candidate validation outcomes | Focused direct-validation candidates and any new validation reports | candidates reviewed, confirmed, weakened, insufficient data, evaluable count, sample size, direct/proxy label, blocker class, validation delta | Record per validation artifact; summarize weekly | Candidate reviewer records outcome labels; no promotion or rejection authority |
| Vocabulary bridge diagnostics | Bridge-affected or diagnostic candidates, especially CHOP/RANGE_BOUND and UNKNOWN cases | samples retained, bridge-added samples, candidates improved, evaluable, confirmed in simulation, still blocked, mapping type, confidence, false-positive risk | Record per diagnostic report; weekly summary if diagnostics occur | Vocabulary reviewer preserves exact-vs-bridge separation |
| Intraday/event validation | High-value intraday and event-required candidates | candidate id, symbols, timeframe, data lineage status, event metadata status, trigger samples, surviving samples, validation outcome, remaining blocker | Record per readiness or validation artifact; day 15 and day 30 summary | Evidence steward records availability and lineage; validation reviewer classifies outcomes |
| Paper-forward outcomes | Paper-forward-ready candidates, plans, observations, and linked outcomes | candidates observed, outcomes recorded, survived, weakened, falsified, needs more data, linkage to current candidate id, review status | Record per outcome; weekly summary; day 30 survival readout | Human reviewer validates linkage and outcome label; no paper placement authority |

## Scoring Rules

### Research Adversary

Use the existing human review instructions for selected batch rows:

- `usefulness_score`: 1-5
- `correctness_score`: 1-5
- `novelty_score`: 1-5
- `time_saved_score`: 1-5
- `final_classification`: `USEFUL`, `PARTIALLY_USEFUL`, or `NOT_USEFUL`

Use the prospective pilot contract for live pilot summaries:

- reviewer usefulness average on the pilot's 0-5 scale;
- false-positive rate;
- estimated research hours saved net of review overhead;
- authority-boundary confirmation.

Do not treat blank human-score fields as zero. Do not compute human-score averages until a human reviewer has filled the relevant fields.

### Research Director

Each prospective prediction must be recorded before the later evidence exists. Score only after future work produces new evidence.

Allowed labels:

- `EXACT`: prediction matched the realized high-value bottleneck, experiment, or debt movement.
- `PARTIAL`: prediction matched the broad area but not the precise causal issue.
- `MISS`: realized high-value work was outside the shadow prediction set.
- `FALSE`: predicted item would have consumed effort without useful learning.
- `PENDING`: insufficient later evidence exists.

Prediction accuracy should count `EXACT` plus `PARTIAL` as useful alignment, with `EXACT` reported separately.

### Candidate Validation

Candidate validation outcomes must preserve the distinction between:

- direct validation evidence;
- proxy-dependent evidence;
- bridge-derived diagnostic evidence;
- intraday-required evidence;
- event-required evidence;
- insufficient-data outcomes;
- true weakened or rejection evidence.

Bridge-only or proxy-only evidence must not be counted as direct confirmation.

### Vocabulary Bridge Diagnostics

Vocabulary bridge results are diagnostic unless a later separately authorized validation process says otherwise.

Required labels:

- exact regime match;
- bridge approximation;
- unsafe mapping;
- unknown or untranslated regime;
- false-positive risk;
- bridge confidence.

`UNKNOWN` must remain unsafe as a validation substitute unless later evidence defines a specific translation. `CHOP -> RANGE_BOUND` may be tracked as an approximation diagnostic, not as canonical validation truth.

### Intraday/Event Validation

Intraday and event outcomes must record evidence completeness before interpretation:

- candidate id;
- symbols;
- timeframe;
- start and end date;
- bar interval;
- required fields;
- data lineage status;
- event timestamp lineage, when applicable;
- trigger samples;
- post-filter samples;
- validation outcome;
- remaining blocker.

Daily evidence must not be treated as intraday evidence for candidates whose rule semantics require intraday bars or event-timestamp alignment.

### Paper-Forward Outcomes

Paper-forward outcomes must be linked to current candidate ids before being used in survival-quality summaries.

Required labels:

- observation plan id, if present;
- candidate id;
- outcome status: `SURVIVED`, `WEAKENED`, `FALSIFIED`, or `NEEDS_MORE_DATA`;
- whether linkage to the current candidate campaign is explicit;
- reviewer notes;
- evidence limitations.

Paper-forward readiness is not outcome proof. Paper-forward outcomes do not authorize trading, capital, broker, sizing, or automatic placement.

## Cadence

| Day | Activity | Output |
| ---: | --- | --- |
| 0 | Freeze baseline from the six reviewed inputs and confirm authority boundary. | Baseline counts and no-authority statement. |
| 1-7 | Record all new Research Adversary reviews, Research Director shadow predictions, candidate validation outcomes, bridge diagnostics, intraday/event readiness updates, and paper-forward outcomes. | Week 1 measurement notes. |
| 7 | First weekly review. Score only items with completed human or later-evidence review. | Weekly metrics snapshot and unresolved `PENDING` list. |
| 8-14 | Continue recording prospective evidence. Preserve direct/proxy/bridge/intraday/event distinctions. | Week 2 measurement notes. |
| 15 | Interim checkpoint. Compare emerging evidence against hold thresholds without changing workflow. | Day 15 hold-risk readout. |
| 16-21 | Continue recording. Pay special attention to whether focused `INSUFFICIENT_DATA` falls below baseline `6/8`. | Week 3 measurement notes. |
| 22-29 | Continue recording and prepare final 30-day scoring packet. | Week 4 measurement notes and scoring queue. |
| 30 | Final prospective measurement review. | Day 30 measurement summary and authority-expansion evidence check. |

Weekly summaries should report counts, denominator quality, and `PENDING` items separately. Do not hide unresolved evidence by dropping it from denominators.

## Success Thresholds

| Lane | Success threshold |
| --- | --- |
| Research Adversary | At least 50 total pilot items reviewed or a clearly documented 30-day sample; usefulness average `>=3.5`; false-positive rate `<=20%`; positive net research hours saved; 0 authority violations. |
| Research Director | At least 4 prospective cycles or comparable cycle events; `EXACT` plus `PARTIAL` alignment `>=70%`; `FALSE` rate `<=20%`; at least one useful bottleneck or debt prediction confirmed by later evidence; 0 authority violations. |
| Candidate validation | Focused insufficient-data count falls from `6/8` to `<=2/8`, or the next focused cohort reaches at least `50%` confirmed with explicit direct/proxy labels; at least 2 evidence-complete weakened or rejected candidates are identified. |
| Vocabulary bridge diagnostics | Every bridge-affected candidate preserves exact-vs-bridge labels; unsafe mappings are rejected; bridge confidence and false-positive risk are recorded; no candidate is confirmed solely by bridge approximation. |
| Intraday/event validation | All high-value intraday and event-required candidates receive candidate-level readiness or outcome records; data lineage and event lineage are explicit; intraday/event evidence resolves at least one focused blocker or identifies a true hold/retire reason. |
| Paper-forward outcomes | At least 10 linked paper-forward outcomes exist before any survival-quality conclusion; survival rate `>=50%` for a positive readout; weakened/falsified outcomes are linked and counted rather than ignored. |

## Hold Thresholds

| Lane | Hold threshold |
| --- | --- |
| Research Adversary | Usefulness average below `3.0`, false-positive rate above `30%`, or reviewer burden offsets estimated time saved. |
| Research Director | `EXACT` plus `PARTIAL` alignment below `50%`, `FALSE` rate above `30%`, or predictions remain mostly unscorable after the window. |
| Candidate validation | Focused insufficient-data remains `>=6/8` and no concrete evidence-expansion path is identified. |
| Vocabulary bridge diagnostics | Bridge results increase ambiguity, rely on `UNKNOWN`, lack confidence labels, or produce no sample/evaluability gain. |
| Intraday/event validation | Required bars, symbol coverage, or event timestamps remain unavailable or unlineaged for material candidates. |
| Paper-forward outcomes | Fewer than 3 linked outcomes exist and no direct confirmation signal improves during the 30-day window. |

## Retire Thresholds

| Lane | Retire threshold |
| --- | --- |
| Research Adversary | Repeated authority-boundary confusion, materially misleading output, negative net research value, or persistent false-positive rate above `40%`. |
| Research Director | Repeated `FALSE` prioritizations over consecutive cycles, no useful bottleneck predictions, or any pressure to treat shadow output as authority. |
| Candidate validation | Evidence-complete candidates consistently fail, with more than `60%` of evidence-complete focused candidates weakened or true rejected and no fixable limitation. |
| Vocabulary bridge diagnostics | Mapping creates false validation, collapses distinct regimes, or cannot distinguish bridge approximation from exact regime evidence. |
| Intraday/event validation | Required intraday/event evidence becomes available and still falsifies or materially weakens the candidate with adequate samples. |
| Paper-forward outcomes | At least 10 linked outcomes exist with survival rate below `25%` and no fixable evidence limitation. |

## Required Evidence Before Any Authority Expansion

No authority expansion is allowed during this 30-day plan. After the window, authority expansion may only be considered if a separate human/governance process reviews evidence showing all of the following:

- 30-day prospective window completed or Research Adversary reached the 50-item pilot denominator;
- 0 authority-boundary violations across all six measurement lanes;
- human-filled Research Adversary scores exist and were not inferred from blank fields;
- Research Director predictions were recorded before the scored evidence existed;
- Research Director accuracy and false-prioritization thresholds were met prospectively;
- candidate validation preserves direct, proxy, bridge, intraday, event, and insufficient-data labels;
- bridge-derived evidence is never the sole basis for confirmation;
- focused insufficient-data materially decreases or candidate-specific hold/retire reasons become evidence-complete;
- intraday/event lineage is sufficient for any candidate whose semantics require it;
- at least 10 linked paper-forward outcomes exist before survival-quality claims are made;
- any proposed implementation or governance change is handled in a separate change process with explicit approval.

Minimum recommendation logic after day 30:

| Result | Use when |
| --- | --- |
| `CONTINUE_MEASUREMENT` | Useful signal exists, but denominators remain too small or evidence lanes remain incomplete. |
| `HOLD_EXPANSION` | One or more hold thresholds are met, or evidence cannot separate quality from validation limitation. |
| `RETIRE_LANE` | One lane meets its retire threshold and should stop being treated as a candidate for authority expansion. |
| `CONSIDER_SEPARATE_AUTHORITY_REVIEW` | All success thresholds relevant to the proposed authority expansion are met with 0 boundary violations. |

## 30-Day Output Packet

The day-30 review should produce a report-only packet containing:

- Research Adversary score summary and false-positive analysis;
- Research Director shadow prediction score table;
- candidate validation outcome summary;
- vocabulary bridge diagnostic summary;
- intraday/event validation readiness and outcome summary;
- paper-forward linkage and outcome summary;
- unresolved `PENDING` evidence list;
- hold/retire threshold assessment;
- explicit no-authority-change conclusion unless a separate approved review is opened.

## Final Operating Plan

For the next 30 days, Atlas should measure prospective evidence in six separate lanes, score each lane only when the relevant human or later-evidence review exists, preserve all evidence-source distinctions, and make no workflow, candidate, replay, qualification, governance, memory, paper-forward, trading, capital, broker, or sizing changes.

The expected day-30 default outcome is `CONTINUE_MEASUREMENT` unless the evidence clearly satisfies success thresholds or triggers a hold/retire threshold.
