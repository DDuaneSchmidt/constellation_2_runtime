# Research Director Shadow Cycle 002

Date: 2026-06-05
Status: SHADOW_MODE_RECORD
Mode: PROSPECTIVE_TRIAL

## Scope

Start collecting prospective evidence for Research Director.

This shadow cycle records what Research Director would predict before the next research cycle is evaluated. It does not change workflow order, mutate backlog state, modify candidates, modify replay, modify validation, modify qualification, change governance, write memory, create paper-forward actions, recommend trades, allocate capital, authorize broker execution, or size positions.

Inputs reviewed:

- `research_journal/reports/research_director_shadow_mode_001.md`
- `research_journal/reports/research_director_next_bottleneck_001.md`
- `research_journal/reports/research_director_service_hardening_001.md`
- `research_journal/reports/candidate_quality_decision_gate_001.md`
- `research_journal/reports/candidate_quality_scoreboard_002.md`
- `research_journal/reports/qualification_failure_attribution_ledger_001.md`
- `research_journal/reports/intraday_event_evidence_readiness_pack_001.md`
- `research_journal/reports/vocabulary_bridge_refinement_plan_001.md`
- `research_journal/reports/event_candidate_feasibility_001.md`

## Prospective Rule

This report is a prediction ledger.

It should be scored only after future work creates new evidence. Do not score it against artifacts that already existed at the time of this report. Do not use it to direct work. Do not treat any item as approved, assigned, required, or authoritative.

## Cycle Context

Cycle 001 predicted:

- top bottleneck: `Data Coverage`
- top uncertainty: `Replay Attrition`
- top experiment: diagnostic `CHOP -> RANGE_BOUND` vocabulary bridge
- top research debt item: `Data Coverage`

Cycle 002 starts after several report-only analyses changed the evidence picture:

- focused direct validation now runs for `8/8` candidates
- `2/8` focused candidates are confirmed
- `6/8` focused candidates remain `INSUFFICIENT_DATA`
- proxy dependence remains `600/600`
- replay gaps remain large at `372/600`
- `4` focused candidates are high-value intraday candidates
- `1` focused candidate remains `EVENT_REQUIRED`
- vocabulary bridge diagnostics show sample recovery but no validation authority
- paper-forward outcomes remain too sparse for survival-quality claims

The predicted bottleneck should therefore move from broad data coverage toward candidate-specific evidence expansion and blocker attribution.

## Predicted Bottlenecks

| Rank | Predicted Bottleneck | Prediction | Why It Should Matter Next | Future Scoring Test |
| ---: | --- | --- | --- | --- |
| 1 | Candidate-specific intraday evidence | `HIGH_VALUE_INTRADAY` evidence will be the next hard validation bottleneck. | Daily/direct friction has improved, but candidate quality still depends on 5m/15m/30m/1h rule-specific evidence. Four focused candidates are already classified as high-value intraday targets. | Score as `EXACT` if future root-cause or validation work shows intraday evidence is required before reducing `INSUFFICIENT_DATA` below `6/8`. |
| 2 | Proxy dependence plateau | Proxy dependence will remain the largest unchanged debt class unless candidate-specific evidence is produced. | `600/600` evaluated candidates still carry proxy penalty exposure. Direct coverage wins have not retired qualification-scale proxy dependence. | Score as `EXACT` if future reports still show proxy dependence blocking candidate-quality conclusions. |
| 3 | Qualification failure attribution | The next useful analysis bottleneck will be distinguishing true weak candidates from fixable validation limitations. | `490/600` candidates fail final qualification, but failure causes mix true weakness, proxy debt, sample-size limits, and intraday/daily mismatch. | Score as `EXACT` if future work needs candidate-level failure categories before choosing remediation. |
| 4 | Event metadata readiness | Event metadata will remain a narrow but high-complexity blocker for `ptc_backtest_final_d5931b24bd391113`. | The single `EVENT_REQUIRED` candidate is not retired, but it requires timestamped event lineage plus 30m bars and is not the fastest validation-throughput path. | Score as `PARTIAL` or `EXACT` if event metadata becomes a blocker in future validation work; score as `FALSE` if event work proves irrelevant or safely retired. |
| 5 | Vocabulary bridge authority boundary | Bridge-derived sample recovery will remain useful diagnostically but risky if confused with validation. | `CHOP -> RANGE_BOUND` recovers samples in simulation, while `LOW_VOLATILITY` and `UNKNOWN` remain unsafe as validation substitutes. | Score as `EXACT` if future work needs explicit separation between exact validation and bridge-derived diagnostics. |

## Predicted Experiments

These are shadow predictions of useful future experiments. They do not authorize implementation or workflow changes.

| Rank | Predicted Experiment | Expected Learning | Success Signal | Failure Signal | Authority Boundary |
| ---: | --- | --- | --- | --- | --- |
| 1 | Intraday readiness comparison for the four `HIGH_VALUE_INTRADAY` candidates | Determines which intraday symbols/timeframes would most reduce focused `INSUFFICIENT_DATA`. | Produces candidate-level readiness matrix and identifies the highest-leverage data cluster. | Adds broad data requirements without reducing candidate uncertainty. | Readiness only; no data acquisition, replay, candidate, or qualification change. |
| 2 | Proxy-vs-direct attribution ledger for qualification failures | Separates true weak candidates from proxy-only and evidence-limited candidates. | Reduces ambiguity in the `490/600` failure population and labels repairable versus reject-for-now cases. | Produces another summary without candidate-level attribution. | Attribution only; no qualification override. |
| 3 | Exact-vs-bridge validation comparison for affected `CHOP` candidates | Measures how much `CHOP -> RANGE_BOUND` changes sample survival while preserving approximation labels. | Reports exact samples, bridge samples, bridge delta, false-positive risk, and remaining blockers per candidate. | Treats bridge samples as canonical validation or expands into `LOW_VOLATILITY`/`UNKNOWN`. | Diagnostic only; no replay or validation authority. |
| 4 | Event candidate bounded feasibility check | Determines whether the single event candidate should stay deferred, move to pursue, or retire. | Shows event metadata is reusable across multiple candidates or that eligible event counts are likely adequate. | Confirms event metadata would serve only one low-priority candidate with weak sample yield. | Feasibility only; no event ingestion or replay change. |
| 5 | Human-scored Research Adversary import dry-run | Tests whether review scores can be validated and summarized without score fabrication. | Produces validation-error counts and summary schema once human scores exist. | Computes averages from pending rows or fabricates scores. | Evaluation only; no memory, workflow, or candidate authority. |

## Predicted Debt Items

| Rank | Predicted Debt Item | Current Evidence | Expected Next Movement | Measurement |
| ---: | --- | --- | --- | --- |
| 1 | Proxy dependence debt | `600/600` evaluated candidates carry proxy penalty exposure. | Should remain high until candidate-specific intraday/direct evidence is available. | Count proxy-penalized candidates and proxy-only paper-forward-ready candidates. |
| 2 | Intraday evidence debt | Focused candidates require rule-specific intraday evidence; four are high-value intraday targets. | Should become the main blocker after daily direct replay friction is reduced. | Count candidates with required timeframe coverage and post-filter sample sufficiency. |
| 3 | Replay/sample attrition debt | `6/8` focused candidates remain `INSUFFICIENT_DATA`; replay gaps remain `372/600`. | Should decrease only if intraday/vocabulary/event evidence changes post-filter sample survival. | Track trigger samples, surviving samples, and sample-size failures per candidate. |
| 4 | Qualification attribution debt | `490/600` fail final qualification; only a materialized preview is deeply attributed. | Should decrease if failure categories are assigned across more candidates. | Count failures with explicit cause: true weak, evidence repair, proxy-only, intraday/event required, vocabulary mismatch. |
| 5 | Vocabulary bridge governance debt | Bridge diagnostics are useful but not authoritative; unsafe mappings remain tempting. | Should decrease if bridge outputs consistently preserve mapping type, confidence, and no-authority labels. | Count bridge reports with explicit exact-vs-bridge separation and rejected unsafe mappings. |
| 6 | Event metadata debt | One focused candidate needs event metadata and 30m event-aware replay. | Should remain deferred unless metadata becomes reusable or already available. | Count event candidates sharing the same metadata lane and eligible event sample estimates. |
| 7 | Human review evidence debt | Research Adversary has generated review volume but human scoring is pending. | Should decrease only after human-filled score rows exist. | Count valid scored rows, pending rows, invalid rows, and authority-boundary failures. |
| 8 | Outcome linkage debt | Paper-forward outcomes remain sparse and not enough for survival-quality claims. | Should remain high until linked outcomes reach minimum volume. | Count linked paper-forward outcomes by current candidate id and outcome status. |

## Predicted Next Gate

Predicted next gate status: `EXPAND_EVIDENCE`.

Reason:

The path has positive evidence, including `2/8` direct confirmations and a decomposed set of evidence lanes. But the focused validation set remains underpowered because `6/8` candidates are still insufficient-data, proxy dependence remains universal at qualification scale, intraday/event requirements remain unresolved, and paper-forward outcomes are too sparse.

Expected future downgrade to `HOLD` if:

- intraday/event readiness does not reduce any focused insufficiency,
- bridge diagnostics cannot safely distinguish vocabulary mismatch from false positives,
- qualification attribution shows most failures are true weak candidates rather than evidence limitations,
- paper-forward outcomes remain sparse and unrelated to current candidates.

Expected future upgrade to `CONTINUE_VALIDATION` if:

- focused insufficient-data falls to `<=2/8`,
- at least `4/8` focused candidates confirm or at least `50%` confirm in the next focused cohort,
- at least `2` evidence-complete candidates are directly weakened or rejected,
- proxy dependence drops below the current universal level,
- paper-forward outcomes become linked and numerous enough to support quality claims.

## Scoring Plan For This Cycle

Future scoring should classify each prediction as:

- `EXACT`: prediction matched the realized high-value bottleneck, experiment, or debt movement.
- `PARTIAL`: prediction matched the broad area but not the precise causal issue.
- `MISS`: realized high-value work was outside this shadow prediction set.
- `FALSE`: predicted item would have consumed effort without useful learning.
- `PENDING`: not enough future evidence exists yet.

Cycle 002 should not be scored until at least one future research/validation cycle produces new evidence after this report.

## Authority Boundary

This report is a prospective shadow record only. It does not authorize workflow changes, backlog changes, implementation, data acquisition, replay changes, validation changes, qualification changes, candidate promotion or rejection, governance changes, paper-forward actions, memory writes, trade recommendations, broker execution, capital allocation, portfolio construction, or position sizing.
