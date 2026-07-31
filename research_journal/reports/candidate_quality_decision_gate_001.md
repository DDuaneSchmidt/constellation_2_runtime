# Candidate Quality Decision Gate 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Scope: read-only decision gate for Atlas anomaly quality. No implementation changes, no replay changes, no qualification changes, no candidate changes, no governance changes, no paper-forward changes, no memory writes, no trade recommendations, no capital authority, no broker execution, and no position sizing.

## Inputs Reviewed

- `research_journal/reports/candidate_quality_scoreboard_002.md`
- `research_journal/reports/qualification_failure_attribution_ledger_001.md`
- `research_journal/reports/intraday_event_evidence_readiness_pack_001.md`

## Gate Status

`EXPAND_EVIDENCE`

Atlas has enough signal to keep the path alive, but not enough validation-quality evidence to continue treating daily/direct friction reduction as anomaly-quality proof. The next gate should require expanded candidate-specific evidence: intraday bars for high-value intraday candidates, event metadata for event-reaction candidates, and clearer separation of true rejections from validation limitations.

## Current State

| Metric | Current Value | Gate Reading |
| --- | ---: | --- |
| Focused direct-validation candidates | 8 | Small but usable validation sample. |
| Confirmed candidates | 2 | Positive signal, but below scale threshold. |
| Weakened candidates | 0 | No direct weakened/rejection signal yet. |
| Insufficient-data candidates | 6 | Too high; validation is still underpowered. |
| Materialized failed candidates ledgered | 56 | Useful preview only, not full 490-failure population. |
| Materialized true rejection / major redesign | 28 | Good rejection signal in preview. |
| Materialized fixable validation limitation | 28 | Half of failed preview is still evidence debt. |
| Proxy-dependent candidates | 600 | Universal qualification-scale limitation. |
| Intraday-required failed rows | 56 | All materialized failed rows require intraday-sensitive evidence. |
| Event-required failed rows | 2 | Small but important metadata-specific lane. |
| High-value intraday candidates | 4 | Clear evidence-expansion lane. |
| Event-required focused candidates | 1 | Event metadata is mandatory before validation is meaningful. |
| Paper-forward-ready candidates | 58 | Observation readiness, not outcome quality. |
| Paper-forward outcomes | 2 | Too small for survival-quality gate. |

## Thresholds

### Confirmed Candidates

| Gate Outcome | Threshold |
| --- | --- |
| `CONTINUE_VALIDATION` | At least `4/8` focused direct candidates confirmed, or at least `50%` confirmed in the next focused validation cohort. |
| `EXPAND_EVIDENCE` | `1-3/8` confirmed with clear evidence gaps remaining. |
| `HOLD` | `0` confirmed and no evidence-expansion path identified. |
| `RETIRE_PATH` | `0` confirmed after candidate-specific intraday/event evidence is available and replay remains weak. |

Current: `2/8`.

Gate result: `EXPAND_EVIDENCE`.

### Weakened Candidates

| Gate Outcome | Threshold |
| --- | --- |
| `CONTINUE_VALIDATION` | At least `2` focused candidates are directly weakened or rejected with adequate evidence, proving the system can reject as well as confirm. |
| `EXPAND_EVIDENCE` | `0-1` weakened because most failures remain insufficient-data. |
| `HOLD` | No weakened candidates and no true-rejection attribution. |
| `RETIRE_PATH` | More than `60%` of evidence-complete focused candidates are weakened or true rejected. |

Current: `0` direct weakened; `28/56` materialized failed rows are true rejection or major redesign in the ledger.

Gate result: `EXPAND_EVIDENCE`.

### Insufficient-Data Reduction

| Gate Outcome | Threshold |
| --- | --- |
| `CONTINUE_VALIDATION` | Focused direct insufficient-data count falls to `<=2/8`. |
| `EXPAND_EVIDENCE` | Insufficient-data remains `3-6/8` and the missing evidence class is known. |
| `HOLD` | Insufficient-data remains `>=6/8` with no known evidence-expansion path. |
| `RETIRE_PATH` | Insufficient-data persists after required intraday/event/vocabulary evidence is available. |

Current: `6/8`, with known intraday, event, vocabulary, and sample-size evidence gaps.

Gate result: `EXPAND_EVIDENCE`.

### True Rejection Rate

| Gate Outcome | Threshold |
| --- | --- |
| `CONTINUE_VALIDATION` | True rejection / major redesign rate is between `30%` and `70%` among materialized failed candidates, with evidence gaps separately labeled. |
| `EXPAND_EVIDENCE` | True rejection exists but only in a partial preview or overlaps heavily with evidence gaps. |
| `HOLD` | Rejections cannot be distinguished from validation limitations. |
| `RETIRE_PATH` | Evidence-complete candidates show `>70%` true rejection or major redesign. |

Current: `28/56` materialized failed candidates, or `50%`, are true rejection / major redesign; however this covers only the materialized failed preview, not all 490 failures.

Gate result: `CONTINUE_VALIDATION` for attribution quality, but not enough to override the overall `EXPAND_EVIDENCE` status.

### Validation Limitation Reduction

| Gate Outcome | Threshold |
| --- | --- |
| `CONTINUE_VALIDATION` | Proxy dependence drops below `75%`, replay gaps drop below `50%`, and focused validation limitations drop below `40%`. |
| `EXPAND_EVIDENCE` | Limitations remain high but are decomposed into actionable evidence lanes. |
| `HOLD` | Limitations remain high and undifferentiated. |
| `RETIRE_PATH` | Limitations remain high after candidate-specific evidence has been supplied. |

Current:

- proxy dependence: `600/600`, or `100%`
- replay gaps: `372/600`, or `62.0%`
- focused insufficient-data: `6/8`, or `75.0%`
- materialized failed validation limitations: `28/56`, or `50%`

Gate result: `EXPAND_EVIDENCE`.

### Paper-Forward Readiness

| Gate Outcome | Threshold |
| --- | --- |
| `CONTINUE_VALIDATION` | At least `10%` of total candidates are paper-forward-ready and candidate-specific evidence limitations are explicitly labeled. |
| `EXPAND_EVIDENCE` | Paper-forward readiness exists but remains proxy/evidence limited. |
| `HOLD` | Paper-forward-ready count is near zero or unsupported by validation evidence. |
| `RETIRE_PATH` | Paper-forward-ready candidates repeatedly fail evidence-complete validation. |

Current: `58/600`, or `9.7%`, paper-forward-ready, with proxy dependence still universal.

Gate result: `EXPAND_EVIDENCE`.

### Paper-Forward Survival

| Gate Outcome | Threshold |
| --- | --- |
| `CONTINUE_VALIDATION` | At least `10` linked paper-forward outcomes and survival rate `>=50%`, with candidate IDs linked to current campaign/readiness set. |
| `EXPAND_EVIDENCE` | Fewer than `10` outcomes, but validation path remains promising. |
| `HOLD` | Fewer than `3` outcomes and no direct confirmation signal. |
| `RETIRE_PATH` | At least `10` linked outcomes with survival rate `<25%` and no fixable evidence limitation. |

Current: `2` outcomes total, `1` survived and `1` weakened; linkage to the current candidate campaign is not sufficient for durable quality claims.

Gate result: `EXPAND_EVIDENCE`.

## Decision Logic

| Status | Use When |
| --- | --- |
| `CONTINUE_VALIDATION` | Confirmation, rejection, limitation reduction, and paper-forward readiness are strong enough to keep validating the current path without first expanding evidence. |
| `EXPAND_EVIDENCE` | The path has positive signal, but validation limitations dominate and candidate-specific evidence is required before the next quality decision. |
| `HOLD` | Evidence is too weak or too ambiguous to justify more validation work, but not poor enough to retire. |
| `RETIRE_PATH` | Evidence-complete candidates consistently fail or weaken, and limitations no longer explain the failures. |

Current path has:

- positive signal: `2` confirmed focused candidates;
- rejection signal: `28/56` materialized failed candidates are true rejection / major redesign;
- unresolved validation debt: `6/8` focused insufficient-data, `600/600` proxy dependence, `372/600` replay gaps;
- clear expansion lanes: `4` high-value intraday candidates and `1` event-required candidate;
- underpowered outcome evidence: `2` paper-forward outcomes.

Therefore the only defensible status is:

```text
EXPAND_EVIDENCE
```

## Required Evidence Before Next Gate

The next gate should not ask for more volume. It should require:

- intraday evidence readiness for the `4` `HIGH_VALUE_INTRADAY` candidates;
- event metadata readiness for the `1` `EVENT_REQUIRED` candidate;
- exact versus bridge-derived validation labels for vocabulary-gap candidates;
- reduction of focused direct `INSUFFICIENT_DATA` from `6` to `<=2`;
- at least `2` evidence-complete weakened/rejected direct candidates;
- proxy-dependence reclassification for candidates where direct evidence now exists;
- at least `10` linked paper-forward outcomes before any survival-quality conclusion.

## Final Output

`EXPAND_EVIDENCE`

## Authority Boundary

This report is a decision-gate artifact only. It does not implement evidence expansion, change validation, change replay, change qualification, change candidate state, modify governance, authorize paper-forward activity, recommend trades, allocate capital, authorize broker execution, size positions, or write memory automatically.
