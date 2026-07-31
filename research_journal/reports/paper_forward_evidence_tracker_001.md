# Paper Forward Evidence Tracker 001

Date: 2026-06-06
Status: MEASUREMENT_ONLY
Scope: paper-forward candidate evidence tracking for final scoreboard preparation.

This artifact records observed paper-forward outcomes only. It contains no predictions, no recommendations, no trade advice, no promotion decisions, no retirement decisions, and no changes to candidate, validation, paper, governance, runtime truth, or capital behavior.

## Measurement Boundary

Population:

- Candidates that entered a paper-forward state.
- Candidates with an observable paper-forward start date.
- Candidates whose current paper-forward state can be classified as survived, failed, or unknown.

Excluded:

- Candidates that never entered paper-forward observation.
- Candidate ideas without a paper-forward start date.
- Hypothetical future candidates.
- Any candidate without enough observed state to assign the requested fields.

## Candidate-Level Tracker

Required fields:

| Field | Measurement |
|---|---|
| `candidate_id` | Stable candidate identifier. |
| `paper_forward_status` | Observed paper-forward status label from source evidence. |
| `start_date` | Date paper-forward observation began. |
| `outcome_date` | Date the survived or failed outcome became observable; blank when unknown or still open. |
| `current_state` | Current observed state at measurement time. |
| `survived` | `1` when the candidate has an observed survival outcome; otherwise `0`. |
| `failed` | `1` when the candidate has an observed failure outcome; otherwise `0`. |
| `unknown` | `1` when the candidate has no resolved survival or failure outcome; otherwise `0`. |

Ledger:

| candidate_id | paper_forward_status | start_date | outcome_date | current_state | survived | failed | unknown | failure_reason | validation_quality_at_entry |
|---|---|---:|---:|---|---:|---:|---:|---|---|

No candidate rows loaded.

## Metric Definitions

### `paper_forward_survival_rate`

Definition:

```text
survival_rate = survived_count / resolved_count
resolved_count = survived_count + failed_count
```

Measurement rule:

- Unknown rows are excluded from the denominator.
- If `resolved_count = 0`, survival rate is `NA`.

Current measurement:

| Metric | Value |
|---|---:|
| survived_count | 0 |
| failed_count | 0 |
| unknown_count | 0 |
| resolved_count | 0 |
| paper_forward_survival_rate | NA |

### `average_duration`

Definition:

```text
duration_days = outcome_date - start_date
average_duration = sum(duration_days for resolved rows) / resolved_count
```

Measurement rule:

- Only resolved survived or failed rows with both `start_date` and `outcome_date` are included.
- Unknown or still-open rows are excluded.
- If no resolved rows have complete dates, average duration is `NA`.

Current measurement:

| Metric | Value |
|---|---:|
| resolved_rows_with_complete_dates | 0 |
| average_duration | NA |

### `failure_reasons`

Definition:

```text
failure_reasons = count of failed rows grouped by failure_reason
```

Measurement rule:

- Only rows with `failed = 1` are included.
- Blank failure reasons are reported as `UNSPECIFIED`.

Current measurement:

| failure_reason | count |
|---|---:|
| NONE_RECORDED | 0 |

### `validation_quality_at_entry`

Definition:

Observed validation quality classification at the time the candidate entered paper-forward tracking.

Measurement rule:

- Record the source classification as observed.
- Do not infer quality from later outcomes.
- Do not convert outcome success or failure into entry quality.

Current measurement:

| validation_quality_at_entry | count |
|---|---:|
| NONE_LOADED | 0 |

## Scoreboard Readiness

| Check | Current State |
|---|---|
| Candidate-level fields present | YES |
| Survival metric defined | YES |
| Duration metric defined | YES |
| Failure reason aggregation defined | YES |
| Entry validation quality aggregation defined | YES |
| Source population loaded | NO |
| Resolved outcomes loaded | NO |

## Authority Boundary

This tracker is a measurement artifact only. It does not create predictions, recommendations, promotion rules, rejection rules, paper-forward authorization, position-management instructions, trading instructions, capital allocation, runtime truth, or governance changes.
