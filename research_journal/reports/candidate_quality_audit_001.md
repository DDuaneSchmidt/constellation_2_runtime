# Candidate Quality Audit 001

Date: 2026-06-05
Session: 5 - Candidate Quality Audit
Status: GENERATED_ONLY
Scope: audit of the 56 materialized failed candidates only. No implementation changes, no replay changes, no qualification changes, no candidate-state changes, and no workflow authority.

## Question

For the 56 materialized failures, how many are:

- `TRUE_REJECTION`
- `VALIDATION_LIMITATION`

This is the most direct measurement of whether the anomalies are weak or validation is weak.

## Inputs Reviewed

- `research_journal/reports/qualification_failure_analysis_001.md`
- `research_journal/reports/qualification_failure_attribution_ledger_001.md`
- `research_journal/reports/qualification_failure_attribution_ledger.csv`
- `research_journal/reports/candidate_quality_scoreboard_002.md`
- `research_journal/reports/candidate_quality_decision_gate_001.md`

## Population

The full qualification surface contains:

| Measure | Count |
|---|---:|
| Total candidates evaluated | 600 |
| Final eligible candidates | 110 |
| Final qualification failures | 490 |
| Materialized excluded preview rows | 100 |
| Materialized `REJECT_FOR_NOW` failures audited here | 56 |

This audit covers the 56 candidate-level materialized failures in the attribution ledger. It does not claim candidate-level attribution for all 490 failures.

## Classification Rule

The existing ledger contains several primary failure classes:

- `TRUE_REJECTION`
- `EVIDENCE_GAP`
- `INTRADAY_REQUIRED`
- `EVENT_REQUIRED`

For this audit, those are folded into the two requested categories:

| Ledger classification | Audit classification | Reason |
|---|---|---|
| `TRUE_REJECTION` | `TRUE_REJECTION` | Candidate has weak backtest evidence and should be held, retired, or redesigned unless the mechanism thesis changes. |
| `EVIDENCE_GAP` | `VALIDATION_LIMITATION` | Failure is caused by insufficient evidence, missing samples, or low sample size. |
| `INTRADAY_REQUIRED` | `VALIDATION_LIMITATION` | Failure is caused by daily/proxy evidence being inadequate for an intraday-sensitive mechanism. |
| `EVENT_REQUIRED` | `VALIDATION_LIMITATION` | Failure is caused by missing event metadata or event-window evidence. |

Proxy dependence, vocabulary mismatch, and unresolved regime labels are also treated as validation limitations unless the candidate is already primarily `TRUE_REJECTION`.

## Headline Result

| Audit Classification | Count | Share of 56 |
|---|---:|---:|
| `TRUE_REJECTION` | 28 | 50.0% |
| `VALIDATION_LIMITATION` | 28 | 50.0% |
| Total | 56 | 100.0% |

## Primary Failure Detail

| Primary Failure Class | Count | Folded Audit Class |
|---|---:|---|
| `TRUE_REJECTION` | 28 | `TRUE_REJECTION` |
| `EVIDENCE_GAP` | 14 | `VALIDATION_LIMITATION` |
| `INTRADAY_REQUIRED` | 13 | `VALIDATION_LIMITATION` |
| `EVENT_REQUIRED` | 1 | `VALIDATION_LIMITATION` |

## Cross-Cutting Validation Limits

Even the 28 true-rejection rows sit inside a validation-limited environment. The ledger shows:

| Flag | Count Among 56 | Interpretation |
|---|---:|---|
| Proxy dependence | 56 | Every materialized failure inherits the global proxy-dependence penalty. |
| Intraday required | 56 | Every materialized failure is intraday-sensitive or carries intraday timeframe requirements. |
| Evidence gap | 28 | Half the rows still have a direct fixable or retestable evidence gap. |
| Regime vocabulary issue | 24 | `CHOP` or `UNKNOWN` regimes create semantic uncertainty. |
| Sample-size issue | 15 | A direct sample-size or insufficient-data failure is present. |
| Event metadata required | 2 | Event-reaction candidates need event-specific validation. |
| Backtest weak | 28 | The cleanest true-rejection or major-redesign signal. |

## Interpretation

The 56 materialized failures are not mostly weak anomalies and not mostly weak validation. They split exactly in half:

- 28 are best treated as `TRUE_REJECTION`.
- 28 are best treated as `VALIDATION_LIMITATION`.

This means Atlas is beginning to separate weak candidates from weak evidence, but it has not yet proven that most failures are genuine anomaly weakness. Half of the materialized failed preview still requires better evidence before the anomaly can be judged.

## Are The Anomalies Weak?

Partly yes.

The 28 `TRUE_REJECTION` rows are driven by weak backtest evidence. These should not be promoted through more validation work unless the hypothesis or mechanism is redesigned. They are the clearest evidence that Atlas can identify weak anomalies.

However, this is only half of the materialized failure set. Treating all 56 as weak anomalies would overstate candidate weakness and understate evidence debt.

## Or Is Validation Weak?

Also partly yes.

The 28 `VALIDATION_LIMITATION` rows are blocked by evidence gaps, intraday requirements, event requirements, vocabulary issues, sample-size limits, or proxy dependence. These are not clean anomaly failures. They are unresolved validation cases.

The validation weakness is especially important because all 56 rows have proxy dependence and intraday sensitivity. That means even the true-rejection half exists inside a broader evidence-fidelity problem.

## Direct Answer

For the 56 materialized failures:

```text
TRUE_REJECTION: 28
VALIDATION_LIMITATION: 28
```

Conclusion:

```text
The materialized failures split 50/50.
Atlas is not merely producing weak anomalies.
Atlas is also carrying substantial validation weakness.
The current failure set is half candidate-quality failure and half evidence-readiness debt.
```

## Decision Implication

The correct next posture remains `EXPAND_EVIDENCE`, not `RETIRE_PATH`.

Reason:

- the `TRUE_REJECTION` half should be held, retired, or redesigned;
- the `VALIDATION_LIMITATION` half should receive targeted evidence expansion before quality judgment;
- the system should not treat insufficient evidence as anomaly falsification.

## Authority Boundary

This report is an audit artifact only. It does not promote candidates, reject candidates operationally, change qualification thresholds, change replay behavior, change validation behavior, alter governance, authorize paper-forward activity, recommend trades, allocate capital, size positions, authorize broker execution, or write memory automatically.
