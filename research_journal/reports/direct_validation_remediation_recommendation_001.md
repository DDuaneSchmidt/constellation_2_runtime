# Direct Validation Remediation Recommendation 001

Date: 2026-06-05

## Scope

This ranks diagnostic and implementation candidates for resolving candidate-specific replay sample attrition. Recommendations are allowed here; implementation is explicitly out of scope.

## Ranking

| rank | action | validation unblock potential | implementation complexity | authority risk | expected candidate evidence gain | rationale |
|---:|---|---|---|---|---|---|
| 1 | Add stage-level telemetry | high | low | low | high | The current report hides raw, trigger, regime-survivor, selected-symbol, and per-symbol counts. Telemetry would identify fatal stages without changing replay decisions. |
| 2 | Improve filter diagnostics | high | low | low | high | The observed fatal stage is regime filtering. Diagnostics should expose candidate regime label, computed sample regimes, and exact rejection counts. |
| 3 | Support symbol-level evidence summaries | medium | low to medium | low | medium | Every zero-sample candidate has per-symbol trigger samples, but the report surfaces only the selected symbol. Symbol summaries would prevent false aggregation diagnoses. |
| 4 | Add regime-label translation diagnostics | high | medium | medium | high | Candidate `CHOP` labels currently do not survive exact daily-bar regime matching. Diagnostics should measure CHOP versus RANGE_BOUND/UNKNOWN compatibility before any rule change. |
| 5 | Add event metadata path | medium | medium | low to medium | medium | The event-reaction zero-sample candidate requires event context and intraday plus daily confirmation. Metadata capture should come before judging event-reaction replay failure. |
| 6 | Add intraday replay path | high for intraday/event candidates | high | medium | high | Intraday confirmation is required for the event-reaction candidate and optional for several others. This is likely useful, but should follow telemetry so scope stays controlled. |
| 7 | Revise universe aggregation diagnostics | medium | medium | low | medium | Aggregation is not the current sample-loss cause, but diagnostics should state whether the candidate used best-symbol, any-symbol, all-symbol, or combined-universe evaluation. |

## Recommended Sequence

1. Add read-only stage-level telemetry to direct validation outputs.
2. Add regime-filter diagnostics that report computed sample-regime distributions and candidate regime constraints.
3. Add symbol-level evidence summaries to direct validation reports.
4. Add analysis-only regime-label translation diagnostics for `CHOP`, `RANGE_BOUND`, `UNKNOWN`, and `LOW_VOLATILITY`.
5. Define event metadata requirements for event-reaction validation.
6. Design, then separately approve, an intraday replay path.
7. Only after diagnostics are stable, consider whether any replay or aggregation behavior should change.

## Not Recommended Now

- Do not relax the regime filter in production based only on this study.
- Do not override replay or qualification.
- Do not promote any candidate.
- Do not infer candidate failure from daily proxy attrition where intraday or event metadata is required.
- Do not change governance or authority boundaries.

## Authority Boundary

This recommendation is diagnostic planning only. It does not implement any remediation, promote candidates, override replay, override qualification, change governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.
