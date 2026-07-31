# Regime Filter Remediation Plan 001

Date: 2026-06-05

## Scope

This ranks remediation actions for the direct-validation regime-filter attrition issue. It is a recommendation artifact only; no implementation is included.

## Ranked Actions

| rank | action | validation unblock potential | implementation complexity | authority risk | expected evidence gain | rationale |
|---:|---|---|---|---|---|---|
| 1 | Add regime-filter telemetry to direct validation reports | high | low | low | high | Current reports expose final sample loss but not the full triggered-sample regime distribution. Telemetry would show exact required labels, emitted labels, and rejection counts. |
| 2 | Separate daily proxy validation from intraday validation | high | medium | low | high | Daily proxy replay can diagnose daily-bar behavior, but it cannot certify candidates requiring intraday/event regime evidence. |
| 3 | Validate candidate-regime label compatibility before replay | high | medium | low to medium | high | `CHOP` currently has no exact daily replay label. Compatibility checks should flag impossible label combinations before sample attrition is mistaken for candidate failure. |
| 4 | Support regime sensitivity reporting | medium | medium | low | medium | Analysis-only sensitivity can show how many samples would survive under declared alternate labels without changing production filters. |
| 5 | Defer candidates requiring intraday regime evidence | medium | low | low | medium | Intraday-required candidates should not be failed by daily proxy regime filters; they should be marked as needing matching timeframe evidence. |

## Recommended Sequence

1. Add read-only regime telemetry fields: candidate regime, allowed regimes, emitted regime counts, rejected count by emitted regime, and exact first failed condition.
2. Add report-only timeframe segmentation: daily-only, daily-plus-optional-intraday, and intraday-required.
3. Add a compatibility precheck that flags candidate labels not emitted by the replay classifier.
4. Add offline sensitivity output for alternate regime labels.
5. Defer intraday-required candidates until matching timeframe data and event metadata are available.

## Explicit Non-Actions

- Do not relax production regime filters.
- Do not change replay behavior.
- Do not promote candidates.
- Do not change qualification.
- Do not change governance.
- Do not add trading, broker, capital, sizing, or paper-placement authority.

## Authority Boundary

This plan is diagnostic and planning-only. It does not implement remediation, change replay behavior, relax production filters, promote candidates, change qualification, change governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.
