# Aegis Research Allocation Recommendation Design v1

## Design

The allocation recommendation producer consumes `aegis_hypothesis_decision_policy_v1` and existing research allocation evidence. It joins rows by `hypothesis_id`, maps decision recommendations to research-attention actions, and emits proposed deltas without mutating underlying allocations.

## Delta Model

The model is intentionally small:

- `INCREASE`: positive research-attention delta.
- `DECREASE`: negative research-attention delta.
- `PAUSE`: negative delta with David review.
- `RETIRE_REVIEW`: full research retirement review recommendation.
- `CAPITAL_REVIEW`: human-visible review point only.
- `HOLD`: zero delta.

## Human Control Point

Rows requiring David review are surfaced in the UI. The artifact is a control-point recommendation, not an execution mechanism.

## Safety Boundary

No broker execution, trade advice, live trading, autonomous execution, order management, real-capital allocation, or automatic real-world position management is implemented.
