# Lite To Research Feedback

Aegis Lite feedback enters Research Lab through `trade_outcome_attribution.v1` and optional manual feedback artifacts.

Flow:

1. Aegis Lite generates a recommendation and operator queue.
2. The operator records `manual_operator_decision.v1`.
3. If manually entered, the operator records `manual_execution_event.v1`.
4. Outcome attribution records skipped-trade, executed-trade, sleeve, edge cluster, governance, and operator execution quality.
5. `ingest_trade_outcome_attribution_to_research_v1.py` imports that feedback into Research Lab as result ledger entries, evidence packets where execution evidence exists, follow-up uppercase research tasks, and `research_inbox_item.v1` items for ambiguous observations.

The bridge is informational only. It does not mutate Aegis Lite runtime, alter the operator queue, submit broker orders, authorize trades, or promote sleeves.

Operator execution quality remains separate from sleeve and edge quality. Lite feedback can create research learning, but it cannot overwrite conclusions; later evidence must write a new `research_conclusion.v1` and supersede prior conclusions if needed.

Preserved feedback types:

- skipped-trade outcome
- executed-trade outcome
- governance adjustment effect
- operator slippage
- stop/effective protection notes when present in source artifacts
- sleeve signal quality
- edge cluster outcome
- implementation quality
- operator execution quality
