# Aegis Lite Manual Feedback Loop

Aegis Lite remains manual-execution-first. The feedback layer records what the operator decided, what was manually entered, what positions exist afterward, whether protective stops exist, and how recommendations performed.

This layer is observational. It does not submit orders, enable transmit, automate fills, or require IB execution authority.

## Flow

1. Aegis Lite produces the EOD report and manual execution queue.
2. The operator decides per candidate: enter, skip, modify, watchlist, or reject.
3. If the operator manually enters a trade in IB paper, the entry is captured as a manual execution event.
4. The operator or CSV/import process records current positions and protective stop status.
5. The next report warns on missing protection and includes current exposure.
6. Forward outcome attribution evaluates all recommendations, including skipped trades.

## Safety Rules

- All artifacts are deterministic and schema validated.
- Broker submit and transmit automation remain false.
- Unsupported manual trade classes fail closed as `UNSUPPORTED_MANUAL_EXECUTION`.
- Open manual positions without confirmed stops create report warnings.
- Missing protective stops, unsupported execution, missing queue, missing recipe, malformed status, and unprotected open positions are do-not-trade blockers for manual readiness.
- Only explicit `PASS` passes data/governance gates; unknown, warning, review-required, missing, or malformed statuses block readiness.
- Sleeve, edge, governance, and operator execution quality are measured separately.
