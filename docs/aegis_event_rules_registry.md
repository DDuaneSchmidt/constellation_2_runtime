# Aegis Event Rules Registry

`event_rules_registry.v1` is the operator-readable source for Event Awareness business rules. The default registry lives at:

`governance/02_REGISTRIES/C2_EVENT_RULES_REGISTRY_V1.json`

Every monitored event type must have a visible rule:

- `PANIC_EXHAUSTION`
- `RECOVERY_FAILURE`
- `FAILED_BREAKOUT`
- `BREADTH_COLLAPSE`
- `DEFENSIVE_ROTATION`
- `VOLATILITY_SPIKE`
- `MACRO_EVENT_REACTION`

Each rule declares required inputs, trigger conditions, thresholds, severity/confidence logic, stale-data rules, tactical-review rules, validity-gate requirements, alert eligibility, enabled status, production status, research status, and owner notes.

Safety rules:

- Missing rule fails closed.
- Disabled rule does not trigger.
- Research-only rule cannot create an actionable alert.
- Rules do not submit trades, mutate EOD state, promote research, or create sleeves.
- `event_tactical_packet.v1` remains non-canonical and manual-execution-only.

The monitor snapshots the registry into runtime truth for each run so the evaluated rule version is replayable.
