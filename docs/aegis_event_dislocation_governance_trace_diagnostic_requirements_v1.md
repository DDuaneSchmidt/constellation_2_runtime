# AEGIS Event Dislocation Governance Trace Diagnostic Requirements v1

T07A is a diagnostics-only package for `C2_EVENT_DISLOCATION_V1`.

It must prove the exact governed candidate field value or missing value that causes candidate contract rejection after T06 found `INSTRUMENT_TYPE_NOT_GOVERNED`.

The diagnostic must not change candidate construction, candidate contracts, governance rules, risk policy, thresholds, scoring, signals, allocation, broker behavior, live trading, or autonomous execution.

Required output:

`truth/reports/aegis_event_dislocation_governance_trace_diagnostic_v1/<TARGET_DAY>/event_dislocation_governance_trace_diagnostic.v1.json`

For each rejected candidate attempt, the report records raw signal fields, candidate draft fields, exact candidate contract input, governance registry lookup, missing fields, invalid fields, rejection codes, exact rejection stage/message, deterministic failure classification, owner, and whether David action is required.
