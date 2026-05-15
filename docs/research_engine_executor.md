# Research Engine Executor

`run_research_lab_task_queue_v1.py` is the deterministic offline executor for the new Research Lab task model.

Inputs:

- one or more queued uppercase `research_task_queue.v1` rows
- explicit `required_inputs`
- the referenced `research_hypothesis.v1`
- optional fixture JSON for deterministic sample metrics

Outputs:

- `research_evidence_packet.v1`
- appended `research_result_ledger.v1` entries
- updated task status
- updated `research_hypothesis.v1`
- queued uppercase follow-up tasks when appropriate

Supported initial task types:

- `DEFINITION_CHECK`
- `DATA_AVAILABILITY_CHECK`
- `REPLAY_ANALYSIS`
- `BACKTEST`
- `FAILURE_MODE_REVIEW`
- `EXPECTANCY_REVIEW`

The executor is offline-only. It does not read broker state, submit orders, enable transmit, mutate Aegis Lite runtime, write conclusions automatically, or promote sleeves.

Fixture metrics may be used until real replay/backtest adapters are attached. Fixture output must still be written as structured evidence and result ledger entries, not as placeholder completion.

Evidence and result output includes methodology version, code version, artifact lineage, reason codes, and reproducibility notes so later conclusions can be audited.
