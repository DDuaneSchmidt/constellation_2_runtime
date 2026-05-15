# Research Task Queue

`research_task_queue.v1` controls what the offline Research Lab works on.

Tasks are separate from hypotheses. A hypothesis records the idea; a task records an explicit offline work request. No task means no research engine action.

Supported task types are:

- `DEFINITION_CHECK`
- `DATA_AVAILABILITY_CHECK`
- `REPLAY_ANALYSIS`
- `BACKTEST`
- `FORWARD_OBSERVATION`
- `FAILURE_MODE_REVIEW`
- `EXPECTANCY_REVIEW`
- `PROMOTION_REVIEW`

Supported task statuses are:

- `QUEUED`
- `RUNNING`
- `COMPLETED`
- `BLOCKED`
- `REJECTED`

The queue may reference required inputs and output artifact refs. It remains offline-only and cannot create trades, submit orders, mutate Aegis Lite, or grant promotion authority.

The repository still contains legacy lower-case task rows used by earlier Research Lab tooling. The schema accepts both row shapes for compatibility, but legacy rows are read-only compatibility. New runners must emit uppercase task rows only and must not generate new legacy task rows.

`run_research_lab_task_queue_v1.py` is the canonical offline executor for the new task model. It consumes queued uppercase tasks, loads explicit required inputs, writes `research_evidence_packet.v1`, appends `research_result_ledger.v1`, updates task status, and queues the next uppercase follow-up task when appropriate.
