# Research Hypothesis

`research_hypothesis.v1` captures research ideas as durable first-class Research Lab objects.

It is the canonical source of truth for a single hypothesis before and during offline research. It records the thesis, edge family, behavioral state, expected regime, direction, holding period, instruments, rationale, expected behavior, failure conditions, invalidation conditions, related sleeves, related research references, confidence, status, source, and notes.

`hypothesis_registry.v1` is legacy compatibility only. New Research Lab work should write `research_hypothesis.v1`; legacy registry rows may be one-way adapted into this model.

Valid statuses are:

- `IDEA`
- `UNDER_INVESTIGATION`
- `BACKTESTING`
- `REPLAY_REVIEW`
- `OBSERVED_IN_MARKET`
- `VALIDATED_RESEARCH`
- `PROMOTION_CANDIDATE`
- `APPROVED_FOR_LITE`
- `REJECTED`
- `ARCHIVED`

Valid sources are:

- `CHATGPT_SEED`
- `MANUAL`
- `RESEARCH_LAB`

Hypotheses do not execute themselves. They do not authorize trades, mutate Aegis Lite, submit broker orders, enable transmit, or promote sleeves. Work is controlled by `research_task_queue.v1`.

Status transitions are driven by `apply_research_result_to_hypothesis_v1`, using evidence refs and result ledger entries. A result ledger entry cannot move a hypothesis directly to `APPROVED_FOR_LITE`; that requires a separate promotion artifact and human approval.

Seed ingestion must use explicit user/Codex-provided seed items only. Hidden conversational memory is not a valid source.
