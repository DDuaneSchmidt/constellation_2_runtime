# research_conclusion.v1

`research_conclusion.v1` preserves durable research knowledge derived from evidence packets and result ledger entries.

Conclusions are immutable research memory. If later evidence changes the view, write a new conclusion and supersede the prior one. Do not overwrite prior conclusions.

Required lineage:
- `hypothesis_id`
- supporting evidence refs
- result ledger refs
- methodology version
- data snapshot refs where available
- code version
- artifact lineage
- reason codes
- reproducibility notes

Conclusions cannot authorize Lite usage, promotion, trades, risk sizing, broker actions, or runtime mutation.
