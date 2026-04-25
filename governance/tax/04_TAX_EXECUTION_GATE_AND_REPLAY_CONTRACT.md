# Tax Execution Gate And Replay Contract

Contract ID: `C2_TAX_EXECUTION_GATE_AND_REPLAY_CONTRACT_V1`

## Purpose

Define the non-invasive execution-facing tax validation seam, stale-decision handling, correction
impact indexing, and replay contract for the tax domain.

## Execution Boundary

Execution consumers may validate tax decisions at an artifact boundary. They must not recompute tax
logic.

The governed boundary is:

- execution candidate or intent artifact
- tax dependency validator
- gate result artifact
- execution eligible, review required, or blocked outcome

If a live control-plane seam is not explicitly proven safe, the tax domain must emit a journalable
gate artifact for later consumption instead of wiring directly into order placement.

## Gate Validation Rules

The execution gate must classify exact fingerprint deltas.

Truth-mismatch blockers:

- accepted fact set hash
- lot state hashes
- wash state hash
- account tax regime hash

Stale blockers:

- resolved policy set id
- ranking policy hash
- rounding policy hash
- algorithm version

Minimum governed gate statuses:

- `EXECUTION_ELIGIBLE`
- `EXECUTION_REVIEW_REQUIRED`
- `EXECUTION_BLOCKED_STALE`
- `EXECUTION_BLOCKED_POLICY`
- `EXECUTION_BLOCKED_TRUTH_MISMATCH`

## Decision-Time Truth And Current Corrected Truth

Replay must preserve:

- the fact set believed at decision time
- the current corrected fact set
- the original decision artifact
- the current recomputed decision artifact

Historical truth must remain queryable and must not be overwritten by later corrections.

## Correction Impact Indexing

Additive corrections must surface:

- superseded fact id
- correction fact id
- affected snapshot ids
- affected decision ids
- replay required boolean
- impact class

Governed impact classes:

- `none`
- `state_only`
- `decision_affecting`
- `report_affecting`

## Replay Contract

Replay is deterministic over:

- original accepted fact set
- current accepted fact set
- current snapshot
- current resolved policy set
- original dependency fingerprint
- original request payload
- governed decision algorithm version

Replay reports must disclose machine-readable divergence reason codes and whether the original
decision remains valid under corrected truth.
