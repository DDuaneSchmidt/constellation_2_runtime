# broker_fact_spine_v1.contract

owner: broker_fact_spine_v1
scope: broker observation only

Core 1 is the single governed broker fact spine for post-entry paper-trade observation.

Responsibilities:
- Raw broker evidence journal is the first durable write.
- Canonical broker fact ledger is append-only and derivable only from raw evidence plus governed rules.
- Canonical event identity must be deterministic across re-materialization from the same raw evidence set.
- Replay, reconnect, and gap semantics are first-class states.
- Attribution is a strict finite model: ATTRIBUTED, PARTIAL, AMBIGUOUS, FOREIGN, UNRESOLVED.
- Downstream trust enforcement is machine-readable and fail-closed.

Non-scope:
- No reconciled trade truth.
- No lifecycle truth.
- No action authority.
- No broker submit authority.

Invariants:
- No normalized fact may exist without raw provenance.
- Raw and normalized writes are append-only.
- Legacy broker-event mirrors are non-canonical and diagnostic only.
