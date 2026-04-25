# broker_fact_identity_v1.contract

owner: broker_fact_identity_v1
scope: deterministic canonical event identity for Core 1 fact classes

Rules:
- Re-materializing from the same raw evidence set must yield the same canonical_event_identity values.
- source_event_identity must be stable across replay, reconnect, and legacy-mirror re-ingest for the same broker callback payload class.
- duplicate_classification and replay_classification must be deterministic.
- Missing or conflicting broker timestamps must not break identity stability.
- Ordering precedence is governed by observed broker timestamp, then source sequence number, then source event identity.
