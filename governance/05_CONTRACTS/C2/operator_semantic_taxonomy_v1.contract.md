# operator_semantic_taxonomy_v1.contract.md

Purpose:
- govern one shared operator-facing semantic taxonomy for derived reports and summary surfaces
- keep semantic interpretation separate from severity, blocker codes, and underlying canonical truth ownership

Reference implementation:
- `constellation_2/common/operator_semantic_classifier_v1.py`

Authoritative semantic classes:
- `NOT_YET_MATERIALIZED`
- `BLOCKED_BY_UPSTREAM_PREREQUISITE`
- `PENDING_PROPAGATION`
- `MATERIALIZED_AND_FAILED`
- `FULLY_OBSERVED_AND_CONFIRMED`

Precedence:
1. when required facts are absent, the surface MUST classify as:
   - `NOT_YET_MATERIALIZED`, or
   - `BLOCKED_BY_UPSTREAM_PREREQUISITE` when already-materialized upstream facts prove that a prerequisite is withholding downstream progress
2. when required facts are present but propagation is not complete, the surface MUST classify as `PENDING_PROPAGATION`
3. when required facts are present and the surface is blocked or inconsistent, it MUST classify as `MATERIALIZED_AND_FAILED`
4. only when required facts are present and no blocking, failure, or propagation-pending condition remains may the surface classify as `FULLY_OBSERVED_AND_CONFIRMED`

Rules:
- this taxonomy governs derived operator-facing interpretation only
- it does not replace canonical runtime ledger truth, lifecycle truth, or any producer-owned facts
- it does not replace severity
- surfaces may retain local detail, blocker chains, and recommended actions, but top-level `semantic_status` must use this taxonomy when present
