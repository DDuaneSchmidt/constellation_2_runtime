# AEGIS Generated Hypothesis Outcome Mark Certification Repair Spec v1

## Classification

- `NONE`: certified target-day mark is present, routed to outcome closure, lineage is green, and Package 020 reaches outcome or the next deterministic close blocker.
- `AUTHORITATIVE_MARK_SOURCE_MISSING`: no price row exists for the required mark symbol.
- `AUTHORITATIVE_MARK_SOURCE_STALE`: price exists but is stale, prior-day, provisional, or otherwise not certifiable for the target day.
- `MARK_CERTIFICATION_MISSING`: certifiable price exists but the paper ledger does not certify it.
- `MARK_ROUTING_MISSING`: certified ledger mark is not routed into paper outcome auto-closure.
- `EVIDENCE_LINEAGE_BROKEN`: Oil Shock mark routing succeeded but portfolio mark lineage remains incomplete.
- `CANDIDATE_DIAGNOSTICS_SEQUENCING_MISSING`: target-day candidate diagnostics remain absent after repair sequencing.

## Safety

The package never fabricates marks, outcomes, validation samples, research quality rows, trades, broker actions, allocation changes, close-rule changes, holding-period changes, or safety-gate changes.
