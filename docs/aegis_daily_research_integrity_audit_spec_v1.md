# Aegis Daily Research Integrity Audit Spec v1

## Producer
Command: `TARGET_DAY=<day> npm run aegis:daily-research-integrity-audit`.

The producer reads only day-scoped runtime truth artifacts and writes `daily_research_integrity_audit.v1.json`.

## Status Rules
`FAIL` means one or more BLOCKER issue rows exist. `WARN` means no blockers and one or more WARNING rows exist. `PASS` means no blockers and no warnings.

Blockers are limited to verified graph/audit safety failures and zero closed outcomes when closure-eligible or overdue positions exist. Missing entry marks, Oil Shock blocker mismatches, allocation visibility mismatches, stale candidate artifacts, producer failures, and REDESIGN/PAUSE sleeves are warnings unless verified graph policy says otherwise.

## Source Artifacts
Primary sources include candidate diagnostics, candidate contracts, candidate-to-paper lifecycle, paper position ledger, outcome registry, paper outcome auto-closure, exit recommendations, entry reference price certification, validation samples, research daily scorecard, Oil Shock candidate flow, generated hypothesis throughput, operator action queue, research allocation recommendation, research capital allocation, and verified runtime graph.

## UI Contract
`/api/aegis/operator/today` includes `daily_research_integrity_audit_v1`. The browser renders this object directly and does not infer blocker counts, warning counts, safety status, allocation visibility, or Oil Shock blocker state.
