# Aegis Daily Research Integrity Audit Design v1

## Data Flow
Authoritative runtime truth artifacts feed `aegis_daily_research_integrity_audit_v1`. The Command Center envelope attaches the artifact. The Command Center renders a compact Daily Integrity section below Today's Research Result and keeps full diagnostic data collapsed.

## Integrity Rows
The producer normalizes each detected defect into an issue row so UI and operators can inspect the affected artifact, affected id, expected value, actual value, source path, and next step without consulting code.

## Read-Only Boundary
The producer performs no repairs, allocations, candidate creation, paper observation creation, exit actions, validation decisions, broker calls, safety-gate changes, or hypothesis-state mutations. Safety fields are hard-coded false/disabled and verified by regression tests.

## Current-Day Expectations
For `TARGET_DAY=2026-06-01`, the audit detects Oil Shock blocker mismatches if scorecard/UI says `MISSING_DATA` while the Oil Shock flow says `PRODUCER_MISSING`; traces missing entry marks if present; reports whether open observations were exit-evaluated; treats zero closed outcomes as informational unless closure-eligible or overdue positions exist; detects allocation UI mismatch when recommendations exist but displayed decisions are zero; and lists REDESIGN/PAUSE sleeves as warnings.
