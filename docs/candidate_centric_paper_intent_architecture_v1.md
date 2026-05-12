# Candidate-Centric PAPER Intent Architecture v1

## Summary

The candidate-centric PAPER architecture separates candidate visibility and shadow ranking from execution authority.

Near-term phases remain observational:
- Phase 1: candidate observability
- Phase 2: shadow candidate arbitration
- Phase 3: one-winner PAPER arbitration
- Phase 4: live-readiness hardening
- Phase 5: distributed multi-intent PAPER execution across scheduled runs

Phase 5 is not simultaneous basket execution. It is a governed future mode where each scheduled PAPER run may admit at most one new entry after independently passing all existing gates.

## Phase 5 Distributed Multi-Intent PAPER Model

Phase 5 should be defined as distributed entry selection across daily PAPER observation windows.

Rules:
- max 1 new PAPER entry per scheduled run
- max 1 new PAPER entry per sleeve per day
- max daily entries equals the number of enabled executable sleeves, currently 7
- once a sleeve receives a PAPER entry, that sleeve is excluded from new entries for the rest of the day
- each run ranks the remaining unused sleeves and candidates
- each selected entry must independently pass risk, authorization, kill-switch, and submit-boundary gates
- aggregate daily PAPER risk cap still applies
- unsupported paired sleeves remain non-executable until explicitly supported
- signal-only sleeves remain non-executable until explicitly supported
- no simultaneous multi-order basket execution

Recommended 7-run PAPER schedule:
- 09:31
- 10:15
- 11:00
- 11:45
- 13:30
- 14:30
- 15:30

## Why Not Basket Execution

Simultaneous multi-order basket execution adds complexity without enough current evidence:
- harder risk accounting
- harder authorization semantics
- harder kill-switch semantics
- harder reconciliation
- harder operator review
- higher risk of correlated entries
- higher chance that one weak sleeve contaminates the day

Distributed multi-intent PAPER execution keeps the audit trail simple: one scheduled run, one candidate ranking, one selected entry at most, one full gate stack per entry.

## Required State Tracking

A future Phase 5 implementation must track daily sleeve usage before ranking candidates:
- sleeve id
- selected candidate id
- entry timestamp
- entry run id
- authorization artifact refs
- risk artifact refs
- submit-boundary artifact refs
- aggregate daily risk consumed

If the daily sleeve usage artifact is missing, stale, or invalid, Phase 5 must fail closed and select no additional distributed entry.

## Per-Run Ranking

Each scheduled run should:
1. load the candidate manifest for the run
2. remove sleeves already used that day
3. remove non-executable candidates
4. preserve blocked, signal-only, and suppressed candidates as diagnostics
5. rank remaining executable candidates
6. select at most one candidate
7. require independent gate proof before any PAPER entry can proceed

Current `selected_intent_pointer` semantics remain unchanged until a separate governed implementation explicitly replaces them.

## Safety Boundaries

Phase 5 must not:
- submit simultaneous baskets
- bypass risk authorization
- bypass kill-switch state
- bypass submit-boundary state
- make dashboards authoritative
- make shadow arbitration authoritative by itself
- treat signal-only sleeves as executable
- treat paired sleeves as executable until paired execution is explicitly supported
- relax trading policy automatically

## Current Status

This document is design only. No code, timers, policy, runtime behavior, arbitration behavior, or execution wiring is changed by this architecture update.
