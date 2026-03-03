---
id: C2_ORCHESTRATOR_V2_CONTRACT
title: "Constellation 2.0 — Orchestrator V2 Contract (Robust, Attempt-Scoped, Pointer-Promoted)"
status: DRAFT
version: 1
created_utc: 2026-03-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - orchestrator
  - determinism
  - attempt_scoped
  - pointer_index
  - replay_integrity
  - fail_closed
  - operator_reliability
  - systemd
  - single_account
---

# Constellation 2.0 — Orchestrator V2 Contract

## 1. Objective

Orchestrator V2 is the **operator-first** daily pipeline coordinator for Constellation 2.0.

It MUST:
- Complete in PAPER and LIVE without “daily babysitting”.
- Emit a **run verdict artifact** for every invocation (even FAIL).
- Preserve deterministic replay.
- Make **no changes to strategy logic** (engines and signals are untouched).

It MUST NOT:
- Brick on expected absence (no intents, no fills, insufficient history).
- Brick on rerun due to immutable day-keyed rewrite conflicts.

## 2. Authority

- Repo root (authoritative): `/home/node/constellation_2_runtime`
- Canonical truth root: `constellation_2/runtime/truth/**`
- Governance authority: `governance/**`

This contract is a canonical governance document.

## 3. Hard non-negotiables

1) Single-account topology is enforced:
   - All sleeves/engines MUST use **IB account DUO847203** in single-account mode.
   - Any mismatch is a hard safety breach (ABORTED + non-zero exit).

2) UI is read-only and deterministic:
   - Orchestrator produces artifacts only; UI consumes artifacts only.
   - No raw JSON in default UI views (UI concern, but orchestrator must produce UI-ready bundles).

3) No canonical overwrites:
   - Orchestrator MUST NOT rewrite immutable canonical day-keyed artifacts.
   - Canonical “current” for V2 families MUST be derived via pointer indices, not filesystem scans.

## 4. Definitions

### 4.1 attempt_id (deterministic)
Each invocation mints an attempt id:

`attempt_id = <day_utc>__<produced_utc>__<git_sha>__<mode>__<symbol>__<ib_account>`

Where:
- day_utc: `YYYY-MM-DD`
- produced_utc: ISO-8601 UTC Z timestamp supplied to orchestrator (observational; not used as canonical produced_utc)
- git_sha: `git rev-parse HEAD`
- mode: `PAPER|LIVE`
- symbol: run symbol (e.g. SPY)
- ib_account: must equal DUO847203 in single-account mode

### 4.2 Attempt-scoped outputs
All V2 stage outputs are written to attempt-scoped directories:

`.../<family>/<day_utc>/attempts/<attempt_id>/...`

Attempt artifacts are immutable per-bytes (never rewritten).

### 4.3 Canonical pointer index (append-only)
Each V2 family uses an append-only canonical pointer index:

`.../<family>/<day_utc>/canonical_pointer_index.v1.jsonl`

Rules:
- Append-only JSONL
- A pointer entry references a specific attempt_id output location
- If a pointer entry already exists for the same attempt_id, it is treated as EXISTS (idempotent)

Canonical heads:
- Display head: highest pointer_seq (any status)
- Authority head: highest pointer_seq where `authoritative=true` and `status=PASS`

Consumers MUST NOT derive canonical heads by directory scanning.

### 4.4 Stage classification matrix
Every stage is classified by:

- required_for_mode:
  - PAPER: bool
  - LIVE: bool
- required_if_activity: bool
  - required only if activity is observed (authorized intents/submissions/fills)
- blocking: bool
  - true only for safety-critical stages

## 5. Safety breach policy (fail-closed only for real breaches)

A “safety breach” is any of:
- Feed attestation invalid / tamper / missing required attestation
- Liquidity / slippage envelope gate FAIL (when required)
- Correlation envelope gate FAIL (when required)
- Convex shock / systemic risk gate FAIL (when required)
- Capital risk envelope FAIL (when required)
- Broker integrity / reconciliation tamper / fatal corruption
- Account mismatch vs DUO847203
- Fatal schema corruption of required artifacts

Safety breach => run verdict ABORTED and non-zero exit.

## 6. Run verdict artifact (mandatory)

For every invocation, orchestrator MUST emit:

`constellation_2/runtime/truth/reports/orchestrator_run_verdict_v2/<day_utc>/<attempt_id>/orchestrator_run_verdict.v2.json`

Verdict states:
- PASS
- DEGRADED
- FAIL
- ABORTED

Semantics:
- PASS: all required stages succeeded; no safety breaches
- DEGRADED: missing/failed optional or activity-gated stages only; no safety breaches
- FAIL: required non-safety stages failed; no safety breaches
- ABORTED: safety breach OR fatal corruption OR account mismatch

## 7. Service exit policy

Orchestrator V2 exit codes:
- Exit 0 for PASS / DEGRADED / FAIL
- Exit non-zero ONLY for ABORTED (including safety breach classes)

Rationale:
- Systemd services MUST NOT enter FAILED loops for normal FAIL/DEGRADED runs.
- ABORTED indicates an operator-actionable safety breach.

## 8. Replay derivation (configuration-derived, attempt-scoped)

Replay integrity MUST be derived from the attempt manifest, not a static “wish list”.

Replay hashing scope for an attempt includes:
- The produced attempt manifest (list of executed stages + outputs)
- Canonical pointer indices used/advanced by the attempt
- Attempt artifacts produced by this attempt (only those actually produced)

If a stage is not executed or is optional and skipped, it MUST NOT be expected.

Replay result MUST be embedded in orchestrator_run_verdict.v2.json.

## 9. Prohibited behaviors (hard violations)

Orchestrator V2 MUST NOT:
- Overwrite immutable canonical artifacts in legacy paths
- Use filesystem ordering (“latest dir”, lexicographic scans) to determine canonical
- Treat optional missing stages as fatal
- Fail the service for normal FAIL/DEGRADED outcomes
- Allow any account other than DUO847203 under single-account mode
- Embed wall-clock produced_utc in canonical day-scoped artifacts (canonical day-scoped produced_utc MUST remain `<DAY>T00:00:00Z` where applicable)

## 10. Acceptance criteria (contract-level)

A) Reruns never brick due to immutable rewrite conflicts.
B) Run verdict artifact is always produced for PASS/DEGRADED/FAIL.
C) systemd does not enter FAILED state for FAIL/DEGRADED.
D) Safety breaches hard-stop: ABORTED + non-zero exit.
E) Replay hashing derives from attempt manifest (no missing optional expectations).
F) DUO847203 enforcement is proven and non-bypassable.
G) Canonical heads derive from append-only pointer indices only.
