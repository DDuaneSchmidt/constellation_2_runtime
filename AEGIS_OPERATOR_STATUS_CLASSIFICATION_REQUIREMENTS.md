# Aegis Operator Status Classification Requirements

## Purpose

Define how Aegis must classify and explain operator-facing status so the operator never sees ambiguous labels like `FAILED`, `BLOCKED`, or `PARTIAL` without knowing what happened, why it happened, what is affected, and what to do next.

## Context

The Positions page previously showed:

- Candidate Capture Status: `FAILED`
- Output candidates captured: `0`
- Ready for operator review: `0`
- Current session lineage count: `26`

That presentation did not explain whether the cause was no qualified candidates, empty sleeve output, missing signal evidence, market data failure, construction failure, contract generation failure, or a pipeline that did not run.

Operator-facing status must be classified, explanatory, and actionable.

## Core Principle

No operator-facing status may be ambiguous.

Every operator-facing status must answer:

1. What happened?
2. Why did it happen?
3. What is affected?
4. What should the operator do next?

## Forbidden Bare Statuses

These statuses must never appear alone in the main operator UI:

- `FAILED`
- `BLOCKED`
- `PARTIAL`
- `UNKNOWN`
- `STALE`
- `MISSING`

They may appear only if paired with:

- classification
- plain-English explanation
- affected object/count
- next action

## Required Status Object

Every operator-facing status panel must include:

```json
{
  "status": "FAILED|BLOCKED|PARTIAL|READY|COMPLETE|NO_ACTION_REQUIRED",
  "classification": "...",
  "summary": "...",
  "explanation": "...",
  "affected_count": 0,
  "affected_objects": [],
  "next_action": "...",
  "repair_command": null,
  "diagnostics_link": null
}
```

## Candidate Capture Classifications

Candidate capture must use one of these classifications:

- `OUTPUT_CANDIDATES_CAPTURED`
- `NO_CANDIDATES_QUALIFIED`
- `SLEEVE_OUTPUTS_EMPTY`
- `SIGNAL_EVIDENCE_MISSING`
- `SIGNAL_EVIDENCE_BLOCKED`
- `CONTRACT_GENERATION_BLOCKED`
- `MARKET_DATA_BLOCKED`
- `CONSTRUCTION_BLOCKED`
- `PIPELINE_EXECUTION_FAILED`
- `SESSION_NOT_RUN`
- `SESSION_STALE`
- `UNKNOWN_REQUIRES_DIAGNOSTICS`

## Required Candidate Capture Examples

### No Candidates Qualified

```text
Candidate Generation Complete

Session:
PAPER-2026-05-29-0950

Result:
No candidates qualified today.

Output candidates:
0

Rejected intents:
26

Action:
None required.
```

### Signal Evidence Missing

```text
Candidate Generation Failed

Session:
PAPER-2026-05-29-0950

Reason:
Signal evidence graph is missing for this session.

Affected:
26 candidate lineage rows.

Action:
Run signal evidence repair.
```

### Construction Blocked

```text
Candidate Construction Blocked

Session:
PAPER-2026-05-29-0950

Reason:
Candidates were captured but construction could not produce entry, stop, or quantity.

Affected:
23 candidates.

Action:
Review candidate construction diagnostics.
```

## Required Mapping Logic

For candidate capture:

If output candidate count is greater than `0`:

- status: `READY` or `COMPLETE`
- classification: `OUTPUT_CANDIDATES_CAPTURED`

If output candidate count is `0`, rejected-intent lineage count is greater than `0`, and signal evidence artifacts are current:

- status: `COMPLETE`
- classification: `NO_CANDIDATES_QUALIFIED` or `SLEEVE_OUTPUTS_EMPTY`
- next_action: `None required`

If signal evidence graph is missing:

- status: `FAILED`
- classification: `SIGNAL_EVIDENCE_MISSING`
- next_action: repair signal evidence

If market-data boundary reports blockers:

- status: `BLOCKED`
- classification: `MARKET_DATA_BLOCKED`
- next_action: repair market data inputs

If contract boundary reports blockers:

- status: `BLOCKED`
- classification: `CONTRACT_GENERATION_BLOCKED`
- next_action: review contract generation boundary

If construction boundary reports blockers:

- status: `BLOCKED`
- classification: `CONSTRUCTION_BLOCKED`
- next_action: review construction boundary

If session artifacts are stale:

- status: `STALE`
- classification: `SESSION_STALE`
- next_action: rerun current session pipeline

If classification cannot be determined:

- status: `UNKNOWN`
- classification: `UNKNOWN_REQUIRES_DIAGNOSTICS`
- next_action: open diagnostics

## UI Requirements

The Candidate Capture Confirmation panel must show:

- headline classification
- one-sentence explanation
- counts
- next action
- diagnostics link if relevant

The panel must not show only:

```text
FAILED
0 candidates
```

## Diagnostics Relationship

The main UI must summarize.

Diagnostics may contain:

- raw status
- artifact paths
- boundary reports
- command IDs
- source details

## Required Tests

Tests must prove:

- `FAILED` never appears alone.
- `BLOCKED` never appears alone.
- `0` output candidates with rejected-intent lineage and current artifacts displays `NO_CANDIDATES_QUALIFIED`, not `FAILED`.
- Missing signal evidence displays `SIGNAL_EVIDENCE_MISSING`.
- Market data blocker displays `MARKET_DATA_BLOCKED`.
- Contract blocker displays `CONTRACT_GENERATION_BLOCKED`.
- Construction blocker displays `CONSTRUCTION_BLOCKED`.
- Stale session displays `SESSION_STALE`.
- Unknown state displays `UNKNOWN_REQUIRES_DIAGNOSTICS` with diagnostics link.
- Candidate capture panel always includes `next_action`.

## Governance Rule

No new operator-facing status may be introduced unless it has:

- classification
- explanation
- affected count/object
- next action
- tests

This document is the authority for operator-facing status language in Aegis. If implementation behavior conflicts with this document, the document wins.
