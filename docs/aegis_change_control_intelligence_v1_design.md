# Aegis Change Control Intelligence Layer V1 Design

## Architecture

```text
Evidence Snapshot
    -> Deterministic Advisor
    -> AI Reviewer
    -> Human Decision
    -> Change Control
```

## Storage Locations

Artifacts are written under `truth/reports`:

* `aegis_change_control_evidence_snapshot_v1/<day>/change_control_evidence_snapshot.v1.json`
* `aegis_change_control_advisor_score_v1/<day>/change_control_advisor_score.v1.json`
* `aegis_change_control_ai_review_v1/<day>/change_control_ai_review.v1.json`

## Artifact Generation Flow

The builder reads the Change Control register and report, hashes the register, freezes the current evidence snapshot, scores records deterministically, and then emits an advisory-only AI review artifact derived from the snapshot and score.

## Deterministic Scoring Flow

Scores are computed from severity, priority, blocker role, parent/child role, dependency impact, validation gaps, safety tags, and strategic rank. The top recommendation is the highest score with stable tie-breaking by priority score and record ID.

## AI Review Flow

The AI reviewer in V1 is a constrained generated review. It explains the deterministic score and produces suggested notes/prompts. It never reads live-changing files directly and never mutates Change Control.

## UI Integration

A read-only `/aegis-change-control-lab` route shows recommended next actions, blocked parents, records needing decision, highest risk items, stale/contradictory records, and copyable suggested Codex prompts.

## Relationship to Change Control Register

The register remains source of truth. The intelligence artifacts are derived evidence and advisory reports. David remains the decision authority.

## How This Avoids AI as Source of Truth

AI consumes only frozen snapshots and advisor scores. Scores are deterministic. Any decision must be recorded through the governed decision workflow.

## How This Avoids Opaque AI Project Management

The score components are visible per record, recommendations include source evidence references, and stale snapshot detection prevents hidden live drift.

## Long-Term Support

This layer gives Aegis a governed product-memory assistant: it can explain what to fix next while preserving evidence-driven validation and human authority.
