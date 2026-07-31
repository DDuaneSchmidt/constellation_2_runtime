# Aegis Research Quality Engine Design v1

## Pipeline Position

Evidence Artifacts -> Research Quality Engine -> Hard Gate Policy -> Hypothesis Decision Policy -> Research Allocation Recommendation -> Human-visible Control Point

## Design

The engine reads existing Aegis research truth artifacts and normalizes them by `hypothesis_id`. It computes fixed rule-based grades from observed fields such as linked sleeves, linked candidates, linked paper positions, validation samples, outcomes, statistical sufficiency, and source artifact freshness.

Hard gates are evaluated after base grades and before downstream policy. This prevents soft evidence from masking missing data, missing paper paths, duplicate hypotheses, stale artifacts, or nondeterministic outcome evidence.

## Deterministic Hashing

The engine hashes input artifacts and embeds those hashes in both top-level and per-row metadata. The artifact `content_hash` excludes `computed_at_utc` and generated timestamps so identical inputs produce stable content hashes across reruns.

## UI Boundary

The UI receives quality rows and renders them directly. It must not recompute grades, hard gates, recommendations, or review requirements.

## Non-Goals

The engine does not execute trades, allocate capital, submit broker orders, recommend trades, manage positions, or alter runtime safety policy.
