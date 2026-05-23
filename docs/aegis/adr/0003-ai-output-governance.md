# ADR 0003: AI Output Governance

## Status
Accepted

## Context
AI may assist with interpretation and recommendation text, but Aegis must not let AI create facts, validate evidence, approve changes, or control runtime permissions.

## Decision
AI outputs are governed artifacts. They must include model/provider status, prompt hash when present, input artifact hashes, output hash, evidence citations, prohibited-claim checks, and approval constraints.

## Consequences
When no live AI provider path exists, Aegis records `ai_used=false` and `deterministic_fallback=true`. AI output is allowed only for interpretation, recommendation, and summary layers.

## Safety Constraints
AI cannot create facts, metrics, approvals, runtime permissions, or execution instructions.
