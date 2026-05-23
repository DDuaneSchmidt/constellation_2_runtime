# ADR 0004: Human Approval Required For Adaptive Governance

## Status
Accepted

## Context
Adaptive research and sleeve governance may identify improvements, risks, and priorities, but automated mutation would blur analysis and control authority.

## Decision
Every adaptive governance recommendation requires explicit human approval before any operational change. The approval ledger is append-only and records proposed, approved, rejected, deferred, expired, superseded, and audited events.

## Consequences
Aegis can recommend research prioritization, sleeve watch status, trust review, and failure investigation, but cannot promote, demote, retire, allocate capital, or execute trades automatically.

## Safety Constraints
No broker execution, autonomous execution, live trading, automatic capital allocation, or automatic sleeve mutation is allowed.
