# Aegis Oil Shock Candidate Flow Design v1

## Architecture
The builder reads deterministic workflow, throughput, paper setup, candidate state, candidate contracts, candidate-to-paper lifecycle, entry certification, exit policy, follow-through, and AI root-cause artifacts. It writes one research-only proof artifact.

## Decision Model
The proof first identifies the Oil Shock hypothesis by id or normalized display name. It then checks whether existing candidate artifacts already contain Oil Shock rows. If they do, the proof reports existing lifecycle progression only. If they do not, it classifies the absence using deterministic artifact presence, paper setup fields, required evidence fields, instrument universe, producer evidence, and policy paths.

## No-Bypass Rule
The proof never constructs candidates. Candidate creation remains owned by existing candidate-generation and candidate-to-paper lifecycle paths.

## UI Integration
The Research page consumes the proof artifact through the existing research-quality endpoint and renders a compact Oil Shock Candidate Flow row near the research quality/follow-through sections.

## Auditability
All source paths and hashes are recorded. Content hashes exclude generated timestamps. Safety flags make the research-only boundary explicit.
