---
id: C2_PAPER_SESSION_ENVELOPE_V1
title: "C2 Paper Session Envelope Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_admission
---

# C2 Paper Session Envelope Contract v1

## Purpose

This contract defines the frozen session envelope bound to a closed paper-session graph.

## Required meaning

The envelope must bind:

- session identity
- graph fingerprint
- dependency keys
- admitted sleeve scope
- allowed truth roots
- execution entrypoint
- contract fingerprints
- config fingerprint
- validity window

## Authority boundary

The envelope is the immutable execution boundary for an admitted session.

Execution outside the envelope is governed divergence.

## Canonical runtime instance path

`constellation_2/runtime/truth/reports/paper_session_envelope_v1/<DAY>/paper_session_envelope.v1.json`

## Fail-closed rules

If the envelope cannot prove its graph fingerprint, validity window, or admitted dependency set, it must fail closed.
