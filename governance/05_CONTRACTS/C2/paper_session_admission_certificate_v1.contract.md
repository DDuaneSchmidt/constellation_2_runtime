---
id: C2_PAPER_SESSION_ADMISSION_CERTIFICATE_V1
title: "C2 Paper Session Admission Certificate Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_admission
---

# C2 Paper Session Admission Certificate Contract v1

## Purpose

This contract defines the legacy paper-session admission certificate compatibility surface.

## Authoritative meaning

This surface is no longer authoritative.

Paper-session authority is owned only by `paper_session_ledger_v1`.

The legacy admission certificate is superseded and must not gate execution or submit authority.

## Required meaning

The certificate must identify:

- session identity
- day
- envelope reference
- closure reference
- blocker ledger reference
- graph fingerprint
- issued time
- validity window

## Canonical runtime instance path

`constellation_2/runtime/truth/reports/paper_session_admission_certificate_v1/<DAY>/paper_session_admission_certificate.v1.json`

## Fail-closed rules

Any retained compatibility emission must derive from the canonical paper-session ledger and must not contradict it.

Expired, drifted, mismatched, or ledger-unbound certificates must fail closed.
