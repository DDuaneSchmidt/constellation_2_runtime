---
id: C2_AUTHORIZED_POST_ENTRY_PAYLOAD_CONTRACT_V1
title: "C2 Authorized Post-Entry Payload Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_authorized_payload
---

# C2 Authorized Post-Entry Payload Contract V1

## Purpose

`authorized_post_entry_payload.v1` is the immutable payload freeze emitted by Core 4 when and only when authorization succeeds.

It answers:

- what exact payload shape downstream transport is allowed to attempt

## Required content

The artifact must carry:

- immutable payload identity
- sealed request reference
- boundary verdict reference
- exact approved payload fields
- payload fingerprint/checksum
- rule version
- generation timestamp

## Relationship to request and boundary verdict

The payload must be traceable to:

- exactly one sealed request
- exactly one `post_entry_submit_boundary.v1` verdict

If no authorized verdict exists, no authorized payload artifact may exist.

## Transport prohibition

Transport may perform only governed lossless translation after authorization.

Transport must not:

- change action semantics
- change approved quantity
- change approved linkage target
- substitute account, sleeve, or client id
- silently add missing parameters

## Immutability

The artifact is immutable after emission.
