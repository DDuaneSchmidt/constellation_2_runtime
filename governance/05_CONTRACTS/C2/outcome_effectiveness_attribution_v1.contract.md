---
id: C2_OUTCOME_EFFECTIVENESS_ATTRIBUTION_V1
title: "C2 Outcome Effectiveness and Attribution Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_outcome_plane
---

# outcome_effectiveness_attribution_v1

Conservative classification law:
- effectiveness MUST NOT imply attribution automatically
- tax effect MUST NOT imply net portfolio benefit automatically
- missed-opportunity claims MUST remain conservative
- conflicting upstream influences MUST reduce claim strength rather than inflate it

Allowed effectiveness states:
- `helpful`
- `neutral`
- `harmful`
- `protective`
- `missed`
- `unproven`

Allowed attribution states:
- `direct`
- `partial`
- `bounded`
- `unsupported`

First-wave conservative rules:
- blocked states MAY be classified as `protective` only when a governed blocker basis exists and realized execution absence is observed in the bounded comparison window
- tax-aware outcomes MAY report realized tax-related observation, but MUST NOT claim net portfolio benefit without stronger governed evidence
- when no explicit linkage exists, attribution MUST remain `unsupported`

