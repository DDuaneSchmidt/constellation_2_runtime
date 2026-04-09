---
id: C2_NO_INTENTS_DAY_V1
title: "No Intents Day v1 Contract"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

## Purpose

`no_intents_day.v1.json` is the governed valid-zero declaration for a day where the active
source-authoritative alpha intent producers emitted no canonical intents.

It is a narrow upstream fact. It is not the day-start authority owner.

## Canonical location

- `constellation_2/runtime/truth/intents_v1/snapshots/<DAY_UTC>/no_intents_day.v1.json`

## Required semantics

- Must be written only on the authoritative source truth root.
- Must be schema-validated against
  `governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/no_intents_day.v1.schema.json`.
- Must not coexist with canonical intent files in the same day directory.
- May create the day directory only as part of writing this governed marker.

## Startup interaction

- `ops/tools/run_intents_day_completeness_v1.py` may accept this marker as the valid-zero path.
- Startup/day-start control remains fail-closed unless downstream control truth explicitly treats
  the resulting completeness state as acceptable.
