---
id: C2_OPPORTUNITY_REVIEW_SNAPSHOT_V1
title: "C2 Opportunity Review Snapshot Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_opportunity_review_snapshot
---

# opportunity_review_snapshot_v1

This contract governs the minimal Bundle 11 review snapshot and delta model.

Review snapshot law:
- a review snapshot is the governed ordered set of current opportunity rows for one day and one scope
- a review snapshot MUST preserve the exact ordering basis used by the kernel
- a review snapshot MAY carry resolved rows from the immediately prior snapshot

Delta law:
- `new`: opportunity id absent from the prior snapshot
- `changed`: same opportunity id present, but summary fingerprint changed
- `unchanged`: same opportunity id present with the same summary fingerprint
- `resolved`: prior opportunity id absent from the current snapshot
- `blocked_but_still_important`: current opportunity is blocked, remains visible, and review priority is `review_now`
- `monitor_only`: current opportunity remains current but does not require immediate review

Historical reconstruction law:
- a snapshot MUST reference the prior governed snapshot when present
- delta MUST be computed from the prior snapshot artifact, not recomputed from raw runtime history when a governed snapshot exists
