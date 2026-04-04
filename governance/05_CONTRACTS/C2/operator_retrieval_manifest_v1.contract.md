---
id: C2_OPERATOR_RETRIEVAL_MANIFEST_V1_CONTRACT
title: "C2 Operator Retrieval Manifest v1 Contract"
status: DRAFT
version: 1
created_utc: 2026-04-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Purpose
Audit the exact retrieval plan and artifact set used by a Batch 3 view or query response.

# Canonical path
- `constellation_2/runtime/truth/reports/operator_retrieval_manifest_v1/<DAY>/<ID>/operator_retrieval_manifest.v1.json`

# Rules
- canonical artifacts only
- explicit rejected artifacts and scope filters
- retrieval status must match presentation trust state
