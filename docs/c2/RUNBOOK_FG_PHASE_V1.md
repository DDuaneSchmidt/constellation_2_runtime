---
doc_id: C2_RUNBOOK_FG_PHASE_V1
title: "Runbook: Bundle F + G Phase (Bootstrap, Audit-Grade)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Runbook: Bundle F + G Phase v1

This runbook is audit-grade and fail-closed.
All commands are deterministic and must terminate at a shell prompt.
No background execution. No implicit state.

---

# 0) Preconditions (must pass)

## 0.1 Repo root + governance preflight

**SAFE TO RUN**
```bash
set -euo pipefail
cd /home/node/constellation_2_runtime
./ops/governance/preflight.sh
echo "OK: preflight PASS"
