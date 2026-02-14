---
id: C2_PHASED_PAPER_BROKER_RUNBOOK_V1
title: Constellation 2.0 Phase D Runbook — Paper Broker Integration + Execution Lifecycle Truth Spine
version: 1
status: DRAFT
type: runbook
created: 2026-02-14
authority_level: ROOT_SUPPORT
---

# Constellation 2.0 — Phase D (Paper Broker Integration)

## 1. Purpose

Phase D introduces the broker execution boundary for C2:

- PAPER-only broker adapter (Interactive Brokers via ib_insync)
- Deterministic WhatIf margin gate
- RiskBudget enforcement
- Idempotent submission control
- Immutable evidence outputs:
  - BrokerSubmissionRecord v2
  - ExecutionEventRecord v1 (when broker IDs exist)
  - or VetoRecord v1 (fail-closed)

Phase D is designed for hostile review:
- deterministic
- fail-closed
- single-writer truth artifacts
- no silent failures

---

## 2. Preconditions

### 2.1 Repository

Repo root must be:

/home/node/constellation_2_runtime

### 2.2 Virtual Environment

Phase D venv location:

constellation_2/.venv/

Install dependencies (governed file):

```bash
set -euo pipefail
cd /home/node/constellation_2_runtime
constellation_2/.venv/bin/python -m pip install -r constellation_2/phaseD/inputs/requirements_phaseD_venv_v1.txt
