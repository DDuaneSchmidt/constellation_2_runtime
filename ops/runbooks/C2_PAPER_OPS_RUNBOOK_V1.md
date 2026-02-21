---
id: C2_PAPER_OPS_RUNBOOK_V1
title: "Constellation 2.0 Paper Ops Runbook v1"
status: ACTIVE
version: 1
created_utc: 2026-02-15
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_ops
---

# Constellation 2.0 Paper Ops Runbook v1

This runbook is suitable for hostile review: it is deterministic, fail-closed, and evidence-based.

## Repo root (authoritative)

- `/home/node/constellation_2_runtime`

## Services (systemd --user)

- `c2-supervisor.service` (always-on): runs `ops/run/c2_supervisor_paper_v2.py`
- `c2-operator-gate.timer` (daily 00:05 UTC): runs the PASS/FAIL operator gate tool (v2 readiness surfaces)

## Source truth vs derived truth

### Source-of-truth (authoritative inputs)
- Source-of-truth submissions: `constellation_2/phaseD/outputs/submissions/` (flat by submission_id)

### Derived truth (authoritative outputs)
- Derived exec evidence truth (mirrored immutably): `constellation_2/runtime/truth/execution_evidence_v1/...`

### Canonical readiness surfaces (v2, pillars-aware)
Submission evidence and readiness are canonicalized through:

- Pillars decisions (preferred submission evidence surface):
  - `constellation_2/runtime/truth/pillars_v1r1/DAY/decisions/*.submission_decision_record.v1.json`

- Pipeline manifest v2 (pillars-aware pipeline completeness):
  - `constellation_2/runtime/truth/reports/pipeline_manifest_v2/DAY/pipeline_manifest.v2.json`

- Gate stack verdict v1 (single final verdict surface):
  - `constellation_2/runtime/truth/reports/gate_stack_verdict_v1/DAY/gate_stack_verdict.v1.json`

### Legacy surface (not required by default)
- Submission index is legacy and no longer required for readiness if pillars decisions exist.
- Supervisor default behavior does NOT generate submission index.
- If explicitly needed for backward compatibility, supervisor can be run with:
  - `--write_submission_index YES`

Legacy submission index path (if enabled):
- `constellation_2/runtime/truth/execution_evidence_v1/submission_index/DAY/submission_index.v1.json`

## Install / Update unit files (authoritative definitions are in repo)

Authoritative unit files live in:

- `ops/systemd/user/c2-supervisor.service`
- `ops/systemd/user/c2-operator-gate.service`
- `ops/systemd/user/c2-operator-gate.timer`

Install by copying into systemd user directory:

- `~/.config/systemd/user/`

## Commands (copy/paste)

### 1) Copy unit files into systemd user dir

```bash
set -euo pipefail
cd /home/node/constellation_2_runtime

cp -f ops/systemd/user/c2-supervisor.service ~/.config/systemd/user/c2-supervisor.service
cp -f ops/systemd/user/c2-operator-gate.service ~/.config/systemd/user/c2-operator-gate.service
cp -f ops/systemd/user/c2-operator-gate.timer ~/.config/systemd/user/c2-operator-gate.timer

ls -la ~/.config/systemd/user/c2-supervisor.service
ls -la ~/.config/systemd/user/c2-operator-gate.service
ls -la ~/.config/systemd/user/c2-operator-gate.timer
