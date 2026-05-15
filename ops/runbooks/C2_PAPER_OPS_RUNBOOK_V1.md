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

## Next Paper Attempt Checklist

Use this checklist for the next governed paper attempt. Check in order and stop at the first real defect.

### 1) Same-day governed market data landed

- Inspect:
  - `constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/market_data_snapshot_v1/dataset_manifest.json`
  - `constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/market_data_snapshot_v1/IWM/2026.jsonl`
  - `constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/market_data_snapshot_v1/SPY/2026.jsonl`
  - `constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/market_data_snapshot_v1/QQQ/2026.jsonl`
- Good:
  - manifest includes `IWM`, `SPY`, `QQQ`
  - each symbol has an exact same-day row
- No signal:
  - rows exist, engines later return `NO_INTENT`
- Real defect:
  - manifest missing a required symbol
  - same-day row missing for a governed symbol

### 2) Real engine execution confirmed

- Inspect:
  - `constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/monitoring_v1/engine_heartbeat_v1/<DAY>/C2_VOL_INCOME_DEFINED_RISK_V1/engine_heartbeat.v1.json`
  - `constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/monitoring_v1/engine_heartbeat_v1/<DAY>/C2_TREND_EQ_PRIMARY_V1/engine_heartbeat.v1.json`
  - `constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/monitoring_v1/engine_heartbeat_v1/<DAY>/C2_MEAN_REVERSION_EQ_V1/engine_heartbeat.v1.json`
- Good:
  - same-day heartbeat exists for all 3 engines
- No signal:
  - heartbeats exist, but no intent files are emitted
- Real defect:
  - missing heartbeat for a required engine after orchestrator run

### 3) Any real intent files created

- Inspect:
  - `constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/intents_v1/snapshots/<DAY>/`
- Good:
  - one or more `*.exposure_intent.v1.json` files from real engines
- No signal:
  - day directory exists and is empty or absent because all engines returned `NO_INTENT`
- Real defect:
  - real engine heartbeat exists but intent path shows simulator-shaped artifacts or other non-governed output

### 4) Any Phase C identity directories released

- Inspect:
  - `constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/phaseC_preflight_v1/<DAY>/`
- Good:
  - at least one identity subdirectory exists
- No signal:
  - no identity subdirectories because no real intents existed
- Real defect:
  - real intent exists, but only veto files appear and the veto reason indicates a missing governed dependency

### 5) Governed submit result

- Inspect:
  - `constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/reports/orchestrator_run_verdict_v2/<DAY>/.../orchestrator_run_verdict.v2.json`
  - `constellation_2/runtime/truth/reports/sleeve_rollup_v1/<DAY>/sleeve_rollup.v1.json`
- Good:
  - `A7A_GOVERNED_SUBMIT_V5` is `OK` and rollup advances beyond `ABORTED`
- No signal:
  - `A7A_GOVERNED_SUBMIT_V5_BLOCKING_FAIL` with no identity directories because no real intents existed
- Real defect:
  - identity directories exist, but governed submit still fail-closes on a narrower downstream reason

### Note on 2026-03-10

- `2026-03-10` was a legitimate no-signal day under the repaired governed path.
- No real entry intents should have existed.
- No Phase C identity directories should have existed.
- No governed submit should have been expected.

## Install / Update unit files (authoritative definitions are in repo)

IB Gateway ownership is intentionally separate from Aegis Lite. The Lite pivot is manual-only and broker-agnostic: no Gateway unit is part of the default Lite runtime, and `broker_required_for_runtime=false`.

The deprecated user unit `c2-ib-gateway.service` must not launch, stop, restart, reconnect, or kill Gateway/IBC processes. It should remain disabled and marker-only. A separate operator-started Gateway unit may be used only for explicit manual paper entry or later reconciliation, not as a scheduler-driven Lite dependency.

Do not use `c2-ib-gateway.service` state as Lite readiness. Broker/account evidence and submit-boundary artifacts belong to the legacy/deferred broker-enabled PAPER path and are not current Lite readiness authority.

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
