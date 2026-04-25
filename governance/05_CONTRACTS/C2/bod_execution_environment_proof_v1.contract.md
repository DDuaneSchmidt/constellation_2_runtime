---
id: C2_BOD_EXECUTION_ENVIRONMENT_PROOF_V1
title: "BOD Execution Environment Proof V1"
status: DRAFT
version: 1
created_utc: 2026-04-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# BOD Execution Environment Proof V1

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/bod_execution_environment_proof_v1/<DAY>/bod_execution_environment_proof.v1.json`

Rules:
- this artifact is the governed proof that the BOD-critical Python execution substrate is valid in the owning runtime context
- canonical writer: `ops/tools/run_bod_execution_environment_proof_v1.py`
- canonical helper: `constellation_2/common/bod_execution_environment_proof_v1.py`
- it must record direct evidence for:
  - Python executable path
  - repo root
  - cwd
  - `PYTHONPATH`
  - `VIRTUAL_ENV`
  - leading `sys.path`
  - direct `constellation_2` import result
  - no-side-effect bridge proof invocation for `ops/tools/bridge_accounting_nav_v2_to_compat_v1.py`
- it must fail closed with `status=BLOCKED_BY_DEFECT` when the bridge proof subprocess is nonzero or the import substrate cannot be established
- `target_day_build_v1` must consume this artifact as a required binding input
- this artifact does not bypass policy or admission; it only proves the execution substrate needed by BOD-critical tooling
