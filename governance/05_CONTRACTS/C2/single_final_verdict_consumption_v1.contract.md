---
id: C2_SINGLE_FINAL_VERDICT_CONSUMPTION_CONTRACT_V1
title: "C2 Single Final Verdict Consumption Contract v1"
status: DRAFT
version: 1
created_utc: 2026-02-20
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Single Final Verdict Consumption Contract v1

## Purpose
Eliminate parallel final-decision authority surfaces by requiring **exactly one** consumed daily final verdict artifact.

This contract governs **consumption**, not production. Legacy artifacts may exist during cutover, but consumers must not treat them as final decision authority.

## Canonical final verdict artifact (authoritative)
For each `DAY_UTC`, the only artifact that may be consumed as the *final* decision surface is:

- `constellation_2/runtime/truth/reports/gate_stack_verdict_v1/<DAY>/gate_stack_verdict.v1.json`

## Prohibited final-decision consumers (non-authoritative)
The following artifacts must **not** be consumed as final-decision authority after this contract is active:

- `constellation_2/runtime/truth/reports/operator_gate_verdict_v1/<DAY>/operator_gate_verdict.v1.json`
- `constellation_2/runtime/truth/reports/operator_gate_verdict_v2/<DAY>/operator_gate_verdict.v2.json`
- `constellation_2/runtime/truth/reports/operator_gate_verdict_v3/<DAY>/operator_gate_verdict.v3.json`
- `constellation_2/runtime/truth/reports/capital_risk_envelope_v1/<DAY>/capital_risk_envelope.v1.json`
- `constellation_2/runtime/truth/reports/capital_risk_envelope_v2/<DAY>/capital_risk_envelope.v2.json`
- `constellation_2/runtime/truth/reports/reconciliation_report_v2/<DAY>/reconciliation_report.v2.json`
- `constellation_2/runtime/truth/reports/reconciliation_report_v3/<DAY>/reconciliation_report.v3.json`

These artifacts may remain as **evidence** during migration but are not authoritative final verdict surfaces.

## Required consumer behavior (fail-closed)
Any consumer that controls entry/exit permission MUST:

1) Read `gate_stack_verdict_v1` for the specified `DAY_UTC`
2) If the file is missing, unreadable, or schema-invalid → fail closed (deny entries)
3) If `status != PASS` → fail closed (deny entries)
4) If any `gates[]` entry with `required=true` has `status != PASS` → fail closed (deny entries)

## Enforcement
Repo preflight MUST fail if the kill switch implementation references prohibited final-decision surfaces or fails to reference `gate_stack_verdict_v1`.

Enforcement script:
- `ops/governance/preflight_require_kill_switch_uses_gate_stack_verdict_v1.sh`

## Rationale
This prevents split-brain “which verdict is final?” ambiguity and constrains failure branches by ensuring exactly one daily verdict input is authoritative.
