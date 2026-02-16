---
id: C2_PHASED_SUBMISSION_RUNNER_V1
title: "Constellation 2.0 Phased Submission Runner v1 (Fail-Closed Paper Trading Entry Point)"
status: ACTIVE
version: 1
created_utc: 2026-02-16
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Constellation 2.0 — Phased Submission Runner v1 (Canonical)

## 1. Objective

This contract defines the **single canonical entrypoint behavior** for producing broker submissions in paper trading.

The submission runner must:

- operate deterministically from runtime truth inputs,
- produce immutable broker-boundary evidence artifacts into the True Evidence Spine,
- fail closed on missing prerequisites,
- and produce day-level manifests proving what was attempted and what completed.

This is the contract quants/risk committees use to answer:  
**“Did the system submit trades, under what authorization, and what did the broker report?”**

## 2. Canonical inputs (required spines)

The submission runner MUST read inputs exclusively from runtime truth:

- Intents snapshot(s) for `{DAY_UTC}`
- Phase C preflight decisions for `{DAY_UTC}`
- OMS decisions for `{DAY_UTC}`
- Allocation outputs for `{DAY_UTC}`

If any required upstream spine is missing, the runner MUST FAIL-CLOSED and MUST NOT submit any broker orders.

The runner MUST be able to produce a deterministic explanation of why it failed (via failure artifacts in the evidence spine).

## 3. Canonical outputs (required)

### 3.1 True Evidence Spine outputs
The runner MUST write into:

- `constellation_2/runtime/truth/execution_evidence_v1/`

and MUST conform to contract:

- `C2_TRUE_EVIDENCE_SPINE_V2`

### 3.2 Day manifest outputs
For each `{DAY_UTC}` run, the runner MUST produce a day manifest under:

- `execution_evidence_v1/manifests/{DAY_UTC}/`

The day manifest MUST:

- enumerate the deterministic set of submission ids,
- include counts by status,
- and assert completeness of each submission directory.

The exact manifest file naming may be implementation-defined, but it MUST be immutable and deterministic, and it MUST be referenced by any "latest" pointer validation.

### 3.3 Failure outputs
On failure, the runner MUST write:

- `execution_evidence_v1/failures/{DAY_UTC}/failure.json`

and MUST exit non-zero.

## 4. Phased execution model (deterministic stages)

The runner MUST execute the following stages in order, recording stage outcomes deterministically:

1) **Load & validate prerequisites**  
   - Verify required upstream truth roots exist for `{DAY_UTC}`.
   - Verify schema validity for each required upstream artifact.

2) **Assemble candidate submissions**  
   - Determine which OMS-approved, allocated items become submission candidates.
   - Deterministically derive `{SUBMISSION_ID}` for each candidate.

3) **Risk gates (fail-closed)**  
   - Apply required risk gates (risk budget gate, drawdown throttle gate, etc).
   - If any required gate input is missing or invalid: FAIL-CLOSED.
   - If a candidate is vetoed by risk: write evidence (binding + broker submission record indicating VETO) and do not submit.

4) **Broker what-if (if configured)**  
   - If what-if is required by policy, the runner MUST execute it and record the result.
   - If what-if fails: FAIL-CLOSED (no submit).

5) **Broker submit**  
   - Submit orders to IB paper.
   - The runner MUST prove the routed account is DU* (paper).
   - If routing account is missing/invalid: FAIL-CLOSED.

6) **Broker observation capture**  
   - Record broker acknowledgement / rejection and order ids.
   - Capture execution events as available within the observation window.

7) **Write immutable evidence artifacts**  
   - For each candidate, write the full required submission directory artifacts (§C2_TRUE_EVIDENCE_SPINE_V2).
   - Produce the day manifest.

No stage may silently skip. Every skip must be explicit in evidence (via manifest counts + reason codes).

## 5. Fail-closed invariants (institutional requirements)

The runner MUST NOT submit any broker orders if any of the following are true:

- required upstream truth inputs are missing for `{DAY_UTC}`,
- any required input artifact fails schema validation,
- risk budget inputs required for gating are missing or invalid,
- IB paper account routing cannot be proven DU*,
- evidence artifacts cannot be written atomically and immutably,
- or the day manifest cannot be produced.

In all such cases, the runner MUST produce a failure artifact and exit non-zero.

## 6. Non-authoritative outputs prohibition

Any outputs outside runtime truth (e.g., under `constellation_2/phaseD/outputs/`) MUST NOT be used as evidence of broker activity in audits.

If such outputs exist, they may be treated as debug logs only.

Audit-grade evidence is runtime truth only.

## 7. Operational accountability (what hostile reviewers will ask)

For any `{DAY_UTC}`, the runner MUST allow auditors to answer:

- How many candidates were assembled?
- How many were vetoed by risk gates, with which reasons?
- How many were submitted, accepted, rejected?
- What broker order ids resulted?
- For each broker order id, what execution events and fills were observed?
- Are there any missing required artifacts per submission id?
- Is the day manifest internally consistent with the filesystem?
- Does any consumer rely on `latest.json` without validation (prohibited)?

## 8. Change control

This runner contract is canonical. Any change that alters:

- candidate selection,
- submission id derivation,
- gating behavior,
- broker boundary behavior,
- or evidence structure

requires:

- governance update,
- explicit contract version bump,
- and deterministic replay proof on a fixed historical day key.
