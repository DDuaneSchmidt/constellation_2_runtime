---
id: C2_PHASEC_README_V1
title: Constellation 2.0 Phase C — Offline Mapping + Submit Preflight + Evidence Writer
version: 1
status: DRAFT
type: phase_readme
created: 2026-02-14
authority_level: ROOT_SUPPORT
---

# Constellation 2.0 — Phase C (Offline Submit Boundary)

## 1. Purpose

Phase C makes the **submission boundary** executable in an **offline-only** manner:

- consumes local JSON inputs
- performs deterministic mapping (Intent → Plan → Binding)
- performs **SUBMIT preflight** without calling a broker
- writes audit-grade evidence artifacts with **single-writer** rules

Phase C is designed for hostile review:
- deterministic
- fail-closed
- reproducible with identical inputs

---

## 2. Inputs (Local JSON Files)

Phase C consumes exactly these inputs:

- `OptionsIntent v2`
- `OptionsChainSnapshot v1`
- `FreshnessCertificate v1`

All inputs MUST validate against C2 schemas under `constellation_2/schemas/`.

---

## 3. Outputs (Deterministic Evidence Artifacts)

Exactly one output path occurs.

### 3.1 Success path (SUBMIT_ALLOWED)

Outputs written to `--out_dir`:

- `order_plan.v1.json`
- `mapping_ledger_record.v1.json`
- `binding_record.v1.json`
- `submit_preflight_decision.v1.json` (decision = `ALLOW`)

### 3.2 Block path (fail-closed)

Outputs written to `--out_dir`:

- `veto_record.v1.json` only

No partial outputs are permitted.

---

## 4. Offline Submit Preflight (What It Does)

Preflight revalidates, at the SUBMIT boundary:

- schemas validate (Draft 2020-12)
- canonical JSON hashing succeeds (SHA-256)
- freshness is valid at `eval_time_utc`:
  - `valid_from_utc <= eval_time_utc <= valid_until_utc`
- snapshot binding is intact:
  - `freshness_certificate.snapshot_hash` matches chain snapshot canonical hash
  - `freshness_certificate.snapshot_as_of_utc` matches chain snapshot `as_of_utc`
- binding chain is intact:
  - intent → plan → mapping ledger → binding hashes match

If any check fails:
→ emit `VetoRecord v1` (boundary = `SUBMIT`) and stop.

---

## 5. Deterministic Time

Phase C is forbidden from using wall-clock time.

The CLI requires:

- `--eval_time_utc` (ISO-8601 with Z suffix)

This value is used as:
- mapping `created_at_utc`
- submit decision `created_at_utc`
- veto `observed_at_utc`

---

## 6. Pricing Determinism and Tick Size

The current C2 Design Pack requires deterministic pricing, but does not define tick-size policy.

Therefore Phase C requires an operator-supplied deterministic input:

- `--tick_size` (decimal string, e.g. `0.01`)

If missing or invalid:
→ veto `C2_PRICE_DETERMINISM_FAILED`.

This is intentionally explicit to avoid implicit defaults.

---

## 7. How to Run (Offline)

Example (using acceptance samples):

```bash
set -euo pipefail
cd /home/node/constellation_2_runtime

rm -rf /tmp/c2_phasec_out || true

python3 constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py \
  --intent constellation_2/acceptance/samples/sample_options_intent.v2.json \
  --chain_snapshot constellation_2/acceptance/samples/sample_chain_snapshot.v1.json \
  --freshness_cert constellation_2/acceptance/samples/sample_freshness_certificate.v1.json \
  --eval_time_utc 2026-02-13T21:52:00Z \
  --tick_size 0.01 \
  --out_dir /tmp/c2_phasec_out

ls -la /tmp/c2_phasec_out
