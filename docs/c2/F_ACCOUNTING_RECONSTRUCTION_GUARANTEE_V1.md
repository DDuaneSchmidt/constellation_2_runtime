---
id: C2_F_ACCOUNTING_RECONSTRUCTION_GUARANTEE_V1
title: "Bundle F — Accounting Reconstruction Guarantee (Hostile Review Grade)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
truth_root: constellation_2/runtime/truth
---

# 1. Guarantee Statement

Bundle F provides a **Reconstruction Guarantee**:

Given the immutable Bundle F outputs for a day `D`, an auditor can reconstruct the exact same output bytes by replaying the hash-bound inputs under the recorded producer identity (git commit) and deterministic serialization rules.

If reconstruction cannot reproduce identical output bytes, the system is in contract violation.

This guarantee is foundational for Constellation 2.0 audit posture.

# 2. Preconditions

Reconstruction is valid only if all of the following are available:

1) The immutable Bundle F artifacts for day `D` exist:
- `constellation_2/runtime/truth/accounting_v1/nav/<D>/nav.json`
- `constellation_2/runtime/truth/accounting_v1/exposure/<D>/exposure.json`
- `constellation_2/runtime/truth/accounting_v1/attribution/<D>/engine_attribution.json`
- optionally `constellation_2/runtime/truth/accounting_v1/latest.json` (pointer-only)

2) Each artifact includes:
- `producer.git_sha`
- `input_manifest[]` entries with `path` + `sha256`

3) The referenced input files exist at the recorded paths in the same repo revision context.

4) The Bundle F implementation used for reconstruction is exactly the recorded git commit.

# 3. Reconstruction Algorithm (Exact Procedure)

For a target day `D`:

## Step 1 — Identify artifact set

Read `nav/<D>/nav.json` and extract:
- `producer.git_sha`
- `input_manifest`
- (optional) any referenced pointers/hashes

## Step 2 — Verify repository state

Checkout the producing git sha:

- The repository must be in a clean state.
- The producing commit must be checked out exactly.

The auditor must record:
- `git rev-parse HEAD`
- `git status --porcelain=v1` (must be empty)

## Step 3 — Verify input integrity

For each `input_manifest` entry:
- read bytes from `path`
- compute sha256(bytes)
- compare to recorded `sha256`

If any mismatch occurs:
- reconstruction fails with `INPUT_HASH_MISMATCH`
- the system is in violation unless a governed migration layer explicitly explains the mismatch (not allowed by default)

## Step 4 — Replay computation deterministically

Run the Bundle F “compute day” entrypoint for the target day `D` in **reconstruction mode**, which must:

- disable any time-based nondeterminism (use day key inputs only)
- disable randomness
- disable reading “latest” pointers for logic decisions (use explicit day inputs)
- produce candidate artifacts in a staging area (never overwrite truth)

The replay must produce candidate bytes for:
- nav.json
- exposure.json
- engine_attribution.json
- (optionally) latest.json (pointer only)

## Step 5 — Verify output equality

Compute sha256(candidate bytes) for each artifact and compare to the sha256 of the immutable truth artifact bytes.

Pass criteria:
- every artifact byte sequence matches exactly

If any mismatch occurs:
- reconstruction fails with `OUTPUT_HASH_MISMATCH`

# 4. Deterministic Serialization Requirements (Binding)

Bundle F must serialize JSON deterministically. The serialization rules are binding:

- UTF-8
- newline at end of file
- stable key ordering (sorted keys)
- stable whitespace policy (implementation-defined, but fixed)
- numeric values must be finite (no NaN/Inf)
- stable float formatting (must not change across runs under same environment)
- arrays must preserve deterministic order (order must be defined by contract)

If environment differences can alter float formatting, Bundle F must either:
- enforce decimal quantization rules, or
- record formatting policy in `producer.build` and require identical environment for reconstruction.

# 5. Failure Semantics (Reconstruction Context)

Reconstruction may fail for only these reasons (canonical):

- `INPUT_MISSING`
- `INPUT_HASH_MISMATCH`
- `OUTPUT_HASH_MISMATCH`
- `SCHEMA_VIOLATION`
- `ATTEMPTED_REWRITE` (if replay was misconfigured and tried to write to truth root)

In all failure cases, the replay must:
- emit a reconstruction report (separate from accounting truth)
- exit non-zero

# 6. Acceptance Tests (Reconstruction)

Bundle F must include runnable tests that prove:

1) **Replay determinism**
   - Recompute day `D` twice using identical inputs → identical bytes

2) **Historical reconstruction**
   - For a chosen day with stable inputs, replay reproduces immutable truth bytes exactly

3) **Tamper detection**
   - If an input byte is modified, reconstruction must fail with `INPUT_HASH_MISMATCH`

4) **No-overwrite guarantee**
   - Replay must never write into truth root when truth already exists; it must stage and compare only

# 7. Auditor Evidence Packet (Recommended)

For hostile review, Bundle F should support exporting an evidence packet containing:

- the three immutable artifacts
- the input manifest list
- a manifest of input files (paths + hashes)
- producing git sha
- reconstruction command used
- resulting verification hashes

This packet is not itself accounting truth; it is an audit support artifact.

# 8. Non-Claims

This guarantee does not claim:
- that the market marks are “correct,” only that they are the marks used and are provably the same
- that inputs were complete, only that the run status correctly labeled degraded vs fail states
- that external systems were available, only that the system handled absence deterministically
