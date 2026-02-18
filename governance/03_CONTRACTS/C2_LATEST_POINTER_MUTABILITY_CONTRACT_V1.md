id: C2_LATEST_POINTER_MUTABILITY_CONTRACT_V1
title: Latest Pointer Mutability Contract v1
status: ACTIVE
type: CONTRACT
domain: governance
version: 1
last_updated_utc: 2026-02-18T00:00:00Z
owners:
  - Constellation Operator
  - Audit/Controls

## Purpose

Define the lifecycle boundary between:

- **Immutable day-keyed truth artifacts** (must never be overwritten)
- **Mutable "latest" pointer artifacts** (must be safely replaceable)

This contract exists to prevent accidental attempts to write mutable pointers using immutable write rules, which would cause permanent operational deadlocks.

## Scope

Applies to all files under runtime truth that represent a *moving pointer* rather than an immutable day-keyed record, including but not limited to:

- `constellation_2/runtime/truth/allocation_v1/latest.json`
- `constellation_2/runtime/truth/accounting_v1/latest.json`
- `constellation_2/runtime/truth/positions_v1/effective_v1/latest_effective.json`

## Requirements

### R1 — Day-keyed truth is immutable

Any artifact stored under a day-keyed path (e.g. `.../<DAY>/...`) is immutable.

- If a file exists at its immutable day-keyed path, it MUST NOT be overwritten.
- If regeneration is required, a new **versioned** artifact path MUST be used.

### R2 — Latest pointers are mutable and atomic

A "latest pointer" file is permitted to change over time.

- It MUST be written using an **atomic replace** mechanism.
- If the file exists and the bytes are identical, the system SHOULD skip writing.
- If the file exists and differs, the system MUST replace it atomically.

### R3 — Latest pointers must never be used as readiness truth

Readiness and audit decisions MUST be based on day-keyed immutable artifacts.

"latest" pointers are operational convenience only.

### R4 — Fail-closed on non-file targets

If a "latest" pointer path exists but is not a file, the writer MUST fail closed.

## Approved Implementation Pattern

The approved primitive for R2 is:

- Write new bytes to a temp file in the same directory
- `fsync`
- `os.replace` into place

## Audit Notes

This contract intentionally makes "latest pointer mutability" explicit to support hostile review. Without it, immutability enforcement may deadlock systems that must legitimately update operational pointers.
