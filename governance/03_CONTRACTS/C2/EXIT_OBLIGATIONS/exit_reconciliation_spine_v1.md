# C2 Exit Reconciliation Spine V1 — Contract

MODE: Audit Surface + Threat Modeling  
STANDARD: Institutional-grade, deterministic, fail-closed, hostile-review safe

## Purpose

This spine produces a deterministic, auditable list of **exit obligations** for Constellation 2.0.

The goal is to eliminate the unsafe ambiguity where an engine can “go silent” and unintentionally hold risk.

**Core rule:** Silence is not a hold instruction. If an engine has an OPEN position and does not emit an explicit intent for the day, the system must create an explicit flatten obligation (`target_notional_pct="0"`).

## Inputs (Authoritative)

1. Positions snapshot (governance schema: `C2_POSITIONS_SNAPSHOT_V2`)
2. Engine intents day directory (truth dir), which may contain multiple intent schemas (e.g., options intents); this spine must only consider **ExposureIntent v1** objects with `schema_id="exposure_intent"` and `schema_version="v1"`.

## Output (Truth Artifact)

`runtime/truth/exit_reconciliation_v1/<DAY>/exit_reconciliation.v1.json`

Schema: `C2_EXIT_RECONCILIATION_V1`

## Determinism Guarantees

Given the same input bytes:
- positions snapshot file bytes
- intents day directory file bytes

The output JSON must be identical (canonical JSON) and content-addressed.

Required:
- stable ordering of obligations
- no non-deterministic timestamps inside obligation content
- produced_utc may vary, but determinism tests must pin it for reproducibility OR compare content hashes excluding produced_utc.

## Threat Model

### Threat: Silent hold
If engine does not emit intent, existing exposure can persist without explicit instruction.

Mitigation:
- Any OPEN position with engine_id missing an explicit ExposureIntent v1 for that engine produces an exit obligation with recommended_target_notional_pct="0".

### Threat: Misclassification of instrument
Positions snapshot may have UNKNOWN instrument fields (bootstrap).

Mitigation:
- If instrument.kind == UNKNOWN or underlying is null, output status must be DEGRADED and reason_codes must include BOOTSTRAP_UNKNOWN_INSTRUMENT_FIELDS; obligations may still be produced using best-effort mapping, but must be clearly marked.

### Threat: Intents dir contains non-exposure intents
Intents truth may include option intents or other schemas.

Mitigation:
- Only consider JSON objects where schema_id=="exposure_intent" AND schema_version=="v1".

### Threat: Partial pipeline completion
Exit reconciliation must not claim OK if positions snapshot missing.

Mitigation:
- If positions snapshot missing/unreadable -> FAIL_MISSING_POSITIONS_SNAPSHOT (fail-closed).

## Fail-Closed Requirements (Downstream enforcement)

This spine alone does not submit orders. The enforcement point is Bundle A2 wiring:

- Submission boundary / OMS must block or enforce explicit flatten for obligations.

This contract requires:
- If exit_reconciliation status is FAIL_* -> trading must be blocked.
- If obligations list is non-empty -> OMS must incorporate them deterministically.

## Audit Lineage Requirements

Output must include:
- producer repo + git_sha + module
- input_manifest with sha256 for each input
- reason_codes
- stable schema_id + schema_version

No output may be overwritten in place; atomic writes required.
