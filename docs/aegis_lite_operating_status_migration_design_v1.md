# Aegis Lite Operating Status Migration Design v1

## Architecture

Current:
`Aegis Lite artifacts -> Runtime Truth -> UI/API`

Target:
`Strategic paper/research artifacts -> thin strategic projections -> equivalence checker -> Runtime Truth`

Aegis Lite remains generated and readable as compatibility evidence.

## Replacement Projection Design

The strategic projections are intentionally thin. They do not decide new truth. They compose existing strategic evidence from:
- Operator Action Model
- Scheduled Run Readiness
- Research Portfolio
- Candidate State
- Paper Position Ledger
- Outcome Registry
- Validation Samples
- Statistical Sufficiency
- Research Capital Allocation
- Verified Runtime Graph when present
- Runtime Truth Kernel when present

## Artifact Generation Flow

1. Existing strategic producers generate their canonical evidence.
2. `aegis:strategic-operating-status` writes `aegis_strategic_operating_status_v1`.
3. `aegis:research-eod-summary` writes `aegis_research_eod_summary_v1`.
4. `aegis:lite-migration-equivalence` compares Lite and strategic semantics.
5. Runtime truth may consume strategic artifacts only when equivalence is explicit.

## Equivalence Flow

The equivalence checker compares semantic responsibilities, not raw field names. It checks day, freshness, candidate/action states, blockers, policy flags, source coverage, and safety flags.

## Runtime Truth Impact

After equivalence passes, runtime truth uses strategic evidence for operating status and EOD summary. The Lite status/report stay available as read-only compatibility artifacts and rollback evidence.

## UI Language Impact

Operator UI should say:
- Aegis Operating Status
- Research EOD Summary
- Manual compatibility layer

It should not present Aegis Lite as strategic architecture.

## Rollback

Rollback is a code-level dependency restoration:
- `DATA_READY` dependency list returns to Lite status/report
- `EOD_ADVISORY_MODE` returns to Lite EOD report
- strategic projections remain diagnostics

## Why This Does Not Duplicate Truth

The new artifacts are projections. They carry source paths and hashes and do not create candidate, paper, research, validation, or policy decisions.
