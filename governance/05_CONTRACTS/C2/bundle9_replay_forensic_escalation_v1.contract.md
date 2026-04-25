# bundle9_replay_forensic_escalation_v1

This contract governs Bundle 9 replay and forensic escalation modes.

Legal modes:
- `normal`
- `exact_ref_replay`
- `bounded_recompute`
- `forensic_replay`

## `normal`
- uses the standard active-path input discovery for the target
- may mutate truth only through the target's existing ratified writers

## `exact_ref_replay`
- allowed input basis:
  - explicit artifact refs or an explicit manifest/verdict path
- legality:
  - only for targets whose current semantics can be reconstructed from those explicit refs
- scope:
  - no broad discovery
- mutation:
  - no mutation except deterministic proof emission or already-ratified replay-compatible publication

## `bounded_recompute`
- allowed input basis:
  - explicit prior manifest/verdict refs plus bounded upstream truth already governed for recompute
- legality:
  - only where the target already has a bounded recompute path or a bounded republish path
- scope:
  - may resolve a narrow, contract-defined upstream set
- mutation:
  - only through already-ratified bounded recompute or republish operations

## `forensic_replay`
- explicit escalated mode
- allowed only when the target contract proves a broader discovery need
- MUST be labeled distinctly in proof output
- MUST NOT silently masquerade as `normal` or `bounded_recompute`

First-wave applicability:
- `paper_day_orchestrator_v2`
  - legal: `normal`, `exact_ref_replay`, `bounded_recompute`
  - not legal in Bundle 9: `forensic_replay`
  - reason: broad stage re-execution and truth mutation would exceed Bundle 9 bounded scope
- `c2_ops_cockpit_status_v2_collector_v1`
  - legal: `normal`
  - not legal in Bundle 9: `exact_ref_replay`, `bounded_recompute`, `forensic_replay`
  - reason: the collector is a current-state projection path, not a replay actor

Escalation law:
- callers MUST request the exact mode explicitly
- an unsupported mode MUST fail closed with a deterministic reason code
