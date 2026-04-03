# C2 Auto Repair Controller v1 Contract

## Purpose

Defines deterministic automation for paper-day health classification and repair triggering.

This contract governs:
- health monitor output state model (`READY|DEGRADED|REPAIR_REQUIRED`)
- automatic repair trigger eligibility
- anti-thrash controls (single active session, cooldown, blocker fingerprinting, relaunch cap)
- operator-auditable outputs

## Scope

Applies to:
- `ops/tools/run_c2_auto_repair_controller_v1.py`
- user-level systemd service/timer wiring for paper-hours execution
- governed report artifacts produced under sleeve truth:
  - `reports/auto_repair_controller_v1/health/<day>/health_supervisor.<timestamp>.v1.json`
  - `reports/auto_repair_controller_v1/trigger/<day>/repair_trigger.<timestamp>.v1.json`
  - `reports/auto_repair_controller_v1/state/controller_state.v1.json`

## Authority and Truth Boundary

- Runtime truth remains authoritative.
- Monitor is read-only with respect to trading lineage surfaces.
- Controller may write only its own governed report/state artifacts.
- Cross-SHA same-day immutability aborts after an earlier canonical PASS are non-blocking advisories.

## Health Classification Rules

Exactly one machine state must be emitted each run:
- `READY`
- `DEGRADED`
- `REPAIR_REQUIRED`

### READY
- Canonical active-day proof exists and is healthy.
- Critical gate stack is PASS.
- Required lifecycle artifacts for expected activity are internally consistent.

### DEGRADED
- Non-blocking advisory conditions exist.
- Includes historical/non-authoritative issues and same-day cross-SHA immutability aborts when canonical PASS already proves health.
- Must not trigger auto-repair.

### REPAIR_REQUIRED
- Active-day healthy proof is missing or invalid.
- Critical gate or required lifecycle contract is broken.
- Blocking orchestration/readiness conditions are present.

## Trigger Rules

Automatic repair launch is allowed only when health state is `REPAIR_REQUIRED`.

Launch must be suppressed when any condition applies:
- existing active repair session is running
- cooldown window is active for same blocker fingerprint
- max relaunch count reached for same blocker/day
- blocker was previously resolved to decision gate and remains unchanged

## Anti-Thrash Requirements

Controller state must persist:
- active session info
- blocker fingerprint history
- launch counts and latest outcomes
- suppression reasons

Controller must recover safely from orphaned/crashed sessions:
- detect inactive PID
- classify last outcome from session log marker
- clear active lock before considering relaunch

## Repair Outcome Markers

Repair session output must end with exactly one marker line:
- `AUTO_REPAIR_OUTCOME: READY`
- `AUTO_REPAIR_OUTCOME: DECISION_GATE`
- `AUTO_REPAIR_OUTCOME: UNRESOLVED`

These markers drive escalation stop/suppression behavior.

## Safety

Forbidden:
- triggering repair from `READY` or `DEGRADED`
- policy relaxation of immutability by controller
- relying on non-authoritative stale artifacts as primary health proof

Required:
- deterministic blocker fingerprinting
- auditable packet/prompt/log paths for each launched session
- fail-closed behavior when essential proof artifacts are missing
