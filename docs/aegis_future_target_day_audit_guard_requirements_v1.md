# AEGIS Future Target-Day Audit Guard Requirements v1

Package 022 adds a deterministic guard for audits whose `TARGET_DAY` is later than the actual runtime date. Future target days must not be treated as final-certification failures simply because target-day marks and outcomes do not exist yet.

The guard emits `aegis_future_target_day_audit_guard_v1` at `truth/reports/aegis_future_target_day_audit_guard_v1/<TARGET_DAY>/future_target_day_audit_guard.v1.json`.

For current or past target days, strict final mark certification, evidence-lineage mark coverage, outcome closure, and validation sample rules remain enforced. For future target days, final mark certification, outcome closure, validation sample creation, and final mark coverage checks are guarded and reported as `TARGET_DAY_IN_FUTURE`.
