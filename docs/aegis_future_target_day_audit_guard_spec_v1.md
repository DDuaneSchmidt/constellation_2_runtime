# AEGIS Future Target-Day Audit Guard Spec v1

## Guard Status

- `NOT_APPLICABLE_TARGET_DAY_CURRENT_OR_PAST`: strict final audit applies.
- `TARGET_DAY_IN_FUTURE_GUARDED`: target day is future relative to actual runtime date; final-only checks are guarded.
- `TARGET_DAY_IN_FUTURE_UNGUARDED`, `FUTURE_DAY_GUARD_FAILED`, and `UNKNOWN_DETERMINISTIC_BLOCKER` are reserved failure states.

## Audit Modes

- `STRICT_FINAL`: current/past target-day audit.
- `FUTURE_TARGET_DAY_PROVISIONAL`: future target-day projection/audit guard.
- `READ_ONLY_PROJECTION` and `BLOCKED` are reserved for future policy extensions.

Future target-day guard output sets final mark certification, outcome closure, validation sample creation, and evidence-lineage final mark checks to not allowed. It does not create or certify marks, outcomes, samples, or research quality rows.
