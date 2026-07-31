# AEGIS Future Target-Day Audit Guard Design v1

The guard is built early in audit sequencing after the runtime truth kernel. A guarded wrapper replaces direct `aegis:run-sleeves-now` execution in audit: current/past days delegate to the existing runner, while future days return a deterministic skip result.

Evidence-lineage still computes raw mark coverage, but when the guard reports `TARGET_DAY_IN_FUTURE_GUARDED`, missing final marks are not emitted as RED corruption failures. Current and past days retain strict failures.

Package 020 and Package 021 consume the guard artifact so future target-day runs return `TARGET_DAY_IN_FUTURE` instead of stale/final-mark corruption. Portal rendered-text regression avoids hard-coded closed-outcome counts for future or unstable target days.
