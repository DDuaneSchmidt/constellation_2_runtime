"""
C2 Operational Day Key Invariant v1

Enforces future-day quarantine per:
- C2_TEST_DAY_QUARANTINE_POLICY_V1

This helper MUST be used by all operational writers that accept --day_utc
and produce immutable runtime truth artifacts.

Invariant:
- day_utc MUST NOT be greater than today_utc (UTC).
- Comparison is lexicographic (YYYY-MM-DD safe).
- Fail-closed.
"""

from __future__ import annotations

from datetime import datetime, timezone


def enforce_operational_day_key_invariant_v1(day_utc: str) -> None:
    if not isinstance(day_utc, str):
        raise SystemExit("FAIL: DAY_UTC_NOT_STRING")

    if len(day_utc) != 10 or day_utc[4] != "-" or day_utc[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {day_utc!r}")

    today_utc = datetime.now(timezone.utc).date().isoformat()

    # Lexicographic compare valid for YYYY-MM-DD
    if day_utc > today_utc:
        raise SystemExit(
            f"FAIL: FUTURE_DAY_UTC_DISALLOWED day_utc={day_utc} today_utc={today_utc}"
        )
