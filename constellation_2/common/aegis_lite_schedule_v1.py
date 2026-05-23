from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

LITE_SLEEVE_RUN_TIMES_UTC = ("09:50", "14:50")
LITE_SLEEVE_ON_CALENDARS = tuple(f"OnCalendar=*-*-* {value}:00 UTC" for value in LITE_SLEEVE_RUN_TIMES_UTC)
LITE_SLEEVE_SCHEDULE_VERSION = "aegis_lite_sleeve_schedule.v1"


def parse_utc_instant_v1(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("UTC timestamp must include timezone")
    return parsed.astimezone(UTC).replace(second=0, microsecond=0)


def sleeve_schedule_run_key_v1(value: str | datetime) -> str:
    parsed = parse_utc_instant_v1(value)
    return f"{parsed.date().isoformat()}T{parsed.hour:02d}:{parsed.minute:02d}Z"


def sleeve_schedule_should_fire_v1(value: str | datetime, *, completed_run_keys: set[str] | None = None) -> dict[str, Any]:
    parsed = parse_utc_instant_v1(value)
    time_utc = f"{parsed.hour:02d}:{parsed.minute:02d}"
    run_key = sleeve_schedule_run_key_v1(parsed)
    scheduled = time_utc in LITE_SLEEVE_RUN_TIMES_UTC
    duplicate = run_key in (completed_run_keys or set())
    return {
        "schema_id": "aegis_lite_sleeve_schedule_decision",
        "schema_version": "v1",
        "schedule_version": LITE_SLEEVE_SCHEDULE_VERSION,
        "timestamp_utc": parsed.isoformat().replace("+00:00", "Z"),
        "day_utc": parsed.date().isoformat(),
        "time_utc": time_utc,
        "run_key": run_key,
        "scheduled_times_utc": list(LITE_SLEEVE_RUN_TIMES_UTC),
        "should_fire": bool(scheduled and not duplicate),
        "scheduled_window": bool(scheduled),
        "duplicate": bool(duplicate),
        "replay_safe": True,
    }


def sleeve_schedule_metadata_v1() -> dict[str, Any]:
    return {
        "schema_id": "aegis_lite_sleeve_schedule",
        "schema_version": "v1",
        "schedule_version": LITE_SLEEVE_SCHEDULE_VERSION,
        "timezone": "UTC",
        "times_utc": list(LITE_SLEEVE_RUN_TIMES_UTC),
        "on_calendar": [item.replace("OnCalendar=", "") for item in LITE_SLEEVE_ON_CALENDARS],
        "daily": True,
        "replay_safe": True,
    }
