from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


OPEN_TIME_RE = re.compile(r"OnCalendar=.*?(\d{2}):(\d{2})\s+([A-Za-z_\/]+)")
ET_ZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class DayOpenWindowV1:
    day_utc: str
    timezone: str
    bod_time_et: str
    cutoff_time_et: str
    bod_time_utc: str
    cutoff_time_utc: str
    window_status: str
    source_timer_path: str
    cutoff_timer_path: str


def _parse_timer_schedule(timer_path: Path) -> tuple[int, int, str]:
    text = timer_path.read_text(encoding="utf-8")
    match = OPEN_TIME_RE.search(text)
    if match is None:
        raise ValueError(f"OPEN_TIMER_SCHEDULE_UNPARSEABLE:path={timer_path}")
    hour = int(match.group(1))
    minute = int(match.group(2))
    timezone_name = str(match.group(3)).strip()
    return hour, minute, timezone_name


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(ZoneInfo("UTC")).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iso_local(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def _window_status(*, now_utc: datetime, bod_dt_et: datetime, cutoff_dt_et: datetime) -> str:
    now_et = now_utc.astimezone(ET_ZONE)
    if now_et < bod_dt_et:
        return "PRE_OPEN"
    if now_et <= cutoff_dt_et:
        return "OPEN_WINDOW"
    return "POST_OPEN_WINDOW"


def build_day_open_window_v1(
    *,
    repo_root: Path,
    day_utc: str,
    now_utc: datetime | None = None,
) -> DayOpenWindowV1:
    root = Path(repo_root).resolve()
    bod_timer_path = (root / "ops/systemd/user/c2-paper-day-orchestrator.timer").resolve()
    cutoff_timer_path = (root / "ops/systemd/user/c2-global-monitoring-refresh.timer").resolve()

    bod_hour, bod_minute, bod_tz = _parse_timer_schedule(bod_timer_path)
    cutoff_hour, cutoff_minute, cutoff_tz = _parse_timer_schedule(cutoff_timer_path)
    if bod_tz != cutoff_tz:
        raise ValueError(
            f"OPEN_WINDOW_TIMER_TIMEZONE_MISMATCH:bod={bod_tz}:cutoff={cutoff_tz}:"
            f"source={bod_timer_path}:cutoff_path={cutoff_timer_path}"
        )

    local_zone = ZoneInfo(bod_tz)
    bod_dt_et = datetime.strptime(day_utc, "%Y-%m-%d").replace(
        hour=bod_hour,
        minute=bod_minute,
        second=0,
        microsecond=0,
        tzinfo=local_zone,
    )
    cutoff_dt_et = datetime.strptime(day_utc, "%Y-%m-%d").replace(
        hour=cutoff_hour,
        minute=cutoff_minute,
        second=0,
        microsecond=0,
        tzinfo=local_zone,
    )
    observed_now_utc = now_utc or datetime.now(tz=ZoneInfo("UTC"))

    return DayOpenWindowV1(
        day_utc=str(day_utc).strip(),
        timezone=bod_tz,
        bod_time_et=_iso_local(bod_dt_et),
        cutoff_time_et=_iso_local(cutoff_dt_et),
        bod_time_utc=_iso_utc(bod_dt_et),
        cutoff_time_utc=_iso_utc(cutoff_dt_et),
        window_status=_window_status(now_utc=observed_now_utc, bod_dt_et=bod_dt_et, cutoff_dt_et=cutoff_dt_et),
        source_timer_path=str(bod_timer_path),
        cutoff_timer_path=str(cutoff_timer_path),
    )
