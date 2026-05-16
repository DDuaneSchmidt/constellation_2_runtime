from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
TIMER_PATH = REPO_ROOT / "ops/systemd/user/aegis-event-monitor-v1.timer"
SERVICE_PATH = REPO_ROOT / "ops/systemd/user/aegis-event-monitor-v1.service"


def _read(path: Path) -> str:
    assert path.is_file(), f"missing systemd unit: {path}"
    return path.read_text(encoding="utf-8")


def _on_calendar_lines(timer_text: str) -> list[str]:
    return [line.strip() for line in timer_text.splitlines() if line.startswith("OnCalendar=")]


def test_event_monitor_timer_exists_and_targets_market_hours_only() -> None:
    lines = _on_calendar_lines(_read(TIMER_PATH))

    assert lines == [
        "OnCalendar=Mon..Fri *-*-* 09:30:00 America/New_York",
        "OnCalendar=Mon..Fri *-*-* 09:45:00 America/New_York",
        "OnCalendar=Mon..Fri *-*-* 10..15:00/15:00 America/New_York",
        "OnCalendar=Mon..Fri *-*-* 16:00:00 America/New_York",
    ]
    assert all("Mon..Fri" in line for line in lines)
    assert all("America/New_York" in line for line in lines)
    assert not any("Sat" in line or "Sun" in line for line in lines)
    assert not any(re.search(r"\s0[0-8]:", line) or "17:" in line or "18:" in line for line in lines)
    assert "Persistent=false" in _read(TIMER_PATH)


def test_event_monitor_service_uses_explicit_truth_root_and_current_release() -> None:
    text = _read(SERVICE_PATH)

    assert "Type=oneshot" in text
    assert "run_current_release_tool_v1.sh run_aegis_event_monitor_v1" in text
    assert "--truth_root /home/node/constellation_runtime_data/truth" in text
    assert "--day_utc @today_utc@" in text
    assert "python3 " not in [line for line in text.splitlines() if line.startswith("ExecStart=")][0]
    assert "WorkingDirectory=/home/node/constellation_active" in text


def test_event_monitor_service_has_no_broker_or_live_email_dependency() -> None:
    combined = _read(TIMER_PATH) + "\n" + _read(SERVICE_PATH)
    forbidden = [
        "ibgateway",
        "IB Gateway",
        "TWS",
        "placeOrder",
        "transmit=true",
        "run_aegis_paper_submit",
        "C2_EMAIL_PASSWORD",
        "smtp.gmail.com",
    ]

    for token in forbidden:
        assert token not in combined
    assert "broker_submit_required" not in combined
    assert "no broker submit" in combined.lower()
    assert "no EOD mutation" in combined
