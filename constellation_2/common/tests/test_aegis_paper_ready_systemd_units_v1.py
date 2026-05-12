from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SERVICE_PATH = REPO_ROOT / "ops/systemd/user/aegis-paper-ready-kernel-v1.service"
TIMER_PATH = REPO_ROOT / "ops/systemd/user/aegis-paper-ready-kernel-v1.timer"
TARGET_TIMES = ["09:31", "10:15", "11:00", "11:45", "13:30", "14:30", "15:30"]


def _read(path: Path) -> str:
    assert path.is_file(), f"missing source systemd unit: {path}"
    return path.read_text(encoding="utf-8")


def _on_calendar_lines(timer_text: str) -> list[str]:
    return [line.strip() for line in timer_text.splitlines() if line.startswith("OnCalendar=")]


def test_paper_ready_timer_uses_target_seven_run_schedule() -> None:
    lines = _on_calendar_lines(_read(TIMER_PATH))

    assert lines == [f"OnCalendar=Mon..Fri *-*-* {time}:00 America/New_York" for time in TARGET_TIMES]
    assert len(lines) == 7
    assert all("America/New_York" in line for line in lines)
    assert not any("09:29" in line or "15:45" in line for line in lines)


def test_paper_ready_timer_points_to_oneshot_kernel_service() -> None:
    timer_text = _read(TIMER_PATH)
    service_text = _read(SERVICE_PATH)

    assert "Unit=aegis-paper-ready-kernel-v1.service" in timer_text
    assert "Type=oneshot" in service_text
    assert "run_current_release_tool_v1.sh run_aegis_paper_ready_kernel_v1" in service_text
    assert "--target-day @today_utc@" in service_text
    assert "--environment PAPER" in service_text
    assert "--scheduled-run true" in service_text


def test_paper_ready_service_uses_launcher_tool_name_form_only() -> None:
    service_text = _read(SERVICE_PATH)
    exec_lines = [line for line in service_text.splitlines() if line.startswith("ExecStart=")]

    assert len(exec_lines) == 1
    exec_line = exec_lines[0]
    match = re.search(r"run_current_release_tool_v1\.sh\s+([A-Za-z0-9_:-]+)", exec_line)
    assert match, exec_line
    tool_name = match.group(1)
    assert tool_name == "run_aegis_paper_ready_kernel_v1"
    assert "/" not in tool_name
    assert not tool_name.endswith(".py")
    assert "python3 " not in exec_line


def test_paper_ready_timer_source_has_no_runtime_or_policy_controls() -> None:
    combined = _read(TIMER_PATH) + "\n" + _read(SERVICE_PATH)
    forbidden = [
        "placeOrder",
        "transmit=true",
        "clear kill",
        "approval",
        "multi_intent",
        "shadow_candidate_arbitration",
    ]

    for token in forbidden:
        assert token not in combined
