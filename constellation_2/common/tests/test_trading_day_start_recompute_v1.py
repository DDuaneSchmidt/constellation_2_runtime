from __future__ import annotations

import json
import sys
from pathlib import Path
from subprocess import CompletedProcess

SOURCE_ROOT = Path("/home/node/constellation").resolve()
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_trading_day_start_recompute_v1 as recompute_module
from ops.tools.run_trading_day_start_recompute_v1 import build_recompute_commands


def test_recompute_commands_run_in_canonical_order() -> None:
    truth_root = Path("/tmp/recompute-truth").resolve()
    commands = build_recompute_commands(day_utc="2026-04-14", truth_root=truth_root)
    assert [Path(cmd[1]).name for cmd in commands] == [
        "run_trading_day_control_plane_v1.py",
        "run_trading_day_execution_control_plane_v1.py",
        "run_trading_day_state_machine_v1.py",
    ]
    for cmd in commands:
        assert cmd[-2:] == ["--truth_root", str(truth_root)]


def test_recompute_sequence_runs_through_expected_blocked_exit_codes(monkeypatch) -> None:
    truth_root = Path("/tmp/recompute-truth").resolve()
    seen: list[tuple[str, str]] = []

    def fake_run(cmd: list[str], *, cwd: str, capture_output: bool, text: bool, env: dict[str, str]) -> CompletedProcess[str]:
        tool_name = Path(cmd[1]).name
        seen.append((tool_name, env["C2_TRUTH_ROOT"]))
        payload = json.dumps({"tool": tool_name})
        return CompletedProcess(cmd, 2 if tool_name != "run_trading_day_state_machine_v1.py" else 0, payload, "")

    monkeypatch.setattr(recompute_module.subprocess, "run", fake_run)

    results = recompute_module.run_recompute_sequence(day_utc="2026-04-14", truth_root=truth_root)

    assert [tool_name for tool_name, _ in seen] == [
        "run_trading_day_control_plane_v1.py",
        "run_trading_day_execution_control_plane_v1.py",
        "run_trading_day_state_machine_v1.py",
    ]
    assert {truth_root_value for _, truth_root_value in seen} == {str(truth_root)}
    assert [row["return_code"] for row in results] == [2, 2, 0]
